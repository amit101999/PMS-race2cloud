import { buildVirtualToActualMap } from "../../util/mapVirtualToActualCodes.js";
// Shared FIFO re-derivation only — the effective-date rule below stays local so
// this module's preview cannot be silently changed by another module's copy.
import { recomputeHoldingsSnapshot } from "../../util/analytics/holdingsFromTable.js";
import { readCaJobProgress, insertCaTrackPiece } from "../../util/caJobProgress.js";

const HOLDINGS_BATCH = 250;

/**
 * The date a Holdings row takes effect for an as-on-date view.
 * Every type is dated by its transaction date, with one exception: `OPI`
 * ("Security in") is dated by its settlement date, because an inward transfer
 * is not owned until it actually settles.
 *
 * Deliberately local to this controller: the Split module owns this rule for
 * its own preview and must not depend on another module's copy, which may
 * still be on the older settlement-date rule.
 */
const splitEffectiveDate = (h) => {
  const setD = String(h.SETTLEMENT_DATE || "").slice(0, 10);
  const txD  = String(h.TRANSACTION_DATE || "").slice(0, 10);
  const typ  = String(h.TYPE || "").trim().toUpperCase();
  // MERGER / DEMERGER rows keep each lot's ORIGINAL trade date in
  // TRANSACTION_DATE and carry the event's record date on CA_DATE. Date them by
  // the event, or one corporate action scatters across the timeline at dates
  // years before it happened. Mirrors `holdingsEffectiveDate` in
  // util/analytics/holdingsFromTable.js — `recomputeHoldingsSnapshot` below
  // groups on the same rule, so the two must agree.
  if (typ === "MERGER" || typ === "DEMERGER") {
    const caD = String(h.CA_DATE || "").slice(0, 10);
    if (caD) return caD;
  }
  return typ === "OPI" ? setD || txD : txD || setD;
};

/**
 * Normalise an incoming date to `YYYY-MM-DD` without ever crossing a timezone
 * boundary.
 *
 * The old `new Date(v).setHours(0,0,0,0)` + `toISOString()` moved the date back
 * a day in any zone ahead of UTC (IST included): local midnight on the 28th is
 * 18:30Z on the 27th, so the chosen date silently became the previous one. It
 * only looked correct because the deployed runtime is UTC, where setHours(0…) is
 * a no-op — which is why this reproduced locally but not on Catalyst.
 */
const toISODateOnly = (v) => {
  const s = String(v ?? "").trim();
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (m) return `${m[1]}-${m[2]}-${m[3]}`; // ISO date or datetime — take the date part
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return "";
  const p = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`;
};

/** Exclusive next-day cutoff, stepped in UTC so the local zone cannot shift it. */
const nextISODate = (isoDate) => {
  const d = new Date(`${isoDate}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + 1);
  return d.toISOString().split("T")[0];
};

export const getAllSecuritiesISINs = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app not initialized" });
    }
    const zcql = app.zcql();

    const LIMIT = 300;
    let offset = 0;
    let hasMore = true;

    const securities = [];

    while (hasMore) {
      const query = `
        SELECT ISIN, Security_Code, Security_Name
        FROM Security_List
        WHERE ISIN IS NOT NULL
        LIMIT ${LIMIT} OFFSET ${offset}
      `;

      const response = await zcql.executeZCQLQuery(query);

      if (!response || response.length === 0) {
        hasMore = false;
        break;
      }

      response.forEach((row) => {
        const sec = row.Security_List;
        securities.push({
          isin: sec.ISIN,
          securityCode: sec.Security_Code,
          securityName: sec.Security_Name,
        });
      });

      offset += LIMIT;
    }

    return res.status(200).json({
      success: true,
      count: securities.length,
      data: securities,
    });
  } catch (error) {
    console.error("Error fetching securities:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to fetch security list",
    });
  }
};

const ZCQL_ROW_LIMIT = 270;

export const previewStockSplit = async (req, res) => {
  try {
    if (!req.catalystApp) {
      return res.status(500).json({
        success: false,
        message: "Catalyst app not initialized",
      });
    }

    const { isin, ratio1, ratio2, issueDate } = req.body;

    const r1 = Number(ratio1);
    const r2 = Number(ratio2);

    if (!isin || !issueDate || !Number.isFinite(r1) || !Number.isFinite(r2) || r1 <= 0) {
      return res.status(400).json({
        success: false,
        message: "Invalid ISIN, ratio or date",
      });
    }

    const issueDateISO = toISODateOnly(issueDate);
    if (!issueDateISO) {
      return res.status(400).json({ success: false, message: "Invalid date" });
    }

    // cutoff = day AFTER issueDate for the date filter (exclusive upper bound)
    const cutoff = nextISODate(issueDateISO);

    const zcql = req.catalystApp.zcql();

    /* ======================================================
       STEP 1: READ Holdings AS OF issueDate
       Fetch all Holdings rows for this ISIN with date filter.
       Walk in FIFO order (CREATEDTIME ASC, ROWID ASC) and keep
       updating per account — last row per account = state on issueDate.
       ====================================================== */
    const latestByAccount = new Map(); // accountCode → last Holdings row
    const rowsByAccount = new Map(); // accountCode → every effective row, for re-derivation
    // ZCQL's OFFSET is 1-based, so each page repeats the previous page's last
    // row; skip it or recomputeHoldingsSnapshot counts that trade twice.
    const seenRowIds = new Set();
    let offset = 0;

    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT ROWID, WS_Account_code, HOLDING, WAP, HOLDING_VALUE,
               QUANTITY, PRICE, TOTAL_AMOUNT,
               TYPE, SETTLEMENT_DATE, TRANSACTION_DATE, CA_DATE
        FROM Holdings
        WHERE ISIN = '${isin}'
          AND (SETTLEMENT_DATE < '${cutoff}' OR TRANSACTION_DATE < '${cutoff}')
        ORDER BY CREATEDTIME ASC, ROWID ASC
        LIMIT ${HOLDINGS_BATCH} OFFSET ${offset}
      `);

      if (!batch || batch.length === 0) break;

      for (const row of batch) {
        const h = row.Holdings || row;
        if (h.ROWID != null) {
          if (seenRowIds.has(h.ROWID)) continue;
          seenRowIds.add(h.ROWID);
        }
        const acc = String(h.WS_Account_code || "").trim();
        if (!acc) continue;
        // The SQL clause above deliberately ORs both dates — it cannot express
        // the OPI exception on its own. Apply the per-type rule here BEFORE the
        // row can become this account's "latest" snapshot, otherwise a row that
        // is not yet effective overwrites (and then wrongly drops) the real one.
        const eff = splitEffectiveDate(h);
        if (eff && eff >= cutoff) continue;
        latestByAccount.set(acc, h); // last row per account wins
        if (!rowsByAccount.has(acc)) rowsByAccount.set(acc, []);
        rowsByAccount.get(acc).push(h);
      }

      if (batch.length < HOLDINGS_BATCH) break;
      offset += HOLDINGS_BATCH;
    }

    if (latestByAccount.size === 0) {
      return res.json({ success: true, data: [] });
    }

    /* The stored HOLDING is a running balance frozen in settlement order at
       rebuild time, so it can contradict the rows the trade-date rule selected
       above — it may include a buy that is not yet effective on the issue date.
       Re-derive the position from the rows themselves. Once the Holdings table
       has been rebuilt on the new basis this becomes a no-op. */
    for (const [acc, accRows] of rowsByAccount) {
      const snap = recomputeHoldingsSnapshot(accRows);
      if (snap.HOLDING <= 0) {
        latestByAccount.delete(acc); // fully sold as at the issue date
        continue;
      }
      latestByAccount.set(acc, { ...latestByAccount.get(acc), ...snap });
    }

    if (latestByAccount.size === 0) {
      return res.json({ success: true, data: [] });
    }

    /* ======================================================
       STEP 2: CALCULATE SPLIT PREVIEW (simple arithmetic)
       Holdings table already has HOLDING, WAP, HOLDING_VALUE
       computed by FIFO. No need to re-run FIFO engine.

       Split logic:
         newHolding      = floor(currentHolding × ratio2 / ratio1)
         holdingValue    = unchanged (same total cost)
         newWAP          = holdingValue / newHolding (price per share halves)
       ====================================================== */
    const virtualToActual = await buildVirtualToActualMap(
      zcql,
      [...latestByAccount.keys()],
    );
    const preview = [];
    const splitMultiplier = r2 / r1;

    for (const [accountCode, h] of latestByAccount) {
      const currentHolding = Number(h.HOLDING) || 0;
      if (currentHolding <= 0) continue; // fully sold before issueDate

      const holdingValue = Number(h.HOLDING_VALUE) || 0;
      const currentWAP   = Number(h.WAP) || 0;

      const newHolding = Math.floor(currentHolding * splitMultiplier);
      if (newHolding <= 0) continue;

      // Cost is unchanged — more shares at same total cost → WAP goes down
      const newWAP = newHolding > 0
        ? Math.round((holdingValue / newHolding) * 100) / 100
        : 0;

      preview.push({
        isin,
        accountCode, // WS_Account_code (virtual)
        actualCode: virtualToActual.get(accountCode) || "",
        currentHolding,
        currentWAP,
        holdingValue,
        newHolding,
        newWAP,
        delta: newHolding - currentHolding,
      });
    }

    return res.json({ success: true, data: preview });
  } catch (error) {
    console.error("Preview split error:", error);
    return res.status(500).json({
      success: false,
      message: error.message,
    });
  }
};

// Each RebuildHoldingtable job handles at most this many accounts. Smaller
// batches → more, shorter parallel jobs (run concurrently in the job pool),
// avoiding a single long-running job timing out as the account count grows.
const REBUILD_HOLDINGS_BATCH = 100;

const escSql = (v) => String(v ?? "").replace(/'/g, "''");

/**
 * Progress tracking for one "Confirm & Apply Split" click, so the UI can show a
 * single "in progress → done" notification that only settles once ALL the
 * background work is actually finished — the holdings rebuild AND any bonus
 * recompute the split triggers (which itself fans out into more rebuild jobs).
 *
 * The mechanism lives in util/caJobProgress.js (shared with the bonus flow):
 * a parent `Jobs` row named with the track key + one `JobStatusPerAccount` row
 * per piece. See finalizeCaTrackIfNeeded / settleCaTrackPiece in
 * functions/RebuildHoldingtable and functions/UpdateBonusTable.
 */

export const addStockSplit = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({
        success: false,
        message: "Catalyst app not initialized",
      });
    }
    const zcql = app.zcql();

    const { securityCode, securityName, ratio1, ratio2, issueDate, isin } =
      req.body;

    if (!securityCode || !securityName || !ratio1 || !ratio2 || !issueDate || !isin) {
      return res.status(400).json({
        message: "Missing required fields (ISIN is required to scope the holdings rebuild)",
      });
    }

    const isinEsc = String(isin ?? "").replace(/'/g, "''");
    const issueDateEsc = String(issueDate ?? "").replace(/'/g, "''");

    // Idempotency guard (mirrors bonus's bonusRowExists): a split event is
    // uniquely identified by ISIN + Issue_Date + ratios. If it already exists,
    // skip the insert AND the rebuild — re-applying would otherwise insert a
    // duplicate Split row and apply the split ratio twice.
    const existingSplit = await zcql.executeZCQLQuery(`
      SELECT ROWID FROM Split
      WHERE ISIN = '${isinEsc}'
        AND Issue_Date = '${issueDateEsc}'
        AND Ratio1 = ${Number(ratio1)}
        AND Ratio2 = ${Number(ratio2)}
      LIMIT 1
    `);
    if (existingSplit && existingSplit.length > 0) {
      console.log(
        `[SplitController] Split already applied for ${isin} on ${issueDate} ` +
          `(${ratio1}:${ratio2}) — idempotent skip, no rebuild.`,
      );
      return res.status(200).json({
        success: true,
        alreadyApplied: true,
        message:
          "Split already applied for this ISIN, date and ratio — skipped (no duplicate inserted).",
      });
    }

    await zcql.executeZCQLQuery(`
      INSERT INTO Split
      (
        Security_Code,
        Security_Name,
        Ratio1,
        Ratio2,
        Issue_Date,
        ISIN
      )
      VALUES
      (
        '${securityCode}',
        '${securityName}',
        ${Number(ratio1)},
        ${Number(ratio2)},
        '${issueDate}',
        '${isin}'
      )
    `);

    const accountSet = new Set();
    let holdOffset = 0;

    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT WS_Account_code
        FROM Transaction
        WHERE ISIN='${isinEsc}'
        LIMIT ${ZCQL_ROW_LIMIT} OFFSET ${holdOffset}
      `);

      if (!batch || batch.length === 0) break;
      batch.forEach((r) => accountSet.add(r.Transaction.WS_Account_code));
      if (batch.length < ZCQL_ROW_LIMIT) break;
      holdOffset += ZCQL_ROW_LIMIT;
    }

    const affectedAccounts = Array.from(accountSet);

    // One track key per Apply-Split click. The parent Jobs row starts RUNNING;
    // each rebuild chunk and each bonus job below registers a "piece" row that
    // it flips when it finishes. The last piece to finish settles the parent.
    const caTrackKey = `SPL${Date.now()}`.slice(0, 20);
    try {
      await zcql.executeZCQLQuery(
        `INSERT INTO Jobs (jobName, status) VALUES ('${escSql(caTrackKey)}', 'RUNNING')`,
      );
    } catch (e) {
      console.warn("[SplitController] parent track row insert failed:", e.message);
    }

    if (affectedAccounts.length > 0) {
      const scheduling = app.jobScheduling();
      for (let i = 0; i < affectedAccounts.length; i += REBUILD_HOLDINGS_BATCH) {
        const chunk = affectedAccounts.slice(i, i + REBUILD_HOLDINGS_BATCH);
        const catalystJobName = `H${Date.now()}${i}`.slice(0, 20);
        const caSubId = `${caTrackKey}_h${i}`;
        await insertCaTrackPiece(zcql, caTrackKey, caSubId);
        await scheduling.JOB.submitJob({
          job_name: catalystJobName,
          jobpool_name: "CorporateActions",
          target_name: "RebuildHoldingtable",
          target_type: "Function",
          /* Catalyst Job Pool: retries only on execution failure. Min interval 1m. */
          job_config: {
            number_of_retries: 5,
            retry_interval: 60 * 1000,
          },
          params: {
            accountCodesJson: JSON.stringify(chunk),
            isinsJson: JSON.stringify([isin]),
            source: "SplitController",
            caTrackKey,
            caSubId,
          },
        });
      }
    }

    // A bonus already applied on/after this split's date had its BonusShare
    // sized on the PRE-split holding — it is now stale. Re-run the bonus apply
    // in recompute mode (the Split row above is now in the DB, so the bonus
    // FIFO will re-size on the post-split holding and overwrite BonusShare).
    // The recompute job re-queues its own RebuildHoldingtable afterward.
    let bonusPiecesRegistered = 0;
    try {
      const bonusRecords = await zcql.executeZCQLQuery(`
        SELECT SecurityCode, SecurityName, ISIN, Ratio1, Ratio2, ExDate
        FROM Bonus_Record
        WHERE ISIN = '${isinEsc}' AND ExDate >= '${issueDateEsc}'
        ORDER BY ExDate ASC
      `);

      const scheduling = app.jobScheduling();
      let bonusIdx = 0;
      for (const row of bonusRecords || []) {
        const b = row.Bonus_Record || row;
        const exDateISO = String(b.ExDate || "").slice(0, 10);
        if (!exDateISO) continue;

        // Unique tracking name so UpdateBonusTable's per-account SUCCESS guard
        // (scoped by jobName) does not skip a prior bonus run for this ex-date.
        const bonusJobName = `BONRC_${isin.slice(-6)}_${exDateISO}_${Date.now()}`;
        // A "bonus phase" piece for the split notification. It stays RUNNING
        // until UpdateBonusTable has re-sized this bonus AND handed off to the
        // rebuild jobs it spawns (which register their own pieces) — so the
        // parent can never read "all done" while a bonus fix is still pending.
        const caSubId = `${caTrackKey}_b${bonusIdx}`;
        await insertCaTrackPiece(zcql, caTrackKey, caSubId);
        bonusPiecesRegistered += 1;
        bonusIdx += 1;

        await scheduling.JOB.submitJob({
          job_name: `BRC${Date.now()}`.slice(0, 20),
          jobpool_name: "CorporateActions",
          target_name: "UpdateBonusTable",
          target_type: "Function",
          job_config: {
            number_of_retries: 5,
            retry_interval: 60 * 1000,
          },
          params: {
            isin,
            ratio1: String(Number(b.Ratio1) || 0),
            ratio2: String(Number(b.Ratio2) || 0),
            exDate: exDateISO,
            secCode: b.SecurityCode || securityCode || "",
            secName: b.SecurityName || securityName || "",
            recompute: "true",
            jobName: bonusJobName,
            caTrackKey,
            caSubId,
          },
        });
        console.log(
          `[SplitController] Queued bonus recompute for ${isin} ex-date ${exDateISO}`,
        );
      }
    } catch (recomputeErr) {
      console.error(
        "[SplitController] Failed to queue bonus recompute:",
        recomputeErr.message,
      );
    }

    // Nothing to wait for (no affected accounts, no bonus to recompute) — settle
    // the parent now so the notification completes on the first status poll.
    if (affectedAccounts.length === 0 && bonusPiecesRegistered === 0) {
      try {
        await zcql.executeZCQLQuery(
          `UPDATE Jobs SET status = 'COMPLETED' WHERE jobName = '${escSql(caTrackKey)}'`,
        );
      } catch (e) {
        console.warn("[SplitController] parent immediate-complete failed:", e.message);
      }
    }

    return res.status(200).json({
      success: true,
      catalystJobName: caTrackKey,
      message: "Stock split applied successfully",
    });
  } catch (error) {
    console.error("Error in addStockSplit", error);
    return res.status(500).json({
      success: false,
      message: "Failed to apply stock split",
    });
  }
};

/**
 * GET /split/job-status?key=SPL...
 * Reports the overall state of one "Confirm & Apply Split" run.
 *
 *   RUNNING               → work still in progress
 *   COMPLETED             → every piece finished cleanly
 *   COMPLETED_WITH_ERRORS → finished, but at least one rebuild chunk failed
 *   ERROR                 → parent row was stuck RUNNING past the stale window
 *   NOT_STARTED           → no such key (unknown / never created)
 *
 * Belt and suspenders: if the parent still says RUNNING but no piece rows are
 * RUNNING any more, this settles it here too (in case the finishing job's own
 * finalize call lost a race).
 */
export const getSplitApplyStatus = async (req, res) => {
  try {
    if (!req.catalystApp) {
      return res.status(500).json({ success: false, message: "Catalyst app not initialized" });
    }
    const key = String(req.query.key ?? req.query.jobName ?? "").trim();
    if (!key) {
      return res.status(400).json({ success: false, message: "Query parameter key is required" });
    }

    const status = await readCaJobProgress(req.catalystApp.zcql(), key);
    return res.status(200).json({ success: true, key, status });
  } catch (error) {
    console.error("[getSplitApplyStatus]", error);
    return res.status(500).json({
      success: false,
      message: error?.message || "Failed to fetch split job status",
    });
  }
};
