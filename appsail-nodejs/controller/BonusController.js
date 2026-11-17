import { buildVirtualToActualMap } from "../util/mapVirtualToActualCodes.js";
// Shared FIFO re-derivation only — the effective-date rule below stays local so
// this module's preview cannot be silently changed by another module's copy.
import { recomputeHoldingsSnapshot } from "../util/analytics/holdingsFromTable.js";
import { readCaJobProgress, insertCaTrackPiece } from "../util/caJobProgress.js";
import { computeBonusPreviewRows } from "../util/analytics/bonusPreviewCalc.js";
import {
  BONUS_STATUS,
  bonusAccountWhere,
  bonusStage,
  isBonusApplied,
  readBonusAccountRows,
} from "../util/analytics/bonusAccountRows.js";
import { readLatestCorpActionRows, readLatestReceiptRows } from "./corporateAction/CustodianUploadController.js";

const escSql = (v) => String(v ?? "").replace(/'/g, "''");

const ZCQL_ROW_LIMIT = 270;
const HOLDINGS_BATCH  = 250;

/**
 * The date a Holdings row takes effect for an as-on-date view.
 * Every type is dated by its transaction date, with one exception: `OPI`
 * ("Security in") is dated by its settlement date, because an inward transfer
 * is not owned until it actually settles.
 *
 * Deliberately local to this controller: the Bonus module owns this rule for
 * its own preview and must not depend on another module's copy, which may
 * still be on the older settlement-date rule.
 */
const bonusEffectiveDate = (h) => {
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

const parseCatalystTime = (ct) => {
  if (!ct) return 0;
  const fixed = ct.replace(/:(\d{3})$/, ".$1");
  const ms = new Date(fixed).getTime();
  return isNaN(ms) ? 0 : ms;
};

/* ======================================================
   GET ALL SECURITIES
   ====================================================== */
export const getAllSecuritiesISINs = async (req, res) => {
  try {
    if (!req.catalystApp) {
      return res.status(500).json({
        success: false,
        message: "Catalyst app not initialized",
      });
    }

    const zcql = req.catalystApp.zcql();
    const LIMIT = 270;
    let offset = 0;
    const securities = [];

    while (true) {
      const rows = await zcql.executeZCQLQuery(`
        SELECT ISIN, Security_Code, Security_Name
        FROM Security_List
        WHERE ISIN IS NOT NULL
        LIMIT ${LIMIT} OFFSET ${offset}
      `);

      if (!rows || rows.length === 0) break;

      rows.forEach((r) => {
        const s = r.Security_List;
        securities.push({
          isin: s.ISIN,
          securityCode: s.Security_Code,
          securityName: s.Security_Name,
        });
      });

      offset += LIMIT;
    }

    return res.json({ success: true, data: securities });
  } catch (error) {
    console.error(error);
    return res.status(500).json({
      success: false,
      message: error.message,
    });
  }
};

/* ======================================================
   PREVIEW BONUS (HOLDINGS TABLE – DATE AWARE)
   ====================================================== */
export const previewStockBonus = async (req, res) => {
  try {
    if (!req.catalystApp) {
      return res.status(500).json({
        success: false,
        message: "Catalyst app not initialized",
      });
    }

    // Eligibility is decided by the holding at the end of the record date.
    const { isin, ratio1, ratio2, recordDate } = req.body;

    const r1 = Number(ratio1);
    const r2 = Number(ratio2);

    if (!isin || !recordDate || !Number.isFinite(r1) || !Number.isFinite(r2) || r1 <= 0) {
      return res.status(400).json({
        success: false,
        message: "Invalid ISIN, ratio or record date",
      });
    }

    const recordDateISO = toISODateOnly(recordDate);
    if (!recordDateISO) {
      return res.status(400).json({ success: false, message: "Invalid record date" });
    }

    // cutoff = day AFTER the record date for the date filter (exclusive upper bound)
    const cutoff = nextISODate(recordDateISO);

    const zcql = req.catalystApp.zcql();

    // Where this bonus is in the custodian reconciliation, and each account's status.
    const bonusEvent = { isin, recordDateISO, r1, r2 };
    const accountRows = await readBonusAccountRows(zcql, bonusEvent);
    const stage = bonusStage(accountRows, await isBonusApplied(zcql, bonusEvent));
    const statusByAccount = new Map(accountRows.map((r) => [r.accountCode, r]));

    /* ======================================================
       STEP 1: READ Holdings AS OF the record date
       Fetch all Holdings rows for this ISIN with date filter.
       Walk in FIFO order (CREATEDTIME ASC, ROWID ASC) and keep
       updating per account — last row per account = state on the record date.
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
        const eff = bonusEffectiveDate(h);
        if (eff && eff >= cutoff) continue;
        // This bonus's own BONUS row (dated on the record date) is its result,
        // not the holding it is sized on. Same rule as computeBonusPreviewRows.
        if (String(h.TYPE || "").trim().toUpperCase() === "BONUS" && eff === recordDateISO) continue;
        latestByAccount.set(acc, h); // last row per account wins
        if (!rowsByAccount.has(acc)) rowsByAccount.set(acc, []);
        rowsByAccount.get(acc).push(h);
      }

      if (batch.length < HOLDINGS_BATCH) break;
      offset += HOLDINGS_BATCH;
    }

    if (latestByAccount.size === 0) {
      return res.json({ success: true, data: [], stage });
    }

    /* The stored HOLDING is a running balance frozen in settlement order at
       rebuild time, so it can contradict the rows the trade-date rule selected
       above — it may include a buy that is not yet effective on the record date.
       Re-derive the position from the rows themselves. Once the Holdings table
       has been rebuilt on the new basis this becomes a no-op. */
    for (const [acc, accRows] of rowsByAccount) {
      const snap = recomputeHoldingsSnapshot(accRows);
      if (snap.HOLDING <= 0) {
        latestByAccount.delete(acc); // fully sold as at the record date
        continue;
      }
      latestByAccount.set(acc, { ...latestByAccount.get(acc), ...snap });
    }

    if (latestByAccount.size === 0) {
      return res.json({ success: true, data: [], stage });
    }

    /* ======================================================
       STEP 2: CALCULATE BONUS PREVIEW (simple arithmetic)
       Holdings table already has HOLDING, WAP, HOLDING_VALUE
       computed by FIFO. No need to re-run FIFO engine.
       ====================================================== */
    const virtualToActual = await buildVirtualToActualMap(
      zcql,
      [...latestByAccount.keys()],
    );
    const preview = [];

    for (const [accountCode, h] of latestByAccount) {
      const currentHolding = Number(h.HOLDING) || 0;
      if (currentHolding <= 0) continue; // fully sold by the record date

      const holdingValue = Number(h.HOLDING_VALUE) || 0;
      const currentWAP   = Number(h.WAP) || 0;

      // Bonus shares = floor(currentHolding × ratio1 / ratio2)
      const bonusShares = Math.floor((currentHolding * r1) / r2);
      if (bonusShares <= 0) continue;

      const newHolding = currentHolding + bonusShares;

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
        bonusShares,
        newHolding,
        newWAP,
        delta: bonusShares,
        status: statusByAccount.get(accountCode)?.status || "",
        receivedDate: statusByAccount.get(accountCode)?.receivedDate || "",
      });
    }

    // An applied bonus can gain an account later (a back-dated trade uploaded
    // after Confirm). Such an account has no account row, so reopen stage 1 for
    // it: Send as Receivable then adds only the missing accounts. Bonuses applied
    // before this reconciliation existed have no account rows and stay RECEIVED.
    const hasMissingAccount = preview.some((row) => !statusByAccount.has(row.accountCode));
    const pageStage =
      stage === "RECEIVED" && accountRows.length > 0 && hasMissingAccount ? "NEW" : stage;

    return res.json({ success: true, data: preview, stage: pageStage });
  } catch (error) {
    console.error("Preview bonus error:", error);
    return res.status(500).json({
      success: false,
      message: error.message,
    });
  }
};

/* ======================================================
   CUSTODIAN RECONCILIATION (Receivable → Received)
   Stage 1: compare with the Ex-Entitlement file, then "Send as Receivable"
            inserts one Bonus_Record account row per account (Status RECEIVABLE).
   Stage 2: compare with the Corporate Benefit Collection file, then
            "Confirm & Apply" stores each account's Received Date and starts
            the bonus job, which marks the accounts RECEIVED once the bonus is
            applied. See util/analytics/bonusAccountRows.js.
   ====================================================== */

const ACCOUNT_ROW_CHUNK = 200;

/**
 * Store the Received Date the Corporate Benefit Collection file gives each
 * Receivable account (matched on its actual / UCC code); Status stays
 * Receivable until the bonus job has applied the bonus. Accounts the file does
 * not list keep no date. A missing Benefit event stops before anything is written.
 *
 * @returns {Promise<{ ok: true } | { code: string, error: string }>}
 */
const recordBonusReceivedDates = async (app, zcql, { isin, recordDateISO, receivable }) => {
  let allocationRows;
  try {
    ({ rows: allocationRows } = await readLatestReceiptRows(app, { caType: "bonus", isin, recordDateISO }));
  } catch (err) {
    if (err.code) return { code: err.code, error: err.message };
    throw err;
  }

  // The latest date is when the last of a client's shares arrived.
  const receivedDateByActual = new Map();
  for (const row of allocationRows) {
    if (!row.receivedDate) continue;
    const previous = receivedDateByActual.get(row.accountCode);
    if (!previous || row.receivedDate > previous) receivedDateByActual.set(row.accountCode, row.receivedDate);
  }

  const virtualToActual = await buildVirtualToActualMap(
    zcql,
    receivable.map((r) => r.accountCode),
  );
  const rowIdsByDate = new Map();
  for (const r of receivable) {
    const actual = String(virtualToActual.get(r.accountCode) || r.accountCode).toUpperCase();
    const date = receivedDateByActual.get(actual);
    if (!date) continue;
    if (!rowIdsByDate.has(date)) rowIdsByDate.set(date, []);
    rowIdsByDate.get(date).push(r.rowId);
  }

  for (const [date, rowIds] of rowIdsByDate) {
    for (let i = 0; i < rowIds.length; i += ACCOUNT_ROW_CHUNK) {
      await zcql.executeZCQLQuery(`
        UPDATE Bonus_Record SET Received_Date = '${escSql(date)}'
        WHERE ROWID IN (${rowIds.slice(i, i + ACCOUNT_ROW_CHUNK).join(", ")})
      `);
    }
  }
  return { ok: true };
};

/**
 * POST /bonus/send-receivable
 * body: { isin, securityCode, securityName, ratio1, ratio2, recordDate, effectiveDate }
 *
 * Stage 1 → 2: store every account of the bonus preview in Bonus_Record (one
 * account row each, Status RECEIVABLE). The rows are recomputed here rather
 * than taken from the browser, and an uploaded Ex-Entitlement file must have
 * this bonus (ISIN + record date).
 *
 * For a bonus that is already applied, only accounts with no account row are
 * sent (an account that turned up later, e.g. a back-dated trade). Accounts
 * already Received are never touched.
 */
export const sendBonusReceivable = async (req, res) => {
  try {
    const app = req.catalystApp;
    const zcql = app.zcql();
    const { isin, securityCode, securityName, ratio1, ratio2, recordDate, effectiveDate } = req.body || {};
    const r1 = Number(ratio1);
    const r2 = Number(ratio2);
    const recordDateISO = toISODateOnly(recordDate);
    const effectiveDateISO = effectiveDate ? toISODateOnly(effectiveDate) : recordDateISO;
    if (!isin || !recordDateISO || !effectiveDateISO || !(r1 > 0) || !(r2 > 0)) {
      return res.status(400).json({ success: false, message: "ISIN, ratio and record date are required." });
    }

    const bonusEvent = { isin, recordDateISO, r1, r2 };
    const alreadyApplied = await isBonusApplied(zcql, bonusEvent);
    const existing = await readBonusAccountRows(zcql, bonusEvent);
    if (existing.some((r) => r.status === BONUS_STATUS.RECEIVABLE)) {
      return res.status(409).json({
        success: false,
        code: "ALREADY_SENT",
        message: "These accounts are already Receivable. Compare with the Corporate Benefit Collection file next.",
      });
    }
    const alreadyAppliedMessage = "This bonus has already been applied.";
    // Applied before this reconciliation existed: no rows to tell which accounts are missing.
    if (alreadyApplied && existing.length === 0) {
      return res.status(409).json({ success: false, code: "ALREADY_SENT", message: alreadyAppliedMessage });
    }

    const previewRows = await computeBonusPreviewRows(zcql, { isin, r1, r2, recordDateISO });
    const listedAccounts = new Set(existing.map((r) => r.accountCode));
    const rowsToSend = previewRows.filter((row) => !listedAccounts.has(row.accountCode));
    if (rowsToSend.length === 0) {
      if (alreadyApplied) {
        return res.status(409).json({ success: false, code: "ALREADY_SENT", message: alreadyAppliedMessage });
      }
      return res.status(409).json({ success: false, code: "NO_HOLDINGS", message: "No eligible holdings on the record date." });
    }

    // Throws NO_CUSTODIAN_ROWS (caught below) when no uploaded file has this bonus.
    await readLatestCorpActionRows(app, { caType: "bonus", isin, recordDateISO });

    const table = app.datastore().table("Bonus_Record");
    const newRows = rowsToSend.map((row) => ({
      ISIN: isin,
      SecurityCode: securityCode || "",
      SecurityName: securityName || "",
      WS_Account_code: row.accountCode,
      Ratio1: r1,
      Ratio2: r2,
      Record_Date: recordDateISO,
      ExDate: effectiveDateISO,
      Old_Holding: row.currentHolding,
      Bonus_Shares: row.bonusShares,
      Status: BONUS_STATUS.RECEIVABLE,
    }));
    try {
      for (let i = 0; i < newRows.length; i += ACCOUNT_ROW_CHUNK) {
        await table.insertRows(newRows.slice(i, i + ACCOUNT_ROW_CHUNK));
      }
    } catch (insertErr) {
      // A half-sent bonus would show as Receivable with accounts missing. No
      // account row was Receivable before this call (checked above), so the
      // Receivable rows are exactly the ones inserted here — remove only those
      // (Received rows of an applied bonus stay) and let the user send again.
      try {
        await zcql.executeZCQLQuery(
          `DELETE FROM Bonus_Record WHERE ${bonusAccountWhere(bonusEvent)} AND Status = '${BONUS_STATUS.RECEIVABLE}'`,
        );
      } catch (cleanupErr) {
        console.error("[BonusController] Could not remove a half-sent receivable:", cleanupErr.message);
      }
      throw insertErr;
    }

    return res.json({
      success: true,
      count: newRows.length,
      message: `${newRows.length} account(s) updated with status Receivable.`,
    });
  } catch (error) {
    if (error.code) return res.status(409).json({ success: false, code: error.code, message: error.message });
    console.error("[BonusController] sendBonusReceivable:", error);
    return res.status(500).json({ success: false, message: error.message || "Could not send the bonus as receivable." });
  }
};

/* ======================================================
   APPLY BONUS (BACKGROUND JOB)
   ====================================================== */
/**
 * POST /bonus/apply — the normal Confirm & Apply (custodian check required).
 * POST /bonus/apply-bypass — temporary: applies without the custodian files.
 * The switch comes from the route only, never from the request body, so the
 * normal route can never skip the check.
 */
export const applyStockBonus = (req, res) => runBonusApply(req, res, { bypassCustodian: false });
export const applyStockBonusBypass = (req, res) => runBonusApply(req, res, { bypassCustodian: true });

const runBonusApply = async (req, res, { bypassCustodian }) => {
  try {
    if (!req.catalystApp) {
      return res.status(500).json({
        success: false,
        message: "Catalyst app not initialized",
      });
    }

    const {
      isin,
      ratio1,
      ratio2,
      recordDate,
      effectiveDate,
      securityCode: bodySecurityCode,
      securityName: bodySecurityName,
    } = req.body;

    const r1 = Number(ratio1);
    const r2 = Number(ratio2);

    if (!isin || !recordDate || !Number.isFinite(r1) || !Number.isFinite(r2) || r1 <= 0) {
      return res.status(400).json({
        success: false,
        message: "Invalid input values (ISIN, ratio and record date are required)",
      });
    }

    /*
     * Record Date drives every holdings calculation (who is eligible, when the
     * bonus shares land in FIFO, the duplicate check). Effective Date is stored
     * in the existing ExDate column for reference only, so bonuses saved before
     * Record_Date existed still read correctly (their ExDate doubles as their
     * record date). Effective Date is optional; when it is left empty the
     * record date is stored.
     */
    const recordDateISO = toISODateOnly(recordDate);
    const effectiveDateISO = effectiveDate ? toISODateOnly(effectiveDate) : recordDateISO;
    if (!recordDateISO || !effectiveDateISO) {
      return res.status(400).json({ success: false, message: "Invalid record date or effective date" });
    }
    if (effectiveDateISO < recordDateISO) {
      return res.status(400).json({
        success: false,
        message: "Effective date cannot be before the record date",
      });
    }

    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();
    const jobScheduling = catalystApp.jobScheduling();

    // Stage 2 of the custodian reconciliation: only accounts sent as Receivable
    // can be confirmed, and their Received Dates are taken from the Benefit
    // Collection file before the job starts. The job marks them Received once
    // the bonus is applied (functions/UpdateBonusTable).
    const bonusEvent = { isin, recordDateISO, r1, r2 };
    const receivable = (await readBonusAccountRows(zcql, bonusEvent)).filter(
      (r) => r.status === BONUS_STATUS.RECEIVABLE,
    );
    if (receivable.length === 0) {
      const applied = await isBonusApplied(zcql, bonusEvent);
      if (applied || !bypassCustodian) {
        return res.status(409).json({
          success: false,
          code: applied ? "ALREADY_APPLIED" : "NOT_RECEIVABLE",
          message: applied
            ? "This bonus has already been applied."
            : `This bonus (${isin}, Record Date ${recordDateISO}) has not been sent as Receivable yet. ` +
              "Compare with the Ex-Entitlement file and send it as receivable first.",
        });
      }
    }
    if (bypassCustodian) {
      // Temporary bypass: no custodian files, so no Received Dates are stored.
      // The bonus job still marks any Receivable accounts Received once applied.
      console.log(
        `[BonusController] Custodian check bypassed: ${isin}, record date ${recordDateISO}, ` +
          `ratio ${r1}:${r2}, ${receivable.length} Receivable account(s) to mark Received.`,
      );
    } else {
      const receipt = await recordBonusReceivedDates(catalystApp, zcql, { isin, recordDateISO, receivable });
      if (receipt.error) {
        return res.status(409).json({ success: false, code: receipt.code, message: receipt.error });
      }
    }

    const jobName = `BON_${isin.slice(-6)}_${recordDateISO}`;

    const existing = await zcql.executeZCQLQuery(
      `SELECT ROWID, status, CREATEDTIME FROM Jobs WHERE jobName = '${jobName}' LIMIT 1`
    );

    if (existing.length) {
      const oldStatus = existing[0].Jobs.status;
      const oldRowId = existing[0].Jobs.ROWID;
      const createdTime = existing[0].Jobs.CREATEDTIME;

      const STALE_TIMEOUT_MS = 60 * 60 * 1000;
      const jobAge = Date.now() - parseCatalystTime(createdTime);
      const isStale = jobAge > STALE_TIMEOUT_MS;

      if ((oldStatus === "PENDING" || oldStatus === "RUNNING") && !isStale) {
        return res.json({
          success: true,
          jobName,
          status: oldStatus,
          alreadyRunning: true,
          message: "Bonus application is already in progress",
        });
      }

      try {
        await zcql.executeZCQLQuery(`DELETE FROM Jobs WHERE ROWID = ${oldRowId}`);
      } catch (delErr) {
        console.error("Error deleting old bonus job:", delErr);
      }
    }

    // Progress tracking for the notification bell — a parent `Jobs` row + one
    // "_root" placeholder piece that UpdateBonusTable hands off to the rebuild
    // jobs it queues, so the parent only settles once the holdings rebuild has
    // actually finished (not just been queued). See util/caJobProgress.js.
    const caTrackKey = `BONA${Date.now()}`.slice(0, 20);
    const caSubId = `${caTrackKey}_root`;
    try {
      await zcql.executeZCQLQuery(
        `INSERT INTO Jobs (jobName, status) VALUES ('${escSql(caTrackKey)}', 'RUNNING')`,
      );
    } catch (e) {
      console.warn("[BonusController] parent track row insert failed:", e.message);
    }
    await insertCaTrackPiece(zcql, caTrackKey, caSubId);

    await jobScheduling.JOB.submitJob({
      job_name: "ApplyBonus",
      jobpool_name: "CorporateActions",
      target_name: "UpdateBonusTable",
      target_type: "Function",
      /* Catalyst Job Pool: retries only run when execution fails. Min interval 1m. */
      job_config: {
        number_of_retries: 5,
        retry_interval: 60 * 1000,
      },
      params: {
        isin,
        ratio1: String(r1),
        ratio2: String(r2),
        recordDate: recordDateISO,
        exDate: effectiveDateISO,
        secCode: bodySecurityCode || "",
        secName: bodySecurityName || "",
        jobName,
        caTrackKey,
        caSubId,
      },
    });

    return res.json({
      success: true,
      jobName,
      catalystJobName: caTrackKey,
      status: "PENDING",
      message: "Bonus application job started",
    });
  } catch (error) {
    console.error("Apply bonus error:", error);
    return res.status(500).json({
      success: false,
      message: error.message,
    });
  }
};

/* ======================================================
   GET BONUS APPLY JOB STATUS
   ====================================================== */
export const getBonusApplyStatus = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();

    const { jobName } = req.query;
    if (!jobName) {
      return res.status(400).json({ success: false, message: "jobName is required" });
    }

    const result = await zcql.executeZCQLQuery(
      `SELECT ROWID, status, CREATEDTIME FROM Jobs WHERE jobName = '${jobName}' LIMIT 1`
    );

    if (!result.length) {
      return res.json({ success: true, status: "NOT_STARTED" });
    }

    let status = result[0].Jobs.status;
    const createdTime = result[0].Jobs.CREATEDTIME;

    const STALE_TIMEOUT_MS = 60 * 60 * 1000;
    const jobAge = Date.now() - parseCatalystTime(createdTime);

    if ((status === "PENDING" || status === "RUNNING") && jobAge > STALE_TIMEOUT_MS) {
      try {
        await zcql.executeZCQLQuery(
          `UPDATE Jobs SET status = 'ERROR' WHERE jobName = '${jobName}'`
        );
      } catch (updateErr) {
        console.error("Failed to mark stale bonus job as ERROR:", updateErr);
      }
      status = "ERROR";
    }

    return res.json({ success: true, jobName, status });
  } catch (error) {
    console.error("Error fetching bonus job status:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to fetch bonus job status",
    });
  }
};

/**
 * GET /bonus/apply-progress?key=BONA...
 * Overall state of one "Confirm & Apply Bonus" run — stays RUNNING until the
 * background holdings rebuild it triggers has actually finished, not just been
 * queued (which is all GET /bonus/apply-status reflects).
 *
 *   RUNNING | COMPLETED | COMPLETED_WITH_ERRORS | ERROR | NOT_STARTED
 */
export const getBonusApplyProgress = async (req, res) => {
  try {
    if (!req.catalystApp) {
      return res.status(500).json({ success: false, message: "Catalyst app not initialized" });
    }
    const key = String(req.query.key ?? "").trim();
    if (!key) {
      return res.status(400).json({ success: false, message: "Query parameter key is required" });
    }

    const status = await readCaJobProgress(req.catalystApp.zcql(), key);
    return res.status(200).json({ success: true, key, status });
  } catch (error) {
    console.error("[getBonusApplyProgress]", error);
    return res.status(500).json({
      success: false,
      message: error?.message || "Failed to fetch bonus job progress",
    });
  }
};
