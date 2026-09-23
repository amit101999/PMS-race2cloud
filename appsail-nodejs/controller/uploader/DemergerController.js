/**
 * Demerger controller — lot-level (line-by-line) implementation.
 *
 * Source of truth for surviving lots is the materialized `Holdings` table
 * (normally maintained by `functions/CalculateHoldingPerAccount`). We walk Holdings rows
 * for each (account, oldIsin) pair to derive each surviving FIFO lot, then
 * compute per-lot demerger math:
 *
 *   For each surviving lot:
 *     oldRetainedQty   = lot.qty                                  (unchanged)
 *     oldRetainedCost  = lot.qty * lot.price * (1 - pct/100)
 *     oldRetainedWAP   = oldRetainedCost / lot.qty
 *     newQty           = floor(lot.qty * r1 / r2)
 *     newCost          = lot.qty * lot.price * (pct/100)
 *     newWAP           = newCost / newQty
 *
 * Apply step writes:
 *   1 row in `Demerger`        (header — single row per demerger event)
 *   1 row in `Security_List`   (idempotent — new ISIN registered)
 *   N×2 rows in `Demerger_Record` (per surviving lot: old-side + new-side)
 *
 * Each Demerger_Record row carries `Source_Tran_ROWID` linking back to the
 * source Holdings ROWID — enables traceability + lossless re-computation.
 *
 * APPLY queues background job `DemergerFn`: inserts Demerger / Security_List /
 * Demerger_Record then queues `RebuildHoldingtable` for affected accounts so
 * Holdings is refreshed (`functions/RebuildHoldingtable/`).
 */

import { readCaJobProgress, insertCaTrackPiece } from "../../util/caJobProgress.js";

const ZCQL_ROW_LIMIT = 270;

const escSql = (s) => String(s ?? "").replace(/'/g, "''");

const round = (n, d = 2) => {
  const f = Math.pow(10, d);
  return Math.round(Number(n || 0) * f) / f;
};

const isBuyType = (t) => /^BY-|SQB|OPI/i.test(String(t || ""));
const isSellType = (t) => /^SL\+|SQS|OPO|NF-/i.test(String(t || ""));
const upperEq = (a, b) => String(a || "").trim().toUpperCase() === b;

/* ====================================================================
   FETCH (BULK, keyset): all Holdings rows for the old ISIN ≤ recordDate,
   grouped by account.
   --------------------------------------------------------------------
   Why keyset (WHERE ROWID > cursor) instead of LIMIT/OFFSET:
   `Holdings` is the large materialized FIFO timeline. With OFFSET each
   page re-scans + re-sorts the whole ISIN match (≈ O(n²)), which made a
   widely-held ISIN (e.g. HUL) overrun the AppSail request budget → 408.
   Keyset walks the ROWID primary-key index forward exactly once (no
   re-skip, no per-page sort), so cost is ~O(n). Rows come back in global
   ROWID order, so each account's slice is re-sorted in memory afterwards
   into the `CREATEDTIME ASC, ROWID ASC` order the lot replay expects.
   ==================================================================== */
/* ===== TRADE_DATE_MIGRATION =====
   Record-date eligibility used to be a single hard `SETTLEMENT_DATE <= recordDate`
   filter, correct while every buy type was settlement-dated. OPI now stays on
   SETTLEMENT_DATE while BY-/SQB move to TRANSACTION_DATE, and ZCQL has no CASE,
   so the SQL widens to an OR superset and this predicate applies the precise cut.
   MERGER / DEMERGER rows are the second exception: the rebuild emits one row per
   surviving lot and preserves each lot's ORIGINAL trade date in TRANSACTION_DATE,
   stamping the event's record date on CA_DATE. Dating them by TRANSACTION_DATE
   scatters one event across the timeline at dates years before it happened, so a
   demerger over an ISIN a merger already touched would judge those lots eligible
   by their pre-merger trade date. Fall back to the lot date only for legacy rows
   written before CA_DATE was stamped.
   NOTE: this filter decides which lots are ELIGIBLE for the demerger, so it moves
   entitlement quantities — not just dates. Mirrors `holdingsEffectiveDate` in
   util/analytics/holdingsFromTable.js; keep in sync with the twin in
   functions/DemergerFn/demergerApplyCore.js. */
const holdingsEligibilityDate = (row) => {
  const setD = String(row.SETTLEMENT_DATE || "").slice(0, 10);
  const txD = String(row.TRANSACTION_DATE || "").slice(0, 10);
  const typ = String(row.TYPE || "").trim().toUpperCase();
  if (typ === "MERGER" || typ === "DEMERGER") {
    const caD = String(row.CA_DATE || "").slice(0, 10);
    if (caD) return caD;
  }
  return /^OPI/i.test(typ) ? setD || txD : txD || setD;
};

async function fetchHoldingRowsByAccountForIsin(zcql, isin, recordDateISO) {
  const byAccount = new Map();
  const cut = String(recordDateISO || "").slice(0, 10);
  let lastRowId = "0";
  while (true) {
    const batch = await zcql.executeZCQLQuery(`
      SELECT ROWID, WS_Account_code, TRANSACTION_DATE, SETTLEMENT_DATE, TYPE, ISIN,
             QUANTITY, PRICE, TOTAL_AMOUNT, HOLDING, WAP, HOLDING_VALUE, STATUS,
             CA_DATE, CREATEDTIME
      FROM Holdings
      WHERE ISIN = '${escSql(isin)}'
        AND (TRANSACTION_DATE <= '${escSql(recordDateISO)}' OR SETTLEMENT_DATE <= '${escSql(recordDateISO)}')
        AND ROWID > ${lastRowId}
      ORDER BY ROWID ASC
      LIMIT ${ZCQL_ROW_LIMIT}
    `);
    if (!batch || batch.length === 0) break;
    for (const r of batch) {
      const h = r.Holdings || r;
      const code = String(h.WS_Account_code ?? "").trim();
      if (!code) continue;
      if (cut && holdingsEligibilityDate(h) > cut) continue;
      let list = byAccount.get(code);
      if (!list) {
        list = [];
        byAccount.set(code, list);
      }
      list.push(h);
    }
    // Advance the keyset cursor to the largest ROWID in this page (rows are
    // ROWID-ascending, so it's the last one). Guard against a non-numeric or
    // non-advancing cursor to avoid an infinite loop.
    const lastRow = batch[batch.length - 1].Holdings || batch[batch.length - 1];
    const nextCursor = String(lastRow.ROWID ?? "").trim();
    if (!/^\d+$/.test(nextCursor) || nextCursor === lastRowId) break;
    lastRowId = nextCursor;
    if (batch.length < ZCQL_ROW_LIMIT) break;
  }

  // Per-account FIFO ordering (mirrors the SQL `CREATEDTIME ASC, ROWID ASC`).
  // ROWIDs are arbitrary-length positive integers — compare by length first,
  // then lexically, so we don't lose precision past Number.MAX_SAFE_INTEGER.
  for (const rows of byAccount.values()) {
    rows.sort((a, b) => {
      const ta = parseCatalystTime(a.CREATEDTIME);
      const tb = parseCatalystTime(b.CREATEDTIME);
      if (ta !== tb) return ta - tb;
      const ra = String(a.ROWID ?? "0");
      const rb = String(b.ROWID ?? "0");
      if (ra.length !== rb.length) return ra.length - rb.length;
      return ra < rb ? -1 : ra > rb ? 1 : 0;
    });
  }

  return byAccount;
}

/* ====================================================================
   MINI-FIFO over Holdings rows → surviving lots
   --------------------------------------------------------------------
   Each Holdings row represents one FIFO event already materialized.
   We walk rows in **`CREATEDTIME` / fifo materialisation order** (same as
   `HOLDINGS_FIFO_ORDER_BY_SQL`) to derive each surviving lot's surviving qty
   (Holdings rows alone do not carry surviving-qty after partial sells).
   ==================================================================== */
function computeSurvivingLotsFromHoldingRows(rows) {
  let queue = []; // { qty, price, buyDate, sourceRowId, sourceType }
  let lastReplaceEventKey = null;

  for (const row of rows) {
    const type = String(row.TYPE || "").trim();
    const qty = Number(row.QUANTITY || 0);
    const price = Number(row.PRICE || 0);
    const buyDate = row.TRANSACTION_DATE || row.SETTLEMENT_DATE || null;
    const sourceRowId = String(row.ROWID || "");

    if (isBuyType(type)) {
      if (qty > 0) {
        queue.push({ qty, price, buyDate, sourceRowId, sourceType: type });
      }
      continue;
    }

    if (upperEq(type, "BONUS")) {
      if (qty > 0) {
        queue.push({ qty, price: 0, buyDate, sourceRowId, sourceType: "BONUS" });
      }
      continue;
    }

    if (isSellType(type)) {
      let remaining = qty;
      while (remaining > 0 && queue.length) {
        const lot = queue[0];
        const used = Math.min(lot.qty, remaining);
        lot.qty -= used;
        remaining -= used;
        if (lot.qty <= 0) queue.shift();
      }
      continue;
    }

    if (upperEq(type, "SPLIT") || upperEq(type, "MERGER") || upperEq(type, "DEMERGER")) {
      // SPLIT / MERGER / DEMERGER each produce one Holdings row per resulting lot.
      // The first row of a new event wipes the prior queue; subsequent rows of the
      // SAME event are added additively (one per resulting lot).
      // Key on the event date, not buyDate: MERGER / DEMERGER rows keep each
      // lot's ORIGINAL trade date, so keying on it made every lot look like a
      // new event and only the last lot survived. holdingsEligibilityDate gives
      // CA_DATE for those types (trade date for SPLIT and legacy rows), matching
      // recomputeHoldingsSnapshot in util/analytics/holdingsFromTable.js.
      const eventKey = `${holdingsEligibilityDate(row)}|${type}`;
      if (lastReplaceEventKey !== eventKey) {
        queue = [];
        lastReplaceEventKey = eventKey;
      }
      // Same rule as `recomputeHoldingsSnapshot`: a merger writes one row per
      // lot RECEIVED on the surviving ISIN, and one row on the merged-away ISIN
      // for the quantity that LEFT the account. Both are TYPE='MERGER' with a
      // positive QUANTITY; only the stored HOLDING tells them apart (0 on the
      // closing row). Without this, a demerger run against an ISIN that has
      // already been merged away would seed the lot queue with a position that
      // no longer exists. DEMERGER / SPLIT rows keep a non-zero HOLDING.
      if ((Number(row.HOLDING) || 0) <= 0) continue;
      if (qty > 0) {
        queue.push({ qty, price, buyDate, sourceRowId, sourceType: type });
      }
      continue;
    }
    // Other types (e.g. DIV+) have no holding effect — ignore.
  }

  return queue.filter((lot) => lot.qty > 0);
}

/* ====================================================================
   LOT-LEVEL DEMERGER MATH for one account
   ==================================================================== */
function buildLotLevelDemergerForAccount({
  accountCode,
  lots,
  oldIsin,
  newIsin,
  r1,
  r2,
  pct,
}) {
  const out = [];
  for (const lot of lots) {
    const Q1 = Number(lot.qty) || 0;
    if (Q1 <= 0) continue;

    const lotPrice = Number(lot.price) || 0;
    const lotCost = Q1 * lotPrice;

    const Q2 = Math.floor((Q1 * r1) / r2);
    const newCost = lotCost * (pct / 100);
    const oldCost = lotCost - newCost;

    const oldWapAfter = Q1 > 0 ? oldCost / Q1 : 0;
    const newWap = Q2 > 0 ? newCost / Q2 : 0;

    out.push({
      accountCode,
      oldIsin,
      sourceLotRowId: lot.sourceRowId,
      sourceLotDate: lot.buyDate,
      sourceLotType: lot.sourceType,
      oldQty: Q1,
      oldWapBefore: round(lotPrice, 4),
      oldWapAfter: round(oldWapAfter, 4),
      oldCostAfter: round(oldCost, 2),
      newIsin,
      newQty: Q2,
      newWap: round(newWap, 4),
      newCost: round(newCost, 2),
    });
  }
  return out;
}

/* ====================================================================
   INPUT VALIDATION (shared by preview + apply)
   ==================================================================== */
function validateDemergerInputs(body, requireApplyFields) {
  const {
    oldIsin,
    newIsin,
    ratio1,
    ratio2,
    effectiveDate,
    recordDate,
    allocationToNewPercent,
  } = body;

  const r1 = Number(ratio1);
  const r2 = Number(ratio2);
  const pct = Number(allocationToNewPercent);

  if (
    !oldIsin ||
    !newIsin ||
    oldIsin === newIsin ||
    !effectiveDate ||
    !recordDate ||
    !Number.isFinite(r1) || r1 <= 0 ||
    !Number.isFinite(r2) || r2 <= 0 ||
    !Number.isFinite(pct) || pct < 0 || pct > 100
  ) {
    return { error: "Invalid input: check ISINs, ratio, dates and allocation %" };
  }

  if (requireApplyFields) {
    const { oldSecurityCode, oldSecurityName, newSecurityCode, newSecurityName } = body;
    if (!oldSecurityCode || !oldSecurityName || !newSecurityCode || !newSecurityName) {
      return { error: "Missing required fields (security code/name)" };
    }
  }

  return {
    r1,
    r2,
    pct,
    effectiveDateISO: new Date(effectiveDate).toISOString().split("T")[0],
    recordDateISO: new Date(recordDate).toISOString().split("T")[0],
  };
}

/* ====================================================================
   PREVIEW (lot-level)
   ==================================================================== */
export const previewDemerger = async (req, res) => {
  try {
    if (!req.catalystApp) {
      return res.status(500).json({
        success: false,
        message: "Catalyst app not initialized",
      });
    }

    const v = validateDemergerInputs(req.body, false);
    if (v.error) return res.status(400).json({ success: false, message: v.error });

    const { oldIsin, newIsin } = req.body;
    const { r1, r2, pct, recordDateISO } = v;
    const zcql = req.catalystApp.zcql();

    // Single bulk read of all Holdings rows for the old ISIN, grouped by
    // account in memory — avoids one sequential query per holding account,
    // which previously timed out the request (→ "Failed to fetch") for
    // widely-held ISINs.
    const rowsByAccount = await fetchHoldingRowsByAccountForIsin(
      zcql,
      oldIsin,
      recordDateISO,
    );
    if (!rowsByAccount.size) return res.json({ success: true, data: [] });

    const preview = [];
    let accountsWithSurvivingLots = 0;
    let totalLots = 0;

    for (const [accountCode, rows] of rowsByAccount) {
      if (!rows.length) continue;

      const lots = computeSurvivingLotsFromHoldingRows(rows);
      if (!lots.length) continue;

      const accountPreview = buildLotLevelDemergerForAccount({
        accountCode,
        lots,
        oldIsin,
        newIsin,
        r1,
        r2,
        pct,
      });

      if (accountPreview.length) {
        accountsWithSurvivingLots++;
        totalLots += accountPreview.length;
        preview.push(...accountPreview);
      }
    }

    return res.json({
      success: true,
      data: preview,
      summary: {
        accounts: accountsWithSurvivingLots,
        lots: totalLots,
      },
    });
  } catch (error) {
    console.error("Preview demerger error:", error);
    return res.status(500).json({ success: false, message: error.message });
  }
};

const DEMERGER_STALE_TIMEOUT_MS = 60 * 60 * 1000;

const parseCatalystTime = (ct) => {
  if (!ct) return 0;
  const fixed = String(ct).replace(/:(\d{3})$/, ".$1");
  const ms = new Date(fixed).getTime();
  return Number.isNaN(ms) ? 0 : ms;
};

/** Deterministic jobName under Catalyst ~50-char limit (matches merger pattern). */
function buildDemergerJobName(oldIsin, newIsin, recordDateIso) {
  const trim = (s, n) => String(s || "").replace(/[^A-Za-z0-9]/g, "").slice(-n);
  const date = String(recordDateIso || "").replace(/[^\d]/g, "");
  return `DMG_${trim(oldIsin, 6)}_${trim(newIsin, 6)}_${date}`;
}

/* ====================================================================
   APPLY → queue Catalyst job `DemergerFn`
   ==================================================================== */
export const applyDemerger = async (req, res) => {
  try {
    if (!req.catalystApp) {
      return res.status(500).json({
        success: false,
        message: "Catalyst app not initialized",
      });
    }

    const v = validateDemergerInputs(req.body, true);
    if (v.error) return res.status(400).json({ success: false, message: v.error });

    const {
      oldIsin,
      oldSecurityCode,
      oldSecurityName,
      newIsin,
      newSecurityCode,
      newSecurityName,
    } = req.body;
    const { r1, r2, pct, effectiveDateISO, recordDateISO } = v;

    const zcql = req.catalystApp.zcql();
    const jobScheduling = req.catalystApp.jobScheduling();

    const jobName = buildDemergerJobName(oldIsin, newIsin, recordDateISO);

    const existing = await zcql.executeZCQLQuery(`
      SELECT ROWID, status, CREATEDTIME FROM Jobs WHERE jobName = '${escSql(jobName)}' LIMIT 1
    `);

    if (existing?.length) {
      const oldStatus = existing[0].Jobs.status;
      const oldRowId = existing[0].Jobs.ROWID;
      const createdTime = existing[0].Jobs.CREATEDTIME;
      const jobAge = Date.now() - parseCatalystTime(createdTime);
      const isStale = jobAge > DEMERGER_STALE_TIMEOUT_MS;

      if (
        (oldStatus === "PENDING" || oldStatus === "RUNNING") &&
        !isStale
      ) {
        return res.json({
          success: true,
          jobName,
          status: oldStatus,
          alreadyRunning: true,
          message: "Demerger application is already in progress",
        });
      }

      try {
        await zcql.executeZCQLQuery(`DELETE FROM Jobs WHERE ROWID = ${oldRowId}`);
      } catch (delErr) {
        console.error("[applyDemerger] Error deleting old job row:", delErr);
      }
    }

    // Progress tracking for the notification bell — a parent `Jobs` row + one
    // "_root" placeholder piece that DemergerFn hands off to the rebuild jobs it
    // queues, so the parent only settles once the holdings rebuild has actually
    // finished (not just been queued). See util/caJobProgress.js.
    const caTrackKey = `DMGT${Date.now()}`.slice(0, 20);
    const caSubId = `${caTrackKey}_root`;
    try {
      await zcql.executeZCQLQuery(
        `INSERT INTO Jobs (jobName, status) VALUES ('${escSql(caTrackKey)}', 'RUNNING')`,
      );
    } catch (e) {
      console.warn("[applyDemerger] parent track row insert failed:", e.message);
    }
    await insertCaTrackPiece(zcql, caTrackKey, caSubId);

    await jobScheduling.JOB.submitJob({
      job_name: "ApplyDemerger",
      jobpool_name: "CorporateActions",
      target_name: "DemergerFn",
      target_type: "Function",
      /* Catalyst Job Pool: retries only run when execution fails. Min interval 1m. */
      job_config: {
        number_of_retries: 5,
        retry_interval: 60 * 1000,
      },
      params: {
        jobName,
        oldIsin,
        oldSecurityCode,
        oldSecurityName,
        newIsin,
        newSecurityCode,
        newSecurityName,
        ratio1: String(r1),
        ratio2: String(r2),
        allocationPct: String(pct),
        effectiveDateIso: effectiveDateISO,
        recordDateIso: recordDateISO,
        caTrackKey,
        caSubId,
      },
    });

    return res.json({
      success: true,
      jobName,
      catalystJobName: caTrackKey,
      status: "PENDING",
      message: "Demerger application job started",
    });
  } catch (error) {
    console.error("Error in applyDemerger:", error);
    return res.status(500).json({
      success: false,
      message: error.message || "Failed to queue demerger apply",
    });
  }
};

export const getDemergerApplyStatus = async (req, res) => {
  try {
    if (!req.catalystApp) {
      return res.status(500).json({
        success: false,
        message: "Catalyst app not initialized",
      });
    }
    const zcql = req.catalystApp.zcql();

    const { jobName } = req.query;
    if (!jobName) {
      return res.status(400).json({
        success: false,
        message: "jobName is required",
      });
    }

    const result = await zcql.executeZCQLQuery(
      `SELECT ROWID, status, CREATEDTIME FROM Jobs WHERE jobName = '${escSql(String(jobName))}' LIMIT 1`,
    );

    if (!result?.length) {
      return res.json({ success: true, status: "NOT_STARTED" });
    }

    let status = result[0].Jobs.status;
    const createdTime = result[0].Jobs.CREATEDTIME;
    const jobAge = Date.now() - parseCatalystTime(createdTime);

    if (
      (status === "PENDING" || status === "RUNNING") &&
      jobAge > DEMERGER_STALE_TIMEOUT_MS
    ) {
      try {
        await zcql.executeZCQLQuery(
          `UPDATE Jobs SET status = 'ERROR' WHERE jobName = '${escSql(String(jobName))}'`,
        );
        status = "ERROR";
      } catch (updateErr) {
        console.error("[getDemergerApplyStatus] Stale-mark failed:", updateErr);
      }
    }

    return res.json({ success: true, status });
  } catch (error) {
    console.error("[getDemergerApplyStatus]", error);
    return res.status(500).json({
      success: false,
      message: error.message || "Status check failed",
    });
  }
};

/**
 * GET /demerger/apply-progress?key=DMGT...
 * Overall state of one "Apply Demerger" run — stays RUNNING until the background
 * holdings rebuild it triggers has actually finished, not just been queued
 * (which is all GET /demerger/apply-status reflects).
 *
 *   RUNNING | COMPLETED | COMPLETED_WITH_ERRORS | ERROR | NOT_STARTED
 */
export const getDemergerApplyProgress = async (req, res) => {
  try {
    if (!req.catalystApp) {
      return res
        .status(500)
        .json({ success: false, message: "Catalyst app not initialized" });
    }
    const key = String(req.query.key ?? "").trim();
    if (!key) {
      return res
        .status(400)
        .json({ success: false, message: "Query parameter key is required" });
    }

    const status = await readCaJobProgress(req.catalystApp.zcql(), key);
    return res.status(200).json({ success: true, key, status });
  } catch (error) {
    console.error("[getDemergerApplyProgress]", error);
    return res.status(500).json({
      success: false,
      message: error?.message || "Failed to fetch demerger job progress",
    });
  }
};
