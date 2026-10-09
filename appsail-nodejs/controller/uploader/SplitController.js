import { buildVirtualToActualMap } from "../../util/mapVirtualToActualCodes.js";
// Shared FIFO re-derivation only — the effective-date rule below stays local so
// this module's preview cannot be silently changed by another module's copy.
import { recomputeHoldingsSnapshot } from "../../util/analytics/holdingsFromTable.js";
import { readCaJobProgress, insertCaTrackPiece } from "../../util/caJobProgress.js";
import {
  computeSplitPreviewRows,
  readSplitAccountRows,
  splitStage,
  SPLIT_STATUS,
} from "../../util/analytics/splitPreviewCalc.js";
import { readLatestCorpActionRows, readLatestReceiptRows } from "../corporateAction/CustodianUploadController.js";

const HOLDINGS_BATCH = 250;
const round4 = (n) => Math.round(n * 10000) / 10000;

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

export const previewStockSplit = async (req, res) => {
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

    // Where this split is in the custodian reconciliation, and each account's status.
    const splitAccountRows = await readSplitAccountRows(zcql, { isin, recordDateISO, r1, r2 });
    const stage = splitStage(splitAccountRows, await isSplitApplied(zcql, { isin, recordDateISO, r1, r2 }));
    const statusByAccount = new Map(splitAccountRows.map((r) => [r.accountCode, r]));

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
        const eff = splitEffectiveDate(h);
        if (eff && eff >= cutoff) continue;
        // This split's own SPLIT row (dated on the record date) is its result,
        // not the holding on the record date. Same rule as computeSplitPreviewRows.
        if (String(h.TYPE || "").trim().toUpperCase() === "SPLIT" && eff === recordDateISO) continue;
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
       STEP 2: CALCULATE SPLIT PREVIEW (simple arithmetic)
       Holdings table already has HOLDING, WAP, HOLDING_VALUE
       computed by FIFO. No need to re-run FIFO engine.

       Split logic:
         newHolding      = currentHolding × ratio2 / ratio1 (fractions kept, 4 dp)
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
      if (currentHolding <= 0) continue; // fully sold by the record date

      const holdingValue = Number(h.HOLDING_VALUE) || 0;
      const currentWAP   = Number(h.WAP) || 0;

      // Fractions are kept (3 shares on 2:3 = 4.5), same as the holdings rebuild.
      const newHolding = round4(currentHolding * splitMultiplier);
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
        delta: round4(newHolding - currentHolding),
        status: statusByAccount.get(accountCode)?.status || "",
        receivedDate: statusByAccount.get(accountCode)?.receivedDate || "",
      });
    }

    // An applied split can gain an account later (a back-dated trade uploaded
    // after Confirm). Such an account has no Split_Account row, so reopen stage 1
    // for it: Send as Receivable then adds only the missing accounts. Splits
    // applied before Split_Account existed have no rows at all and stay RECEIVED.
    const hasMissingAccount = preview.some((row) => !statusByAccount.has(row.accountCode));
    const pageStage =
      stage === "RECEIVED" && splitAccountRows.length > 0 && hasMissingAccount ? "NEW" : stage;

    return res.json({ success: true, data: preview, stage: pageStage });
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

/* ======================================================
   CUSTODIAN RECONCILIATION (Receivable → Received)
   Stage 1: compare with the Ex-Entitlement file, then "Send as Receivable"
            inserts one Split_Account row per account with Status RECEIVABLE.
   Stage 2: compare with the Corporate Benefit Collection file, then
            "Confirm & Apply" applies the split and marks the accounts
            RECEIVED (with the file's Received Date).
   Only this controller writes Status / Received_Date. The holdings rebuild
   updates the holding columns in place and leaves RECEIVABLE rows alone
   (functions/RebuildHoldingtable). Whether a split is applied is read from
   the Split table, never from the statuses.
   ====================================================== */

const STATUS_UPDATE_CHUNK = 200;

const NOT_RECEIVABLE_MESSAGE = (isin, recordDateISO) =>
  `Record Date mismatch: ${isin} with Record Date ${recordDateISO} has not been compared with the ` +
  "Ex-Entitlement file yet. Compare with the entitlement file and send it as receivable first, then do this step.";

/** Is this split (ISIN + record date + ratio) already in the Split table? Old rows have no Record_Date. */
const isSplitApplied = async (zcql, { isin, recordDateISO, r1, r2 }) => {
  const rows = await zcql.executeZCQLQuery(`
    SELECT ROWID, Record_Date, Issue_Date FROM Split
    WHERE ISIN = '${escSql(isin)}'
      AND Ratio1 = ${Number(r1)}
      AND Ratio2 = ${Number(r2)}
  `);
  return (rows || []).some((row) => {
    const s = row.Split || row;
    return String(s.Record_Date || s.Issue_Date || "").slice(0, 10) === recordDateISO;
  });
};

/** Run `UPDATE Split_Account SET <assignments>` for the given rows, in chunks. */
const updateSplitAccountRows = async (zcql, rowIds, assignments) => {
  for (let i = 0; i < rowIds.length; i += STATUS_UPDATE_CHUNK) {
    const chunk = rowIds.slice(i, i + STATUS_UPDATE_CHUNK);
    await zcql.executeZCQLQuery(`
      UPDATE Split_Account SET ${assignments}
      WHERE ROWID IN (${chunk.join(", ")})
    `);
  }
};

/**
 * Work out the Received Date of every Receivable account of this split from
 * the Corporate Benefit Collection file (matched on its actual / UCC code).
 * Accounts the file does not list are still marked Received, with no date.
 * Nothing is written here, so a bad file stops Confirm before any change.
 *
 * @returns {Promise<{ rowIdsByDate: Map<string, string[]> } | { code: string, error: string }>}
 */
const planSplitReceipt = async (app, zcql, { isin, recordDateISO, receivable }) => {
  let allocationRows;
  try {
    ({ rows: allocationRows } = await readLatestReceiptRows(app, { caType: "split", isin, recordDateISO }));
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
  const rowIdsByDate = new Map(); // received date ("" = not in the file) → ROWIDs
  for (const r of receivable) {
    const actual = String(virtualToActual.get(r.accountCode) || r.accountCode).toUpperCase();
    const date = receivedDateByActual.get(actual) || "";
    if (!rowIdsByDate.has(date)) rowIdsByDate.set(date, []);
    rowIdsByDate.get(date).push(r.rowId);
  }

  return { rowIdsByDate };
};

/** Mark the planned accounts Received. Safe to run again: it only sets values. */
const markSplitReceived = async (zcql, rowIdsByDate) => {
  for (const [date, rowIds] of rowIdsByDate) {
    const assignments = date
      ? `Status = '${SPLIT_STATUS.RECEIVED}', Received_Date = '${escSql(date)}'`
      : `Status = '${SPLIT_STATUS.RECEIVED}'`;
    await updateSplitAccountRows(zcql, rowIds, assignments);
  }
};

/**
 * POST /split/send-receivable
 * body: { isin, securityCode, securityName, ratio1, ratio2, recordDate, effectiveDate }
 *
 * Stage 1 → 2: store every account of the split preview in Split_Account with
 * Status RECEIVABLE. The rows are recomputed here rather than taken from the
 * browser, and an uploaded Ex-Entitlement file must have this split (ISIN + record date).
 *
 * For a split that is already applied, only accounts with no Split_Account row
 * are sent (an account that turned up later, e.g. a back-dated trade). Accounts
 * already Received are never touched.
 */
export const sendSplitReceivable = async (req, res) => {
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

    const alreadyApplied = await isSplitApplied(zcql, { isin, recordDateISO, r1, r2 });
    const existing = await readSplitAccountRows(zcql, { isin, recordDateISO, r1, r2 });
    if (existing.some((r) => r.status === SPLIT_STATUS.RECEIVABLE)) {
      return res.status(409).json({
        success: false,
        code: "ALREADY_SENT",
        message: "These accounts are already Receivable. Compare with the Corporate Benefit Collection file next.",
      });
    }
    const alreadyAppliedMessage = "This split has already been applied.";
    // Applied before Split_Account existed: no rows to tell which accounts are missing.
    if (alreadyApplied && existing.length === 0) {
      return res.status(409).json({ success: false, code: "ALREADY_SENT", message: alreadyAppliedMessage });
    }

    const previewRows = await computeSplitPreviewRows(zcql, { isin, r1, r2, recordDateISO });
    const listedAccounts = new Set(existing.map((r) => r.accountCode));
    const rowsToSend = previewRows.filter((row) => !listedAccounts.has(row.accountCode));
    if (rowsToSend.length === 0) {
      if (alreadyApplied) {
        return res.status(409).json({ success: false, code: "ALREADY_SENT", message: alreadyAppliedMessage });
      }
      return res.status(409).json({ success: false, code: "NO_HOLDINGS", message: "No eligible holdings on the record date." });
    }

    // Throws NO_CUSTODIAN_ROWS (caught below) when no uploaded file has this split.
    await readLatestCorpActionRows(app, { caType: "split", isin, recordDateISO });

    const table = app.datastore().table("Split_Account");
    const newRows = rowsToSend.map((row) => ({
      ISIN: isin,
      WS_Account_code: row.accountCode,
      Security_Code: securityCode || "",
      Security_Name: securityName || "",
      Ratio1: r1,
      Ratio2: r2,
      Record_Date: recordDateISO,
      Issue_Date: effectiveDateISO,
      Old_Holding: row.currentHolding,
      New_Holding: row.newHolding,
      Status: SPLIT_STATUS.RECEIVABLE,
    }));
    try {
      for (let i = 0; i < newRows.length; i += STATUS_UPDATE_CHUNK) {
        await table.insertRows(newRows.slice(i, i + STATUS_UPDATE_CHUNK));
      }
    } catch (insertErr) {
      // A half-sent split would show as Receivable with accounts missing.
      // No row of this event was Receivable before this call (checked above),
      // so the Receivable rows are exactly the ones inserted here — remove only
      // those (Received rows of an applied split stay) and let the user send again.
      try {
        await zcql.executeZCQLQuery(`
          DELETE FROM Split_Account
          WHERE ISIN = '${escSql(isin)}'
            AND Record_Date = '${recordDateISO}'
            AND Ratio1 = ${r1}
            AND Ratio2 = ${r2}
            AND Status = '${SPLIT_STATUS.RECEIVABLE}'
        `);
      } catch (cleanupErr) {
        console.error("[SplitController] Could not remove a half-sent receivable:", cleanupErr.message);
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
    console.error("[SplitController] sendSplitReceivable:", error);
    return res.status(500).json({ success: false, message: error.message || "Could not send the split as receivable." });
  }
};

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

    const { securityCode, securityName, ratio1, ratio2, recordDate, effectiveDate, isin } =
      req.body;

    if (!securityCode || !securityName || !ratio1 || !ratio2 || !recordDate || !isin) {
      return res.status(400).json({
        message: "Missing required fields (ISIN, ratio and record date are required)",
      });
    }

    /*
     * Record Date drives every holdings calculation (who is eligible, when the
     * split lands in FIFO, the duplicate check, the bonus recompute below).
     * Effective Date is stored in Issue_Date for reference only — Issue_Date is
     * kept as the column name so splits saved before Record_Date existed still
     * read correctly (their Issue_Date doubles as their record date).
     * Effective Date is optional; when it is left empty the record date is stored.
     */
    const recordDateISO = toISODateOnly(recordDate);
    const effectiveDateISO = effectiveDate ? toISODateOnly(effectiveDate) : recordDateISO;
    if (!recordDateISO || !effectiveDateISO) {
      return res.status(400).json({ message: "Invalid record date or effective date" });
    }
    if (effectiveDateISO < recordDateISO) {
      return res.status(400).json({ message: "Effective date cannot be before the record date" });
    }

    const isinEsc = escSql(isin);

    const splitEvent = { isin, recordDateISO, r1: Number(ratio1), r2: Number(ratio2) };

    // Idempotency guard (mirrors bonus's bonusRowExists): a split event is
    // uniquely identified by ISIN + record date + ratios. If it already exists,
    // skip the insert — re-applying would otherwise insert a duplicate Split
    // row and apply the split ratio twice. Older rows have no Record_Date, so
    // their Issue_Date stands in for it.
    const alreadyApplied = await isSplitApplied(zcql, splitEvent);
    const receivable = (await readSplitAccountRows(zcql, splitEvent)).filter(
      (r) => r.status === SPLIT_STATUS.RECEIVABLE,
    );
    // Applied and nothing left Receivable: fully done, skip the rebuild too.
    // (Applied with Receivable accounts left = an earlier Confirm stopped
    // half-way; it carries on below to finish the job, minus the insert.)
    if (alreadyApplied && receivable.length === 0) {
      console.log(
        `[SplitController] Split already applied for ${isin} with record date ${recordDateISO} ` +
          `(${ratio1}:${ratio2}) — idempotent skip, no rebuild.`,
      );
      return res.status(200).json({
        success: true,
        alreadyApplied: true,
        message:
          "Split already applied for this ISIN, record date and ratio — skipped (no duplicate inserted).",
      });
    }

    // Stage 2 of the custodian reconciliation: without stage 1 there is nothing
    // to confirm. The Benefit Collection file is checked before anything is
    // written, so a bad file leaves the split untouched.
    if (receivable.length === 0) {
      return res
        .status(409)
        .json({ success: false, code: "NOT_RECEIVABLE", message: NOT_RECEIVABLE_MESSAGE(isin, recordDateISO) });
    }
    const receipt = await planSplitReceipt(app, zcql, { isin, recordDateISO, receivable });
    if (receipt.error) {
      return res.status(409).json({ success: false, code: receipt.code, message: receipt.error });
    }

    // Order: Split row → rebuild/bonus jobs → statuses Received (at the end).
    // If anything fails before the end, the accounts stay Receivable, the page
    // still offers Confirm, and the next click skips this insert and sends the
    // jobs again (a rebuild is safe to repeat). Marking Received earlier would
    // make the next click treat the split as done and never send the rebuild.
    if (!alreadyApplied) {
      await zcql.executeZCQLQuery(`
      INSERT INTO Split
      (
        Security_Code,
        Security_Name,
        Ratio1,
        Ratio2,
        Record_Date,
        Issue_Date,
        ISIN
      )
      VALUES
      (
        '${escSql(securityCode)}',
        '${escSql(securityName)}',
        ${Number(ratio1)},
        ${Number(ratio2)},
        '${recordDateISO}',
        '${effectiveDateISO}',
        '${isinEsc}'
      )
    `);
    }

    // Accounts to rebuild = every account holding the ISIN on the record date,
    // read from the Holdings ledger (same source and rule as the bonus apply).
    // The Transaction table is not enough: a client whose shares all came from
    // a merger or demerger has no Transaction row for this ISIN and was skipped.
    // When the split was already applied, only the Receivable accounts still
    // need it (accounts that turned up later, or a Confirm that stopped half-way);
    // the Received ones already have it and are left alone.
    let affectedAccounts;
    if (alreadyApplied) {
      affectedAccounts = receivable.map((r) => r.accountCode);
    } else {
      const eligibleRows = await computeSplitPreviewRows(zcql, {
        isin,
        r1: splitEvent.r1,
        r2: splitEvent.r2,
        recordDateISO,
      });
      affectedAccounts = eligibleRows.map((row) => row.accountCode);
    }

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

    // A bonus already applied on/after this split's record date had its BonusShare
    // sized on the PRE-split holding — it is now stale. Re-run the bonus apply
    // in recompute mode (the Split row above is now in the DB, so the bonus
    // FIFO will re-size on the post-split holding and overwrite BonusShare).
    // The recompute job re-queues its own RebuildHoldingtable afterward.
    let bonusPiecesRegistered = 0;
    try {
      // Compared on each bonus's record date (ExDate for bonuses saved before
      // Record_Date existed), so the filter is applied here rather than in SQL.
      // Bonus_Record also holds one row per account (WS_Account_code set) for the
      // Bonus page's custodian reconciliation; only the event rows are bonuses.
      const bonusRecords = await zcql.executeZCQLQuery(`
        SELECT SecurityCode, SecurityName, ISIN, Ratio1, Ratio2, Record_Date, ExDate
        FROM Bonus_Record
        WHERE ISIN = '${isinEsc}' AND WS_Account_code IS NULL
        ORDER BY ExDate ASC
      `);

      const scheduling = app.jobScheduling();
      let bonusIdx = 0;
      for (const row of bonusRecords || []) {
        const b = row.Bonus_Record || row;
        const bonusEffectiveDateISO = String(b.ExDate || "").slice(0, 10);
        const bonusRecordDateISO = String(b.Record_Date || "").slice(0, 10) || bonusEffectiveDateISO;
        if (!bonusRecordDateISO || bonusRecordDateISO < recordDateISO) continue;

        // Unique tracking name so UpdateBonusTable's per-account SUCCESS guard
        // (scoped by jobName) does not skip a prior bonus run for this bonus.
        const bonusJobName = `BONRC_${isin.slice(-6)}_${bonusRecordDateISO}_${Date.now()}`;
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
            recordDate: bonusRecordDateISO,
            exDate: bonusEffectiveDateISO || bonusRecordDateISO,
            secCode: b.SecurityCode || securityCode || "",
            secName: b.SecurityName || securityName || "",
            recompute: "true",
            jobName: bonusJobName,
            caTrackKey,
            caSubId,
          },
        });
        console.log(
          `[SplitController] Queued bonus recompute for ${isin} record date ${bonusRecordDateISO}`,
        );
      }
    } catch (recomputeErr) {
      console.error(
        "[SplitController] Failed to queue bonus recompute:",
        recomputeErr.message,
      );
    }

    // Every job is sent — only now is the split done from the page's view.
    await markSplitReceived(zcql, receipt.rowIdsByDate);

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
