"use strict";

/**
 * Split impact handler for ExportCorpActionReport.
 *
 * Reports how every split in a date range affected every client. Source of
 * truth is the APPLIED data, read from two tables (same shape as the bonus report):
 *   1. `Split`         — the authoritative list of splits that happened (ratio +
 *                        metadata). Same table addStockSplit writes and the
 *                        "All Corporate Actions" export reads.
 *   2. `Split_Account` — one row per client per split, written by the holdings
 *                        rebuild (functions/RebuildHoldingtable/holdingsRebuildFromSources.js)
 *                        together with Holdings, so it always matches Holdings.
 *                        Record_Date = the split's Record_Date, or Issue_Date for
 *                        splits saved before Record_Date existed.
 *
 *   SHARES_ALLOCATED = Split_Account.New_Holding (holding just after the split,
 *                      fractions kept, e.g. 3 shares on 2:3 = 4.5)
 */

const { buildVirtualToActualMap } = require("../virtualToActual.js");

const MANIFEST_BATCH = 270;
const SPLIT_ACCOUNT_BATCH = 250;

const esc = (s) => String(s ?? "").replace(/'/g, "''");
const isIsoDate = (s) => /^\d{4}-\d{2}-\d{2}$/.test(String(s || "").trim());

function round4(n) {
  const x = Number(n);
  if (!Number.isFinite(x)) return 0;
  return Math.round(x * 10000) / 10000;
}

/**
 * Frozen work list: every split whose record date or effective date (Issue_Date)
 * falls in [fromDate, toDate] — the same range rule as Demerger.
 * One manifest entry == one split event.
 */
async function buildManifest(zcql, fromDate, toDate) {
  const from = esc(fromDate);
  const to = esc(toDate);
  const events = [];
  // ZCQL's OFFSET is 1-based, so each page repeats the previous page's last
  // row; skip it so the same row is not reported twice.
  const seenRowIds = new Set();
  let offset = 0;

  while (true) {
    const rows = await zcql.executeZCQLQuery(`
      SELECT ROWID, Security_Code, Security_Name, Ratio1, Ratio2, Record_Date, Issue_Date, ISIN
      FROM Split
      WHERE (Issue_Date >= '${from}' AND Issue_Date <= '${to}')
         OR (Record_Date >= '${from}' AND Record_Date <= '${to}')
      ORDER BY Issue_Date ASC, ROWID ASC
      LIMIT ${MANIFEST_BATCH} OFFSET ${offset}
    `);

    if (!rows || rows.length === 0) break;

    for (const r of rows) {
      const s = r.Split || r;
      if (s.ROWID != null) {
        if (seenRowIds.has(s.ROWID)) continue;
        seenRowIds.add(s.ROWID);
      }
      const effectiveDate = String(s.Issue_Date ?? "").slice(0, 10);
      events.push({
        isin: String(s.ISIN ?? "").trim(),
        recordDate: String(s.Record_Date ?? "").slice(0, 10) || effectiveDate,
        effectiveDate,
        ratio1: Number(s.Ratio1),
        ratio2: Number(s.Ratio2),
        securityCode: s.Security_Code ?? "",
        securityName: s.Security_Name ?? "",
      });
    }

    if (rows.length < MANIFEST_BATCH) break;
    offset += MANIFEST_BATCH;
  }

  return events;
}

/**
 * All CSV rows for ONE split event (one row per affected account).
 * Returns "" when the split has no Split_Account rows (e.g. no holders, or
 * the rebuild hasn't run for its accounts yet).
 */
async function buildEventCsv({ zcql, event, generatedAt, fromDate, toDate, csvCell }) {
  // A manifest frozen before this change carries only `issueDate`, which then
  // served as both dates.
  const { isin, ratio1, ratio2 } = event;
  const recordDate = event.recordDate || event.issueDate;
  const effectiveDate = event.effectiveDate || event.issueDate;
  if (!isin || !isIsoDate(recordDate)) return "";

  const r1 = Number(ratio1);
  const r2 = Number(ratio2);
  const multiplier = r2 / r1;
  if (!Number.isFinite(multiplier) || multiplier <= 0) return "";

  /* ---- One Split_Account row per affected account for this split ----
     The rebuild writes New_Holding = the account's holding just after the
     split (fractions kept), so it is read directly — no summing of lots. */
  const newHoldingByAccount = new Map(); // account -> New_Holding
  // ZCQL's OFFSET is 1-based, so each page repeats the previous page's last
  // row; the Map keyed by account makes that repeat harmless.
  let offset = 0;
  const isinEsc = esc(isin);
  // The rebuild stores the split's record date (Issue_Date for old splits).
  const dateEsc = esc(recordDate);

  while (true) {
    const rows = await zcql.executeZCQLQuery(`
      SELECT ROWID, WS_Account_code, New_Holding
      FROM Split_Account
      WHERE ISIN = '${isinEsc}' AND Record_Date = '${dateEsc}'
      ORDER BY ROWID ASC
      LIMIT ${SPLIT_ACCOUNT_BATCH} OFFSET ${offset}
    `);

    if (!rows || rows.length === 0) break;

    for (const r of rows) {
      const s = r.Split_Account || r;
      const acc = String(s.WS_Account_code || "").trim();
      if (!acc) continue;
      newHoldingByAccount.set(acc, Number(s.New_Holding) || 0);
    }

    if (rows.length < SPLIT_ACCOUNT_BATCH) break;
    offset += SPLIT_ACCOUNT_BATCH;
  }

  if (newHoldingByAccount.size === 0) return "";

  const accounts = [...newHoldingByAccount.keys()];
  const virtualToActual = await buildVirtualToActualMap(zcql, accounts);

  const ratioLabel = `${r1}:${r2}`;
  let text = "";

  for (const [acc, newHolding] of newHoldingByAccount) {
    if (newHolding <= 1e-6) continue; // dust / fully-closed

    const sharesAllocated = round4(newHolding);
    const actual = virtualToActual.get(acc) || "";

    text +=
      [
        generatedAt,
        fromDate,
        toDate,
        recordDate,
        effectiveDate,
        acc,
        actual,
        isin,
        "SPLIT",
        ratioLabel,
        sharesAllocated,
      ]
        .map(csvCell)
        .join(",") + "\n";
  }

  return text;
}

module.exports = {
  reportType: "split",
  header:
    "GENERATED_AT,FROM_DATE,TO_DATE,RECORD_DATE,EFFECTIVE_DATE,VIRTUAL_CODE,ACTUAL_CODE," +
    "ISIN,TYPE,SPLIT_RATIO,SHARES_ALLOCATED\n",
  buildManifest,
  buildEventCsv,
};
