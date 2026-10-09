/**
 * The Dividend page's custodian reconciliation (Receivable → Received), stored
 * on the per-account Dividend_Record rows.
 *
 *   "Send as Receivable" inserts one Dividend_Record row per account with
 *   Status RECEIVABLE (no cash yet). "Confirm & Apply" writes the Received
 *   Date from the Corporate Benefit Collection file, and UpdateDividendData
 *   credits the cash and marks the row RECEIVED.
 *
 * Rows applied before this reconciliation existed carry Status 'Draft' and
 * count as applied. Anything that sums dividends paid must skip RECEIVABLE rows.
 */

const PAGE = 250;
const escSql = (v) => String(v ?? "").replace(/'/g, "''");

export const DIVIDEND_STATUS = {
  RECEIVABLE: "RECEIVABLE",
  RECEIVED: "RECEIVED",
};

/** WHERE clause for one dividend's account rows (ISIN + record date). */
export const dividendAccountWhere = ({ isin, recordDateISO }) =>
  `ISIN = '${escSql(isin)}' AND RecordDate = '${escSql(recordDateISO)}'`;

/** Datastore returns an empty value as null or the text "null". */
const dateOf = (v) => (v && v !== "null" ? String(v).slice(0, 10) : "");

/** Datastore returns an empty number as null or the text "null"; keep it empty (not 0). */
const amountOf = (v) => (v === null || v === undefined || v === "" || v === "null" ? null : Number(v));

/**
 * Every Dividend_Record row of one dividend, one per virtual account code.
 * `amount` is the gross dividend; `taxAmount` / `netAmount` (cash credited) are
 * set by UpdateDividendData when the account is paid, and null before that.
 * @returns {Promise<Array<{ rowId, accountCode, holding, amount, taxAmount, netAmount, status, receivedDate }>>}
 */
export async function readDividendAccountRows(zcql, event) {
  const rows = [];
  let lastRowId = "0";
  for (;;) {
    const page = (
      (await zcql.executeZCQLQuery(`
        SELECT ROWID, WS_Account_code, Holding, Dividend_Amount, Tax_Amount, Net_Amount, Status, Received_Date
        FROM Dividend_Record
        WHERE ${dividendAccountWhere(event)} AND ROWID > ${lastRowId}
        ORDER BY ROWID ASC
        LIMIT ${PAGE}
      `)) || []
    ).map((row) => row.Dividend_Record || row);

    for (const d of page) {
      rows.push({
        rowId: d.ROWID,
        accountCode: String(d.WS_Account_code || "").trim(),
        holding: Number(d.Holding) || 0,
        amount: Number(d.Dividend_Amount) || 0,
        taxAmount: amountOf(d.Tax_Amount),
        netAmount: amountOf(d.Net_Amount),
        status: String(d.Status || "").trim().toUpperCase(),
        receivedDate: dateOf(d.Received_Date),
      });
    }
    if (page.length < PAGE) return rows;
    lastRowId = page[page.length - 1].ROWID;
  }
}

/** Is this dividend (ISIN + record date) applied? UpdateDividendData writes its master row. */
export async function isDividendApplied(zcql, event) {
  const rows = await zcql.executeZCQLQuery(`
    SELECT ROWID FROM Dividend WHERE ${dividendAccountWhere(event)} LIMIT 1
  `);
  return (rows || []).length > 0;
}

/**
 * Where a dividend is in the custodian reconciliation (same rules as splitStage):
 *   RECEIVABLE — some accounts are still Receivable (stage 2), including a
 *                dividend confirmed for only part of its accounts so far.
 *   RECEIVED   — applied, nothing left Receivable (also dividends applied
 *                before this reconciliation existed).
 *   NEW        — not applied and not sent as receivable yet (stage 1).
 */
export function dividendStage(accountRows, isApplied) {
  if (accountRows.some((r) => r.status === DIVIDEND_STATUS.RECEIVABLE)) return "RECEIVABLE";
  if (isApplied) return "RECEIVED";
  return "NEW";
}
