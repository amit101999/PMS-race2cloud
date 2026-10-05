/**
 * The Bonus page's custodian reconciliation (Receivable → Received), stored in
 * Bonus_Record next to the bonus events.
 *
 * Bonus_Record holds two kinds of rows:
 *   - event rows    — one per applied bonus, WS_Account_code empty. Everything
 *                     that lists bonuses (split recompute, CA reports,
 *                     UpdateBonusTable) reads only these.
 *   - account rows  — one per account per bonus, WS_Account_code set, with
 *                     Old_Holding, Bonus_Shares, Status and Received_Date.
 *                     "Send as Receivable" inserts them; UpdateBonusTable marks
 *                     them RECEIVED once the bonus is applied.
 */

const PAGE = 250;
const escSql = (v) => String(v ?? "").replace(/'/g, "''");

export const BONUS_STATUS = {
  RECEIVABLE: "RECEIVABLE",
  RECEIVED: "RECEIVED",
};

/** WHERE clause for one bonus's account rows (ISIN + record date + ratio). */
export const bonusAccountWhere = ({ isin, recordDateISO, r1, r2 }) =>
  `ISIN = '${escSql(isin)}' AND Record_Date = '${escSql(recordDateISO)}' ` +
  `AND Ratio1 = ${Number(r1)} AND Ratio2 = ${Number(r2)} AND WS_Account_code IS NOT NULL`;

/**
 * Every account row of one bonus, one per virtual account code.
 * @returns {Promise<Array<{ rowId, accountCode, oldHolding, bonusShares, status, receivedDate }>>}
 */
export async function readBonusAccountRows(zcql, event) {
  const rows = [];
  let lastRowId = "0";
  for (;;) {
    const page = (
      (await zcql.executeZCQLQuery(`
        SELECT ROWID, WS_Account_code, Old_Holding, Bonus_Shares, Status, Received_Date
        FROM Bonus_Record
        WHERE ${bonusAccountWhere(event)} AND ROWID > ${lastRowId}
        ORDER BY ROWID ASC
        LIMIT ${PAGE}
      `)) || []
    ).map((row) => row.Bonus_Record || row);

    for (const b of page) {
      rows.push({
        rowId: b.ROWID,
        accountCode: String(b.WS_Account_code || "").trim(),
        oldHolding: Number(b.Old_Holding) || 0,
        bonusShares: Number(b.Bonus_Shares) || 0,
        status: String(b.Status || "").trim().toUpperCase(),
        receivedDate: b.Received_Date && b.Received_Date !== "null" ? String(b.Received_Date).slice(0, 10) : "",
      });
    }
    if (page.length < PAGE) return rows;
    lastRowId = page[page.length - 1].ROWID;
  }
}

/**
 * Is this bonus (ISIN + record date) applied? UpdateBonusTable writes its event
 * row once the bonus is in; bonuses saved before Record_Date existed carry the
 * record date in ExDate.
 */
export async function isBonusApplied(zcql, { isin, recordDateISO }) {
  const rows = await zcql.executeZCQLQuery(`
    SELECT ROWID, Record_Date, ExDate FROM Bonus_Record
    WHERE ISIN = '${escSql(isin)}' AND WS_Account_code IS NULL
  `);
  return (rows || []).some((row) => {
    const b = row.Bonus_Record || row;
    const recordDate = b.Record_Date && b.Record_Date !== "null" ? b.Record_Date : b.ExDate;
    return String(recordDate || "").slice(0, 10) === recordDateISO;
  });
}

/**
 * Where a bonus is in the custodian reconciliation (same rules as splitStage):
 *   RECEIVABLE — some accounts are still Receivable (stage 2), including a
 *                bonus whose apply job has not finished marking them Received.
 *   RECEIVED   — applied, nothing left Receivable (also bonuses applied before
 *                this reconciliation existed, which have no account rows).
 *   NEW        — not applied and not sent as receivable yet (stage 1).
 */
export function bonusStage(accountRows, isApplied) {
  if (accountRows.some((r) => r.status === BONUS_STATUS.RECEIVABLE)) return "RECEIVABLE";
  if (isApplied) return "RECEIVED";
  return "NEW";
}
