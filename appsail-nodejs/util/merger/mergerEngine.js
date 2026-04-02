import { runFifoEngine } from "../analytics/transactionHistory/fifo.js";

export const ZCQL_ROW_LIMIT = 270;

export function escSql(value) {
  return String(value ?? "").replace(/'/g, "''");
}

/**
 * Accounts that have any Transaction row for this ISIN (same seed set as Bonus preview).
 */
export async function fetchAccountsForIsin(zcql, isin) {
  const accountSet = new Set();
  let offset = 0;
  while (true) {
    const batch = await zcql.executeZCQLQuery(`
      SELECT WS_Account_code
      FROM Transaction
      WHERE ISIN='${escSql(isin)}'
      LIMIT ${ZCQL_ROW_LIMIT} OFFSET ${offset}
    `);
    if (!batch || batch.length === 0) break;
    batch.forEach((r) => accountSet.add(r.Transaction.WS_Account_code));
    if (batch.length < ZCQL_ROW_LIMIT) break;
    offset += ZCQL_ROW_LIMIT;
  }
  return accountSet;
}

async function fetchAllDeduped(zcql, table, isin, dateCol, dateISO) {
  const out = [];
  const seenRowIds = new Set();
  let off = 0;
  while (true) {
    const r = await zcql.executeZCQLQuery(`
      SELECT *
      FROM ${table}
      WHERE ISIN='${escSql(isin)}' AND ${dateCol} <= '${escSql(dateISO)}'
      ORDER BY ${dateCol} ASC, ROWID ASC
      LIMIT ${ZCQL_ROW_LIMIT} OFFSET ${off}
    `);
    if (!r || r.length === 0) break;
    for (const row of r) {
      const rec = row[table] || row.Transaction || row.Bonus || row.Split || row;
      const rowId = rec.ROWID;
      if (rowId != null && seenRowIds.has(rowId)) continue;
      if (rowId != null) seenRowIds.add(rowId);
      out.push(row);
    }
    if (r.length < ZCQL_ROW_LIMIT) break;
    off += ZCQL_ROW_LIMIT;
  }
  return out;
}

/**
 * FIFO snapshot (card mode) for one ISIN as of record date — holdings, carried cost (holdingValue), WAP.
 */
export async function fifoCardForIsinOnRecordDate(zcql, isin, recordDateISO) {
  const txRows = await fetchAllDeduped(
    zcql,
    "Transaction",
    isin,
    "SETDATE",
    recordDateISO,
  );
  const bonusRows = await fetchAllDeduped(zcql, "Bonus", isin, "ExDate", recordDateISO);
  const splitRows = await fetchAllDeduped(
    zcql,
    "Split",
    isin,
    "Issue_Date",
    recordDateISO,
  );

  const txByAccount = {};
  txRows.forEach((row) => {
    const t = row.Transaction;
    (txByAccount[t.WS_Account_code] ||= []).push(t);
  });

  const bonusByAccount = {};
  bonusRows.forEach((row) => {
    const b = row.Bonus;
    (bonusByAccount[b.WS_Account_code] ||= []).push(b);
  });

  const splits = splitRows.map((row) => {
    const s = row.Split;
    return {
      issueDate: s.Issue_Date,
      ratio1: Number(s.Ratio1) || 0,
      ratio2: Number(s.Ratio2) || 0,
      isin: s.ISIN,
    };
  });

  /** @param {string} accountCode */
  return (accountCode) => {
    const transactions = txByAccount[accountCode] || [];
    if (!transactions.length) {
      return {
        holdings: 0,
        holdingValue: 0,
        averageCostOfHoldings: 0,
      };
    }
    const bonuses = bonusByAccount[accountCode] || [];
    return runFifoEngine(transactions, bonuses, splits, true);
  };
}

/**
 * New shares from one old leg: ratio is "new shares per 1 old share" (e.g. 1 => 1:1).
 */
export function newSharesFromLeg(holdings, ratio) {
  const r = Number(ratio);
  if (!Number.isFinite(r) || r <= 0 || holdings <= 0) return 0;
  return Math.floor(holdings * r);
}

/**
 * Merger ratio: Ratio1 : Ratio2 as entered → mergedQty = floor(holdings × Ratio1 ÷ Ratio2).
 * Same as bonus (e.g. 1 : 2 ⇒ 480 → 240). 2 : 1 ⇒ double.
 */
export function newMergedSharesFromRatio(holdings, ratio1, ratio2) {
  const h = Number(holdings);
  const r1 = Number(ratio1);
  const r2 = Number(ratio2);
  if (!Number.isFinite(h) || h <= 0 || !Number.isFinite(r1) || !Number.isFinite(r2) || r1 <= 0 || r2 <= 0) {
    return 0;
  }
  return Math.floor((h * r1) / r2);
}
