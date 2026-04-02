import { runFifoEngine } from "../analytics/transactionHistory/fifo.js";

export const ZCQL_ROW_LIMIT = 270;

/** Matches Analytics `tabs/holding/AnalyticsControllers.js` (holding summary as-on date). */
const isBuyType = (type) => /^BY-|SQB|OPI/i.test(String(type || ""));

const getEffectiveDate = (t) => {
  const setDate = t.SETDATE || t.setdate;
  const tradeDate = t.TRANDATE || t.trandate;
  return isBuyType(t.Tran_Type || t.tranType)
    ? setDate || tradeDate
    : tradeDate || setDate;
};

function nextDayIso(asOnDateISO) {
  const nextDay = new Date(asOnDateISO);
  nextDay.setDate(nextDay.getDate() + 1);
  return nextDay.toISOString().split("T")[0];
}

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

async function fetchTransactionsForIsinAsOnDate(zcql, isin, asOnDateISO) {
  const nextDayStr = nextDayIso(asOnDateISO);
  const out = [];
  const seenTxnRowIds = new Set();
  let off = 0;
  while (true) {
    const r = await zcql.executeZCQLQuery(`
      SELECT *
      FROM Transaction
      WHERE ISIN='${escSql(isin)}'
        AND (TRANDATE < '${escSql(nextDayStr)}' OR SETDATE < '${escSql(nextDayStr)}')
      ORDER BY ROWID ASC
      LIMIT ${ZCQL_ROW_LIMIT} OFFSET ${off}
    `);
    if (!r || r.length === 0) break;
    for (const row of r) {
      const t = row.Transaction || row;
      if (seenTxnRowIds.has(t.ROWID)) continue;
      seenTxnRowIds.add(t.ROWID);
      out.push(row);
    }
    if (r.length < ZCQL_ROW_LIMIT) break;
    off += ZCQL_ROW_LIMIT;
  }

  const cutoff = nextDayStr;
  return out.filter((row) => {
    const t = row.Transaction || row;
    const effectiveDate = getEffectiveDate(t);
    return !effectiveDate || effectiveDate < cutoff;
  });
}

async function fetchBonusRowsForIsinAsOnDate(zcql, isin, asOnDateISO) {
  const nextDayStr = nextDayIso(asOnDateISO);
  const out = [];
  const seenBonusRowIds = new Set();
  let off = 0;
  while (true) {
    const r = await zcql.executeZCQLQuery(`
      SELECT *
      FROM Bonus
      WHERE ISIN='${escSql(isin)}' AND ExDate < '${escSql(nextDayStr)}'
      ORDER BY ROWID ASC
      LIMIT ${ZCQL_ROW_LIMIT} OFFSET ${off}
    `);
    if (!r || r.length === 0) break;
    for (const row of r) {
      const b = row.Bonus || row;
      if (!b || !b.ROWID) continue;
      if (seenBonusRowIds.has(b.ROWID)) continue;
      seenBonusRowIds.add(b.ROWID);
      out.push(row);
    }
    if (r.length < ZCQL_ROW_LIMIT) break;
    off += ZCQL_ROW_LIMIT;
  }
  return out;
}

async function fetchSplitRowsForIsinAsOnDate(zcql, isin, asOnDateISO) {
  const nextDayStr = nextDayIso(asOnDateISO);
  const out = [];
  const seenSplitRowIds = new Set();
  let off = 0;
  while (true) {
    const r = await zcql.executeZCQLQuery(`
      SELECT *
      FROM Split
      WHERE ISIN='${escSql(isin)}' AND Issue_Date < '${escSql(nextDayStr)}'
      ORDER BY ROWID ASC
      LIMIT ${ZCQL_ROW_LIMIT} OFFSET ${off}
    `);
    if (!r || r.length === 0) break;
    for (const row of r) {
      const s = row.Split || row;
      if (!s || !s.ROWID) continue;
      if (seenSplitRowIds.has(s.ROWID)) continue;
      seenSplitRowIds.add(s.ROWID);
      out.push(row);
    }
    if (r.length < ZCQL_ROW_LIMIT) break;
    off += ZCQL_ROW_LIMIT;
  }
  return out;
}

/**
 * FIFO snapshot (card mode) for one ISIN as of record date — same inputs as Analytics holding tab
 * (effective-date filter, TRANDATE/SETDATE window, bonus/split &lt; next day, ORDER BY ROWID).
 */
export async function fifoCardForIsinOnRecordDate(zcql, isin, recordDateISO) {
  if (!recordDateISO || !/^\d{4}-\d{2}-\d{2}$/.test(recordDateISO)) {
    throw new Error("fifoCardForIsinOnRecordDate: recordDateISO must be yyyy-mm-dd.");
  }

  const txRows = await fetchTransactionsForIsinAsOnDate(zcql, isin, recordDateISO);
  const bonusRows = await fetchBonusRowsForIsinAsOnDate(zcql, isin, recordDateISO);
  const splitRows = await fetchSplitRowsForIsinAsOnDate(zcql, isin, recordDateISO);

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
