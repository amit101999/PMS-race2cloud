import { runLotFifo } from "./lotFifo.js";

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
 * Per-account demerger applied rows live in Demerger_Record (not Demerger,
 * which is the master/header table with Old_ISIN / New_ISIN columns).
 * Demerger_Record columns: WS_Account_code, TRANDATE, SETDATE, Tran_Type,
 * ISIN, Security_Code, Security_Name, QTY, PRICE, TOTAL_AMOUNT, HOLDING,
 * WAP, HOLDING_VALUE, PL.
 */
async function fetchDemergerRowsForIsinAsOnDate(zcql, isin, asOnDateISO) {
  const nextDayStr = nextDayIso(asOnDateISO);
  const out = [];
  const seen = new Set();
  let off = 0;
  while (true) {
    const r = await zcql.executeZCQLQuery(`
      SELECT *
      FROM Demerger_Record
      WHERE ISIN='${escSql(isin)}' AND TRANDATE < '${escSql(nextDayStr)}'
      ORDER BY ROWID ASC
      LIMIT ${ZCQL_ROW_LIMIT} OFFSET ${off}
    `);
    if (!r || r.length === 0) break;
    for (const row of r) {
      const d = row.Demerger_Record || row;
      if (!d || !d.ROWID) continue;
      if (seen.has(d.ROWID)) continue;
      seen.add(d.ROWID);
      out.push(row);
    }
    if (r.length < ZCQL_ROW_LIMIT) break;
    off += ZCQL_ROW_LIMIT;
  }
  return out;
}

async function fetchPriorMergerRowsForIsinAsOnDate(zcql, isin, asOnDateISO) {
  const nextDayStr = nextDayIso(asOnDateISO);
  const out = [];
  const seen = new Set();
  let off = 0;
  while (true) {
    const r = await zcql.executeZCQLQuery(`
      SELECT *
      FROM Merger
      WHERE ISIN='${escSql(isin)}' AND TRANDATE < '${escSql(nextDayStr)}'
      ORDER BY ROWID ASC
      LIMIT ${ZCQL_ROW_LIMIT} OFFSET ${off}
    `);
    if (!r || r.length === 0) break;
    for (const row of r) {
      const m = row.Merger || row;
      if (!m || !m.ROWID) continue;
      if (seen.has(m.ROWID)) continue;
      seen.add(m.ROWID);
      out.push(row);
    }
    if (r.length < ZCQL_ROW_LIMIT) break;
    off += ZCQL_ROW_LIMIT;
  }
  return out;
}

/**
 * Lot-level FIFO snapshot for one ISIN as of record date.
 *
 * Reads all Transaction / Bonus / Split / Demerger / prior Merger rows (≤ recordDate)
 * for the ISIN in single paginated passes, groups by WS_Account_code, then returns a
 * lookup `(accountCode) => openLots[]`. Each lot is `{ sourceType, sourceRowId,
 * originalTrandate, qty, costPerShare, totalCost }`.
 *
 * Identical replay rules as functions/MegerFn so preview === apply at the lot level.
 */
export async function fifoLotsForIsinOnRecordDate(zcql, isin, recordDateISO) {
  if (!recordDateISO || !/^\d{4}-\d{2}-\d{2}$/.test(recordDateISO)) {
    throw new Error("fifoLotsForIsinOnRecordDate: recordDateISO must be yyyy-mm-dd.");
  }

  const txRows = await fetchTransactionsForIsinAsOnDate(zcql, isin, recordDateISO);
  const bonusRows = await fetchBonusRowsForIsinAsOnDate(zcql, isin, recordDateISO);
  const splitRows = await fetchSplitRowsForIsinAsOnDate(zcql, isin, recordDateISO);
  const demergerRows = await fetchDemergerRowsForIsinAsOnDate(zcql, isin, recordDateISO);
  const priorMergerRows = await fetchPriorMergerRowsForIsinAsOnDate(zcql, isin, recordDateISO);

  const txByAccount = {};
  txRows.forEach((row) => {
    const t = row.Transaction || row;
    if (!t?.WS_Account_code) return;
    (txByAccount[t.WS_Account_code] ||= []).push(t);
  });

  const bonusByAccount = {};
  bonusRows.forEach((row) => {
    const b = row.Bonus || row;
    if (!b?.WS_Account_code) return;
    (bonusByAccount[b.WS_Account_code] ||= []).push(b);
  });

  const demergerByAccount = {};
  demergerRows.forEach((row) => {
    const d = row.Demerger_Record || row;
    if (!d?.WS_Account_code) return;
    if (String(d.Tran_Type || d.tran_type || "").toUpperCase() !== "DEMERGER") return;
    (demergerByAccount[d.WS_Account_code] ||= []).push(d);
  });

  const priorMergerByAccount = {};
  priorMergerRows.forEach((row) => {
    const m = row.Merger || row;
    if (!m?.WS_Account_code) return;
    (priorMergerByAccount[m.WS_Account_code] ||= []).push(m);
  });

  const splits = splitRows.map((row) => {
    const s = row.Split || row;
    return {
      issueDate: s.Issue_Date,
      ratio1: Number(s.Ratio1) || 0,
      ratio2: Number(s.Ratio2) || 0,
      isin: s.ISIN,
    };
  });

  /** @param {string} accountCode @returns {Array<{sourceRowId:string|null,sourceType:string,originalTrandate:string|null,qty:number,costPerShare:number,totalCost:number}>} */
  return (accountCode) => {
    const transactions = txByAccount[accountCode] || [];
    if (!transactions.length) return [];
    const bonuses = bonusByAccount[accountCode] || [];
    const demergers = demergerByAccount[accountCode] || [];
    const priorMergers = priorMergerByAccount[accountCode] || [];
    return runLotFifo(transactions, bonuses, splits, demergers, priorMergers);
  };
}
