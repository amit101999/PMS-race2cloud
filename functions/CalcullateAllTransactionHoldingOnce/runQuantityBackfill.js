/**
 * Running quantity backfill: for each account and ISIN, compute holding after each
 * event (transaction, bonus, split) and insert into Holding_Quantity.
 * No FIFO, no cost/WAP/P&L — quantity only.
 */

const { getAllAccountCodesFromDatabase } = require("./accountCodes");
const { fetchStockTransactions } = require("./transactions");
const { fetchBonusesForStock } = require("./Bonuses");
const { fetchSplitForStock } = require("./split");

const esc = (s) => String(s).replace(/'/g, "''");

function isBuy(t) {
  return /^BY-|SQB|OPI/i.test(String(t));
}
function isSell(t) {
  return /^SL\+|SQS|OPO|NF-/i.test(String(t));
}

function normalizeDate(d) {
  if (!d) return "9999-12-31";
  const s = String(d).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  const [y, m, day] = s.split("-").map(Number);
  if (!y || !m || !day) return "9999-12-31";
  const fullYear = y < 100 ? 2000 + y : y;
  return `${fullYear}-${String(m).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

/**
 * Get distinct ISINs for an account from Transaction table.
 */
async function getIsinsForAccount(zcql, accountCode) {
  const seen = new Set();
  const list = [];
  let offset = 0;
  const limit = 250;

  while (true) {
    const q = `
      SELECT ISIN FROM Transaction
      WHERE WS_Account_code = '${esc(accountCode)}'
      ORDER BY ISIN ASC
      LIMIT ${limit} OFFSET ${offset}
    `;
    const rows = await zcql.executeZCQLQuery(q);
    if (!rows || rows.length === 0) break;

    for (const row of rows) {
      const r = row.Transaction || row;
      const isin = (r.ISIN || "").toString().trim();
      if (isin && !seen.has(isin)) {
        seen.add(isin);
        list.push(isin);
      }
    }
    if (rows.length < limit) break;
    offset += rows.length;
  }

  return list;
}

/**
 * Merge transactions, bonuses, splits; sort by date; compute running holdings.
 * Returns array of { trandate, tranType, qty, holdings }.
 */
function calculateRunningQuantity(transactions, bonuses, splits) {
  const events = [];

  for (const t of transactions || []) {
    events.push({
      type: "TXN",
      date: normalizeDate(t.setdate || t.SETDATE || t.trandate || t.TRANDATE),
      data: t,
    });
  }
  for (const b of bonuses || []) {
    events.push({
      type: "BONUS",
      date: normalizeDate(b.exDate || b.ExDate),
      data: b,
    });
  }
  for (const s of splits || []) {
    events.push({
      type: "SPLIT",
      date: normalizeDate(s.issueDate || s.date),
      data: s,
    });
  }

  events.sort((a, b) => {
    const d = a.date.localeCompare(b.date);
    if (d !== 0) return d;
    const order = { TXN: 0, BONUS: 1, SPLIT: 2 };
    return (order[a.type] ?? 3) - (order[b.type] ?? 3);
  });

  let holdings = 0;
  const output = [];

  for (const e of events) {
    if (e.type === "TXN") {
      const t = e.data;
      const qty = Math.abs(Number(t.qty ?? t.QTY) || 0);
      const tranType = t.tranType ?? t.Tran_Type ?? "";

      if (isBuy(tranType)) holdings += qty;
      if (isSell(tranType)) {
        holdings -= qty;
        if (holdings < 0) holdings = 0;
      }

      output.push({
        trandate: e.date,
        tranType,
        qty,
        holdings,
      });
    } else if (e.type === "BONUS") {
      const qty = Number(e.data.bonusShare ?? e.data.BonusShare) || 0;
      holdings += qty;
      output.push({
        trandate: e.date,
        tranType: "BONUS",
        qty,
        holdings,
      });
    } else if (e.type === "SPLIT") {
      const r1 = Number(e.data.ratio1 ?? e.data.Ratio1) || 0;
      const r2 = Number(e.data.ratio2 ?? e.data.Ratio2) || 0;
      if (r1 && r2) holdings = Math.floor(holdings * (r2 / r1));
      if (holdings < 0) holdings = 0;
      output.push({
        trandate: e.date,
        tranType: "SPLIT",
        qty: 0,
        holdings,
      });
    }
  }

  return output;
}

/**
 * Single entry: loop accounts → loop ISINs → fetch → compute running quantity → insert.
 */
async function runQuantityBackfill(zcql) {
  const accounts = await getAllAccountCodesFromDatabase(zcql, "clientIds");
  if (!accounts || accounts.length === 0) {
    return;
  }

  for (const client of accounts) {
    const accountCode = client.clientIds.WS_Account_code;
    if (!accountCode) continue;

    const isins = await getIsinsForAccount(zcql, accountCode);

    for (const isin of isins) {
      const transactions = await fetchStockTransactions({
        zcql,
        tableName: "Transaction",
        accountCode,
        isin,
      });
      const bonuses = await fetchBonusesForStock({
        zcql,
        accountCode,
        isin,
      });
      const splits = await fetchSplitForStock({
        zcql,
        isin,
        tableName: "Split",
      });

      const ledger = calculateRunningQuantity(transactions, bonuses, splits);

      for (const row of ledger) {
        const insertQuery = `
          INSERT INTO Daily_Holding_Quantity
          ( WS_Account_code, ISIN, TRANDATE, Tran_Type, QTY, HOLDINGS )
          VALUES
          ( '${esc(accountCode)}', '${esc(isin)}', '${esc(row.trandate)}', '${esc(row.tranType)}', ${Number(row.qty) || 0}, ${Number(row.holdings) || 0} )
        `;
        await zcql.executeZCQLQuery(insertQuery);
      }
    }
  }
}

module.exports = { runQuantityBackfill };
