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

function getIsinsForAccount(zcql, accountCode) {
  return (async () => {
    const seen = new Set();
    const list = [];
    let offset = 0;
    const limit = 250;

    while (true) {
      try {
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
      } catch (err) {
        console.error(`Error fetching ISINs for ${accountCode} at offset ${offset}:`, err);
        break;
      }
    }

    return list;
  })();
}

/**
 * Merge transactions, bonuses, splits; sort by date; compute running holdings.
 * Consistent with the Analytics holding page FIFO engine (quantity-only version).
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
      if (!qty) continue;

      const tranType = t.tranType ?? t.Tran_Type ?? "";
      const price = Number(t.netrate ?? t.NETRATE) || 0;
      const netAmount = Number(t.netAmount ?? t.Net_Amount) || 0;

      if (
        String(tranType).toUpperCase() === "OPI" &&
        qty ==1  &&
        price === 0 &&
        netAmount === 0
      ) {
        continue;
      }

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
      if (!qty) continue;
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
      if (r1 && r2) holdings = holdings * (r2 / r1);
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

async function runQuantityBackfill(zcql) {
  const accounts = [{ clientIds: { WS_Account_code: "AYAN002" } }];

  for (const client of accounts) {
    const accountCode = client.clientIds.WS_Account_code;
    if (!accountCode) continue;

    console.log(`[Backfill] Processing account: ${accountCode}`);

    const isins = await getIsinsForAccount(zcql, accountCode);
    console.log(`[Backfill] Found ${isins.length} ISINs for ${accountCode}`);

    try {
      await zcql.executeZCQLQuery(
        `DELETE FROM Daily_Holding_Quantity WHERE WS_Account_code = '${esc(accountCode)}'`
      );
      console.log(`[Backfill] Deleted old rows for ${accountCode}`);
    } catch (delErr) {
      console.error(`[Backfill] Error deleting old rows for ${accountCode}:`, delErr);
    }

    let totalInserted = 0;
    let totalErrors = 0;

    for (const isin of isins) {
      try {
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
          try {
            await zcql.executeZCQLQuery(`
              INSERT INTO Daily_Holding_Quantity
              ( WS_Account_code, ISIN, TRANDATE, Tran_Type, QTY, HOLDINGS )
              VALUES
              ( '${esc(accountCode)}', '${esc(isin)}', '${esc(row.trandate)}', '${esc(row.tranType)}', ${Number(row.qty) || 0}, ${Number(row.holdings) || 0} )
            `);
            totalInserted++;
          } catch (insertErr) {
            totalErrors++;
            console.error(`[Backfill] Insert error ${accountCode}/${isin}/${row.trandate}:`, insertErr);
          }
        }
      } catch (isinErr) {
        totalErrors++;
        console.error(`[Backfill] Error processing ISIN ${isin} for ${accountCode}:`, isinErr);
      }
    }

    console.log(`[Backfill] ${accountCode} done. Inserted: ${totalInserted}, Errors: ${totalErrors}`);
  }
}

module.exports = { runQuantityBackfill };
