import { getAllAccountCodesFromDatabase } from "../../../../util/allAccountCodes.js";
import { runFifoEngine } from "../../../../util/analytics/transactionHistory/fifo.js";

export const getAllAccountCodes = async (req, res) => {
  try {
    const zohoCatalyst = req.catalystApp;
    let zcql = zohoCatalyst.zcql();
    let tableName = "clientIds";
    const cliendIds = await getAllAccountCodesFromDatabase(zcql, tableName);
    return res.status(200).json({ data: cliendIds });
  } catch (error) {
    console.log("Error in fetching data", error);
    res.status(400).json({ error: error.message });
  }
};

export const getHoldingsSummarySimple = async (req, res) => {
  try {
    const accountCode = req.query.accountCode;
    if (!accountCode)
      return res.status(400).json({ message: "accountCode required" });

    const data = await calculateHoldingsSummary({
      catalystApp: req.catalystApp,
      accountCode,
      asOnDate: req.query.asOnDate,
    });

    return res.json(data);
  } catch (err) {
    console.error("Holding summary error:", err);
    res.status(500).json({ message: "Failed", error: err.message });
  }
};

/* ================= CALCULATE HOLDINGS SUMMARY ================= */
export const calculateHoldingsSummary = async ({
  catalystApp,
  accountCode,
  asOnDate,
}) => {
  const zcql = catalystApp.zcql();
  const batchLimit = 250;

  let txnDateCondition = "";
  let bonusDateCondition = "";
  let splitDateCondition = "";

  if (asOnDate && /^\d{4}-\d{2}-\d{2}$/.test(asOnDate)) {
    const nextDay = new Date(asOnDate);
    nextDay.setDate(nextDay.getDate() + 1);
    const nextDayStr = nextDay.toISOString().split("T")[0];

    txnDateCondition = `AND SETDATE < '${nextDayStr}'`;
    bonusDateCondition = `AND ExDate < '${nextDayStr}'`;
    splitDateCondition = `AND Issue_Date < '${nextDayStr}'`;
  }

  /* ================= FETCH TRANSACTIONS ================= */
  const transactions = [];
  const uniqueISINs = new Set();
  let offset = 0;
  const seenTxnRowIds = new Set();
  const seenBonusRowIds = new Set();
  const seenSplitRowIds = new Set();

  while (true) {
    try {
      const rows = await zcql.executeZCQLQuery(`
        SELECT Security_Name, Security_code, Tran_Type, QTY, SETDATE,
               NETRATE, Net_Amount, ISIN, ROWID
        FROM Transaction
        WHERE WS_Account_code = '${accountCode}'
        ${txnDateCondition}
        ORDER BY ROWID ASC
        LIMIT ${batchLimit} OFFSET ${offset}
      `);

      if (!rows.length) break;

      for (const r of rows) {
        const t = r.Transaction || r;
        if (seenTxnRowIds.has(t.ROWID)) continue;
        seenTxnRowIds.add(t.ROWID);
        transactions.push(t);
        if (t.ISIN) uniqueISINs.add(t.ISIN);
      }

      if (rows.length < batchLimit) break;
      offset += batchLimit;
    } catch (err) {
      console.error(`Error fetching transactions for ${accountCode} at offset ${offset}:`, err);
      break;
    }
  }

  /* ================= FETCH BONUS ================= */
  const bonuses = [];
  offset = 0;

  while (true) {
    try {
      const rows = await zcql.executeZCQLQuery(`
        SELECT SecurityName, BonusShare, ExDate, ISIN, ROWID
        FROM Bonus
        WHERE WS_Account_code = '${accountCode}'
        ${bonusDateCondition}
        ORDER BY ROWID ASC
        LIMIT ${batchLimit} OFFSET ${offset}
      `);

      if (!rows.length) break;

      for (const r of rows) {
        const b = r.Bonus || r;
        if (!b || !b.ROWID) continue;
        if (seenBonusRowIds.has(b.ROWID)) continue;
        seenBonusRowIds.add(b.ROWID);
        bonuses.push(b);
      }

      if (rows.length < batchLimit) break;
      offset += batchLimit;
    } catch (err) {
      console.error(`Error fetching bonuses for ${accountCode} at offset ${offset}:`, err);
      break;
    }
  }

  /* ================= FETCH SPLITS ================= */
  const splits = [];
  offset = 0;

  const isinList = [...uniqueISINs];
  if (isinList.length) {
    const inClause = isinList.map((i) => `'${i}'`).join(",");

    while (true) {
      try {
        const rows = await zcql.executeZCQLQuery(`
          SELECT Issue_Date, Ratio1, Ratio2, ISIN, ROWID
          FROM Split
          WHERE ISIN IN (${inClause})
          ${splitDateCondition}
          ORDER BY ROWID ASC
          LIMIT ${batchLimit} OFFSET ${offset}
        `);

        if (!rows.length) break;
        for (const r of rows) {
          const s = r.Split || r;
          if (!s || !s.ROWID) continue;
          if (seenSplitRowIds.has(s.ROWID)) continue;
          seenSplitRowIds.add(s.ROWID);
          splits.push({
            issueDate: s.Issue_Date,
            ratio1: s.Ratio1,
            ratio2: s.Ratio2,
            isin: s.ISIN,
          });
        }

        if (rows.length < batchLimit) break;
        offset += batchLimit;
      } catch (err) {
        console.error(`Error fetching splits for ${accountCode} at offset ${offset}:`, err);
        break;
      }
    }
  }

  /* ================= GROUP BY ISIN ================= */
  const txByISIN = {};
  const bonusByISIN = {};
  const splitByISIN = {};
  const holdingsMap = {};

  for (const t of transactions) {
    if (!t.ISIN) continue;
    const key = t.ISIN;

    if (!txByISIN[key]) txByISIN[key] = [];
    txByISIN[key].push(t);

    if (!holdingsMap[key]) {
      holdingsMap[key] = {
        stockName: t.Security_Name,
        securityCode: t.Security_code,
        isin: t.ISIN,
      };
    }
  }

  for (const b of bonuses) {
    if (!b.ISIN) continue;
    if (!bonusByISIN[b.ISIN]) bonusByISIN[b.ISIN] = [];
    bonusByISIN[b.ISIN].push(b);
  }

  for (const s of splits) {
    if (!s.isin) continue;
    if (!splitByISIN[s.isin]) splitByISIN[s.isin] = [];
    splitByISIN[s.isin].push(s);
  }

  /* ================= FETCH BHAV PRICES ================= */

  const todayStr = new Date().toISOString().split("T")[0];
  const priceDate = asOnDate && asOnDate < todayStr ? asOnDate : todayStr;

  const priceMap = {};

  if (isinList.length) {
    for (const isin of isinList) {
      if (isin in priceMap) continue;
      try {
        const priceRows = await zcql.executeZCQLQuery(`
          SELECT ISIN, ClsPric, TradDt
          FROM Bhav_Copy
          WHERE ISIN = '${isin}'
            AND TradDt <= '${priceDate}'
          ORDER BY TradDt DESC
          LIMIT 1
        `);

        if (priceRows.length) {
          const row = priceRows[0].Bhav_Copy || priceRows[0];
          priceMap[isin] = row.ClsPric || 0;
        } else {
          priceMap[isin] = 0;
        }
      } catch (err) {
        console.error(`Error fetching price for ISIN ${isin}:`, err);
        priceMap[isin] = 0;
      }
    }
  }

  /* ================= FINAL FIFO ================= */

  const result = [];

  for (const key of Object.keys(holdingsMap)) {
    const fifo = runFifoEngine(
      txByISIN[key],
      bonusByISIN[key] || [],
      splitByISIN[key] || [],
      true,
    );

    if (!fifo || fifo.holdings <= 0) continue;

    const lastPrice = priceMap[key] || 0;

    result.push({
      isin: key,
      stockName: holdingsMap[key].stockName,
      securityCode: holdingsMap[key].securityCode,
      currentHolding: fifo.holdings,
      avgPrice: fifo.averageCostOfHoldings,
      holdingValue: fifo.holdingValue,
      lastPrice,
      marketValue: fifo.holdings * lastPrice,
    });
  }

  return result.sort((a, b) => (a.stockName || "").localeCompare(b.stockName || ""));
};