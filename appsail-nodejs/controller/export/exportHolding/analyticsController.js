import { runFifoEngine } from "../../../util/export/fifo.js";

// export const calculateHoldingsSummary = async ({
//   catalystApp,
//   accountCode,
//   asOnDate,
// }) => {
//   const zcql = catalystApp.zcql();
//   const batchLimit = 250;

//   let txnDateCondition = "";
//   let bonusDateCondition = "";
//   let splitDateCondition = "";

//   if (asOnDate && /^\d{4}-\d{2}-\d{2}$/.test(asOnDate)) {
//     const nextDay = new Date(asOnDate);
//     nextDay.setDate(nextDay.getDate() + 1);
//     const nextDayStr = nextDay.toISOString().split("T")[0];

//     txnDateCondition = `AND SETDATE < '${nextDayStr}'`;
//     bonusDateCondition = `AND ExDate < '${nextDayStr}'`;
//     splitDateCondition = `AND Issue_Date < '${nextDayStr}'`;
//   }

//   /* ================= FETCH TRANSACTIONS ================= */
//   const transactions = [];
//   const uniqueISINs = new Set();
//   let offset = 0;

//   while (true) {
//     const rows = await zcql.executeZCQLQuery(`
//       SELECT Security_Name, Security_code, Tran_Type, QTY, TRANDATE,
//              NETRATE, Net_Amount, ISIN, ROWID
//       FROM Transaction
//       WHERE WS_Account_code = '${accountCode}'
//       ${txnDateCondition}
//       ORDER BY TRANDATE ASC, ROWID ASC
//       LIMIT ${batchLimit} OFFSET ${offset}
//     `);

//     if (!rows.length) break;

//     for (const r of rows) {
//       const t = r.Transaction || r;
//       transactions.push(t);
//       if (t.ISIN) uniqueISINs.add(t.ISIN);
//     }

//     if (rows.length < batchLimit) break;
//     offset += batchLimit;
//   }

//   /* ================= FETCH BONUS ================= */
//   const bonuses = [];
//   offset = 0;

//   while (true) {
//     const rows = await zcql.executeZCQLQuery(`
//       SELECT SecurityName, BonusShare, ExDate, ISIN, ROWID
//       FROM Bonus
//       WHERE WS_Account_code = '${accountCode}'
//       ${bonusDateCondition}
//       ORDER BY ExDate ASC, ROWID ASC
//       LIMIT ${batchLimit} OFFSET ${offset}
//     `);

//     if (!rows.length) break;
//     bonuses.push(...rows.map((r) => r.Bonus || r));

//     if (rows.length < batchLimit) break;
//     offset += batchLimit;
//   }

//   /* ================= FETCH SPLITS ================= */
//   const splits = [];
//   offset = 0;

//   const isinList = [...uniqueISINs];
//   if (isinList.length) {
//     const inClause = isinList.map((i) => `'${i}'`).join(",");

//     while (true) {
//       const rows = await zcql.executeZCQLQuery(`
//         SELECT Issue_Date, Ratio1, Ratio2, ISIN, ROWID
//         FROM Split
//         WHERE ISIN IN (${inClause})
//         ${splitDateCondition}
//         ORDER BY Issue_Date ASC, ROWID ASC
//         LIMIT ${batchLimit} OFFSET ${offset}
//       `);

//       if (!rows.length) break;
//       if (!rows.length) break;
//       splits.push(
//         ...rows.map((r) => {
//           const s = r.Split || r;
//           return {
//             issueDate: s.Issue_Date,
//             ratio1: s.Ratio1,
//             ratio2: s.Ratio2,
//             isin: s.ISIN,
//           };
//         })
//       );

//       if (rows.length < batchLimit) break;
//       offset += batchLimit;
//     }
//   }

//   /* ================= GROUP BY ISIN ================= */
//   const txByISIN = {};
//   const bonusByISIN = {};
//   const splitByISIN = {};
//   const holdingsMap = {};

//   for (const t of transactions) {
//     if (!t.ISIN) continue;
//     const key = t.ISIN;

//     if (!txByISIN[key]) txByISIN[key] = [];
//     txByISIN[key].push(t);

//     if (!holdingsMap[key]) {
//       holdingsMap[key] = {
//         stockName: t.Security_Name,
//         securityCode: t.Security_code,
//         isin: t.ISIN,
//       };
//     }
//   }

//   for (const b of bonuses) {
//     if (!b.ISIN) continue;
//     if (!bonusByISIN[b.ISIN]) bonusByISIN[b.ISIN] = [];
//     bonusByISIN[b.ISIN].push(b);
//   }

//   for (const s of splits) {
//     if (!s.isin) continue;
//     if (!splitByISIN[s.isin]) splitByISIN[s.isin] = [];
//     splitByISIN[s.isin].push(s);
//   }

//   /* ================= FETCH BHAV PRICES ================= */

//   const todayStr = new Date().toISOString().split("T")[0];
//   const priceDate = asOnDate && asOnDate < todayStr ? asOnDate : todayStr;

//   const priceMap = {};

//   if (isinList.length) {
//     const inClause = isinList.map((i) => `'${i}'`).join(",");

//     const priceRows = await zcql.executeZCQLQuery(`
//       SELECT ISIN, ClsPric, TradDt
//       FROM Bhav_Copy
//       WHERE ISIN IN (${inClause})
//         AND TradDt <= '${priceDate}'
//       ORDER BY ISIN ASC, TradDt DESC
//     `);

//     for (const r of priceRows) {
//       const row = r.Bhav_Copy || r;
//       if (!priceMap[row.ISIN]) {
//         priceMap[row.ISIN] = row.ClsPric || 0;
//       }
//     }
//   }

//   /* ================= FINAL FIFO ================= */

//   const result = [];

//   for (const key of Object.keys(holdingsMap)) {
//     const fifo = runFifoEngine(
//       txByISIN[key],
//       bonusByISIN[key] || [],
//       splitByISIN[key] || [],
//       true
//     );

//     if (!fifo || fifo.holdings <= 0) continue;

//     const lastPrice = priceMap[key] || 0;

//     result.push({
//       isin: key,
//       stockName: holdingsMap[key].stockName,
//       securityCode: holdingsMap[key].securityCode,
//       currentHolding: fifo.holdings,
//       avgPrice: fifo.averageCostOfHoldings,
//       holdingValue: fifo.holdingValue,
//       lastPrice,
//       marketValue: fifo.holdings * lastPrice,
//     });
//   }

//   return result.sort((a, b) => a.stockName.localeCompare(b.stockName));
// };
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
  }

  /* ================= FETCH BONUS ================= */
  const bonuses = [];
  offset = 0;

  while (true) {
    const rows = await zcql.executeZCQLQuery(`
      SELECT SecurityName, BonusShare, ExDate, ISIN, ROWID
      FROM Bonus
      WHERE WS_Account_code = '${accountCode}'
      ${bonusDateCondition}
      ORDER BY  ROWID ASC
      LIMIT ${batchLimit} OFFSET ${offset}
    `);

    if (!rows.length) break;
    for (const r of rows) {
      const b = r.Bonus || r;

      // safety guard
      if (!b || !b.ROWID) continue;

      // duplicate protection
      if (seenBonusRowIds.has(b.ROWID)) continue;

      seenBonusRowIds.add(b.ROWID);
      bonuses.push(b);
    }

    if (rows.length < batchLimit) break;
    offset += batchLimit;
  }

  /* ================= FETCH SPLITS ================= */
  const splits = [];
  offset = 0;

  const isinList = [...uniqueISINs];
  if (isinList.length) {
    const inClause = isinList.map((i) => `'${i}'`).join(",");

    while (true) {
      const rows = await zcql.executeZCQLQuery(`
        SELECT Issue_Date, Ratio1, Ratio2, ISIN, ROWID
        FROM Split
        WHERE ISIN IN (${inClause})
        ${splitDateCondition}
        ORDER BY  ROWID ASC
        LIMIT ${batchLimit} OFFSET ${offset}
      `);

      if (!rows.length) break;
      for (const r of rows) {
        const s = r.Split || r;
        if (!s || !s.ROWID) continue;
        // duplicate protection
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
    const inClause = isinList.map((i) => `'${i}'`).join(",");

    const priceRows = await zcql.executeZCQLQuery(`
      SELECT ISIN, ClsPric, TradDt
      FROM Bhav_Copy
      WHERE ISIN IN (${inClause})
        AND TradDt <= '${priceDate}'
      ORDER BY ISIN ASC, TradDt DESC
    `);

    for (const r of priceRows) {
      const row = r.Bhav_Copy || r;
      if (!priceMap[row.ISIN]) {
        priceMap[row.ISIN] = row.ClsPric || 0;
      }
    }
  }

  /* ================= FINAL FIFO ================= */

  const result = [];

  console.log("split are : ", splitByISIN);

  for (const key of Object.keys(holdingsMap)) {
    const fifo = runFifoEngine(
      txByISIN[key],
      bonusByISIN[key] || [],
      splitByISIN[key] || [],
      true
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

  return result.sort((a, b) => a.stockName.localeCompare(b.stockName));
};
