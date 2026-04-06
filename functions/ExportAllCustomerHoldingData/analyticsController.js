const { runFifoEngine } = require("./fifo");

const isBuyType = (type) => /^BY-|SQB|OPI/i.test(String(type || ""));

const getEffectiveDate = (t) => {
  const setDate = t.SETDATE || t.setdate;
  const tradeDate = t.TRANDATE || t.trandate;
  return isBuyType(t.Tran_Type || t.tranType)
    ? setDate || tradeDate
    : tradeDate || setDate;
};

// exports.calculateHoldingsSummary = async ({
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
//       SELECT Security_Name, Security_code, Tran_Type, QTY, SETDATE,
//              NETRATE, Net_Amount, ISIN, ROWID
//       FROM Transaction
//       WHERE WS_Account_code = '${accountCode}'
//       ${txnDateCondition}
//       ORDER BY SETDATE ASC, ROWID ASC
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
//       splits.push(...rows.map((r) => r.Split || r));

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
//     if (!s.ISIN) continue;
//     if (!splitByISIN[s.ISIN]) splitByISIN[s.ISIN] = [];
//     splitByISIN[s.ISIN].push(s);
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
exports.calculateHoldingsSummary = async ({
  catalystApp,
  accountCode,
  asOnDate,
  sharedPriceMap,
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

    txnDateCondition = `AND (TRANDATE < '${nextDayStr}' OR SETDATE < '${nextDayStr}')`;
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
        SELECT Security_Name, Security_code, Tran_Type, QTY, SETDATE, TRANDATE,
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

  if (asOnDate && /^\d{4}-\d{2}-\d{2}$/.test(asOnDate)) {
    const nextDay = new Date(asOnDate);
    nextDay.setDate(nextDay.getDate() + 1);
    const cutoff = nextDay.toISOString().split("T")[0];

    const beforeCount = transactions.length;
    const filtered = [];
    for (let i = 0; i < transactions.length; i++) {
      const effectiveDate = getEffectiveDate(transactions[i]);
      if (!effectiveDate || effectiveDate < cutoff) {
        filtered.push(transactions[i]);
      }
    }
    transactions.length = 0;
    for (const t of filtered) transactions.push(t);

    console.log(`Effective-date filter: ${beforeCount} → ${transactions.length} transactions (cutoff ${cutoff})`);
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

  /* ================= FETCH DEMERGER_RECORD ================= */
  const demergerRows = [];
  {
    let dateCondition = "";
    if (asOnDate && /^\d{4}-\d{2}-\d{2}$/.test(asOnDate)) {
      const nd = new Date(asOnDate);
      nd.setDate(nd.getDate() + 1);
      const nds = nd.toISOString().split("T")[0];
      dateCondition = ` AND (TRANDATE < '${nds}' OR SETDATE < '${nds}')`;
    }
    const seenDem = new Set();
    let dOff = 0;
    while (true) {
      try {
        const batch = await zcql.executeZCQLQuery(`
          SELECT *
          FROM Demerger_Record
          WHERE WS_Account_code = '${accountCode}'
          ${dateCondition}
          ORDER BY TRANDATE ASC, ROWID ASC
          LIMIT ${batchLimit} OFFSET ${dOff}
        `);
        if (!batch || !batch.length) break;
        for (const row of batch) {
          const d = row.Demerger_Record || row;
          const rid = d.ROWID;
          if (rid != null && seenDem.has(rid)) continue;
          if (rid != null) seenDem.add(rid);
          if (String(d.Tran_Type || d.tran_type || "").toUpperCase() === "DEMERGER") {
            demergerRows.push(d);
            if (d.ISIN) uniqueISINs.add(d.ISIN);
          }
        }
        if (batch.length < batchLimit) break;
        dOff += batchLimit;
      } catch (err) {
        console.error(`Error fetching demerger records for ${accountCode} at offset ${dOff}:`, err);
        break;
      }
    }
  }

  /* ================= FETCH MERGER ================= */
  const mergerRows = [];
  {
    let dateCondition = "";
    if (asOnDate && /^\d{4}-\d{2}-\d{2}$/.test(asOnDate)) {
      const nd = new Date(asOnDate);
      nd.setDate(nd.getDate() + 1);
      const nds = nd.toISOString().split("T")[0];
      dateCondition = ` AND (TRANDATE < '${nds}' OR SETDATE < '${nds}')`;
    }
    const seenMer = new Set();
    let mOff = 0;
    while (true) {
      try {
        const batch = await zcql.executeZCQLQuery(`
          SELECT *
          FROM Merger
          WHERE WS_Account_code = '${accountCode}'
          ${dateCondition}
          ORDER BY TRANDATE ASC, ROWID ASC
          LIMIT ${batchLimit} OFFSET ${mOff}
        `);
        if (!batch || !batch.length) break;
        for (const row of batch) {
          const m = row.Merger || row;
          const rid = m.ROWID;
          if (rid != null && seenMer.has(rid)) continue;
          if (rid != null) seenMer.add(rid);
          if (String(m.Tran_Type || m.tran_type || "").toUpperCase() === "MERGER") {
            mergerRows.push(m);
            if (m.ISIN) uniqueISINs.add(m.ISIN);
          }
        }
        if (batch.length < batchLimit) break;
        mOff += batchLimit;
      } catch (err) {
        console.error(`Error fetching merger records for ${accountCode} at offset ${mOff}:`, err);
        break;
      }
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
  const demergerByISIN = {};
  const mergerByISIN = {};
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

  for (const d of demergerRows) {
    if (!d.ISIN) continue;
    if (!demergerByISIN[d.ISIN]) demergerByISIN[d.ISIN] = [];
    demergerByISIN[d.ISIN].push(d);
    if (!holdingsMap[d.ISIN]) {
      holdingsMap[d.ISIN] = {
        stockName: d.Security_Name,
        securityCode: d.Security_Code,
        isin: d.ISIN,
      };
    }
  }

  for (const m of mergerRows) {
    if (!m.ISIN) continue;
    if (!mergerByISIN[m.ISIN]) mergerByISIN[m.ISIN] = [];
    mergerByISIN[m.ISIN].push(m);
    if (!holdingsMap[m.ISIN]) {
      holdingsMap[m.ISIN] = {
        stockName: m.Security_Name,
        securityCode: m.SecurityCode,
        isin: m.ISIN,
      };
    }
  }

  /* ================= FETCH BHAV PRICES ================= */

  const todayStr = new Date().toISOString().split("T")[0];
  const priceDate = asOnDate && asOnDate < todayStr ? asOnDate : todayStr;

  const priceMap = sharedPriceMap || {};

  if (isinList.length) {
    const missingIsins = isinList.filter((isin) => !(isin in priceMap));

    for (const isin of missingIsins) {
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

  const mergedAwayIsins = new Set();
  for (const m of mergerRows) {
    const oldIsin = m.OldISIN || m.oldIsin || "";
    if (oldIsin) mergedAwayIsins.add(oldIsin);
  }

  const result = [];

  for (const key of Object.keys(holdingsMap)) {
    if (mergedAwayIsins.has(key)) continue;

    const fifo = runFifoEngine(
      txByISIN[key] || [],
      bonusByISIN[key] || [],
      splitByISIN[key] || [],
      true,
      demergerByISIN[key] || [],
      mergerByISIN[key] || [],
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
