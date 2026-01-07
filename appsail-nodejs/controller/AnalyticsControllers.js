import { getAllAccountCodesFromDatabase } from "../util/allAccountCodes.js";
import { fetchBonusesForStock } from "../util/bonuses.js";
import { runFifoEngine } from "../util/fifo.js";
import { fetchStockTransactions } from "../util/transactions.js";
const txByStock = {};
const bonusByStock = {};

export const getAllAccountCodes = async (req, res) => {
  try {
    const zohoCatalyst = req.catalystApp;
    let zcql = zohoCatalyst.zcql();
    let tableName = "clientIds";
    const cliendIds = await getAllAccountCodesFromDatabase(zcql, tableName);
    return res.status(200).json({ data: cliendIds });
  } catch (error) {
    console.log("Error in fething data", error);
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

    txnDateCondition = `AND TRANDATE < '${nextDayStr}'`;
    bonusDateCondition = `AND ExDate < '${nextDayStr}'`;
    splitDateCondition = `AND Issue_Date < '${nextDayStr}'`;
  }

  /* ================= FETCH TRANSACTIONS ================= */
  const transactions = [];
  const uniqueISINs = new Set();
  let offset = 0;

  while (true) {
    const rows = await zcql.executeZCQLQuery(`
      SELECT Security_Name, Security_code, Tran_Type, QTY, TRANDATE,
             NETRATE, Net_Amount, ISIN, ROWID
      FROM Transaction
      WHERE WS_Account_code = '${accountCode}'
      ${txnDateCondition}
      ORDER BY TRANDATE ASC, ROWID ASC
      LIMIT ${batchLimit} OFFSET ${offset}
    `);

    if (!rows.length) break;

    for (const r of rows) {
      const t = r.Transaction || r;
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
      ORDER BY ExDate ASC, ROWID ASC
      LIMIT ${batchLimit} OFFSET ${offset}
    `);

    if (!rows.length) break;
    bonuses.push(...rows.map((r) => r.Bonus || r));

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
        SELECT Security_Code, Security_Name, Issue_Date,
               Ratio1, Ratio2, ISIN, ROWID
        FROM Split
        WHERE ISIN IN (${inClause})
        ${splitDateCondition}
        ORDER BY Issue_Date ASC, ROWID ASC
        LIMIT ${batchLimit} OFFSET ${offset}
      `);

      if (!rows.length) break;
      splits.push(...rows.map((r) => r.Split || r));

      if (rows.length < batchLimit) break;
      offset += batchLimit;
    }
  }

  /* ================= GROUP BY ISIN ================= */
  const txByISIN = {};
  const bonusByISIN = {};
  const splitByISIN = {};
  const holdingsMap = {};

  /* ---- TRANSACTIONS ---- */
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
        buy: 0,
        sell: 0,
        bonus: 0,
      };
    }

    const qty = Math.abs(Number(t.QTY) || 0);
    if (["BY-", "SQB", "OPI"].includes(t.Tran_Type))
      holdingsMap[key].buy += qty;

    if (["SL+", "SQS", "OPO", "NF-"].includes(t.Tran_Type))
      holdingsMap[key].sell += qty;
  }

  /* ---- BONUS ---- */
  for (const b of bonuses) {
    if (!b.ISIN) continue;
    const key = b.ISIN;

    if (!bonusByISIN[key]) bonusByISIN[key] = [];
    bonusByISIN[key].push(b);

    if (!holdingsMap[key]) {
      holdingsMap[key] = {
        stockName: b.SecurityName,
        securityCode: "",
        isin: b.ISIN,
        buy: 0,
        sell: 0,
        bonus: 0,
      };
    }

    holdingsMap[key].bonus += Number(b.BonusShare) || 0;
  }

  /* ---- SPLITS ---- */
  for (const s of splits) {
    if (!s.ISIN) continue;
    const key = s.ISIN;

    if (!splitByISIN[key]) splitByISIN[key] = [];
    splitByISIN[key].push({
      issueDate: s.Issue_Date,
      ratio1: s.Ratio1,
      ratio2: s.Ratio2,
      isin: s.ISIN,
    });
  }

  /* ================= FINAL FIFO ================= */
  const result = [];

  for (const key of Object.keys(holdingsMap)) {
    const fifo = runFifoEngine(
      txByISIN[key],
      bonusByISIN[key] || [],
      splitByISIN[key] || [],
      true
    );

    if (!fifo || fifo.holdings <= 0) continue;

    result.push({
      isin: key,
      stockName: holdingsMap[key].stockName,
      securityCode: holdingsMap[key].securityCode,
      currentHolding: fifo.holdings,
      holdingValue: fifo.holdingValue,
      avgPrice: fifo.averageCostOfHoldings,
    });
  }

  return result.sort((a, b) => a.stockName.localeCompare(b.stockName));
};
