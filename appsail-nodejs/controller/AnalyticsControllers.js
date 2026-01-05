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
  const uniqueSecurityCodes = new Set();
  let offset = 0;

  while (true) {
    const rows = await zcql.executeZCQLQuery(`
      SELECT Security_Name, Security_code, Tran_Type, QTY, TRANDATE, NETRATE, Net_Amount
      FROM Transaction
      WHERE WS_Account_code = '${accountCode}'
      ${txnDateCondition}
      ORDER BY TRANDATE ASC
      LIMIT ${batchLimit} OFFSET ${offset}
    `);

    if (!rows.length) break;

    for (const r of rows) {
      const t = r.Transaction || r;
      transactions.push(t);
      if (t.Security_code) uniqueSecurityCodes.add(t.Security_code);
    }

    if (rows.length < batchLimit) break;
    offset += batchLimit;
  }

  /* ================= FETCH BONUS ================= */
  const bonuses = [];
  offset = 0;

  while (true) {
    const rows = await zcql.executeZCQLQuery(`
      SELECT SecurityName, BonusShare, ExDate
      FROM Bonus
      WHERE WS_Account_code = '${accountCode}'
      ${bonusDateCondition}
      LIMIT ${batchLimit} OFFSET ${offset}
    `);

    if (!rows.length) break;
    bonuses.push(...rows.map((r) => r.Bonus || r));
    if (rows.length < batchLimit) break;
    offset += batchLimit;
  }

  /* ================= FETCH SPLITS (SINGLE QUERY) ================= */
  const splits = [];
  const securityCodes = [...uniqueSecurityCodes];

  if (securityCodes.length) {
    const inClause = securityCodes.map((c) => `'${c}'`).join(",");

    const rows = await zcql.executeZCQLQuery(`
      SELECT Security_Code, Security_Name, Issue_Date, Ratio1, Ratio2
      FROM Split
      WHERE Security_Code IN (${inClause})
      ${splitDateCondition}
      ORDER BY Issue_Date ASC
    `);

    splits.push(...rows.map((r) => r.Split || r));
  }

  /* ================= GROUPING ================= */
  const normalize = (s = "") =>
    s
      .toUpperCase()
      .replace(/\bLIMITED\b/g, "LTD")
      .replace(/\s+/g, " ")
      .trim();

  const txByStock = {};
  const bonusByStock = {};
  const splitByStock = {};
  const holdingsMap = {};

  for (const t of transactions) {
    const stock = normalize(t.Security_Name);
    if (!txByStock[stock]) txByStock[stock] = [];
    txByStock[stock].push(t);

    if (!holdingsMap[stock]) {
      holdingsMap[stock] = {
        buy: 0,
        sell: 0,
        bonus: 0,
        securityCode: t.Security_code,
      };
    }

    const qty = Math.abs(Number(t.QTY) || 0);
    if (["BY-", "SQB", "OPI"].includes(t.Tran_Type))
      holdingsMap[stock].buy += qty;
    if (["SL+", "SQS", "OPO", "NF-"].includes(t.Tran_Type))
      holdingsMap[stock].sell += qty;
  }

  for (const b of bonuses) {
    const stock = normalize(b.SecurityName);
    if (!bonusByStock[stock]) bonusByStock[stock] = [];
    bonusByStock[stock].push(b);

    if (!holdingsMap[stock])
      holdingsMap[stock] = { buy: 0, sell: 0, bonus: 0, securityCode: "" };

    holdingsMap[stock].bonus += Number(b.BonusShare) || 0;
  }

  for (const s of splits) {
    const stock = normalize(s.Security_Name);
    if (!splitByStock[stock]) splitByStock[stock] = [];
    splitByStock[stock].push({
      issueDate: s.Issue_Date,
      ratio1: s.Ratio1,
      ratio2: s.Ratio2,
    });
  }

  /* ================= FINAL RESULT ================= */
  const result = [];

  for (const stock of Object.keys(holdingsMap)) {
    const needsFifo =
      splitByStock[stock]?.length || bonusByStock[stock]?.length;

    // if (!needsFifo) {
    //   const { buy, sell, bonus, securityCode } = holdingsMap[stock];
    //   const holding = buy - sell + bonus;
    //   if (holding <= 0) continue;

    //   result.push({
    //     stockName: stock,
    //     securityCode,
    //     currentHolding: holding,
    //     holdingValue: 0,
    //     avgPrice: 0,
    //   });
    //   continue;
    // }

    const fifo = runFifoEngine(
      txByStock[stock],
      bonusByStock[stock] || [],
      splitByStock[stock] || [],
      true
    );

    if (!fifo || fifo.holdings <= 0) continue;

    result.push({
      stockName: stock,
      securityCode: holdingsMap[stock].securityCode,
      currentHolding: fifo.holdings,
      holdingValue: fifo.holdingValue,
      avgPrice: fifo.averageCostOfHoldings,
    });
  }

  return result.sort((a, b) => a.stockName.localeCompare(b.stockName));
};
