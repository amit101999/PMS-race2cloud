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
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app missing" });
    }

    const accountCode = req.query.accountCode;
    if (!accountCode) {
      return res.status(400).json({ message: "Valid accountCode required" });
    }
    const asOnDate = req.query.asOnDate;
    const zcql = app.zcql();
    const batchLimit = 250;

    let txnDateCondition = "";
    let bonusDateCondition = "";

    if (asOnDate && /^\d{4}-\d{2}-\d{2}$/.test(asOnDate)) {
      const nextDay = new Date(asOnDate);
      nextDay.setDate(nextDay.getDate() + 1);
      const nextDayStr = nextDay.toISOString().split("T")[0];

      txnDateCondition = `AND TRANDATE < '${nextDayStr}'`;
      bonusDateCondition = `AND ExDate < '${nextDayStr}'`;
    }

    const transactions = [];
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
      transactions.push(...rows.map((r) => r.Transaction || r));
      if (rows.length < batchLimit) break;
      offset += batchLimit;
    }

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

    const normalize = (s = "") =>
      s
        .toUpperCase()
        .replace(/\bLIMITED\b/g, "LTD")
        .replace(/\s+/g, " ")
        .trim();

    const txByStock = {};
    const bonusByStock = {};
    const holdingsMap = {};

    transactions.forEach((t) => {
      const stock = normalize(t.Security_Name);
      if (!stock) return;

      if (!txByStock[stock]) txByStock[stock] = [];
      txByStock[stock].push(t);

      if (!holdingsMap[stock]) {
        holdingsMap[stock] = {
          buy: 0,
          sell: 0,
          bonus: 0,
          securityCode: t.Security_code || "",
        };
      }

      const qty = Math.abs(Number(t.QTY) || 0);
      if (["BY-", "SQB", "OPI"].includes(t.Tran_Type))
        holdingsMap[stock].buy += qty;
      if (["SL+", "SQS", "OPO", "NF-"].includes(t.Tran_Type))
        holdingsMap[stock].sell += qty;
    });

    bonuses.forEach((b) => {
      const stock = normalize(b.SecurityName);
      if (!stock) return;

      if (!bonusByStock[stock]) bonusByStock[stock] = [];
      bonusByStock[stock].push(b);

      if (!holdingsMap[stock]) {
        holdingsMap[stock] = { buy: 0, sell: 0, bonus: 0, securityCode: "" };
      }

      holdingsMap[stock].bonus += Number(b.BonusShare) || 0;
    });
    const split = [];
    const result = [];

    for (const stock of Object.keys(holdingsMap)) {
      const { buy, sell, bonus, securityCode } = holdingsMap[stock];
      const currentHolding = buy - sell + bonus;
      if (currentHolding <= 0) continue;

      const fifo = runFifoEngine(
        txByStock[stock] || [],
        bonusByStock[stock] || [],
        split || [],
        true
      );
      if (fifo.holdingValue <= 0) continue;

      result.push({
        stockName: stock,
        securityCode,
        currentHolding,
        holdingValue: fifo.holdingValue,
        avgPrice: fifo.averageCostOfHoldings,
      });
    }

    result.sort((a, b) => a.stockName.localeCompare(b.stockName));
    return res.status(200).json(result);
  } catch (err) {
    console.error("[getHoldingsSummarySimple]", err);
    return res.status(500).json({
      message: "Failed",
      error: err.message,
    });
  }
};
