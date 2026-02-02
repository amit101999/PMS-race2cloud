import { fetchBonusesForStock } from "../util/analytics/transactionHistory/bonuses.js";
import { runFifoEngine } from "../util/analytics/transactionHistory/fifo.js";
import { fetchSplitForStock } from "../util/analytics/transactionHistory/Split.js";
import { fetchStockTransactions } from "../util/analytics/transactionHistory/transactions.js";

export const getStockTransactionHistory = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) throw new Error("Catalyst missing");

    const accountCode = String(req.query.accountCode).trim();
    const isin = decodeURIComponent(req.query.isin || "").trim();

    if (!accountCode || !isin) {
      return res.status(400).json({
        message: "accountCode and isin are required",
      });
    }

    const zcql = app.zcql();

    const asOnDate = req.query.asOnDate;

    const transactions = await fetchStockTransactions({
      zcql,
      tableName: "Transaction",
      accountCode,
      // securityCode,
      isin,
      asOnDate,
    });

    const bonuses = await fetchBonusesForStock({
      zcql,
      accountCode,
      // securityCode,
      isin,
      asOnDate,
    });
    const split = await fetchSplitForStock({
      zcql,
      // securityCode,
      isin,
      tableName: "Split",
    });

    const result = runFifoEngine(transactions, bonuses, split, false);
    return res.json(result);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
};

export const getTotalBuyQty = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app missing" });
    }

    const accountCode = String(req.query.accountCode || "").trim();
    const isin = decodeURIComponent(req.query.isin || "").trim();
    const asOnDate = req.query.asOnDate;

    if (!accountCode || !isin) {
      return res.status(400).json({
        message: "accountCode and isin are required",
      });
    }

    const zcql = app.zcql();

    // ----------------------------
    // 1️⃣ FETCH TRANSACTIONS
    // ----------------------------
    const transactions = await fetchStockTransactions({
      zcql,
      tableName: "Transaction",
      accountCode,
      isin,
      asOnDate,
    });

    // ----------------------------
    // 2️⃣ FETCH BONUS
    // ----------------------------
    const bonuses = await fetchBonusesForStock({
      zcql,
      accountCode,
      isin,
      asOnDate,
    });

    // ----------------------------
    // 3️⃣ SUM ALL BUY QTY
    // ----------------------------
    // const isBuy = (t) => /^BY-|SQB|OPI/.test(String(t).toUpperCase());
        const isBuy = (t) => /^BY-|SQB/.test(String(t).toUpperCase());  // OPI excluded from buy

    let totalBuyQty = 0;

    // Transaction BUY qty
    for (const t of transactions) {
      if (isBuy(t.tranType)) {
        totalBuyQty += Math.abs(Number(t.qty) || 0);
      }
    }

    // Bonus qty (security in)
    for (const b of bonuses) {
      totalBuyQty += Number(b.bonusShare) || 0;
    }

    return res.status(200).json({
      accountCode,
      isin,
      asOnDate: asOnDate || null,
      totalBuyQty,
    });
  } catch (err) {
    console.error("[getTotalBuyQty]", err);
    return res.status(500).json({
      message: "Failed to calculate total buy quantity",
      error: err.message,
    });
  }
};

export const getTotalSellQty = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app missing" });
    }

    const accountCode = String(req.query.accountCode || "").trim();
    const isin = decodeURIComponent(req.query.isin || "").trim();
    const asOnDate = req.query.asOnDate;

    if (!accountCode || !isin) {
      return res.status(400).json({
        message: "accountCode and isin are required",
      });
    }

    const zcql = app.zcql();

    // ----------------------------
    // 1️⃣ FETCH ALL TRANSACTIONS
    // ----------------------------
    const transactions = await fetchStockTransactions({
      zcql,
      tableName: "Transaction",
      accountCode,
      isin,
      asOnDate,
    });

    // ----------------------------
    // 2️⃣ SUM ALL SELL QTY
    // ----------------------------
    const isSell = (t) => /^SL\+|SQS|OPO|NF-/.test(String(t).toUpperCase());

    let totalSellQty = 0;

    for (const t of transactions) {
      if (isSell(t.tranType)) {
        totalSellQty += Math.abs(Number(t.qty) || 0);
      }
    }

    return res.status(200).json({
      accountCode,
      isin,
      totalSellQty,
      asOnDate: asOnDate || null,
    });
  } catch (err) {
    console.error("[getTotalSellQty]", err);
    return res.status(500).json({
      message: "Failed to calculate total sell quantity",
      error: err.message,
    });
  }
};
