import {
  fetchHoldingsRowsForAccountIsin,
  mapHoldingsRowToStockHistoryDto,
  isHoldingsBuyType,
  isHoldingsSellType,
} from "../util/analytics/holdingsFromTable.js";

export const getStockTransactionHistory = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) throw new Error("Catalyst missing");

    const accountCode = String(req.query.accountCode || "").trim();
    const isin = decodeURIComponent(req.query.isin || "").trim();

    if (!accountCode || !isin) {
      return res.status(400).json({
        message: "accountCode and isin are required",
      });
    }

    const zcql = app.zcql();
    const asOnDate = req.query.asOnDate;

    const rows = await fetchHoldingsRowsForAccountIsin(
      zcql,
      accountCode,
      isin,
      asOnDate,
    );

    const result = rows.map((h) => mapHoldingsRowToStockHistoryDto(h, isin));
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
    const rows = await fetchHoldingsRowsForAccountIsin(
      zcql,
      accountCode,
      isin,
      asOnDate,
    );

    let totalBuyQty = 0;
    for (const h of rows) {
      const typ = h.TYPE || h.type;
      if (isHoldingsBuyType(typ) || String(typ || "").toUpperCase() === "BONUS") {
        totalBuyQty += Math.abs(Number(h.QUANTITY) || 0);
      }
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
    const rows = await fetchHoldingsRowsForAccountIsin(
      zcql,
      accountCode,
      isin,
      asOnDate,
    );

    let totalSellQty = 0;
    for (const h of rows) {
      const typ = h.TYPE || h.type;
      if (isHoldingsSellType(typ)) {
        totalSellQty += Math.abs(Number(h.QUANTITY) || 0);
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
