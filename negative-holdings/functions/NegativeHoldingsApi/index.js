"use strict";

const catalyst = require("zcatalyst-sdk-node");
const { getAllAccountCodesFromDatabase } = require("./allAccountCodes.js");
const {
  fetchHoldingsRowsPaged,
  fetchHoldingsRowsForAccountIsin,
  fetchSecurityListByIsins,
  isHoldingsBuyType,
  isHoldingsSellType,
  mapHoldingsRowToStockHistoryDto,
} = require("./holdingsRead.js");
const { findNegativeHoldings, walkShareRows } = require("./negativeHoldings.js");

function sendJson(res, statusCode, data) {
  res.writeHead(statusCode, {
    "Content-Type": "application/json",
    // The page has no login, so at least keep this out of search engines.
    "X-Robots-Tag": "noindex, nofollow",
  });
  res.end(JSON.stringify(data));
}

/** Returns the date, "" when none was sent, or null when it isn't a real date. */
function readAsOnDate(query) {
  const value = String(query.get("asOnDate") || "").trim();
  if (!value) return "";
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime()) || date.toISOString().slice(0, 10) !== value) return null;
  return value;
}

async function getAccounts(zcql, res) {
  const data = await getAllAccountCodesFromDatabase(zcql, "clientIds");
  sendJson(res, 200, { data });
}

// One account at a time, so each request stays inside the function's 30-second limit.
async function getNegativeHoldings(zcql, query, res) {
  const accountCode = String(query.get("accountCode") || "").trim();
  if (!accountCode) {
    return sendJson(res, 400, { message: "accountCode is required" });
  }
  const asOnDate = readAsOnDate(query);
  if (asOnDate === null) {
    return sendJson(res, 400, { message: "asOnDate must be a real date in YYYY-MM-DD format" });
  }

  const rows = await fetchHoldingsRowsPaged(zcql, accountCode, asOnDate);
  const shares = findNegativeHoldings(rows);
  const names = await fetchSecurityListByIsins(
    zcql,
    shares.map((share) => share.isin).filter(Boolean),
  );
  sendJson(res, 200, {
    accountCode,
    shares: shares.map((share) => ({
      ...share,
      securityName: names[share.isin]?.securityName || "",
    })),
  });
}

// Same data as the main backend's Analytics drill-down, plus shortBy and realHolding.
async function getShareHistory(zcql, query, res) {
  const accountCode = String(query.get("accountCode") || "").trim();
  const isin = String(query.get("isin") || "").trim();
  if (!accountCode || !isin) {
    return sendJson(res, 400, { message: "accountCode and isin are required" });
  }
  const asOnDate = readAsOnDate(query);
  if (asOnDate === null) {
    return sendJson(res, 400, { message: "asOnDate must be a real date in YYYY-MM-DD format" });
  }

  const rows = await fetchHoldingsRowsForAccountIsin(zcql, accountCode, isin, asOnDate);

  let totalBuyQty = 0;
  let totalSellQty = 0;
  for (const h of rows) {
    const quantity = Math.abs(Number(h.QUANTITY) || 0);
    if (isHoldingsBuyType(h.TYPE) || String(h.TYPE || "").toUpperCase() === "BONUS") {
      totalBuyQty += quantity;
    }
    if (isHoldingsSellType(h.TYPE)) {
      totalSellQty += quantity;
    }
  }

  const result = walkShareRows(rows).rows.map(({ row, shortBy, realHolding }) => ({
    ...mapHoldingsRowToStockHistoryDto(row, isin),
    shortBy,
    realHolding,
  }));
  sendJson(res, 200, { rows: result, totalBuyQty, totalSellQty });
}

module.exports = async (req, res) => {
  // For testing the page from this computer. A hosted page's domain goes in the
  // console under Authorized Domains instead, and Catalyst adds its own header.
  // Setting a header for that domain here too would make the browser reject it.
  const origin = req.headers.origin || "";
  if (/^http:\/\/localhost(:\d+)?$/.test(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
  }

  if (req.method !== "GET") {
    return sendJson(res, 405, { message: "Only GET is allowed" });
  }

  const url = new URL(req.url, "http://function.local");
  // Catalyst includes /execute in the path it passes to the function.
  const route = url.pathname.replace(/^\/execute/, "").replace(/\/$/, "");

  try {
    const zcql = catalyst.initialize(req, { scope: "admin" }).zcql();

    if (route === "/accounts") return await getAccounts(zcql, res);
    if (route === "/negative-holdings") return await getNegativeHoldings(zcql, url.searchParams, res);
    if (route === "/share-history") return await getShareHistory(zcql, url.searchParams, res);

    return sendJson(res, 404, { message: "Not found" });
  } catch (err) {
    // Logged here only: the function is public, so details stay out of the response.
    console.error(`${route} failed:`, err);
    return sendJson(res, 500, { message: "The request failed" });
  }
};
