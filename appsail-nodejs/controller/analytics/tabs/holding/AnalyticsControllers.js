import { getAllAccountCodesFromDatabase } from "../../../../util/allAccountCodes.js";
import {
  escHoldingsSql,
  fetchHoldingsRowsByAccountForIsin,
  fetchHoldingsRowsPaged,
  fetchSecurityListByIsins,
  rollupLastSnapshotByIsin,
} from "../../../../util/analytics/holdingsFromTable.js";
import { fetchLastPricesByIsin } from "../../../../util/analytics/bhavPrices.js";
import { buildVirtualToActualMap } from "../../../../util/mapVirtualToActualCodes.js";

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

/**
 * Distinct `Actual_Code` values from `clientIds`. Used by the consolidated
 * holdings export, where one actual code groups several virtual codes.
 * Returned in the same shape as `getAllAccountCodes` so the UI hook can parse
 * either list with the same code.
 */
export const getAllActualCodes = async (req, res) => {
  try {
    const zcql = req.catalystApp.zcql();
    const seen = new Set();
    const codes = [];
    let offset = 0;
    const LIMIT = 270;

    while (true) {
      const batch = await zcql.executeZCQLQuery(
        `SELECT Actual_Code FROM clientIds LIMIT ${LIMIT} OFFSET ${offset}`,
      );
      if (!batch?.length) break;

      for (const r of batch) {
        const row = r.clientIds || r;
        const code = String(row.Actual_Code ?? "").trim();
        if (code && !seen.has(code)) {
          seen.add(code);
          codes.push(code);
        }
      }

      if (batch.length < LIMIT) break;
      offset += LIMIT;
    }

    codes.sort((a, b) => a.localeCompare(b));
    return res.status(200).json({
      data: codes.map((code) => ({ clientIds: { WS_Account_code: code } })),
    });
  } catch (error) {
    console.log("Error fetching actual codes", error);
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

export const getHoldingsByIsin = async (req, res) => {
  try {
    const isin = String(req.query.isin ?? "").trim();
    if (!isin) return res.status(400).json({ message: "isin required" });

    const data = await calculateHoldingsByIsin({
      catalystApp: req.catalystApp,
      isin,
      asOnDate: req.query.asOnDate,
    });

    return res.json(data);
  } catch (err) {
    console.error("ISIN holdings summary error:", err);
    res.status(500).json({ message: "Failed", error: err.message });
  }
};

/**
 * Aggregate current positions from the materialised `Holdings` table + Bhav prices.
 */
export const calculateHoldingsSummary = async ({
  catalystApp,
  accountCode,
  asOnDate,
}) => {
  const zcql = catalystApp.zcql();

  const rows = await fetchHoldingsRowsPaged(zcql, accountCode, asOnDate, "");

  // Show the saved Holdings values as they are (HOLDING, WAP, HOLDING_VALUE),
  // never re-derived, so every screen shows exactly what the holdings
  // calculation saved. Re-deriving from the saved PRICE column (4 decimals only)
  // could be a few paise off. Rows are already cut at the as-on date and in saved
  // order, so the last row per ISIN is the position on that date.
  const snapshots = rollupLastSnapshotByIsin(rows);
  const isinList = snapshots.map((s) => s.isin);

  const metaByIsin = await fetchSecurityListByIsins(zcql, isinList);

  const todayStr = new Date().toISOString().split("T")[0];
  const priceDate = asOnDate && asOnDate < todayStr ? asOnDate : todayStr;

  // One batched read instead of a query per ISIN — see `fetchLastPricesByIsin`.
  const priceMap = await fetchLastPricesByIsin(zcql, isinList, priceDate);

  const result = [];
  for (const { isin, lastRow } of snapshots) {
    const hold = Number(lastRow.HOLDING) || 0;
    const wap = Number(lastRow.WAP) || 0;
    const hv = Number(lastRow.HOLDING_VALUE) || hold * wap;
    const meta = metaByIsin[isin] || {};
    const lastPrice = priceMap[isin] || 0;

    result.push({
      isin,
      stockName: meta.securityName || isin,
      securityCode: meta.securityCode || "",
      currentHolding: hold,
      avgPrice: wap,
      holdingValue: hv,
      lastPrice,
      marketValue: hold * lastPrice,
    });
  }

  return result.sort((a, b) =>
    (a.stockName || "").localeCompare(b.stockName || ""),
  );
};

export const calculateHoldingsByIsin = async ({
  catalystApp,
  isin,
  asOnDate,
}) => {
  const zcql = catalystApp.zcql();
  const isinTrim = String(isin ?? "").trim();
  if (!isinTrim) return [];

  // Already effective-date filtered and in FIFO order per account. Uses keyset
  // paging — ZCQL's 1-based OFFSET repeated a row at every page boundary.
  const rowsByAccount = await fetchHoldingsRowsByAccountForIsin(zcql, isinTrim, asOnDate);

  const latestByAccount = new Map();
  for (const [accountCode, accRows] of rowsByAccount) {
    latestByAccount.set(accountCode, accRows[accRows.length - 1]);
  }

  if (latestByAccount.size === 0) return [];

  // The saved last row is shown as it is (see calculateHoldingsSummary); closed
  // positions (HOLDING 0) are skipped in the result loop below.

  const metaByIsin = await fetchSecurityListByIsins(zcql, [isinTrim]);
  const meta = metaByIsin[isinTrim] || {};
  const virtualToActual = await buildVirtualToActualMap(
    zcql,
    [...latestByAccount.keys()],
  );

  const todayStr = new Date().toISOString().split("T")[0];
  const priceDate = asOnDate && asOnDate < todayStr ? asOnDate : todayStr;
  let lastPrice = 0;

  try {
    const priceRows = await zcql.executeZCQLQuery(`
      SELECT ISIN, ClsPric, TradDt
      FROM Bhav_Copy
      WHERE ISIN = '${escHoldingsSql(isinTrim)}'
        AND TradDt <= '${escHoldingsSql(priceDate)}'
      ORDER BY TradDt DESC
      LIMIT 1
    `);
    if (priceRows?.length) {
      const row = priceRows[0].Bhav_Copy || priceRows[0];
      lastPrice = Number(row.ClsPric) || 0;
    }
  } catch (err) {
    console.error(`Error fetching price for ISIN ${isinTrim}:`, err);
  }

  const result = [];
  for (const [virtualCode, row] of latestByAccount) {
    const qty = Number(row.HOLDING) || 0;
    if (qty <= 1e-6) continue;

    const avgPrice = Number(row.WAP) || 0;
    const holdingValue = Number(row.HOLDING_VALUE) || qty * avgPrice;

    result.push({
      isin: isinTrim,
      stockName: meta.securityName || isinTrim,
      securityCode: meta.securityCode || "",
      virtualCode,
      actualCode: virtualToActual.get(virtualCode) || "",
      currentHolding: qty,
      avgPrice,
      holdingValue,
      lastPrice,
      marketValue: qty * lastPrice,
    });
  }

  return result.sort((a, b) =>
    (a.virtualCode || "").localeCompare(b.virtualCode || ""),
  );
};
