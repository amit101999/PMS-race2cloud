const {
  fetchHoldingsRowsPaged,
  fetchSecurityListByIsins,
  fetchLatestPricesByIsins,
  rollupLastSnapshotByIsin,
} = require("./holdingsFromTable.js");

/**
 * Per-account holdings for bulk export — from materialised `Holdings` + Bhav_Copy.
 * Matches AppSail `exportHolding/analyticsController.js` and dashboard summary.
 *
 * @param {{ catalystApp: object, accountCode: string, asOnDate?: string, sharedPriceMap?: Record<string, number> }} opts
 */
exports.calculateHoldingsSummary = async ({
  catalystApp,
  accountCode,
  asOnDate,
  sharedPriceMap,
}) => {
  const zcql = catalystApp.zcql();

  const rows = await fetchHoldingsRowsPaged(zcql, accountCode, asOnDate, "");
  const snapshots = rollupLastSnapshotByIsin(rows);
  const isinList = snapshots.map((s) => s.isin);

  const metaByIsin = await fetchSecurityListByIsins(zcql, isinList);

  const todayStr = new Date().toISOString().split("T")[0];
  const priceDate = asOnDate && asOnDate < todayStr ? asOnDate : todayStr;

  const priceMap = sharedPriceMap || {};
  await fetchLatestPricesByIsins(zcql, isinList, priceDate, priceMap);

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
