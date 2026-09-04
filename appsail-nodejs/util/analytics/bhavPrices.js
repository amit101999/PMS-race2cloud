/** Last traded close per ISIN, read from `Bhav_Copy`. */

import { escHoldingsSql } from "./holdingsFromTable.js";

const BATCH = 250;

/** ISINs per `IN (...)` clause — same chunk size as `fetchSecurityListByIsins`. */
const ISIN_CHUNK = 200;

/**
 * How far back the batched window reaches. Wide enough to clear a long
 * weekend + public holidays, narrow enough that the window query returns
 * roughly `nIsins x tradingDays` rows rather than the whole price history.
 */
const PRICE_WINDOW_DAYS = 10;

/** Shift a yyyy-mm-dd string by whole days without a local-time round trip. */
function shiftIsoDays(isoDate, days) {
  const [y, m, d] = String(isoDate ?? "").split("-").map(Number);
  if (!y || !m || !d) return null;
  return new Date(Date.UTC(y, m - 1, d + days)).toISOString().slice(0, 10);
}

/** Open-ended lookup for one ISIN — the original per-ISIN query. */
async function fetchPriceForIsin(zcql, isin, priceDate) {
  try {
    const priceRows = await zcql.executeZCQLQuery(`
      SELECT ISIN, ClsPric, TradDt
      FROM Bhav_Copy
      WHERE ISIN = '${escHoldingsSql(isin)}'
        AND TradDt <= '${escHoldingsSql(priceDate)}'
      ORDER BY TradDt DESC
      LIMIT 1
    `);
    if (priceRows?.length) {
      const row = priceRows[0].Bhav_Copy || priceRows[0];
      return Number(row.ClsPric) || 0;
    }
  } catch (err) {
    console.error(`Error fetching price for ISIN ${isin}:`, err);
  }
  return 0;
}

/**
 * BULK: last close on or before `priceDate`, for many ISINs at once.
 *
 * Replaces a per-ISIN query loop. One batched `IN` scan over a short trailing
 * window covers every actively traded security; only the ISINs that window
 * misses — suspended or illiquid, no trade in `PRICE_WINDOW_DAYS` — fall back
 * to the open-ended per-ISIN lookup. The fallback is what keeps the result
 * identical to querying each ISIN separately: without it, a stock that has not
 * traded recently would silently price at 0 and drop out of market value.
 *
 * Same reasoning as `fetchHoldingsRowsByAccountForIsin` in holdingsFromTable.js:
 * a sequential per-row query loop passes locally and overruns the deployed
 * AppSail request budget.
 *
 * @returns {Promise<Record<string, number>>} ISIN → close price (0 if unpriced).
 */
export async function fetchLastPricesByIsin(zcql, isins, priceDate) {
  const priceMap = Object.create(null);

  const unique = [
    ...new Set(
      (isins || []).map((i) => String(i ?? "").trim()).filter(Boolean),
    ),
  ];
  if (!unique.length) return priceMap;

  const upper = escHoldingsSql(priceDate);
  const windowStart = shiftIsoDays(priceDate, -PRICE_WINDOW_DAYS);

  // Best TradDt seen so far per ISIN, so the newest close wins across pages.
  const bestDate = Object.create(null);

  if (windowStart) {
    for (let i = 0; i < unique.length; i += ISIN_CHUNK) {
      const chunk = unique.slice(i, i + ISIN_CHUNK);
      const inClause = chunk.map((x) => `'${escHoldingsSql(x)}'`).join(",");
      let offset = 0;

      while (true) {
        let batch;
        try {
          batch = await zcql.executeZCQLQuery(`
            SELECT ISIN, ClsPric, TradDt
            FROM Bhav_Copy
            WHERE ISIN IN (${inClause})
              AND TradDt <= '${upper}'
              AND TradDt >= '${escHoldingsSql(windowStart)}'
            ORDER BY TradDt DESC
            LIMIT ${BATCH} OFFSET ${offset}
          `);
        } catch (err) {
          // Leave this chunk to the per-ISIN fallback rather than zero it out.
          console.error("[fetchLastPricesByIsin] window query failed:", err.message);
          break;
        }

        if (!batch?.length) break;

        for (const r of batch) {
          const row = r.Bhav_Copy || r;
          const isin = String(row.ISIN ?? "").trim();
          if (!isin) continue;
          const tradDt = String(row.TradDt ?? "").slice(0, 10);
          if (!tradDt) continue;

          if (!bestDate[isin] || tradDt > bestDate[isin]) {
            bestDate[isin] = tradDt;
            priceMap[isin] = Number(row.ClsPric) || 0;
          }
        }

        if (batch.length < BATCH) break;
        offset += BATCH;
      }
    }
  }

  for (const isin of unique) {
    if (isin in priceMap) continue;
    priceMap[isin] = await fetchPriceForIsin(zcql, isin, priceDate);
  }

  return priceMap;
}
