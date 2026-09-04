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

/**
 * Open-ended lookup for one ISIN — the original per-ISIN query.
 * Retained as the per-ISIN recovery path when a batched query fails.
 */
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
 * BULK second stage: newest close per ISIN with no date floor.
 *
 * Replaces the per-ISIN fallback loop. Each pass asks only for the ISINs still
 * unresolved and keeps the first row it sees for each: the scan is ordered
 * `TradDt DESC` over exactly that pending set, so an ISIN's first row is its
 * newest — the same row the single-ISIN `LIMIT 1` query would have returned.
 *
 * Dropping resolved ISINs from the next `IN (...)` clause is what keeps the
 * scan moving. Their rows stop competing for space in the page, so each pass
 * reads from the newest end of a strictly smaller result set and walks back
 * towards the stale ISINs instead of re-reading history the caller already has.
 * That is also why no OFFSET is needed — and OFFSET would page unstably over
 * the non-unique `TradDt` ordering anyway.
 *
 * Terminates: the first row of any non-empty page belongs to a pending ISIN and
 * resolves it, so every pass removes at least one; an empty page means the
 * remainder have no `Bhav_Copy` row at all and correctly stay at 0.
 *
 * Same shape as the batching in `buildVirtualToActualMap`: chunked `IN (...)`
 * reads, with the single-key lookup kept only for error recovery.
 */
async function resolvePricesOpenEnded(zcql, isins, priceDate, priceMap) {
  const upper = escHoldingsSql(priceDate);

  for (let i = 0; i < isins.length; i += ISIN_CHUNK) {
    const pending = new Set(isins.slice(i, i + ISIN_CHUNK));

    while (pending.size) {
      const inClause = [...pending]
        .map((x) => `'${escHoldingsSql(x)}'`)
        .join(",");

      let batch;
      try {
        batch = await zcql.executeZCQLQuery(`
          SELECT ISIN, ClsPric, TradDt
          FROM Bhav_Copy
          WHERE ISIN IN (${inClause})
            AND TradDt <= '${upper}'
          ORDER BY TradDt DESC
          LIMIT ${BATCH}
        `);
      } catch (err) {
        console.error(
          "[fetchLastPricesByIsin] open-ended batch failed, falling back:",
          err.message,
        );
        // Price this chunk the old way rather than reporting 0 for all of it.
        for (const isin of pending) {
          priceMap[isin] = await fetchPriceForIsin(zcql, isin, priceDate);
        }
        break;
      }

      // Nothing on or before `priceDate` for anything still pending.
      if (!batch?.length) break;

      let resolved = 0;
      for (const r of batch) {
        const row = r.Bhav_Copy || r;
        const isin = String(row.ISIN ?? "").trim();
        // Skip an ISIN's older rows — its newest already won above.
        if (!pending.has(isin)) continue;
        priceMap[isin] = Number(row.ClsPric) || 0;
        pending.delete(isin);
        resolved++;
      }

      // Defensive: a page carrying nothing from the pending set would
      // otherwise re-issue the identical query forever.
      if (resolved === 0) break;
    }
  }
}

/**
 * BULK: last close on or before `priceDate`, for many ISINs at once.
 *
 * Replaces a per-ISIN query loop. One batched `IN` scan over a short trailing
 * window covers every actively traded security; only the ISINs that window
 * misses — suspended or illiquid, no trade in `PRICE_WINDOW_DAYS` — go on to
 * `resolvePricesOpenEnded`, which is batched the same way. That second stage is
 * what keeps the result identical to querying each ISIN separately: without it,
 * a stock that has not traded recently would silently price at 0 and drop out
 * of market value.
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

  // Seed 0 before the second stage so every requested ISIN has an entry even
  // when it has no Bhav_Copy row at all — callers read priceMap[isin] directly.
  const missing = unique.filter((isin) => !(isin in priceMap));
  if (missing.length) {
    for (const isin of missing) priceMap[isin] = 0;
    await resolvePricesOpenEnded(zcql, missing, priceDate, priceMap);
  }

  return priceMap;
}
