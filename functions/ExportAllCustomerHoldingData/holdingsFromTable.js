/**
 * Read materialised `Holdings` — mirrors appsail-nodejs/util/analytics/holdingsFromTable.js
 * (CommonJS for Catalyst function package).
 */

const { queryWithTimeout } = require("./zcqlTimeout.js");

const BATCH = 250;
const HOLDINGS_FIFO_ORDER_BY_SQL = "CREATEDTIME ASC, ROWID ASC";

/**
 * How many calendar days back from the price date the bulk `ISIN IN (...)`
 * price query is allowed to look. Kept small so a single ISIN's history can't
 * fill the ZCQL row cap and starve the other ISINs in the same chunk; ISINs
 * with no trade in this window fall back to an unbounded per-ISIN lookup.
 */
const PRICE_LOOKBACK_DAYS = 15;

/** ISIN count per bulk price query — `PRICE_ISIN_CHUNK * ~11 trading days` stays well under the ZCQL cap. */
const PRICE_ISIN_CHUNK = 100;

function escHoldingsSql(s) {
  return String(s ?? "").replace(/'/g, "''");
}

function holdingsDateFilterClause(asOnDate) {
  if (!asOnDate || !/^\d{4}-\d{2}-\d{2}$/.test(String(asOnDate).trim())) return "";
  const nextDay = new Date(asOnDate);
  nextDay.setDate(nextDay.getDate() + 1);
  const cutoff = nextDay.toISOString().split("T")[0];
  return ` AND (SETTLEMENT_DATE < '${escHoldingsSql(cutoff)}' OR TRANSACTION_DATE < '${escHoldingsSql(cutoff)}')`;
}

function isHoldingsBuyType(type) {
  return /^BY-|SQB|OPI/i.test(String(type || ""));
}

function isHoldingsOpiType(type) {
  return String(type || "").trim().toUpperCase() === "OPI";
}

/**
 * The date a Holdings row should be filtered on for an as-on-date view.
 *
 * SETTLEMENT-DATE RULE (disabled for BY-/SQB buys — kept for future restore):
 * Buys used to settle on SETTLEMENT_DATE, everything else took effect on
 * TRANSACTION_DATE (mirrors the rebuild job's old `getEffectiveDate`):
 *
 *   return isHoldingsBuyType(row.TYPE) ? setD || txD : txD || setD;
 *
 * Current rule: effective date is Transaction Date for everything EXCEPT
 * OPI (opening/initial-position) rows, which keep using Settlement Date.
 */
function holdingsEffectiveDate(row) {
  const setD = String(row.SETTLEMENT_DATE || "").slice(0, 10);
  const txD = String(row.TRANSACTION_DATE || "").slice(0, 10);
  return isHoldingsOpiType(row.TYPE) ? setD || txD : txD || setD;
}

/**
 * All Holdings rows for an account — same ORDER BY as fifo materialisation.
 * Sequential page-by-page: the Catalyst ZCQL client does not tolerate
 * concurrent calls (concurrent reads were observed to hang rather than
 * error), so pages are read one at a time as before.
 */
async function fetchHoldingsRowsPaged(zcql, accountCode, asOnDate, extraSql = "") {
  const clause = holdingsDateFilterClause(asOnDate);
  const rows = [];
  let offset = 0;
  const acc = escHoldingsSql(accountCode);
  while (true) {
    const batch = await queryWithTimeout(
      zcql,
      `
      SELECT ROWID, TRANSACTION_DATE, SETTLEMENT_DATE, TYPE, ISIN,
             QUANTITY, PRICE, TOTAL_AMOUNT, HOLDING, WAP, HOLDING_VALUE, P_L, STATUS,
             CREATEDTIME
      FROM Holdings
      WHERE WS_Account_code = '${acc}'
      ${clause}
      ${extraSql}
      ORDER BY ${HOLDINGS_FIFO_ORDER_BY_SQL}
      LIMIT ${BATCH} OFFSET ${offset}
    `
    );
    if (!batch || batch.length === 0) break;
    for (const r of batch) rows.push(r.Holdings || r);
    if (batch.length < BATCH) break;
    offset += BATCH;
  }

  // Refine the loose SQL date clause (which ORs both dates): buy rows must
  // respect SETTLEMENT_DATE, others TRANSACTION_DATE — same as the rebuild job.
  // Without this, a not-yet-settled buy (e.g. OPI) leaks in via its trade date.
  const trimmed = String(asOnDate ?? "").trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    const nextDay = new Date(trimmed);
    nextDay.setDate(nextDay.getDate() + 1);
    const cutoff = nextDay.toISOString().split("T")[0];
    return rows.filter((r) => {
      const d = holdingsEffectiveDate(r);
      return !d || d < cutoff;
    });
  }

  return rows;
}

function rollupLastSnapshotByIsin(sortedHoldingsRows) {
  const lastByIsin = new Map();
  for (const h of sortedHoldingsRows) {
    const isin = String(h.ISIN || "").trim();
    if (!isin) continue;
    lastByIsin.set(isin, h);
  }
  const out = [];
  for (const [isin, row] of lastByIsin) {
    const hold = Number(row.HOLDING) || 0;
    // Treat sub-epsilon dust as a closed position (fully sold), not an open one.
    if (hold <= 1e-6) continue;
    out.push({ isin, lastRow: row });
  }
  return out;
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

async function fetchSecurityListByIsins(zcql, isins) {
  const map = Object.create(null);
  if (!isins || !isins.length) return map;
  for (const part of chunk(isins, 200)) {
    const inClause = part.map((i) => `'${escHoldingsSql(i)}'`).join(",");
    let offset = 0;
    while (true) {
      const batch = await queryWithTimeout(
        zcql,
        `
        SELECT ISIN, Security_Code, Security_Name FROM Security_List
        WHERE ISIN IN (${inClause})
        ORDER BY ROWID ASC
        LIMIT ${BATCH} OFFSET ${offset}
      `
      );
      if (!batch || batch.length === 0) break;
      for (const r of batch) {
        const row = r.Security_List || r;
        if (row.ISIN)
          map[String(row.ISIN).trim()] = {
            securityCode: row.Security_Code || "",
            securityName: row.Security_Name || "",
          };
      }
      if (batch.length < BATCH) break;
      offset += BATCH;
    }
  }
  return map;
}

function isoDaysBefore(dateStr, days) {
  const d = new Date(dateStr);
  d.setDate(d.getDate() - days);
  return d.toISOString().split("T")[0];
}

/**
 * Latest Bhav_Copy close on/before `priceDate` for each ISIN, in bulk.
 *
 * Replaces the old one-query-per-ISIN loop: runs a single
 * `WHERE ISIN IN (...) AND TradDt BETWEEN windowStart AND priceDate`
 * query per chunk (paginated), then picks the newest row per ISIN in memory.
 * ISINs with no trade inside the lookback window fall back to the original
 * unbounded per-ISIN lookup (rare: suspended / delisted names).
 *
 * Fills and returns `priceMap` (ISIN -> ClsPric). ISINs already present in
 * `priceMap` are skipped, so the shared per-job cache still means each ISIN
 * is fetched at most once across the whole export.
 */
async function fetchLatestPricesByIsins(zcql, isins, priceDate, priceMap) {
  const map = priceMap || {};
  const pending = [
    ...new Set((isins || []).map((i) => String(i).trim()).filter(Boolean)),
  ].filter((isin) => !(isin in map));
  if (!pending.length) return map;

  const windowStart = isoDaysBefore(priceDate, PRICE_LOOKBACK_DAYS);

  for (const part of chunk(pending, PRICE_ISIN_CHUNK)) {
    const inClause = part.map((i) => `'${escHoldingsSql(i)}'`).join(",");
    let offset = 0;
    try {
      while (true) {
        const rows = await queryWithTimeout(
          zcql,
          `
          SELECT ISIN, ClsPric, TradDt FROM Bhav_Copy
          WHERE ISIN IN (${inClause})
            AND TradDt >= '${escHoldingsSql(windowStart)}'
            AND TradDt <= '${escHoldingsSql(priceDate)}'
          ORDER BY TradDt DESC
          LIMIT ${BATCH} OFFSET ${offset}
        `
        );
        if (!rows || rows.length === 0) break;
        for (const r of rows) {
          const row = r.Bhav_Copy || r;
          const isin = String(row.ISIN || "").trim();
          // Rows arrive newest-first, so the first one seen for an ISIN is its latest.
          if (!isin || isin in map) continue;
          map[isin] = row.ClsPric || 0;
        }
        if (rows.length < BATCH || part.every((isin) => isin in map)) break;
        offset += BATCH;
      }
    } catch (err) {
      // Chunk query failed — leave this chunk's ISINs for the per-ISIN fallback below.
      console.error(`Bulk price chunk failed (${part.length} ISINs):`, err.message);
    }
  }

  const stillMissing = pending.filter((isin) => !(isin in map));
  for (const isin of stillMissing) {
    try {
      const rows = await queryWithTimeout(
        zcql,
        `
        SELECT ISIN, ClsPric, TradDt FROM Bhav_Copy
        WHERE ISIN = '${escHoldingsSql(isin)}'
          AND TradDt <= '${escHoldingsSql(priceDate)}'
        ORDER BY TradDt DESC
        LIMIT 1
      `
      );
      const row = rows.length ? rows[0].Bhav_Copy || rows[0] : null;
      map[isin] = row ? row.ClsPric || 0 : 0;
    } catch (err) {
      console.error(`Error fetching fallback price for ISIN ${isin}:`, err);
      map[isin] = 0;
    }
  }

  return map;
}

module.exports = {
  escHoldingsSql,
  fetchHoldingsRowsPaged,
  fetchSecurityListByIsins,
  fetchLatestPricesByIsins,
  rollupLastSnapshotByIsin,
};
