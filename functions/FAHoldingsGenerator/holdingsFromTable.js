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

/** Prefix match, mirroring appsail-nodejs — OPI is the only OPI-prefixed type. */
function isHoldingsOpiType(type) {
  return /^OPI/i.test(String(type || "").trim());
}

function isHoldingsSellType(type) {
  return /^SL\+|SQS|OPO|NF-/i.test(String(type || "").trim());
}

/** MERGER / DEMERGER rows are dated by the corporate action, not the lot. */
function isHoldingsReplaceType(type) {
  const t = String(type || "").trim().toUpperCase();
  return t === "MERGER" || t === "DEMERGER";
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
 * OPI (opening/initial-position) rows, which keep using Settlement Date, and
 * MERGER / DEMERGER rows, which are dated by CA_DATE. The rebuild emits one row
 * per surviving lot and preserves each lot's ORIGINAL trade date in
 * TRANSACTION_DATE, stamping the event's record date on CA_DATE — so dating
 * those rows by TRANSACTION_DATE scatters one event across the timeline at
 * dates years before it happened. Falls back to the lot date for legacy rows
 * written before CA_DATE was stamped, matching the rebuild's `recDate || td`.
 *
 * Keep in sync with appsail-nodejs/util/analytics/holdingsFromTable.js.
 */
function holdingsEffectiveDate(row) {
  const setD = String(row.SETTLEMENT_DATE || "").slice(0, 10);
  const txD = String(row.TRANSACTION_DATE || "").slice(0, 10);
  if (isHoldingsReplaceType(row.TYPE)) {
    const caD = String(row.CA_DATE || "").slice(0, 10);
    if (caD) return caD;
  }
  return isHoldingsOpiType(row.TYPE) ? setD || txD : txD || setD;
}

/** Same-day ordering as the FIFO materialisation (SPLIT before BONUS, etc). */
function holdingsTypePriority(type) {
  const t = String(type || "").trim().toUpperCase();
  if (t === "SPLIT") return 1;
  if (t === "BONUS") return 2;
  if (t === "DIV+" || t === "DIV") return 3;
  if (t === "DEMERGER") return 4;
  if (t === "MERGER") return 5;
  return 0;
}

/** Numeric compare of ROWID strings (they exceed Number precision). */
function compareRowIds(a, b) {
  const ra = String(a ?? "0");
  const rb = String(b ?? "0");
  if (ra.length !== rb.length) return ra.length - rb.length;
  return ra < rb ? -1 : ra > rb ? 1 : 0;
}

/** In-memory equivalent of `HOLDINGS_FIFO_ORDER_BY_SQL`. */
function compareFifoOrder(a, b) {
  const ta = String(a.CREATEDTIME ?? "");
  const tb = String(b.CREATEDTIME ?? "");
  if (ta !== tb) return ta < tb ? -1 : 1;
  return compareRowIds(a.ROWID, b.ROWID);
}

/**
 * All Holdings rows for an account, returned in fifo materialisation order
 * (`HOLDINGS_FIFO_ORDER_BY_SQL`).
 * Sequential page-by-page: the Catalyst ZCQL client does not tolerate
 * concurrent calls (concurrent reads were observed to hang rather than
 * error), so pages are read one at a time as before.
 *
 * Paged by keyset (`ROWID > cursor`), NOT LIMIT/OFFSET: ZCQL's OFFSET is 1-based
 * (`OFFSET 250` skips only 249 rows), so OFFSET paging returned every page
 * boundary row twice and `recomputeHoldingsSnapshot` counted that trade twice.
 * Keep in sync with appsail-nodejs/util/analytics/holdingsFromTable.js.
 */
async function fetchHoldingsRowsPaged(zcql, accountCode, asOnDate, extraSql = "") {
  const clause = holdingsDateFilterClause(asOnDate);
  const rows = [];
  let lastRowId = "0";
  const acc = escHoldingsSql(accountCode);
  while (true) {
    const batch = await queryWithTimeout(
      zcql,
      `
      SELECT ROWID, TRANSACTION_DATE, SETTLEMENT_DATE, TYPE, ISIN,
             QUANTITY, PRICE, TOTAL_AMOUNT, HOLDING, WAP, HOLDING_VALUE, P_L, STATUS,
             CA_DATE, CREATEDTIME
      FROM Holdings
      WHERE WS_Account_code = '${acc}'
        AND ROWID > ${lastRowId}
      ${clause}
      ${extraSql}
      ORDER BY ROWID ASC
      LIMIT ${BATCH}
    `
    );
    if (!batch || batch.length === 0) break;
    let maxRowId = lastRowId;
    for (const r of batch) {
      const h = r.Holdings || r;
      rows.push(h);
      if (compareRowIds(h.ROWID, maxRowId) > 0) maxRowId = String(h.ROWID);
    }
    // Guard against a non-advancing cursor so a bad ROWID can't loop forever.
    if (maxRowId === lastRowId) break;
    lastRowId = maxRowId;
    if (batch.length < BATCH) break;
  }
  rows.sort(compareFifoOrder);

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

const QTY_EPS = 1e-6;

/**
 * Re-derive HOLDING / WAP / HOLDING_VALUE from the rows themselves.
 *
 * PORT of `recomputeHoldingsSnapshot` in
 * appsail-nodejs/util/analytics/holdingsFromTable.js — keep the two identical.
 * This job runs as a standalone Catalyst function and cannot import from the
 * AppSail app, which is exactly how the two drifted apart before: the AppSail
 * side gained the as-on-date re-derivation and this one kept exporting the
 * frozen `HOLDING` column, so a dated export disagreed with the dashboard.
 *
 * WHY THIS EXISTS
 * The `HOLDING` column is a running balance frozen at rebuild time, accumulated
 * in *insert* order — settlement order for any table built before the trade-date
 * migration. Filtering rows by the new effective date picks a different "last
 * row", but that row's stored total was still accumulated the old way, so it can
 * include a trade the new rule should have excluded.
 *
 * `rows` must already be effective-date filtered by the caller, and must include
 * CA_DATE so merger / demerger rows date and group by their event rather than by
 * each lot's original trade date.
 */
function recomputeHoldingsSnapshot(rows) {
  const ordered = rows
    .map((r, i) => ({ r, i }))
    .sort((a, b) => {
      const da = holdingsEffectiveDate(a.r) || "";
      const db = holdingsEffectiveDate(b.r) || "";
      if (da !== db) return da < db ? -1 : 1;
      const pa = holdingsTypePriority(a.r.TYPE);
      const pb = holdingsTypePriority(b.r.TYPE);
      if (pa !== pb) return pa - pb;
      return a.i - b.i; // stable: keep original insert order within a day
    })
    .map((x) => x.r);

  let lots = []; // FIFO queue of { qty, price }
  // First row of a SPLIT / MERGER / DEMERGER event closes the prior position;
  // the rest of that same event accumulate onto it. See the replace branch below.
  let lastReplaceEventKey = null;

  for (const h of ordered) {
    const typ = String(h.TYPE || "").trim().toUpperCase();
    const qty = Math.abs(Number(h.QUANTITY) || 0);

    if (typ === "DIV" || typ === "DIV+") continue; // cash only, no quantity effect

    if (isHoldingsSellType(typ)) {
      let remaining = qty;
      while (remaining > QTY_EPS && lots.length) {
        const lot = lots[0];
        const used = Math.min(lot.qty, remaining);
        lot.qty -= used;
        remaining -= used;
        if (lot.qty <= QTY_EPS) lots.shift();
      }
      continue;
    }

    /*
     * MERGER / DEMERGER RESTATE the position — they do not add to it. The first
     * row of an event clears the prior lots and the rest accumulate onto the
     * empty queue, matching the rebuild job (`buyQueue.length = 0; holdings = 0;`
     * in holdingsRebuildFromSources.js). Treating them as additive double-counted
     * every demerged security: the old-ISIN row carries the UNCHANGED quantity
     * (only its cost is reduced), so it stacked on the lots it should replace.
     *
     * LEGACY ROWS (no CA_DATE) fall back to each lot's trade date and so split
     * into one "event" per lot — an undercount. Deliberate: the rebuild keys on
     * the same `recordDate || trandate` fallback and undercounts identically, so
     * re-derived and stored HOLDING still agree. Fix the data, not one side.
     *
     * SPLIT follows the same rule: the rebuild writes one row per surviving
     * lot (QUANTITY / PRICE are that lot's post-split size and cost), all
     * dated on the split date. Keeping the lots separate is what lets a later
     * sell consume the oldest lot first. Collapsing them into one lot at the
     * row's running WAP blended the costs, so a split followed by a sell showed
     * a different WAP / holding value here than the stored ledger.
     */
    if (typ === "SPLIT" || isHoldingsReplaceType(typ)) {
      const eventKey = `${typ}|${holdingsEffectiveDate(h)}`;
      if (eventKey !== lastReplaceEventKey) {
        lots = [];
        lastReplaceEventKey = eventKey;
      }
      /*
       * A merger writes TWO kinds of row and they look identical here: the
       * surviving ISIN gets one row per lot RECEIVED, and the merged-away ISIN
       * gets one row for the quantity that LEFT the account. Both are
       * TYPE='MERGER' with a positive QUANTITY, so restating the position to
       * QUANTITY revives the dead security at its full pre-merger size — it
       * then shows up next to the surviving one for the same quantity (a
       * double count), and counts as eligible for bonus / split / dividend.
       * The stored HOLDING is what tells them apart: 0 on the closing row, the
       * running balance on a receiving one. The event has already cleared the
       * lots above, so a closing row simply adds nothing back.
       * DEMERGER is unaffected: its old-ISIN row keeps the unchanged quantity
       * and a non-zero HOLDING (only the cost is reduced).
       */
      if ((Number(h.HOLDING) || 0) <= QTY_EPS) continue;
      if (qty > QTY_EPS) {
        let price = Number(h.PRICE) || 0;
        const total = Number(h.TOTAL_AMOUNT) || 0;
        if (!price && total) price = total / qty;
        lots.push({ qty, price });
      }
      continue;
    }

    // Everything else adds: BY-/SQB/OPI, BONUS.
    if (qty > QTY_EPS) {
      let price = Number(h.PRICE) || 0;
      const total = Number(h.TOTAL_AMOUNT) || 0;
      if (!price && total) price = total / qty;
      lots.push({ qty, price });
    }
  }

  const holding = lots.reduce((s, l) => s + l.qty, 0);
  const cost = lots.reduce((s, l) => s + l.qty * l.price, 0);
  const wap = holding > QTY_EPS ? cost / holding : 0;

  return {
    HOLDING: holding <= QTY_EPS ? 0 : holding,
    WAP: Math.round(wap * 100) / 100,
    HOLDING_VALUE: Math.round(cost * 100) / 100,
  };
}

/**
 * Same contract as `rollupLastSnapshotByIsin`, but `lastRow` carries re-derived
 * HOLDING / WAP / HOLDING_VALUE instead of the frozen settlement-order values.
 */
function rollupRecomputedSnapshotByIsin(sortedHoldingsRows) {
  const rowsByIsin = new Map();
  for (const h of sortedHoldingsRows) {
    const isin = String(h.ISIN || "").trim();
    if (!isin) continue;
    if (!rowsByIsin.has(isin)) rowsByIsin.set(isin, []);
    rowsByIsin.get(isin).push(h);
  }

  const out = [];
  for (const [isin, rows] of rowsByIsin) {
    const snap = recomputeHoldingsSnapshot(rows);
    if (snap.HOLDING <= QTY_EPS) continue; // closed position
    out.push({ isin, lastRow: { ...rows[rows.length - 1], ...snap } });
  }
  return out;
}

module.exports = {
  escHoldingsSql,
  fetchHoldingsRowsPaged,
  fetchSecurityListByIsins,
  fetchLatestPricesByIsins,
  rollupLastSnapshotByIsin,
  rollupRecomputedSnapshotByIsin,
  recomputeHoldingsSnapshot,
};
