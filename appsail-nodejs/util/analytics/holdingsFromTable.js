/** Read materialised Holding rows — same ledger as CalculateHoldingPerAccount / catalyst rebuild. */

/*
 * HAND-COPIED INTO CATALYST FUNCTIONS. Functions can't import from AppSail, so
 * the date rule and position maths below are ported into:
 *   functions/ExportAllCustomerHoldingData/holdingsFromTable.js
 *   functions/UpdateBonusTable/holdingsSnapshot.js
 *   functions/UpdateDividendData/holdingsSnapshot.js      (+ fetch in index.js)
 *   functions/ExportDividendAccounts/holdingsSnapshot.js  (+ fetch in index.js)
 * A change to holdingsEffectiveDate, the buy/sell/replace type tests or
 * recomputeHoldingsSnapshot must be made in every copy, or preview and apply
 * will disagree.
 */

const BATCH = 250;

/**
 * SQL fragment: order rows as they were emitted by FIFO sequential INSERT
 * (`CalculateHoldingPerAccount`, `RebuildHoldingtable`). Use wherever you read `Holdings`
 * timeline so `HOLDING` behaves as a running balance top → bottom.
 */
export const HOLDINGS_FIFO_ORDER_BY_SQL = "CREATEDTIME ASC, ROWID ASC";

export function escHoldingsSql(s) {
  return String(s ?? "").replace(/'/g, "''");
}

/** Mirrors analytics transaction date cutoff (exclusive next-day). */
export function holdingsDateFilterClause(asOnDate) {
  if (!asOnDate || !/^\d{4}-\d{2}-\d{2}$/.test(String(asOnDate).trim()))
    return "";
  const nextDay = new Date(asOnDate);
  nextDay.setDate(nextDay.getDate() + 1);
  const cutoff = nextDay.toISOString().split("T")[0];
  return ` AND (SETTLEMENT_DATE < '${escHoldingsSql(cutoff)}' OR TRANSACTION_DATE < '${escHoldingsSql(cutoff)}')`;
}

export function isHoldingsBuyType(type) {
  return /^BY-|SQB|OPI/i.test(String(type || ""));
}

export function isHoldingsSellType(type) {
  return /^SL\+|SQS|OPO|NF-/i.test(String(type || "").trim());
}

/** Only OPI is settlement-dated — see `holdingsEffectiveDate` for why. */
export function isHoldingsOpiType(type) {
  return /^OPI/i.test(String(type || "").trim());
}

/* ===== TRADE_DATE_MIGRATION: OLD (settlement basis for ALL buys) — retained, do not delete =====
export function holdingsEffectiveDate(row) {
  const setD = String(row.SETTLEMENT_DATE || "").slice(0, 10);
  const txD = String(row.TRANSACTION_DATE || "").slice(0, 10);
  return isHoldingsBuyType(row.TYPE) ? setD || txD : txD || setD;
}
   ============================================================================================= */

/** MERGER / DEMERGER rows are dated by the corporate action, not the lot. */
export function isHoldingsReplaceType(type) {
  const t = String(type || "").trim().toUpperCase();
  return t === "MERGER" || t === "DEMERGER";
}

/**
 * The date a Holdings row should be filtered on for an as-on-date view.
 * Mirrors the rebuild job's `getEffectiveDate` (RebuildHoldingtable): every type
 * takes effect on TRANSACTION_DATE, except OPI. OPI rows are inception / opening
 * positions where TRANDATE is the original acquisition date and SETDATE is the
 * PMS onboarding date — genuinely different events, so OPI stays settlement-dated.
 *
 * MERGER / DEMERGER are the second exception. The rebuild emits one row per
 * surviving lot and deliberately preserves each lot's ORIGINAL trade date in
 * TRANSACTION_DATE, stamping the event's record date on CA_DATE instead — see
 * the `date: normalizeDate(recDate || td)` comments in
 * RebuildHoldingtable/holdingsRebuildFromSources.js. Dating those rows by
 * TRANSACTION_DATE scatters one event across the timeline at dates years before
 * it happened, so the position appears restated in the past. CA_DATE is the date
 * the event actually took effect; fall back to the lot date only for legacy rows
 * written before CA_DATE was stamped (the rebuild's own `recDate || td`).
 *
 * `isHoldingsBuyType` is deliberately NOT used here: it still classifies buy/sell
 * direction for quantity maths and must keep matching BY-/SQB/OPI.
 *
 * NOTE: callers must SELECT CA_DATE for this to apply — without the column the
 * fallback silently restores the old lot-date behaviour.
 */
export function holdingsEffectiveDate(row) {
  const setD = String(row.SETTLEMENT_DATE || "").slice(0, 10);
  const txD = String(row.TRANSACTION_DATE || "").slice(0, 10);
  if (isHoldingsReplaceType(row.TYPE)) {
    const caD = String(row.CA_DATE || "").slice(0, 10);
    if (caD) return caD;
  }
  return isHoldingsOpiType(row.TYPE) ? setD || txD : txD || setD;
}

/**
 * Pagination / sort alignment with FIFO interleaving (same-day merge order).
 */
export function holdingsTypePriority(type) {
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
 * All Holdings rows for an account (optional extra AND clause).
 * Sorted by **`CREATEDTIME` then `ROWID`** — same sequence as Catalyst FIFO emit / insert
 * order from `RebuildHoldingtable` so `HOLDING`/`WAP`/etc. columns read as a correct
 * running ledger top → bottom. (Sorting by transaction date alone reorders snapshots and
 * breaks running-balance intuition.)
 *
 * Paged by keyset (`ROWID > cursor`), NOT LIMIT/OFFSET: ZCQL's OFFSET is 1-based
 * (`OFFSET 250` skips only 249 rows), so OFFSET paging returned every page
 * boundary row twice. `recomputeHoldingsSnapshot` then counted that trade twice —
 * e.g. a duplicated buy showed a fully sold position as still held.
 */
export async function fetchHoldingsRowsPaged(zcql, accountCode, asOnDate, extraSql = "") {
  const clause = holdingsDateFilterClause(asOnDate);
  const rows = [];
  let lastRowId = "0";
  const acc = escHoldingsSql(accountCode);
  while (true) {
    const batch = await zcql.executeZCQLQuery(`
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
    `);
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

/**
 * BULK: every Holdings row for ONE ISIN across ALL accounts, grouped by account.
 *
 * Same result as calling `fetchHoldingsRowsForAccountIsin` once per account, but
 * in a handful of queries instead of one per account. The per-account loop
 * overran the deployed AppSail request budget for widely-held ISINs (hundreds of
 * sequential round trips) while passing locally, where no such limit applies.
 * `DemergerController.fetchHoldingRowsByAccountForIsin` already solved this the
 * same way — keep the two in sync.
 *
 * Keyset (`ROWID > cursor`) rather than LIMIT/OFFSET: with OFFSET each page
 * re-scans and re-sorts the whole ISIN match (≈ O(n²)); keyset walks the ROWID
 * primary-key index forward exactly once (~O(n)).
 *
 * Rows arrive in global ROWID order, so each account's slice is re-sorted in
 * memory into `CREATEDTIME ASC, ROWID ASC` — the FIFO materialisation order the
 * running `HOLDING`/`WAP` columns assume.
 *
 * @returns {Promise<Map<string, object[]>>} account code → its rows, FIFO order.
 */
export async function fetchHoldingsRowsByAccountForIsin(zcql, isin, asOnDate) {
  const byAccount = new Map();
  const isinTrim = String(isin ?? "").trim();
  if (!isinTrim) return byAccount;

  const clause = holdingsDateFilterClause(asOnDate);
  let lastRowId = "0";

  while (true) {
    const batch = await zcql.executeZCQLQuery(`
      SELECT ROWID, WS_Account_code, TRANSACTION_DATE, SETTLEMENT_DATE, TYPE, ISIN,
             QUANTITY, PRICE, TOTAL_AMOUNT, HOLDING, WAP, HOLDING_VALUE, P_L, STATUS,
             CA_DATE, CREATEDTIME
      FROM Holdings
      WHERE ISIN = '${escHoldingsSql(isinTrim)}'
        AND ROWID > ${lastRowId}
      ${clause}
      ORDER BY ROWID ASC
      LIMIT ${BATCH}
    `);
    if (!batch || batch.length === 0) break;

    let maxRowId = lastRowId;
    for (const r of batch) {
      const h = r.Holdings || r;
      const rowId = String(h.ROWID ?? "");
      // Track the cursor across ALL rows, including ones the refine drops —
      // filtering first could stall the cursor and loop forever.
      if (rowId && (rowId.length > maxRowId.length || (rowId.length === maxRowId.length && rowId > maxRowId))) {
        maxRowId = rowId;
      }
      const code = String(h.WS_Account_code ?? "").trim();
      if (!code) continue;
      let list = byAccount.get(code);
      if (!list) {
        list = [];
        byAccount.set(code, list);
      }
      list.push(h);
    }

    // Guard against a non-advancing cursor (non-numeric / duplicate ROWIDs).
    if (maxRowId === lastRowId) break;
    lastRowId = maxRowId;
    if (batch.length < BATCH) break;
  }

  // Refine the loose SQL date clause exactly as `fetchHoldingsRowsPaged` does,
  // then restore FIFO order within each account.
  const trimmed = String(asOnDate ?? "").trim();
  const cutoff = /^\d{4}-\d{2}-\d{2}$/.test(trimmed)
    ? (() => {
        const nextDay = new Date(trimmed);
        nextDay.setDate(nextDay.getDate() + 1);
        return nextDay.toISOString().split("T")[0];
      })()
    : "";

  for (const [code, rows] of byAccount) {
    const kept = cutoff
      ? rows.filter((r) => {
          const d = holdingsEffectiveDate(r);
          return !d || d < cutoff;
        })
      : rows;

    kept.sort(compareFifoOrder);

    if (kept.length) byAccount.set(code, kept);
    else byAccount.delete(code);
  }

  return byAccount;
}

/** One ISIN only — fifo materialisation order (same ORDER BY as `fetchHoldingsRowsPaged`). */
export async function fetchHoldingsRowsForAccountIsin(
  zcql,
  accountCode,
  isin,
  asOnDate,
) {
  const isinTrim = String(isin ?? "").trim();
  if (!isinTrim) return [];
  const extra = ` AND ISIN = '${escHoldingsSql(isinTrim)}' `;
  return fetchHoldingsRowsPaged(zcql, accountCode, asOnDate, extra);
}

/** DISTINCT ISIN from Holdings for an account (for security dropdown / Bhav lists). */
export async function fetchDistinctHoldingsIsins(zcql, accountCode, asOnDate) {
  const clause = holdingsDateFilterClause(asOnDate);
  const isins = new Set();
  let offset = 0;
  const acc = escHoldingsSql(accountCode);
  while (true) {
    const batch = await zcql.executeZCQLQuery(`
      SELECT ISIN FROM Holdings
      WHERE WS_Account_code = '${acc}'
        AND ISIN IS NOT NULL AND ISIN != ''
      ${clause}
      ORDER BY ROWID ASC
      LIMIT ${BATCH} OFFSET ${offset}
    `);
    if (!batch || batch.length === 0) break;
    for (const r of batch) {
      const isin = (r.Holdings || r)?.ISIN;
      if (isin) isins.add(String(isin).trim());
    }
    if (batch.length < BATCH) break;
    offset += BATCH;
  }
  return [...isins];
}

/**
 * Map Catalyst Security_List rows to ISIN metadata.
 */
export async function fetchSecurityListByIsins(zcql, isins) {
  const map = Object.create(null);
  if (!isins || !isins.length) return map;
  const chunk = (arr, size) => {
    const out = [];
    for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
    return out;
  };
  for (const part of chunk(isins, 200)) {
    const inClause = part.map((i) => `'${escHoldingsSql(i)}'`).join(",");
    let offset = 0;
    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT ISIN, Security_Code, Security_Name FROM Security_List
        WHERE ISIN IN (${inClause})
        ORDER BY ROWID ASC
        LIMIT ${BATCH} OFFSET ${offset}
      `);
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

/**
 * Last materialised snapshot per ISIN.
 * Rows should follow fifo materialisation order (`HOLDINGS_FIFO_ORDER_BY_SQL`). Only keeps ISIN
 * whose latest row has final HOLDING > 0.
 */
export function rollupLastSnapshotByIsin(sortedHoldingsRows) {
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
    out.push({
      isin,
      lastRow: row,
    });
  }
  return out;
}

const QTY_EPS = 1e-6;

/**
 * Re-derive HOLDING / WAP / HOLDING_VALUE from the rows themselves.
 *
 * WHY THIS EXISTS
 * The `HOLDING` column is a running balance frozen at rebuild time, accumulated
 * in *insert* order — which is settlement order for any table built before the
 * trade-date migration. Simply filtering rows by the new effective date picks a
 * different "last row", but that row's stored total was still accumulated the old
 * way, so it can include a trade that the new rule should have excluded.
 *
 * Re-walking the rows in effective-date order fixes the numbers without touching
 * the database. Once `RebuildHoldingtable` has been re-run, the stored column and
 * this function agree and the caller can go back to reading `HOLDING` directly.
 *
 * ACCURACY: exact for buys and sells (the migration's target). BONUS quantities
 * are deltas and accumulate correctly too. SPLIT, MERGER and DEMERGER restate
 * the position rather than adding to it: the first row of an event clears the
 * prior lots and the remaining rows of that event accumulate onto the empty
 * queue, matching the rebuild job. That restatement is exact — the per-lot rows
 * carry the full post-event position, so nothing is inherited from a stored
 * snapshot.
 *
 * `rows` must already be effective-date filtered by the caller, and must include
 * CA_DATE so merger / demerger rows date and group by their event rather than by
 * each lot's original trade date.
 */
export function recomputeHoldingsSnapshot(rows) {
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
     * MERGER / DEMERGER RESTATE the position — they do not add to it.
     *
     * The rebuild emits one row per surviving lot and, on the FIRST row of an
     * event, closes every prior lot and resets the running holding to 0 before
     * accumulating the new lots (`buyQueue.length = 0; holdings = 0;` in the
     * DEMERGER / MERGER branches of holdingsRebuildFromSources.js). The same
     * rule is in DemergerController's `computeSurvivingLotsFromHoldingRows`.
     *
     * Treating these rows as additive double-counted the position: a demerger's
     * old-ISIN side carries the UNCHANGED quantity (only its cost is reduced),
     * so adding it on top of the lots it was meant to replace roughly doubled
     * the holding for every demerged security.
     *
     * Rows of one event are grouped by effective date — which is CA_DATE for
     * these types (see `holdingsEffectiveDate`), so every lot of one event
     * shares a key and lands together in the sort above.
     *
     * LEGACY ROWS (no CA_DATE): the key falls back to each lot's trade date, so
     * a multi-lot event splits into one "event" per lot and only the last lot
     * survives — an undercount. That is deliberate: the rebuild keys on the same
     * `recordDate || trandate` fallback (holdingsRebuildFromSources.js, the
     * `demergerDate` / `mergerDate` event keys) and undercounts identically, so
     * re-derived and stored HOLDING still agree, which is the point of this
     * function. Do NOT "fix" only this side — that reintroduces the mismatch.
     * The real fix is backfilling Record_Date on legacy Demerger_Record /
     * Merger rows and re-running the rebuild so CA_DATE is populated.
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
 * Callers need no other change.
 */
export function rollupRecomputedSnapshotByIsin(sortedHoldingsRows) {
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

/** Map Holdings row → TransactionPage / fifo timeline element shape (append-only fields safe for JSON). */
export function mapHoldingsRowToStockHistoryDto(h, isinFromQuery) {
  const plRaw = h.P_L;
  const plNum = plRaw === null || plRaw === undefined || plRaw === "" ? null : Number(plRaw);

  let isActive = true;
  const st = h.STATUS;
  if (typeof st === "boolean") isActive = st;
  else if (String(st).toLowerCase() === "false") isActive = false;

  const txD = String(h.TRANSACTION_DATE || "").slice(0, 10);
  const setD = String(h.SETTLEMENT_DATE || "").slice(0, 10);

  return {
    rowId: h.ROWID != null ? String(h.ROWID) : null,
    createdTime:
      h.CREATEDTIME != null && String(h.CREATEDTIME).trim() !== ""
        ? String(h.CREATEDTIME).trim()
        : null,
    trandate: txD || setD || null,
    originalTrandate: txD || setD || null,
    setdate: setD || txD || null,
    tranType: String(h.TYPE || "").trim() || "",
    qty: Number(h.QUANTITY) || 0,
    price: Number(h.PRICE) || 0,
    netAmount: Number(h.TOTAL_AMOUNT) || 0,
    holdings: Number(h.HOLDING) || 0,
    costOfHoldings: Number(h.HOLDING_VALUE) || 0,
    averageCostOfHoldings: Number(h.WAP) || 0,
    profitLoss: Number.isFinite(plNum) ? plNum : null,
    isActive,
    isin: String(h.ISIN || isinFromQuery || "").trim(),
    // Corporate-action date (e.g. merger record date); blank for normal rows.
    caDate: String(h.CA_DATE || "").slice(0, 10) || null,
  };
}
