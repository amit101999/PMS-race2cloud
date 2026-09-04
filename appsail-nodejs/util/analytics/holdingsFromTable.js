/** Read materialised Holding rows — same ledger as CalculateHoldingPerAccount / catalyst rebuild. */

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
  return /^SL[+-]|SQS|OPO|NF-/i.test(String(type || "").trim());
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

/**
 * The date a Holdings row should be filtered on for an as-on-date view.
 * Mirrors the rebuild job's `getEffectiveDate` (RebuildHoldingtable): every type
 * takes effect on TRANSACTION_DATE, except OPI. OPI rows are inception / opening
 * positions where TRANDATE is the original acquisition date and SETDATE is the
 * PMS onboarding date — genuinely different events, so OPI stays settlement-dated.
 *
 * `isHoldingsBuyType` is deliberately NOT used here: it still classifies buy/sell
 * direction for quantity maths and must keep matching BY-/SQB/OPI.
 */
export function holdingsEffectiveDate(row) {
  const setD = String(row.SETTLEMENT_DATE || "").slice(0, 10);
  const txD = String(row.TRANSACTION_DATE || "").slice(0, 10);
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

/**
 * All Holdings rows for an account (optional extra AND clause).
 * Sorted by **`CREATEDTIME` then `ROWID`** — same sequence as Catalyst FIFO emit / insert
 * order from `RebuildHoldingtable` so `HOLDING`/`WAP`/etc. columns read as a correct
 * running ledger top → bottom. (Sorting by transaction date alone reorders snapshots and
 * breaks running-balance intuition.)
 */
export async function fetchHoldingsRowsPaged(zcql, accountCode, asOnDate, extraSql = "") {
  const clause = holdingsDateFilterClause(asOnDate);
  const rows = [];
  let offset = 0;
  const acc = escHoldingsSql(accountCode);
  while (true) {
    const batch = await zcql.executeZCQLQuery(`
      SELECT ROWID, TRANSACTION_DATE, SETTLEMENT_DATE, TYPE, ISIN,
             QUANTITY, PRICE, TOTAL_AMOUNT, HOLDING, WAP, HOLDING_VALUE, P_L, STATUS,
             CA_DATE, CREATEDTIME
      FROM Holdings
      WHERE WS_Account_code = '${acc}'
      ${clause}
      ${extraSql}
      ORDER BY ${HOLDINGS_FIFO_ORDER_BY_SQL}
      LIMIT ${BATCH} OFFSET ${offset}
    `);
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

    kept.sort((a, b) => {
      const ta = String(a.CREATEDTIME ?? "");
      const tb = String(b.CREATEDTIME ?? "");
      if (ta !== tb) return ta < tb ? -1 : 1;
      const ra = String(a.ROWID ?? "0");
      const rb = String(b.ROWID ?? "0");
      if (ra.length !== rb.length) return ra.length - rb.length;
      return ra < rb ? -1 : ra > rb ? 1 : 0;
    });

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
 * ACCURACY: exact for buys and sells (the migration's target). BONUS / MERGER /
 * DEMERGER quantities are deltas and accumulate correctly too. SPLIT restates the
 * position rather than adding to it, so a split re-anchors the running lots on that
 * row's stored snapshot and accumulation resumes from there — accurate after the
 * split, and inherently only as good as the stored value at the split itself.
 *
 * `rows` must already be effective-date filtered by the caller.
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

  for (const h of ordered) {
    const typ = String(h.TYPE || "").trim().toUpperCase();
    const qty = Math.abs(Number(h.QUANTITY) || 0);

    if (typ === "SPLIT") {
      // Non-additive: QUANTITY is the post-split total, not a delta.
      const anchorQty = Number(h.HOLDING) || 0;
      const anchorWap = Number(h.WAP) || 0;
      lots = anchorQty > QTY_EPS ? [{ qty: anchorQty, price: anchorWap }] : [];
      continue;
    }

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

    // Everything else adds: BY-/SQB/OPI, BONUS, MERGER, DEMERGER.
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
