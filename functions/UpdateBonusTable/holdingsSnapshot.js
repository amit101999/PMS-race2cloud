/**
 * Holdings-ledger position maths for the bonus apply job.
 *
 * PORT of the matching helpers in
 * appsail-nodejs/util/analytics/holdingsFromTable.js — keep the two identical.
 * This job runs as a standalone Catalyst function and cannot import from the
 * AppSail app, which is how the previous divergence happened: the Bonus PREVIEW
 * (AppSail) sized a grant from the Holdings ledger while this job replayed the
 * raw Transaction / Bonus / Split tables through its own FIFO engine. That
 * engine was never given merger or demerger inputs, so shares that arrived
 * through a corporate action were invisible to it and the applied grant could
 * be smaller than the approved preview — or zero, when the client had never
 * traded the ISIN directly at all.
 *
 * Only the position maths lives here. Fetching and date-filtering the rows is
 * the caller's job, exactly as in `computeBonusPreviewRows`.
 */

const QTY_EPS = 1e-6;

function isHoldingsBuyType(type) {
  return /^BY-|SQB|OPI/i.test(String(type || ""));
}

/** Prefix match, mirroring appsail-nodejs — OPI is the only OPI-prefixed type. */
function isHoldingsOpiType(type) {
  return /^OPI/i.test(String(type || "").trim());
}

function isHoldingsSellType(type) {
  return /^SL[+-]|SQS|OPO|NF-/i.test(String(type || "").trim());
}

/** MERGER / DEMERGER rows are dated by the corporate action, not the lot. */
function isHoldingsReplaceType(type) {
  const t = String(type || "").trim().toUpperCase();
  return t === "MERGER" || t === "DEMERGER";
}

/**
 * The date a Holdings row takes effect.
 *
 * Transaction Date for everything EXCEPT OPI (opening/initial-position) rows,
 * which use Settlement Date, and MERGER / DEMERGER rows, which are dated by
 * CA_DATE. The rebuild emits one row per surviving lot and preserves each lot's
 * ORIGINAL trade date in TRANSACTION_DATE, stamping the event's record date on
 * CA_DATE — so dating those rows by TRANSACTION_DATE scatters one event across
 * the timeline at dates years before it happened. Falls back to the lot date for
 * legacy rows written before CA_DATE was stamped, matching the rebuild's
 * `recDate || td`.
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

/**
 * Re-derive HOLDING / WAP / HOLDING_VALUE from the rows themselves.
 *
 * The stored `HOLDING` column is a running balance frozen at rebuild time,
 * accumulated in *insert* order — settlement order for any table built before
 * the trade-date migration. Filtering rows by the new effective date picks a
 * different "last row", but that row's stored total was still accumulated the
 * old way, so it can include a trade the new rule should have excluded.
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
  // First row of a MERGER / DEMERGER event closes the prior position; the rest
  // of that same event accumulate onto it. See the replace branch below.
  let lastReplaceEventKey = null;

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
     */
    if (isHoldingsReplaceType(typ)) {
      const eventKey = `${typ}|${holdingsEffectiveDate(h)}`;
      if (eventKey !== lastReplaceEventKey) {
        lots = [];
        lastReplaceEventKey = eventKey;
      }
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

/** Exclusive next-day cutoff, stepped in UTC so the local zone cannot shift it. */
function nextISODate(isoDate) {
  const d = new Date(`${isoDate}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + 1);
  return d.toISOString().split("T")[0];
}

module.exports = {
  QTY_EPS,
  isHoldingsBuyType,
  isHoldingsOpiType,
  isHoldingsSellType,
  isHoldingsReplaceType,
  holdingsEffectiveDate,
  holdingsTypePriority,
  recomputeHoldingsSnapshot,
  nextISODate,
};
