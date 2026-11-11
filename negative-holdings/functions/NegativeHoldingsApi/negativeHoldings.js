const { holdingsEffectiveDate, isHoldingsSellType } = require("./holdingsRead.js");

/*
 * Every holdings job caps a sell at what is held (sellQty = min(qty, holding)),
 * so the stored HOLDING never goes below zero: hold 100, sell 150, and it shows 0.
 * The sell row keeps its full QUANTITY, so the shortfall can be recovered by
 * comparing it with the holding on the row before.
 *
 * We read the stored HOLDING instead of adding up trades because it already
 * includes splits, bonuses, mergers and demergers.
 */

const STATUS = {
  STILL_NEGATIVE: "Still negative",
  COVERED_LATER: "Covered later",
  CHECK_BY_HAND: "Check by hand",
};

const QTY_EPS = 1e-6;
const roundQty = (n) => Math.round(n * 1e6) / 1e6;

/**
 * Works through one client's rows for one share, in created order.
 * Returns each row with its shortBy and realHolding, and a summary that is null
 * when the share never went short.
 */
function walkShareRows(rows) {
  let previousHolding = 0;
  let totalShort = 0;
  let realHolding = 0;
  let lowestRealHolding = 0;
  let firstShortDate = null;
  let restatedAfterShort = false;
  let holdingBeforeCorporateAction = null;

  const annotated = rows.map((row) => {
    const type = String(row.TYPE || "").trim().toUpperCase();

    // Dividend rows don't change the quantity.
    if (type === "DIV" || type === "DIV+") {
      return { row, shortBy: 0, realHolding: restatedAfterShort ? null : realHolding };
    }

    const quantity = Math.abs(Number(row.QUANTITY) || 0);
    const storedHolding = Number(row.HOLDING) || 0;
    let shortBy = 0;

    if (isHoldingsSellType(type) && quantity > previousHolding + QTY_EPS) {
      shortBy = roundQty(quantity - previousHolding);
      totalShort = roundQty(totalShort + shortBy);
      if (!firstShortDate) firstShortDate = holdingsEffectiveDate(row) || null;
    }

    // A split, merger or demerger restates the position. An earlier shortfall
    // was in the old quantity and can't be converted reliably, so from here on
    // the real holding is unknown.
    if (totalShort > 0 && (type === "SPLIT" || type === "MERGER" || type === "DEMERGER")) {
      if (!restatedAfterShort) holdingBeforeCorporateAction = realHolding;
      restatedAfterShort = true;
    }

    realHolding = roundQty(storedHolding - totalShort);
    if (!restatedAfterShort && realHolding < lowestRealHolding) {
      lowestRealHolding = realHolding;
    }
    previousHolding = storedHolding;

    return { row, shortBy, realHolding: restatedAfterShort ? null : realHolding };
  });

  if (totalShort === 0) return { rows: annotated, summary: null };

  let status = STATUS.COVERED_LATER;
  if (restatedAfterShort) status = STATUS.CHECK_BY_HAND;
  else if (realHolding < -QTY_EPS) status = STATUS.STILL_NEGATIVE;

  return {
    rows: annotated,
    summary: {
      totalShort,
      firstShortDate,
      lowestRealHolding,
      realHolding: restatedAfterShort ? null : realHolding,
      holdingBeforeCorporateAction,
      // What the main app shows: its holding stops at zero.
      storedHolding: previousHolding,
      status,
    },
  };
}

/** All of a client's Holdings rows → one entry per share that went short. */
function findNegativeHoldings(rows) {
  const rowsByIsin = new Map();
  for (const row of rows) {
    const isin = String(row.ISIN || "").trim();
    if (!rowsByIsin.has(isin)) rowsByIsin.set(isin, []);
    rowsByIsin.get(isin).push(row);
  }

  const shares = [];
  for (const [isin, shareRows] of rowsByIsin) {
    const { summary } = walkShareRows(shareRows);
    if (summary) shares.push({ isin, ...summary });
  }
  return shares;
}

module.exports = { STATUS, walkShareRows, findNegativeHoldings };
