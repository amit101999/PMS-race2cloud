import { buildVirtualToActualMap } from "../mapVirtualToActualCodes.js";

const HOLDINGS_BATCH = 250;

/**
 * Shared fast-path split preview calculation, used by both:
 *  - previewStockSplit (POST /split/preview)
 *  - exportSplitPreviewFile (GET /split/export-preview)
 *
 * ONE paginated query against Holdings for the whole ISIN (no per-account
 * loop) — this is what keeps preview at ~5s. The export handler used to
 * re-derive the same numbers via a much slower per-account fan-out, which
 * blew past Catalyst's ~30s request limit on deployed environments (408).
 *
 * Split logic:
 *   newHolding   = floor(currentHolding × ratio2 / ratio1)
 *   holdingValue = unchanged (same total cost)
 *   newWAP       = holdingValue / newHolding (price per share changes)
 */
export async function computeSplitPreviewRows(zcql, { isin, r1, r2, issueDateISO }) {
  // cutoff = day AFTER issueDate for the date filter
  const cutoffObj = new Date(issueDateISO);
  cutoffObj.setDate(cutoffObj.getDate() + 1);
  const cutoff = cutoffObj.toISOString().split("T")[0];

  /* STEP 1: READ Holdings AS OF issueDate — walk in FIFO order and keep
     updating per account; last row per account = state on issueDate. */
  const latestByAccount = new Map(); // accountCode → last Holdings row
  let offset = 0;

  while (true) {
    const batch = await zcql.executeZCQLQuery(`
      SELECT WS_Account_code, HOLDING, WAP, HOLDING_VALUE,
             SETTLEMENT_DATE, TRANSACTION_DATE
      FROM Holdings
      WHERE ISIN = '${isin}'
        AND (SETTLEMENT_DATE < '${cutoff}' OR TRANSACTION_DATE < '${cutoff}')
      ORDER BY CREATEDTIME ASC, ROWID ASC
      LIMIT ${HOLDINGS_BATCH} OFFSET ${offset}
    `);

    if (!batch || batch.length === 0) break;

    for (const row of batch) {
      const h = row.Holdings || row;
      const acc = String(h.WS_Account_code || "").trim();
      if (!acc) continue;
      latestByAccount.set(acc, h); // last row per account wins
    }

    if (batch.length < HOLDINGS_BATCH) break;
    offset += HOLDINGS_BATCH;
  }

  if (latestByAccount.size === 0) return [];

  /* STEP 2: CALCULATE SPLIT PREVIEW (simple arithmetic) */
  const virtualToActual = await buildVirtualToActualMap(
    zcql,
    [...latestByAccount.keys()],
  );
  const preview = [];
  const splitMultiplier = r2 / r1;

  for (const [accountCode, h] of latestByAccount) {
    const currentHolding = Number(h.HOLDING) || 0;
    if (currentHolding <= 0) continue; // fully sold before issueDate

    const holdingValue = Number(h.HOLDING_VALUE) || 0;
    const currentWAP = Number(h.WAP) || 0;

    const newHolding = Math.floor(currentHolding * splitMultiplier);
    if (newHolding <= 0) continue;

    // Cost is unchanged — more shares at same total cost → WAP goes down
    const newWAP =
      newHolding > 0 ? Math.round((holdingValue / newHolding) * 100) / 100 : 0;

    preview.push({
      isin,
      accountCode, // WS_Account_code (virtual)
      actualCode: virtualToActual.get(accountCode) || "",
      currentHolding,
      currentWAP,
      holdingValue,
      newHolding,
      newWAP,
      delta: newHolding - currentHolding,
    });
  }

  return preview;
}
