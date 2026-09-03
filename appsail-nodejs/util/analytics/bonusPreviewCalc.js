import { buildVirtualToActualMap } from "../mapVirtualToActualCodes.js";

const HOLDINGS_BATCH = 250;

/**
 * Shared fast-path bonus preview calculation, used by exportBonusPreviewFile
 * (GET /bonus/export-preview) — mirrors computeSplitPreviewRows in
 * splitPreviewCalc.js.
 *
 * ONE paginated query against Holdings for the whole ISIN (no per-account
 * loop). The export handler used to re-derive the same numbers via a
 * per-account fan-out (one Holdings query per eligible account), which was
 * fast locally but blew past Catalyst's ~30s request limit on deployed
 * environments (408) once an ISIN has enough holders — the same issue Split
 * hit before being switched to this bulk-query shape.
 *
 * Bonus logic (unchanged):
 *   bonusShares = floor(currentHolding * r1 / r2)
 *   newHolding  = currentHolding + bonusShares
 *   delta       = newHolding - currentHolding
 */
export async function computeBonusPreviewRows(zcql, { isin, r1, r2, exDateISO }) {
  // cutoff = day AFTER exDate for the date filter
  const cutoffObj = new Date(exDateISO);
  cutoffObj.setDate(cutoffObj.getDate() + 1);
  const cutoff = cutoffObj.toISOString().split("T")[0];

  /* STEP 1: READ Holdings AS OF exDate — walk in FIFO order and keep
     updating per account; last row per account = state on exDate. */
  const latestByAccount = new Map(); // accountCode → last Holdings row
  let offset = 0;

  while (true) {
    const batch = await zcql.executeZCQLQuery(`
      SELECT WS_Account_code, HOLDING, SETTLEMENT_DATE, TRANSACTION_DATE
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

  /* STEP 2: CALCULATE BONUS PREVIEW (simple arithmetic) */
  const virtualToActual = await buildVirtualToActualMap(
    zcql,
    [...latestByAccount.keys()],
  );

  const preview = [];
  for (const [accountCode, h] of latestByAccount) {
    const currentHolding = Number(h.HOLDING) || 0;
    if (currentHolding <= 1e-6) continue; // fully sold before exDate

    const bonusShares = Math.floor((currentHolding * r1) / r2);
    if (bonusShares <= 0) continue;

    const newHolding = currentHolding + bonusShares;

    preview.push({
      isin,
      accountCode, // WS_Account_code (virtual)
      actualCode: virtualToActual.get(accountCode) || "",
      currentHolding,
      bonusShares,
      newHolding,
      delta: newHolding - currentHolding,
    });
  }

  return preview;
}
