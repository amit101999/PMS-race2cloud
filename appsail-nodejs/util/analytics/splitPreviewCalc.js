import { buildVirtualToActualMap } from "../mapVirtualToActualCodes.js";
import {
  holdingsEffectiveDate,
  recomputeHoldingsSnapshot,
} from "./holdingsFromTable.js";

const HOLDINGS_BATCH = 250;
const SPLIT_ACCOUNT_BATCH = 250;
const round4 = (n) => Math.round(n * 10000) / 10000;
const escSql = (v) => String(v ?? "").replace(/'/g, "''");

/** Split_Account.Status values set by the Split page's custodian reconciliation. */
export const SPLIT_STATUS = {
  RECEIVABLE: "RECEIVABLE",
  RECEIVED: "RECEIVED",
};

/**
 * Every Split_Account row of one split event (ISIN + record date + ratio), one
 * per virtual account code.
 *
 * Rows appear in two ways: "Send as Receivable" inserts them (Status
 * RECEIVABLE) before the split is applied, and the holdings rebuild writes them
 * for applied splits, keeping any Status / Received_Date already there.
 *
 * @returns {Promise<Array<{ rowId, accountCode, oldHolding, newHolding, status, receivedDate }>>}
 */
export async function readSplitAccountRows(zcql, { isin, recordDateISO, r1, r2 }) {
  const rows = [];
  // ZCQL's OFFSET is 1-based, so each page repeats the previous page's last row.
  const seenRowIds = new Set();
  let offset = 0;

  while (true) {
    const batch = await zcql.executeZCQLQuery(`
      SELECT ROWID, WS_Account_code, Old_Holding, New_Holding, Status, Received_Date
      FROM Split_Account
      WHERE ISIN = '${escSql(isin)}'
        AND Record_Date = '${escSql(recordDateISO)}'
        AND Ratio1 = ${Number(r1)}
        AND Ratio2 = ${Number(r2)}
      ORDER BY ROWID ASC
      LIMIT ${SPLIT_ACCOUNT_BATCH} OFFSET ${offset}
    `);
    if (!batch || batch.length === 0) break;

    for (const row of batch) {
      const s = row.Split_Account || row;
      if (seenRowIds.has(s.ROWID)) continue;
      seenRowIds.add(s.ROWID);
      rows.push({
        rowId: s.ROWID,
        accountCode: String(s.WS_Account_code || "").trim(),
        oldHolding: Number(s.Old_Holding) || 0,
        newHolding: Number(s.New_Holding) || 0,
        status: String(s.Status || "").trim().toUpperCase(),
        receivedDate: String(s.Received_Date || "").slice(0, 10),
      });
    }

    if (batch.length < SPLIT_ACCOUNT_BATCH) break;
    offset += SPLIT_ACCOUNT_BATCH;
  }

  return rows;
}

/**
 * Where a split event is in the custodian reconciliation:
 *   NEW        — not yet compared with the Ex-Entitlement file (stage 1)
 *   RECEIVABLE — sent as receivable, waiting for the Benefit Collection file (stage 2)
 *   RECEIVED   — confirmed and applied. Rows with no Status belong to a split
 *                applied before this reconciliation existed, so they count too.
 */
export function splitStage(splitAccountRows) {
  if (splitAccountRows.some((r) => r.status === SPLIT_STATUS.RECEIVABLE)) return "RECEIVABLE";
  if (splitAccountRows.length > 0) return "RECEIVED";
  return "NEW";
}

/** Exclusive next-day cutoff, stepped in UTC so the local zone cannot shift it. */
const nextISODate = (isoDate) => {
  const d = new Date(`${isoDate}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + 1);
  return d.toISOString().split("T")[0];
};

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
 * Must stay numerically identical to `previewStockSplit` in SplitController —
 * the two feed the same screen and its CSV export, so any divergence shows up
 * as a preview that disagrees with the file the user downloads from it. That
 * means the transaction-date basis below, not just the SQL date filter:
 * `holdingsEffectiveDate` is the same OPI rule the controller applies locally,
 * and is also what `recomputeHoldingsSnapshot` orders by internally.
 *
 * Split logic:
 *   newHolding   = currentHolding × ratio2 / ratio1 (fractions kept, 4 dp)
 *   holdingValue = unchanged (same total cost)
 *   newWAP       = holdingValue / newHolding (price per share changes)
 */
export async function computeSplitPreviewRows(zcql, { isin, r1, r2, recordDateISO }) {
  // cutoff = day AFTER the record date for the date filter (exclusive upper bound)
  const cutoff = nextISODate(recordDateISO);

  /* STEP 1: READ Holdings AS OF the record date — walk in FIFO order and keep
     updating per account; last row per account = state on the record date. */
  const latestByAccount = new Map(); // accountCode → last Holdings row
  const rowsByAccount = new Map(); // accountCode → every effective row, for re-derivation
  // ZCQL's OFFSET is 1-based, so each page repeats the previous page's last
  // row; skip it or recomputeHoldingsSnapshot counts that trade twice.
  const seenRowIds = new Set();
  let offset = 0;

  while (true) {
    const batch = await zcql.executeZCQLQuery(`
      SELECT ROWID, WS_Account_code, HOLDING, WAP, HOLDING_VALUE,
             QUANTITY, PRICE, TOTAL_AMOUNT,
             TYPE, SETTLEMENT_DATE, TRANSACTION_DATE, CA_DATE
      FROM Holdings
      WHERE ISIN = '${isin}'
        AND (SETTLEMENT_DATE < '${cutoff}' OR TRANSACTION_DATE < '${cutoff}')
      ORDER BY CREATEDTIME ASC, ROWID ASC
      LIMIT ${HOLDINGS_BATCH} OFFSET ${offset}
    `);

    if (!batch || batch.length === 0) break;

    for (const row of batch) {
      const h = row.Holdings || row;
      if (h.ROWID != null) {
        if (seenRowIds.has(h.ROWID)) continue;
        seenRowIds.add(h.ROWID);
      }
      const acc = String(h.WS_Account_code || "").trim();
      if (!acc) continue;
      // The SQL clause above deliberately ORs both dates — it cannot express
      // the OPI exception on its own. Apply the per-type rule here BEFORE the
      // row can become this account's "latest" snapshot, otherwise a row that
      // is not yet effective overwrites (and then wrongly drops) the real one.
      const eff = holdingsEffectiveDate(h);
      if (eff && eff >= cutoff) continue;
      latestByAccount.set(acc, h); // last row per account wins
      if (!rowsByAccount.has(acc)) rowsByAccount.set(acc, []);
      rowsByAccount.get(acc).push(h);
    }

    if (batch.length < HOLDINGS_BATCH) break;
    offset += HOLDINGS_BATCH;
  }

  if (latestByAccount.size === 0) return [];

  /* The stored HOLDING is a running balance frozen in settlement order at
     rebuild time, so it can contradict the rows the trade-date rule selected
     above — it may include a buy that is not yet effective on the issue date.
     Re-derive the position from the rows themselves. Once the Holdings table
     has been rebuilt on the new basis this becomes a no-op. */
  for (const [acc, accRows] of rowsByAccount) {
    const snap = recomputeHoldingsSnapshot(accRows);
    if (snap.HOLDING <= 0) {
      latestByAccount.delete(acc); // fully sold as at the issue date
      continue;
    }
    latestByAccount.set(acc, { ...latestByAccount.get(acc), ...snap });
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
    if (currentHolding <= 0) continue; // fully sold by the record date

    const holdingValue = Number(h.HOLDING_VALUE) || 0;
    const currentWAP = Number(h.WAP) || 0;

    // Fractions are kept (3 shares on 2:3 = 4.5), same as the holdings rebuild.
    const newHolding = round4(currentHolding * splitMultiplier);
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
      delta: round4(newHolding - currentHolding),
    });
  }

  return preview;
}
