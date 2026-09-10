/**
 * Custody Matching — clientIds lookup for stage S1.
 *
 * Resolves the account codes that actually appear in the transaction file, in
 * batched IN-lists, rather than one query per code.
 *
 * Why batched-by-file rather than loading the whole clientIds table: the cost
 * then scales with the FILE (a few dozen queries for a few thousand distinct
 * codes) instead of with the TABLE, which keeps growing. The proven precedent is
 * functions/ExportCorpActionReport/virtualToActual.js, which uses the same
 * shape. What must never happen is the per-code lookup in
 * util/mapVirtualToActualCodes.js — at ~2,000 distinct accounts that is 2,000
 * sequential round trips, roughly three minutes of a fifteen-minute budget spent
 * on something that should take one second.
 *
 * Two passes, because a code in the file may be either kind:
 *   1. WS_Account_code IN (...) — the virtual codes, giving their Actual_Code
 *   2. Actual_Code    IN (...) — whatever is left, confirming it is already actual
 *
 * Anything still unresolved after both passes is genuinely unknown, and becomes
 * an UNMAPPED_CODE exception rather than a crash.
 */

/**
 * Codes per IN-list.
 *
 * Kept well under the 300-row ZCQL ceiling: at one clientIds row per code a
 * batch of 100 returns 100 rows, leaving room for duplicate mappings. A batch
 * that comes back at exactly the cap is re-run in halves, because a silently
 * truncated result here would look like an unmapped account rather than an error.
 */
const LOOKUP_BATCH = 100;
const ZCQL_ROW_CAP = 300;

const esc = (s) => String(s ?? "").replace(/'/g, "''");

const unwrap = (rows) => (rows || []).map((r) => r?.clientIds || r).filter(Boolean);

const chunk = (arr, size) => {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
};

/**
 * Run one IN-list query, splitting the batch if the result hits the row cap.
 *
 * @param {(codes:string[])=>string} buildSql
 */
const queryBatched = async (zcql, codes, buildSql, depth = 0) => {
  if (codes.length === 0) return [];

  let rows;
  try {
    rows = unwrap(await zcql.executeZCQLQuery(buildSql(codes)));
  } catch (err) {
    console.error(
      `[custodyMatch/lookupCodes] batch of ${codes.length} failed: ${err?.message || err}`,
    );
    throw err;
  }

  // A full page means the result may have been cut short; halve and retry so a
  // truncated read never masquerades as an unmapped account.
  if (rows.length >= ZCQL_ROW_CAP && codes.length > 1 && depth < 8) {
    const mid = Math.ceil(codes.length / 2);
    const [a, b] = await Promise.all([
      queryBatched(zcql, codes.slice(0, mid), buildSql, depth + 1),
      queryBatched(zcql, codes.slice(mid), buildSql, depth + 1),
    ]);
    return [...a, ...b];
  }

  return rows;
};

/**
 * Resolve every account code in a transaction file.
 *
 * @param {object} zcql
 * @param {string[]} codes account codes as they appear in the file
 * @returns {Promise<{virtualToActual:Map<string,string>, knownActual:Set<string>, unresolved:string[]}>}
 */
export const buildCodeMaps = async (zcql, codes) => {
  const unique = [
    ...new Set((codes || []).map((c) => String(c ?? "").trim().toUpperCase()).filter(Boolean)),
  ];

  const virtualToActual = new Map();
  const knownActual = new Set();

  if (unique.length === 0) {
    return { virtualToActual, knownActual, unresolved: [] };
  }

  // ---- pass 1: treat them as virtual codes -------------------------------
  for (const batch of chunk(unique, LOOKUP_BATCH)) {
    const rows = await queryBatched(
      zcql,
      batch,
      (list) =>
        `SELECT WS_Account_code, Actual_Code FROM clientIds WHERE WS_Account_code IN (${list
          .map((c) => `'${esc(c)}'`)
          .join(",")})`,
    );

    for (const row of rows) {
      const virtual = String(row.WS_Account_code ?? "").trim().toUpperCase();
      const actual = String(row.Actual_Code ?? "").trim().toUpperCase();
      if (!virtual || !actual) continue;
      // Prefer a non-empty mapping when a virtual code appears more than once.
      if (!virtualToActual.has(virtual)) virtualToActual.set(virtual, actual);
      knownActual.add(actual);
    }
  }

  // ---- pass 2: whatever is left may already BE an actual code -------------
  const leftover = unique.filter((c) => !virtualToActual.has(c) && !knownActual.has(c));

  for (const batch of chunk(leftover, LOOKUP_BATCH)) {
    const rows = await queryBatched(
      zcql,
      batch,
      (list) =>
        `SELECT Actual_Code FROM clientIds WHERE Actual_Code IN (${list
          .map((c) => `'${esc(c)}'`)
          .join(",")})`,
    );

    for (const row of rows) {
      const actual = String(row.Actual_Code ?? "").trim().toUpperCase();
      if (actual) knownActual.add(actual);
    }
  }

  const unresolved = unique.filter(
    (c) => !virtualToActual.has(c) && !knownActual.has(c),
  );

  return { virtualToActual, knownActual, unresolved };
};

export const LOOKUP_BATCH_SIZE = LOOKUP_BATCH;
