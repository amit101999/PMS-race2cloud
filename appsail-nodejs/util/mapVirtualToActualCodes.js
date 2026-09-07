const CLIENT_IDS_BATCH = 270;

/** Virtual codes per `IN (...)` clause. */
const VIRTUAL_CODE_CHUNK = 200;

const escSql = (s) => String(s ?? "").replace(/'/g, "''");

/**
 * Resolve Actual_Code for one virtual code (WS_Account_code).
 * Mirrors the working lookup in exportSingleClientHolding.js.
 * Retained as the per-code fallback when a batched chunk query fails.
 */
async function lookupActualForVirtual(zcql, virtualCode) {
  const esc = escSql(virtualCode);
  if (!esc) return "";

  const rows = await zcql.executeZCQLQuery(
    `SELECT Actual_Code FROM clientIds WHERE WS_Account_code = '${esc}' LIMIT 5`,
  );
  if (!rows?.length) return "";

  // Prefer the first non-empty Actual_Code if duplicate clientIds rows exist.
  for (const r of rows) {
    const row = r.clientIds || r;
    const actual = String(row?.Actual_Code ?? "").trim();
    if (actual) return actual;
  }
  return "";
}

/** One code at a time — the pre-batching path, kept for error recovery. */
async function resolveChunkIndividually(zcql, codes, map) {
  for (const code of codes) {
    try {
      map.set(code, await lookupActualForVirtual(zcql, code));
    } catch (err) {
      console.error(
        `[mapVirtualToActualCodes] lookup failed for ${code}:`,
        err.message,
      );
      map.set(code, "");
    }
  }
}

/**
 * Build WS_Account_code (virtual) → Actual_Code map from clientIds.
 * When `virtualCodes` is provided, looks up only those codes (reliable + fast).
 * Otherwise pages the full clientIds table.
 *
 * The scoped branch reads the codes in `IN (...)` chunks rather than one query
 * per code: a widely-held ISIN resolves hundreds of accounts, and a sequential
 * lookup loop there is the request-budget trap documented in
 * `fetchHoldingsRowsByAccountForIsin`. Both branches apply the same non-empty
 * tie-break, so the resulting map is unchanged.
 */
export async function buildVirtualToActualMap(zcql, virtualCodes = null) {
  const map = new Map();

  if (Array.isArray(virtualCodes) && virtualCodes.length > 0) {
    const unique = [
      ...new Set(
        virtualCodes.map((c) => String(c ?? "").trim()).filter(Boolean),
      ),
    ];

    // Seed every requested code so callers always get an entry, including codes
    // with no clientIds row — the per-code lookup returns "" for those.
    for (const code of unique) map.set(code, "");

    for (let i = 0; i < unique.length; i += VIRTUAL_CODE_CHUNK) {
      const chunk = unique.slice(i, i + VIRTUAL_CODE_CHUNK);
      const inClause = chunk.map((c) => `'${escSql(c)}'`).join(",");
      let offset = 0;
      let failed = false;

      while (true) {
        let batch;
        try {
          batch = await zcql.executeZCQLQuery(
            `SELECT WS_Account_code, Actual_Code FROM clientIds WHERE WS_Account_code IN (${inClause}) ORDER BY ROWID ASC LIMIT ${CLIENT_IDS_BATCH} OFFSET ${offset}`,
          );
        } catch (err) {
          console.error(
            "[mapVirtualToActualCodes] batch lookup failed, falling back:",
            err.message,
          );
          failed = true;
          break;
        }

        if (!batch?.length) break;

        for (const r of batch) {
          const row = r.clientIds || r;
          const virtual = String(row.WS_Account_code ?? "").trim();
          // Ignore anything outside the requested set.
          if (!virtual || !map.has(virtual)) continue;
          const actual = String(row.Actual_Code ?? "").trim();
          // Prefer a non-empty Actual_Code when duplicate rows exist for one
          // virtual code — same rule as the full-table branch below.
          if (!map.get(virtual) && actual) map.set(virtual, actual);
        }

        if (batch.length < CLIENT_IDS_BATCH) break;
        offset += CLIENT_IDS_BATCH;
      }

      // Batch query died — resolve this chunk the old way rather than
      // reporting blank Actual_Codes for it.
      if (failed) await resolveChunkIndividually(zcql, chunk, map);
    }

    return map;
  }

  let offset = 0;
  while (true) {
    const batch = await zcql.executeZCQLQuery(
      `SELECT WS_Account_code, Actual_Code FROM clientIds LIMIT ${CLIENT_IDS_BATCH} OFFSET ${offset}`,
    );
    if (!batch?.length) break;

    for (const r of batch) {
      const row = r.clientIds || r;
      const virtual = String(row.WS_Account_code ?? "").trim();
      if (!virtual) continue;
      const actual = String(row.Actual_Code ?? "").trim();
      // Prefer non-empty Actual_Code when duplicate virtual codes exist.
      if (!map.has(virtual) || (!map.get(virtual) && actual)) {
        map.set(virtual, actual);
      }
    }

    if (batch.length < CLIENT_IDS_BATCH) break;
    offset += CLIENT_IDS_BATCH;
  }

  return map;
}
