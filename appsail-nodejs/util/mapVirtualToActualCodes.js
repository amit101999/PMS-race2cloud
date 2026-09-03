const CLIENT_IDS_BATCH = 270;
const VIRTUAL_CODES_LOOKUP_BATCH = 50;

/**
 * Build WS_Account_code (virtual) → Actual_Code map from clientIds.
 * When `virtualCodes` is provided, looks up only those codes in IN-list
 * batches (one query per 50 codes, not one query per code — a per-code loop
 * here previously reintroduced the same per-account fan-out that caused CA
 * preview endpoints to 408 on deployed environments once an ISIN had enough
 * holders, even after their Holdings-query loops were fixed).
 * Otherwise pages the full clientIds table.
 */
export async function buildVirtualToActualMap(zcql, virtualCodes = null) {
  const map = new Map();

  if (Array.isArray(virtualCodes) && virtualCodes.length > 0) {
    const unique = [
      ...new Set(
        virtualCodes.map((c) => String(c ?? "").trim()).filter(Boolean),
      ),
    ];

    for (let i = 0; i < unique.length; i += VIRTUAL_CODES_LOOKUP_BATCH) {
      const slice = unique.slice(i, i + VIRTUAL_CODES_LOOKUP_BATCH);
      const inList = slice
        .map((c) => `'${c.replace(/'/g, "''")}'`)
        .join(",");
      let rows = [];
      try {
        rows = await zcql.executeZCQLQuery(
          `SELECT WS_Account_code, Actual_Code FROM clientIds WHERE WS_Account_code IN (${inList})`,
        );
      } catch (err) {
        console.error(
          "[mapVirtualToActualCodes] batch lookup failed:",
          err.message,
        );
        for (const code of slice) map.set(code, "");
        continue;
      }

      for (const code of slice) map.set(code, "");
      for (const r of rows || []) {
        const row = r.clientIds || r;
        const virtual = String(row.WS_Account_code ?? "").trim();
        if (!virtual) continue;
        const actual = String(row.Actual_Code ?? "").trim();
        // Prefer the first non-empty Actual_Code if duplicate clientIds rows exist.
        if (!map.get(virtual) && actual) map.set(virtual, actual);
      }
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
