const escSql = (s) => String(s ?? "").replace(/'/g, "''");

const BATCH = 250;

/**
 * Merger rows for an account (optional as-on date, same window as Transaction in analytics).
 * Merger table stores per-account rows with Tran_Type = 'MERGER'.
 */
export async function fetchMergerRecordsForAccount({ zcql, accountCode, asOnDate }) {
  let dateCondition = "";
  if (asOnDate && /^\d{4}-\d{2}-\d{2}$/.test(asOnDate)) {
    const nextDay = new Date(asOnDate);
    nextDay.setDate(nextDay.getDate() + 1);
    const nextDayStr = nextDay.toISOString().split("T")[0];
    dateCondition = ` AND (TRANDATE < '${nextDayStr}' OR SETDATE < '${nextDayStr}')`;
  }

  const rows = [];
  const seen = new Set();
  let offset = 0;

  while (true) {
    const batch = await zcql.executeZCQLQuery(`
      SELECT *
      FROM Merger
      WHERE WS_Account_code = '${escSql(accountCode)}'
      ${dateCondition}
      ORDER BY TRANDATE ASC, ROWID ASC
      LIMIT ${BATCH} OFFSET ${offset}
    `);
    if (!batch || batch.length === 0) break;
    for (const row of batch) {
      const m = row.Merger || row;
      const rid = m.ROWID;
      if (rid != null && seen.has(rid)) continue;
      if (rid != null) seen.add(rid);
      rows.push(m);
    }
    if (batch.length < BATCH) break;
    offset += BATCH;
  }

  return rows.filter(
    (m) => String(m.Tran_Type || m.tran_type || "").toUpperCase() === "MERGER",
  );
}

/**
 * Merger rows for one account + ISIN (new ISIN side — for FIFO / stock history).
 */
export async function fetchMergerRecordsForStock({
  zcql,
  accountCode,
  isin,
  asOnDate,
}) {
  const all = await fetchMergerRecordsForAccount({ zcql, accountCode, asOnDate });
  const u = String(isin || "").trim().toUpperCase();
  return all.filter(
    (m) => String(m.ISIN || m.isin || "").trim().toUpperCase() === u,
  );
}
