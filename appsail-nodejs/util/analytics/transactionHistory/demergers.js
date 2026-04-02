const escSql = (s) => String(s ?? "").replace(/'/g, "''");

const BATCH = 250;

/**
 * Demerger_Record rows for an account (optional as-on date, same window as Transaction in analytics).
 */
export async function fetchDemergerRecordsForAccount({ zcql, accountCode, asOnDate }) {
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
      FROM Demerger_Record
      WHERE WS_Account_code = '${escSql(accountCode)}'
      ${dateCondition}
      ORDER BY TRANDATE ASC, ROWID ASC
      LIMIT ${BATCH} OFFSET ${offset}
    `);
    if (!batch || batch.length === 0) break;
    for (const row of batch) {
      const d = row.Demerger_Record || row;
      const rid = d.ROWID;
      if (rid != null && seen.has(rid)) continue;
      if (rid != null) seen.add(rid);
      rows.push(d);
    }
    if (batch.length < BATCH) break;
    offset += BATCH;
  }

  return rows.filter(
    (d) => String(d.Tran_Type || d.tran_type || "").toUpperCase() === "DEMERGER",
  );
}

/**
 * Demerger rows for one account + ISIN (FIFO / stock history).
 */
export async function fetchDemergerRecordsForStock({
  zcql,
  accountCode,
  isin,
  asOnDate,
}) {
  const all = await fetchDemergerRecordsForAccount({ zcql, accountCode, asOnDate });
  const u = String(isin || "").trim().toUpperCase();
  return all.filter(
    (d) => String(d.ISIN || d.isin || "").trim().toUpperCase() === u,
  );
}
