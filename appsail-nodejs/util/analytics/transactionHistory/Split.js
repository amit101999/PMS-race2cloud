export const fetchSplitForStock = async ({
  zcql,
  isin,
  tableName,
  asOnDate,
}) => {
  let dateCondition = "";
  if (asOnDate && /^\d{4}-\d{2}-\d{2}$/.test(asOnDate)) {
    const nextDay = new Date(asOnDate);
    nextDay.setDate(nextDay.getDate() + 1);
    const nextDayStr = nextDay.toISOString().split("T")[0];
    // Record_Date <= Issue_Date, so this is "record date < cutoff"; the
    // Issue_Date half covers splits saved before Record_Date existed.
    dateCondition = ` AND (Record_Date < '${nextDayStr}' OR Issue_Date < '${nextDayStr}')`;
  }

  const rows = [];
  const seenRowIds = new Set();
  let offset = 0;
  const limit = 250;

  while (true) {
    try {
      const query = `
        SELECT Security_Code, Security_Name, Record_Date, Issue_Date, Ratio1, Ratio2, ISIN, ROWID
        FROM ${tableName}
        WHERE ISIN = '${isin.replace(/'/g, "''")}'
        ${dateCondition}
        ORDER BY Issue_Date ASC, ROWID ASC
        LIMIT ${limit} OFFSET ${offset}
      `;

      const batch = await zcql.executeZCQLQuery(query);
      if (!batch || batch.length === 0) break;

      for (const row of batch) {
        const s = row["Split"] || row;
        if (s.ROWID && seenRowIds.has(s.ROWID)) continue;
        if (s.ROWID) seenRowIds.add(s.ROWID);
        rows.push(s);
      }

      if (batch.length < limit) break;
      offset += limit;
    } catch (err) {
      console.error(`Error fetching splits for ISIN ${isin} at offset ${offset}:`, err);
      break;
    }
  }

  return rows.map((b) => {
    // The split takes effect in holdings on its record date. Splits saved
    // before Record_Date existed only have Issue_Date, which served that role.
    const splitDate = b.Record_Date || b.Issue_Date;
    return {
      securityCode: b.Security_Code,
      securityName: b.Security_Name,
      date: splitDate,
      ratio1: Number(b.Ratio1) || 0,
      ratio2: Number(b.Ratio2) || 0,
      issueDate: splitDate,
      isin: b.ISIN || "",
    };
  });
};
