const fetchSplitForStock = async ({
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
    dateCondition = ` AND Issue_Date < '${nextDayStr}'`;
  }

  const rows = [];
  const seenRowIds = new Set();
  let offset = 0;
  const limit = 250;

  while (true) {
    try {
      const query = `
        SELECT Security_Code, Security_Name, Issue_Date, Ratio1, Ratio2, ISIN, ROWID
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

  return rows.map((b) => ({
    securityCode: b.Security_Code,
    securityName: b.Security_Name,
    date: b.Issue_Date,
    ratio1: Number(b.Ratio1) || 0,
    ratio2: Number(b.Ratio2) || 0,
    issueDate: b.Issue_Date,
    isin: b.ISIN || "",
  }));
};

module.exports = { fetchSplitForStock };
