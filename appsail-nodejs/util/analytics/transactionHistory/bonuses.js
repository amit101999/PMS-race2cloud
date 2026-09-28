export const fetchBonusesForStock = async ({
  zcql,
  accountCode,
  // securityCode,
  isin,
  asOnDate,
}) => {
  let dateCondition = "";

  if (asOnDate && /^\d{4}-\d{2}-\d{2}$/.test(asOnDate)) {
    const nextDay = new Date(asOnDate);
    nextDay.setDate(nextDay.getDate() + 1);

    const nextDayStr = nextDay.toISOString().split("T")[0];

    // Record_Date <= ExDate, so this is "record date < cutoff"; the ExDate
    // half covers bonuses saved before Record_Date existed.
    dateCondition = ` AND (Record_Date < '${nextDayStr}' OR ExDate < '${nextDayStr}')`;
  }
  const rows = [];
  const seenRowIds = new Set();
  let offset = 0;
  const limit = 250;

  while (true) {
    try {
      const query = `
        SELECT SecurityCode, SecurityName, Record_Date, ExDate, BonusShare, ISIN, ROWID
        FROM Bonus
        WHERE WS_Account_code = '${accountCode.replace(/'/g, "''")}'
        AND ISIN = '${isin.replace(/'/g, "''")}'
        ${dateCondition}
        ORDER BY ExDate ASC, ROWID ASC
        LIMIT ${limit} OFFSET ${offset}
      `;

      const batch = await zcql.executeZCQLQuery(query);
      if (!batch || batch.length === 0) break;

      for (const row of batch) {
        const b = row.Bonus || row;
        if (b.ROWID && seenRowIds.has(b.ROWID)) continue;
        if (b.ROWID) seenRowIds.add(b.ROWID);
        rows.push(b);
      }

      if (batch.length < limit) break;
      offset += limit;
    } catch (err) {
      console.error(`Error fetching bonuses for ${accountCode}/${isin} at offset ${offset}:`, err);
      break;
    }
  }

  return rows.map((b) => ({
    securityCode: b.SecurityCode,
    securityName: b.SecurityName,
    // The bonus lands in holdings on its record date. Bonuses saved before
    // Record_Date existed only have ExDate, which served that role.
    exDate: b.Record_Date || b.ExDate,
    bonusShare: Number(b.BonusShare) || 0,
    isin: b.ISIN || "",
  }));
};
