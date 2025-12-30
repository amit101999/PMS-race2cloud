export const fetchBonusesForStock = async ({
  zcql,
  accountCode,
  securityCode,
  asOnDate,
}) => {

  let dateCondition = "";

if (asOnDate && /^\d{4}-\d{2}-\d{2}$/.test(asOnDate)) {
  const nextDay = new Date(asOnDate);
  nextDay.setDate(nextDay.getDate() + 1);

  const nextDayStr = nextDay.toISOString().split("T")[0];

  dateCondition = ` AND ExDate < '${nextDayStr}'`;
}
  const rows = [];
  let offset = 0;
  const limit = 250;

  while (true) {
    const query = `
        SELECT SecurityCode,SecurityName,ExDate,BonusShare
        FROM Bonus
        WHERE WS_Account_code = '${accountCode.replace(/'/g, "''")}'
        AND SecurityCode = '${securityCode.replace(/'/g, "''")}'
        ${dateCondition}
        LIMIT ${limit} OFFSET ${offset}
      `;

    const batch = await zcql.executeZCQLQuery(query);
    if (!batch || batch.length === 0) break;

    rows.push(...batch);
    if (batch.length < limit) break;
    offset += limit;
  }

  return rows.map((row) => {
    const b = row.Bonus || row;
    return {
      securityCode: b.SecurityCode,
      securityName: b.SecurityName, // display only
      exDate: b.ExDate,
      bonusShare: Number(b.BonusShare) || 0,
    };
  });
}; 
