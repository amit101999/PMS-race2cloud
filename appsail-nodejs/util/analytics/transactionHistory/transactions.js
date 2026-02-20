export const fetchStockTransactions = async ({
  zcql,
  tableName,
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
    dateCondition = ` AND SETDATE < '${nextDayStr}'`;
  }

  const where = `
    WHERE WS_Account_code = '${accountCode.replace(/'/g, "''")}'
    AND ISIN = '${isin.replace(/'/g, "''")}'
    ${dateCondition}
  `;

  const rows = [];
  let offset = 0;
  const limit = 250;

  while (true) {
    const query = `
      SELECT SETDATE, Tran_Type, Security_code, QTY, NETRATE, Net_Amount, ISIN
      FROM Transaction
      ${where}
      ORDER BY SETDATE ASC, ROWID ASC
      LIMIT ${limit} OFFSET ${offset}
    `;

    const batch = await zcql.executeZCQLQuery(query);
    if (!batch || batch.length === 0) break;

    rows.push(...batch);
    if (batch.length < limit) break;
    offset += limit;
  }

  return rows.map((row) => {
    const r = row.Transaction || row[tableName] || row;
    return {
      trandate: r.SETDATE,
      tranType: r.Tran_Type,
      securityCode: r.Security_code,
      qty: Number(r.QTY) || 0,
      netrate: Number(r.NETRATE) || 0,
      netAmount: Number(r.Net_Amount) || 0,
      isin: r.ISIN || "",
    };
  });
};
