export const fetchStockTransactions = async ({
  zcql,
  tableName,
  accountCode,
  securityCode,
  asOnDate,
}) => {
  let dateCondition = "";

  if (asOnDate && /^\d{4}-\d{2}-\d{2}$/.test(asOnDate)) {
    const nextDay = new Date(asOnDate);
    nextDay.setDate(nextDay.getDate() + 1);

    const nextDayStr = nextDay.toISOString().split("T")[0];

    dateCondition = ` AND TRANDATE < '${nextDayStr}'`;
  }
  let where = `
      WHERE WS_Account_code = '${accountCode.replace(/'/g, "''")}'
      AND Security_code = '${securityCode.replace(/'/g, "''")}'
      ${dateCondition}
    `;

  const rows = [];
  let offset = 0;
  const limit = 250;

  while (true) {
    const query = `
        SELECT TRANDATE ,Tran_Type,Security_code,QTY,NETRATE,Net_Amount,ISIN
        FROM Transaction
        ${where}
        ORDER BY TRANDATE ASC
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
      trandate: r.TRANDATE,
      tranType: r.Tran_Type,
      securityCode: r.Security_code,
      qty: Number(r.QTY) || 0,
      netrate: Number(r.NETRATE) || 0,
      netAmount: Number(r.Net_Amount) || 0,
      isin: r.ISIN || "",
    };
  });
};
