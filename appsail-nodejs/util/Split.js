export const fetchSplitForStock = async ({ zcql, securityCode, tableName }) => {
  const rows = [];
  let offset = 0;
  const limit = 250;

  while (true) {
    const query = `
        SELECT Security_Code , Security_Name,Issue_Date,Ratio1,Ratio2
        FROM ${tableName}
        WHERE Security_Code = '${securityCode.replace(/'/g, "''")}'
        LIMIT ${limit} OFFSET ${offset}
      `;

    const batch = await zcql.executeZCQLQuery(query);
    if (!batch || batch.length === 0) break;

    rows.push(...batch);
    if (batch.length < limit) break;
    offset += limit;
  }

  return rows.map((row) => {
    const b = row["Split"];
    return {
      securityCode: b.Security_Code,
      securityName: b.Security_Name, // display only
      date: b.Issue_Date,
      ratio1: b.Ratio1,
      ratio2: b.Ratio2,
      issueDate: b.Issue_Date,
    };
  });
};
