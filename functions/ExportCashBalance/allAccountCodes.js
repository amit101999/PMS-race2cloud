exports.getAllAccountCodesFromDatabase = async (zcql, tableName) => {
  try {
    let offset = 0;
    const limit = 270;
    let hasNext = true;

    const rawRows = [];
    while (hasNext) {
      const query = `SELECT WS_Account_code FROM ${tableName} LIMIT ${limit} OFFSET ${offset}`;
      const result = await zcql.executeZCQLQuery(query);
      rawRows.push(...result);
      offset += limit;
      if (!result.length) {
        hasNext = false;
      }
    }

    const seen = new Set();
    const clientIds = [];
    for (const row of rawRows) {
      const r = row.clientIds || row;
      const code = (r.WS_Account_code ?? "").toString().trim();
      if (!code || seen.has(code)) continue;
      seen.add(code);
      clientIds.push({ clientIds: { WS_Account_code: code } });
    }

    return clientIds.sort((a, b) =>
      (a.clientIds.WS_Account_code || "").localeCompare(b.clientIds.WS_Account_code || "")
    );
  } catch (error) {
    console.error("Error fetching account codes:", error);
    throw error;
  }
};
