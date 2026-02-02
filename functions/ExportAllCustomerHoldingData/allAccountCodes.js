exports.getAllAccountCodesFromDatabase = async (zcql, tableName) => {
  try {
    let offset = 0;
    let limit = 270;
    let hasNext = true;

    const rawRows = [];
    while (hasNext) {
      const query = `SELECT WS_Account_code FROM ${tableName} LIMIT ${limit} OFFSET ${offset}`;
      const result = await zcql.executeZCQLQuery(query);
      rawRows.push(...result);
      offset = offset + limit;
      if (result.length <= 0) {
        hasNext = false;
      }
    }

    // Deduplicate by WS_Account_code (table may have multiple rows per account)
    const seen = new Set();
    const cliendIds = [];
    for (const row of rawRows) {
      const r = row.clientIds || row;
      const code = (r.WS_Account_code ?? "").toString().trim();
      if (!code || seen.has(code)) continue;
      seen.add(code);
      cliendIds.push({ clientIds: { WS_Account_code: code } });
    }

    return cliendIds.sort((a, b) =>
      (a.clientIds.WS_Account_code || "").localeCompare(b.clientIds.WS_Account_code || "")
    );
  } catch (error) {
    console.error("Error fetching account codes:", error);
    throw error;
  }
};
