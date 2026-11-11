// Copy of appsail-nodejs/util/allAccountCodes.js. Keep the two in step.

const getAllAccountCodesFromDatabase = async (zcql, tableName) => {
  let offset = 0;
  const limit = 270;
  let hasNext = true;

  const rawRows = [];
  while (hasNext) {
    const query = `SELECT WS_Account_code FROM ${tableName} LIMIT ${limit} OFFSET ${offset}`;
    const result = await zcql.executeZCQLQuery(query);
    rawRows.push(...result);
    offset = offset + limit;
    if (result.length < limit) {
      hasNext = false;
    }
  }

  // The table can hold several rows per account, and OFFSET paging repeats
  // the row at each page boundary, so remove repeats.
  const seen = new Set();
  const codes = [];
  for (const row of rawRows) {
    const r = row.clientIds || row;
    const code = (r.WS_Account_code ?? "").toString().trim();
    if (!code || seen.has(code)) continue;
    seen.add(code);
    codes.push(code);
  }

  return codes.sort((a, b) => a.localeCompare(b));
};

module.exports = { getAllAccountCodesFromDatabase };
