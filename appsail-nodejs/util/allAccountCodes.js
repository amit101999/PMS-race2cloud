export const getAllAccountCodesFromDatabase = async (zcql, tableName) => {
  try {
    let offset = 0;
    let limit = 270;
    let hasNext = true;

    let cliendIds = [];
    while (hasNext) {
      let query = `select WS_Account_code from ${tableName} limit ${limit} offset ${offset}`;
      let result = await zcql.executeZCQLQuery(query);
      cliendIds.push(...result);
      offset = offset + limit;
      if (result.length <= 0) {
        hasNext = false;
      }
    }
    return cliendIds;
  } catch (error) {
    console.error("Error fetching account codes:", error);
    throw error;
  }
};
