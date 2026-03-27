const isBuyType = (type) => /^BY-|SQB|OPI/i.test(String(type || ""));

const getEffectiveDate = (r) => {
  const setDate = r.SETDATE || r.setdate;
  const tradeDate = r.TRANDATE || r.trandate;
  return isBuyType(r.Tran_Type || r.tranType)
    ? setDate || tradeDate
    : tradeDate || setDate;
};

export const fetchStockTransactions = async ({
  zcql,
  tableName,
  accountCode,
  // securityCode,
  isin,
  asOnDate,
}) => {
  let dateCondition = "";
  let cutoff = null;

  if (asOnDate && /^\d{4}-\d{2}-\d{2}$/.test(asOnDate)) {
    const nextDay = new Date(asOnDate);
    nextDay.setDate(nextDay.getDate() + 1);
    const nextDayStr = nextDay.toISOString().split("T")[0];
    dateCondition = ` AND (TRANDATE < '${nextDayStr}' OR SETDATE < '${nextDayStr}')`;
    cutoff = nextDayStr;
  }

  const where = `
    WHERE WS_Account_code = '${accountCode.replace(/'/g, "''")}'
    AND ISIN = '${isin.replace(/'/g, "''")}'
    ${dateCondition}
  `;

  const rows = [];
  const seenRowIds = new Set();
  let offset = 0;
  const limit = 250;

  while (true) {
    try {
      const query = `
        SELECT SETDATE, TRANDATE, Tran_Type, Security_code, QTY, NETRATE, Net_Amount, ISIN, ROWID
        FROM Transaction
        ${where}
        ORDER BY SETDATE ASC, ROWID ASC
        LIMIT ${limit} OFFSET ${offset}
      `;

      const batch = await zcql.executeZCQLQuery(query);
      if (!batch || batch.length === 0) break;

      for (const row of batch) {
        const r = row.Transaction || row[tableName] || row;
        if (r.ROWID && seenRowIds.has(r.ROWID)) continue;
        if (r.ROWID) seenRowIds.add(r.ROWID);
        rows.push(r);
      }

      if (batch.length < limit) break;
      offset += limit;
    } catch (err) {
      console.error(`Error fetching transactions for ${accountCode}/${isin} at offset ${offset}:`, err);
      break;
    }
  }

  const filteredRows = cutoff
    ? rows.filter((r) => {
        const effectiveDate = getEffectiveDate(r);
        return !effectiveDate || effectiveDate < cutoff;
      })
    : rows;

  return filteredRows.map((r) => ({
    SETDATE: r.SETDATE,
    TRANDATE: r.TRANDATE,
    tranType: r.Tran_Type,
    securityCode: r.Security_code,
    qty: Number(r.QTY) || 0,
    netrate: Number(r.NETRATE) || 0,
    netAmount: Number(r.Net_Amount) || 0,
    isin: r.ISIN || "",
  }));
};
