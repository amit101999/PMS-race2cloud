const CASH_ADD = ["CS+", "SL+", "CSI", "IN1", "IN+", "DIO", "DI0", "DI1", "OI1", "DIS", "SQS", "DIVIDEND"];
const CASH_SUBTRACT = ["BY-", "CS-", "MGF", "E22", "E01", "CUS", "E23", "MGE", "E10", "PRF", "NF-", "SQB", "TDO", "TDI"];
const BATCH = 300;

export const getIsinList = async (req, res) => {
  try {
    const zcql = req.catalystApp.zcql();
    const accountCode = (req.query.accountCode || "").trim();
    if (!accountCode) return res.status(400).json({ message: "accountCode is required" });

    const esc = (s) => String(s).replace(/'/g, "''");
    const isinMap = new Map();
    let offset = 0;

    while (true) {
      const rows = await zcql.executeZCQLQuery(
        `SELECT ISIN, Security_Name FROM Cash_Balance_Per_Transaction WHERE Account_Code = '${esc(accountCode)}' AND ISIN IS NOT NULL LIMIT ${BATCH} OFFSET ${offset}`
      );
      if (!rows || rows.length === 0) break;
      for (const r of rows) {
        const row = r.Cash_Balance_Per_Transaction || r;
        const isin = (row.ISIN || "").trim();
        const name = (row.Security_Name || "").trim();
        if (isin && !isinMap.has(isin)) {
          isinMap.set(isin, name);
        }
      }
      if (rows.length < BATCH) break;
      offset += BATCH;
    }

    const data = Array.from(isinMap.entries())
      .map(([isin, name]) => ({ isin, name: name || isin }))
      .sort((a, b) => a.name.localeCompare(b.name));

    return res.json({ data });
  } catch (error) {
    console.error("ISIN list fetch error:", error);
    res.status(500).json({ message: "Failed to fetch ISIN list", error: error.message });
  }
};

export const getCashPassbook = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();

    const {
      accountCode,
      page = "1",
      pageSize = "25",
      fromDate,
      toDate,
      search,
      isin,
    } = req.query;

    if (!accountCode) {
      return res.status(400).json({ message: "accountCode is required" });
    }

    const pg = Math.max(1, parseInt(page, 10) || 1);
    const size = Math.min(100, Math.max(1, parseInt(pageSize, 10) || 25));
    const offset = (pg - 1) * size;

    let conditions = `Account_Code = '${accountCode}'`;

    if (fromDate && /^\d{4}-\d{2}-\d{2}$/.test(fromDate)) {
      conditions += ` AND Transaction_Date >= '${fromDate}'`;
    }

    if (toDate && /^\d{4}-\d{2}-\d{2}$/.test(toDate)) {
      const nextDay = new Date(toDate);
      nextDay.setDate(nextDay.getDate() + 1);
      const nextDayStr = nextDay.toISOString().split("T")[0];
      conditions += ` AND Transaction_Date < '${nextDayStr}'`;
    }

    if (isin && isin.trim()) {
      const i = isin.trim().replace(/'/g, "''");
      conditions += ` AND ISIN = '${i}'`;
    } else if (search && search.trim()) {
      const s = search.trim().replace(/'/g, "''");
      conditions += ` AND (ISIN LIKE '%${s}%' OR Security_Name LIKE '%${s}%')`;
    }

    const countResult = await zcql.executeZCQLQuery(
      `SELECT COUNT(ROWID) FROM Cash_Balance_Per_Transaction WHERE ${conditions}`
    );
    const countRow = countResult?.[0]?.Cash_Balance_Per_Transaction || countResult?.[0] || {};
    const totalCount = Number(
      countRow["COUNT(ROWID)"] ?? countRow.cnt ?? countRow["count"] ?? Object.values(countRow)[0] ?? 0
    );

    const dataRows = await zcql.executeZCQLQuery(
      `SELECT ROWID, Account_Code, Transaction_Date, Settlement_Date, ISIN, Security_Name,
              Transaction_Type, Quantity, Price, Total_Amount, Cash_Balance, STT, Sequence
       FROM Cash_Balance_Per_Transaction
       WHERE ${conditions}
       ORDER BY Sequence ASC
       LIMIT ${offset}, ${size}`
    );

    const data = dataRows.map((r) => {
      const row = r.Cash_Balance_Per_Transaction || r;
      const tranType = row.Transaction_Type || "";
      const amount = Number(row.Total_Amount) || 0;

      let debit = null;
      let credit = null;
      if (CASH_SUBTRACT.includes(tranType)) {
        debit = amount;
      } else if (CASH_ADD.includes(tranType)) {
        credit = amount;
      }

      return {
        ROWID: row.ROWID,
        Account_Code: row.Account_Code,
        Transaction_Date: (row.Transaction_Date || "").toString().slice(0, 10),
        Settlement_Date: (row.Settlement_Date || "").toString().slice(0, 10),
        ISIN: row.ISIN,
        Security_Name: row.Security_Name,
        Transaction_Type: tranType,
        Quantity: row.Quantity,
        Price: row.Price,
        Total_Amount: row.Total_Amount,
        Cash_Balance: row.Cash_Balance,
        STT: row.STT,
        Sequence: row.Sequence,
        Debit: debit,
        Credit: credit,
      };
    });

    return res.json({
      data,
      totalCount,
      page: pg,
      pageSize: size,
      totalPages: Math.ceil(totalCount / size),
    });
  } catch (error) {
    console.error("Cash passbook fetch error:", error);
    res.status(500).json({
      message: "Failed to fetch cash passbook",
      error: error.message,
    });
  }
};
