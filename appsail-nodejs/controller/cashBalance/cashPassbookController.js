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
    } = req.query;

    if (!accountCode) {
      return res.status(400).json({ message: "accountCode is required" });
    }

    const pg = Math.max(1, parseInt(page, 10) || 1);
    const size = Math.min(100, Math.max(1, parseInt(pageSize, 10) || 25));
    const offset = (pg - 1) * size;

    let conditions = `Account_Code = '${accountCode}'`;

    if (fromDate && /^\d{4}-\d{2}-\d{2}$/.test(fromDate)) {
      conditions += ` AND Impact_Date >= '${fromDate}'`;
    }

    if (toDate && /^\d{4}-\d{2}-\d{2}$/.test(toDate)) {
      const nextDay = new Date(toDate);
      nextDay.setDate(nextDay.getDate() + 1);
      const nextDayStr = nextDay.toISOString().split("T")[0];
      conditions += ` AND Impact_Date < '${nextDayStr}'`;
    }

    if (search && search.trim()) {
      const s = search.trim().replace(/'/g, "''");
      conditions += ` AND (ISIN LIKE '%${s}%' OR Security_Name LIKE '%${s}%' OR Security_Code LIKE '%${s}%')`;
    }

    const countResult = await zcql.executeZCQLQuery(
      `SELECT COUNT(ROWID) AS cnt FROM Cash_Balance_Per_Transaction WHERE ${conditions}`
    );
    const totalCount =
      Number(
        countResult?.[0]?.Cash_Balance_Per_Transaction?.cnt ??
        countResult?.[0]?.cnt ??
        0
      );

    const dataRows = await zcql.executeZCQLQuery(
      `SELECT ROWID, Account_Code, Impact_Date, ISIN, Security_Name, Security_Code,
              Tran_Type, QTY, Rate, Debit, Credit, Running_Balance
       FROM Cash_Balance_Per_Transaction
       WHERE ${conditions}
       ORDER BY Impact_Date ASC, ROWID ASC
       LIMIT ${offset}, ${size}`
    );

    const data = dataRows.map((r) => {
      const row = r.Cash_Balance_Per_Transaction || r;
      return {
        ROWID: row.ROWID,
        Account_Code: row.Account_Code,
        Impact_Date: row.Impact_Date,
        ISIN: row.ISIN,
        Security_Name: row.Security_Name,
        Security_Code: row.Security_Code,
        Tran_Type: row.Tran_Type,
        QTY: row.QTY,
        Rate: row.Rate,
        Debit: row.Debit,
        Credit: row.Credit,
        Running_Balance: row.Running_Balance,
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
