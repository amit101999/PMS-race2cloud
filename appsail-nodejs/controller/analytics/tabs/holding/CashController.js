
// CashController.js

export const getCashBalanceSummary = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();

    const { accountCode, asOnDate } = req.query;

    if (!accountCode) {
      return res.status(400).json({ message: "accountCode required" });
    }

    let dateCondition = "";

    // Validate and apply asOnDate filter
    if (asOnDate && /^\d{4}-\d{2}-\d{2}$/.test(asOnDate)) {
      const nextDay = new Date(asOnDate);
      nextDay.setDate(nextDay.getDate() + 1);
      const nextDayStr = nextDay.toISOString().split("T")[0];

      dateCondition = `AND Tran_Date < '${nextDayStr}'`;
    }

    /*
      Logic:
      - Order by Tran_Date DESC
      - ROWID DESC ensures deterministic "latest" record
      - LIMIT 1 gives latest cash balance
    */
    const rows = await zcql.executeZCQLQuery(`
      SELECT Account_Code, Tran_Date, Cash_Balance, Last_Tran_Id, ROWID
      FROM Cash_Ledger
      WHERE Account_Code = '${accountCode}'
      ${dateCondition}
      ORDER BY Tran_Date DESC, ROWID DESC
      LIMIT 1
    `);

    if (!rows.length) {
      return res.json({
        accountCode,
        cashBalance: 0,
        asOnDate: asOnDate || null,
        message: "No cash ledger entries found",
      });
    }

    const row = rows[0].Cash_Ledger || rows[0];

    return res.json({
      accountCode: row.Account_Code,
      cashBalance: row.Cash_Balance || 0,
      tranDate: row.Tran_Date,
      lastTranId: row.Last_Tran_Id,
    });
  } catch (error) {
    console.error("Cash balance fetch error:", error);
    res.status(500).json({
      message: "Failed to fetch cash balance",
      error: error.message,
    });
  }
};
