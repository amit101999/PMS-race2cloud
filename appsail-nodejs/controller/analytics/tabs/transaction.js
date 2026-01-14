export const getPaginatedTransactions = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) return res.status(500).json({ message: "Catalyst app missing" });
    const zcql = app.zcql();

    const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
    const limit = Math.max(parseInt(req.query.limit, 10) || 20, 1);
    const offset = (page - 1) * limit;

    const accountCode = (req.query.accountCode || "").trim();
    const rawAsOnDate = (req.query.asOnDate || "").trim();
    const normalizeDate = (d) => {
      if (!d) return null;
      if (/^\d{4}-\d{2}-\d{2}$/.test(d)) return d;
      if (/^\d{2}\/\d{2}\/\d{4}$/.test(d)) {
        const [dd, mm, yyyy] = d.split("/");
        return `${yyyy}-${mm}-${dd}`;
      }
      return null;
    };

    const asOnDate = normalizeDate(rawAsOnDate);

    const whereClauses = [];
    if (accountCode) {
      whereClauses.push(
        `WS_Account_code = '${accountCode.replace(/'/g, "''")}'`
      );
    }
    if (asOnDate) {
      const nextDay = new Date(asOnDate);
      nextDay.setDate(nextDay.getDate() + 1);
      const nextDayStr = nextDay.toISOString().split("T")[0];
      whereClauses.push(`TRANDATE < '${nextDayStr}'`);
    }
    // getPaginatedTransactions: add this near accountCode/asOnDate
    const securityName = (req.query.securityName || "").trim();
    if (securityName) {
      whereClauses.push(`Security_Name = '${securityName.replace(/'/g, "''")}'`);
    }
    const whereSql =
      whereClauses.length ? `WHERE ${whereClauses.join(" AND ")}` : "";

    const countRows = await zcql.executeZCQLQuery(`
      SELECT COUNT(ROWID) AS total
      FROM Transaction
      ${whereSql}
    `);

    let total = 0;
    if (countRows && countRows.length) {
      const r = countRows[0].Transaction || countRows[0];
      total =
        Number(r.total) ||
        Number(r["COUNT(ROWID)"]) ||
        Number(Object.values(r).find((v) => !isNaN(Number(v))) || 0);
    }

    const rows = await zcql.executeZCQLQuery(`
      SELECT
        ROWID,
        TRANDATE,
        Tran_Type,
        Security_Name,
        Security_code,
        ISIN,
        QTY,
        NETRATE,
        Net_Amount
      FROM Transaction
      ${whereSql}
      ORDER BY TRANDATE DESC, ROWID DESC
      LIMIT ${limit} OFFSET ${offset}
    `);

    const transactions = rows.map((row) => {
      const t = row.Transaction || row;
      return {
        rowId: t.ROWID,
        date: t.TRANDATE,
        type: t.Tran_Type,
        securityName: t.Security_Name,
        securityCode: t.Security_code,
        isin: t.ISIN,
        quantity: Number(t.QTY) || 0,
        price: Number(t.NETRATE) || 0,
        totalAmount: Number(t.Net_Amount) || 0,
      };
    });

    return res.status(200).json({
      data: transactions,
      pagination: {
        page,
        limit,
        total,
        totalPages: Math.ceil(total / limit) || 1,
      },
    });
  } catch (err) {
    console.error("[getPaginatedTransactions]", err);
    return res
      .status(500)
      .json({ message: "Failed to fetch transactions", error: err.message });
  }
};
export const getSecurityNameOptions = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) return res.status(500).json({ message: "Catalyst app missing" });
    const zcql = app.zcql();

    const rawSearch = (req.query.search || "").trim();
    const accountCode = (req.query.accountCode || "").trim();
    const safe = rawSearch.replace(/'/g, "''").toLowerCase();

    const clauses = [];
    if (accountCode) clauses.push(`WS_Account_code = '${accountCode.replace(/'/g, "''")}'`);
    if (safe) clauses.push(`LOWER(Security_Name) LIKE '%${safe}%'`);

    const whereSql = clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";

    const rows = await zcql.executeZCQLQuery(`
      SELECT DISTINCT Security_Name
      FROM Transaction
      ${whereSql}
      ORDER BY Security_Name ASC
    `);

    const names = rows.map((r) => (r.Transaction || r).Security_Name).filter(Boolean);
    return res.status(200).json({ data: names });
  } catch (err) {
    console.error("[getSecurityNameOptions]", err);
    return res.status(500).json({ message: "Failed to fetch security names", error: err.message });
  }
};