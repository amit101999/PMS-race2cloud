export const applyCashEffect = (balance, tranType, amount) => {
  if (
    tranType === "CS+" ||
    tranType === "SL+" ||
    tranType === "DIO" ||
    tranType === "DI0" ||
    tranType === "CSI" ||
    tranType === "DIS" ||
    tranType === "IN1" ||
    tranType === "OI1" ||
    tranType === "SQS" ||
    tranType === "DI1" ||
    tranType === "IN+"
  ) {
    return balance + amount;
  }

  if (
    tranType === "BY-" ||
    tranType === "MGF" ||
    tranType === "TDO" ||
    tranType === "TDI" ||
    tranType === "E22" ||
    tranType === "E01" ||
    tranType === "CUS" ||
    tranType === "E23" ||
    tranType === "CS-" ||
    tranType === "MGE" ||
    tranType === "E10" ||
    tranType === "PRF" ||
    tranType === "NF-" ||
    tranType === "SQB"
  ) {
    return balance - amount;
  }

  return balance;
};

export const getPaginatedTransactions = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app missing" });
    }

    const zcql = app.zcql();
    const limit = Math.max(parseInt(req.query.limit, 10) || 20, 1);

    /* ================= CURSOR ================= */
    const lastDate = (req.query.lastDate || "").trim() || null;
    const lastPriority =
      req.query.lastPriority != null
        ? parseInt(String(req.query.lastPriority), 10)
        : null;
    const lastRowId =
      req.query.lastRowId != null
        ? parseInt(String(req.query.lastRowId), 10)
        : null;

    const direction = req.query.direction === "prev" ? "prev" : "next";

    const hasCursor =
      lastDate &&
      lastPriority != null &&
      !Number.isNaN(lastPriority) &&
      lastRowId != null &&
      !Number.isNaN(lastRowId);

    /* ================= FILTERS ================= */
    const accountCode = (req.query.accountCode || "").trim();
    const rawAsOnDate = (req.query.asOnDate || "").trim();
    const securityName = (req.query.securityName || "").trim();

    if (!accountCode) {
      return res.status(200).json({
        data: [],
        nextCursor: null,
        prevCursor: null,
        hasNext: false,
        hasPrev: false,
      });
    }

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

    const whereParts = [`WS_Account_code = '${accountCode.replace(/'/g, "''")}'`];

    if (asOnDate) {
      const nextDay = new Date(asOnDate);
      nextDay.setDate(nextDay.getDate() + 1);
      const nextDayStr = nextDay.toISOString().split("T")[0];
      whereParts.push(`TRANDATE < '${nextDayStr}'`);
    }

    if (securityName) {
      whereParts.push(
        `Security_Name = '${securityName.replace(/'/g, "''")}'`
      );
    }

    let whereSql = `WHERE ${whereParts.join(" AND ")}`;

    /* ================= CURSOR CONDITION ================= */

    if (hasCursor) {
      const operator =
        direction === "next"
          ? `
            TRANDATE > '${lastDate}'
            OR (
              TRANDATE = '${lastDate}'
              AND executionPriority > ${lastPriority}
            )
            OR (
              TRANDATE = '${lastDate}'
              AND executionPriority = ${lastPriority}
              AND ROWID > ${lastRowId}
            )
          `
          : `
            TRANDATE < '${lastDate}'
            OR (
              TRANDATE = '${lastDate}'
              AND executionPriority < ${lastPriority}
            )
            OR (
              TRANDATE = '${lastDate}'
              AND executionPriority = ${lastPriority}
              AND ROWID < ${lastRowId}
            )
          `;

      whereSql += ` AND (${operator})`;
    }

    /* ================= ORDERING ================= */

    const orderSql =
      direction === "next"
        ? "ORDER BY TRANDATE ASC, executionPriority ASC, ROWID ASC"
        : "ORDER BY TRANDATE DESC, executionPriority DESC, ROWID DESC";

    /* ================= FETCH PAGE ================= */

    const rowsRaw = await zcql.executeZCQLQuery(`
      SELECT
        ROWID,
        TRANDATE,
        executionPriority,
        Tran_Type,
        Security_Name,
        Security_code,
        ISIN,
        QTY,
        NETRATE,
        Net_Amount,
        STT
      FROM Transaction
      ${whereSql}
      ${orderSql}
      LIMIT 0, ${limit}
    `);

    const rows = direction === "prev"
      ? [...(rowsRaw || [])].reverse()
      : rowsRaw || [];

    if (rows.length === 0) {
      return res.status(200).json({
        data: [],
        nextCursor: null,
        prevCursor: null,
        hasNext: false,
        hasPrev: hasCursor,
      });
    }

    const getRow = (row) => {
      const t = row.Transaction || row;
      return {
        TRANDATE: t.TRANDATE ?? t.Trandate ?? null,
        executionPriority: Number(t.executionPriority ?? t.ExecutionPriority ?? 0) || 0,
        ROWID: t.ROWID ?? t.rowid ?? null,
      };
    };

    /* ================= OPENING BALANCE ================= */

    const first = getRow(rows[0]);
    const firstDate = first.TRANDATE;
    const firstPriority = first.executionPriority;
    const firstRowId = first.ROWID;

    let openingBalance = 0;

    if (
      firstDate != null &&
      firstRowId != null &&
      !Number.isNaN(Number(firstRowId))
    ) {
      const openingWhere = `
      WHERE ${whereParts.join(" AND ")}
      AND (
        TRANDATE < '${firstDate}'
        OR (
          TRANDATE = '${firstDate}'
          AND executionPriority < ${firstPriority}
        )
        OR (
          TRANDATE = '${firstDate}'
          AND executionPriority = ${firstPriority}
          AND ROWID < ${firstRowId}
        )
      )
      ORDER BY TRANDATE ASC, executionPriority ASC, ROWID ASC
    `;

      const BATCH = 300;
      let offset = 0;

      try {
        while (true) {
          const batch = await zcql.executeZCQLQuery(`
            SELECT Tran_Type, Net_Amount, STT
            FROM Transaction
            ${openingWhere}
            LIMIT ${offset}, ${BATCH}
          `);

          if (!batch || batch.length === 0) break;

          for (const row of batch) {
            const t = row.Transaction || row;
            const stt = Number(t.STT ?? t.Stt ?? 0) || 0;
            openingBalance = applyCashEffect(
              openingBalance,
              t.Tran_Type,
              Number(t.Net_Amount) || 0
            );
            openingBalance -= stt;
          }

          if (batch.length < BATCH) break;
          offset += BATCH;
        }
      } catch (openingErr) {
        console.error("[getPaginatedTransactions] opening balance:", openingErr);
        openingBalance = 0;
      }
    }

    /* ================= RUNNING BALANCE ================= */

    let runningBalance = openingBalance;

    const transactions = rows.map((row) => {
      const t = row.Transaction || row;
      const stt = Number(t.STT ?? t.Stt ?? 0) || 0;
      runningBalance = applyCashEffect(
        runningBalance,
        t.Tran_Type,
        Number(t.Net_Amount) || 0
      );
      runningBalance -= stt;

      return {
        rowId: t.ROWID,
        date: t.TRANDATE,
        executionPriority: Number(t.executionPriority) || 0,
        type: t.Tran_Type,
        securityName: t.Security_Name,
        securityCode: t.Security_code,
        isin: t.ISIN,
        quantity: Number(t.QTY) || 0,
        price: Number(t.NETRATE) || 0,
        totalAmount: Number(t.Net_Amount) || 0,
        stt: stt,
        cashBalance: runningBalance,
      };
    });

    /* ================= CURSORS ================= */

    const firstRow = getRow(rows[0]);
    const lastRow = getRow(rows[rows.length - 1]);

    const prevCursor =
      firstRow.TRANDATE != null && firstRow.ROWID != null
        ? {
            lastDate: firstRow.TRANDATE,
            lastPriority: firstRow.executionPriority,
            lastRowId: firstRow.ROWID,
          }
        : null;

    const nextCursor =
      lastRow.TRANDATE != null && lastRow.ROWID != null
        ? {
            lastDate: lastRow.TRANDATE,
            lastPriority: lastRow.executionPriority,
            lastRowId: lastRow.ROWID,
          }
        : null;

    return res.status(200).json({
      data: transactions,
      nextCursor,
      prevCursor,
      hasNext: transactions.length === limit,
      hasPrev: hasCursor,
    });

  } catch (err) {
    console.error("[getPaginatedTransactions]", err);
    return res.status(500).json({
      message: "Failed to fetch transactions",
      error: err.message,
    });
  }
};

// export const getSecurityNameOptions = async (req, res) => {
//   try {
//     const app = req.catalystApp;
//     if (!app) return res.status(500).json({ message: "Catalyst app missing" });
//     const zcql = app.zcql();

//     const rawSearch = (req.query.search || "").trim();
//     const accountCode = (req.query.accountCode || "").trim();
//     const safe = rawSearch.replace(/'/g, "''").toLowerCase();

//     const clauses = [];
//     if (accountCode)
//       clauses.push(`WS_Account_code = '${accountCode.replace(/'/g, "''")}'`);
//     if (safe) clauses.push(`LOWER(Security_Name) LIKE '%${safe}%'`);

//     const whereSql = clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
//     const table = "Transaction";

//     const rows = await zcql.executeZCQLQuery(`
//       SELECT DISTINCT Security_Name
//       FROM ${table}
//       ${whereSql}
//       ORDER BY Security_Name ASC
//     `);

//     const names = rows
//       .map((r) => (r.Transaction || r).Security_Name)
//       .filter(Boolean);
//     return res.status(200).json({ data: names });
//   } catch (err) {
//     console.error("[getSecurityNameOptions]", err);
//     return res
//       .status(500)
//       .json({ message: "Failed to fetch security names", error: err.message });
//   }
// };
export const getSecurityNameOptions = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app missing" });
    }

    const zcql = app.zcql();

    const rawSearch = (req.query.search || "").trim();
    const accountCode = (req.query.accountCode || "").trim();
    const safe = rawSearch.replace(/'/g, "''").toLowerCase();

    const clauses = [];
    if (accountCode) {
      clauses.push(`WS_Account_code = '${accountCode.replace(/'/g, "''")}'`);
    }
    if (safe) {
      clauses.push(`LOWER(Security_Name) LIKE '%${safe}%'`);
    }

    const whereSql = clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
    const table = "Transaction";

    const limit = 300; // Catalyst max
    let offset = 0;

    const securityNameSet = new Set();

    while (true) {
      const rows = await zcql.executeZCQLQuery(`
        SELECT DISTINCT Security_Name
        FROM ${table}
        ${whereSql}
        ORDER BY Security_Name ASC
        LIMIT ${limit} OFFSET ${offset}
      `);

      if (!rows.length) break;

      for (const r of rows) {
        const row = r.Transaction || r;
        if (row.Security_Name) {
          securityNameSet.add(row.Security_Name);
        }
      }

      if (rows.length < limit) break;
      offset += limit;
    }

    return res.status(200).json({
      data: Array.from(securityNameSet),
    });
  } catch (err) {
    console.error("[getSecurityNameOptions]", err);
    return res.status(500).json({
      message: "Failed to fetch security names",
      error: err.message,
    });
  }
};
