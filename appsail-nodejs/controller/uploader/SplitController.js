import { runFifoEngine } from "../../util/analytics/transactionHistory/fifo.js";

export const getAllSecuritiesISINs = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app not initialized" });
    }
    const zcql = app.zcql();

    const LIMIT = 300;
    let offset = 0;
    let hasMore = true;

    const securities = [];

    while (hasMore) {
      const query = `
        SELECT ISIN, Security_Code, Security_Name
        FROM Security_List
        WHERE ISIN IS NOT NULL
        LIMIT ${LIMIT} OFFSET ${offset}
      `;

      const response = await zcql.executeZCQLQuery(query);

      if (!response || response.length === 0) {
        hasMore = false;
        break;
      }

      response.forEach((row) => {
        const sec = row.Security_List;
        securities.push({
          isin: sec.ISIN,
          securityCode: sec.Security_Code,
          securityName: sec.Security_Name,
        });
      });

      offset += LIMIT;
    }

    return res.status(200).json({
      success: true,
      count: securities.length,
      data: securities,
    });
  } catch (error) {
    console.error("Error fetching securities:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to fetch security list",
    });
  }
};

// export const addStockSplit = async (req, res) => {
//   try {
//     const zohoCatalyst = req.catalystApp;
//     let zcql = zohoCatalyst.zcql();

//     console.log("boyd data is :::", req);
//     const { securityCode, securityName, ratio1, ratio2, issueDate, isin } =
//       req.body;

//     if (!securityCode || !securityName || !ratio1 || !ratio2 || !issueDate) {
//       return res.status(400).json({
//         message: "Missing required fields",
//       });
//     }
//     /* ===========================
//          INSERT INTO SPLIT TABLE
//          =========================== */
//     await zcql.executeZCQLQuery(`
//         INSERT INTO Split
//         (
//           Security_Code,
//           Security_Name,
//           Ratio1,
//           Ratio2,
//           Issue_Date,
//           ISIN
//         )
//         VALUES
//         (
//           '${securityCode}',
//           '${securityName}',
//           ${Number(ratio1)},
//           ${Number(ratio2)},
//           '${issueDate}',
//           '${isin}'
//         )
//       `);

//     return res.status(200).json({
//       message: "Stock split saved successfully",
//     });
//   } catch (error) {
//     console.log("Error in saving split", error);
//     res.status(400).json({ error: error.message });
//   }
// };
const fetchAllAccountsForISIN = async (zcql, isin) => {
  const LIMIT = 300;
  let offset = 0;
  let hasMore = true;

  const accountSet = new Set();

  while (hasMore) {
    const res = await zcql.executeZCQLQuery(`
      SELECT WS_Account_code
      FROM Transaction
      WHERE ISIN='${isin}'
      LIMIT ${LIMIT} OFFSET ${offset}
    `);

    if (!res || res.length === 0) {
      hasMore = false;
      break;
    }

    res.forEach((row) => {
      accountSet.add(row.Transaction.Account_Code);
    });

    offset += LIMIT;
  }

  return Array.from(accountSet);
}

const ZCQL_ROW_LIMIT = 270;

export const previewStockSplit = async (req, res) => {
  try {
    if (!req.catalystApp) {
      return res.status(500).json({
        success: false,
        message: "Catalyst app not initialized",
      });
    }

    const { isin, ratio1, ratio2, issueDate } = req.body;

    const r1 = Number(ratio1);
    const r2 = Number(ratio2);

    if (!isin || !issueDate || !Number.isFinite(r1) || !Number.isFinite(r2) || r1 <= 0) {
      return res.status(400).json({
        success: false,
        message: "Invalid ISIN, ratio or date",
      });
    }

    /* Normalize Issue Date */
    const issueDateObj = new Date(issueDate);
    issueDateObj.setHours(0, 0, 0, 0);
    const issueDateISO = issueDateObj.toISOString().split("T")[0];

    const zcql = req.catalystApp.zcql();

    /* ======================================================
       STEP 1: FETCH HOLDINGS (ELIGIBLE ACCOUNTS)
       ====================================================== */
    const holdingRows = [];
    let holdOffset = 0;

    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT WS_Account_code, QTY
        FROM Holdings
        WHERE ISIN='${isin}' AND QTY > 0
        ORDER BY ROWID ASC
        LIMIT ${ZCQL_ROW_LIMIT} OFFSET ${holdOffset}
      `);

      if (!batch || batch.length === 0) break;
      holdingRows.push(...batch);
      if (batch.length < ZCQL_ROW_LIMIT) break;
      holdOffset += ZCQL_ROW_LIMIT;
    }

    if (!holdingRows.length) {
      return res.json({ success: true, data: [] });
    }

    /* ======================================================
       STEP 2: FETCH TRANSACTIONS ≤ ISSUE DATE
       ====================================================== */
    const txRows = [];
    let txOffset = 0;

    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT *
        FROM Transaction
        WHERE ISIN='${isin}'
        AND TRANDATE <= '${issueDateISO}'
        ORDER BY TRANDATE ASC, ROWID ASC
        LIMIT ${ZCQL_ROW_LIMIT} OFFSET ${txOffset}
      `);

      if (!batch || batch.length === 0) break;
      txRows.push(...batch);
      if (batch.length < ZCQL_ROW_LIMIT) break;
      txOffset += ZCQL_ROW_LIMIT;
    }

    /* ======================================================
       STEP 3: FETCH BONUSES < ISSUE DATE
       ====================================================== */
    const bonusRows = [];
    let bonusOffset = 0;

    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT *
        FROM Bonus
        WHERE ISIN='${isin}'
        AND ExDate < '${issueDateISO}'
        ORDER BY ExDate ASC, ROWID ASC
        LIMIT ${ZCQL_ROW_LIMIT} OFFSET ${bonusOffset}
      `);

      if (!batch || batch.length === 0) break;
      bonusRows.push(...batch);
      if (batch.length < ZCQL_ROW_LIMIT) break;
      bonusOffset += ZCQL_ROW_LIMIT;
    }

    /* ======================================================
       STEP 4: FETCH SPLITS < ISSUE DATE
       ====================================================== */
    const splitRows = [];
    let splitOffset = 0;

    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT *
        FROM Split
        WHERE ISIN='${isin}'
        AND Issue_Date < '${issueDateISO}'
        ORDER BY Issue_Date ASC, ROWID ASC
        LIMIT ${ZCQL_ROW_LIMIT} OFFSET ${splitOffset}
      `);

      if (!batch || batch.length === 0) break;
      splitRows.push(...batch);
      if (batch.length < ZCQL_ROW_LIMIT) break;
      splitOffset += ZCQL_ROW_LIMIT;
    }

    /* ======================================================
       STEP 5: GROUP DATA BY ACCOUNT
       ====================================================== */
    const txByAccount = {};
    txRows.forEach((r) => {
      const t = r.Transaction;
      if (!txByAccount[t.WS_Account_code]) txByAccount[t.WS_Account_code] = [];
      txByAccount[t.WS_Account_code].push(t);
    });

    const bonusByAccount = {};
    bonusRows.forEach((r) => {
      const b = r.Bonus;
      if (!bonusByAccount[b.WS_Account_code]) bonusByAccount[b.WS_Account_code] = [];
      bonusByAccount[b.WS_Account_code].push(b);
    });

    const splits = splitRows.map((r) => r.Split);

    /* ======================================================
       STEP 6: FIFO PREVIEW
       ====================================================== */
    const preview = [];

    for (const h of holdingRows) {
      const accountCode = h.Holdings.WS_Account_code;
      const transactions = txByAccount[accountCode] || [];
      if (!transactions.length) continue;

      const bonuses = bonusByAccount[accountCode] || [];

      // Run FIFO ONCE to get holding as of split date
      const fifoBefore = runFifoEngine(transactions, bonuses, splits, true);

      if (!fifoBefore || fifoBefore.holdings <= 0) continue;

      // Apply split directly on quantity
      const splitMultiplier = r2 / r1;

      // Choose rounding rule (MOST brokers use floor)
      const newHolding = Math.floor(fifoBefore.holdings * splitMultiplier);

      preview.push({
        isin,
        accountCode,
        currentHolding: fifoBefore.holdings,
        newHolding,
        delta: newHolding - fifoBefore.holdings,
      });
    }

    return res.json({ success: true, data: preview });
  } catch (error) {
    console.error("Preview split error:", error);
    return res.status(500).json({
      success: false,
      message: error.message,
    });
  }
};



export const addStockSplit = async (req, res) => {
  try {
    const app = req.catalystApp;
    const zcql = app.zcql();

    const { securityCode, securityName, ratio1, ratio2, issueDate, isin } =
      req.body;

    if (!securityCode || !securityName || !ratio1 || !ratio2 || !issueDate) {
      return res.status(400).json({
        message: "Missing required fields",
      });
    }

    await zcql.executeZCQLQuery(`
      INSERT INTO Split
      (
        Security_Code,
        Security_Name,
        Ratio1,
        Ratio2,
        Issue_Date,
        ISIN
      )
      VALUES
      (
        '${securityCode}',
        '${securityName}',
        ${Number(ratio1)},
        ${Number(ratio2)},
        '${issueDate}',
        '${isin}'
      )
    `);

    return res.status(200).json({
      success: true,
      message: "Stock split applied successfully",
    });
  } catch (error) {
    console.error("Error in addStockSplit", error);
    return res.status(500).json({
      success: false,
      message: "Failed to apply stock split",
    });
  }
};


