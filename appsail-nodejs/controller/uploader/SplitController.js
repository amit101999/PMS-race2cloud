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

    const issueDateObj = new Date(issueDate);
    issueDateObj.setHours(0, 0, 0, 0);
    const issueDateISO = issueDateObj.toISOString().split("T")[0];

    const zcql = req.catalystApp.zcql();

    /* ======================================================
       STEP 1: FIND ACCOUNTS WITH TRANSACTIONS FOR THIS ISIN
       ====================================================== */
    const accountSet = new Set();
    let holdOffset = 0;

    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT WS_Account_code
        FROM Transaction
        WHERE ISIN='${isin}'
        LIMIT ${ZCQL_ROW_LIMIT} OFFSET ${holdOffset}
      `);

      if (!batch || batch.length === 0) break;
      batch.forEach((r) => accountSet.add(r.Transaction.WS_Account_code));
      if (batch.length < ZCQL_ROW_LIMIT) break;
      holdOffset += ZCQL_ROW_LIMIT;
    }

    const eligibleAccounts = Array.from(accountSet);

    if (!eligibleAccounts.length) {
      return res.json({ success: true, data: [] });
    }

    /* ======================================================
       STEP 2: FETCH TRANSACTIONS (<= ISSUE DATE)
       ====================================================== */
    const txRows = [];
    const seenTxnRowIds = new Set();
    let txOffset = 0;

    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT *
        FROM Transaction
        WHERE ISIN='${isin}'
        AND SETDATE <= '${issueDateISO}'
        ORDER BY SETDATE ASC, ROWID ASC
        LIMIT ${ZCQL_ROW_LIMIT} OFFSET ${txOffset}
      `);

      if (!batch || batch.length === 0) break;
      for (const row of batch) {
        const t = row.Transaction || row;
        const rowId = t.ROWID;
        if (rowId != null && seenTxnRowIds.has(rowId)) continue;
        if (rowId != null) seenTxnRowIds.add(rowId);
        txRows.push(row);
      }
      if (batch.length < ZCQL_ROW_LIMIT) break;
      txOffset += ZCQL_ROW_LIMIT;
    }

    /* ======================================================
       STEP 3: FETCH BONUSES (<= ISSUE DATE)
       ====================================================== */
    const bonusRows = [];
    const seenBonusRowIds = new Set();
    let bonusOffset = 0;

    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT *
        FROM Bonus
        WHERE ISIN='${isin}'
        AND ExDate <= '${issueDateISO}'
        ORDER BY ExDate ASC, ROWID ASC
        LIMIT ${ZCQL_ROW_LIMIT} OFFSET ${bonusOffset}
      `);

      if (!batch || batch.length === 0) break;
      for (const row of batch) {
        const b = row.Bonus || row;
        const rowId = b.ROWID;
        if (rowId != null && seenBonusRowIds.has(rowId)) continue;
        if (rowId != null) seenBonusRowIds.add(rowId);
        bonusRows.push(row);
      }
      if (batch.length < ZCQL_ROW_LIMIT) break;
      bonusOffset += ZCQL_ROW_LIMIT;
    }

    /* ======================================================
       STEP 4: FETCH PAST SPLITS (<= ISSUE DATE)
       ====================================================== */
    const splitRows = [];
    const seenSplitRowIds = new Set();
    let splitOffset = 0;

    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT *
        FROM Split
        WHERE ISIN='${isin}'
        AND Issue_Date <= '${issueDateISO}'
        ORDER BY Issue_Date ASC, ROWID ASC
        LIMIT ${ZCQL_ROW_LIMIT} OFFSET ${splitOffset}
      `);

      if (!batch || batch.length === 0) break;
      for (const row of batch) {
        const s = row.Split || row;
        const rowId = s.ROWID;
        if (rowId != null && seenSplitRowIds.has(rowId)) continue;
        if (rowId != null) seenSplitRowIds.add(rowId);
        splitRows.push(row);
      }
      if (batch.length < ZCQL_ROW_LIMIT) break;
      splitOffset += ZCQL_ROW_LIMIT;
    }

    /* ======================================================
       STEP 5: GROUP DATA BY ACCOUNT
       ====================================================== */
    const txByAccount = {};
    txRows.forEach((r) => {
      const t = r.Transaction;
      (txByAccount[t.WS_Account_code] ||= []).push(t);
    });

    const bonusByAccount = {};
    bonusRows.forEach((r) => {
      const b = r.Bonus;
      (bonusByAccount[b.WS_Account_code] ||= []).push(b);
    });

    const pastSplits = splitRows.map((r) => {
      const s = r.Split;
      return {
        issueDate: s.Issue_Date,
        ratio1: Number(s.Ratio1) || 0,
        ratio2: Number(s.Ratio2) || 0,
        isin: s.ISIN,
      };
    });

    /* ======================================================
       STEP 6: FIFO PREVIEW
       ====================================================== */
    const preview = [];
    const splitMultiplier = r2 / r1;

    for (const accountCode of eligibleAccounts) {
      const transactions = txByAccount[accountCode] || [];
      if (!transactions.length) continue;

      const bonuses = bonusByAccount[accountCode] || [];

      const before = runFifoEngine(transactions, bonuses, pastSplits, true);
      if (!before || before.holdings <= 0) continue;

      const newHolding = Math.floor(before.holdings * splitMultiplier);

      preview.push({
        isin,
        accountCode,
        currentHolding: before.holdings,
        newHolding,
        delta: newHolding - before.holdings,
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
