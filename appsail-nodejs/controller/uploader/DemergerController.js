import { runFifoEngine } from "../../util/analytics/transactionHistory/fifo.js";

const ZCQL_ROW_LIMIT = 270;

const escSql = (s) => String(s ?? "").replace(/'/g, "''");

/* ======================================================
   PREVIEW DEMERGER (FIFO BASED)
   ====================================================== */
export const previewDemerger = async (req, res) => {
  try {
    if (!req.catalystApp) {
      return res.status(500).json({
        success: false,
        message: "Catalyst app not initialized",
      });
    }

    const {
      oldIsin,
      newIsin,
      ratio1,
      ratio2,
      effectiveDate,
      recordDate,
      allocationToNewPercent,
    } = req.body;

    const r1 = Number(ratio1);
    const r2 = Number(ratio2);
    const pct = Number(allocationToNewPercent);

    if (
      !oldIsin ||
      !newIsin ||
      oldIsin === newIsin ||
      !effectiveDate ||
      !recordDate ||
      !Number.isFinite(r1) || r1 <= 0 ||
      !Number.isFinite(r2) || r2 <= 0 ||
      !Number.isFinite(pct) || pct < 0 || pct > 100
    ) {
      return res.status(400).json({
        success: false,
        message: "Invalid input: check ISINs, ratio, dates and allocation %",
      });
    }

    const recordDateISO = new Date(recordDate).toISOString().split("T")[0];

    const zcql = req.catalystApp.zcql();

    /* ======================================================
       STEP 1: FIND ACCOUNTS WITH TRANSACTIONS FOR OLD ISIN
       ====================================================== */
    const accountSet = new Set();
    let holdOffset = 0;

    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT WS_Account_code
        FROM Transaction
        WHERE ISIN='${escSql(oldIsin)}'
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
       STEP 2: FETCH TRANSACTIONS FOR OLD ISIN (<= recordDate)
       Same inclusive date logic as Bonus, Split, Dividend
       ====================================================== */
    const txRows = [];
    const seenTxnRowIds = new Set();
    let txOffset = 0;

    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT *
        FROM Transaction
        WHERE ISIN='${escSql(oldIsin)}'
        AND SETDATE <= '${recordDateISO}'
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
       STEP 3: FETCH BONUSES FOR OLD ISIN (<= recordDate)
       ====================================================== */
    const bonusRows = [];
    const seenBonusRowIds = new Set();
    let bonusOffset = 0;

    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT *
        FROM Bonus
        WHERE ISIN='${escSql(oldIsin)}'
        AND ExDate <= '${recordDateISO}'
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
       STEP 4: FETCH SPLITS FOR OLD ISIN (<= recordDate)
       ====================================================== */
    const splitRows = [];
    const seenSplitRowIds = new Set();
    let splitOffset = 0;

    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT *
        FROM Split
        WHERE ISIN='${escSql(oldIsin)}'
        AND Issue_Date <= '${recordDateISO}'
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

    const splits = splitRows.map((r) => {
      const s = r.Split;
      return {
        issueDate: s.Issue_Date,
        ratio1: Number(s.Ratio1) || 0,
        ratio2: Number(s.Ratio2) || 0,
        isin: s.ISIN,
      };
    });

    /* ======================================================
       STEP 6: DEMERGER PREVIEW PER ACCOUNT
       ====================================================== */
    const preview = [];

    for (const accountCode of eligibleAccounts) {
      const transactions = txByAccount[accountCode] || [];
      if (!transactions.length) continue;

      const bonuses = bonusByAccount[accountCode] || [];

      const fifoBefore = runFifoEngine(transactions, bonuses, splits, true);
      if (!fifoBefore || fifoBefore.holdings <= 0) continue;

      const Q1 = fifoBefore.holdings;
      const TOTAL_COST = fifoBefore.holdingValue;
      const beforePrice = fifoBefore.averageCostOfHoldings;

      const Q2 = Math.floor((Q1 * r1) / r2);
      if (Q2 <= 0) continue;

      const newCost = TOTAL_COST * (pct / 100);
      const oldCost = TOTAL_COST - newCost;

      preview.push({
        accountCode,
        oldIsin,
        oldQty: Q1,
        oldBeforePrice: Math.round(beforePrice * 100) / 100,
        oldNewPrice: Math.round((oldCost / Q1) * 100) / 100,
        newIsin,
        newQty: Q2,
        newPrice: Math.round((newCost / Q2) * 100) / 100,
      });
    }

    return res.json({ success: true, data: preview });
  } catch (error) {
    console.error("Preview demerger error:", error);
    return res.status(500).json({
      success: false,
      message: error.message,
    });
  }
};

/* ======================================================
   APPLY DEMERGER
   1. INSERT master row into `Demerger`
   2. For each eligible account: run FIFO, compute new ISIN
      data, INSERT row into `Demerger_Record`
   ====================================================== */
export const applyDemerger = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({
        success: false,
        message: "Catalyst app not initialized",
      });
    }

    const zcql = app.zcql();

    const {
      oldIsin,
      oldSecurityCode,
      oldSecurityName,
      newIsin,
      newSecurityCode,
      newSecurityName,
      ratio1,
      ratio2,
      effectiveDate,
      recordDate,
      allocationToNewPercent,
    } = req.body;

    if (
      !oldIsin ||
      !newIsin ||
      oldIsin === newIsin ||
      !oldSecurityCode ||
      !oldSecurityName ||
      !newSecurityCode ||
      !newSecurityName ||
      !ratio1 ||
      !ratio2 ||
      !effectiveDate ||
      !recordDate
    ) {
      return res.status(400).json({
        success: false,
        message: "Missing required fields",
      });
    }

    const r1 = Number(ratio1);
    const r2 = Number(ratio2);
    const pct = Number(allocationToNewPercent);

    if (
      !Number.isFinite(r1) || r1 <= 0 ||
      !Number.isFinite(r2) || r2 <= 0 ||
      !Number.isFinite(pct) || pct < 0 || pct > 100
    ) {
      return res.status(400).json({
        success: false,
        message: "Invalid ratio or allocation %",
      });
    }

    const dateISO = new Date(effectiveDate).toISOString().split("T")[0];
    const recordDateISO = new Date(recordDate).toISOString().split("T")[0];

    /* ======================================================
       STEP 1: INSERT MASTER ROW INTO `Demerger`
       ====================================================== */
    await zcql.executeZCQLQuery(`
      INSERT INTO Demerger
      (
        Old_ISIN,
        Old_Security_Code,
        Old_Security_Name,
        New_ISIN,
        New_Security_Code,
        New_Security_Name,
        Ratio1,
        Ratio2,
        Effective_Date,
        Record_Date,
        Allocation_To_New_Pct
      )
      VALUES
      (
        '${escSql(oldIsin)}',
        '${escSql(oldSecurityCode)}',
        '${escSql(oldSecurityName)}',
        '${escSql(newIsin)}',
        '${escSql(newSecurityCode)}',
        '${escSql(newSecurityName)}',
        ${r1},
        ${r2},
        '${dateISO}',
        '${recordDateISO}',
        ${pct}
      )
    `);

    /* ======================================================
       STEP 1b: INSERT NEW SECURITY INTO `Security_List`
       ====================================================== */
    try {
      await zcql.executeZCQLQuery(`
        INSERT INTO Security_List
        (
          Security_Code,
          ISIN,
          Security_Name
        )
        VALUES
        (
          '${escSql(newSecurityCode)}',
          '${escSql(newIsin)}',
          '${escSql(newSecurityName)}'
        )
      `);
    } catch (secErr) {
      console.warn("Security_List insert skipped (may already exist):", secErr.message);
    }

    /* ======================================================
       STEP 2: FIND ACCOUNTS HOLDING OLD ISIN
       ====================================================== */
    const accountSet = new Set();
    let holdOffset = 0;

    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT WS_Account_code
        FROM Transaction
        WHERE ISIN='${escSql(oldIsin)}'
        LIMIT ${ZCQL_ROW_LIMIT} OFFSET ${holdOffset}
      `);

      if (!batch || batch.length === 0) break;
      batch.forEach((r) => accountSet.add(r.Transaction.WS_Account_code));
      if (batch.length < ZCQL_ROW_LIMIT) break;
      holdOffset += ZCQL_ROW_LIMIT;
    }

    const eligibleAccounts = Array.from(accountSet);

    if (!eligibleAccounts.length) {
      return res.status(200).json({
        success: true,
        message: "Demerger master saved. No accounts hold the old ISIN.",
        recordsInserted: 0,
      });
    }

    /* ======================================================
       STEP 3: LOAD TRANSACTION + BONUS + SPLIT FOR OLD ISIN
       Same inclusive date logic as Bonus, Split, Dividend
       ====================================================== */
    const txRows = [];
    const seenTxnRowIds = new Set();
    let txOffset = 0;

    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT *
        FROM Transaction
        WHERE ISIN='${escSql(oldIsin)}'
        AND SETDATE <= '${recordDateISO}'
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

    const bonusRows = [];
    const seenBonusRowIds = new Set();
    let bonusOffset = 0;

    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT *
        FROM Bonus
        WHERE ISIN='${escSql(oldIsin)}'
        AND ExDate <= '${recordDateISO}'
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

    const splitRows = [];
    const seenSplitRowIds = new Set();
    let splitOffset = 0;

    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT *
        FROM Split
        WHERE ISIN='${escSql(oldIsin)}'
        AND Issue_Date <= '${recordDateISO}'
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
       STEP 4: GROUP BY ACCOUNT
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

    const splits = splitRows.map((r) => {
      const s = r.Split;
      return {
        issueDate: s.Issue_Date,
        ratio1: Number(s.Ratio1) || 0,
        ratio2: Number(s.Ratio2) || 0,
        isin: s.ISIN,
      };
    });

    /* ======================================================
       STEP 5: COMPUTE & INSERT Demerger_Record PER ACCOUNT
       (2 rows per account: old ISIN + new ISIN)
       ====================================================== */
    let recordsInserted = 0;

    for (const accountCode of eligibleAccounts) {
      const transactions = txByAccount[accountCode] || [];
      if (!transactions.length) continue;

      const bonuses = bonusByAccount[accountCode] || [];

      const fifoBefore = runFifoEngine(transactions, bonuses, splits, true);
      if (!fifoBefore || fifoBefore.holdings <= 0) continue;

      const Q1 = fifoBefore.holdings;
      const TOTAL_COST = fifoBefore.holdingValue;

      const Q2 = Math.floor((Q1 * r1) / r2);
      if (Q2 <= 0) continue;

      const newCost = TOTAL_COST * (pct / 100);
      const oldCost = TOTAL_COST - newCost;

      const oldNewPrice = Math.round((oldCost / Q1) * 100) / 100;
      const newPrice = Math.round((newCost / Q2) * 100) / 100;

      const oldCostRounded = Math.round(oldCost * 100) / 100;
      const newCostRounded = Math.round(newCost * 100) / 100;

      /* --- Row 1: Old ISIN (cost reduced) --- */
      await zcql.executeZCQLQuery(`
        INSERT INTO Demerger_Record
        (
          WS_Account_code,
          TRANDATE,
          SETDATE,
          Tran_Type,
          ISIN,
          Security_Code,
          Security_Name,
          QTY,
          PRICE,
          TOTAL_AMOUNT,
          HOLDING,
          WAP,
          HOLDING_VALUE,
          PL
        )
        VALUES
        (
          '${escSql(accountCode)}',
          '${dateISO}',
          '${dateISO}',
          'DEMERGER',
          '${escSql(oldIsin)}',
          '${escSql(oldSecurityCode)}',
          '${escSql(oldSecurityName)}',
          ${Q1},
          ${oldNewPrice},
          ${oldCostRounded},
          ${Q1},
          ${oldNewPrice},
          ${oldCostRounded},
          0
        )
      `);

      /* --- Row 2: New ISIN (new shares received) --- */
      await zcql.executeZCQLQuery(`
        INSERT INTO Demerger_Record
        (
          WS_Account_code,
          TRANDATE,
          SETDATE,
          Tran_Type,
          ISIN,
          Security_Code,
          Security_Name,
          QTY,
          PRICE,
          TOTAL_AMOUNT,
          HOLDING,
          WAP,
          HOLDING_VALUE,
          PL
        )
        VALUES
        (
          '${escSql(accountCode)}',
          '${dateISO}',
          '${dateISO}',
          'DEMERGER',
          '${escSql(newIsin)}',
          '${escSql(newSecurityCode)}',
          '${escSql(newSecurityName)}',
          ${Q2},
          ${newPrice},
          ${newCostRounded},
          ${Q2},
          ${newPrice},
          ${newCostRounded},
          0
        )
      `);

      recordsInserted += 2;
    }

    return res.status(200).json({
      success: true,
      message: "Demerger applied successfully",
      recordsInserted,
    });
  } catch (error) {
    console.error("Error in applyDemerger:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to apply demerger",
    });
  }
};
