import { runFifoEngine } from "../util/analytics/transactionHistory/fifo.js";

const ZCQL_ROW_LIMIT = 270;

/* ======================================================
   GET ALL SECURITIES
   ====================================================== */
export const getAllSecuritiesISINs = async (req, res) => {
  try {
    if (!req.catalystApp) {
      return res.status(500).json({
        success: false,
        message: "Catalyst app not initialized",
      });
    }

    const zcql = req.catalystApp.zcql();
    const LIMIT = 270;
    let offset = 0;
    const securities = [];

    while (true) {
      const rows = await zcql.executeZCQLQuery(`
        SELECT ISIN, Security_Code, Security_Name
        FROM Security_List
        WHERE ISIN IS NOT NULL
        LIMIT ${LIMIT} OFFSET ${offset}
      `);

      if (!rows || rows.length === 0) break;

      rows.forEach((r) => {
        const s = r.Security_List;
        securities.push({
          isin: s.ISIN,
          securityCode: s.Security_Code,
          securityName: s.Security_Name,
        });
      });

      offset += LIMIT;
    }

    return res.json({ success: true, data: securities });
  } catch (error) {
    console.error(error);
    return res.status(500).json({
      success: false,
      message: error.message,
    });
  }
};

/* ======================================================
   PREVIEW BONUS (FIFO BASED – DATE AWARE)
   ====================================================== */
export const previewStockBonus = async (req, res) => {
  try {
    if (!req.catalystApp) {
      return res.status(500).json({
        success: false,
        message: "Catalyst app not initialized",
      });
    }

    const { isin, ratio1, ratio2, exDate } = req.body;

    const r1 = Number(ratio1);
    const r2 = Number(ratio2);

    if (!isin || !exDate || !Number.isFinite(r1) || !Number.isFinite(r2) || r1 <= 0) {
      return res.status(400).json({
        success: false,
        message: "Invalid ISIN, ratio or date",
      });
    }

    /* Normalize Ex-Date (start of day) */
    const exDateObj = new Date(exDate);
    exDateObj.setHours(0, 0, 0, 0);
    const exDateISO = exDateObj.toISOString().split("T")[0];

    
    const zcql = req.catalystApp.zcql();

    /* ======================================================
       STEP 1: FETCH ELIGIBLE ACCOUNTS
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
       STEP 2: FETCH TRANSACTIONS (<= EX-DATE)
       ====================================================== */
    const txRows = [];
    let txOffset = 0;

    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT *
        FROM Transaction
        WHERE ISIN='${isin}'
        AND TRANDATE <= '${exDateISO}'
        ORDER BY TRANDATE ASC, ROWID ASC
        LIMIT ${ZCQL_ROW_LIMIT} OFFSET ${txOffset}
      `);

      if (!batch || batch.length === 0) break;
      txRows.push(...batch);
      if (batch.length < ZCQL_ROW_LIMIT) break;
      txOffset += ZCQL_ROW_LIMIT;
    }

    /* ======================================================
       STEP 3: FETCH BONUSES (< EX-DATE)
       ====================================================== */
    const bonusRows = [];
    let bonusOffset = 0;

    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT *
        FROM Bonus
        WHERE ISIN='${isin}'
        AND ExDate < '${exDateISO}'
        ORDER BY ExDate ASC, ROWID ASC
        LIMIT ${ZCQL_ROW_LIMIT} OFFSET ${bonusOffset}
      `);

      if (!batch || batch.length === 0) break;
      bonusRows.push(...batch);
      if (batch.length < ZCQL_ROW_LIMIT) break;
      bonusOffset += ZCQL_ROW_LIMIT;
    }

    /* ======================================================
       STEP 4: FETCH SPLITS (< EX-DATE)
       ====================================================== */
    const splitRows = [];
    let splitOffset = 0;

    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT *
        FROM Split
        WHERE ISIN='${isin}'
        AND Issue_Date < '${exDateISO}'
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

      const fifoBefore = runFifoEngine(transactions, bonuses, splits, true);
      if (!fifoBefore || fifoBefore.holdings <= 0) continue;

      const bonusShares = Math.floor(
        fifoBefore.holdings / r2
      );
      
      if (bonusShares <= 0) continue;

      const previewBonus = {
        ISIN: isin,
        WS_Account_code: accountCode,
        BonusShare: bonusShares,
        ExDate: exDateISO,
      };

      const fifoAfter = runFifoEngine(
        transactions,
        [...bonuses, previewBonus],
        splits,
        true
      );

      preview.push({
        isin,
        accountCode,
        currentHolding: fifoBefore.holdings,
        bonusShares,
        newHolding: fifoAfter.holdings,
        delta: fifoAfter.holdings - fifoBefore.holdings,
      });
    }

    return res.json({ success: true, data: preview });
  } catch (error) {
    console.error("Preview bonus error:", error);
    return res.status(500).json({
      success: false,
      message: error.message,
    });
  }
};

/* ======================================================
   APPLY BONUS (SAME LOGIC AS PREVIEW)
   ====================================================== */
/* ======================================================
   APPLY BONUS (FIFO-BASED – DATE AWARE & CONSISTENT)
   ====================================================== */
   export const applyStockBonus = async (req, res) => {
    try {
      if (!req.catalystApp) {
        return res.status(500).json({
          success: false,
          message: "Catalyst app not initialized",
        });
      }
  
      const { isin, ratio1, ratio2, exDate } = req.body;
  
      const r1 = Number(ratio1);
      const r2 = Number(ratio2);
  
      if (!isin || !exDate || !Number.isFinite(r1) || !Number.isFinite(r2) || r1 <= 0) {
        return res.status(400).json({
          success: false,
          message: "Invalid input values",
        });
      }
  
      /* Normalize Ex-Date */
      const exDateObj = new Date(exDate);
      exDateObj.setHours(0, 0, 0, 0);
      const exDateISO = exDateObj.toISOString().split("T")[0];
  
      
      const zcql = req.catalystApp.zcql();
  
      /* ======================================================
         STEP 1: FETCH HOLDINGS (ACCOUNTS)
         ====================================================== */
      const holdingRows = [];
      let holdOffset = 0;
  
      while (true) {
        const batch = await zcql.executeZCQLQuery(`
          SELECT WS_Account_code
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
        return res.json({
          success: true,
          message: "No eligible accounts",
          affectedAccounts: 0,
        });
      }
  
      /* ======================================================
         STEP 2: FETCH TRANSACTIONS ≤ EX-DATE
         ====================================================== */
      const txRows = [];
      let txOffset = 0;
  
      while (true) {
        const batch = await zcql.executeZCQLQuery(`
          SELECT *
          FROM Transaction
          WHERE ISIN='${isin}'
            AND TRANDATE <= '${exDateISO}'
          ORDER BY TRANDATE ASC, ROWID ASC
          LIMIT ${ZCQL_ROW_LIMIT} OFFSET ${txOffset}
        `);
  
        if (!batch || batch.length === 0) break;
        txRows.push(...batch);
        if (batch.length < ZCQL_ROW_LIMIT) break;
        txOffset += ZCQL_ROW_LIMIT;
      }
  
      /* ======================================================
         STEP 3: FETCH BONUSES < EX-DATE
         ====================================================== */
      const bonusRows = [];
      let bonusOffset = 0;
  
      while (true) {
        const batch = await zcql.executeZCQLQuery(`
          SELECT *
          FROM Bonus
          WHERE ISIN='${isin}'
            AND ExDate < '${exDateISO}'
          ORDER BY ExDate ASC, ROWID ASC
          LIMIT ${ZCQL_ROW_LIMIT} OFFSET ${bonusOffset}
        `);
  
        if (!batch || batch.length === 0) break;
        bonusRows.push(...batch);
        if (batch.length < ZCQL_ROW_LIMIT) break;
        bonusOffset += ZCQL_ROW_LIMIT;
      }
  
      /* ======================================================
         STEP 4: FETCH SPLITS < EX-DATE
         ====================================================== */
      const splitRows = [];
      let splitOffset = 0;
  
      while (true) {
        const batch = await zcql.executeZCQLQuery(`
          SELECT *
          FROM Split
          WHERE ISIN='${isin}'
            AND Issue_Date < '${exDateISO}'
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
         STEP 6: FIFO + INSERT BONUS
         ====================================================== */
      let inserted = 0;
  
      for (const h of holdingRows) {
        const accountCode = h.Holdings.WS_Account_code;
  
        const transactions = txByAccount[accountCode] || [];
        if (!transactions.length) continue;
  
        const bonuses = bonusByAccount[accountCode] || [];
  
        const fifoBefore = runFifoEngine(transactions, bonuses, splits, true);
        if (!fifoBefore || fifoBefore.holdings <= 0) continue;
  
        const bonusShares = Math.floor(
          fifoBefore.holdings / r2
        );
        
  
        if (bonusShares <= 0) continue;
  
        await zcql.executeZCQLQuery(`
          INSERT INTO Bonus
          (
            ISIN,
            WS_Account_code,
            BonusShare,
            ExDate
          )
          VALUES
          (
            '${isin}',
            '${accountCode}',
            ${bonusShares},
            '${exDateISO}'
          )
        `);
  
        inserted++;
      }
  
      return res.json({
        success: true,
        affectedAccounts: inserted,
        message:
          inserted === 0
            ? "No accounts required bonus application"
            : "Bonus applied successfully",
      });
    } catch (error) {
      console.error("Apply bonus error:", error);
      return res.status(500).json({
        success: false,
        message: error.message,
      });
    }
  };
  
