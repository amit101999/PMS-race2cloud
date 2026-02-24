import { runFifoEngine } from "../../util/analytics/transactionHistory/fifo.js";

const ZCQL_ROW_LIMIT = 270;

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

  export const previewStockDividend = async (req, res) => {
    try {
      if (!req.catalystApp) {
        return res.status(500).json({
          success: false,
          message: "Catalyst app not initialized",
        });
      }

      const { isin, recordDate, rate, paymentDate } = req.body;
      const rateNum = Number(rate);

      if (!Number.isFinite(rateNum) || rateNum <= 0) {
        return res.status(400).json({
          success: false,
          message: "Invalid dividend rate value",
        });
      }
      if(!isin || !recordDate || !paymentDate){
        return res.status(400).json({
          success: false,
          message: "Invalid ISIN, record date or payment date",
        });
      }
      /* Normalize Record Date (used for calculation – holdings as on this date) */
      const recordDateObj = new Date(recordDate);
      recordDateObj.setHours(0, 0, 0, 0);
      const recordDateISO = recordDateObj.toISOString().split("T")[0];
      /* Normalize Payment Date (start of day) */
      const paymentDateObj = new Date(paymentDate);
      paymentDateObj.setHours(0, 0, 0, 0);
      const paymentDateISO = paymentDateObj.toISOString().split("T")[0];
      const zcql = req.catalystApp.zcql();
  
      /* ======================================================
         STEP 1: FETCH ELIGIBLE ACCOUNTS
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
        return res.json({ success: true, data: [] });
      }
  
      /* ======================================================
         STEP 2: FETCH TRANSACTIONS (<= RECORD DATE) – dedupe by ROWID
         ====================================================== */
      const txRows = [];
      const seenTxnRowIds = new Set();
      let txOffset = 0;
  
      while (true) {
        const batch = await zcql.executeZCQLQuery(`
          SELECT *
          FROM Transaction
          WHERE ISIN='${isin}'
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
         STEP 3: FETCH BONUSES (< RECORD DATE)
         ====================================================== */
      const bonusRows = [];
      let bonusOffset = 0;
  
      while (true) {
        const batch = await zcql.executeZCQLQuery(`
          SELECT *
          FROM Bonus
          WHERE ISIN='${isin}'
          AND ExDate < '${recordDateISO}'
          ORDER BY ExDate ASC, ROWID ASC
          LIMIT ${ZCQL_ROW_LIMIT} OFFSET ${bonusOffset}
        `);
  
        if (!batch || batch.length === 0) break;
        bonusRows.push(...batch);
        if (batch.length < ZCQL_ROW_LIMIT) break;
        bonusOffset += ZCQL_ROW_LIMIT;
      }
  
      /* ======================================================
         STEP 4: FETCH SPLITS (< RECORD DATE)
         ====================================================== */
      const splitRows = [];
      let splitOffset = 0;
  
      while (true) {
        const batch = await zcql.executeZCQLQuery(`
          SELECT *
          FROM Split
          WHERE ISIN='${isin}'
          AND Issue_Date < '${recordDateISO}'
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
         STEP 6: FIFO PREVIEW (dedupe by account via Set)
         ====================================================== */
      const preview = [];
      const seenAccounts = new Set();

      for (const h of holdingRows) {
        const accountCode = h.Holdings.WS_Account_code;
        if (seenAccounts.has(accountCode)) continue;
        seenAccounts.add(accountCode);

        const transactions = txByAccount[accountCode] || [];
        if (!transactions.length) continue;

        const bonuses = bonusByAccount[accountCode] || [];

        const fifoResult = runFifoEngine(transactions, bonuses, splits, true);
        if (!fifoResult || fifoResult.holdings <= 0) continue;

        const holdingAsOnRecordDate = fifoResult.holdings;
        const dividendAmount = holdingAsOnRecordDate * rateNum;

        preview.push({
          isin,
          accountCode,
          holdingAsOnRecordDate,
          rate: rateNum,
          paymentDate: paymentDateISO,
          dividendAmount,
        });
      }
  
      return res.json({ success: true, data: preview });
    } catch (error) {
      console.error("Preview dividend error:", error);
      return res.status(500).json({
        success: false,
        message: error.message,
      });
    }
  };

  export const applyStockDividendMaster = async (req, res) => {
    try {
      if (!req.catalystApp) {
        return res.status(500).json({
          success: false,
          message: "Catalyst app not initialized",
        });
      }
  
      const {
        isin,
        securityCode,
        securityName,
        rate: rateParam,
        exDate,
        recordDate,
        paymentDate,
        dividendType
      } = req.body;

      const rate = Number(rateParam);


      if (!Number.isFinite(rate) || rate <= 0) {
        return res.status(400).json({
          success: false,
          message: "Invalid dividend rate value",
        });
      }
      if (
        !isin ||
        !securityCode ||
        !securityName ||
        !recordDate ||
        !paymentDate
      ) {
        return res.status(400).json({
          success: false,
          message: "Invalid or missing required fields",
        });
      }

      /* Normalize Dates (exDate optional for UI – ExDate column; recordDate used for duplicate check) */
      const normalizeDate = (d) => {
        const dt = new Date(d);
        dt.setHours(0, 0, 0, 0);
        return dt.toISOString().split("T")[0];
      };

      const recordDateISO = normalizeDate(recordDate);
      const exDateISO = (exDate && String(exDate).trim())
        ? normalizeDate(exDate)
        : recordDateISO;
      const paymentDateISO = normalizeDate(paymentDate);
  
      const zcql = req.catalystApp.zcql();
  
      /* ======================================================
         CHECK DUPLICATE (ISIN + RECORD DATE)
         ====================================================== */
      const existing = await zcql.executeZCQLQuery(`
        SELECT ROWID
        FROM Dividend
        WHERE ISIN='${isin}'
        AND RecordDate='${recordDateISO}'
        LIMIT 1
      `);
  
      if (existing && existing.length > 0) {
        return res.status(400).json({
          success: false,
          message: "Dividend already exists for this ISIN and Record Date",
        });
      }
  
      /* ======================================================
         INSERT MASTER RECORD
         ====================================================== */
      await zcql.executeZCQLQuery(`
        INSERT INTO Dividend
        (
          SecurityCode,
          Security_Name,
          ISIN,
          Rate,
          ExDate,
          RecordDate,
          PaymentDate,
          Dividend_Type,
          Status
        )
        VALUES
        (
          '${securityCode}',
          '${securityName}',
          '${isin}',
          ${rate},
          '${exDateISO}',
          '${recordDateISO}',
          '${paymentDateISO}',
          '${dividendType || "Final"}',
          'Draft'
        )
      `);
  
      return res.json({
        success: true,
        message: "Dividend master created successfully"
      });
  
    } catch (error) {
      console.error("Apply dividend master error:", error);
      return res.status(500).json({
        success: false,
        message: error.message,
      });
    }
  };
  