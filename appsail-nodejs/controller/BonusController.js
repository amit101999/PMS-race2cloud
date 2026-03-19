import { runFifoEngine } from "../util/analytics/transactionHistory/fifo.js";

const ZCQL_ROW_LIMIT = 270;

const parseCatalystTime = (ct) => {
  if (!ct) return 0;
  const fixed = ct.replace(/:(\d{3})$/, ".$1");
  const ms = new Date(fixed).getTime();
  return isNaN(ms) ? 0 : ms;
};

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

    const exDateObj = new Date(exDate);
    exDateObj.setHours(0, 0, 0, 0);
    const exDateISO = exDateObj.toISOString().split("T")[0];

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
       STEP 2: FETCH TRANSACTIONS (<= EX-DATE)
       ====================================================== */
    const txRows = [];
    const seenTxnRowIds = new Set();
    let txOffset = 0;

    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT *
        FROM Transaction
        WHERE ISIN='${isin}'
        AND SETDATE <= '${exDateISO}'
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
       STEP 3: FETCH BONUSES (<= EX-DATE)
       ====================================================== */
    const bonusRows = [];
    const seenBonusRowIds = new Set();
    let bonusOffset = 0;

    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT *
        FROM Bonus
        WHERE ISIN='${isin}'
        AND ExDate <= '${exDateISO}'
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
       STEP 4: FETCH SPLITS (<= EX-DATE)
       ====================================================== */
    const splitRows = [];
    const seenSplitRowIds = new Set();
    let splitOffset = 0;

    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT *
        FROM Split
        WHERE ISIN='${isin}'
        AND Issue_Date <= '${exDateISO}'
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
      if (!txByAccount[t.WS_Account_code]) txByAccount[t.WS_Account_code] = [];
      txByAccount[t.WS_Account_code].push(t);
    });

    const bonusByAccount = {};
    bonusRows.forEach((r) => {
      const b = r.Bonus;
      if (!bonusByAccount[b.WS_Account_code]) bonusByAccount[b.WS_Account_code] = [];
      bonusByAccount[b.WS_Account_code].push(b);
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
       STEP 6: FIFO PREVIEW (one per account)
       ====================================================== */
    const preview = [];

    for (const accountCode of eligibleAccounts) {
      const transactions = txByAccount[accountCode] || [];
      if (!transactions.length) continue;

      const bonuses = bonusByAccount[accountCode] || [];

      const fifoBefore = runFifoEngine(transactions, bonuses, splits, true);
      if (!fifoBefore || fifoBefore.holdings <= 0) continue;

      const bonusShares = Math.floor((fifoBefore.holdings * r1) / r2);
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
   APPLY BONUS (BACKGROUND JOB)
   ====================================================== */
export const applyStockBonus = async (req, res) => {
  try {
    if (!req.catalystApp) {
      return res.status(500).json({
        success: false,
        message: "Catalyst app not initialized",
      });
    }

    const {
      isin,
      ratio1,
      ratio2,
      exDate,
      securityCode: bodySecurityCode,
      securityName: bodySecurityName,
    } = req.body;

    const r1 = Number(ratio1);
    const r2 = Number(ratio2);

    if (!isin || !exDate || !Number.isFinite(r1) || !Number.isFinite(r2) || r1 <= 0) {
      return res.status(400).json({
        success: false,
        message: "Invalid input values",
      });
    }

    const exDateObj = new Date(exDate);
    exDateObj.setHours(0, 0, 0, 0);
    const exDateISO = exDateObj.toISOString().split("T")[0];

    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();
    const jobScheduling = catalystApp.jobScheduling();

    const jobName = `BON_${isin.slice(-6)}_${exDateISO}`;

    const existing = await zcql.executeZCQLQuery(
      `SELECT ROWID, status, CREATEDTIME FROM Jobs WHERE jobName = '${jobName}' LIMIT 1`
    );

    if (existing.length) {
      const oldStatus = existing[0].Jobs.status;
      const oldRowId = existing[0].Jobs.ROWID;
      const createdTime = existing[0].Jobs.CREATEDTIME;

      const STALE_TIMEOUT_MS = 60 * 60 * 1000;
      const jobAge = Date.now() - parseCatalystTime(createdTime);
      const isStale = jobAge > STALE_TIMEOUT_MS;

      if ((oldStatus === "PENDING" || oldStatus === "RUNNING") && !isStale) {
        return res.json({
          success: true,
          jobName,
          status: oldStatus,
          message: "Bonus application is already in progress",
        });
      }

      try {
        await zcql.executeZCQLQuery(`DELETE FROM Jobs WHERE ROWID = ${oldRowId}`);
      } catch (delErr) {
        console.error("Error deleting old bonus job:", delErr);
      }
    }

    await jobScheduling.JOB.submitJob({
      job_name: "ApplyBonus",
      jobpool_name: "Export",
      target_name: "UpdateBonusTable",
      target_type: "Function",
      params: {
        isin,
        ratio1: String(r1),
        ratio2: String(r2),
        exDate: exDateISO,
        secCode: bodySecurityCode || "",
        secName: bodySecurityName || "",
        jobName,
      },
    });

    return res.json({
      success: true,
      jobName,
      status: "PENDING",
      message: "Bonus application job started",
    });
  } catch (error) {
    console.error("Apply bonus error:", error);
    return res.status(500).json({
      success: false,
      message: error.message,
    });
  }
};

/* ======================================================
   GET BONUS APPLY JOB STATUS
   ====================================================== */
export const getBonusApplyStatus = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();

    const { jobName } = req.query;
    if (!jobName) {
      return res.status(400).json({ success: false, message: "jobName is required" });
    }

    const result = await zcql.executeZCQLQuery(
      `SELECT ROWID, status, CREATEDTIME FROM Jobs WHERE jobName = '${jobName}' LIMIT 1`
    );

    if (!result.length) {
      return res.json({ success: true, status: "NOT_STARTED" });
    }

    let status = result[0].Jobs.status;
    const createdTime = result[0].Jobs.CREATEDTIME;

    const STALE_TIMEOUT_MS = 60 * 60 * 1000;
    const jobAge = Date.now() - parseCatalystTime(createdTime);

    if ((status === "PENDING" || status === "RUNNING") && jobAge > STALE_TIMEOUT_MS) {
      try {
        await zcql.executeZCQLQuery(
          `UPDATE Jobs SET status = 'ERROR' WHERE jobName = '${jobName}'`
        );
      } catch (updateErr) {
        console.error("Failed to mark stale bonus job as ERROR:", updateErr);
      }
      status = "ERROR";
    }

    return res.json({ success: true, jobName, status });
  } catch (error) {
    console.error("Error fetching bonus job status:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to fetch bonus job status",
    });
  }
};
