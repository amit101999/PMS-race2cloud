const { runFifoEngine } = require("./fifo.js");
const catalyst = require("zcatalyst-sdk-node");

const ZCQL_ROW_LIMIT = 270;

/**
 * @param {import("./types/job").JobRequest} jobRequest
 * @param {import("./types/job").Context} context
 */
module.exports = async (jobRequest, context) => {
  const catalystApp = catalyst.initialize(context);
  const zcql = catalystApp.zcql();
  let jobName = "";

  try {
    console.log("Bonus apply job started");

    const params = jobRequest.getAllJobParams();
    const { isin, exDate, secCode, secName } = params;
    const r1 = Number(params.ratio1);
    const r2 = Number(params.ratio2);
    jobName = params.jobName;

    await zcql.executeZCQLQuery(
      `INSERT INTO Jobs (jobName, status) VALUES ('${jobName}', 'PENDING')`
    );

    const exDateISO = exDate;

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
      console.log("No eligible accounts found");
      await zcql.executeZCQLQuery(
        `UPDATE Jobs SET status = 'COMPLETED' WHERE jobName = '${jobName}'`
      );
      context.closeWithSuccess();
      return;
    }

    /* ======================================================
       STEP 2: FETCH TRANSACTIONS <= EX-DATE (dedupe by ROWID)
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
       STEP 6: FIFO + INSERT BONUS (one per account)
       ====================================================== */
    let inserted = 0;
    let errorCount = 0;
    const processedAccounts = new Set();

    for (const h of holdingRows) {
      const accountCode = h.Holdings.WS_Account_code;
      if (processedAccounts.has(accountCode)) continue;
      processedAccounts.add(accountCode);

      try {
        const transactions = txByAccount[accountCode] || [];
        if (!transactions.length) continue;

        const bonuses = bonusByAccount[accountCode] || [];

        const fifoBefore = runFifoEngine(transactions, bonuses, splits, true);
        if (!fifoBefore || fifoBefore.holdings <= 0) continue;

        const bonusShares = Math.floor((fifoBefore.holdings * r1) / r2);
        if (bonusShares <= 0) continue;

        await zcql.executeZCQLQuery(`
          INSERT INTO Bonus
          (
            ISIN,
            SecurityCode,
            SecurityName,
            WS_Account_code,
            BonusShare,
            ExDate
          )
          VALUES
          (
            '${isin}',
            '${(secCode || "").replace(/'/g, "''")}',
            '${(secName || "").replace(/'/g, "''")}',
            '${accountCode}',
            ${bonusShares},
            '${exDateISO}'
          )
        `);

        inserted++;
        console.log(`Bonus applied for ${accountCode}: ${bonusShares} shares`);
      } catch (accountErr) {
        errorCount++;
        console.error(`Error applying bonus for ${accountCode}:`, accountErr);
      }
    }

    /* ======================================================
       STEP 7: INSERT BONUS_RECORD (master record)
       ====================================================== */
    if (inserted > 0) {
      try {
        await zcql.executeZCQLQuery(`
          INSERT INTO Bonus_Record
          (
            SecurityCode,
            SecurityName,
            ISIN,
            Ratio1,
            Ratio2,
            ExDate
          )
          VALUES
          (
            '${(secCode || "").replace(/'/g, "''")}',
            '${(secName || "").replace(/'/g, "''")}',
            '${isin}',
            ${r1},
            ${r2},
            '${exDateISO}'
          )
        `);
      } catch (recordErr) {
        console.error("Error inserting Bonus_Record:", recordErr);
      }
    }

    console.log(`Bonus job completed: ${inserted} accounts updated, ${errorCount} errors`);

    await zcql.executeZCQLQuery(
      `UPDATE Jobs SET status = 'COMPLETED' WHERE jobName = '${jobName}'`
    );
    context.closeWithSuccess();
  } catch (error) {
    console.error("Bonus apply job failed:", error);

    try {
      await zcql.executeZCQLQuery(
        `UPDATE Jobs SET status = 'FAILED' WHERE jobName = '${jobName}'`
      );
    } catch (updateErr) {
      console.error("Failed to update job status to FAILED:", updateErr);
    }
    context.closeWithFailure();
  }
};
