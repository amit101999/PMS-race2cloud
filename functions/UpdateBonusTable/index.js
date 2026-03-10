const catalyst = require("zcatalyst-sdk-node");
const { runFifoEngine } = require("./fifo.js");

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
      console.log("No eligible accounts found");
      await zcql.executeZCQLQuery(
        `UPDATE Jobs SET status = 'COMPLETED' WHERE jobName = '${jobName}'`
      );
      context.closeWithSuccess();
      return;
    }

    /* ======================================================
       STEP 2: FETCH TX / BONUS / SPLIT
       ====================================================== */
    const fetchAll = async (table, dateCol, operator) => {
      const out = [];
      let off = 0;
      while (true) {
        const r = await zcql.executeZCQLQuery(`
          SELECT *
          FROM ${table}
          WHERE ISIN='${isin}' AND ${dateCol} ${operator} '${exDateISO}'
          LIMIT ${ZCQL_ROW_LIMIT} OFFSET ${off}
        `);
        if (!r || r.length === 0) break;
        out.push(...r);
        if (r.length < ZCQL_ROW_LIMIT) break;
        off += ZCQL_ROW_LIMIT;
      }
      return out;
    };

    const txRows = await fetchAll("Transaction", "SETDATE", "<=");
    const bonusRows = await fetchAll("Bonus", "ExDate", "<");
    const splitRows = await fetchAll("Split", "Issue_Date", "<");

    const txByAcc = {};
    txRows.forEach((r) => {
      const t = r.Transaction;
      (txByAcc[t.WS_Account_code] ||= []).push(t);
    });

    const bonusByAcc = {};
    bonusRows.forEach((r) => {
      const b = r.Bonus;
      (bonusByAcc[b.WS_Account_code] ||= []).push(b);
    });

    const splits = splitRows.map((r) => r.Split);

    /* ======================================================
       STEP 3: FIFO + INSERT BONUS (one per eligible account)
       ====================================================== */
    let inserted = 0;
    let errorCount = 0;

    for (const accountCode of eligibleAccounts) {
      try {
        const tx = txByAcc[accountCode] || [];
        if (!tx.length) continue;

        const bonuses = bonusByAcc[accountCode] || [];

        const fifo = runFifoEngine(tx, bonuses, splits, true);
        if (!fifo || fifo.holdings <= 0) continue;

        const bonusShares = Math.floor((fifo.holdings * r1) / r2);
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
       STEP 4: INSERT BONUS_RECORD (master record)
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
