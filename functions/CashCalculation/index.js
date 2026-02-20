/**
 *
 * @param {import("./types/job").JobRequest} jobRequest
 * @param {import("./types/job").Context} context
 */

const catalyst = require("zcatalyst-sdk-node");

const applyCashEffect = (balance, tranType, amount) => {
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
module.exports = async (jobRequest, context) => {
  const catalystApp = catalyst.initialize(context);
  const zcql = catalystApp.zcql();

  try {
    let { accountCode, jobName } = jobRequest.getAllJobParams();

    if (!accountCode) {
      throw new Error("accountCode is required");
    }

    if (!jobName) {
      throw new Error("jobName is required");
    }

    console.log(`Cash ledger job started: ${jobName}`);
    console.log("Cash ledger job started for accounts:", accountCode);

    await zcql.executeZCQLQuery(`
      INSERT INTO Jobs (jobName, status)
      VALUES ('${jobName}', 'PENDING')
    `);

    /* ================= PROCESS EACH ACCOUNT ================= */
    for (const acc of accountCode) {
      console.log(`Processing account: ${acc}`);

      try {
        // 1️⃣ Get last ledger state
        const lastLedger = await zcql.executeZCQLQuery(`
          SELECT Cash_Balance, Tran_Date, Last_Tran_Id
          FROM Cash_Ledger
          WHERE Account_Code = '${acc.replace(/'/g, "''")}'
          ORDER BY Tran_Date DESC, Last_Tran_Id DESC
          LIMIT 1
        `);

        let balance = 0;
        let lastTranId = null;
        let lastDate = null;

        if (lastLedger.length) {
          balance = Number(lastLedger[0].Cash_Ledger.Cash_Balance) || 0;
          lastTranId = lastLedger[0].Cash_Ledger.Last_Tran_Id;
          lastDate = lastLedger[0].Cash_Ledger.Tran_Date;
        }

        const limit = 300;

        // 2️⃣ Fetch & process new transactions
        while (true) {
          const query = `
            SELECT ROWID, SETDATE, Tran_Type, Net_Amount
            FROM Transaction
            WHERE WS_Account_code = '${acc.replace(/'/g, "''")}'
            ${lastTranId ? `AND ROWID > '${lastTranId}'` : ""}
            ORDER BY SETDATE ASC, ROWID ASC
            LIMIT ${limit}
          `;

          const batch = await zcql.executeZCQLQuery(query);
          if (!batch || batch.length === 0) break;

          for (const row of batch) {
            const t = row.Transaction || row;

            balance = applyCashEffect(
              balance,
              t.Tran_Type,
              Number(t.Net_Amount) || 0
            );

            // same date → UPDATE
            if (lastDate === t.SETDATE) {
              await zcql.executeZCQLQuery(`
                UPDATE Cash_Ledger
                SET Cash_Balance = ${balance},
                    Last_Tran_Id = '${t.ROWID}'
                WHERE Account_Code = '${acc.replace(/'/g, "''")}'
                  AND Tran_Date = '${t.SETDATE}'
              `);
            }
            // new date → INSERT
            else {
              await zcql.executeZCQLQuery(`
                INSERT INTO Cash_Ledger
                (Account_Code, Tran_Date, Cash_Balance, Last_Tran_Id)
                VALUES (
                  '${acc.replace(/'/g, "''")}',
                  '${t.SETDATE}',
                  ${balance},
                  '${t.ROWID}'
                )
              `);
              lastDate = t.SETDATE;
            }

            lastTranId = t.ROWID;
          }

          if (batch.length < limit) break;
        }

        console.log(`Cash ledger completed for account ${acc}`);
      } catch (accErr) {
        // ❗ isolate account failure
        console.error(`Failed for account ${acc}`, accErr);
      }
    }

    await zcql.executeZCQLQuery(`
      UPDATE Jobs
      SET status='COMPLETED'
      WHERE jobName='${jobName}'
    `);

    context.closeWithSuccess();
  } catch (error) {
    console.error("Daily cash ledger job failed:", error);

    await zcql.executeZCQLQuery(`
      UPDATE Jobs
      SET status='FAILED'
      WHERE jobName='${jobName}'
    `);

    context.closeWithFailure();
  }
};
