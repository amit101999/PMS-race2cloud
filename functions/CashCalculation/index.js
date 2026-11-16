/**
 *
 * @param {import("./types/job").JobRequest} jobRequest
 * @param {import("./types/job").Context} context
 */

const catalyst = require("zcatalyst-sdk-node");

const applyCashEffect = (balance, tranType, amount, stt = 0) => {
  const amt = Math.abs(Number(amount)) || 0;
  const sttVal = Math.abs(Number(stt)) || 0;

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
    return balance + amt - sttVal;
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
    return balance - amt - sttVal;
  }

  return balance;
};

const esc = (v) => String(v ?? "").replace(/'/g, "''");

const PAGE_SIZE = 300;
const INSERT_CHUNK = 100;
// ZCQL DELETE removes at most ~300 rows per call, so it is repeated.
const DELETE_MAX_ROUNDS = 1000;

/**
 * Every transaction of one account.
 *
 * Paged by ROWID keyset only to walk the whole set; ROWID says nothing about
 * time (Catalyst does not hand them out in order), so the rows are put in date
 * order afterwards.
 */
async function fetchAccountTransactions(zcql, accountCode) {
  const rows = [];
  let lastRowId = 0;
  while (true) {
    const batch = await zcql.executeZCQLQuery(`
      SELECT ROWID, SETDATE, TRANDATE, Tran_Type, Net_Amount, STT, Security_Type
      FROM Transaction
      WHERE WS_Account_code = '${esc(accountCode)}'
        AND ROWID > ${lastRowId}
      ORDER BY ROWID ASC
      LIMIT ${PAGE_SIZE}
    `);
    if (!batch || batch.length === 0) break;
    for (const row of batch) rows.push(row.Transaction || row);
    lastRowId = rows[rows.length - 1].ROWID;
    if (batch.length < PAGE_SIZE) break;
  }
  return rows;
}

/** The day a row moves cash: its settlement date, or its trade date when it has none (fees and charges). */
const cashDate = (t) => String(t.SETDATE || t.TRANDATE || "").slice(0, 10);

/** One closing balance per day, oldest day first. */
function dailyClosingBalances(accountCode, transactions) {
  const undated = transactions.filter((t) => !cashDate(t));
  if (undated.length > 0) {
    // Leaving them out would make every balance after them wrong without saying so.
    throw new Error(
      `${undated.length} transaction(s) of ${accountCode} have neither a settlement nor a trade date (first ROWID ${undated[0].ROWID}).`,
    );
  }

  const sorted = [...transactions].sort((a, b) => cashDate(a).localeCompare(cashDate(b)));
  const days = [];
  let balance = 0;
  for (const t of sorted) {
    // A mutual fund row's Net_Amount already has STT taken off (sell), so it is not applied again.
    // Same rule as functions/Cal_CB_Append_TxnUpload and Cal_CB_Per_TNX.
    const isMutualFund = String(t.Security_Type ?? "").trim().toLowerCase() === "m";
    const stt = isMutualFund ? 0 : Number(t.STT) || 0;
    balance = applyCashEffect(balance, t.Tran_Type, Number(t.Net_Amount) || 0, stt);
    const date = cashDate(t);
    const today = days[days.length - 1];
    // Order within a day does not change the day's closing balance.
    if (today && today.date === date) {
      today.balance = balance;
      today.lastTranId = t.ROWID;
    } else {
      days.push({ date, balance, lastTranId: t.ROWID });
    }
  }
  return days;
}

async function deleteAccountLedger(zcql, accountCode) {
  for (let round = 0; round < DELETE_MAX_ROUNDS; round++) {
    const counted = await zcql.executeZCQLQuery(
      `SELECT COUNT(ROWID) FROM Cash_Ledger WHERE Account_Code = '${esc(accountCode)}'`,
    );
    const r = counted?.[0]?.Cash_Ledger || counted?.[0] || {};
    const remaining = Number(r["COUNT(ROWID)"] ?? Object.values(r)[0] ?? 0);
    if (remaining === 0) return;
    await zcql.executeZCQLQuery(`DELETE FROM Cash_Ledger WHERE Account_Code = '${esc(accountCode)}'`);
  }
  throw new Error(`Cash_Ledger rows for ${accountCode} were still left after ${DELETE_MAX_ROUNDS} delete rounds.`);
}

/**
 * Rebuild one account's ledger from all its transactions.
 *
 * Rebuilt rather than appended to: an append needs to know which transactions
 * are new, and the old "ROWID greater than the last one done" test skipped
 * trades whose ROWID happened to be smaller. A trade dated before the last
 * ledger day also needs every later day redone, which a rebuild does anyway.
 */
async function rebuildAccountLedger(catalystApp, zcql, accountCode) {
  const transactions = await fetchAccountTransactions(zcql, accountCode);
  const days = dailyClosingBalances(accountCode, transactions);

  await deleteAccountLedger(zcql, accountCode);

  const table = catalystApp.datastore().table("Cash_Ledger");
  for (let i = 0; i < days.length; i += INSERT_CHUNK) {
    await table.insertRows(
      days.slice(i, i + INSERT_CHUNK).map((d) => ({
        Account_Code: accountCode,
        Tran_Date: d.date,
        Cash_Balance: Number(d.balance.toFixed(2)),
        Last_Tran_Id: d.lastTranId,
      })),
    );
  }
  return { transactions: transactions.length, days: days.length };
}

module.exports = async (jobRequest, context) => {
  const catalystApp = catalyst.initialize(context);
  const zcql = catalystApp.zcql();
  let jobName = "";

  try {
    const params = jobRequest.getAllJobParams();
    const accountCode = params.accountCode;
    jobName = String(params.jobName || "");

    if (!Array.isArray(accountCode) || accountCode.length === 0) {
      throw new Error("accountCode must be a non-empty list");
    }

    if (!jobName) {
      throw new Error("jobName is required");
    }

    console.log(`Cash ledger job started: ${jobName}`);
    console.log("Cash ledger job started for accounts:", accountCode);

    // The name carries only the day, so a second run that day finds the row already there.
    try {
      await zcql.executeZCQLQuery(`INSERT INTO Jobs (jobName, status) VALUES ('${esc(jobName)}', 'PENDING')`);
    } catch {
      await zcql.executeZCQLQuery(`UPDATE Jobs SET status = 'PENDING' WHERE jobName = '${esc(jobName)}'`);
    }

    /* ================= PROCESS EACH ACCOUNT ================= */
    const failedAccounts = [];
    for (const acc of accountCode) {
      console.log(`Processing account: ${acc}`);

      try {
        const result = await rebuildAccountLedger(catalystApp, zcql, acc);
        console.log(
          `Cash ledger rebuilt for account ${acc}: ${result.transactions} transaction(s), ${result.days} day(s)`,
        );
      } catch (accErr) {
        // One account failing does not stop the others, but the job must not report success.
        console.error(`Failed for account ${acc}`, accErr);
        failedAccounts.push(acc);
      }
    }

    if (failedAccounts.length > 0) {
      throw new Error(`Cash ledger failed for ${failedAccounts.length} account(s): ${failedAccounts.join(", ")}`);
    }

    await zcql.executeZCQLQuery(`
      UPDATE Jobs
      SET status='COMPLETED'
      WHERE jobName='${esc(jobName)}'
    `);

    context.closeWithSuccess();
  } catch (error) {
    console.error("Daily cash ledger job failed:", error);

    if (jobName) {
      try {
        await zcql.executeZCQLQuery(`
          UPDATE Jobs
          SET status='FAILED'
          WHERE jobName='${esc(jobName)}'
        `);
      } catch (statusErr) {
        console.error("Could not mark the cash ledger job FAILED:", statusErr.message);
      }
    }

    context.closeWithFailure();
  }
};
