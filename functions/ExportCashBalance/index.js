const { Readable } = require("stream");
const { getAllAccountCodesFromDatabase } = require("./allAccountCodes.js");
const catalyst = require("zcatalyst-sdk-node");

const esc = (s) => String(s ?? "").replace(/'/g, "''");

/** yyyy-mm-dd -> yyyy/mm/dd for CSV DATE column */
function formatDateSlash(yyyyMmDd) {
  const parts = String(yyyyMmDd || "").split("-");
  if (parts.length !== 3) return String(yyyyMmDd);
  return `${parts[0]}/${parts[1]}/${parts[2]}`;
}

function csvCell(v) {
  const s = String(v ?? "");
  if (/[",\r\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

/** Closing balance from passbook: correct columns are Transaction_Date + Sequence. */
async function getCashBalanceAsOn(zcql, accountCode, asOnDate) {
  const nextDay = new Date(asOnDate);
  nextDay.setDate(nextDay.getDate() + 1);
  const nextDayStr = nextDay.toISOString().split("T")[0];

  const rows = await zcql.executeZCQLQuery(`
    SELECT Cash_Balance
    FROM Cash_Balance_Per_Transaction
    WHERE Account_Code = '${esc(accountCode)}'
      AND Transaction_Date < '${nextDayStr}'
    ORDER BY Sequence DESC
    LIMIT 1
  `);

  if (!rows.length) return 0;
  const row = rows[0].Cash_Balance_Per_Transaction || rows[0];
  return Number(row.Cash_Balance) || 0;
}

/**
 * All-clients cash snapshot as on one date → CSV (DATE, ACCOUNT_CODE, CASH_BALANCE).
 * @param {import("./types/job").JobRequest} jobRequest
 * @param {import("./types/job").Context} context
 */
module.exports = async (jobRequest, context) => {
  const catalystApp = catalyst.initialize(context);
  const zcql = catalystApp.zcql();
  let jobName = "";

  try {
    const params = jobRequest.getAllJobParams();
    const asOnDate = params.asOnDate;
    const fileName = params.fileName;
    jobName = params.jobName;

    if (!asOnDate || !/^\d{4}-\d{2}-\d{2}$/.test(asOnDate)) {
      throw new Error("Invalid asOnDate");
    }

    console.log(`ExportCashBalance (all clients): asOnDate=${asOnDate}, file=${fileName}`);

    const stratus = catalystApp.stratus();
    const bucket = stratus.bucket("upload-data-bucket");

    await zcql.executeZCQLQuery(
      `INSERT INTO Jobs (jobName, status) VALUES ('${esc(jobName)}', 'PENDING')`
    );

    const clientIds = await getAllAccountCodesFromDatabase(zcql, "clientIds");
    const dateCol = formatDateSlash(asOnDate);

    const lines = [];
    lines.push("DATE,ACCOUNT_CODE,CASH_BALANCE\n");

    for (const client of clientIds) {
      const accountCode = client.clientIds.WS_Account_code;
      try {
        const bal = await getCashBalanceAsOn(zcql, accountCode, asOnDate);
        const balStr = (Number(bal) || 0).toFixed(2);
        lines.push([dateCol, accountCode, balStr].map(csvCell).join(",") + "\n");
      } catch (rowErr) {
        console.error(`Cash balance error for ${accountCode}:`, rowErr);
        lines.push([dateCol, accountCode, "0.00"].map(csvCell).join(",") + "\n");
      }
    }

    const csvContent = lines.join("");
    await bucket.putObject(fileName, Readable.from([csvContent]), {
      overwrite: true,
      contentType: "text/csv",
    });

    await zcql.executeZCQLQuery(
      `UPDATE Jobs SET status = 'COMPLETED' WHERE jobName = '${esc(jobName)}'`
    );

    console.log("ExportCashBalance (all clients) completed");
    context.closeWithSuccess();
  } catch (error) {
    console.error("ExportCashBalance (all clients) failed:", error);
    try {
      if (jobName) {
        await zcql.executeZCQLQuery(
          `UPDATE Jobs SET status = 'FAILED' WHERE jobName = '${esc(jobName)}'`
        );
      }
    } catch (e) {
      console.error("Failed to mark job FAILED:", e);
    }
    context.closeWithFailure();
  }
};
