const { PassThrough } = require("stream");
const { getAllAccountCodesFromDatabase } = require("./allAccountCodes.js");
const { calculateHoldingsSummary } = require("./analyticsController.js");
const catalyst = require("zcatalyst-sdk-node");

/**
 * @param {import("./types/job").JobRequest} jobRequest
 * @param {import("./types/job").Context} context
 */
module.exports = async (jobRequest, context) => {
  try {
    console.log("Export job started");
    const catalystApp = catalyst.initialize(context);

    const jobDetails = jobRequest.getJobDetails();
    const { asOnDate } = jobDetails || {};

    const stratus = catalystApp.stratus();
    const bucket = stratus.bucket("upload-data-bucket");

    const zcql = catalystApp.zcql();
    const tableName = "clientIds";

    /* ---------------- GET CLIENT IDS ---------------- */
    const clientIds = await getAllAccountCodesFromDatabase(zcql, tableName);

    if (!clientIds.length) {
      console.log("No clients found");
      context.closeWithSuccess();
      return;
    }

    /* ---------------- CREATE STREAM ---------------- */
    const csvStream = new PassThrough();
    const fileName = `all-clients-export-${Date.now()}.csv`;

    const uploadPromise = bucket.putObject(fileName, csvStream, {
      overwrite: true,
      contentType: "text/csv",
    });

    /* ---------------- CSV HEADER ---------------- */
    csvStream.write(
      "ACCOUNT_CODE,SECURITY_NAME,SECURITY_CODE,ISIN,HOLDING,WAP,HOLDING_VALUE\n"
    );

    /* ---------------- FETCH & WRITE DATA ---------------- */
    let count = 0;

    for (const client of clientIds) {
      const accountCode = client.clientIds.WS_Account_code;

      console.log(
        `Processing client ${count + 1}/${clientIds.length} : ${accountCode}`
      );

      const rows = await calculateHoldingsSummary({
        catalystApp,
        accountCode,
        asOnDate,
      });

      if (!Array.isArray(rows) || !rows.length) {
        count++;
        continue;
      }

      for (const row of rows) {
        const line = [
          accountCode,
          row.stockName ?? "",
          row.securityCode ?? "",
          row.isin ?? "",
          row.currentHolding ?? "",
          row.avgPrice ?? "",
          row.holdingValue ?? "",
        ]
          .map((v) => `"${String(v).replace(/"/g, '""')}"`)
          .join(",");

        csvStream.write(line + "\n");
      }

      count++;
    }

    /* ---------------- CLOSE STREAM ---------------- */
    csvStream.end();
    await uploadPromise;

    console.log("Export job completed successfully");

    context.closeWithSuccess();
  } catch (error) {
    console.error("Export job failed:", error);
    context.closeWithFailure();
  }
};
