const { PassThrough } = require("stream");
const { getAllAccountCodesFromDatabase } = require("./allAccountCodes.js");
const { calculateHoldingsSummary } = require("./analyticsController.js");
const catalyst = require("zcatalyst-sdk-node");

/**
 * @param {import("./types/job").JobRequest} jobRequest
 * @param {import("./types/job").Context} context
 */
module.exports = async (jobRequest, context) => {
  const catalystApp = catalyst.initialize(context);
  const zcql = catalystApp.zcql();
  let jobName = "";
  try {
    console.log("Export job started");

    const jobDetails = jobRequest.getAllJobParams();
    const { asOnDate, fileName } = jobDetails;
    jobName = jobDetails.jobName;

    const stratus = catalystApp.stratus();
    const bucket = stratus.bucket("upload-data-bucket");

    const tableName = "clientIds";

    // Insert job status as PENDING
    await zcql.executeZCQLQuery(
      `INSERT INTO Jobs (jobName, status) VALUES ('${jobName}', 'PENDING')`
    );

    /* ---------------- GET CLIENT IDS ---------------- */
    const clientIds = await getAllAccountCodesFromDatabase(zcql, tableName);

    if (!clientIds.length) {
      console.log("No clients found");
      await zcql.executeZCQLQuery(
        `UPDATE Jobs SET status = 'COMPLETED' WHERE jobName = '${jobName}'`
      );
      context.closeWithSuccess();
      return;
    }

    /* ---------------- CREATE STREAM ---------------- */
    const csvStream = new PassThrough();
    let streamError = null;
    csvStream.on("error", (err) => {
      streamError = err;
      console.error("CSV stream error:", err);
    });

    const uploadPromise = bucket.putObject(fileName, csvStream, {
      overwrite: true,
      contentType: "text/csv",
    });

    /* ---------------- CSV HEADER ---------------- */
    csvStream.write(
      "ACCOUNT_CODE,SECURITY_NAME,SECURITY_CODE,ISIN,HOLDING,WAP,HOLDING_VALUE,LAST_PRICE,MARKET_VALUE\n",
    );

    /* ---------------- FETCH & WRITE DATA ---------------- */
    let count = 0;
    let errorCount = 0;
    const sharedPriceMap = {};

    for (const client of clientIds) {
      const accountCode = client.clientIds.WS_Account_code;

      try {
        console.log(
          `Processing client ${count + 1}/${clientIds.length} : ${accountCode}`,
        );

        const rows = await calculateHoldingsSummary({
          catalystApp,
          accountCode,
          asOnDate,
          sharedPriceMap,
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
            row.lastPrice ?? "",
            row.marketValue ?? "",
          ]
            .map((v) => `"${String(v).replace(/"/g, '""')}"`)
            .join(",");

          csvStream.write(line + "\n");
        }

        count++;
      } catch (clientErr) {
        errorCount++;
        console.error(`Error processing client ${accountCode} (${errorCount} errors so far):`, clientErr);
        count++;
      }
    }

    console.log(`Processed ${count} clients, ${errorCount} had errors`);

    /* ---------------- CLOSE STREAM ---------------- */
    csvStream.end();
    await uploadPromise;

    if (streamError) {
      throw new Error(`Stream error during upload: ${streamError.message}`);
    }

    console.log("Export job completed successfully");

    try {
      await zcql.executeZCQLQuery(
        `UPDATE Jobs SET status = 'COMPLETED' WHERE jobName = '${jobName}'`
      );
    } catch (statusErr) {
      console.error("Failed to mark job as COMPLETED (file was uploaded successfully):", statusErr);
    }
    context.closeWithSuccess();
  } catch (error) {
    console.error("Export job failed:", error);

    // Mark job as failed
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
