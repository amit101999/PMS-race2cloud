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
        });

        if (!Array.isArray(rows) || !rows.length) {
          count++;
          continue;
        }

        try {
          await zcql.executeZCQLQuery(
            `DELETE FROM Holdings WHERE WS_Account_code = '${accountCode}' AND Holding_Date = '${asOnDate}'`
          );
        } catch (delErr) {
          console.error(`Error deleting old holdings for ${accountCode}:`, delErr);
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

          try {
            await zcql.executeZCQLQuery(
              `INSERT INTO Holdings (WS_Account_code, ISIN, QTY, Holding_Date) VALUES ('${accountCode}', '${(row.isin || "").replace(/'/g, "''")}', '${row.currentHolding}', '${asOnDate}')`
            );
          } catch (insertErr) {
            console.error(`Error inserting holding for ${accountCode} / ${row.isin}:`, insertErr);
          }

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

    console.log("Export job completed successfully");

    // Mark job as completed
    await zcql.executeZCQLQuery(
      `UPDATE Jobs SET status = 'COMPLETED' WHERE jobName = '${jobName}'`
    );
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
