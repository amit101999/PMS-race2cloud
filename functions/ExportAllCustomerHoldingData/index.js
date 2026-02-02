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
  try {
    console.log("Export job started");

    const jobDetails = jobRequest.getAllJobParams();
    const { asOnDate, jobName, fileName } = jobDetails;

    const stratus = catalystApp.stratus();
    const bucket = stratus.bucket("upload-data-bucket");

    const tableName = "clientIds";

    // make the jobs status as 'Pending' in the Jobs table
    await zcql.executeZCQLQuery(`
      INSERT INTO Jobs (jobName, status)
      VALUES ('${jobName}', 'PENDING')
    `);

    /* ---------------- GET CLIENT IDS ---------------- */
    const clientIds = await getAllAccountCodesFromDatabase(zcql, tableName);

    if (!clientIds.length) {
      console.log("No clients found");
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
      "ACCOUNT_CODE,SECURITY_NAME,SECURITY_CODE,ISIN,HOLDING,WAP,HOLDING_VALUE,LAST_PRICE, MARKET_VALUE\n",
    );

    /* ---------------- FETCH & WRITE DATA ---------------- */
    let count = 0;

    for (const client of clientIds) {
      const accountCode = client.clientIds.WS_Account_code;

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

        await zcql.executeZCQLQuery(`
            INSERT INTO Holdings ( WS_Account_code , ISIN , QTY , Holding_Date) 
              VALUES ('${accountCode}' , '${row.isin}' , '${row.currentHolding}' , '${asOnDate}') `);

        csvStream.write(line + "\n");
      }

      count++;
    }

    /* ---------------- CLOSE STREAM ---------------- */
    csvStream.end();
    await uploadPromise;

    console.log("Export job completed successfully");

    // make the jobs status as 'Completed' in the Jobs table
    await zcql.executeZCQLQuery(`
        UPDATE Jobs
        SET status='COMPLETED'
       WHERE jobName='${jobName}'
    `);
    context.closeWithSuccess();
  } catch (error) {
    console.error("Export job failed:", error);

    // make the jobs status as 'Failed' in the Jobs table
    await zcql.executeZCQLQuery(`
        UPDATE Jobs
        SET status='FAILED'
       WHERE jobName='${jobName}'
    `);
    context.closeWithFailure();
  }
};
