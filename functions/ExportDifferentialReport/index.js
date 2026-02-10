"use strict";

const { PassThrough } = require("stream");
const catalyst = require("zcatalyst-sdk-node");
const { getAllDiffRows, rowsToCsvLines } = require("./diffLogic.js");

/**
 * @param {import("./types/job").JobRequest} jobRequest
 * @param {import("./types/job").Context} context
 */
module.exports = async (jobRequest, context) => {
  const catalystApp = catalyst.initialize(context);
  const zcql = catalystApp.zcql();

  try {
    const jobDetails = jobRequest.getAllJobParams();
    const jobName = jobDetails.jobName || "DifferentialReport";
    const fileName =
      jobDetails.fileName ||
      `temp-files/differential/differential-report-${Date.now()}.csv`;

    const stratus = catalystApp.stratus();
    const bucket = stratus.bucket("upload-data-bucket");

    await zcql.executeZCQLQuery(`
      INSERT INTO Jobs (jobName, status)
      VALUES ('${String(jobName).replace(/'/g, "''")}', 'PENDING')
    `);

    const rows = await getAllDiffRows(zcql);
    const csvContent = rowsToCsvLines(rows);

    const csvStream = new PassThrough();
    const uploadPromise = bucket.putObject(fileName, csvStream, {
      overwrite: true,
      contentType: "text/csv",
    });
    csvStream.write(csvContent, "utf8");
    csvStream.end();
    await uploadPromise;

    await zcql.executeZCQLQuery(`
      UPDATE Jobs
      SET status = 'COMPLETED'
      WHERE jobName = '${String(jobName).replace(/'/g, "''")}'
    `);

    context.closeWithSuccess();
  } catch (error) {
    console.error("ExportDifferentialReport failed:", error);
    try {
      const jobDetails = jobRequest.getAllJobParams();
      const jobName = jobDetails.jobName || "DifferentialReport";
      await zcql.executeZCQLQuery(`
        UPDATE Jobs
        SET status = 'FAILED'
        WHERE jobName = '${String(jobName).replace(/'/g, "''")}'
      `);
    } catch (updateErr) {
      console.error("Failed to update job status:", updateErr);
    }
    context.closeWithFailure();
  }
};
