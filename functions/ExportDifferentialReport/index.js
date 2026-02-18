"use strict";

const catalyst = require("zcatalyst-sdk-node");
const { startDifferentialExportJob } = require("./differentialExport.js");

const escapeSql = (val) => String(val).replace(/'/g, "''");

async function insertJob(zcql, jobName) {
  try {
    await zcql.executeZCQLQuery(`
      DELETE FROM Jobs WHERE jobName='${escapeSql(jobName)}'
    `);
  } catch (_) {}
  await zcql.executeZCQLQuery(`
    INSERT INTO Jobs (jobName, status)
    VALUES ('${escapeSql(jobName)}', 'PENDING')
  `);
}

async function updateJobStatus(zcql, jobName, status) {
  await zcql.executeZCQLQuery(`
    UPDATE Jobs
    SET status='${escapeSql(status)}'
    WHERE jobName='${escapeSql(jobName)}'
  `);
}

module.exports = async (jobRequest, context) => {
  const catalystApp = catalyst.initialize(context);
  const zcql = catalystApp.zcql();

  const params = jobRequest.getAllJobParams() || {};
  const jobName = params.jobName || `DifferentialReport_${Date.now()}`;
  const fileName = params.fileName || `temp-files/differential/differential-report-${Date.now()}.csv`;

  try {
    await insertJob(zcql, jobName);
  } catch (initErr) {
    console.error(`[Export] Failed to insert job record: ${jobName}`, initErr);
    context.closeWithFailure();
    return;
  }

  try {
    const result = await startDifferentialExportJob(catalystApp, { jobName, fileName });

    await updateJobStatus(zcql, result.jobName, "COMPLETED");
    context.closeWithSuccess();
  } catch (error) {
    console.error(`[Export] Job failed: ${jobName}`, error);
    try {
      await updateJobStatus(zcql, jobName, "FAILED");
    } catch (updateErr) {}
    context.closeWithFailure();
  }
};