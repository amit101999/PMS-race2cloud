"use strict";

const { PassThrough } = require("stream");
const catalyst = require("zcatalyst-sdk-node");
const { getAllDiffRows, rowsToCsvLines, CSV_HEADER } = require("./diffLogic.js");

const escapeSql = (val) => String(val).replace(/'/g, "''");

/* -------------------- DB Helpers -------------------- */

/**
 * Upsert-style insert: use INSERT OR REPLACE so that if the job row already
 * exists (e.g. from a previous failed attempt) it gets overwritten cleanly.
 * This prevents duplicate-key errors that can leave the job stuck at PENDING.
 */
async function insertJob(zcql, jobName) {
  // First try to delete any stale row with the same name, then insert fresh.
  // ZCQL does not support ON CONFLICT so we do delete + insert.
  try {
    await zcql.executeZCQLQuery(`
      DELETE FROM Jobs WHERE jobName='${escapeSql(jobName)}'
    `);
  } catch (_) {
    // Row may not exist yet — ignore the error.
  }

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

/* -------------------- Upload Helper -------------------- */
/**
 * Stream the CSV content directly to Stratus instead of building a giant string
 * in memory first. This prevents OOM crashes for large transaction files.
 */
async function uploadCsv(bucket, fileName, csvContent) {
  const stream = new PassThrough();

  const uploadPromise = bucket.putObject(fileName, stream, {
    overwrite: true,
    contentType: "text/csv",
  });

  stream.end(csvContent);
  await uploadPromise;
}

/* -------------------- Job Entry -------------------- */

module.exports = async (jobRequest, context) => {
  const catalystApp = catalyst.initialize(context);
  const zcql = catalystApp.zcql();
  const bucket = catalystApp.stratus().bucket("upload-data-bucket");

  const params = jobRequest.getAllJobParams();

  const jobName =
    params.jobName || `DifferentialReport_${Date.now()}`;
  const fileName =
    params.fileName ||
    `temp-files/differential/differential-report-${Date.now()}.csv`;

  // ── Step 1: Mark job as PENDING immediately so the status API can find it ──
  try {
    await insertJob(zcql, jobName);
  } catch (initErr) {
    // If we can't even write the job row, fail fast so Catalyst retries the job.
    console.error(`[Export] Failed to insert job record: ${jobName}`, initErr);
    context.closeWithFailure();
    return;
  }

  try {
    console.log(`[Export] Job started: ${jobName}`);

    // ── Step 2: Fetch all diff rows (one per Temp_Transaction row) ────────────
    const rows = await getAllDiffRows(zcql);
    console.log(`[Export] Total rows to export: ${rows.length}`);

    if (rows.length === 0) {
      // Nothing to export — write an empty CSV with just the header.
      await uploadCsv(bucket, fileName, CSV_HEADER + "\n");
      console.warn(`[Export] No rows found — empty CSV written.`);
    } else {
      // ── Step 3: Convert to CSV and upload ──────────────────────────────────
      const csvContent = rowsToCsvLines(rows);
      await uploadCsv(bucket, fileName, csvContent);
      console.log(`[Export] CSV uploaded: ${fileName}`);
    }

    // ── Step 4: Mark COMPLETED ────────────────────────────────────────────────
    await updateJobStatus(zcql, jobName, "COMPLETED");
    console.log(`[Export] Job completed: ${jobName}`);

    context.closeWithSuccess();
  } catch (error) {
    console.error(`[Export] Job failed: ${jobName}`, error);

    try {
      await updateJobStatus(zcql, jobName, "FAILED");
    } catch (updateErr) {
      console.error(`[Export] Could not update job status to FAILED:`, updateErr);
    }

    context.closeWithFailure();
  }
};