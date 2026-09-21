/**
 * Holding Comparison — FA holdings generation.
 *
 * Generate submits the FAHoldingsGenerator job, which works out every account's
 * holdings as on the date and writes FA-Holdings/<date>/fa-holdings-<ms>.csv.
 * Its progress is tracked on the Jobs row HCFA_<date>:
 * PENDING -> RUNNING -> COMPLETED | FAILED.
 */

import {
  DATE_PATTERN,
  FA_GENERATOR_TARGET,
  FA_STATUS,
  JOB_POOL,
  faJobName,
} from "../../util/holdingComparison/constants.js";
import { FOLDER, getBucket, latestForDate } from "../../util/holdingComparison/storage.js";

/*
 * The generator refreshes the row's MODIFIEDTIME on every invocation, and one
 * invocation runs for at most 15 minutes, so a row untouched for this long
 * belongs to a generation that died.
 */
const GENERATION_STALE_MS = 30 * 60 * 1000;

/*
 * Stratus lists a new file about 20 seconds after it is written, so a COMPLETED
 * row with no file yet is normal for a short while. Past this, the file has been
 * deleted and the date has nothing generated.
 */
const SNAPSHOT_LISTING_GRACE_MS = 2 * 60 * 1000;

const fail = (res, status, body) => res.status(status).json({ success: false, ...body });

/** Catalyst CREATEDTIME/MODIFIEDTIME is an IST wall-clock string like "2026-06-19 17:41:28:675". */
const parseCatalystTime = (value) => {
  if (!value) return 0;
  const iso = String(value).trim().replace(" ", "T").replace(/:(\d{3})$/, ".$1");
  const ms = new Date(`${iso}+05:30`).getTime();
  return Number.isNaN(ms) ? 0 : ms;
};

/** The Jobs row for a job name, or null. Job names here are built from a validated date. */
const readJobRow = async (catalystApp, jobName) => {
  const rows = await catalystApp
    .zcql()
    .executeZCQLQuery(
      `SELECT ROWID, status, MODIFIEDTIME FROM Jobs WHERE jobName = '${jobName}' LIMIT 1`,
    );
  const row = rows?.[0]?.Jobs;
  return row ? { ...row, modifiedMs: parseCatalystTime(row.MODIFIEDTIME) } : null;
};

const setJobStatus = async (catalystApp, jobName, status) => {
  const table = catalystApp.datastore().table("Jobs");
  const existing = await readJobRow(catalystApp, jobName);
  if (existing) await table.updateRow({ ROWID: existing.ROWID, status });
  else await table.insertRow({ jobName, status });
};

const submitGenerator = (catalystApp, asOnDate) =>
  catalystApp.jobScheduling().JOB.submitJob({
    // job_name: alphanumeric and underscore only, 20 characters at most.
    job_name: `HCFA_${Date.now()}`.slice(0, 20),
    jobpool_name: JOB_POOL,
    target_name: FA_GENERATOR_TARGET,
    target_type: "Function",
    job_config: { number_of_retries: 2, retry_interval: 60 * 1000 },
    params: { asOnDate, generationId: String(Date.now()) },
  });

/**
 * Where FA holdings generation stands for a date.
 *
 * @returns {Promise<{ status: string, message: string, latestSnapshot: object|null }>}
 */
export const readFaStatus = async (catalystApp, date) => {
  const faJob = await readJobRow(catalystApp, faJobName(date));
  let status = faJob?.status || FA_STATUS.NOT_STARTED;
  let message = "";

  if (
    (status === FA_STATUS.PENDING || status === FA_STATUS.RUNNING) &&
    Date.now() - faJob.modifiedMs > GENERATION_STALE_MS
  ) {
    await setJobStatus(catalystApp, faJobName(date), FA_STATUS.FAILED);
    status = FA_STATUS.FAILED;
    message = "FA holdings generation stopped without finishing. Try generating again.";
  }

  if (status === FA_STATUS.FAILED && !message) {
    message = "FA holdings generation failed. Try generating again.";
  }

  const latestSnapshot = await latestForDate(getBucket(catalystApp), FOLDER.FA, date);

  // The Jobs row outlives the file: once the snapshot is deleted from Stratus,
  // report the date as not generated rather than COMPLETED with nothing to show.
  if (
    status === FA_STATUS.COMPLETED &&
    !latestSnapshot &&
    Date.now() - faJob.modifiedMs > SNAPSHOT_LISTING_GRACE_MS
  ) {
    status = FA_STATUS.NOT_STARTED;
  }

  return { status, message, latestSnapshot };
};

/** POST /api/holding-comparison/fa-holdings/generate  body: { businessDate } */
export const generateFaHoldings = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const date = String(req.body?.businessDate || "").trim();
    if (!DATE_PATTERN.test(date)) {
      return fail(res, 400, { code: "BAD_DATE", message: "businessDate must be yyyy-mm-dd." });
    }

    const current = await readJobRow(catalystApp, faJobName(date));
    if (
      current &&
      (current.status === FA_STATUS.PENDING || current.status === FA_STATUS.RUNNING) &&
      Date.now() - current.modifiedMs <= GENERATION_STALE_MS
    ) {
      return fail(res, 409, {
        code: "FA_GENERATION_IN_PROGRESS",
        message: "FA holdings for this date are already being generated.",
      });
    }

    await setJobStatus(catalystApp, faJobName(date), FA_STATUS.PENDING);
    try {
      await submitGenerator(catalystApp, date);
    } catch (err) {
      console.error("[HoldingComparison] submit FAHoldingsGenerator:", err);
      await setJobStatus(catalystApp, faJobName(date), FA_STATUS.FAILED);
      return fail(res, 502, {
        code: "JOB_SUBMIT_FAILED",
        message: `Could not start FA holdings generation: ${err.message}`,
      });
    }

    return res.status(202).json({
      success: true,
      businessDate: date,
      status: FA_STATUS.PENDING,
      message: "FA holdings generation started.",
    });
  } catch (error) {
    console.error("[HoldingComparison] generateFaHoldings:", error);
    return fail(res, 500, { message: error.message || "Could not start FA holdings generation." });
  }
};

/** GET /api/holding-comparison/fa-holdings/:date/status */
export const getFaHoldingsStatus = async (req, res) => {
  try {
    const date = String(req.params.date || "").trim();
    if (!DATE_PATTERN.test(date)) {
      return fail(res, 400, { code: "BAD_DATE", message: "Date must be yyyy-mm-dd." });
    }
    return res.json({ success: true, businessDate: date, ...(await readFaStatus(req.catalystApp, date)) });
  } catch (error) {
    console.error("[HoldingComparison] getFaHoldingsStatus:", error);
    return fail(res, 500, { message: error.message || "Could not read FA holdings status." });
  }
};
