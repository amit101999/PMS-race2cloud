/**
 * Holding Comparison — Gate 1, run start, polling and the exception preview.
 *
 * Gate 1 is enforced here, not in the browser: a comparison starts only when a
 * custodian file is staged AND FA holdings have been generated for the date.
 * There is no Gate 2 — the module never writes to production.
 */

import {
  DATE_PATTERN,
  FA_STATUS,
  JOB_POOL,
  RUNNER_TARGET,
  RUN_STATUS,
} from "../../util/holdingComparison/constants.js";
import { parseDifferenceCsv } from "../../util/holdingComparison/parse.js";
import {
  createRun,
  failRun,
  getRun,
  listRunsForDate,
  newRunId,
} from "../../util/holdingComparison/runStore.js";
import {
  FOLDER,
  getBucket,
  getBuffer,
  isUploadedSinceCleared,
  parseKey,
  stagedCustodyFile,
} from "../../util/holdingComparison/storage.js";
import { STRATUS_GET_URL_OPTS, stratusSignedUrlToString } from "../../util/stratusSignedUrl.js";
import { readFaStatus } from "./HoldingComparisonFaHoldings.js";

const PREVIEW_ROWS = 10;

const fail = (res, status, body) => res.status(status).json({ success: false, ...body });

const toRunView = (run) => {
  if (!run) return null;
  return {
    runId: run.runId,
    businessDate: run.businessDate,
    status: run.status,
    stage: run.stage,
    stageDetail: run.stageDetail,
    counts: {
      custodyRows: run.custodyRowCount,
      faRows: run.faRowCount,
      totalKeys: run.totalKeyCount,
      exceptions: run.exceptionCount,
      quantityMismatch: run.quantityMismatchCount,
      missingInCustody: run.missingInCustodyCount,
      missingInFa: run.missingInFaCount,
    },
    hasReport: Boolean(run.differenceKey),
    error: run.errorCode ? { code: run.errorCode, message: run.errorMessage } : null,
    startedAtMs: run.startedAtMs,
    finishedAtMs: run.finishedAtMs,
    isRunning: run.status === RUN_STATUS.QUEUED || run.status === RUN_STATUS.RUNNING,
  };
};

/**
 * The custodian file this date's run should use.
 *
 * Stratus listings lag a fresh write by about twenty seconds, so a file uploaded
 * a moment ago may not be listed yet. The screen sends the key its upload got
 * back; it is used when it belongs to this date, still exists, and is newer
 * than anything listed. Otherwise the run could silently compare the older file.
 */
const resolveCustodyFile = async (bucket, date, clientKey) => {
  const listed = await stagedCustodyFile(bucket, date);
  const sent = parseKey(String(clientKey || ""));
  const sentIsNewer =
    sent &&
    sent.key.startsWith(`${FOLDER.CUSTODY}/${date}/`) &&
    (!listed || sent.timeMs > listed.timeMs);

  if (
    sentIsNewer &&
    (await isUploadedSinceCleared(bucket, date, sent.timeMs)) &&
    (await bucket.headObject(sent.key, { throwErr: false }))
  ) {
    return sent;
  }
  return listed;
};

const gateOneBlockers = (custodyFile, fa) => {
  const blockers = [];
  if (!custodyFile) {
    blockers.push({
      code: "NO_CUSTODY_FILE",
      message: "No custodian holdings file has been uploaded for this date.",
    });
  }
  if (fa.status !== FA_STATUS.COMPLETED || !fa.latestSnapshot) {
    blockers.push({
      code: "NO_FA_HOLDINGS",
      message: "FA holdings have not been generated for this date.",
    });
  }
  return blockers;
};

/** GET /api/holding-comparison/day/:date — everything the screen needs for a date. */
export const getDay = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const date = String(req.params.date || "").trim();
    if (!DATE_PATTERN.test(date)) {
      return fail(res, 400, { code: "BAD_DATE", message: "Date must be yyyy-mm-dd." });
    }

    const [custodyFile, fa, runs] = await Promise.all([
      stagedCustodyFile(getBucket(catalystApp), date),
      readFaStatus(catalystApp, date),
      listRunsForDate(catalystApp, date),
    ]);

    // Only a run over the files staged right now describes them. An older run
    // (a failure over a replaced file, say) would be shown next to files it
    // never read, so the screen shows no result until the next comparison.
    const run =
      runs.find(
        (r) => r.custodyKey === custodyFile?.key && r.faHoldingsKey === fa.latestSnapshot?.key,
      ) || null;

    return res.json({
      success: true,
      businessDate: date,
      custodyFile,
      fa,
      run: toRunView(run),
      blockers: gateOneBlockers(custodyFile, fa),
    });
  } catch (error) {
    console.error("[HoldingComparison] getDay:", error);
    return fail(res, 500, { message: error.message || "Could not read that date." });
  }
};

/** POST /api/holding-comparison/run  body: { businessDate, custodyKey? } — Gate 1 */
export const startRun = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const businessDate = String(req.body?.businessDate || "").trim();
    if (!DATE_PATTERN.test(businessDate)) {
      return fail(res, 400, { code: "BAD_DATE", message: "businessDate must be yyyy-mm-dd." });
    }

    const bucket = getBucket(catalystApp);
    const [custodyFile, fa] = await Promise.all([
      resolveCustodyFile(bucket, businessDate, req.body?.custodyKey),
      readFaStatus(catalystApp, businessDate),
    ]);

    const blockers = gateOneBlockers(custodyFile, fa);
    if (blockers.length > 0) {
      return fail(res, 409, {
        code: "GATE_1_BLOCKED",
        message: "The comparison cannot start yet.",
        blockers,
      });
    }

    const custodyKey = custodyFile.key;
    const faHoldingsKey = fa.latestSnapshot.key;

    /*
     * The same inputs can only give the same answer, so a finished run over
     * exactly these two files is handed back instead of recomputed. Superseded
     * runs never match: that status means an input or the mapping changed.
     */
    const runs = await listRunsForDate(catalystApp, businessDate);
    const reusable = runs.find(
      (r) =>
        r.custodyKey === custodyKey &&
        r.faHoldingsKey === faHoldingsKey &&
        (r.status === RUN_STATUS.COMPLETED_CLEAN ||
          r.status === RUN_STATUS.COMPLETED_WITH_EXCEPTIONS ||
          r.status === RUN_STATUS.QUEUED ||
          r.status === RUN_STATUS.RUNNING),
    );
    if (reusable) {
      return res.json({
        success: true,
        reused: true,
        run: toRunView(reusable),
        message: reusable.status === RUN_STATUS.QUEUED || reusable.status === RUN_STATUS.RUNNING
          ? "A comparison of these files is already running."
          : "These files have already been compared. Showing that result — upload a new file or regenerate FA holdings to compare again.",
      });
    }

    const runId = newRunId();
    const run = await createRun(catalystApp, { runId, businessDate, custodyKey, faHoldingsKey });

    try {
      await catalystApp.jobScheduling().JOB.submitJob({
        job_name: `HC_${Date.now()}`.slice(0, 20),
        jobpool_name: JOB_POOL,
        target_name: RUNNER_TARGET,
        target_type: "Function",
        job_config: { number_of_retries: 2, retry_interval: 60 * 1000 },
        params: { runId, businessDate },
      });
    } catch (submitErr) {
      console.error("[HoldingComparison] submitJob:", submitErr);
      // Otherwise the screen would poll a run that nothing will ever pick up.
      await failRun(catalystApp, runId, {
        errorCode: "JOB_SUBMIT_FAILED",
        errorMessage: submitErr.message,
      });
      return fail(res, 502, {
        code: "JOB_SUBMIT_FAILED",
        runId,
        message: `The comparison could not be queued: ${submitErr.message}`,
        hint: `Check that the "${JOB_POOL}" job pool exists and that ${RUNNER_TARGET} is deployed.`,
      });
    }

    return res.status(202).json({
      success: true,
      reused: false,
      run: toRunView(run),
      message: "Comparison started.",
    });
  } catch (error) {
    console.error("[HoldingComparison] startRun:", error);
    return fail(res, 500, { message: error.message || "Could not start the comparison." });
  }
};

/** GET /api/holding-comparison/run/:runId — poll target */
export const getRunStatus = async (req, res) => {
  try {
    const run = await getRun(req.catalystApp, String(req.params.runId || "").trim());
    if (!run) return fail(res, 404, { code: "RUN_NOT_FOUND", message: "No such comparison run." });
    return res.json({ success: true, run: toRunView(run) });
  } catch (error) {
    console.error("[HoldingComparison] getRunStatus:", error);
    return fail(res, 500, { message: error.message || "Could not read the run." });
  }
};

/**
 * GET /api/holding-comparison/run/:runId/exceptions
 *
 * A sample only: the first 10 rows of the run's difference report. The full
 * result is the Export.
 */
export const getExceptions = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const run = await getRun(catalystApp, String(req.params.runId || "").trim());
    if (!run) return fail(res, 404, { code: "RUN_NOT_FOUND", message: "No such comparison run." });
    if (!run.differenceKey) {
      return res.json({ success: true, rows: [], totalCount: 0 });
    }

    const all = await parseDifferenceCsv(await getBuffer(getBucket(catalystApp), run.differenceKey));

    const numberOrNull = (v) => (v === "" || v === undefined ? null : Number(v));
    return res.json({
      success: true,
      rows: all.slice(0, PREVIEW_ROWS).map((r) => ({
        accountCode: r.ACCOUNT_CODE,
        isin: r.ISIN,
        securityName: r.SECURITY_NAME,
        asOfDate: r.AS_OF_DATE,
        faQuantity: numberOrNull(r.FA_HOLDING_QTY),
        custodyQuantity: numberOrNull(r.CUSTODY_QTY),
        difference: numberOrNull(r.DIFFERENCE),
        exceptionType: r.EXCEPTION_TYPE,
      })),
      totalCount: all.length,
    });
  } catch (error) {
    console.error("[HoldingComparison] getExceptions:", error);
    return fail(res, 500, { message: error.message || "Could not read the differences." });
  }
};

/** GET /api/holding-comparison/run/:runId/export — presigned URL for the full difference report */
export const exportExceptions = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const run = await getRun(catalystApp, String(req.params.runId || "").trim());
    if (!run) return fail(res, 404, { code: "RUN_NOT_FOUND", message: "No such comparison run." });
    if (!run.differenceKey) {
      return fail(res, 409, { code: "NO_REPORT", message: "This run has no difference report." });
    }

    const raw = await getBucket(catalystApp).generatePreSignedUrl(
      run.differenceKey,
      "GET",
      STRATUS_GET_URL_OPTS,
    );
    const downloadUrl = stratusSignedUrlToString(raw);
    if (!downloadUrl) {
      return fail(res, 404, { code: "NOT_FOUND", message: "Could not prepare the download." });
    }
    return res.json({ success: true, runId: run.runId, downloadUrl, expiresInSeconds: 3600 });
  } catch (error) {
    console.error("[HoldingComparison] exportExceptions:", error);
    return fail(res, 500, { message: error.message || "Could not export the differences." });
  }
};
