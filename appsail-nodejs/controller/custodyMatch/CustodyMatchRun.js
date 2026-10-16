/**
 * Custody Matching — trigger, poll, report and the two gates.
 *
 * Both gates are enforced HERE, not in the browser. The disabled buttons on the
 * screen are a courtesy to the user; if the only thing standing between an
 * unreconciled file and production were a `disabled` attribute in the DOM, this
 * whole feature would be decoration.
 *
 *   Gate 1  POST /run      — every custody scheme complete AND a transaction file staged
 *   Gate 2  POST /run/:id/proceed — status COMPLETED_CLEAN AND exceptionCount === 0
 */

import { FA_FILE_TYPE, FILE_SIDE, RUN_STATUS } from "../../util/custodyMatch/constants.js";
import {
  buildRunDatesKey,
  getBucket,
  getBuffer,
  getJson,
  loadFieldMapProfiles,
  signedGetUrl,
} from "../../util/custodyMatch/storage.js";
import { PROFILE_TYPE, isDefaultProfile } from "../../util/custodyMatch/fieldMap.js";
import { faRowsToConsolidatedCsv, parseFaFile } from "../../util/custodyMatch/parseFA.js";
import {
  IMPORTED_MARKER,
  allExceptions,
  createRun,
  exceptionTypeCounts,
  failRun,
  getLatestRunForDate,
  getRun,
  isGateOpen,
  listRunsWithExceptions,
  newRunId,
  pageExceptions,
  setStageDetail,
  findImportedRunForDate,
  listFiles,
  listRunsForDate,
} from "../../util/custodyMatch/runStore.js";
import {
  DIFFERENCE_CSV_COLUMNS,
  DIFFERENCE_CSV_HEADERS,
} from "../../util/custodyMatch/compare.js";
import { uploadTempTransaction } from "../uploader/TempTransactionUpload.js";
import { uploadDayFiles } from "./CustodyMatchUpload.js";

const JOB_POOL = "CustodyMatch";
const JOB_TARGET = "CustodyMatchRunner";

const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

const fail = (res, status, body) =>
  res.status(status).json({ success: false, ...body });

const noApp = (res) =>
  fail(res, 500, { code: "CATALYST_NOT_INITIALIZED", message: "Catalyst not initialized" });

/** Shape a run row for the screen, adding the derived bits the UI would otherwise recompute. */
const toRunView = (run) => {
  if (!run) return null;
  const total = Number(run.totalKeyCount) || 0;
  const done = Number(run.comparedKeyCount) || 0;
  return {
    runId: run.runId,
    businessDate: run.businessDate,
    uploadDate: run.uploadDate || "",
    status: run.status,
    stage: run.stage,
    stageDetail: run.stageDetail,
    counts: {
      faRows: Number(run.faRowCount) || 0,
      custodyRows: Number(run.custodyRowCount) || 0,
      resolved: Number(run.resolvedCount) || 0,
      unmapped: Number(run.unmappedCount) || 0,
      totalKeys: total,
      comparedKeys: done,
      exceptions: Number(run.exceptionCount) || 0,
    },
    progressPercent: total > 0 ? Math.min(Math.round((done / total) * 100), 100) : 0,
    keys: {
      faRaw: run.faRawKey || "",
      faFinal: run.faFinalKey || "",
      custodyFinal: run.custodyFinalKey || "",
      difference: run.differenceKey || "",
    },
    error: run.errorCode ? { code: run.errorCode, message: run.errorMessage } : null,
    attemptCount: Number(run.attemptCount) || 0,
    startedAtMs: Number(run.startedAtMs) || 0,
    finishedAtMs: Number(run.finishedAtMs) || 0,
    startedBy: run.startedBy || "",
    // Terminal states the screen should stop polling on.
    isRunning: run.status === RUN_STATUS.QUEUED || run.status === RUN_STATUS.RUNNING,
    canProceed: isGateOpen(run),
    alreadyImported: String(run.stageDetail || "").startsWith(IMPORTED_MARKER),
  };
};

/** The files for a file date under one upload date, split by side. Match compares exactly these. */
const filesForDate = async (catalystApp, businessDate, uploadDate) => {
  const files = uploadDate
    ? await listFiles(catalystApp, { forDate: businessDate, uploadedOn: uploadDate })
    : [];
  return {
    faFiles: files.filter((f) => f.side === FILE_SIDE.FA),
    custodyFiles: files.filter((f) => f.side === FILE_SIDE.CUSTODY),
  };
};

/**
 * Everything Gate 1 requires, checked in one place so the reason can be shown
 * before the button is pressed rather than as a failed run afterwards.
 */
const checkGateOne = ({ faFiles, custodyFiles }) => {
  const blockers = [];

  if (faFiles.length === 0) {
    blockers.push({
      code: "NO_TRANSACTION_FILE",
      message: "FA file is not present for this date.",
    });
  }
  if (custodyFiles.length === 0) {
    blockers.push({
      code: "NO_CUSTODY_FILES",
      message: "Custodian file is not present for this date.",
    });
  }
  return blockers;
};

/**
 * A finished run for this file date and upload date whose answer still stands.
 *
 * SUPERSEDED runs are skipped: that status is set only when a file or a mapping
 * changed, so their answer no longer describes what is stored. A run that is
 * merely older is fine — nothing about it has changed.
 */
const findReusableRun = async (catalystApp, businessDate, uploadDate) => {
  const runs = await listRunsForDate(catalystApp, businessDate, 50, uploadDate);
  return (
    runs.find(
      (r) =>
        r.status === RUN_STATUS.COMPLETED_CLEAN ||
        r.status === RUN_STATUS.COMPLETED_WITH_EXCEPTIONS,
    ) || null
  );
};

/**
 * POST /api/custody-match/run
 * body: { businessDate, uploadDate }
 *
 * Gate 1. Re-checks its own preconditions, then queues the job. One run
 * compares one file date's files under one upload date; the screen starts one
 * per date the user ticks.
 */
export const startRun = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);

    const businessDate = String(req.body?.businessDate || "").trim();
    const uploadDate = String(req.body?.uploadDate || "").trim();
    if (!DATE_PATTERN.test(businessDate) || !DATE_PATTERN.test(uploadDate)) {
      return fail(res, 400, {
        code: "BAD_DATE",
        message: `businessDate and uploadDate must be yyyy-mm-dd; received "${businessDate}" and "${uploadDate}".`,
      });
    }

    const { faFiles, custodyFiles } = await filesForDate(catalystApp, businessDate, uploadDate);
    const blockers = checkGateOne({ faFiles, custodyFiles });
    if (blockers.length > 0) {
      return fail(res, 409, {
        code: "GATE_1_BLOCKED",
        message: `${businessDate} cannot be matched for upload date ${uploadDate} yet. ${blockers.map((b) => b.message).join(" ")}`,
        blockers,
        businessDate,
        uploadDate,
      });
    }

    /*
     * A pair already matched is not matched again.
     *
     * The same files can only produce the same report, so re-running would
     * spend the whole pipeline to write a second copy. The existing run is
     * handed back instead. Uploading, removing or remapping a file supersedes
     * the pair's runs, which is what makes a stale answer un-reusable.
     */
    const existing = await findReusableRun(catalystApp, businessDate, uploadDate);
    if (existing) {
      return res.status(200).json({
        success: true,
        reused: true,
        runId: existing.runId,
        businessDate,
        uploadDate,
        status: existing.status,
        message: `${businessDate} (uploaded ${uploadDate}) has already been matched. Showing that report — upload or change a file to match it again.`,
      });
    }

    const runId = newRunId();
    await createRun(catalystApp, {
      runId,
      businessDate,
      uploadDate,
      startedBy: String(req.body?.startedBy || req.headers?.["x-user"] || ""),
      // A record of what the run started from. The job reads the file table
      // itself, so these name the files rather than driving the read.
      faRawKey: faFiles[0].fileKey,
      custodyFiles: custodyFiles.map((f) => ({
        scheme: f.scheme,
        fileType: f.fileType,
        key: f.fileKey,
      })),
    });

    // job_name: alphanumeric and underscore only, and existing code caps it at
    // 20 characters — so the entropy goes at the front, not after a long prefix.
    const jobName = `CM_${Date.now()}`.slice(0, 20);

    try {
      await catalystApp.jobScheduling().JOB.submitJob({
        job_name: jobName,
        jobpool_name: JOB_POOL,
        target_name: JOB_TARGET,
        target_type: "Function",
        job_config: { number_of_retries: 3, retry_interval: 60 * 1000 },
        params: { runId, businessDate, uploadDate },
      });
    } catch (submitErr) {
      // The run row exists but nothing will pick it up. FAILED, not RUNNING, so
      // the screen stops polling and Retry can queue it again.
      console.error("[CustodyMatchRun] submitJob:", submitErr);
      await failRun(catalystApp, runId, {
        errorCode: "JOB_SUBMIT_FAILED",
        errorMessage: `Could not queue the job: ${submitErr.message}`,
      });
      return fail(res, 502, {
        code: "JOB_SUBMIT_FAILED",
        runId,
        message: `The run was created but could not be queued: ${submitErr.message}`,
        hint: `Check that the "${JOB_POOL}" job pool exists and that ${JOB_TARGET} is deployed.`,
      });
    }

    return res.status(202).json({
      success: true,
      runId,
      jobName,
      businessDate,
      uploadDate,
      status: RUN_STATUS.QUEUED,
      message: "Matching started.",
      files: {
        transaction: faFiles.map((f) => f.originalFileName).join(", "),
        custody: custodyFiles.length,
        schemes: [...new Set(custodyFiles.map((f) => f.scheme))].sort(),
      },
    });
  } catch (error) {
    console.error("[CustodyMatchRun] startRun:", error);
    return fail(res, 500, { message: error.message || "Could not start matching." });
  }
};

/** GET /api/custody-match/run/:runId — the screen's poll target. */
/**
 * GET /api/custody-match/reports?limit=10
 *
 * Past difference reports — only runs that actually found something. A clean
 * run has no report to revisit, so listing it would be noise in a list whose
 * whole purpose is "what did not reconcile".
 */
export const listReports = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);

    const limit = Math.min(Math.max(parseInt(req.query?.limit, 10) || 10, 1), 50);
    const runs = await listRunsWithExceptions(catalystApp, limit);
    const bucket = getBucket(catalystApp);

    /*
     * Which dates each run compared. Runs from before one-match-per-date could
     * be limited to some of the dates in their files, recorded in a sidecar;
     * every newer run compares just its own date.
     */
    const datesFor = async (run) => {
      try {
        const saved = await getJson(bucket, buildRunDatesKey(run.businessDate, run.runId));
        return Array.isArray(saved?.dates) ? saved.dates : [];
      } catch {
        return [];
      }
    };
    const datesByRun = await Promise.all(runs.map(datesFor));

    return res.json({
      success: true,
      count: runs.length,
      reports: runs.map((run, i) => ({
        runId: run.runId,
        businessDate: run.businessDate,
        uploadDate: run.uploadDate || "",
        dates: datesByRun[i].length > 0 ? datesByRun[i] : [run.businessDate],
        // The stored file's own name, which is what a person downloads.
        reportName: (run.differenceKey || "").split("/").pop() || "",
        differenceKey: run.differenceKey || "",
        notMatching: Number(run.exceptionCount) || 0,
        comparedKeyCount: Number(run.comparedKeyCount) || 0,
        faRowCount: Number(run.faRowCount) || 0,
        custodyRowCount: Number(run.custodyRowCount) || 0,
        matchedAtMs: Number(run.finishedAtMs) || null,
        imported: String(run.stageDetail || "").startsWith(IMPORTED_MARKER),
        // Kept for reference, but no longer the run this date would import.
        superseded: run.status === RUN_STATUS.SUPERSEDED,
      })),
    });
  } catch (error) {
    console.error("[CustodyMatchRun] listReports:", error);
    return fail(res, 500, {
      code: "REPORT_LIST_FAILED",
      message: error.message || "Could not list the previous reports.",
    });
  }
};

export const getRunStatus = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);

    const run = await getRun(catalystApp, String(req.params.runId || "").trim());
    if (!run) {
      return fail(res, 404, { code: "RUN_NOT_FOUND", message: "No such matching run." });
    }

    const view = toRunView(run);

    // Once the run is finished the breakdown is what the banner shows, so it
    // rides along rather than costing the screen a second request.
    if (!view.isRunning && view.counts.exceptions > 0) {
      view.byType = await exceptionTypeCounts(catalystApp, run.runId);
    }

    return res.status(200).json({ success: true, run: view });
  } catch (error) {
    console.error("[CustodyMatchRun] getRunStatus:", error);
    return fail(res, 500, { message: error.message || "Could not read the run." });
  }
};

/** GET /api/custody-match/latest-run/:date?uploadDate= — restores the screen on reload. */
export const getLatestRun = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);

    const date = String(req.params.date || "").trim();
    if (!DATE_PATTERN.test(date)) {
      return fail(res, 400, { code: "BAD_DATE", message: "Date must be yyyy-mm-dd." });
    }

    // Runs belong to an upload date; without one there is no run to show.
    const uploadDate = String(req.query.uploadDate || "").trim();
    if (uploadDate && !DATE_PATTERN.test(uploadDate)) {
      return fail(res, 400, { code: "BAD_UPLOAD_DATE", message: "uploadDate must be yyyy-mm-dd." });
    }

    const run = uploadDate ? await getLatestRunForDate(catalystApp, date, uploadDate) : null;
    const day = await uploadDayFiles(catalystApp, date, uploadDate);
    const view = toRunView(run);

    // Same breakdown getRunStatus attaches, so a page reload shows the banner
    // without costing the screen a second request.
    if (view && !view.isRunning && view.counts.exceptions > 0) {
      view.byType = await exceptionTypeCounts(catalystApp, run.runId);
    }

    return res.status(200).json({
      success: true,
      businessDate: date,
      run: view,
      day,
    });
  } catch (error) {
    console.error("[CustodyMatchRun] getLatestRun:", error);
    return fail(res, 500, { message: error.message || "Could not read that date." });
  }
};

/**
 * GET /api/custody-match/run/:runId/exceptions
 * query: page, size (default 10), exceptionType, scheme, status, q, from, to
 */
export const getExceptions = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);

    const runId = String(req.params.runId || "").trim();
    const run = await getRun(catalystApp, runId);
    if (!run) {
      return fail(res, 404, { code: "RUN_NOT_FOUND", message: "No such matching run." });
    }

    const page = await pageExceptions(catalystApp, runId, {
      page: req.query.page,
      size: req.query.size,
      exceptionType: req.query.exceptionType,
      scheme: req.query.scheme,
      status: req.query.status,
      q: req.query.q,
      from: req.query.from,
      to: req.query.to,
    });

    return res.status(200).json({
      success: true,
      runId,
      businessDate: run.businessDate,
      ...page,
    });
  } catch (error) {
    console.error("[CustodyMatchRun] getExceptions:", error);
    return fail(res, 500, { message: error.message || "Could not read the exceptions." });
  }
};

/**
 * GET /api/custody-match/run/:runId/exceptions/export
 *
 * Serves the difference file the run already wrote, rather than regenerating it:
 * an export that rebuilds its own data can disagree with the grid beside it.
 * Falls back to building from the table only if the stored file is missing.
 */
export const exportExceptions = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);

    const runId = String(req.params.runId || "").trim();
    const run = await getRun(catalystApp, runId);
    if (!run) {
      return fail(res, 404, { code: "RUN_NOT_FOUND", message: "No such matching run." });
    }

    if (run.differenceKey) {
      const url = await signedGetUrl(getBucket(catalystApp), run.differenceKey, 3600);
      if (url) {
        return res.status(200).json({
          success: true,
          runId,
          key: run.differenceKey,
          downloadUrl: url,
          expiresInSeconds: 3600,
          source: "stored",
        });
      }
    }

    const rows = await allExceptions(catalystApp, runId);
    return res.status(200).json({
      success: true,
      runId,
      source: "rebuilt",
      rows,
      // Sent so the client writes the same columns the stored file would have.
      // Left to guess from the row keys, a rebuild would emit whatever the table
      // happens to hold — a different report from the same button.
      columns: DIFFERENCE_CSV_COLUMNS,
      headers: DIFFERENCE_CSV_HEADERS,
      note: "The stored difference file was unavailable, so this was rebuilt from the exception table.",
    });
  } catch (error) {
    console.error("[CustodyMatchRun] exportExceptions:", error);
    return fail(res, 500, { message: error.message || "Could not export the differences." });
  }
};

/**
 * POST /api/custody-match/run/:runId/retry
 *
 * For a run that FAILED for a system reason. A run that completed with
 * exceptions is not retried — the answer would be identical; the files have to
 * change first.
 */
export const retryRun = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);

    const runId = String(req.params.runId || "").trim();
    const run = await getRun(catalystApp, runId);
    if (!run) {
      return fail(res, 404, { code: "RUN_NOT_FOUND", message: "No such matching run." });
    }

    if (run.status !== RUN_STATUS.FAILED) {
      return fail(res, 409, {
        code: "NOT_RETRYABLE",
        status: run.status,
        message:
          run.status === RUN_STATUS.COMPLETED_WITH_EXCEPTIONS
            ? "This run finished successfully and found real differences. Retrying would produce the same result — correct the source files and run the match again."
            : `A run with status ${run.status} cannot be retried.`,
      });
    }

    const jobName = `CM_${Date.now()}`.slice(0, 20);
    await catalystApp.jobScheduling().JOB.submitJob({
      job_name: jobName,
      jobpool_name: JOB_POOL,
      target_name: JOB_TARGET,
      target_type: "Function",
      job_config: { number_of_retries: 3, retry_interval: 60 * 1000 },
      params: { runId, businessDate: run.businessDate, uploadDate: run.uploadDate || "" },
    });

    return res.status(202).json({
      success: true,
      runId,
      jobName,
      resumingFrom: run.stage,
      message: `Retrying from stage ${run.stage}.`,
    });
  } catch (error) {
    console.error("[CustodyMatchRun] retryRun:", error);
    return fail(res, 500, { message: error.message || "Could not retry the run." });
  }
};

/**
 * POST /api/custody-match/run/:runId/proceed
 *
 * Gate 2 — the import.
 *
 * Exceptions do NOT block this. Matching reports what the two sides disagree
 * about; deciding whether that disagreement should stop the day is the
 * operator's call, so an unreconciled import is allowed and recorded rather
 * than refused.
 *
 * What still refuses: a run that is still working (its answer is not in yet),
 * and a run whose file has already been imported.
 *
 * The staged file is read back from the bucket and handed to the EXISTING
 * uploadTempTransaction handler unchanged, via a synthetic request carrying the
 * only two things that handler reads (`catalystApp` and `files.file`).
 */
export const proceedToImport = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);

    const runId = String(req.params.runId || "").trim();
    const run = await getRun(catalystApp, runId);
    if (!run) {
      return fail(res, 404, { code: "RUN_NOT_FOUND", message: "No such matching run." });
    }

    // A run that has not finished has no answer yet, so there is nothing to
    // import against and no count to record.
    if (run.status === RUN_STATUS.QUEUED || run.status === RUN_STATUS.RUNNING) {
      return fail(res, 409, {
        code: "RUN_NOT_FINISHED",
        status: run.status,
        message: `This run is still ${run.status.toLowerCase()}. Wait for the match to finish before importing.`,
      });
    }

    /*
     * A superseded run was answered against files that have since changed — the
     * transaction file may have been replaced outright. Importing it would put
     * in a file this run never checked, so it has to be matched again first.
     */
    if (run.status === RUN_STATUS.SUPERSEDED) {
      return fail(res, 409, {
        code: "RUN_SUPERSEDED",
        status: run.status,
        message:
          "The files behind this match changed after it ran, so its result no longer describes what is stored. Run the match again before importing.",
      });
    }

    const openExceptions = Number(run.exceptionCount) || 0;
    const reconciled = isGateOpen(run);

    // Soft guard against a double import. See the note below.
    if (String(run.stageDetail || "").startsWith(IMPORTED_MARKER)) {
      return fail(res, 409, {
        code: "ALREADY_IMPORTED",
        message: "This run has already been imported.",
      });
    }

    /*
     * The import takes the WHOLE transaction file for the date, not just the
     * dates this run compared. So the guard has to be per date: matching the
     * 20th and the 21st in two passes gives two importable runs over one file,
     * and importing both would put every row in twice.
     */
    const alreadyImported = await findImportedRunForDate(
      catalystApp,
      run.businessDate,
      IMPORTED_MARKER,
    );
    if (alreadyImported) {
      return fail(res, 409, {
        code: "DATE_ALREADY_IMPORTED",
        message: `The transaction file for ${run.businessDate} was already imported by an earlier match. Importing again would duplicate every row.`,
        importedByRunId: alreadyImported.runId,
      });
    }

    /*
     * The same files the match compared: the FA files for the run's file date
     * under the run's upload date. The guard above keeps this to one import per
     * file date, whichever upload date it comes from.
     */
    const faFiles = (
      await filesForDate(catalystApp, run.businessDate, run.uploadDate)
    ).faFiles.map((f) => ({
      key: f.fileKey,
      fileType: f.fileType,
      originalFileName: f.originalFileName,
    }));
    if (faFiles.length === 0) {
      return fail(res, 409, {
        code: "FA_FILE_MISSING",
        message:
          "The staged transaction file is no longer stored. Upload it again and re-run the match.",
      });
    }

    /*
     * What the import is handed.
     *
     * Normally the ORIGINAL file, byte for byte — whatever was exported is what
     * reaches the Transaction table, and every cell the matcher never looked at
     * survives untouched.
     *
     * The exception is a custom FA mapping. The import accepts one layout only:
     * the 26 template columns, in order, spelled exactly. A remapped file is by
     * definition not that, so handing it over unchanged fails on the header
     * check. Those files are re-laid-out into the template first.
     *
     * Deliberately re-read and re-laid-out here rather than reusing the file S1
     * wrote: that one has virtual codes rewritten to actual ones and holds only
     * the dates the run compared. The import wants the codes the user exported
     * and the whole file.
     *
     * A day may hold the regular file and the mutual fund file. Both are
     * imported — they are two halves of the day's transactions — and since the
     * import takes one file, they are rewritten into a single template-shaped
     * one. That rewrite is also what reconciles their two layouts.
     */
    const bucket = getBucket(catalystApp);
    const profiles = await loadFieldMapProfiles(catalystApp);

    const profileTypeOf = (f) =>
      f.fileType === FA_FILE_TYPE.MUTUAL_FUND ? PROFILE_TYPE.FA_MUTUAL_FUND : PROFILE_TYPE.FA;

    const only = faFiles.length === 1 ? faFiles[0] : null;
    const passThrough =
      only &&
      only.fileType !== FA_FILE_TYPE.MUTUAL_FUND &&
      isDefaultProfile(PROFILE_TYPE.FA, profiles[PROFILE_TYPE.FA]);

    let data;
    let importFileName;

    if (passThrough) {
      // One regular file on the built-in template: hand over the original bytes,
      // exactly as before the mutual fund file existed.
      data = await getBuffer(bucket, only.key);
      importFileName = only.originalFileName || "transaction.csv";
    } else {
      const merged = [];
      for (const f of faFiles) {
        const type = profileTypeOf(f);
        const relaid = await parseFaFile(await getBuffer(bucket, f.key), {
          profile: profiles[type],
          type,
        });
        if (!relaid.ok) {
          return fail(res, 409, {
            code: "FA_RELAYOUT_FAILED",
            message:
              "The transaction file no longer reads against the saved field mapping, so it cannot be rewritten into the format the import expects. Check the mapping, upload the file again and re-run the match.",
            fileName: f.originalFileName,
          });
        }
        merged.push(...relaid.rows);
      }
      // TDS and Stamp Duty come only from the mutual fund file; without it the
      // import file stays the plain 26-column template.
      const withDeductions = faFiles.some((f) => f.fileType === FA_FILE_TYPE.MUTUAL_FUND);
      data = Buffer.from(faRowsToConsolidatedCsv(merged, { withDeductions }), "utf8");
      importFileName = only?.originalFileName || `transactions-${run.businessDate}.csv`;
    }

    // Put back exactly what was there if the import refuses the file, rather
    // than a hardcoded "no differences" that would be a lie on a run that had some.
    const priorStageDetail = run.stageDetail || "";

    /*
     * Marked BEFORE the import starts, so a second click while the first is
     * still running cannot produce two imports. The marker also records whether
     * the day reconciled: an import made over open exceptions should be visible
     * afterwards, not indistinguishable from a clean one.
     */
    await setStageDetail(
      catalystApp,
      runId,
      reconciled
        ? IMPORTED_MARKER
        : `${IMPORTED_MARKER} with ${openExceptions} open exception(s)`,
    );

    const syntheticReq = {
      catalystApp,
      // Lets a background import failure take the mark back off this run.
      body: { custodyRunId: runId },
      query: {},
      headers: {},
      files: { file: { name: importFileName, data } },
    };

    // Watch the status the import sets. If it rejects the file, the marker has
    // to come back off — otherwise one failed import would lock this run out of
    // ever being imported, with no way back except a fresh match.
    const setStatus = res.status.bind(res);
    let importStatus = 200;
    res.status = (code) => {
      importStatus = code;
      return setStatus(code);
    };

    try {
      // uploadTempTransaction writes its own response, so the client sees the
      // existing pipeline's reply verbatim.
      const result = await uploadTempTransaction(syntheticReq, res);
      if (importStatus >= 400) {
        await setStageDetail(catalystApp, runId, priorStageDetail);
      }
      return result;
    } catch (importErr) {
      await setStageDetail(catalystApp, runId, priorStageDetail);
      throw importErr;
    } finally {
      res.status = setStatus;
    }
  } catch (error) {
    console.error("[CustodyMatchRun] proceedToImport:", error);
    if (!res.headersSent) {
      return fail(res, 500, { message: error.message || "Could not start the import." });
    }
    return undefined;
  }
};
