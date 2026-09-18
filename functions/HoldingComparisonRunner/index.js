"use strict";

/**
 * HoldingComparisonRunner — compares FA holdings with custodian holdings (S1-S5).
 *
 * Params (from POST /api/holding-comparison/run):
 *   runId         Holding_Comparison_Run.runId
 *   businessDate  yyyy-mm-dd
 *
 *   S1 READ_CUSTODY  parse the staged custodian file with the saved field mapping
 *   S2 READ_FA       parse the FA holdings snapshot
 *   S3 COMPARE       key = account code + ISIN; only quantity is compared
 *   S4 REPORT        write the difference CSV (fixed key per run, so a retry overwrites it)
 *   S5 DONE          counts and COMPLETED_CLEAN / COMPLETED_WITH_EXCEPTIONS
 *
 * Each stage is recorded on the run row BEFORE it starts, so a crash shows the
 * stage that needs re-running. Every stage is cheap and repeatable, so a retry
 * simply starts again from S1.
 *
 * ./holdingComparison/ is a straight copy of appsail-nodejs/util/holdingComparison/.
 * Re-copy after changing it:
 *
 *   cp appsail-nodejs/util/holdingComparison/*.js functions/HoldingComparisonRunner/holdingComparison/
 */

const catalyst = require("zcatalyst-sdk-node");

const withCode = (message, code) => Object.assign(new Error(message), { code });

module.exports = async (jobRequest, context) => {
  // Admin scope is required: without it Data Store calls in a job stall until the timeout.
  const app = catalyst.initialize(context, { scope: "admin" });

  // The shared helpers are ESM; this function is CommonJS.
  const HC = await import("./holdingComparison/index.js");

  const params = jobRequest.getAllJobParams() || {};
  const runId = String(params.runId || "").trim();
  const businessDate = String(params.businessDate || "").trim();
  const log = (msg) => console.log(`[HoldingComparison ${runId}] ${msg}`);

  if (!runId || !businessDate) {
    console.error(`HoldingComparisonRunner: missing params (runId="${runId}", businessDate="${businessDate}")`);
    context.closeWithFailure();
    return;
  }

  try {
    const run = await HC.getRun(app, runId);
    if (!run) {
      console.error(`HoldingComparisonRunner: no run row for ${runId}`);
      context.closeWithFailure();
      return;
    }

    // Catalyst can redeliver a job that already finished.
    if (run.status !== HC.RUN_STATUS.QUEUED && run.status !== HC.RUN_STATUS.RUNNING) {
      log(`already ${run.status}; nothing to do`);
      context.closeWithSuccess();
      return;
    }

    const bucket = HC.getBucket(app);

    const readStaged = async (key, what) => {
      try {
        return await HC.getBuffer(bucket, key);
      } catch (err) {
        throw withCode(`The ${what} (${key}) is no longer stored: ${err.message}`, "FILE_MISSING");
      }
    };

    /* S1 — custodian side */
    await HC.setStage(app, runId, HC.STAGE.READ_CUSTODY, "Reading the custodian holdings file");
    const profile = await HC.readFieldMapProfile(bucket, HC.CUSTODY_PROFILE_TYPE);
    const custody = await HC.parseCustodyHoldings(await readStaged(run.custodyKey, "custodian file"), profile);
    if (custody.fileError) {
      // The field mapping may have changed since the file was uploaded.
      throw withCode(custody.fileError.message, custody.fileError.code);
    }
    if (!custody.ok) {
      throw withCode(
        `${custody.rowErrorCount} row(s) in the custodian file could not be read. Upload the file again.`,
        "CUSTODY_VALIDATION_FAILED",
      );
    }
    log(`S1: ${custody.rowCount} rows, ${custody.holdings.size} holdings`);

    /* S2 — FA side */
    await HC.setStage(app, runId, HC.STAGE.READ_FA, "Reading the FA holdings file");
    const fa = await HC.parseFaHoldings(await readStaged(run.faHoldingsKey, "FA holdings file"));
    if (fa.fileError) throw withCode(fa.fileError.message, fa.fileError.code);
    if (!fa.ok) {
      throw withCode(
        `${fa.rowErrorCount} row(s) in the FA holdings file are missing an account code, ISIN or holding. Generate FA holdings again.`,
        "FA_VALIDATION_FAILED",
      );
    }
    log(`S2: ${fa.rowCount} rows, ${fa.holdings.size} holdings`);

    /* S3 — compare */
    await HC.setStage(app, runId, HC.STAGE.COMPARE, "Comparing holdings");
    const { exceptions, totalKeyCount, counts } = HC.compareHoldings(fa.holdings, custody.holdings);
    log(`S3: ${totalKeyCount} keys, ${exceptions.length} difference(s)`);

    /* S4 — report */
    await HC.setStage(app, runId, HC.STAGE.REPORT, "Writing the difference report");
    const differenceKey = HC.buildDifferenceKey(businessDate, runId);
    await HC.putText(bucket, differenceKey, HC.exceptionsToCsv(exceptions));

    /* S5 — done */
    await HC.finishRun(app, runId, {
      differenceKey,
      exceptionCount: exceptions.length,
      totalKeyCount,
      custodyRowCount: custody.rowCount,
      faRowCount: fa.rowCount,
      ...counts,
    });

    log(exceptions.length === 0 ? "done — holdings agree" : `done — ${exceptions.length} difference(s)`);
    context.closeWithSuccess();
  } catch (error) {
    const code = error?.code || "UNEXPECTED_ERROR";
    const message = error?.message || String(error);
    console.error(`HoldingComparisonRunner failed (${code}):`, message);
    try {
      await HC.failRun(app, runId, { errorCode: code, errorMessage: message });
    } catch (writeErr) {
      console.error("HoldingComparisonRunner: could not record the failure:", writeErr?.message || writeErr);
    }
    context.closeWithFailure();
  }
};
