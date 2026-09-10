"use strict";

/**
 * CustodyMatchRunner — the custody matching pipeline (stages S1-S6).
 *
 * Params (from POST /api/custody-match/run):
 *   runId         Custody_Match_Run.runId
 *   businessDate  yyyy-mm-dd
 *
 * Every stage writes its checkpoint to Custody_Match_Run BEFORE the stage's work
 * starts, so a crash leaves `stage` pointing at the step that needs re-running
 * rather than the one that finished. Catalyst auto-retries job functions, so
 * each stage is written to be safely repeatable:
 *
 *   S1 BUILD_FA        re-reads the raw file; writes final-<date>-FA.csv at a fixed key
 *   S2 BUILD_CUSTODY   re-reads the custodian files; writes final-<date>-Custodian.csv
 *   S3 PERSIST         verifies both finals read back before anything is compared
 *   S4 COMPARE         always re-walks from zero (see the note at the walk) and
 *                      reports live progress via comparedKeyCount
 *   S5 REPORT          clears this run's exceptions first, then rewrites them
 *   S6 DONE            terminal status and counts
 *
 * The two finals are persisted rather than held in memory precisely so S4 and S5
 * can resume without redoing S1 and S2 — and so a disputed result can be
 * reproduced later from exactly what was compared.
 *
 * ---------------------------------------------------------------------------
 * MODULE LAYOUT
 *
 * ./custodyMatch/ is a straight file copy of appsail-nodejs/util/custodyMatch/,
 * carrying its own package.json with { "type": "module" } so those ESM files load
 * unchanged inside this CommonJS function. Re-vendor after any change to the
 * shared module:
 *
 *   cp appsail-nodejs/util/custodyMatch/*.js functions/CustodyMatchRunner/custodyMatch/
 * ---------------------------------------------------------------------------
 */

const catalyst = require("zcatalyst-sdk-node");

/** Keys compared between checkpoint writes. Small enough to lose little on a crash, large enough not to hammer the run row. */
const COMPARE_CHUNK = 2000;

/** Stop and hand back before the 15-minute wall so the checkpoint is always written. */
const TIME_BUFFER_MS = 45_000;

const STAGE_ORDER = ["S0", "S1", "S2", "S3", "S4", "S5", "S6"];

const atOrBefore = (current, target) =>
  STAGE_ORDER.indexOf(current) <= STAGE_ORDER.indexOf(target);

function getJobParams(jobRequest) {
  try {
    if (jobRequest && typeof jobRequest.getAllJobParams === "function") {
      return jobRequest.getAllJobParams() || {};
    }
  } catch (err) {
    console.error("CustodyMatchRunner: could not read job params:", err?.message || err);
  }
  return {};
}

function remainingMs(context) {
  try {
    if (typeof context.getRemainingExecutionTimeMs === "function") {
      return context.getRemainingExecutionTimeMs();
    }
  } catch {
    /* fall through */
  }
  return Number.POSITIVE_INFINITY;
}

module.exports = async (jobRequest, context) => {
  const startedAt = Date.now();

  // Admin scope is not optional. Without it every DataStore and ZCQL call here
  // does not error — it stalls roughly 60 seconds, until the 15-minute timeout
  // kills the job with nothing useful in the logs.
  const app = catalyst.initialize(context, { scope: "admin" });

  // The shared module is ESM; this function is CommonJS. Dynamic import bridges
  // the two without a second copy of the logic in a different module system.
  const CM = await import("./custodyMatch/index.js");

  /*
   * The saved field mappings, loaded once for the whole run. Every parse below
   * goes through them, so the job reads each file exactly the way the upload
   * screen validated it. Unsaved mappings fall back to the built-in layouts.
   */
  const profiles = await CM.loadFieldMapProfiles(app);

  const params = getJobParams(jobRequest);
  const runId = String(params.runId || "").trim();
  const businessDate = String(params.businessDate || "").trim();

  if (!runId || !businessDate) {
    console.error(
      `CustodyMatchRunner: missing params (runId="${runId}", businessDate="${businessDate}")`,
    );
    context.closeWithFailure();
    return;
  }

  const log = (msg) => console.log(`[CustodyMatch ${runId}] ${msg}`);

  try {
    const run = await CM.getRun(app, runId);
    if (!run) {
      console.error(`CustodyMatchRunner: no run row for ${runId}`);
      context.closeWithFailure();
      return;
    }

    // A retry of an already-finished run is a no-op, not a re-run. Catalyst can
    // redeliver a job whose handler already completed.
    if (
      run.status === CM.RUN_STATUS.COMPLETED_CLEAN ||
      run.status === CM.RUN_STATUS.COMPLETED_WITH_EXCEPTIONS ||
      run.status === CM.RUN_STATUS.SUPERSEDED
    ) {
      log(`already ${run.status}; nothing to do`);
      context.closeWithSuccess();
      return;
    }

    await CM.incrementAttempt(app, runId);

    const bucket = CM.getBucket(app);
    const zcql = app.zcql();

    /*
     * An optional date filter, written beside the run when it was created.
     * Absent means compare every date the files contain. Read from the bucket
     * rather than the job params so a retry — which arrives as a fresh
     * submission — still honours what the run was asked to do.
     */
    let selectedDates = null;
    try {
      const saved = await CM.getJson(bucket, CM.buildRunDatesKey(businessDate, runId));
      if (saved && Array.isArray(saved.dates) && saved.dates.length > 0) {
        selectedDates = new Set(saved.dates);
      }
    } catch {
      // No sidecar is the normal case: the run was not limited to any dates.
    }
    const wanted = (d) => !selectedDates || selectedDates.has(d);

    /*
     * A custodian file covers a span of days, so it belongs to the run when ANY
     * day it covers was selected — rows outside the selection are dropped later,
     * row by row, the same way the FA side is.
     */
    const rangeWanted = (from, to) => {
      if (!selectedDates) return true;
      for (const d of selectedDates) {
        if (d >= from && d <= to) return true;
      }
      return false;
    };
    if (selectedDates) log(`limited to ${[...selectedDates].sort().join(", ")}`);
    const resumeFrom = STAGE_ORDER.includes(run.stage) ? run.stage : "S0";
    if (resumeFrom !== "S0" && resumeFrom !== "S1") {
      log(`resuming from ${resumeFrom} (attempt ${(run.attemptCount || 0) + 1})`);
    }

    const day = await CM.listDay(app, businessDate, { bucket });

    const faFinalKey = CM.buildFaFinalKey(businessDate);
    const custodyFinalKey = CM.buildCustodyFinalKey(businessDate);

    let unmapped = [];
    let resolvedCount = 0;
    let actualToVirtual = {};

    /* ─── S1 — build the FA side ──────────────────────────────────── */
    if (atOrBefore(resumeFrom, "S1")) {
      await CM.setStage(app, runId, CM.STAGE.BUILD_FA, "Reading the transaction file");

      const raw = day.fa.latestRaw;
      if (!raw) {
        throw Object.assign(new Error("No transaction file is stored for this date."), {
          code: "FA_FILE_MISSING",
        });
      }

      /*
       * Stratus keeps listing an object for around twenty seconds after it is
       * deleted, so a file the user removed just before pressing Match can still
       * be in `day`. Say so plainly; the raw NoSuchKey reads like a bucket fault.
       */
      let faBuffer;
      try {
        faBuffer = await CM.getBuffer(bucket, raw.key);
      } catch (readErr) {
        throw Object.assign(
          new Error(
            `The transaction file ${raw.originalFileName} is no longer stored. It was probably removed after this run was queued — upload it again and match once more.`,
          ),
          { code: "FA_FILE_MISSING", cause: readErr },
        );
      }
      const parsed = await CM.parseFaFile(faBuffer, {
        businessDate,
        profile: profiles[CM.PROFILE_TYPE.FA],
      });
      if (parsed.fileError) {
        throw Object.assign(new Error(parsed.fileError.message), {
          code: parsed.fileError.code,
        });
      }
      if (parsed.rowErrors.length > 0) {
        // Upload-time validation should have caught this; reaching here means the
        // stored file is not the one that was validated.
        throw Object.assign(
          new Error(
            `${parsed.rowErrors.length} row(s) in the stored transaction file could not be read.`,
          ),
          { code: "FA_VALIDATION_FAILED" },
        );
      }

      await CM.setStageDetail(
        app,
        runId,
        `Resolving ${parsed.rows.length} rows onto actual codes`,
      );

      /*
       * Filter before resolution, not after. Doing it afterwards would leave the
       * unmapped-code list and the resolved counts describing rows this run was
       * never asked about — and unmapped codes from an unselected day would
       * surface as exceptions.
       */
      if (selectedDates) {
        const before = parsed.rows.length;
        parsed.rows = parsed.rows.filter((r) => wanted(r.tranDate));
        log(`date filter kept ${parsed.rows.length} of ${before} FA rows`);
        if (parsed.rows.length === 0) {
          throw Object.assign(
            new Error(
              `The transaction file has no rows on the selected date(s): ${[...selectedDates].sort().join(", ")}.`,
            ),
            { code: "NO_FA_ROWS_FOR_DATES" },
          );
        }
      }

      const { virtualToActual, knownActual } = await CM.buildCodeMaps(
        zcql,
        parsed.rows.map((r) => r.accountCode),
      );

      const resolved = CM.resolveActualCodes(parsed.rows, virtualToActual, {
        knownActualCodes: knownActual,
      });

      unmapped = resolved.unmapped;
      resolvedCount = resolved.resolvedCount;
      actualToVirtual = resolved.actualToVirtual;

      await CM.putText(
        bucket,
        faFinalKey,
        CM.faRowsToConsolidatedCsv(resolved.rows),
        "text/csv",
      );

      // Two things the 26-column consolidated file has no room for: codes that
      // could not be resolved at all, and which virtual codes rolled into each
      // actual code. Both are needed after S1 — see buildFaMetaKey.
      await CM.putJson(bucket, CM.buildFaMetaKey(businessDate), {
        runId,
        businessDate,
        resolvedCount: resolved.resolvedCount,
        passthroughCount: resolved.passthroughCount,
        unmapped: resolved.unmapped,
        actualToVirtual: resolved.actualToVirtual,
      });

      await CM.setObjectKeys(app, runId, { faFinalKey });
      await CM.setCounts(app, runId, {
        faRowCount: parsed.rows.length,
        resolvedCount: resolved.resolvedCount,
        unmappedCount: resolved.unmapped.length,
      });

      log(
        `S1: ${parsed.rows.length} rows, ${resolved.resolvedCount} resolved, ` +
          `${resolved.passthroughCount} already actual, ${resolved.unmapped.length} unmapped`,
      );
    }

    /* ─── S2 — merge the custodian side ───────────────────────────── */
    if (atOrBefore(resumeFrom, "S2")) {
      await CM.setStage(app, runId, CM.STAGE.BUILD_CUSTODY, "Merging the custodian files");

      const custodyForRun = day.custody.files.filter((f) => rangeWanted(f.coveredFrom, f.coveredTo));

      if (day.custody.files.length === 0) {
        throw Object.assign(new Error("No custodian files are stored for this date."), {
          code: "CUSTODY_FILES_MISSING",
        });
      }
      if (custodyForRun.length === 0) {
        // Every custodian file sits outside the selection. Comparing would
        // report the whole FA side as missing, which is a misleading way to say
        // "you picked dates the custodian never sent".
        throw Object.assign(
          new Error(
            `No custodian file covers the selected date(s): ${[...selectedDates].sort().join(", ")}.`,
          ),
          { code: "NO_CUSTODY_FOR_DATES" },
        );
      }

      const files = [];
      for (const f of custodyForRun) {
        await CM.setStageDetail(
          app,
          runId,
          `Reading ${f.originalFileName} (${files.length + 1} of ${custodyForRun.length})`,
        );

        let buffer;
        try {
          buffer = await CM.getBuffer(bucket, f.key);
        } catch (readErr) {
          /*
           * Same twenty-second listing lag as the FA side. A removed custodian
           * file is left out rather than failing the run: leaving it out is what
           * removing it meant, and its rows then report as missing in custody.
           * If every file has gone the merge below finds no rows and stops.
           */
          log(`skipped ${f.originalFileName} — no longer stored (${readErr.message})`);
          continue;
        }

        files.push({
          buffer,
          scheme: f.scheme,
          fileType: f.fileType,
          fileName: f.originalFileName,
        });
      }

      if (files.length === 0) {
        throw Object.assign(
          new Error(
            "None of the custodian files for this run could be read; they were all removed after it was queued.",
          ),
          { code: "CUSTODY_FILES_MISSING" },
        );
      }

      const merged = await CM.mergeCustodyFiles(files, { businessDate, profiles });
      if (merged.fileErrors.length > 0) {
        throw Object.assign(new Error(merged.fileErrors[0].message), {
          code: merged.fileErrors[0].code || "CUSTODY_PARSE_FAILED",
        });
      }
      if (merged.rowErrors.length > 0) {
        throw Object.assign(
          new Error(
            `${merged.rowErrors.length} row(s) across the custodian files could not be read.`,
          ),
          { code: "CUSTODY_VALIDATION_FAILED" },
        );
      }

      /*
       * A custodian file can span days, so the file-level filter above only
       * decides which files to open. The selection is applied here, row by row,
       * exactly as it is on the FA side.
       */
      if (selectedDates) {
        merged.rows = merged.rows.filter((r) => wanted(r.tranDate));
        if (merged.rows.length === 0) {
          throw Object.assign(
            new Error(
              `The custodian files have no rows on the selected date(s): ${[...selectedDates].sort().join(", ")}.`,
            ),
            { code: "NO_CUSTODY_FOR_DATES" },
          );
        }
      }

      await CM.putText(
        bucket,
        custodyFinalKey,
        CM.custodyRowsToCsv(merged.rows),
        "text/csv",
      );

      await CM.setObjectKeys(app, runId, { custodyFinalKey });
      await CM.setCounts(app, runId, { custodyRowCount: merged.rows.length });

      log(`S2: ${merged.rows.length} rows merged from ${files.length} file(s)`);
    }

    /* ─── S3 — verify both finals read back ───────────────────────── */
    if (atOrBefore(resumeFrom, "S3")) {
      await CM.setStage(app, runId, CM.STAGE.PERSIST, "Verifying the consolidated files");

      // Cheap, and it catches a truncated or failed write before time is spent
      // comparing — a half-written final would otherwise read as a pile of
      // missing trades.
      for (const key of [faFinalKey, custodyFinalKey]) {
        const buf = await CM.getBuffer(bucket, key);
        if (!buf || buf.length === 0) {
          throw Object.assign(new Error(`Consolidated file ${key} is empty.`), {
            code: "FINAL_FILE_EMPTY",
          });
        }
      }
    }

    /* ─── S4 — compare ────────────────────────────────────────────── */
    await CM.setStage(app, runId, CM.STAGE.COMPARE, "Comparing FA against custody");

    const [faFinalBuf, custodyFinalBuf] = await Promise.all([
      CM.getBuffer(bucket, faFinalKey),
      CM.getBuffer(bucket, custodyFinalKey),
    ]);

    /*
     * No profile here, deliberately. faRowsToConsolidatedCsv always emits the
     * 26-column template — that is the whole point of the rewrite — so the
     * consolidated file is read with the built-in mapping even when the file the
     * user uploaded was in some other layout entirely.
     */
    const faFinal = await CM.parseFaFile(faFinalBuf, { businessDate });
    const custodyRows = await CM.parseCustodyFinalCsv(custodyFinalBuf);

    // The consolidated file is already keyed on actual codes.
    const faRows = faFinal.rows.map((r) => ({ ...r, actualCode: r.accountCode }));

    // Recover what S1 worked out when this attempt did not run S1 itself.
    if (Object.keys(actualToVirtual).length === 0) {
      try {
        const saved = await CM.getJson(bucket, CM.buildFaMetaKey(businessDate));
        unmapped = saved.unmapped || unmapped;
        resolvedCount = saved.resolvedCount || resolvedCount;
        actualToVirtual = saved.actualToVirtual || {};
      } catch {
        // Written by every S1; absent only for a date that predates this file.
        log("S4: no fa-meta file found; virtual codes will not be shown");
      }
    }

    // The consolidated file is keyed on actual codes, so the virtual codes have
    // to be put back before aggregation — they are what lets a user find the
    // offending row in their own file.
    for (const row of faRows) {
      const virtuals = actualToVirtual[row.actualCode];
      if (virtuals && virtuals.length) row.virtualCodes = virtuals;
    }

    const faMap = CM.aggregateByKey(faRows);
    const custodyMap = CM.aggregateByKey(custodyRows);
    const keys = CM.buildKeyUnion(faMap, custodyMap);

    /*
     * The key walk always starts at zero.
     *
     * A partial walk's exceptions live only in memory, so resuming at an offset
     * would silently drop everything the crashed attempt had already found —
     * worse than redoing the work. And the work is cheap: 15,000 rows a side
     * aggregate and compare in well under a second, which is why the expensive
     * stages to avoid repeating are S1 (parse + clientIds lookup) and S5 (row
     * writes), and those are exactly what the persisted finals already protect.
     *
     * comparedKeyCount is therefore live progress for the screen, not a resume
     * point. Restarting the walk keeps S4 exactly-once by construction.
     */
    let exceptions = [];
    let matchedKeyCount = 0;
    let compared = 0;

    while (compared < keys.length) {
      if (remainingMs(context) < TIME_BUFFER_MS) {
        throw Object.assign(
          new Error(
            `Ran out of time comparing (${compared} of ${keys.length} keys). This day is larger than the job budget allows.`,
          ),
          { code: "COMPARE_TIMEOUT" },
        );
      }

      const end = Math.min(compared + COMPARE_CHUNK, keys.length);
      const slice = CM.compareKeyRange(faMap, custodyMap, keys, compared, end, {});
      exceptions.push(...slice.exceptions);
      matchedKeyCount += slice.matchedCount;
      compared = end;

      await CM.checkpointCompare(app, runId, compared, keys.length);
    }

    if (CM.DEFAULT_OPTIONS.repairOrphans) {
      exceptions = CM.repairOrphans(exceptions);
    }
    if (CM.DEFAULT_OPTIONS.unmappedAsException && unmapped.length > 0) {
      exceptions.push(...CM.unmappedToExceptions(unmapped));
    }

    exceptions.sort(
      (a, b) =>
        a.sortKey.localeCompare(b.sortKey) ||
        a.exceptionType.localeCompare(b.exceptionType),
    );

    log(
      `S4: ${keys.length} keys, ${matchedKeyCount} matched, ${exceptions.length} exception(s)`,
    );

    /* ─── S5 — report ─────────────────────────────────────────────── */
    await CM.setStage(app, runId, CM.STAGE.REPORT, "Writing the difference report");

    // Clear first. Catalyst auto-retries, and without this one transient timeout
    // here doubles the exception count with duplicates.
    await CM.clearExceptions(app, runId);

    if (exceptions.length > 0) {
      await CM.insertExceptions(app, runId, businessDate, exceptions, {
        onProgress: (done, total) => {
          if (done % 1000 === 0) log(`S5: ${done}/${total} exceptions written`);
        },
      });
    }

    const differenceKey = CM.buildDifferenceKey(businessDate, runId);
    await CM.putText(bucket, differenceKey, CM.exceptionsToCsv(exceptions), "text/csv");
    await CM.setObjectKeys(app, runId, { differenceKey });

    /* ─── S6 — finalise ───────────────────────────────────────────── */
    await CM.finishRun(app, runId, {
      exceptionCount: exceptions.length,
      totalKeyCount: keys.length,
      comparedKeyCount: keys.length,
      faRowCount: faRows.length,
      custodyRowCount: custodyRows.length,
      resolvedCount,
      unmappedCount: unmapped.length,
    });

    log(
      `done in ${Date.now() - startedAt}ms — ${
        exceptions.length === 0 ? "CLEAN" : `${exceptions.length} exception(s)`
      }`,
    );
    context.closeWithSuccess();
  } catch (error) {
    const code = error?.code || "UNEXPECTED_ERROR";
    const message = error?.message || String(error);
    console.error(`CustodyMatchRunner failed (${code}):`, message);

    try {
      await CM.failRun(app, runId, { errorCode: code, errorMessage: message });
    } catch (writeErr) {
      console.error(
        "CustodyMatchRunner: could not record the failure on the run row:",
        writeErr?.message || writeErr,
      );
    }

    context.closeWithFailure();
  }
};
