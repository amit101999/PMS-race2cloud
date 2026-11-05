"use strict";

/**
 * CustodyMatchRunner — the custody matching pipeline (stages S1-S6).
 *
 * Params (from POST /api/custody-match/run):
 *   runId         Custody_Match_Run.runId
 *   businessDate  yyyy-mm-dd, the file date
 *   uploadDate    yyyy-mm-dd, the upload date whose files are compared
 *
 * Every stage writes its checkpoint to Custody_Match_Run BEFORE the stage's work
 * starts, so a crash leaves `stage` pointing at the step that needs re-running
 * rather than the one that finished. Catalyst auto-retries job functions, so
 * each stage is written to be safely repeatable:
 *
 *   S1 BUILD_FA        re-reads the raw file; writes final-<date>-<runId>-FA.csv
 *   S2 BUILD_CUSTODY   re-reads the custodian files; writes final-<date>-<runId>-Custodian.csv
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

    const resumeFrom = STAGE_ORDER.includes(run.stage) ? run.stage : "S0";
    if (resumeFrom !== "S0" && resumeFrom !== "S1") {
      log(`resuming from ${resumeFrom} (attempt ${(run.attemptCount || 0) + 1})`);
    }

    /*
     * The files to compare: the file date's rows in Custody_Match_File under
     * this run's upload date. Read from the table rather than the Stratus
     * listing, which keeps showing a deleted object for a while. The run row is
     * the fallback for the upload date, so a job submitted before the param
     * existed still resolves it.
     */
    const uploadDate = String(params.uploadDate || run.uploadDate || "").trim();
    if (!uploadDate) {
      throw Object.assign(
        new Error("This run has no upload date, so its files cannot be found. Start a new match."),
        { code: "NO_UPLOAD_DATE" },
      );
    }
    /*
     * Equity tab (EquityReload.js): the FA side is the date's final list, already
     * built into one 26-column file (books + pending files, duplicates merged),
     * and the custodian side is every custodian file of the date, whatever upload
     * date it came under. The consolidated file is always the built-in template
     * plus the Transaction ID column, so it is read with FA_CONSOLIDATED_PROFILE,
     * not the saved mapping.
     */
    const consolidatedKey = String(params.consolidatedKey || "").trim();
    const activeFiles = await CM.listFiles(
      app,
      consolidatedKey ? { forDate: businessDate } : { forDate: businessDate, uploadedOn: uploadDate },
    );
    const faFiles = consolidatedKey
      ? [{ key: consolidatedKey, fileType: CM.FA_FILE_TYPE.REGULAR, originalFileName: "the date's final list", template: true }]
      : activeFiles
          .filter((f) => f.side === CM.FILE_SIDE.FA)
          .map((f) => ({ key: f.fileKey, fileType: f.fileType, originalFileName: f.originalFileName }));
    const custodyFiles = activeFiles
      .filter((f) => f.side === CM.FILE_SIDE.CUSTODY)
      .map((f) => ({
        key: f.fileKey,
        scheme: f.scheme,
        fileType: f.fileType,
        originalFileName: f.originalFileName,
      }));

    // Per run: the same file date can be matched once per upload date.
    const faFinalKey = CM.buildFaFinalKey(businessDate, runId);
    const custodyFinalKey = CM.buildCustodyFinalKey(businessDate, runId);

    let unmapped = [];
    let resolvedCount = 0;
    let actualToVirtual = {};

    /* ─── S1 — build the FA side ──────────────────────────────────── */
    if (atOrBefore(resumeFrom, "S1")) {
      await CM.setStage(app, runId, CM.STAGE.BUILD_FA, "Reading the transaction file");

      /*
       * Every transaction file stored for the date, newest first. A day may hold
       * the regular file and the mutual fund file; each is read through its OWN
       * mapping, because the two layouts differ and one mapping cannot describe
       * both. Their rows are then one set — from here on nothing cares which
       * file a row arrived in.
       */
      if (faFiles.length === 0) {
        throw Object.assign(new Error("No transaction file is stored for this date."), {
          code: "FA_FILE_MISSING",
        });
      }

      const parsed = { rows: [], warnings: [] };

      for (const raw of faFiles) {
        const profileType =
          raw.fileType === CM.FA_FILE_TYPE.MUTUAL_FUND
            ? CM.PROFILE_TYPE.FA_MUTUAL_FUND
            : CM.PROFILE_TYPE.FA;

        // The file table and the bucket can disagree if a file was removed after
        // this run was queued. Say so plainly; NoSuchKey reads like a bucket fault.
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

        const one = await CM.parseFaFile(faBuffer, {
          businessDate,
          profile: raw.template ? CM.FA_CONSOLIDATED_PROFILE : profiles[profileType],
          type: profileType,
        });
        if (one.fileError) {
          throw Object.assign(new Error(one.fileError.message), {
            code: one.fileError.code,
          });
        }
        if (one.rowErrors.length > 0) {
          // Upload-time validation should have caught this; reaching here means the
          // stored file is not the one that was validated.
          throw Object.assign(
            new Error(
              `${one.rowErrors.length} row(s) in the stored transaction file ${raw.originalFileName} could not be read.`,
            ),
            { code: "FA_VALIDATION_FAILED" },
          );
        }

        parsed.rows.push(...one.rows);
        parsed.warnings.push(...one.warnings);
      }

      await CM.setStageDetail(
        app,
        runId,
        `Resolving ${parsed.rows.length} rows onto actual codes`,
      );

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
      await CM.putJson(bucket, CM.buildFaMetaKey(businessDate, runId), {
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

      if (custodyFiles.length === 0) {
        throw Object.assign(new Error("No custodian files are stored for this date."), {
          code: "CUSTODY_FILES_MISSING",
        });
      }

      const files = [];
      for (const f of custodyFiles) {
        await CM.setStageDetail(
          app,
          runId,
          `Reading ${f.originalFileName} (${files.length + 1} of ${custodyFiles.length})`,
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
        const saved = await CM.getJson(bucket, CM.buildFaMetaKey(businessDate, runId));
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

    /*
     * Two comparisons: mutual fund FA rows against the Mutual Funds custodian
     * files, everything else against the Residential and NRE/NRO files.
     */
    const groups = CM.splitMutualFunds(faRows, custodyRows).map((group) => {
      const faMap = CM.aggregateByKey(group.faRows);
      const custodyMap = CM.aggregateByKey(group.custodyRows);
      return { ...group, faMap, custodyMap, keys: CM.buildKeyUnion(faMap, custodyMap) };
    });
    const totalKeys = groups.reduce((sum, g) => sum + g.keys.length, 0);

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
    let comparedTotal = 0;

    for (const group of groups) {
      let groupExceptions = [];
      let compared = 0;

      while (compared < group.keys.length) {
        if (remainingMs(context) < TIME_BUFFER_MS) {
          throw Object.assign(
            new Error(
              `Ran out of time comparing (${comparedTotal} of ${totalKeys} keys). This day is larger than the job budget allows.`,
            ),
            { code: "COMPARE_TIMEOUT" },
          );
        }

        const end = Math.min(compared + COMPARE_CHUNK, group.keys.length);
        const slice = CM.compareKeyRange(group.faMap, group.custodyMap, group.keys, compared, end, {});
        groupExceptions.push(...slice.exceptions);
        matchedKeyCount += slice.matchedCount;
        comparedTotal += end - compared;
        compared = end;

        await CM.checkpointCompare(app, runId, comparedTotal, totalKeys);
      }

      // Per group, so a mutual fund orphan is never paired with an equity one.
      if (CM.DEFAULT_OPTIONS.repairOrphans) {
        groupExceptions = CM.repairOrphans(groupExceptions);
      }
      exceptions.push(...groupExceptions);

      log(
        `S4 ${group.name}: ${group.faRows.length} FA rows, ${group.custodyRows.length} custodian rows, ${group.keys.length} keys`,
      );
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
      `S4: ${totalKeys} keys, ${matchedKeyCount} matched, ${exceptions.length} exception(s)`,
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
      totalKeyCount: totalKeys,
      comparedKeyCount: totalKeys,
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

    // A file or mapping changed mid-run (runStore refuses the status write).
    // The run stays SUPERSEDED; succeeding stops Catalyst retrying it.
    if (code === "RUN_SUPERSEDED") {
      log("superseded while working — stopping; the date must be matched again");
      context.closeWithSuccess();
      return;
    }

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
