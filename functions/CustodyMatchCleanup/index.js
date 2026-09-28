"use strict";

/**
 * CustodyMatchCleanup — nightly sweep of the custody-matching working set.
 *
 * The bucket folders and the two tables hold WORKING data: raw uploads, the two
 * consolidated finals, the difference file, and one run row per attempt with its
 * exceptions. None of it is production data — that lives in the Transaction
 * table and in client-transaction-files, and this function never touches either.
 * Without a sweep, "temporary-files" is temporary in name only.
 *
 * ---------------------------------------------------------------------------
 * THIS FUNCTION DELETES THINGS ON A SCHEDULE, UNATTENDED.
 *
 * So it is written to fail safe rather than to be clever:
 *
 *   - a HARD FLOOR of MIN_RETENTION_DAYS that configuration cannot go below, so
 *     a mistyped environment variable cannot wipe this week's work
 *   - dates are compared as yyyy-mm-dd STRINGS, never as parsed Dates, because
 *     Catalyst stores timestamps in the project timezone with no UTC offset and
 *     a timezone slip here deletes a day early
 *   - a date with a QUEUED or RUNNING run is skipped whatever its age: something
 *     is still reading those files
 *   - dry run is the DEFAULT: unset, it reports what it would remove and removes
 *     nothing. Deleting requires CLEANUP_DRY_RUN=false, spelled out
 * ---------------------------------------------------------------------------
 *
 * Environment variables (all optional):
 *   CUSTODY_RETENTION_DAYS  default 30, floored at MIN_RETENTION_DAYS
 *   CLEANUP_DRY_RUN         defaults to dry run. Set to exactly "false" to delete.
 */

const catalyst = require("zcatalyst-sdk-node");

const DEFAULT_RETENTION_DAYS = 30;

/** Configuration cannot ask for a shorter window than this. */
const MIN_RETENTION_DAYS = 7;

/** Stop before the 15-minute wall so a large backlog resumes tomorrow rather than dying mid-delete. */
const TIME_BUFFER_MS = 60_000;

/** yyyy-mm-dd for "today minus n days", built from UTC parts so no local timezone enters. */
function isoDaysAgo(days) {
  const ms = Date.now() - days * 24 * 60 * 60 * 1000;
  return new Date(ms).toISOString().slice(0, 10);
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

module.exports = async (cronDetails, context) => {
  const startedAt = Date.now();

  // Admin scope, for the same reason the job function needs it: a background
  // function has no user token, and without admin every DataStore call stalls
  // ~60s instead of erroring.
  const app = catalyst.initialize(context, { scope: "admin" });

  // The shared module is ESM; this function is CommonJS. ./custodyMatch carries
  // its own package.json with { "type": "module" } so the copy loads unchanged.
  const CM = await import("./custodyMatch/index.js");

  /*
   * Dry run is the DEFAULT, and deleting has to be asked for by name.
   *
   * An unattended deleter that is destructive until configured otherwise gets
   * exactly one chance to be wrong. This way a cron scheduled before anybody has
   * thought about retention reports what it would remove and removes nothing;
   * turning it on is a deliberate act with the reason written down.
   */
  const dryRun = String(process.env.CLEANUP_DRY_RUN || "true").toLowerCase() !== "false";

  const requested = Number(process.env.CUSTODY_RETENTION_DAYS);
  const retentionDays = Number.isFinite(requested)
    ? Math.max(Math.trunc(requested), MIN_RETENTION_DAYS)
    : DEFAULT_RETENTION_DAYS;

  if (Number.isFinite(requested) && requested < MIN_RETENTION_DAYS) {
    console.warn(
      `[CustodyMatchCleanup] CUSTODY_RETENTION_DAYS=${requested} is below the ` +
        `${MIN_RETENTION_DAYS}-day floor; using ${retentionDays}.`,
    );
  }

  if (!dryRun) {
    console.warn(
      "[CustodyMatchCleanup] CLEANUP_DRY_RUN=false — this run WILL DELETE files, " +
        `runs and exceptions older than ${retentionDays} days.`,
    );
  }

  // Everything strictly BEFORE this date is eligible. String comparison on
  // yyyy-mm-dd is exact and timezone-free.
  const cutoff = isoDaysAgo(retentionDays);

  /*
   * A folder's date is the day its files are FOR, not the day they were
   * uploaded: a file for last year can be uploaded this morning. So an old date
   * is only deleted once nothing in it has been uploaded or run within the
   * retention window. Upload times come from the object keys; run start times
   * from the run rows.
   */
  const cutoffMs = Date.now() - retentionDays * 24 * 60 * 60 * 1000;

  const report = {
    cutoff,
    retentionDays,
    dryRun,
    datesExamined: 0,
    datesDeleted: [],
    datesSkipped: [],
    objectsDeleted: 0,
    objectsFailed: [],
    runsDeleted: 0,
    exceptionsDeleted: 0,
    stoppedEarly: false,
  };

  try {
    const dates = await CM.listStoredDates(app);
    const expired = dates.filter((d) => d < cutoff);
    report.datesExamined = dates.length;

    console.log(
      `[CustodyMatchCleanup] ${dates.length} date(s) stored; ${expired.length} older than ` +
        `${cutoff} (${retentionDays}-day retention)${dryRun ? " — DRY RUN" : ""}.`,
    );

    const bucket = CM.getBucket(app);

    for (const date of expired) {
      if (remainingMs(context) < TIME_BUFFER_MS) {
        report.stoppedEarly = true;
        console.warn(
          `[CustodyMatchCleanup] Out of time with ${expired.length - report.datesDeleted.length} ` +
            "date(s) left; the rest will be swept on the next run.",
        );
        break;
      }

      // Something may still be reading this date's files. Age is not enough on
      // its own — a stuck run would otherwise have its inputs deleted underneath it.
      const runs = await CM.listRunsForDate(app, date, 300);
      const active = runs.filter(
        (r) => r.status === CM.RUN_STATUS.QUEUED || r.status === CM.RUN_STATUS.RUNNING,
      );
      if (active.length > 0) {
        report.datesSkipped.push({
          date,
          reason: `${active.length} run(s) still ${active[0].status}`,
        });
        console.warn(`[CustodyMatchCleanup] Skipping ${date}: ${active.length} run(s) still active.`);
        continue;
      }

      const day = await CM.listDay(app, date, { bucket });
      const lastActivityMs = Math.max(
        0,
        ...day.fa.raw.map((f) => f.uploadedAtMs || 0),
        ...day.custody.files.map((f) => f.uploadedAtMs || 0),
        ...runs.map((r) => r.startedAtMs || 0),
      );
      if (lastActivityMs >= cutoffMs) {
        report.datesSkipped.push({
          date,
          reason: `last upload or run was ${new Date(lastActivityMs).toISOString()}, inside the retention window`,
        });
        continue;
      }

      if (dryRun) {
        const objects = [
          ...(await CM.listAllObjects(bucket, CM.faDayPrefix(date))),
          ...(await CM.listAllObjects(bucket, CM.custodyDayPrefix(date))),
        ];
        report.datesDeleted.push({ date, objects: objects.length, runs: runs.length });
        report.objectsDeleted += objects.length;
        report.runsDeleted += runs.length;
        continue;
      }

      const removed = await CM.deleteDay(app, date, { bucket });
      // The file rows go with the files: an ACTIVE row whose object is gone
      // would offer the date in the Match dialog and then fail the run.
      await CM.deleteFileRowsForDate(app, date);
      const objectCount = removed.fa.count + removed.custody.count;
      report.objectsDeleted += objectCount;
      report.objectsFailed.push(...removed.fa.failed, ...removed.custody.failed);
      report.datesDeleted.push({ date, objects: objectCount, runs: runs.length });
    }

    // Run rows and their exceptions, swept by the same cutoff. Done after the
    // objects so a crash in between leaves rows pointing at missing files — which
    // is recoverable and visible — rather than files with no record of what they
    // were, which is neither.
    if (!dryRun && !report.stoppedEarly) {
      // Dates whose files were deliberately kept must keep their run rows too:
      // the run row is WHY the files were kept, and a still-active run would
      // fail on its next status write if its row vanished underneath it.
      const rows = await CM.deleteRunsBefore(app, cutoff, {
        skipDates: report.datesSkipped.map((d) => d.date),
        startedBeforeMs: cutoffMs,
      });
      report.runsDeleted = rows.runs;
      report.exceptionsDeleted = rows.exceptions;
      report.runsPreserved = rows.skipped;
    }

    console.log(
      `[CustodyMatchCleanup] Done in ${Date.now() - startedAt}ms — ` +
        `${report.datesDeleted.length} date(s), ${report.objectsDeleted} object(s), ` +
        `${report.runsDeleted} run(s), ${report.exceptionsDeleted} exception(s)` +
        `${report.objectsFailed.length ? `, ${report.objectsFailed.length} delete(s) failed` : ""}` +
        `${dryRun ? " [DRY RUN — nothing was deleted]" : ""}.`,
    );

    if (report.objectsFailed.length > 0) {
      console.error(
        "[CustodyMatchCleanup] Failed deletes:",
        JSON.stringify(report.objectsFailed.slice(0, 20)),
      );
    }

    context.closeWithSuccess();
  } catch (error) {
    console.error(
      "[CustodyMatchCleanup] failed:",
      error?.message || error,
      JSON.stringify(report),
    );
    context.closeWithFailure();
  }
};
