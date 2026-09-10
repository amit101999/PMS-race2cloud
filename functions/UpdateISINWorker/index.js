"use strict";

/**
 * UpdateISINWorker (Catalyst Job Function) — timeout-proof ISIN renamer.
 *
 * Renames an ISIN (old → new) across EVERY table that stores it, in small
 * batches, so no single invocation exceeds the 15-minute function timeout.
 * This IS the whole flow — a plain rename with no follow-up recompute step.
 * (An earlier version also rebuilt Holdings from source data afterward; that
 * was deliberately removed — see git history if it's ever needed again.)
 *
 * How it survives large tables (Transaction ~10–12 lakh rows):
 *   - Self-draining queue: each batch updates rows OUT of the `WHERE col = old`
 *     set, so the remaining work shrinks on its own — no OFFSET, no row counting.
 *   - Baton pass: when the time budget runs low mid-table, the worker re-submits
 *     itself (same params + the current target index) and exits cleanly. A fresh
 *     invocation resumes exactly where this one stopped.
 *
 * Merger / Demerger tables store TWO ISINs per row (an old side and a new side).
 * The renamed ISIN can sit on either side in different rows, so those tables list
 * BOTH columns and each column is drained independently.
 *
 * Required job params (queued by UpdateISIN master):
 *   - old_isin     : ISIN being replaced
 *   - new_isin     : replacement ISIN (caller guarantees it is fresh / unused)
 *   - status_key   : Jobs.jobName row to mark SUCCESS when fully drained
 * Optional:
 *   - target_index : resume point (set by the baton pass; defaults to 0)
 */

const catalyst = require("zcatalyst-sdk-node");

/* ============================== CONFIG ============================== */

/** Job pool the worker re-submits itself into (same pool the masters use). */
const WORKER_JOBPOOL = "UpdateMasters";

/** This function's own target name (used for the baton-pass re-submit). */
const WORKER_TARGET = "UpdateISINWorker";

/** Rows updated per batch. Sized small so each UPDATE returns quickly. */
const BATCH = 300;

/** Stop and hand off before this elapsed time (< the 15-min function cap). */
const TIME_BUDGET_MS = 12 * 60 * 1000;

/**
 * Per-target retry policy. A "target" = one table in ISIN_TARGETS (all of its
 * columns). A failing SELECT/UPDATE batch gets this many quick attempts
 * (a short pause between each) before the whole target is given up on for
 * this run — deliberately no further automatic retry beyond this, and no
 * manual retry endpoint; recovering means submitting the rename again from
 * the UI, which is cheap because of the self-draining query (see below).
 */
const TARGET_RETRY_ATTEMPTS = 3;
const TARGET_RETRY_DELAY_MS = 3000;

/**
 * Every table that stores an ISIN, with the exact column(s) to rename.
 * - Plain tables: one column, "ISIN".
 * - Merger / Merger_Record: "ISIN" (new side) + "OldISIN" (old side).
 * - Demerger: "Old_ISIN" + "New_ISIN" (this table has NO plain "ISIN" column).
 * - Demerger_Record: single "ISIN" column.
 *
 * IMPORTANT: appsail-nodejs/controller/isin/updateIsin.js keeps its own
 * RENAME_TARGET_COUNT constant equal to this array's length, so it can build
 * the exact set of "<jobName>_t0".."_t(N-1)" progress-row names to fetch (see
 * progressRowName above — ZCQL's LIKE doesn't support % wildcards). If this
 * array's length ever changes, update that constant too.
 */
const ISIN_TARGETS = [
  { table: "Security_List", cols: ["ISIN"] },
  { table: "Transaction", cols: ["ISIN"] },
  { table: "Bonus", cols: ["ISIN"] },
  { table: "Bonus_Record", cols: ["ISIN"] },
  { table: "Split", cols: ["ISIN"] },
  { table: "Dividend", cols: ["ISIN"] },
  { table: "Dividend_Record", cols: ["ISIN"] },
  { table: "Temp_Transaction", cols: ["ISIN"] },
  { table: "Temp_Custodian", cols: ["ISIN"] },
  { table: "Bhav_Copy", cols: ["ISIN"] },
  { table: "Cash_Balance_Per_Transaction", cols: ["ISIN"] },
  { table: "Holdings", cols: ["ISIN"] },
  { table: "Demerger_Record", cols: ["ISIN"] },
  { table: "Merger", cols: ["ISIN", "OldISIN"] },
  { table: "Merger_Record", cols: ["ISIN", "OldISIN"] },
  { table: "Demerger", cols: ["Old_ISIN", "New_ISIN"] },
];

/* ============================== HELPERS ============================== */

const esc = (v) => String(v ?? "").replace(/'/g, "''");

function getAllParams(jobRequest) {
  try {
    if (jobRequest && typeof jobRequest.getAllJobParams === "function") {
      return jobRequest.getAllJobParams() || {};
    }
  } catch (e) {
    console.warn("[UpdateISINWorker] getAllParams:", e.message);
  }
  return {};
}

/** Read up to BATCH ROWIDs that still carry the old ISIN in this column. */
async function fetchOldRowIds(zcql, table, col, oldSql) {
  const rows = await zcql.executeZCQLQuery(
    `SELECT ROWID FROM ${table} WHERE ${col} = '${oldSql}' LIMIT ${BATCH}`,
  );
  const out = [];
  for (const r of rows || []) {
    const v = (r[table] || r).ROWID;
    if (v != null) out.push(v);
  }
  return out;
}

async function finalizeJobsRow(zcql, jobName, status) {
  if (!jobName) return;
  try {
    await zcql.executeZCQLQuery(
      `UPDATE Jobs SET status = '${esc(status)}' WHERE jobName = '${esc(jobName)}'`,
    );
  } catch (e) {
    console.error(`[UpdateISINWorker] finalize Jobs '${jobName}' failed:`, e.message);
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Progress-tracking row name for one ISIN_TARGETS entry (reuses the Jobs
 * table). Index-only — NOT "<key>_t<index>_<table>" — because the reader
 * (appsail-nodejs) needs to fetch all of them with an exact-match `IN (...)`
 * query. ZCQL's `LIKE` does not support `%` wildcards (matches nothing,
 * silently), so a prefix-style lookup can't be used here; the reader instead
 * knows the fixed target count (RENAME_TARGET_COUNT, kept in sync with
 * ISIN_TARGETS.length below) and builds the exact list of row names itself.
 * The table name is carried in the status text instead (see markTargetFailed).
 */
function progressRowName(statusKey, index) {
  return `${statusKey}_t${index}`;
}

/**
 * Open a PENDING progress row for every target, once, at the start of a
 * fresh rename run (startIndex === 0). A resumed run (after a timeout baton
 * pass) skips this — the rows from the original run are still there.
 * Lets the frontend show "13/16 done, 1 failed (Bhav_Copy), 2 pending"
 * instead of a plain spinner.
 */
async function initializeProgressRows(zcql, statusKey) {
  if (!statusKey) return;
  for (let i = 0; i < ISIN_TARGETS.length; i++) {
    const rowName = progressRowName(statusKey, i);
    try {
      await zcql.executeZCQLQuery(
        `INSERT INTO Jobs (jobName, status) VALUES ('${esc(rowName)}', 'PENDING')`,
      );
    } catch (e) {
      // Already exists, or a transient error — non-fatal. The per-target
      // finalize call at the end of that target still records the truth.
    }
  }
}

async function markTargetDone(zcql, rowName) {
  if (!rowName) return;
  try {
    await zcql.executeZCQLQuery(
      `UPDATE Jobs SET status = 'DONE' WHERE jobName = '${esc(rowName)}'`,
    );
  } catch (e) {
    console.warn(`[UpdateISINWorker] mark DONE '${rowName}' failed:`, e.message);
  }
}

/** status = "FAILED:<table>:<error>" — table travels in the status text since
 * the row name is now index-only (see progressRowName). */
async function markTargetFailed(zcql, rowName, table, errorMessage) {
  if (!rowName) return;
  const detail = esc(`${table}:${String(errorMessage || "").slice(0, 160)}`);
  try {
    await zcql.executeZCQLQuery(
      `UPDATE Jobs SET status = 'FAILED:${detail}' WHERE jobName = '${esc(rowName)}'`,
    );
  } catch (e) {
    console.warn(`[UpdateISINWorker] mark FAILED '${rowName}' failed:`, e.message);
  }
}

/** Re-queue this worker to resume at `targetIndex` (the baton pass). */
async function submitContinuation(app, params, targetIndex, startedAt) {
  const jobName = `IW_${targetIndex}_${startedAt}`.slice(0, 20);
  await app.jobScheduling().JOB.submitJob({
    job_name: jobName,
    jobpool_name: WORKER_JOBPOOL,
    target_name: WORKER_TARGET,
    target_type: "Function",
    job_config: { number_of_retries: 5, retry_interval: 60 * 1000 },
    params: {
      old_isin: params.old_isin,
      new_isin: params.new_isin,
      status_key: params.status_key,
      target_index: String(targetIndex),
    },
  });
}

/* ============================== ENTRY ============================== */

module.exports = async (jobRequest, context) => {
  const app = catalyst.initialize(context);
  const zcql = app.zcql();
  const startedAt = Date.now();

  try {
    const p = getAllParams(jobRequest);
    const oldIsin = String(p.old_isin ?? "").trim();
    const newIsin = String(p.new_isin ?? "").trim();
    const statusKey = String(p.status_key ?? "").trim();
    const startIndex = Math.max(0, Number(p.target_index ?? 0) || 0);

    if (!oldIsin || !newIsin) {
      console.error("[UpdateISINWorker] old_isin and new_isin are required.");
      context.closeWithFailure();
      return;
    }
    if (oldIsin === newIsin) {
      console.error("[UpdateISINWorker] old and new ISIN must differ.");
      context.closeWithFailure();
      return;
    }

    const oldSql = esc(oldIsin);
    const newSql = esc(newIsin);

    console.log(
      `[UpdateISINWorker] "${oldIsin}" → "${newIsin}" | resume at target ${startIndex}`,
    );

    let totalUpdated = 0;
    const failedTargets = [];

    // First invocation of a fresh run (not a timeout-resumed one) — lay down
    // the full 16-row checklist up front so the frontend can show progress
    // even before any target has finished.
    if (startIndex === 0) {
      await initializeProgressRows(zcql, statusKey);
    }

    for (let i = startIndex; i < ISIN_TARGETS.length; i++) {
      const { table, cols } = ISIN_TARGETS[i];
      const rowName = progressRowName(statusKey, i);
      let targetError = "";

      columnsLoop: for (const col of cols) {
        let attempts = 0;

        // Drain this column batch-by-batch until no old-ISIN rows remain.
        while (true) {
          // Hand off before the timeout; resume this same target next time.
          if (Date.now() - startedAt > TIME_BUDGET_MS) {
            console.log(
              `[UpdateISINWorker] time budget hit at ${table}.${col} ` +
                `(updated ${totalUpdated} so far) — passing baton at target ${i}.`,
            );
            await submitContinuation(app, p, i, startedAt);
            context.closeWithSuccess();
            return;
          }

          let ids;
          try {
            ids = await fetchOldRowIds(zcql, table, col, oldSql);
          } catch (e) {
            attempts++;
            console.warn(
              `[UpdateISINWorker] select ${table}.${col} failed ` +
                `(attempt ${attempts}/${TARGET_RETRY_ATTEMPTS}):`,
              e.message,
            );
            if (attempts < TARGET_RETRY_ATTEMPTS) {
              await sleep(TARGET_RETRY_DELAY_MS);
              continue;
            }
            targetError = `${col}: ${e.message}`;
            break columnsLoop;
          }

          if (ids.length === 0) break; // this column is fully renamed

          try {
            await zcql.executeZCQLQuery(
              `UPDATE ${table} SET ${col} = '${newSql}' WHERE ROWID IN (${ids.join(",")})`,
            );
            totalUpdated += ids.length;
            attempts = 0; // this batch succeeded — reset before the next one
          } catch (e) {
            attempts++;
            console.warn(
              `[UpdateISINWorker] update ${table}.${col} failed ` +
                `(attempt ${attempts}/${TARGET_RETRY_ATTEMPTS}):`,
              e.message,
            );
            if (attempts < TARGET_RETRY_ATTEMPTS) {
              await sleep(TARGET_RETRY_DELAY_MS);
              continue;
            }
            targetError = `${col}: ${e.message}`;
            break columnsLoop;
          }
        }

        console.log(`[UpdateISINWorker] drained ${table}.${col}`);
      }

      if (targetError) {
        console.error(`[UpdateISINWorker] target FAILED: ${table} — ${targetError}`);
        await markTargetFailed(zcql, rowName, table, targetError);
        failedTargets.push({ index: i, table, error: targetError });
        continue; // targets are independent — keep going to the next table
      }

      await markTargetDone(zcql, rowName);
    }

    if (failedTargets.length > 0) {
      // Some table(s) couldn't be renamed even after retries. Report exactly
      // which ones, distinct from a full FAILURE. No further automatic or
      // manual retry here by design; re-submitting the same rename from the
      // UI is the recovery path (self-draining query means it only
      // reprocesses what's still stuck).
      console.error(
        `[UpdateISINWorker] rename finished with ${failedTargets.length} failed target(s): ` +
          failedTargets.map((f) => `${f.table} (${f.error})`).join("; "),
      );
      await finalizeJobsRow(zcql, statusKey, "PARTIAL_FAILURE");
      // A handled, recorded outcome — not a crash — so close as success and
      // don't trigger Catalyst's own function-level retry.
      context.closeWithSuccess();
      return;
    }

    // Rename fully done across all 16 tables — that's the whole flow now, no
    // follow-up Holdings recompute step.
    console.log(
      `[UpdateISINWorker] rename DONE "${oldIsin}" → "${newIsin}" | ${totalUpdated} row(s) ` +
        `in ${Date.now() - startedAt}ms.`,
    );
    await finalizeJobsRow(zcql, statusKey, "SUCCESS");
    context.closeWithSuccess();
  } catch (err) {
    console.error("[UpdateISINWorker] Fatal:", err?.message || err);
    context.closeWithFailure();
  }
};
