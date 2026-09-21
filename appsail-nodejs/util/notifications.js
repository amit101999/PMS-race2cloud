/**
 * Server-backed notification bell.
 *
 * One `Notifications` Data Store row per background task, keyed by `jobName`
 * (the same job / track key the React page already uses). Columns you create:
 *   jobName, status, title, message   (+ automatic ROWID / CREATEDTIME / MODIFIEDTIME)
 *
 * `status` is one of: in_progress | completed | failed | partial_failure.
 *
 * The bell shows the newest 5 rows to every admin and stores nothing in the
 * browser, so the list survives a refresh and is the same for everyone.
 *
 * It does NOT poll each job. Instead `listRecentNotifications` reconciles any
 * row still "in_progress" against the `Jobs` table that every background job
 * already writes its end state into (and `readCaJobProgress` for the
 * corporate-action keys that fan out into rebuild jobs). So a row still settles
 * to completed / failed even if the admin who started it has closed the browser
 * — the next time anyone opens the bell.
 */

import { readCaJobProgress } from "./caJobProgress.js";
import { TABLE as CM_TABLE } from "./custodyMatch/runStore.js";
import { RUN_STATUS as CM_RUN_STATUS } from "./custodyMatch/constants.js";

const MAX_SHOWN = 5;
const PRUNE_AFTER_MS = 24 * 60 * 60 * 1000; // drop every row older than 24 hours
const PRUNE_EVERY_MS = 60 * 60 * 1000; // ...but only actually sweep once an hour
const ALLOWED_STATUS = ["in_progress", "completed", "failed", "partial_failure"];

// Per-process throttle so the sweep doesn't run on every poll (see pruneOldNotifications).
let lastPruneAt = 0;

const escSql = (v) => String(v ?? "").replace(/'/g, "''");
const onlyDigits = (v) => String(v ?? "").replace(/[^0-9]/g, "");

const parseCatalystTime = (ct) => {
  if (!ct) return 0;
  const ms = new Date(String(ct).replace(/:(\d{3})$/, ".$1")).getTime();
  return Number.isNaN(ms) ? 0 : ms;
};

// Corporate-action track keys fan out into background rebuild jobs, so their
// true end state comes from readCaJobProgress, not a single Jobs row.
const isCaKey = (jobName) => /^(SPL|BONA|MRGT|DMGT)/.test(String(jobName || ""));

// FA vs Custody matching runs ("CM-<ms>") record their end state on their own
// run row, not in Jobs, so they are settled from Custody_Match_Run instead.
const isCmRunKey = (jobName) => /^CM-\d+$/.test(String(jobName || ""));

async function settleCmRun(zcql, runId) {
  const rows = await zcql.executeZCQLQuery(
    `SELECT status, exceptionCount, errorMessage FROM ${CM_TABLE.RUN} ` +
      `WHERE runId = '${escSql(runId)}' LIMIT 1`,
  );
  const run = rows?.[0]?.[CM_TABLE.RUN] || rows?.[0] || {};
  const exceptions = Number(run.exceptionCount) || 0;

  switch (run.status) {
    case CM_RUN_STATUS.COMPLETED_CLEAN:
      return { status: "completed", message: "Matching finished — no differences found." };
    case CM_RUN_STATUS.COMPLETED_WITH_EXCEPTIONS:
      return {
        status: "completed",
        message: `Matching finished — ${exceptions.toLocaleString()} exception(s) to review.`,
      };
    case CM_RUN_STATUS.FAILED:
      return { status: "failed", message: run.errorMessage || "Matching failed. Open the run to retry." };
    case CM_RUN_STATUS.SUPERSEDED:
      return { status: "completed", message: "Files changed after this match — run it again." };
    default:
      return { status: "in_progress" }; // QUEUED | RUNNING | row not written yet
  }
}

function statusFromCaProgress(progress) {
  if (progress === "COMPLETED") return "completed";
  if (progress === "COMPLETED_WITH_ERRORS") return "partial_failure";
  if (progress === "ERROR") return "failed";
  return "in_progress"; // NOT_STARTED | RUNNING
}

// The various job functions don't share one vocabulary for the Jobs row:
// exports write COMPLETED / NO_DATA, dividend writes COMPLETED, the ISIN worker
// writes SUCCESS, ISIN partials write PARTIAL_FAILURE, etc. Map them all here.
function statusFromJobsRow(raw) {
  const s = String(raw || "").toUpperCase();
  if (
    ["COMPLETED", "COMPLETE", "DONE", "SUCCESS", "SUCCESSFUL", "NO_DATA", "OK"].includes(s)
  ) {
    return "completed";
  }
  if (["PARTIAL_FAILURE", "PARTIAL", "COMPLETED_WITH_ERRORS"].includes(s)) {
    return "partial_failure";
  }
  if (
    ["FAILED", "FAILURE", "ERROR"].includes(s) ||
    s.startsWith("FAILED:") ||
    s.startsWith("FAILURE:")
  ) {
    return "failed";
  }
  return "in_progress"; // PENDING | RUNNING | IN_PROGRESS | row not written yet
}

/**
 * Plain-language closing sentence, built from the row's own title so we don't
 * need a per-module lookup. "Merger — INE1 → INE2" -> "Merger completed."
 */
function composeMessage(title, status) {
  const noun = String(title || "").split(/[—:\-]/)[0].trim() || "The task";
  if (status === "completed") return `${noun} completed.`;
  if (status === "failed") return `${noun} could not be completed. Please try again.`;
  if (status === "partial_failure") {
    return `${noun} completed, but a few items could not be updated.`;
  }
  return null;
}

function rowToJson(r) {
  const row = r.Notifications || r;
  return {
    rowId: String(row.ROWID),
    jobName: row.jobName || "",
    status: row.status || "in_progress",
    title: row.title || "",
    message: row.message || "",
    updatedAt: parseCatalystTime(row.MODIFIEDTIME || row.CREATEDTIME),
  };
}

/**
 * Create the row, or update the existing row for this job key. Keeping one row
 * per key means an admin re-submitting the same job (or the sync "started" then
 * "completed" pair the Reports exports send) updates in place instead of piling
 * up duplicates.
 */
export async function insertNotification(zcql, { jobName, title, message, status }) {
  const key = escSql(jobName);
  const safeStatus = ALLOWED_STATUS.includes(status) ? status : "in_progress";
  const existing = await zcql.executeZCQLQuery(
    `SELECT ROWID FROM Notifications WHERE jobName = '${key}' LIMIT 1`,
  );
  if (existing?.length) {
    const rowId = onlyDigits(existing[0].Notifications?.ROWID ?? existing[0].ROWID);
    await zcql.executeZCQLQuery(
      `UPDATE Notifications SET status = '${escSql(safeStatus)}', ` +
        `title = '${escSql(title)}', message = '${escSql(message)}' WHERE ROWID = ${rowId}`,
    );
    return;
  }
  await zcql.executeZCQLQuery(
    `INSERT INTO Notifications (jobName, status, title, message) ` +
      `VALUES ('${key}', '${escSql(safeStatus)}', '${escSql(title)}', '${escSql(message)}')`,
  );
}

async function reconcileRow(zcql, row) {
  if (row.status !== "in_progress" || !row.jobName) return row;

  let next;
  let cmMessage = null;
  try {
    if (isCmRunKey(row.jobName)) {
      const settled = await settleCmRun(zcql, row.jobName);
      next = settled.status;
      cmMessage = settled.message || null;
    } else if (isCaKey(row.jobName)) {
      next = statusFromCaProgress(await readCaJobProgress(zcql, row.jobName));
    } else {
      const jr = await zcql.executeZCQLQuery(
        `SELECT status FROM Jobs WHERE jobName = '${escSql(row.jobName)}' LIMIT 1`,
      );
      const raw = jr?.length ? jr[0].Jobs?.status ?? jr[0].status : "";
      next = statusFromJobsRow(raw);
    }
  } catch (e) {
    console.warn(`[notifications] reconcile '${row.jobName}' failed:`, e.message);
    return row;
  }

  if (next === "in_progress" || next === row.status) return row;

  const message = cmMessage || composeMessage(row.title, next) || row.message;
  try {
    await zcql.executeZCQLQuery(
      `UPDATE Notifications SET status = '${escSql(next)}', message = '${escSql(message)}' ` +
        `WHERE ROWID = ${onlyDigits(row.rowId)}`,
    );
  } catch (e) {
    console.warn(`[notifications] settle '${row.jobName}' failed:`, e.message);
    return row;
  }
  return { ...row, status: next, message, updatedAt: Date.now() };
}

export async function listRecentNotifications(zcql) {
  const rows = await zcql.executeZCQLQuery(
    `SELECT ROWID, jobName, status, title, message, CREATEDTIME, MODIFIEDTIME ` +
      `FROM Notifications ORDER BY CREATEDTIME DESC LIMIT ${MAX_SHOWN}`,
  );
  const list = (rows || []).map(rowToJson);
  return Promise.all(list.map((row) => reconcileRow(zcql, row)));
}

export async function deleteNotification(zcql, rowId) {
  const id = onlyDigits(rowId);
  if (!id) return;
  await zcql.executeZCQLQuery(`DELETE FROM Notifications WHERE ROWID = ${id}`);
}

export async function clearNotifications(zcql, rowIds) {
  const ids = (Array.isArray(rowIds) ? rowIds : []).map(onlyDigits).filter(Boolean);
  for (const id of ids) {
    try {
      await zcql.executeZCQLQuery(`DELETE FROM Notifications WHERE ROWID = ${id}`);
    } catch (e) {
      console.warn(`[notifications] clear row ${id} failed:`, e.message);
    }
  }
}

/**
 * Best-effort housekeeping: delete every notification older than 24 hours,
 * whatever its status.
 *
 * Called on each list fetch but does real work at most once an hour per AppSail
 * process — the poll runs every ~5s and there is almost never anything to sweep.
 * When it does run it's one SELECT plus a single batched DELETE, not a delete
 * per row. If several instances each sweep once an hour that's harmless (the
 * DELETE is idempotent).
 */
export async function pruneOldNotifications(zcql) {
  if (Date.now() - lastPruneAt < PRUNE_EVERY_MS) return;
  try {
    const rows = await zcql.executeZCQLQuery(
      `SELECT ROWID, CREATEDTIME FROM Notifications ORDER BY CREATEDTIME ASC LIMIT 200`,
    );
    const stale = (rows || [])
      .map((r) => r.Notifications || r)
      .filter((r) => Date.now() - parseCatalystTime(r.CREATEDTIME) > PRUNE_AFTER_MS)
      .map((r) => onlyDigits(r.ROWID))
      .filter(Boolean);
    if (stale.length) {
      await zcql.executeZCQLQuery(
        `DELETE FROM Notifications WHERE ROWID IN (${stale.join(",")})`,
      );
    }
    lastPruneAt = Date.now();
  } catch (e) {
    console.warn("[notifications] prune failed:", e.message);
  }
}
