"use strict";

/**
 * UpdateISIN (Catalyst job) — master / orchestrator. Two modes.
 *
 *   mode = "rename" (default, params: old_isin, new_isin, status_key?)
 *     Does NO renaming itself. It opens a Jobs status row and dispatches one
 *     UpdateISINWorker job, which renames old_isin → new_isin across every
 *     ISIN-bearing table in small, timeout-proof batches (and walks the
 *     merger/demerger old + new ISIN columns). See UpdateISINWorker/index.js.
 *
 *   mode = "apply-new" (params: isin, security_code?, security_name?, status_key?)
 *     Backs the "New ISIN" Apply panel. First checks whether `isin` already
 *     exists in Security_List:
 *       - Doesn't exist yet → CREATE it: INSERT INTO Security_List with the
 *         given ISIN/Code/Name (both are required in this case — a fresh
 *         record can't be half-filled). This is just one extra step in front
 *         of the same pipeline below, not a separate mode/code path.
 *       - Already exists → today's behavior: at least one of Code/Name must
 *         be given.
 *     Either way, it then syncs Security_Code / Security_Name for that ISIN
 *     across NEW_ISIN_TARGETS (Security_List + Transaction). Per column:
 *       - Skip if existing value already equals the new value.
 *       - Otherwise (NULL, empty, or different) → UPDATE.
 *     (For a brand-new ISIN these syncs are harmless no-ops — Security_List
 *     already has the right values from the INSERT, and Transaction has no
 *     rows for an ISIN that never existed before.)
 *     A value param that's blank/missing means that column is never touched.
 *     Column casing differs by table — handled per entry in NEW_ISIN_TARGETS.
 *
 *     Every step (the create, plus each column sync) gets up to
 *     TARGET_RETRY_ATTEMPTS tries before being marked FAILED — mirrors
 *     UpdateISINWorker's per-table retry-then-continue, just for this mode's
 *     much smaller, dynamic-length step list. The Jobs row is finalized
 *     SUCCESS only if every attempted step succeeded, PARTIAL_FAILURE
 *     otherwise (naming which step and why) — reusing the exact same
 *     progress-row naming ("<status_key>_t<index>") and job-status/
 *     failedTargets lookup already built for rename, so no frontend or
 *     appsail-nodejs endpoint changes are needed to read this mode's status.
 */

const catalyst = require("zcatalyst-sdk-node");

const esc = (v) => String(v ?? "").replace(/'/g, "''");

/** Worker that performs the batched rename, and the pool it runs in. */
const WORKER_TARGET = "UpdateISINWorker";
const WORKER_JOBPOOL = "UpdateMasters";

/** Same retry policy as UpdateISINWorker's per-table retry (see there for why). */
const TARGET_RETRY_ATTEMPTS = 3;
const TARGET_RETRY_DELAY_MS = 3000;

const NEW_ISIN_TARGETS = [
  { table: "Security_List", codeCol: "Security_Code", nameCol: "Security_Name" },
  { table: "Transaction", codeCol: "Security_code", nameCol: "Security_Name" },
];

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Progress-row name for one apply-new sync slot — same format UpdateISINWorker
 * uses for rename targets, so the shared job-status/failedTargets lookup in
 * appsail-nodejs works unmodified for this mode too. */
function progressRowName(statusKey, index) {
  return `${statusKey}_t${index}`;
}

async function markTargetDone(zcql, rowName) {
  if (!rowName) return;
  try {
    await zcql.executeZCQLQuery(
      `UPDATE Jobs SET status = 'DONE' WHERE jobName = '${esc(rowName)}'`,
    );
  } catch (e) {
    console.warn(`[UpdateISIN/apply-new] mark DONE '${rowName}' failed:`, e.message);
  }
}

/** status = "FAILED:<table.col>:<error>" — same shape UpdateISINWorker writes. */
async function markTargetFailed(zcql, rowName, label, errorMessage) {
  if (!rowName) return;
  const detail = esc(`${label}:${String(errorMessage || "").slice(0, 160)}`);
  try {
    await zcql.executeZCQLQuery(
      `UPDATE Jobs SET status = 'FAILED:${detail}' WHERE jobName = '${esc(rowName)}'`,
    );
  } catch (e) {
    console.warn(`[UpdateISIN/apply-new] mark FAILED '${rowName}' failed:`, e.message);
  }
}

async function finalizeJobsRow(zcql, jobName, status) {
  if (!jobName) return;
  try {
    await zcql.executeZCQLQuery(
      `UPDATE Jobs SET status = '${esc(status)}' WHERE jobName = '${esc(jobName)}'`,
    );
  } catch (e) {
    console.error(`[UpdateISIN/apply-new] finalize Jobs '${jobName}' failed:`, e.message);
  }
}

/** Open (or reset) the Jobs row the worker marks SUCCESS when the rename finishes. */
async function ensureJobsRowRunning(zcql, jobName) {
  if (!jobName) return;
  try {
    await zcql.executeZCQLQuery(
      `INSERT INTO Jobs (jobName, status) VALUES ('${esc(jobName)}', 'RUNNING')`,
    );
  } catch (insertErr) {
    try {
      await zcql.executeZCQLQuery(
        `UPDATE Jobs SET status = 'RUNNING' WHERE jobName = '${esc(jobName)}'`,
      );
    } catch (upErr) {
      console.warn(`[UpdateISIN] ensure Jobs RUNNING failed for ${jobName}:`, upErr.message);
    }
  }
}

async function runRenameMode(app, zcql, params) {
  const oldIsin = String(params.old_isin ?? "").trim();
  const newIsin = String(params.new_isin ?? "").trim();
  const statusKey = String(params.status_key ?? "").trim();

  console.log(`[UpdateISIN/rename] Dispatching worker: "${oldIsin}" → "${newIsin}"`);

  if (!oldIsin || !newIsin) {
    throw new Error("Parameters old_isin and new_isin are required for rename mode");
  }
  if (oldIsin === newIsin) {
    throw new Error("Old and new ISIN must differ");
  }

  await ensureJobsRowRunning(zcql, statusKey);

  const workerJobName = `IW0_${Date.now()}`.slice(0, 20);
  await app.jobScheduling().JOB.submitJob({
    job_name: workerJobName,
    jobpool_name: WORKER_JOBPOOL,
    target_name: WORKER_TARGET,
    target_type: "Function",
    job_config: { number_of_retries: 5, retry_interval: 60 * 1000 },
    params: {
      old_isin: oldIsin,
      new_isin: newIsin,
      status_key: statusKey,
      target_index: "0",
    },
  });

  console.log(`[UpdateISIN/rename] Worker queued (${workerJobName}) for "${oldIsin}" → "${newIsin}"`);
}

/** Deliberately lets a failure throw — if we can't tell whether the ISIN
 * exists, we can't safely decide create-vs-update, so the job should fail
 * outright here rather than guessing. */
async function isinExistsInSecurityList(zcql, isin) {
  const rows = await zcql.executeZCQLQuery(
    `SELECT ROWID FROM Security_List WHERE ISIN = '${esc(isin)}' LIMIT 1`,
  );
  return !!(rows && rows.length);
}

async function insertNewSecurity(zcql, isin, code, name) {
  await zcql.executeZCQLQuery(
    `INSERT INTO Security_List (ISIN, Security_Code, Security_Name)
     VALUES ('${esc(isin)}', '${esc(code)}', '${esc(name)}')`,
  );
}

async function countRowsToSync(zcql, table, col, isin, newVal) {
  try {
    const rows = await zcql.executeZCQLQuery(
      `SELECT COUNT(ROWID) FROM ${table}
       WHERE ISIN = '${esc(isin)}'
         AND (${col} IS NULL OR ${col} != '${esc(newVal)}')`
    );
    const r = rows?.[0]?.[table] || rows?.[0] || {};
    return Number(
      r["COUNT(ROWID)"] ?? r.cnt ?? r["count"] ?? Object.values(r)[0] ?? 0
    );
  } catch (err) {
    console.warn(`[UpdateISIN/apply-new] count ${table}.${col} failed:`, err?.message);
    return -1;
  }
}

/**
 * Runs the UPDATE and lets a real failure THROW — the caller (runApplyNewMode)
 * is what retries and marks the target failed after retries are exhausted.
 * (Previously this caught its own errors and returned 0, indistinguishable
 * from "0 rows needed updating" — that's what let the whole job silently
 * report SUCCESS even when every update had actually failed.)
 */
async function syncColumn(zcql, table, col, isin, newVal) {
  const before = await countRowsToSync(zcql, table, col, isin, newVal);
  await zcql.executeZCQLQuery(
    `UPDATE ${table}
     SET ${col} = '${esc(newVal)}'
     WHERE ISIN = '${esc(isin)}'
       AND (${col} IS NULL OR ${col} != '${esc(newVal)}')`
  );
  console.log(`[UpdateISIN/apply-new] ${table}.${col}: updated ~${before} row(s)`);
  return before >= 0 ? before : 0;
}

async function runApplyNewMode(zcql, params) {
  const isin = String(params.isin ?? "").trim();
  const securityCode = String(params.security_code ?? "").trim();
  const securityName = String(params.security_name ?? "").trim();
  const statusKey = String(params.status_key ?? "").trim();

  console.log(
    `[UpdateISIN/apply-new] Started: isin="${isin}" code="${securityCode}" name="${securityName}"`
  );

  if (!isin) {
    throw new Error("Parameter isin is required for apply-new mode");
  }

  const exists = await isinExistsInSecurityList(zcql, isin);

  if (!exists) {
    // Creating a fresh record — a half-filled one isn't useful, so both
    // are required (unlike updating an existing ISIN, where either alone
    // is a valid correction).
    if (!securityCode || !securityName) {
      throw new Error(
        "security_code and security_name are both required to create a new ISIN"
      );
    }
  } else if (!securityCode && !securityName) {
    throw new Error(
      "At least one of security_code / security_name must be provided for apply-new mode"
    );
  }

  await ensureJobsRowRunning(zcql, statusKey);

  // Dynamic, ordered list of steps for this run. Index order is what the
  // progress rows key off, so it only needs to be consistent within a
  // single run, not fixed across runs.
  //   - Doesn't exist yet: step 0 creates it; the sync steps that follow
  //     are then harmless no-ops (Security_List already holds the values
  //     just inserted; Transaction has no rows yet for a brand-new ISIN) —
  //     unless this ISIN's code/name genuinely needs correcting somewhere
  //     else too, in which case they do real work.
  //   - Already exists: no create step, just the sync steps, same as before.
  const steps = [];

  if (!exists) {
    steps.push({
      label: "Security_List (create)",
      run: () => insertNewSecurity(zcql, isin, securityCode, securityName),
    });
  }

  for (const { table, codeCol, nameCol } of NEW_ISIN_TARGETS) {
    if (securityCode) {
      steps.push({
        label: `${table}.${codeCol}`,
        run: () => syncColumn(zcql, table, codeCol, isin, securityCode),
      });
    }
    if (securityName) {
      steps.push({
        label: `${table}.${nameCol}`,
        run: () => syncColumn(zcql, table, nameCol, isin, securityName),
      });
    }
  }

  const summary = [];
  const failedTargets = [];

  for (let i = 0; i < steps.length; i++) {
    const { label, run } = steps[i];
    const rowName = progressRowName(statusKey, i);

    let attempts = 0;
    let succeeded = false;
    let lastErr = "";

    while (attempts < TARGET_RETRY_ATTEMPTS) {
      try {
        await run();
        succeeded = true;
        break;
      } catch (err) {
        attempts++;
        lastErr = err?.message || String(err);
        console.warn(
          `[UpdateISIN/apply-new] ${label} failed (attempt ${attempts}/${TARGET_RETRY_ATTEMPTS}):`,
          lastErr,
        );
        if (attempts < TARGET_RETRY_ATTEMPTS) {
          await sleep(TARGET_RETRY_DELAY_MS);
        }
      }
    }

    if (!succeeded) {
      console.error(`[UpdateISIN/apply-new] step FAILED: ${label} — ${lastErr}`);
      await markTargetFailed(zcql, rowName, label, lastErr);
      failedTargets.push({ label, error: lastErr });
      summary.push({ label, failed: true, error: lastErr });
      continue;
    }

    await markTargetDone(zcql, rowName);
    summary.push({ label, done: true });
  }

  console.log(
    `[UpdateISIN/apply-new] Done isin="${isin}" (created=${!exists}) — ${JSON.stringify(summary)}`
  );

  if (failedTargets.length > 0) {
    console.error(
      `[UpdateISIN/apply-new] finished with ${failedTargets.length} failed step(s): ` +
        failedTargets.map((f) => `${f.label} (${f.error})`).join("; "),
    );
    await finalizeJobsRow(zcql, statusKey, "PARTIAL_FAILURE");
  } else {
    await finalizeJobsRow(zcql, statusKey, "SUCCESS");
  }
}

/**
 * @param {import("./types/job").JobRequest} jobRequest
 * @param {import("./types/job").Context} context
 */
module.exports = async (jobRequest, context) => {
  const catalystApp = catalyst.initialize(context);
  const zcql = catalystApp.zcql();

  try {
    const params = jobRequest.getAllJobParams();
    const mode = String(params.mode ?? "rename").trim();

    if (mode === "rename") {
      await runRenameMode(catalystApp, zcql, params);
    } else if (mode === "apply-new") {
      await runApplyNewMode(zcql, params);
    } else {
      throw new Error(`Unknown mode: "${mode}" (expected "rename" or "apply-new")`);
    }

    context.closeWithSuccess();
  } catch (err) {
    console.error("[UpdateISIN] Fatal Error:", err?.message || err);
    context.closeWithFailure();
  }
};
