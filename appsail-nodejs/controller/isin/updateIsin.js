/**
 * GET /api/isin/security-list-isins — ISINs from Security_List (dropdown data).
 * POST /api/isin/update — Queues UpdateISIN job; old ISIN must exist in Security_List
 * and new ISIN must not already be assigned to a security.
 * The job replaces the old ISIN with the new one across all relevant database tables.
 */

/** Catalyst console mein jis job pool mein "UpdateISIN" function add hai, wahi naam yahan hona chahiye. */
const UPDATE_ISIN_JOBPOOL = "UpdateMasters";
const SECURITY_LIST_BATCH = 270;

/**
 * Must match ISIN_TARGETS.length in functions/UpdateISINWorker/index.js.
 * Used to build the exact "<jobName>_t0".."_t(N-1)" progress-row names to
 * fetch via `IN (...)` — ZCQL's LIKE does not support `%` wildcards (it
 * silently matches nothing), so a "<jobName>_t%" prefix lookup can't be used.
 */
const RENAME_TARGET_COUNT = 16;

const escSql = (value) => String(value ?? "").replace(/'/g, "''");

/**
 * GET /api/isin/security-list-isins
 * ISINs from Security_List for the Old ISIN dropdown (deduped, sorted).
 */
export const getSecurityListIsins = async (req, res) => {
  try {
    if (!req.catalystApp) {
      return res.status(500).json({
        success: false,
        message: "Catalyst not initialized",
      });
    }

    const zcql = req.catalystApp.zcql();
    const raw = [];
    let offset = 0;

    while (true) {
      const rows = await zcql.executeZCQLQuery(`
        SELECT ISIN, Security_Code, Security_Name
        FROM Security_List
        WHERE ISIN IS NOT NULL
        ORDER BY ISIN ASC, ROWID ASC
        LIMIT ${SECURITY_LIST_BATCH} OFFSET ${offset}
      `);

      if (!rows?.length) break;

      for (const r of rows) {
        const s = r.Security_List || r;
        const isin = String(s.ISIN ?? "").trim();
        if (!isin) continue;
        raw.push({
          isin,
          securityCode: String(s.Security_Code ?? "").trim(),
          securityName: String(s.Security_Name ?? "").trim(),
        });
      }

      if (rows.length < SECURITY_LIST_BATCH) break;
      offset += SECURITY_LIST_BATCH;
    }

    const seen = new Set();
    const data = [];
    for (const row of raw) {
      if (seen.has(row.isin)) continue;
      seen.add(row.isin);
      data.push(row);
    }

    return res.status(200).json({ success: true, data });
  } catch (error) {
    console.error("[getSecurityListIsins]", error);
    return res.status(500).json({
      success: false,
      message: error?.message || "Failed to load Security List ISINs",
    });
  }
};

export const postUpdateIsin = async (req, res) => {
  try {
    if (!req.catalystApp) {
      return res.status(500).json({
        success: false,
        message: "Catalyst not initialized",
      });
    }

    const { oldIsin, newIsin } = req.body || {};
    const oldTrim = String(oldIsin ?? "").trim();
    const newTrim = String(newIsin ?? "").trim();

    if (!oldTrim || !newTrim) {
      return res.status(400).json({
        success: false,
        message: "Old ISIN and New ISIN are required.",
      });
    }
    if (oldTrim === newTrim) {
      return res.status(400).json({
        success: false,
        message: "Old ISIN and New ISIN must be different.",
      });
    }

    const zcql = req.catalystApp.zcql();
    const existsRows = await zcql.executeZCQLQuery(`
      SELECT ROWID
      FROM Security_List
      WHERE ISIN='${escSql(oldTrim)}'
      LIMIT 1
    `);
    if (!existsRows?.length) {
      return res.status(400).json({
        success: false,
        message: "Old ISIN must exist in Security List.",
      });
    }

    const newIsinRows = await zcql.executeZCQLQuery(`
      SELECT ROWID
      FROM Security_List
      WHERE ISIN='${escSql(newTrim)}'
      LIMIT 1
    `);
    if (newIsinRows?.length) {
      return res.status(400).json({
        success: false,
        message: "New ISIN is already assigned to a security in Security List.",
      });
    }

    const catalystApp = req.catalystApp;

    // Catalyst submitJob: job_name must be 1–20 characters (not the Stratus object key).
    // This name also keys the Jobs status row the worker marks SUCCESS when done.
    const catalystJobName = `U${Date.now()}`.slice(0, 20);

    const submitted = await catalystApp.jobScheduling().JOB.submitJob({
      job_name: catalystJobName,
      jobpool_name: UPDATE_ISIN_JOBPOOL,
      target_name: "UpdateISIN",
      target_type: "Function",
      params: {
        mode: "rename",
        old_isin: oldTrim,
        new_isin: newTrim,
        status_key: catalystJobName,
      },
    });

    const jobId = submitted?.job_id ?? submitted?.jobId ?? null;

    return res.status(200).json({
      success: true,
      message:
        "UpdateISIN job queued; old ISIN rows will be updated to the new ISIN in the database.",
      catalystJobName,
      jobId,
    });
  } catch (error) {
    console.error("[updateIsin]", error);
    return res.status(500).json({
      success: false,
      message: error?.message || "Failed to queue ISIN update",
    });
  }
};

/**
 * POST /api/isin/apply-new
 * Queues the UpdateISIN job in "apply-new" mode (the "New ISIN" panel):
 * syncs Security_Code / Security_Name for the given ISIN across Security_List
 * and Transaction. Per-column rule:
 *   - Skip rows where the value already equals the new value.
 *   - Update everywhere else (NULL, empty, or different).
 */
export const postApplyNewISIN = async (req, res) => {
  try {
    if (!req.catalystApp) {
      return res.status(500).json({
        success: false,
        message: "Catalyst not initialized",
      });
    }

    const { isin, securityCode, securityName } = req.body || {};
    const isinTrim = String(isin ?? "").trim();
    const codeTrim = String(securityCode ?? "").trim();
    const nameTrim = String(securityName ?? "").trim();

    if (!isinTrim) {
      return res.status(400).json({
        success: false,
        message: "ISIN is required.",
      });
    }
    if (!codeTrim && !nameTrim) {
      return res.status(400).json({
        success: false,
        message: "Provide at least one of Security Code or Security Name.",
      });
    }

    const zcql = req.catalystApp.zcql();
    const existsRows = await zcql.executeZCQLQuery(`
      SELECT ROWID
      FROM Security_List
      WHERE ISIN='${escSql(isinTrim)}'
      LIMIT 1
    `);
    if (!existsRows?.length) {
      return res.status(400).json({
        success: false,
        message: "ISIN must exist in Security List.",
      });
    }

    const catalystApp = req.catalystApp;
    const catalystJobName = `N${Date.now()}`.slice(0, 20);

    const submitted = await catalystApp.jobScheduling().JOB.submitJob({
      job_name: catalystJobName,
      jobpool_name: UPDATE_ISIN_JOBPOOL,
      target_name: "UpdateISIN",
      target_type: "Function",
      params: {
        mode: "apply-new",
        isin: isinTrim,
        security_code: codeTrim,
        security_name: nameTrim,
      },
    });

    const jobId = submitted?.job_id ?? submitted?.jobId ?? null;

    return res.status(200).json({
      success: true,
      message:
        "New ISIN apply queued; Security_Code / Security_Name will be updated where missing or different.",
      catalystJobName,
      jobId,
    });
  } catch (error) {
    console.error("[postApplyNewISIN]", error);
    return res.status(500).json({
      success: false,
      message: error?.message || "Failed to queue New ISIN apply",
    });
  }
};

/**
 * GET /api/isin/job-status?jobId=
 * Poll Catalyst for UpdateISIN (or any) job execution status.
 */
export const getIsinUpdateJobStatus = async (req, res) => {
  try {
    if (!req.catalystApp) {
      return res.status(500).json({
        success: false,
        message: "Catalyst not initialized",
      });
    }

    // Preferred: read the Jobs status row the worker maintains (jobName = the
    // status_key returned by /update). The master job itself completes as soon
    // as it dispatches the worker, so its native jobId is NOT the real status.
    const jobNameRaw = req.query.jobName;
    if (jobNameRaw != null && String(jobNameRaw).trim() !== "") {
      const jobName = String(jobNameRaw).trim();
      const rows = await req.catalystApp.zcql().executeZCQLQuery(`
        SELECT status FROM Jobs WHERE jobName = '${escSql(jobName)}' LIMIT 1
      `);
      const st = String(rows?.[0]?.Jobs?.status ?? rows?.[0]?.status ?? "").toUpperCase();
      let jobStatus;
      if (["SUCCESS", "SUCCESSFUL", "COMPLETED", "COMPLETE"].includes(st)) {
        jobStatus = "SUCCESSFUL";
      } else if (["FAILURE", "FAILED", "ERROR"].includes(st)) {
        jobStatus = "FAILURE";
      } else if (st === "PARTIAL_FAILURE") {
        // Rename finished, but one or more tables couldn't be updated even
        // after retries — a terminal state, distinct from a full FAILURE.
        // No automatic/manual retry for this run; surface which table(s)
        // failed and why, using the per-target progress rows the worker
        // wrote as "<jobName>_t<index>" (see UpdateISINWorker).
        jobStatus = "PARTIAL_FAILURE";
      } else {
        jobStatus = st || "IN_PROGRESS";
      }

      if (jobStatus === "PARTIAL_FAILURE") {
        // Exact-match IN (...) over the fixed, known set of target indices —
        // NOT `LIKE '<jobName>_t%'`. ZCQL's LIKE does not support % wildcards
        // (it silently matches zero rows rather than erroring), which would
        // make this always come back empty.
        const targetNames = [];
        for (let i = 0; i < RENAME_TARGET_COUNT; i++) {
          targetNames.push(`'${escSql(jobName)}_t${i}'`);
        }
        const progressRows = await req.catalystApp.zcql().executeZCQLQuery(`
          SELECT status FROM Jobs WHERE jobName IN (${targetNames.join(",")})
        `);
        const failedTargets = (progressRows || [])
          .map((r) => r?.Jobs ?? r)
          .filter((r) => String(r?.status ?? "").toUpperCase().startsWith("FAILED"))
          .map((r) => {
            // status = "FAILED:<table>:<error>"
            const body = String(r?.status ?? "").replace(/^FAILED:/i, "");
            const sepIdx = body.indexOf(":");
            const table = (sepIdx >= 0 ? body.slice(0, sepIdx) : body) || "(unknown table)";
            const error = sepIdx >= 0 ? body.slice(sepIdx + 1) : "";
            return { table, error };
          });
        return res.status(200).json({ success: true, jobName, jobStatus, failedTargets });
      }

      return res.status(200).json({ success: true, jobName, jobStatus });
    }

    const raw = req.query.jobId;
    if (raw == null || String(raw).trim() === "") {
      return res.status(400).json({
        success: false,
        message: "Query parameter jobId or jobName is required.",
      });
    }

    const jobId = String(raw).trim();
    const details = await req.catalystApp.jobScheduling().JOB.getJob(jobId);

    const rawStatus = String(details.job_status ?? "").toUpperCase();
    console.log("[getIsinUpdateJobStatus] raw status:", rawStatus);

    // Normalize Catalyst status to what the frontend expects
    let jobStatus;
    if (["SUCCESSFUL", "SUCCESS", "COMPLETED", "COMPLETE"].includes(rawStatus)) {
      jobStatus = "SUCCESSFUL";
    } else if (["FAILURE", "FAILED", "ERROR"].includes(rawStatus)) {
      jobStatus = "FAILURE";
    } else {
      jobStatus = rawStatus; // IN_PROGRESS, QUEUED, etc.
    }

    return res.status(200).json({
      success: true,
      jobId: details.job_id,
      jobStatus,
    });
  } catch (error) {
    console.error("[getIsinUpdateJobStatus]", error);
    return res.status(500).json({
      success: false,
      message: error?.message || "Failed to fetch job status",
    });
  }
};
