/**
 * POST /api/isin/update
 * Triggers the UpdateISIN Catalyst job, passing oldIsin and newIsin directly as job params.
 * The job then replaces the old ISIN with the new one across all relevant database tables.
 */

/** Catalyst console mein jis job pool mein "UpdateISIN" function add hai, wahi naam yahan hona chahiye. */
const UPDATE_ISIN_JOBPOOL = "UpdateMasters";

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

    const catalystApp = req.catalystApp;

    // Catalyst submitJob: job_name must be 1–20 characters (not the Stratus object key).
    const catalystJobName = `U${Date.now()}`.slice(0, 20);

    const submitted = await catalystApp.jobScheduling().JOB.submitJob({
      job_name: catalystJobName,
      jobpool_name: UPDATE_ISIN_JOBPOOL,
      target_name: "UpdateISIN",
      target_type: "Function",
      params: {
        old_isin: oldTrim,
        new_isin: newTrim,
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

    const raw = req.query.jobId;
    if (raw == null || String(raw).trim() === "") {
      return res.status(400).json({
        success: false,
        message: "Query parameter jobId is required.",
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
