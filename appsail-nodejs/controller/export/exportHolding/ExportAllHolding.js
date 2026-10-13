const parseCatalystTime = (ct) => {
  if (!ct) return 0;
  // Catalyst CREATEDTIME/MODIFIEDTIME is an IST (UTC+05:30) wall-clock string like
  // "2026-06-19 17:41:28:675". Parse it explicitly as IST so the AppSail
  // server's own timezone (UTC) doesn't shift it +5:30. Without the explicit
  // offset the time renders 5.5h ahead in the UI after a refresh.
  const iso = String(ct).trim().replace(" ", "T").replace(/:(\d{3})$/, ".$1");
  const ms = new Date(`${iso}+05:30`).getTime();
  return isNaN(ms) ? 0 : ms;
};

/**
 * Job/file naming per report mode. Scheme-wise keeps the original `EA_` /
 * `ea-` names so existing exports stay reachable; consolidated uses `EAC_` /
 * `eac-` so the two modes never collide for the same date.
 */
const isConsolidated = (mode) => String(mode || "").toLowerCase() === "consolidated";
const jobNameFor = (mode, dateStr) =>
  `${isConsolidated(mode) ? "EAC" : "EA"}_${dateStr}`;
const fileNameFor = (mode, dateStr) =>
  `${isConsolidated(mode) ? "eac" : "ea"}-${dateStr}.csv`;

export const exportAllData = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();
    const jobScheduling = catalystApp.jobScheduling();

    const { asOnDate, mode } = req.query;
    const dateStr = asOnDate || new Date().toISOString().split("T")[0];

    // Short names to stay within 20-char Catalyst param limit
    const jobName = jobNameFor(mode, dateStr);
    const fileName = fileNameFor(mode, dateStr);

    // Check if a job for this date already exists. There is one Jobs row per
    // jobName, reused by every re-export, so job age is measured from
    // MODIFIEDTIME (last status change), not CREATEDTIME (first-ever export).
    const existing = await zcql.executeZCQLQuery(
      `SELECT ROWID, status, MODIFIEDTIME FROM Jobs WHERE jobName = '${jobName}' LIMIT 1`
    );

    if (existing.length) {
      const oldStatus = existing[0].Jobs.status;
      const updatedTime = existing[0].Jobs.MODIFIEDTIME;

      const STALE_TIMEOUT_MS = 60 * 60 * 1000;
      const jobAge = Date.now() - parseCatalystTime(updatedTime);
      const isStale = jobAge > STALE_TIMEOUT_MS;

      // If still running AND not stale, don't allow re-export
      if ((oldStatus === "PENDING" || oldStatus === "RUNNING") && !isStale) {
        return res.json({
          jobName,
          fileName,
          asOnDate: dateStr,
          status: oldStatus,
          createdAt: updatedTime
            ? new Date(parseCatalystTime(updatedTime)).toISOString()
            : new Date().toISOString(),
          message: "Export is already in progress for this date",
        });
      }

      // COMPLETED, FAILED, or stale — do NOT clear the Jobs row or Stratus
      // file here. ExportAllCustomerHoldingData/index.js needs to see them
      // still in place on its first invocation so it can decide for itself:
      // if this date's file was already generated earlier *today*, reuse it
      // as-is; otherwise delete-and-rebuild. Deleting them here would erase
      // that evidence before the function ever gets to look at it.
      //
      // The function's own verified delete-then-recreate of fileName (see
      // ensureObjectDeleted in checkpoint.js) is what actually guarantees a
      // rebuild never collides with a prior run's leftovers, so nothing is
      // lost by not cleaning up here.
    }

    // Submit a fresh job
    await jobScheduling.JOB.submitJob({
      job_name: "ExportAll",
      jobpool_name: "Export",
      target_name: "ExportAllCustomerHoldingData",
      target_type: "Function",
      params: {
        asOnDate: dateStr,
        jobName,
        fileName,
        mode: isConsolidated(mode) ? "consolidated" : "scheme",
      },
    });

    return res.json({
      jobName,
      fileName,
      asOnDate: dateStr,
      mode: isConsolidated(mode) ? "consolidated" : "scheme",
      status: "PENDING",
      createdAt: new Date().toISOString(),
      message: "Export job started",
    });
  } catch (error) {
    console.error("Error scheduling export job:", error);
    return res.status(500).json({
      message: "Failed to schedule export job",
      error: error.message,
    });
  }
};

export const getExportAllJobStatus = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();

    const { asOnDate, mode } = req.query;
    const dateStr = asOnDate || new Date().toISOString().split("T")[0];
    const jobName = jobNameFor(mode, dateStr);

    const result = await zcql.executeZCQLQuery(
      `SELECT ROWID, status, MODIFIEDTIME FROM Jobs WHERE jobName = '${jobName}' LIMIT 1`
    );

    if (!result.length) {
      return res.json({ status: "NOT_STARTED" });
    }

    let status = result[0].Jobs.status;
    const updatedTime = result[0].Jobs.MODIFIEDTIME;

    const STALE_TIMEOUT_MS = 60 * 60 * 1000;
    const jobAge = Date.now() - parseCatalystTime(updatedTime);

    if ((status === "PENDING" || status === "RUNNING") && jobAge > STALE_TIMEOUT_MS) {
      try {
        await zcql.executeZCQLQuery(
          `UPDATE Jobs SET status = 'ERROR' WHERE jobName = '${jobName}'`
        );
      } catch (updateErr) {
        console.error("Failed to mark stale job as ERROR:", updateErr);
      }
      status = "ERROR";
    }

    return res.json({
      jobName,
      status,
    });
  } catch (error) {
    console.error("Error fetching export job status:", error);
    return res.status(500).json({
      message: "Failed to fetch export job status",
    });
  }
};

export const getExportAllHistory = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();

    const limit = Number(req.query.limit || 10);

    // ZCQL uses * as LIKE wildcard (not %); _ is literal, so 'EA_*' does not
    // match 'EAC_...'. Run separate queries per format: scheme-wise new (EA_),
    // consolidated (EAC_), and old format (EXPORT_ALL_).
    // Order by MODIFIEDTIME, not ROWID: a re-export reuses its date's old Jobs
    // row, so ROWID order would leave a fresh re-export of an old date off the list.
    let newJobs = [];
    let consolidatedJobs = [];
    let oldJobs = [];
    try {
      newJobs = await zcql.executeZCQLQuery(
        `SELECT jobName, status, MODIFIEDTIME FROM Jobs WHERE jobName LIKE 'EA_*' ORDER BY MODIFIEDTIME DESC LIMIT ${limit}`
      );
    } catch (e) { console.error("Error fetching new jobs:", e); }
    try {
      consolidatedJobs = await zcql.executeZCQLQuery(
        `SELECT jobName, status, MODIFIEDTIME FROM Jobs WHERE jobName LIKE 'EAC_*' ORDER BY MODIFIEDTIME DESC LIMIT ${limit}`
      );
    } catch (e) { console.error("Error fetching consolidated jobs:", e); }
    try {
      oldJobs = await zcql.executeZCQLQuery(
        `SELECT jobName, status, MODIFIEDTIME FROM Jobs WHERE jobName LIKE 'EXPORT_ALL_*' ORDER BY MODIFIEDTIME DESC LIMIT ${limit}`
      );
    } catch (e) { console.error("Error fetching old jobs:", e); }

    const allResults = [...(newJobs || []), ...(consolidatedJobs || []), ...(oldJobs || [])];

    const STALE_TIMEOUT_MS = 60 * 60 * 1000;
    const now = Date.now();

    const jobs = [];
    for (const row of allResults) {
      const jobName = row.Jobs.jobName;
      let status = row.Jobs.status;
      const updatedTime = row.Jobs.MODIFIEDTIME;
      const updatedAtMs = parseCatalystTime(updatedTime);

      let asOnDate;
      let mode = "scheme";
      if (jobName.startsWith("EAC_")) {
        asOnDate = jobName.replace("EAC_", "");
        mode = "consolidated";
      } else if (jobName.startsWith("EA_")) {
        asOnDate = jobName.replace("EA_", "");
      } else {
        asOnDate = jobName.replace("EXPORT_ALL_", "");
      }

      const jobAge = now - updatedAtMs;
      if ((status === "PENDING" || status === "RUNNING") && jobAge > STALE_TIMEOUT_MS) {
        try {
          await zcql.executeZCQLQuery(
            `UPDATE Jobs SET status = 'ERROR' WHERE jobName = '${jobName}'`
          );
        } catch (updateErr) {
          console.error(`Failed to mark stale job ${jobName} as ERROR:`, updateErr);
        }
        status = "ERROR";
      }

      jobs.push({
        jobName,
        asOnDate,
        mode,
        status,
        createdAt:
          updatedAtMs > 0
            ? new Date(updatedAtMs).toISOString()
            : new Date().toISOString(),
        _sortMs: updatedAtMs,
      });
    }

    jobs.sort((a, b) => b._sortMs - a._sortMs);
    const limited = jobs.slice(0, limit).map(({ _sortMs, ...rest }) => rest);

    return res.json(limited);
  } catch (error) {
    console.error("Error fetching export history:", error);
    return res.status(500).json({
      message: "Failed to fetch export history",
    });
  }
};

export const downloadExportFile = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();

    const { asOnDate, mode } = req.query;
    if (!asOnDate) {
      return res.status(400).json({ message: "asOnDate is required" });
    }

    const jobName = jobNameFor(mode, asOnDate);
    const fileName = fileNameFor(mode, asOnDate);

    const job = await zcql.executeZCQLQuery(
      `SELECT status FROM Jobs WHERE jobName = '${jobName}' LIMIT 1`
    );

    if (!job.length || job[0].Jobs.status !== "COMPLETED") {
      return res.status(400).json({
        status: "NOT_READY",
        message: "Export not completed yet",
      });
    }

    const stratus = catalystApp.stratus();
    const bucket = stratus.bucket("upload-data-bucket");

    // A signed URL is issued even for a missing key, so the browser would land
    // on a raw 404. The file can be missing while a re-export of the same date
    // is rebuilding it (the old file is deleted first) or after a failed run.
    const fileExists = await bucket.headObject(fileName, { throwErr: false });
    if (!fileExists) {
      return res.status(404).json({
        status: "NOT_FOUND",
        message: "Export file is not available. If an export for this date is running, wait for it to finish; otherwise export again.",
      });
    }

    const downloadUrl = await bucket.generatePreSignedUrl(fileName, "GET", {
      expiresIn: 3600,
    });

    return res.json({
      status: "READY",
      fileName,
      downloadUrl,
    });
  } catch (error) {
    console.error("Error downloading export file:", error);
    return res.status(404).json({
      status: "NOT_FOUND",
      message: "File not found or not ready yet",
    });
  }
};
