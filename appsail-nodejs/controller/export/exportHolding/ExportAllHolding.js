export const exportAllData = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();
    const jobScheduling = catalystApp.jobScheduling();

    const { asOnDate } = req.query;
    const dateStr = asOnDate || new Date().toISOString().split("T")[0];

    // Short names to stay within 20-char Catalyst param limit
    const jobName = `EA_${dateStr}`;
    const fileName = `ea-${dateStr}.csv`;

    // Check if a job for this date already exists
    const existing = await zcql.executeZCQLQuery(
      `SELECT ROWID, status, CREATEDTIME FROM Jobs WHERE jobName = '${jobName}' LIMIT 1`
    );

    if (existing.length) {
      const oldStatus = existing[0].Jobs.status;
      const oldRowId = existing[0].Jobs.ROWID;
      const createdTime = existing[0].Jobs.CREATEDTIME;

      // Check if job is stale (stuck in PENDING/RUNNING for over 30 minutes)
      const STALE_TIMEOUT_MS = 30 * 60 * 1000;
      const jobAge = createdTime ? Date.now() - new Date(createdTime).getTime() : 0;
      const isStale = jobAge > STALE_TIMEOUT_MS;

      // If still running AND not stale, don't allow re-export
      if ((oldStatus === "PENDING" || oldStatus === "RUNNING") && !isStale) {
        return res.json({
          jobName,
          fileName,
          asOnDate: dateStr,
          status: oldStatus,
          message: "Export is already in progress for this date",
        });
      }

      // COMPLETED, FAILED, or stale — delete old job, old file, so we can re-export
      try {
        await zcql.executeZCQLQuery(`DELETE FROM Jobs WHERE ROWID = ${oldRowId}`);
      } catch (delErr) {
        console.error("Error deleting old job:", delErr);
      }

      // Delete old CSV file from Stratus (try both new and old file name formats)
      try {
        const stratus = catalystApp.stratus();
        const bucket = stratus.bucket("upload-data-bucket");
        // New format file
        try { await bucket.deleteObject(`ea-${dateStr}.csv`); } catch (_) { }
        // Old format file
        try { await bucket.deleteObject(`all-clients-export-${dateStr}.csv`); } catch (_) { }
      } catch (stratusErr) {
        console.error("Error deleting old file from Stratus:", stratusErr);
      }
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
      },
    });

    return res.json({
      jobName,
      fileName,
      asOnDate: dateStr,
      status: "PENDING",
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

    const { asOnDate } = req.query;
    const dateStr = asOnDate || new Date().toISOString().split("T")[0];
    const jobName = `EA_${dateStr}`;

    const result = await zcql.executeZCQLQuery(
      `SELECT status FROM Jobs WHERE jobName = '${jobName}' LIMIT 1`
    );

    if (!result.length) {
      return res.json({ status: "NOT_STARTED" });
    }

    return res.json({
      jobName,
      status: result[0].Jobs.status,
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

    // ZCQL uses * as LIKE wildcard (not %)
    // Run two queries: one for new format (EA_), one for old format (EXPORT_ALL_)
    let newJobs = [];
    let oldJobs = [];
    try {
      newJobs = await zcql.executeZCQLQuery(
        `SELECT jobName, status FROM Jobs WHERE jobName LIKE 'EA_*' ORDER BY ROWID DESC LIMIT ${limit}`
      );
    } catch (e) { console.error("Error fetching new jobs:", e); }
    try {
      oldJobs = await zcql.executeZCQLQuery(
        `SELECT jobName, status FROM Jobs WHERE jobName LIKE 'EXPORT_ALL_*' ORDER BY ROWID DESC LIMIT ${limit}`
      );
    } catch (e) { console.error("Error fetching old jobs:", e); }

    const allResults = [...(newJobs || []), ...(oldJobs || [])];

    const jobs = allResults.map((row) => {
      const jobName = row.Jobs.jobName;
      let asOnDate;
      if (jobName.startsWith("EA_")) {
        asOnDate = jobName.replace("EA_", "");
      } else {
        asOnDate = jobName.replace("EXPORT_ALL_", "");
      }

      return {
        jobName,
        asOnDate,
        status: row.Jobs.status,
      };
    });

    return res.json(jobs);
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

    const { asOnDate } = req.query;
    if (!asOnDate) {
      return res.status(400).json({ message: "asOnDate is required" });
    }

    const jobName = `EA_${asOnDate}`;
    const fileName = `ea-${asOnDate}.csv`;

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
