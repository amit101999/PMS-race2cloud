export const exportAllData = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();
    const jobScheduling = catalystApp.jobScheduling();

    const { asOnDate } = req.query;
    const dateStr = asOnDate || new Date().toISOString().split("T")[0];

    const jobName = `EXPORT_ALL_${dateStr}`;
    const fileName = `all-clients-export-${dateStr}.csv`;

    const existing = await zcql.executeZCQLQuery(`
      SELECT status
      FROM Jobs
      WHERE jobName='${jobName}'
      LIMIT 1
    `);

    if (existing.length) {
      return res.json({
        jobName,
        fileName,
        status: existing[0].Jobs.status,
        message: "Export job already exists",
      });
    }

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
    const jobName = `EXPORT_ALL_${dateStr}`;
    const fileName = `all-clients-export-${dateStr}.csv`;

    const result = await zcql.executeZCQLQuery(`
      SELECT status
      FROM Jobs
      WHERE jobName='${jobName}'
      LIMIT 1
    `);

    if (!result.length) {
      return res.json({ status: "NOT_STARTED" });
    }

    return res.json({
      jobName,
      status: result[0].Jobs.status,
      fileName,
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

    const result = await zcql.executeZCQLQuery(`
      SELECT jobName, status
      FROM Jobs
      WHERE jobName LIKE 'EXPORT_ALL_*'
      ORDER BY ROWID DESC
      LIMIT ${limit}
    `);
    console.log(result);

    const jobs = result.map((row) => {
      const jobName = row.Jobs.jobName;
      const asOnDate = jobName.replace("EXPORT_ALL_", "");

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

    const jobName = `EXPORT_ALL_${asOnDate}`;
    const fileName = `all-clients-export-${asOnDate}.csv`;

    const job = await zcql.executeZCQLQuery(`
      SELECT status
      FROM Jobs
      WHERE jobName='${jobName}'
      LIMIT 1
    `);

    if (!job.length || job[0].Jobs.status !== "COMPLETED") {
      return res.status(400).json({
        status: "NOT_READY",
        message: "Export not completed yet",
      });
    }

    const stratus = catalystApp.stratus();
    const bucket = stratus.bucket("upload-data-bucket");

    // await bucket.getObjectDetails(fileName);

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
