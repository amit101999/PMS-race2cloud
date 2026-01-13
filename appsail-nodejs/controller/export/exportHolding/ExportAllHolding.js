export const exportAllData = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const { asOnDate } = req.query;

    const zcql = catalystApp.zcql();
    const jobScheduling = catalystApp.jobScheduling();

    const dateStr = asOnDate || new Date().toISOString().split("T")[0];

    const jobName = `EXPORT_ALL_${dateStr}`;
    const fileName = `all-clients-export-${dateStr}.csv`;

    // 🔍 Check if job already exists for this date
    const existingJob = await zcql.executeZCQLQuery(`
      SELECT ROWID, status 
      FROM Jobs 
      WHERE jobName='${jobName}'
      LIMIT 1
    `);

    if (existingJob.length) {
      return res.json({
        jobName,
        fileName,
        status: existingJob[0].Jobs.status,
      });
    }

    const job = await jobScheduling.JOB.submitJob({
      job_name: "ExportAll",
      jobpool_name: "Export",
      target_name: "ExportAllCustomerHoldingData",
      target_type: "Function",
      job_details: {
        asOnDate: dateStr,
        jobName,
        fileName,
      },
    });

    // 📝 Insert into Jobs table
    await zcql.executeZCQLQuery(`
      INSERT INTO Jobs (JobIds, jobName, status)
      VALUES (${job.job_id}, '${jobName}', 'PENDING')
    `);

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

    const today = new Date().toISOString().split("T")[0];
    const jobName = `EXPORT_ALL_${today}`;

    const result = await zcql.executeZCQLQuery(`
      SELECT jobName, status 
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
      fileName: "all-clients-export.csv",
    });
  } catch (error) {
    console.error("Error fetching export job status:", error);
    return res.status(500).json({
      message: "Failed to fetch export job status",
    });
  }
};

export const downloadExportFile = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const { fileName } = req.query;

    if (!fileName) {
      return res.status(400).json({
        message: "fileName is required",
      });
    }

    const stratus = catalystApp.stratus();
    const bucket = stratus.bucket("upload-data-bucket");

    await bucket.getObjectDetails(fileName);

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
