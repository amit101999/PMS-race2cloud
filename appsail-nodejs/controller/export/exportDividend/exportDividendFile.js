import { runFifoEngine } from "../../../util/analytics/transactionHistory/fifo.js";

const BATCH_SIZE = 270;

const parseCatalystTime = (ct) => {
  if (!ct) return 0;
  const fixed = ct.replace(/:(\d{3})$/, ".$1");
  const ms = new Date(fixed).getTime();
  return isNaN(ms) ? 0 : ms;
};

/**
 * Schedule dividend export as a background job
 */
export const exportDividendPreviewFile = async (req, res) => {
  try {
    if (!req.catalystApp) {
      return res
        .status(500)
        .json({ success: false, message: "Catalyst not initialized" });
    }

    const { isin, exDate, recordDate, rate, paymentDate } = req.query;
    const rateNum = Number(rate);

    if (!isin || !recordDate || !paymentDate || !Number.isFinite(rateNum) || rateNum <= 0) {
      return res
        .status(400)
        .json({ success: false, message: "Invalid input: isin, recordDate, rate, paymentDate required" });
    }

    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();
    const jobScheduling = catalystApp.jobScheduling();

    const jobName = `DIV_${isin}_${Date.now()}`;
    const fileName = `DividendExport_${isin}_${Date.now()}.csv`;

    const existing = await zcql.executeZCQLQuery(
      `SELECT ROWID, status, CREATEDTIME FROM Jobs WHERE jobName LIKE 'DIV_${isin}_*' ORDER BY ROWID DESC LIMIT 1`
    );

    if (existing.length) {
      const oldStatus = existing[0].Jobs.status;
      const oldRowId = existing[0].Jobs.ROWID;
      const createdTime = existing[0].Jobs.CREATEDTIME;

      const STALE_TIMEOUT_MS = 60 * 60 * 1000;
      const jobAge = Date.now() - parseCatalystTime(createdTime);
      const isStale = jobAge > STALE_TIMEOUT_MS;

      if ((oldStatus === "PENDING" || oldStatus === "RUNNING") && !isStale) {
        return res.json({
          success: true,
          jobName: existing[0].Jobs.jobName || jobName,
          status: oldStatus,
          message: "Dividend export is already in progress for this ISIN",
        });
      }

      try {
        await zcql.executeZCQLQuery(`DELETE FROM Jobs WHERE ROWID = ${oldRowId}`);
      } catch (delErr) {
        console.error("Error deleting old dividend job:", delErr);
      }
    }

    await jobScheduling.JOB.submitJob({
      job_name: "ExportDividend",
      jobpool_name: "Export",
      target_name: "ExportDividendAccounts",
      target_type: "Function",
      params: {
        isin,
        exDate: exDate || "",
        recordDate,
        rate: String(rateNum),
        paymentDate,
        jobName,
        fileName,
      },
    });

    return res.json({
      success: true,
      jobName,
      fileName,
      status: "PENDING",
      message: "Dividend export job started",
    });
  } catch (err) {
    console.error("EXPORT DIVIDEND SCHEDULE ERROR:", err);
    return res.status(500).json({ success: false, message: err.message });
  }
};

/**
 * Check dividend export job status
 */
export const getDividendExportStatus = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();
    const { jobName } = req.query;

    if (!jobName) {
      return res.status(400).json({ success: false, message: "jobName is required" });
    }

    const result = await zcql.executeZCQLQuery(
      `SELECT ROWID, status, CREATEDTIME FROM Jobs WHERE jobName = '${jobName}' LIMIT 1`
    );

    if (!result.length) {
      return res.json({ success: true, status: "NOT_STARTED" });
    }

    let status = result[0].Jobs.status;
    const createdTime = result[0].Jobs.CREATEDTIME;

    const STALE_TIMEOUT_MS = 60 * 60 * 1000;
    const jobAge = Date.now() - parseCatalystTime(createdTime);

    if ((status === "PENDING" || status === "RUNNING") && jobAge > STALE_TIMEOUT_MS) {
      try {
        await zcql.executeZCQLQuery(
          `UPDATE Jobs SET status = 'ERROR' WHERE jobName = '${jobName}'`
        );
      } catch (updateErr) {
        console.error("Failed to mark stale dividend job as ERROR:", updateErr);
      }
      status = "ERROR";
    }

    return res.json({ success: true, jobName, status });
  } catch (error) {
    console.error("Error fetching dividend export job status:", error);
    return res.status(500).json({ success: false, message: "Failed to fetch job status" });
  }
};

/**
 * Download completed dividend export file
 */
export const downloadDividendExportFile = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();
    const { jobName, fileName } = req.query;

    if (!jobName || !fileName) {
      return res.status(400).json({ success: false, message: "jobName and fileName are required" });
    }

    const job = await zcql.executeZCQLQuery(
      `SELECT status FROM Jobs WHERE jobName = '${jobName}' LIMIT 1`
    );

    if (!job.length || job[0].Jobs.status !== "COMPLETED") {
      return res.status(400).json({
        success: false,
        status: "NOT_READY",
        message: "Export not completed yet",
      });
    }

    const stratus = catalystApp.stratus();
    const bucket = stratus.bucket("export-app-data");

    const signedUrl = await bucket.generatePreSignedUrl(fileName, "GET", {
      expiresIn: 3600,
    });

    return res.json({
      success: true,
      downloadUrl: { signature: signedUrl },
    });
  } catch (error) {
    console.error("Error downloading dividend export file:", error);
    return res.status(404).json({
      success: false,
      status: "NOT_FOUND",
      message: "File not found or not ready yet",
    });
  }
};
