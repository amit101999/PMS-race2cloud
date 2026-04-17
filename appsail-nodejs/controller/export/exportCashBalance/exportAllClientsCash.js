import { stratusSignedUrlToString, STRATUS_GET_URL_OPTS } from "../../../util/stratusSignedUrl.js";

const parseCatalystTime = (ct) => {
  if (!ct) return 0;
  const fixed = ct.replace(/:(\d{3})$/, ".$1");
  const ms = new Date(fixed).getTime();
  return Number.isNaN(ms) ? 0 : ms;
};

const esc = (s) => String(s ?? "").replace(/'/g, "''");

/** Job name + file for one as-on date (all clients cash snapshot). */
const jobKey = (dateStr) => `CC_${dateStr}`;
const fileKey = (dateStr) => `cash-all-${dateStr}.csv`;

export const exportAllClientsCash = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();
    const jobScheduling = catalystApp.jobScheduling();

    const { asOnDate } = req.query;
    if (!asOnDate || !/^\d{4}-\d{2}-\d{2}$/.test(asOnDate)) {
      return res.status(400).json({ message: "asOnDate is required (YYYY-MM-DD)" });
    }

    const dateStr = asOnDate;
    const jobName = jobKey(dateStr);
    const fileName = fileKey(dateStr);

    const existing = await zcql.executeZCQLQuery(
      `SELECT ROWID, status, CREATEDTIME FROM Jobs WHERE jobName = '${esc(jobName)}' LIMIT 1`
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
          jobName,
          fileName,
          asOnDate: dateStr,
          status: oldStatus,
          createdAt: createdTime
            ? new Date(parseCatalystTime(createdTime)).toISOString()
            : new Date().toISOString(),
          message: "Export is already in progress for this date",
        });
      }

      try {
        await zcql.executeZCQLQuery(`DELETE FROM Jobs WHERE ROWID = ${oldRowId}`);
      } catch (delErr) {
        console.error("Error deleting old cash-all job:", delErr);
      }

      try {
        const stratus = catalystApp.stratus();
        const bucket = stratus.bucket("upload-data-bucket");
        try {
          await bucket.deleteObject(fileName);
        } catch (_) {
          /* ignore */
        }
      } catch (stratusErr) {
        console.error("Error deleting old cash-all file:", stratusErr);
      }
    }

    await jobScheduling.JOB.submitJob({
      job_name: "ExportCashBalanceAll",
      jobpool_name: "Export",
      target_name: "ExportCashBalance",
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
      createdAt: new Date().toISOString(),
      message: "Export job started",
    });
  } catch (error) {
    console.error("exportAllClientsCash error:", error);
    return res.status(500).json({
      message: "Failed to schedule export job",
      error: error.message,
    });
  }
};

export const getAllClientsCashExportStatus = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();

    const { asOnDate } = req.query;
    if (!asOnDate || !/^\d{4}-\d{2}-\d{2}$/.test(asOnDate)) {
      return res.status(400).json({ message: "asOnDate is required (YYYY-MM-DD)" });
    }

    const jobName = jobKey(asOnDate);

    const result = await zcql.executeZCQLQuery(
      `SELECT ROWID, status, CREATEDTIME FROM Jobs WHERE jobName = '${esc(jobName)}' LIMIT 1`
    );

    if (!result.length) {
      return res.json({ status: "NOT_STARTED", jobName });
    }

    let status = result[0].Jobs.status;
    const createdTime = result[0].Jobs.CREATEDTIME;

    const STALE_TIMEOUT_MS = 60 * 60 * 1000;
    const jobAge = Date.now() - parseCatalystTime(createdTime);

    if ((status === "PENDING" || status === "RUNNING") && jobAge > STALE_TIMEOUT_MS) {
      try {
        await zcql.executeZCQLQuery(
          `UPDATE Jobs SET status = 'ERROR' WHERE jobName = '${esc(jobName)}'`
        );
      } catch (updateErr) {
        console.error("Failed to mark stale cash-all job as ERROR:", updateErr);
      }
      status = "ERROR";
    }

    return res.json({ jobName, status });
  } catch (error) {
    console.error("getAllClientsCashExportStatus error:", error);
    return res.status(500).json({ message: "Failed to fetch export job status" });
  }
};

export const downloadAllClientsCashExport = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();

    const { asOnDate } = req.query;
    if (!asOnDate || !/^\d{4}-\d{2}-\d{2}$/.test(asOnDate)) {
      return res.status(400).json({ message: "asOnDate is required (YYYY-MM-DD)" });
    }

    const jobName = jobKey(asOnDate);
    const fileName = fileKey(asOnDate);

    const job = await zcql.executeZCQLQuery(
      `SELECT status FROM Jobs WHERE jobName = '${esc(jobName)}' LIMIT 1`
    );

    if (!job.length || job[0].Jobs.status !== "COMPLETED") {
      return res.status(400).json({
        status: "NOT_READY",
        message: "Export not completed yet",
      });
    }

    const stratus = catalystApp.stratus();
    const bucket = stratus.bucket("upload-data-bucket");
    const rawUrl = await bucket.generatePreSignedUrl(fileName, "GET", STRATUS_GET_URL_OPTS);
    const urlStr = stratusSignedUrlToString(rawUrl);
    if (!urlStr) {
      return res.status(500).json({
        status: "ERROR",
        message: "Could not generate download URL",
      });
    }

    return res.json({
      status: "READY",
      fileName,
      downloadUrl: { signature: urlStr },
    });
  } catch (error) {
    console.error("downloadAllClientsCashExport error:", error);
    return res.status(404).json({
      status: "NOT_FOUND",
      message: "File not found or not ready yet",
    });
  }
};
