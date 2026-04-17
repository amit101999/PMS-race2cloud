import { stratusSignedUrlToString, STRATUS_GET_URL_OPTS } from "../../util/stratusSignedUrl.js";

const esc = (s) => String(s ?? "").replace(/'/g, "''");

const parseCatalystTime = (ct) => {
  if (!ct) return 0;
  const fixed = ct.replace(/:(\d{3})$/, ".$1");
  const ms = new Date(fixed).getTime();
  return isNaN(ms) ? 0 : ms;
};

const STALE_MS = 60 * 60 * 1000;

export const triggerCashBalExport = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();
    const jobScheduling = catalystApp.jobScheduling();

    const { accountCode, isin, fromDate, toDate } = req.query;
    if (!accountCode) {
      return res.status(400).json({ message: "accountCode is required" });
    }

    const ts = Date.now();
    const jobName = `CB_${accountCode}_${ts}`;
    const fileName = `cash-bal-${accountCode}-${ts}.csv`;

    const existing = await zcql.executeZCQLQuery(
      `SELECT ROWID, status, CREATEDTIME FROM Jobs WHERE jobName LIKE 'CB_${esc(accountCode)}_*' ORDER BY ROWID DESC LIMIT 1`
    );

    if (existing.length) {
      const old = existing[0].Jobs;
      const jobAge = Date.now() - parseCatalystTime(old.CREATEDTIME);
      if ((old.status === "PENDING" || old.status === "RUNNING") && jobAge < STALE_MS) {
        return res.json({
          jobName: old.jobName || jobName,
          status: old.status,
          message: "Export already in progress for this account",
        });
      }
    }

    await jobScheduling.JOB.submitJob({
      job_name: "ExportCashBal",
      jobpool_name: "Export",
      target_name: "ExportCashBalSingleClient",
      target_type: "Function",
      params: {
        accountCode,
        isin: isin || "",
        fromDate: fromDate || "",
        toDate: toDate || "",
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
    console.error("triggerCashBalExport error:", error);
    return res.status(500).json({ message: "Failed to start export", error: error.message });
  }
};

export const getCashBalExportHistory = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();
    const limit = Math.min(Math.max(Number(req.query.limit) || 10, 1), 50);

    let cbRows = [];
    let ccRows = [];
    try {
      cbRows = await zcql.executeZCQLQuery(
        `SELECT jobName, status, CREATEDTIME FROM Jobs WHERE jobName LIKE 'CB_*' ORDER BY ROWID DESC LIMIT ${limit}`
      );
    } catch (e) {
      console.error("getCashBalExportHistory CB query:", e);
    }
    try {
      ccRows = await zcql.executeZCQLQuery(
        `SELECT jobName, status, CREATEDTIME FROM Jobs WHERE jobName LIKE 'CC_*' ORDER BY ROWID DESC LIMIT ${limit}`
      );
    } catch (e) {
      console.error("getCashBalExportHistory CC query:", e);
    }

    const now = Date.now();
    const jobs = [];

    for (const row of [...(cbRows || []), ...(ccRows || [])]) {
      const j = row.Jobs || row;
      const jobName = j.jobName || "";
      let status = j.status || "UNKNOWN";
      const createdAtMs = parseCatalystTime(j.CREATEDTIME);
      const jobAge = now - createdAtMs;

      if ((status === "PENDING" || status === "RUNNING") && jobAge > STALE_MS) {
        try {
          await zcql.executeZCQLQuery(`UPDATE Jobs SET status = 'ERROR' WHERE jobName = '${esc(jobName)}'`);
        } catch (_) {
          /* ignore */
        }
        status = "ERROR";
      }

      const isAllClients = jobName.startsWith("CC_");
      const parts = jobName.split("_");
      const accountCode =
        !isAllClients && parts.length >= 3 ? parts.slice(1, -1).join("_") : "";
      const asOnDate = isAllClients && jobName.length > 3 ? jobName.slice(3) : undefined;

      jobs.push({
        jobName,
        accountCode,
        ...(asOnDate ? { asOnDate } : {}),
        status,
        createdAt:
          createdAtMs > 0 ? new Date(createdAtMs).toISOString() : new Date().toISOString(),
        _sortMs: createdAtMs,
      });
    }

    jobs.sort((a, b) => b._sortMs - a._sortMs);
    const limited = jobs.slice(0, limit).map(({ _sortMs, ...rest }) => rest);

    return res.json(limited);
  } catch (error) {
    console.error("getCashBalExportHistory error:", error);
    return res.status(500).json({ message: "Failed to fetch export history", error: error.message });
  }
};

export const getCashBalExportStatus = async (req, res) => {
  try {
    const zcql = req.catalystApp.zcql();
    const { jobName } = req.query;
    if (!jobName) return res.status(400).json({ message: "jobName is required" });

    const result = await zcql.executeZCQLQuery(
      `SELECT status, CREATEDTIME FROM Jobs WHERE jobName = '${esc(jobName)}' LIMIT 1`
    );

    if (!result.length) return res.json({ status: "NOT_FOUND" });

    let status = result[0].Jobs.status;
    const jobAge = Date.now() - parseCatalystTime(result[0].Jobs.CREATEDTIME);

    if ((status === "PENDING" || status === "RUNNING") && jobAge > STALE_MS) {
      try {
        await zcql.executeZCQLQuery(`UPDATE Jobs SET status = 'ERROR' WHERE jobName = '${esc(jobName)}'`);
      } catch (_) {}
      status = "ERROR";
    }

    return res.json({ jobName, status });
  } catch (error) {
    console.error("getCashBalExportStatus error:", error);
    return res.status(500).json({ message: "Failed to check status", error: error.message });
  }
};

export const downloadCashBalExport = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();
    const { jobName } = req.query;
    if (!jobName) return res.status(400).json({ message: "jobName is required" });

    const job = await zcql.executeZCQLQuery(
      `SELECT status FROM Jobs WHERE jobName = '${esc(jobName)}' LIMIT 1`
    );

    if (!job.length || job[0].Jobs.status !== "COMPLETED") {
      return res.status(400).json({ status: "NOT_READY", message: "Export not completed yet" });
    }

    const parts = jobName.split("_");
    const accountCode = parts[1] || "";
    const ts = parts[2] || "";
    const fileName = `cash-bal-${accountCode}-${ts}.csv`;

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

    return res.json({ status: "READY", fileName, downloadUrl: { signature: urlStr } });
  } catch (error) {
    console.error("downloadCashBalExport error:", error);
    return res.status(404).json({ status: "NOT_FOUND", message: "File not found" });
  }
};
