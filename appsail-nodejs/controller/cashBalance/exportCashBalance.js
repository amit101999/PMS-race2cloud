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
    const signedUrl = await bucket.generatePreSignedUrl(fileName, "GET", { expiresIn: 3600 });

    return res.json({ status: "READY", fileName, downloadUrl: { signature: signedUrl } });
  } catch (error) {
    console.error("downloadCashBalExport error:", error);
    return res.status(404).json({ status: "NOT_FOUND", message: "File not found" });
  }
};
