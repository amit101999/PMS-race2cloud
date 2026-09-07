import { computeSplitPreviewRows } from "../../../util/analytics/splitPreviewCalc.js";

export const exportSplitPreviewFile = async (req, res) => {
  try {
    if (!req.catalystApp) {
      return res
        .status(500)
        .json({ success: false, message: "Catalyst not initialized" });
    }

    const { isin, ratio1, ratio2, issueDate } = req.query;
    const r1 = Number(ratio1);
    const r2 = Number(ratio2);

    if (!isin || !issueDate || r1 <= 0 || r2 <= 0) {
      return res
        .status(400)
        .json({ success: false, message: "Invalid input" });
    }

    const issueDateISO = new Date(issueDate).toISOString().split("T")[0];

    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();
    const bucket = catalystApp.stratus().bucket("export-app-data");

    /* ================= CALCULATE (same fast path as /split/preview) =================
       One paginated Holdings query for the whole ISIN — no per-account
       fan-out. The previous implementation re-queried Holdings once per
       eligible account, which was fast locally but blew past Catalyst's
       ~30s request limit on deployed environments (408 Request Timeout). */
    const preview = await computeSplitPreviewRows(zcql, {
      isin,
      r1,
      r2,
      issueDateISO,
    });

    if (!preview.length) {
      return res.json({ success: false, message: "No eligible accounts" });
    }

    let csv =
      "ISIN,VIRTUAL_CODE,ACTUAL_CODE,CURRENT_HOLDING,NEW_HOLDING,DELTA\n";
    for (const row of preview) {
      csv += `"${row.isin}","${row.accountCode}","${row.actualCode}","${row.currentHolding}","${row.newHolding}","${row.delta}"\n`;
    }

    /* ================= UPLOAD ================= */
    const fileName = `SplitExport_${isin}_${Date.now()}.csv`;
    await bucket.putObject(fileName, Buffer.from(csv), {
      overwrite: true,
      contentType: "text/csv",
    });

    const signedUrl = await bucket.generatePreSignedUrl(fileName, "GET", {
      expiresIn: 3600,
    });

    return res.json({
      success: true,
      downloadUrl: { signature: signedUrl },
    });

  } catch (err) {
    console.error("EXPORT SPLIT PREVIEW ERROR:", err);
    return res
      .status(500)
      .json({ success: false, message: err.message });
  }
};
