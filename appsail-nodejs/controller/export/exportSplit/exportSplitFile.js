import { runFifoEngine } from "../../../util/analytics/transactionHistory/fifo.js";

const BATCH_SIZE = 200;

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

    /* ================= CSV HEADER ================= */
    let csv =
      "ISIN,ACCOUNT_CODE,CURRENT_HOLDING,NEW_HOLDING,DELTA\n";

    /* ================= FETCH HOLDINGS ================= */
    const holdings = [];
    let offset = 0;

    while (true) {
      const rows = await zcql.executeZCQLQuery(`
        SELECT WS_Account_code, QTY
        FROM Holdings
        WHERE ISIN='${isin}' AND QTY > 0
        LIMIT ${BATCH_SIZE} OFFSET ${offset}
      `);

      if (!rows || rows.length === 0) break;
      holdings.push(...rows);
      if (rows.length < BATCH_SIZE) break;
      offset += BATCH_SIZE;
    }

    if (!holdings.length) {
      return res.json({
        success: false,
        message: "No eligible accounts",
      });
    }

    /* ================= FETCH TX / BONUS / PAST SPLITS ================= */
    const fetchAll = async (table, dateCol, operator) => {
      const out = [];
      let off = 0;

      while (true) {
        const r = await zcql.executeZCQLQuery(`
          SELECT *
          FROM ${table}
          WHERE ISIN='${isin}' AND ${dateCol} ${operator} '${issueDateISO}'
          LIMIT ${BATCH_SIZE} OFFSET ${off}
        `);

        if (!r || r.length === 0) break;
        out.push(...r);
        if (r.length < BATCH_SIZE) break;
        off += BATCH_SIZE;
      }
      return out;
    };

    const txRows = await fetchAll("Transaction", "SETDATE", "<=");
    const bonusRows = await fetchAll("Bonus", "ExDate", "<=");
    const splitRows = await fetchAll("Split", "Issue_Date", "<"); // past splits only

    /* ================= GROUP DATA ================= */
    const txByAcc = {};
    txRows.forEach((r) => {
      const t = r.Transaction;
      (txByAcc[t.WS_Account_code] ||= []).push(t);
    });

    const bonusByAcc = {};
    bonusRows.forEach((r) => {
      const b = r.Bonus;
      (bonusByAcc[b.WS_Account_code] ||= []).push(b);
    });

    const pastSplits = splitRows.map((r) => r.Split);

    /* ================= FIFO + CSV ================= */
    const splitMultiplier = r2 / r1;

    for (const h of holdings) {
      const acc = h.Holdings.WS_Account_code;
      const tx = txByAcc[acc] || [];
      if (!tx.length) continue;

      const bonuses = bonusByAcc[acc] || [];

      // FIFO BEFORE split (same as preview)
      const before = runFifoEngine(tx, bonuses, pastSplits, true);
      if (!before || before.holdings <= 0) continue;

      // APPLY SPLIT (same logic as preview)
      const newHolding = Math.floor(before.holdings * splitMultiplier);
      const delta = newHolding - before.holdings;

      csv += `"${isin}","${acc}","${before.holdings}","${newHolding}","${delta}"\n`;
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
