import { runFifoEngine } from "../../../util/analytics/transactionHistory/fifo.js";

const BATCH_SIZE = 270;

export const exportDividendPreviewFile = async (req, res) => {
  try {
    if (!req.catalystApp) {
      return res
        .status(500)
        .json({ success: false, message: "Catalyst not initialized" });
    }

    const { isin, exDate, rate, paymentDate } = req.query;
    const rateNum = Number(rate);

    if (!isin || !exDate || !paymentDate || !Number.isFinite(rateNum) || rateNum <= 0) {
      return res
        .status(400)
        .json({ success: false, message: "Invalid input: isin, exDate, rate, paymentDate required" });
    }

    const exDateObj = new Date(exDate);
    exDateObj.setHours(0, 0, 0, 0);
    const exDateISO = exDateObj.toISOString().split("T")[0];
    const paymentDateObj = new Date(paymentDate);
    paymentDateObj.setHours(0, 0, 0, 0);
    const paymentDateISO = paymentDateObj.toISOString().split("T")[0];

    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();
    const bucket = catalystApp.stratus().bucket("export-app-data");

    const UTF8_BOM = "\uFEFF";
    let csv = UTF8_BOM + "ISIN,ACCOUNT_CODE,HOLDING_EX_DATE,RATE,EX_DATE,PAYMENT_DATE,DIVIDEND_AMOUNT\n";

    /* Fetch holdings */
    const holdingRows = [];
    let holdOffset = 0;
    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT WS_Account_code
        FROM Holdings
        WHERE ISIN='${isin}' AND QTY > 0
        ORDER BY ROWID ASC
        LIMIT ${BATCH_SIZE} OFFSET ${holdOffset}
      `);
      if (!batch || batch.length === 0) break;
      holdingRows.push(...batch);
      if (batch.length < BATCH_SIZE) break;
      holdOffset += BATCH_SIZE;
    }

    if (!holdingRows.length) {
      return res.json({ success: false, message: "No eligible accounts" });
    }

    /* Fetch transactions (<= exDate), dedupe by ROWID */
    const txRows = [];
    const seenTxnRowIds = new Set();
    let txOffset = 0;
    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT *
        FROM Transaction
        WHERE ISIN='${isin}' AND TRANDATE <= '${exDateISO}'
        ORDER BY TRANDATE ASC, ROWID ASC
        LIMIT ${BATCH_SIZE} OFFSET ${txOffset}
      `);
      if (!batch || batch.length === 0) break;
      for (const row of batch) {
        const t = row.Transaction || row;
        const rowId = t.ROWID;
        if (rowId != null && seenTxnRowIds.has(rowId)) continue;
        if (rowId != null) seenTxnRowIds.add(rowId);
        txRows.push(row);
      }
      if (batch.length < BATCH_SIZE) break;
      txOffset += BATCH_SIZE;
    }

    /* Fetch bonuses (< exDate) */
    const bonusRows = [];
    let bonusOffset = 0;
    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT * FROM Bonus
        WHERE ISIN='${isin}' AND ExDate < '${exDateISO}'
        ORDER BY ExDate ASC, ROWID ASC
        LIMIT ${BATCH_SIZE} OFFSET ${bonusOffset}
      `);
      if (!batch || batch.length === 0) break;
      bonusRows.push(...batch);
      if (batch.length < BATCH_SIZE) break;
      bonusOffset += BATCH_SIZE;
    }

    /* Fetch splits (< exDate) */
    const splitRows = [];
    let splitOffset = 0;
    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT * FROM Split
        WHERE ISIN='${isin}' AND Issue_Date < '${exDateISO}'
        ORDER BY Issue_Date ASC, ROWID ASC
        LIMIT ${BATCH_SIZE} OFFSET ${splitOffset}
      `);
      if (!batch || batch.length === 0) break;
      splitRows.push(...batch);
      if (batch.length < BATCH_SIZE) break;
      splitOffset += BATCH_SIZE;
    }

    const txByAccount = {};
    txRows.forEach((r) => {
      const t = r.Transaction;
      if (!txByAccount[t.WS_Account_code]) txByAccount[t.WS_Account_code] = [];
      txByAccount[t.WS_Account_code].push(t);
    });
    const bonusByAccount = {};
    bonusRows.forEach((r) => {
      const b = r.Bonus;
      if (!bonusByAccount[b.WS_Account_code]) bonusByAccount[b.WS_Account_code] = [];
      bonusByAccount[b.WS_Account_code].push(b);
    });
    const splits = splitRows.map((r) => r.Split);

    const seenAccounts = new Set();
    for (const h of holdingRows) {
      const accountCode = h.Holdings.WS_Account_code;
      if (seenAccounts.has(accountCode)) continue;
      seenAccounts.add(accountCode);

      const transactions = txByAccount[accountCode] || [];
      if (!transactions.length) continue;

      const bonuses = bonusByAccount[accountCode] || [];
      const fifoResult = runFifoEngine(transactions, bonuses, splits, true);
      if (!fifoResult || fifoResult.holdings <= 0) continue;

      const holdingAsOnExDate = fifoResult.holdings;
      const dividendAmount = holdingAsOnExDate * rateNum;
      const exDateValue = exDateISO || "";
      csv += `"${isin}","${accountCode}","${holdingAsOnExDate}","${rateNum}","${exDateValue}","${paymentDateISO}","${dividendAmount}"\n`;
    }

    const fileName = `DividendExport_${isin}_${Date.now()}.csv`;
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
    console.error("EXPORT DIVIDEND PREVIEW ERROR:", err);
    return res.status(500).json({ success: false, message: err.message });
  }
};