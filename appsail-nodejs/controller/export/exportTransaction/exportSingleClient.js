import { PassThrough } from "stream";
import { applyCashEffect } from "../../analytics/tabs/transaction/transaction.js";

export const exportTransactionPerAccount = async (req, res) => {
  try {
    /* ================= VALIDATION ================= */
    const { accountCode, asOnDate, securityName } = req.query;

    if (!accountCode) {
      return res.status(400).json({
        message: "accountCode is required for transaction export",
      });
    }

    /* ================= INIT ================= */
    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();

    const stratus = catalystApp.stratus();
    const bucket = stratus.bucket("export-app-data");

    /* ================= CREATE CSV STREAM ================= */
    const csvStream = new PassThrough();
    const fileName = `transaction-export-${accountCode}-${Date.now()}.csv`;

    const uploadPromise = bucket.putObject(fileName, csvStream, {
      overwrite: true,
      contentType: "text/csv",
    });

    /* ================= CSV HEADER ================= */
    csvStream.write(
      "DATE,TYPE,SECURITY_NAME,SECURITY_CODE,ISIN,QUANTITY,PRICE,TOTAL_AMOUNT,STT,CASH_BALANCE\n",
    );

    /* ================= WHERE CLAUSE ================= */
    const clauses = [`WS_Account_code = '${accountCode.replace(/'/g, "''")}'`];

    if (securityName) {
      clauses.push(`Security_Name = '${securityName.replace(/'/g, "''")}'`);
    }

    if (asOnDate) {
      const nextDay = new Date(asOnDate);
      nextDay.setDate(nextDay.getDate() + 1);
      const nextDayStr = nextDay.toISOString().split("T")[0];
      clauses.push(`TRANDATE < '${nextDayStr}'`);
    }

    const whereSql = `WHERE ${clauses.join(" AND ")}`;

    /* ================= OPENING CASH BALANCE ================= */
    const BATCH_SIZE = 300;
    let openingBalance = 0;
    let offset = 0;

    while (true) {
      const rows = await zcql.executeZCQLQuery(`
        SELECT Tran_Type, Net_Amount, STT
        FROM Transaction
        ${whereSql}
        ORDER BY TRANDATE ASC, executionPriority ASC, ROWID ASC
        LIMIT ${BATCH_SIZE} OFFSET ${offset}
      `);

      if (!rows.length) break;

      for (const r of rows) {
        const t = r.Transaction || r;
        const stt = Number(t.STT ?? t.Stt ?? 0) || 0;
        openingBalance = applyCashEffect(
          openingBalance,
          t.Tran_Type,
          Number(t.Net_Amount) || 0,
        );
        openingBalance -= stt;
      }

      if (rows.length < BATCH_SIZE) break;
      offset += BATCH_SIZE;
    }

    /* ================= FETCH FULL TRANSACTIONS ================= */
    const EXPORT_BATCH_SIZE = 300;
    let exportOffset = 0;
    let runningBalance = openingBalance;
    let totalExported = 0;
    while (true) {
      const rows = await zcql.executeZCQLQuery(`
        SELECT
          TRANDATE,
          Tran_Type,
          Security_Name,
          Security_code,
          ISIN,
          QTY,
          NETRATE,
          Net_Amount,
          STT
        FROM Transaction
        ${whereSql}
        ORDER BY TRANDATE ASC, executionPriority ASC, ROWID ASC
        LIMIT ${EXPORT_BATCH_SIZE} OFFSET ${exportOffset}
      `);

      if (!rows.length) break;

      for (const r of rows) {
        const t = r.Transaction || r;
        const stt = Number(t.STT ?? t.Stt ?? 0) || 0;
        runningBalance = applyCashEffect(
          runningBalance,
          t.Tran_Type,
          Number(t.Net_Amount) || 0,
        );
        runningBalance -= stt;
        const line = [
          t.TRANDATE,
          t.Tran_Type,
          t.Security_Name,
          t.Security_code,
          t.ISIN,
          Number(t.QTY) || 0,
          Number(t.NETRATE) || 0,
          Number(t.Net_Amount) || 0,
          stt,
          runningBalance,
        ]
          .map((v) => `"${String(v ?? "").replace(/"/g, '""')}"`)
          .join(",");

        csvStream.write(line + "\n");
        totalExported++;
      }

      if (rows.length < EXPORT_BATCH_SIZE) break;
      exportOffset += EXPORT_BATCH_SIZE;

    }
    if (totalExported === 0) {
      csvStream.end();
      await uploadPromise;
      return res.status(404).json({
        message: `No transactions found for accountCode ${accountCode}`,
      });
    }

    csvStream.end();
    await uploadPromise;

    const downloadUrl = await bucket.generatePreSignedUrl(fileName, "GET", {
      expiresIn: 3600,
    });

    return res.status(200).json({
      message: "Transaction export successful",
      downloadUrl,
    });
  } catch (error) {
    console.error("Transaction export error:", error);
    return res.status(500).json({
      message: "Internal Server Error",
      error: error.message,
    });
  }
};
