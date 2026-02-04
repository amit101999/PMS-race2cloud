import { PassThrough, Readable } from "stream";
import csv from "csv-parser";
import { runFifoEngine } from "../../util/analytics/transactionHistory/fifo.js";

const runPerRowSimulation = ({
  dbTransactions,
  fileTransactions,
  bonuses,
  splits,
}) => {
  const results = [];

  // Start with DB-only transactions
  let currentTransactions = [...dbTransactions];

  // Initial FIFO (before any file rows)
  let prevFifo = runFifoEngine(
    currentTransactions,
    bonuses,
    splits,
    true
  );

  for (const row of fileTransactions) {
    const beforeHolding = prevFifo?.holdings || 0;

    // Apply ONE row
    currentTransactions.push(row);

    const afterFifo = runFifoEngine(
      currentTransactions,
      bonuses,
      splits,
      true
    );

    const afterHolding = afterFifo?.holdings || 0;

    results.push({
      WS_Account_code: row.WS_Account_code,
      ISIN: row.ISIN,
      Security_code: row.Security_code ?? "",
      Security_Name: row.Security_Name ?? "",
      Tran_Type: row.Tran_Type,
      transactionQty: row.QTY,
      oldQuantity: beforeHolding,
      newQuantity: afterHolding,
    });

    // move state forward
    prevFifo = afterFifo;
  }

  return results;
};

const fetchTransactionsPaginated = async (zcql, isin, accountCode) => {
  const batchSize = 250;
  let offset = 0;

  const results = [];
  const seenRowIds = new Set();

  while (true) {
    const rows = await zcql.executeZCQLQuery(`
      SELECT ROWID, Security_Name, Security_code, Tran_Type,
             QTY, TRANDATE, NETRATE, ISIN
      FROM Transaction
      WHERE ISIN = '${isin}'
        AND WS_Account_code = '${accountCode}'
      ORDER BY ROWID ASC
      LIMIT ${batchSize} OFFSET ${offset}
    `);

    if (!rows.length) break;

    for (const r of rows) {
      const t = r.Transaction || r;

      if (!t?.ROWID) continue;
      if (seenRowIds.has(t.ROWID)) continue; // 🚫 duplicate

      seenRowIds.add(t.ROWID);
      results.push(t);
    }

    if (rows.length < batchSize) break;
    offset += batchSize;
  }

  return results;
};

const fetchBonusPaginated = async (zcql, isin, accountCode) => {
  const batchSize = 250;
  let offset = 0;

  const results = [];
  const seenRowIds = new Set();

  while (true) {
    const rows = await zcql.executeZCQLQuery(`
      SELECT ROWID, BonusShare, ExDate, ISIN
      FROM Bonus
      WHERE ISIN = '${isin}'
        AND WS_Account_code = '${accountCode}'
      ORDER BY ROWID ASC
      LIMIT ${batchSize} OFFSET ${offset}
    `);

    if (!rows.length) break;

    for (const r of rows) {
      const b = r.Bonus || r;
      if (!b?.ROWID) continue;
      if (seenRowIds.has(b.ROWID)) continue;

      seenRowIds.add(b.ROWID);
      results.push(b);
    }

    if (rows.length < batchSize) break;
    offset += batchSize;
  }

  return results;
};

const fetchSplitPaginated = async (zcql, isin) => {
  const batchSize = 250;
  let offset = 0;

  const results = [];
  const seenRowIds = new Set();

  while (true) {
    const rows = await zcql.executeZCQLQuery(`
      SELECT ROWID, Issue_Date, Ratio1, Ratio2, ISIN
      FROM Split
      WHERE ISIN = '${isin}'
      ORDER BY ROWID ASC
      LIMIT ${batchSize} OFFSET ${offset}
    `);

    if (!rows.length) break;

    for (const r of rows) {
      const s = r.Split || r;
      if (!s?.ROWID) continue;
      if (seenRowIds.has(s.ROWID)) continue;

      seenRowIds.add(s.ROWID);
      results.push({
        issueDate: s.Issue_Date,
        ratio1: s.Ratio1,
        ratio2: s.Ratio2,
        isin: s.ISIN,
      });
    }

    if (rows.length < batchSize) break;
    offset += batchSize;
  }

  return results;
};


const normalizeFileTransactions = (parsedRows) => {
  const seen = new Set();
  const result = [];

  for (const r of parsedRows) {
    const key = [
      r.WS_Account_code,
      r.ISIN,
      r.TRANDATE,
      r.Tran_Type,
      r.QTY,
      r.NETRATE
    ].join("|");

    if (seen.has(key)) continue; // 🚫 duplicate CSV row
    seen.add(key);

    result.push({
      WS_Account_code: r.WS_Account_code,
      ISIN: r.ISIN,
      Security_Name: r.Security_Name,
      Security_code: r.Security_code,
      Tran_Type: r.Tran_Type,
      QTY: Number(r.QTY),
      // TRANDATE: r.TRANDATE,
      TRANDATE:  (() => {
        const raw = r.TRANDATE;
        if (raw == null || raw === "") return "";
        if (typeof raw === "string" && /^\d{4}-\d{2}-\d{2}/.test(raw.trim())) return raw.trim().substring(0, 10);
        const d = new Date(raw);
        return !isNaN(d.getTime()) ? d.toISOString().split("T")[0] : "";
      })(),
      NETRATE: Number(r.NETRATE || 0),
    });
  }

  return result;
};  


const getHoldingValues = async (
  zcql,
  isin,
  accountCode,
  fileTransactions = []
) => {
  const dbTransactions = await fetchTransactionsPaginated(
    zcql,
    isin,
    accountCode
  );

  const bonuses = await fetchBonusPaginated(
    zcql,
    isin,
    accountCode
  );

  const splits = await fetchSplitPaginated(
    zcql,
    isin
  );

  // 🚨 Merge DB + file txns
  const allTransactions = [...dbTransactions, ...fileTransactions];

  return runPerRowSimulation({
    dbTransactions,
    fileTransactions,
    bonuses,
    splits,
  });
};


export const simulateHoldings = async (req, res) => {
  try {
    const file = req.files?.file;
    const catalystApp = req.catalystApp;
const zcql = catalystApp.zcql();

    /* ---------- validations ---------- */
    if (!file) {
      return res.status(400).json({
        success: false,
        message: "CSV file is required (field name: file)",
      });
    }

    if (!file.name.toLowerCase().endsWith(".csv")) {
      return res.status(400).json({
        success: false,
        message: "Only CSV files are allowed",
      });
    }

    if (!Buffer.isBuffer(file.data)) {
      return res.status(400).json({
        success: false,
        message: "Invalid file buffer",
      });
    }

    /* ---------- parse CSV ---------- */
    const parsedRows = await parseCSVFromBuffer(file.data);

    const fileTxns = normalizeFileTransactions(parsedRows);

    const grouped = {};
    for (const tx of fileTxns) {
      const key = `${tx.WS_Account_code}__${tx.ISIN}`;
      if (!grouped[key]) {
        grouped[key] = {
          accountCode: tx.WS_Account_code,
          isin: tx.ISIN,
          fileTransactions: [],
        };
      }
      grouped[key].fileTransactions.push(tx);
    }

    const data = [];

    for (const key of Object.keys(grouped)) {
      const { accountCode, isin, fileTransactions } = grouped[key];

      const rowWiseResult = await getHoldingValues(
        zcql,
        isin,
        accountCode,
        fileTransactions
      );

      data.push(...rowWiseResult);
    }

    /* ---------- response: only WS_Account_code, ISIN, Security_code, Security_Name, Tran_Type, oldQuantity, newQuantity ---------- */
    return res.status(200).json({
      success: true,
      message: "Holdings simulated successfully",
      count: data.length,
      data,
    });

  } catch (error) {
    console.error("simulateHoldings error:", error);
    return res.status(500).json({
      success: false,
      message: "Simulation failed",
      error: error.message,
    });
  }
};

const parseCSVFromBuffer = (buffer) => {
  return new Promise((resolve, reject) => {
    const results = [];

    Readable.from(buffer)
      .pipe(
        csv({
          mapHeaders: ({ header }) =>
            header?.trim(), // remove extra spaces
        })
      )
      .on("data", (row) => {
        results.push(row);
      })
      .on("end", () => {
        resolve(results);
      })
      .on("error", (err) => {
        reject(err);
      });
  });
};

/**
 * Reduce simulated holdings to one row per (UCC, ISIN) with latest newQuantity.
 * Rows are in transaction order, so last row per key = final quantity.
 */
const latestQuantityPerAccountIsin = (simulatedHoldings = []) => {
  const byKey = {};
  for (const row of simulatedHoldings) {
    const ucc = row.WS_Account_code ?? "";
    const isin = row.ISIN ?? "";
    const key = `${ucc}__${isin}`;
    byKey[key] = {
      UCC: ucc,
      ISIN: isin,
      newQuantity: Number(row.newQuantity) || 0,
      Security_Name: row.Security_Name ?? "",
    };
  }
  return byKey;
};

/**
 * Custodian CSV: reuse CSV parser.
 */
const parseCustodianFromBuffer = (buffer) => {
  return parseCSVFromBuffer(buffer);
};

/**
 * Build comparison rows: match by UCC + ISIN.
 * - Qty FA = newQuantity from our simulation
 * - Qty Cust = NetBalance from custodian
 * - Report Date = today; Holding Date = from custodian (LTPDate) or empty
 * - Scheme Code = empty for now
 */
const buildComparisonData = (simulatedHoldings = [], custodianRows = []) => {
  const ourByKey = latestQuantityPerAccountIsin(simulatedHoldings);
  const reportDate = new Date().toISOString().split("T")[0];
  const addedTime = new Date().toISOString();

  const custodianByKey = {};
  for (const r of custodianRows) {
    const ucc = (r.UCC ?? "").toString().trim();
    const isin = (r.ISIN ?? "").toString().trim();
    if (!ucc && !isin) continue;
    const key = `${ucc}__${isin}`;
    custodianByKey[key] = {
      UCC: ucc,
      ISIN: isin,
      NetBalance: parseFloat(r.NetBalance) || 0,
      ClientName: (r.ClientName ?? r["Client Name"] ?? "").toString().trim(),
      SecurityName: (r.SecurityName ?? "").toString().trim(),
      LTPDate: (r.LTPDate ?? "").toString().trim(),
    };
  }

  const allKeys = new Set([
    ...Object.keys(ourByKey),
    ...Object.keys(custodianByKey),
  ]);

  const rows = [];
  for (const key of allKeys) {
    const our = ourByKey[key];
    const cust = custodianByKey[key];
    const ucc = our?.UCC ?? cust?.UCC ?? "";
    const isin = our?.ISIN ?? cust?.ISIN ?? "";
    const qtyFA = our?.newQuantity ?? 0;
    const qtyCust = cust?.NetBalance ?? 0;
    const diff = qtyFA - qtyCust;
    const matching = diff === 0 ? "Yes" : "No";
    let mismatchReason = "";
    if (diff !== 0) mismatchReason = "Qty Mismatch";
    if (!our && cust) mismatchReason = "Only in Custodian";
    if (our && !cust) mismatchReason = "Only in FA";

    rows.push({
      "Report Date": reportDate,
      "Holding Date": cust?.LTPDate ?? "",
      UCC: ucc,
      ISIN: isin,
      "Qty Cust": qtyCust,
      "Qty FA": qtyFA,
      Diff: diff,
      "Mismatch Reason": mismatchReason,
      Matching: matching,
      Security: our?.Security_Name ?? cust?.SecurityName ?? "",
      "Client name": cust?.ClientName ?? "",
      "Added Time": addedTime,
      Comments: "",
      "Scheme Code": "",
      "LTP Date": cust?.LTPDate ?? "",
    });
  }

  return rows;
};

/**
 * POST /transaction-uploader/compare-custodian
 * Body: { simulatedHoldings: Array } (from /parse-data response data)
 * File: custodian CSV (field name: file)
 */
export const compareWithCustodian = async (req, res) => {
  try {
    const file = req.files?.file;
    let simulatedHoldings = req.body?.simulatedHoldings;
    console.log("simulatedHoldings count:", Array.isArray(simulatedHoldings) ? simulatedHoldings.length : "not array");

    if (!file) {
      return res.status(400).json({
        success: false,
        message: "Custodian CSV file is required (field name: file)",
      });
    }

    if (!file.name || !file.name.toLowerCase().endsWith(".csv")) {
      return res.status(400).json({
        success: false,
        message: "Only CSV files are allowed",
      });
    }

    if (!Buffer.isBuffer(file.data)) {
      return res.status(400).json({
        success: false,
        message: "Invalid file buffer",
      });
    }

    if (typeof simulatedHoldings === "string") {
      try {
        simulatedHoldings = JSON.parse(simulatedHoldings);
      } catch {
        simulatedHoldings = [];
      }
    }
    if (!Array.isArray(simulatedHoldings)) simulatedHoldings = [];

    const custodianRows = await parseCustodianFromBuffer(file.data);
    const data = buildComparisonData(simulatedHoldings, custodianRows);

    return res.status(200).json({
      success: true,
      message: "Comparison generated",
      count: data.length,
      data,
    });
  } catch (error) {
    console.error("compareWithCustodian error:", error);
    return res.status(500).json({
      success: false,
      message: "Comparison failed",
      error: error.message,
    });
  }
};

export const uploadTransactionFileToStratus = async (req, res) => {
  try {
    /* ---------------- INIT ---------------- */
    const catalystApp = req.catalystApp;
    const stratus = catalystApp.stratus();
    const datastore = catalystApp.datastore();
    const bucket = stratus.bucket("upload-data-bucket");

    /* ---------------- VALIDATE FILE ---------------- */
    const file = req.files?.file;

    if (!file) {
      return res.status(400).json({ message: "CSV file is required" });
    }

    if (!file.name.endsWith(".csv")) {
      return res.status(400).json({ message: "Only CSV files are allowed" });
    }

    /* ---------------- FILE NAME ---------------- */
    const storedFileName = `Txn-${Date.now()}-${file.name}`;
    console.log("storedFileName:", storedFileName);

    /* ---------------- STREAM TO STRATUS ---------------- */
    const passThrough = new PassThrough();

    const uploadPromise = bucket.putObject(storedFileName, passThrough, {
      overwrite: true,
      contentType: "text/csv",
    });

    // Write file buffer to stream
    passThrough.end(file.data);

    // Wait for upload to complete
    await uploadPromise;

    const jobScheduling = catalystApp.jobScheduling();
const jobName = `TXN_IMPORT_${Date.now()}`;

await jobScheduling.JOB.submitJob({
  job_name: "UptSecurity_Clients",
  jobpool_name: "UpdateMasters",  // create "Import" job pool in Catalyst console if needed
  target_name: "UpdateSecurity_ClientMaster",
  target_type: "Function",
  params: {
    fileName: storedFileName,
    jobName,
  },
})
    
    /* ---------------- TRIGGER BULK IMPORT ---------------- */
    const bulkWrite = datastore.table("Transaction").bulkJob("write");

    const bulkWriteJob = await bulkWrite.createJob(
      {
        bucket_name: "upload-data-bucket",
        object_key: storedFileName,
      },
      {
        operation: "insert",
      },
    );

    /* ---------------- RESPONSE ---------------- */
    return res.status(200).json({
      message: "File uploaded and bulk import job created",
      fileName: storedFileName,
      bucket: "upload-data-bucket",
      jobId: bulkWriteJob.job_id,
      status: bulkWriteJob.status,
    });
  } catch (error) {
    console.error("Stratus upload or bulk import error:", error);

    return res.status(500).json({
      message: "Failed to upload file or create bulk import job",
      error: error.message,
    });
  }
};

/**
 * POST /transaction/import-bulk
 * Create Catalyst Bulk Write Job from Stratus CSV (manual trigger, still available)
 */
export const triggerTransactionBulkImport = async (req, res) => {
  try {
    /* ---------------- INIT ---------------- */
    const catalystApp = req.catalystApp;
    const datastore = catalystApp.datastore();

    const { stratusFileName } = req.body;

    if (!stratusFileName) {
      return res.status(400).json({
        message: "stratusFileName is required",
      });
    }

    /* ---------------- BULK WRITE INSTANCE ---------------- */
    const bulkWrite = datastore.table("Transaction").bulkJob("write");

    /* ---------------- STRATUS OBJECT DETAILS ---------------- */
    const objectDetails = {
      bucket_name: "upload-data-bucket",
      object_key: stratusFileName,
    };

    /* ---------------- CREATE BULK WRITE JOB ---------------- */
    const bulkWriteJob = await bulkWrite.createJob(objectDetails, {
      operation: "insert",
    });

    /* ---------------- RESPONSE ---------------- */
    return res.status(200).json({
      message: "Bulk write job created successfully",
      jobId: bulkWriteJob.job_id,
      status: bulkWriteJob.status,
    });
  } catch (error) {
    console.error("Bulk write error:", error);

    return res.status(500).json({
      message: "Failed to create bulk write job",
      error: error.message,
    });
  }
};
