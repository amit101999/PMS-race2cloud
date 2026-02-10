import { PassThrough, Readable } from "stream";
import csv from "csv-parser";
import { runFifoEngine } from "../../util/analytics/transactionHistory/fifo.js";

export const uploadTempTransactionFile = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const stratus = catalystApp.stratus();
    const datastore = catalystApp.datastore();
    const bucket = stratus.bucket("upload-data-bucket");

    const file = req.files?.file;
    if (!file) {
      return res.status(400).json({ message: "Transaction CSV is required" });
    }

    if (!file.name.toLowerCase().endsWith(".csv")) {
      return res.status(400).json({ message: "Only CSV files are allowed" });
    }

    const storedFileName = `temp-files/transactions/Txn-${Date.now()}-${file.name}`;

    const passThrough = new PassThrough();
    const uploadPromise = bucket.putObject(storedFileName, passThrough, {
      overwrite: true,
      contentType: "text/csv",
    });

    passThrough.end(file.data);
    await uploadPromise;

    /* -------- BULK INSERT INTO TEMP TABLE -------- */
    const bulkWrite = datastore.table("Temp_Transaction").bulkJob("write");

    const bulkJob = await bulkWrite.createJob(
      {
        bucket_name: "upload-data-bucket",
        object_key: storedFileName,
      },
      { operation: "insert" }
    );

    return res.status(200).json({
      success: true,
      message: "Transaction file uploaded to temp and bulk insert started",
      fileName: storedFileName,
      jobId: bulkJob.job_id,
      status: bulkJob.status,
    });

  } catch (error) {
    console.error("uploadTempTransactionFile error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to upload transaction temp file",
      error: error.message,
    });
  }
};

export const uploadTempCustodianFile = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const stratus = catalystApp.stratus();
    const datastore = catalystApp.datastore();
    const bucket = stratus.bucket("upload-data-bucket");

    const file = req.files?.file;
    if (!file) {
      return res.status(400).json({ message: "Custodian CSV is required" });
    }

    if (!file.name.toLowerCase().endsWith(".csv")) {
      return res.status(400).json({ message: "Only CSV files are allowed" });
    }

    const storedFileName = `temp-files/custodian/Cust-${Date.now()}-${file.name}`;

    const passThrough = new PassThrough();
    const uploadPromise = bucket.putObject(storedFileName, passThrough, {
      overwrite: true,
      contentType: "text/csv",
    });

    passThrough.end(file.data);
    await uploadPromise;

    /* -------- BULK INSERT INTO TEMP CUSTODIAN -------- */
    const bulkWrite = datastore.table("Temp_Custodian").bulkJob("write");

    const bulkJob = await bulkWrite.createJob(
      {
        bucket_name: "upload-data-bucket",
        object_key: storedFileName,
      },
      { operation: "insert" }
    );

    return res.status(200).json({
      success: true,
      message: "Custodian file uploaded to temp and bulk insert started",
      fileName: storedFileName,
      jobId: bulkJob.job_id,
      status: bulkJob.status,
    });

  } catch (error) {
    console.error("uploadTempCustodianFile error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to upload custodian temp file",
      error: error.message,
    });
  }
};
const runPerRowSimulationPaginated = ({
  dbTransactions,
  fileTransactions,
  bonuses,
  splits,
  page = 1,
  pageSize = 20,
}) => {
  const results = [];

  const startIndex = (page - 1) * pageSize;
  const endIndex = page * pageSize;

  // FIFO base state (DB transactions only)
  let currentTransactions = [...dbTransactions];

  let prevFifo = runFifoEngine(
    currentTransactions,
    bonuses,
    splits,
    true
  );

  // IMPORTANT: FIFO must run from 0 → endIndex
  for (let i = 0; i < fileTransactions.length; i++) {
    const row = fileTransactions[i];
    const beforeHolding = prevFifo?.holdings || 0;

    currentTransactions.push(row);

    const afterFifo = runFifoEngine(
      currentTransactions,
      bonuses,
      splits,
      true
    );

    const afterHolding = afterFifo?.holdings || 0;

    // ✅ Only collect rows that belong to this page
    if (i >= startIndex && i < endIndex) {
      results.push({
        WS_Account_code: row.WS_Account_code,
        ISIN: row.ISIN,
        Security_code: row.Security_code ?? "",
        Security_Name: row.Security_Name ?? "",
        Tran_Type: row.Tran_Type,
        transactionQty: row.QTY,
        oldQuantity: beforeHolding,
        newQuantity: afterHolding,
        rowIndex: i + 1,
      });
    }

    prevFifo = afterFifo;

    // 🚀 Optimization: stop once page is done
    if (i >= endIndex - 1) break;
  }

  return {
    page,
    pageSize,
    totalRows: fileTransactions.length,
    data: results,
  };
};

/** Normalize DB row to FIFO shape (Transaction or Temp_Transaction) */
const toFifoTxnRow = (r) => {
  const t = r.Transaction || r.Temp_Transaction || r;
  return {
    WS_Account_code: t.WS_Account_code,
    ISIN: t.ISIN,
    Security_code: t.Security_code ?? "",
    Security_Name: t.Security_Name ?? "",
    Tran_Type: t.Tran_Type,
    QTY: Number(t.QTY) || 0,
    TRANDATE: t.TRANDATE ?? "",
    NETRATE: Number(t.NETRATE) || 0,
  };
};

const BATCH = 250;

const fetchTransactionRows = async (zcql, isin, accountCode) => {
  const out = [];
  let offset = 0;
  const esc = (s) => String(s).replace(/'/g, "''");
  while (true) {
    const rows = await zcql.executeZCQLQuery(`
      SELECT ROWID, Security_Name, Security_code, Tran_Type, QTY, TRANDATE, NETRATE, ISIN, WS_Account_code
      FROM Transaction
      WHERE ISIN = '${esc(isin)}' AND WS_Account_code = '${esc(accountCode)}'
      ORDER BY ROWID ASC
      LIMIT ${BATCH} OFFSET ${offset}
    `);
    if (!rows?.length) break;
    rows.forEach((r) => out.push(toFifoTxnRow(r)));
    if (rows.length < BATCH) break;
    offset += BATCH;
  }
  return out;
};

const fetchTempTransactionRows = async (zcql, isin, accountCode) => {
  const out = [];
  let offset = 0;
  const esc = (s) => String(s).replace(/'/g, "''");
  while (true) {
    const rows = await zcql.executeZCQLQuery(`
      SELECT ROWID, Security_Name, Security_code, Tran_Type, QTY, TRANDATE, NETRATE, ISIN, WS_Account_code
      FROM Temp_Transaction
      WHERE ISIN = '${esc(isin)}' AND WS_Account_code = '${esc(accountCode)}'
      ORDER BY ROWID ASC
      LIMIT ${BATCH} OFFSET ${offset}
    `);
    if (!rows?.length) break;
    rows.forEach((r) => out.push(toFifoTxnRow(r)));
    if (rows.length < BATCH) break;
    offset += BATCH;
  }
  return out;
};

const fetchBonusRows = async (zcql, isin, accountCode) => {
  const out = [];
  let offset = 0;
  const esc = (s) => String(s).replace(/'/g, "''");
  while (true) {
    const rows = await zcql.executeZCQLQuery(`
      SELECT ROWID, BonusShare, ExDate, ISIN
      FROM Bonus
      WHERE ISIN = '${esc(isin)}' AND WS_Account_code = '${esc(accountCode)}'
      ORDER BY ROWID ASC
      LIMIT ${BATCH} OFFSET ${offset}
    `);
    if (!rows?.length) break;
    rows.forEach((r) => {
      const b = r.Bonus || r;
      out.push({ ROWID: b.ROWID, BonusShare: b.BonusShare, ExDate: b.ExDate, ISIN: b.ISIN });
    });
    if (rows.length < BATCH) break;
    offset += BATCH;
  }
  return out;
};

const fetchSplitRows = async (zcql, isin) => {
  const out = [];
  let offset = 0;
  const esc = (s) => String(s).replace(/'/g, "''");
  while (true) {
    const rows = await zcql.executeZCQLQuery(`
      SELECT ROWID, Issue_Date, Ratio1, Ratio2, ISIN
      FROM Split
      WHERE ISIN = '${esc(isin)}'
      ORDER BY ROWID ASC
      LIMIT ${BATCH} OFFSET ${offset}
    `);
    if (!rows?.length) break;
    rows.forEach((r) => {
      const s = r.Split || r;
      out.push({
        issueDate: s.Issue_Date,
        ratio1: s.Ratio1,
        ratio2: s.Ratio2,
        isin: s.ISIN,
      });
    });
    if (rows.length < BATCH) break;
    offset += BATCH;
  }
  return out;
};

/** [{ accountCode, isin, count, startIndex }] in deterministic order */
const getGroupsWithCumulativeCounts = async (zcql) => {
  const rows = await zcql.executeZCQLQuery(`
    SELECT WS_Account_code, ISIN, COUNT(ROWID) as cnt
    FROM Temp_Transaction
    GROUP BY WS_Account_code, ISIN
    ORDER BY WS_Account_code, ISIN
  `);
  const list = [];
  let start = 0;
  for (const r of rows || []) {
    const t = r.Temp_Transaction || r;
    const accountCode = (t.WS_Account_code ?? "").toString().trim();
    const isin = (t.ISIN ?? "").toString().trim();
    const count = Number(t.cnt) || Number(t["COUNT(ROWID)"]) || 0;
    if (!accountCode || !isin || count === 0) continue;
    list.push({ accountCode, isin, count, startIndex: start });
    start += count;
  }
  return list;
};

const getFifoTotalRows = async (zcql) => {
  const rows = await zcql.executeZCQLQuery(`SELECT COUNT(ROWID) as total FROM Temp_Transaction`);
  const first = rows?.[0];
  const r = first?.Temp_Transaction ?? first;
  if (!r) return 0;
  if (r.total !== undefined) return Number(r.total) || 0;
  if (r["COUNT(ROWID)"] !== undefined) return Number(r["COUNT(ROWID)"]) || 0;
  const num = Number(Object.values(r).find((v) => !isNaN(Number(v))));
  return !isNaN(num) ? num : 0;
};

/** Paginated FIFO: Transaction + Temp_Transaction; return only requested page. */
export const getFifoPage = async (zcql, page = 1, pageSize = 20) => {
  const totalRows = await getFifoTotalRows(zcql);
  const start = (page - 1) * pageSize;
  if (totalRows === 0 || start >= totalRows) return { page, pageSize, totalRows, data: [] };

  const groups = await getGroupsWithCumulativeCounts(zcql);
  const data = [];

  for (const g of groups) {
    if (data.length >= pageSize) break;
    const groupEnd = g.startIndex + g.count;
    if (groupEnd <= start) continue;

    const dbTransactions = await fetchTransactionRows(zcql, g.isin, g.accountCode);
    const tempTransactions = await fetchTempTransactionRows(zcql, g.isin, g.accountCode);
    const bonuses = await fetchBonusRows(zcql, g.isin, g.accountCode);
    const splits = await fetchSplitRows(zcql, g.isin);

    const chunk = runPerRowSimulationPaginated({
      dbTransactions,
      fileTransactions: tempTransactions,
      bonuses,
      splits,
      page: 1,
      pageSize: g.count,
    });

    const skip = Math.max(0, start - g.startIndex);
    const take = Math.min(chunk.data.length - skip, pageSize - data.length);
    for (let i = skip; i < skip + take && i < chunk.data.length; i++) {
      const row = { ...chunk.data[i] };
      delete row.rowIndex;
      data.push(row);
    }
  }

  return { page, pageSize, totalRows, data };
};

const getFinalHoldingsForGroup = async (zcql, accountCode, isin) => {
  const dbTransactions = await fetchTransactionRows(zcql, isin, accountCode);
  const tempTransactions = await fetchTempTransactionRows(zcql, isin, accountCode);
  const bonuses = await fetchBonusRows(zcql, isin, accountCode);
  const splits = await fetchSplitRows(zcql, isin);
  const result = runPerRowSimulationPaginated({
    dbTransactions,
    fileTransactions: tempTransactions,
    bonuses,
    splits,
    page: 1,
    pageSize: tempTransactions.length,
  });
  const last = result.data[result.data.length - 1];
  return {
    UCC: accountCode,
    ISIN: isin,
    Qty_FA: last ? Number(last.newQuantity) || 0 : 0,
    Security: last?.Security_Name ?? "",
    fromTemp: tempTransactions.length > 0,
  };
};

const getOrderedDiffKeys = async (zcql) => {
  const keys = new Map();
  const add = (ucc, isin) => {
    const u = (ucc ?? "").toString().trim();
    const i = (isin ?? "").toString().trim();
    if (u && i) keys.set(`${u}__${i}`, { UCC: u, ISIN: i });
  };
  let offset = 0;
  while (true) {
    const rows = await zcql.executeZCQLQuery(`
      SELECT WS_Account_code, ISIN FROM Temp_Transaction ORDER BY WS_Account_code, ISIN LIMIT ${BATCH} OFFSET ${offset}
    `);
    if (!rows?.length) break;
    rows.forEach((r) => { const t = r.Temp_Transaction || r; add(t.WS_Account_code, t.ISIN); });
    if (rows.length < BATCH) break;
    offset += BATCH;
  }
  offset = 0;
  while (true) {
    const rows = await zcql.executeZCQLQuery(`
      SELECT UCC, ISIN FROM Temp_Custodian ORDER BY UCC, ISIN LIMIT ${BATCH} OFFSET ${offset}
    `);
    if (!rows?.length) break;
    rows.forEach((r) => { const t = r.Temp_Custodian || r; add(t.UCC, t.ISIN); });
    if (rows.length < BATCH) break;
    offset += BATCH;
  }
  return Array.from(keys.values()).sort((a, b) => (a.UCC + a.ISIN).localeCompare(b.UCC + b.ISIN));
};

/**
 * Temp_Custodian columns we use (must match DB schema):
 * UCC, ISIN, NetBalance, ClientName, SecurityName, LTPDate, LTPPrice
 */
const getCustodianRow = async (zcql, ucc, isin) => {
  const esc = (s) => String(s).replace(/'/g, "''");
  const rows = await zcql.executeZCQLQuery(`
    SELECT UCC, ISIN, NetBalance, ClientName, SecurityName, LTPDate, LTPPrice
    FROM Temp_Custodian WHERE UCC = '${esc(ucc)}' AND ISIN = '${esc(isin)}' LIMIT 1
  `);
  const r = rows?.[0]?.Temp_Custodian ?? rows?.[0];
  if (!r) return null;
  return {
    UCC: r.UCC ?? ucc,
    ISIN: r.ISIN ?? isin,
    NetBalance: parseFloat(r.NetBalance) || 0,
    ClientName: (r.ClientName ?? "").toString().trim(),
    SecurityName: (r.SecurityName ?? "").toString().trim(),
    LTPDate: (r.LTPDate ?? "").toString().trim(),
    LTPPrice: parseFloat(r.LTPPrice) != null ? parseFloat(r.LTPPrice) : null,
  };
};

/** Paginated Diff: FIFO final holdings + Temp_Custodian; only keys on this page. */
export const getDiffPage = async (zcql, page = 1, pageSize = 20) => {
  const allKeys = await getOrderedDiffKeys(zcql);
  const totalRows = allKeys.length;
  const start = (page - 1) * pageSize;
  const pageKeys = allKeys.slice(start, start + pageSize);
  if (pageKeys.length === 0) return { page, pageSize, totalRows, data: [] };

  const data = [];
  for (const k of pageKeys) {
    const fa = await getFinalHoldingsForGroup(zcql, k.UCC, k.ISIN);
    const cust = await getCustodianRow(zcql, k.UCC, k.ISIN);
    const qtyFA = fa.Qty_FA;
    const qtyCust = cust ? cust.NetBalance : 0;
    const diff = qtyFA - qtyCust;
    let Mismatch_Reason = "";
    if (!cust) Mismatch_Reason = "Only in FA";
    else if (cust && !fa.fromTemp) Mismatch_Reason = "Only in Custodian";
    else if (diff !== 0) Mismatch_Reason = "Qty Mismatch";
    data.push({
      UCC: k.UCC,
      ISIN: k.ISIN,
      Qty_FA: qtyFA,
      Qty_Cust: qtyCust,
      Diff: diff,
      Matching: diff === 0 ? "Yes" : "No",
      Mismatch_Reason,
      Security: fa.Security || (cust && cust.SecurityName) || "",
      ClientName: cust ? cust.ClientName : "",
      LTPDate: cust ? cust.LTPDate : "",
      LTPPrice: cust && cust.LTPPrice != null ? cust.LTPPrice : "",
    });
  }
  return { page, pageSize, totalRows, data };
};

export const getFifoPageHandler = async (req, res) => {
  try {
    const zcql = req.catalystApp?.zcql();
    if (!zcql) return res.status(500).json({ success: false, message: "Catalyst not initialized" });
    const page = Math.max(1, Number(req.query.page) || 1);
    const pageSize = Math.min(100, Math.max(1, Number(req.query.pageSize) || 20));
    const result = await getFifoPage(zcql, page, pageSize);
    return res.status(200).json({ success: true, ...result });
  } catch (err) {
    console.error("getFifoPage error:", err);
    return res.status(500).json({ success: false, message: err.message });
  }
};

export const getDiffPageHandler = async (req, res) => {
  try {
    const zcql = req.catalystApp?.zcql();
    if (!zcql) return res.status(500).json({ success: false, message: "Catalyst not initialized" });
    const page = Math.max(1, Number(req.query.page) || 1);
    const pageSize = Math.min(100, Math.max(1, Number(req.query.pageSize) || 20));
    const result = await getDiffPage(zcql, page, pageSize);
    return res.status(200).json({ success: true, ...result });
  } catch (err) {
    console.error("getDiffPage error:", err);
    return res.status(500).json({ success: false, message: err.message });
  }
};

const DIFFERENTIAL_JOB_PREFIX = "DFReport";
const FIVE_DAYS_MS = 5 * 24 * 60 * 60 * 1000;

function isDifferentialJobWithinFiveDays(jobName) {
  const match = String(jobName).match(/^DifferentialReport_(\d+)$/);
  if (!match) return false;
  const ts = parseInt(match[1], 10);
  return Date.now() - ts < FIVE_DAYS_MS;
}

export const startDifferentialExportJob = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();
    const jobScheduling = catalystApp.jobScheduling();

    const ts = Date.now();
    const jobName = `${DIFFERENTIAL_JOB_PREFIX}${ts}`;
    const fileName = `temp-files/differential/differential-report-${ts}.csv`;
    const catalystJobName = "DiffReport";

    await jobScheduling.JOB.submitJob({
      job_name: catalystJobName,
      jobpool_name: "Export",
      target_name: "ExportDifferentialReport",
      target_type: "Function",
      params: { jobName, fileName },
    });

    return res.status(200).json({
      jobName,
      fileName,
      status: "PENDING",
      message: "Differential export job started",
    });
  } catch (err) {
    console.error("startDifferentialExportJob error:", err);
    return res.status(500).json({ success: false, message: err?.message || "Failed to start export job" });
  }
};

export const getDifferentialExportStatus = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();
    const jobName = req.query.jobName;

    if (!jobName) {
      return res.status(400).json({ success: false, message: "jobName is required" });
    }

    const esc = (s) => String(s).replace(/'/g, "''");
    const rows = await zcql.executeZCQLQuery(`
      SELECT jobName, status
      FROM Jobs
      WHERE jobName = '${esc(jobName)}'
      LIMIT 1
    `);

    if (!rows?.length) {
      return res.status(200).json({ jobName, status: "NOT_FOUND" });
    }

    const row = rows[0].Jobs || rows[0];
    const status = row.status;

    const out = { jobName, status };

    if (status === "COMPLETED" && isDifferentialJobWithinFiveDays(jobName)) {
      const fileName = `temp-files/differential/differential-report-${jobName.replace(DIFFERENTIAL_JOB_PREFIX, "")}.csv`;
      const stratus = catalystApp.stratus();
      const bucket = stratus.bucket("upload-data-bucket");
      const downloadUrl = await bucket.generatePreSignedUrl(fileName, "GET", { expiresIn: 3600 });
      out.downloadUrl = downloadUrl;
      out.fileName = fileName;
    }

    return res.status(200).json(out);
  } catch (err) {
    console.error("getDifferentialExportStatus error:", err);
    return res.status(500).json({ success: false, message: err?.message || "Failed to get status" });
  }
};

export const listDifferentialExportJobs = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();

    const rows = await zcql.executeZCQLQuery(`
      SELECT jobName, status
      FROM Jobs
      WHERE jobName LIKE 'DifferentialReport_%'
      ORDER BY jobName DESC
      LIMIT 100
    `);

    const jobs = (rows || []).map((r) => {
      const j = r.Jobs || r;
      return { jobName: j.jobName, status: j.status };
    }).filter((j) => isDifferentialJobWithinFiveDays(j.jobName));

    return res.status(200).json({ success: true, jobs });
  } catch (err) {
    console.error("listDifferentialExportJobs error:", err);
    return res.status(500).json({ success: false, message: err?.message || "Failed to list jobs" });
  }
};

// old code ::::::::::::::::::::::::::::::::::::::::::::::::::::::

// const runPerRowSimulation = ({
//   dbTransactions,
//   fileTransactions,
//   bonuses,
//   splits,
// }) => {
//   const results = [];

//   // Start with DB-only transactions
//   let currentTransactions = [...dbTransactions];

//   // Initial FIFO (before any file rows)
//   let prevFifo = runFifoEngine(
//     currentTransactions,
//     bonuses,
//     splits,
//     true
//   );

//   for (const row of fileTransactions) {
//     const beforeHolding = prevFifo?.holdings || 0;

//     // Apply ONE row
//     currentTransactions.push(row);

//     const afterFifo = runFifoEngine(
//       currentTransactions,
//       bonuses,
//       splits,
//       true
//     );

//     const afterHolding = afterFifo?.holdings || 0;

//     results.push({
//       WS_Account_code: row.WS_Account_code,
//       ISIN: row.ISIN,
//       Security_code: row.Security_code ?? "",
//       Security_Name: row.Security_Name ?? "",
//       Tran_Type: row.Tran_Type,
//       transactionQty: row.QTY,
//       oldQuantity: beforeHolding,
//       newQuantity: afterHolding,
//     });

//     // move state forward
//     prevFifo = afterFifo;
//   }

//   return results;
// };

// const fetchTransactionsPaginated = async (zcql, isin, accountCode) => {
//   const batchSize = 250;
//   let offset = 0;

//   const results = [];
//   const seenRowIds = new Set();

//   while (true) {
//     const rows = await zcql.executeZCQLQuery(`
//       SELECT ROWID, Security_Name, Security_code, Tran_Type,
//              QTY, TRANDATE, NETRATE, ISIN
//       FROM Transaction
//       WHERE ISIN = '${isin}'
//         AND WS_Account_code = '${accountCode}'
//       ORDER BY ROWID ASC
//       LIMIT ${batchSize} OFFSET ${offset}
//     `);

//     if (!rows.length) break;

//     for (const r of rows) {
//       const t = r.Transaction || r;

//       if (!t?.ROWID) continue;
//       if (seenRowIds.has(t.ROWID)) continue; // 🚫 duplicate

//       seenRowIds.add(t.ROWID);
//       results.push(t);
//     }

//     if (rows.length < batchSize) break;
//     offset += batchSize;
//   }

//   return results;
// };

// const fetchBonusPaginated = async (zcql, isin, accountCode) => {
//   const batchSize = 250;
//   let offset = 0;

//   const results = [];
//   const seenRowIds = new Set();

//   while (true) {
//     const rows = await zcql.executeZCQLQuery(`
//       SELECT ROWID, BonusShare, ExDate, ISIN
//       FROM Bonus
//       WHERE ISIN = '${isin}'
//         AND WS_Account_code = '${accountCode}'
//       ORDER BY ROWID ASC
//       LIMIT ${batchSize} OFFSET ${offset}
//     `);

//     if (!rows.length) break;

//     for (const r of rows) {
//       const b = r.Bonus || r;
//       if (!b?.ROWID) continue;
//       if (seenRowIds.has(b.ROWID)) continue;

//       seenRowIds.add(b.ROWID);
//       results.push(b);
//     }

//     if (rows.length < batchSize) break;
//     offset += batchSize;
//   }

//   return results;
// };

// const fetchSplitPaginated = async (zcql, isin) => {
//   const batchSize = 250;
//   let offset = 0;

//   const results = [];
//   const seenRowIds = new Set();

//   while (true) {
//     const rows = await zcql.executeZCQLQuery(`
//       SELECT ROWID, Issue_Date, Ratio1, Ratio2, ISIN
//       FROM Split
//       WHERE ISIN = '${isin}'
//       ORDER BY ROWID ASC
//       LIMIT ${batchSize} OFFSET ${offset}
//     `);

//     if (!rows.length) break;

//     for (const r of rows) {
//       const s = r.Split || r;
//       if (!s?.ROWID) continue;
//       if (seenRowIds.has(s.ROWID)) continue;

//       seenRowIds.add(s.ROWID);
//       results.push({
//         issueDate: s.Issue_Date,
//         ratio1: s.Ratio1,
//         ratio2: s.Ratio2,
//         isin: s.ISIN,
//       });
//     }

//     if (rows.length < batchSize) break;
//     offset += batchSize;
//   }

//   return results;
// };


// const normalizeFileTransactions = (parsedRows) => {
//   const seen = new Set();
//   const result = [];

//   for (const r of parsedRows) {
//     const key = [
//       r.WS_Account_code,
//       r.ISIN,
//       r.TRANDATE,
//       r.Tran_Type,
//       r.QTY,
//       r.NETRATE
//     ].join("|");

//     if (seen.has(key)) continue; // 🚫 duplicate CSV row
//     seen.add(key);

//     result.push({
//       WS_Account_code: r.WS_Account_code,
//       ISIN: r.ISIN,
//       Security_Name: r.Security_Name,
//       Security_code: r.Security_code,
//       Tran_Type: r.Tran_Type,
//       QTY: Number(r.QTY),
//       // TRANDATE: r.TRANDATE,
//       TRANDATE:  (() => {
//         const raw = r.TRANDATE;
//         if (raw == null || raw === "") return "";
//         if (typeof raw === "string" && /^\d{4}-\d{2}-\d{2}/.test(raw.trim())) return raw.trim().substring(0, 10);
//         const d = new Date(raw);
//         return !isNaN(d.getTime()) ? d.toISOString().split("T")[0] : "";
//       })(),
//       NETRATE: Number(r.NETRATE || 0),
//     });
//   }

//   return result;
// };  


// const getHoldingValues = async (
//   zcql,
//   isin,
//   accountCode,
//   fileTransactions = []
// ) => {
//   const dbTransactions = await fetchTransactionsPaginated(
//     zcql,
//     isin,
//     accountCode
//   );

//   const bonuses = await fetchBonusPaginated(
//     zcql,
//     isin,
//     accountCode
//   );

//   const splits = await fetchSplitPaginated(
//     zcql,
//     isin
//   );

//   // 🚨 Merge DB + file txns
//   const allTransactions = [...dbTransactions, ...fileTransactions];

//   return runPerRowSimulation({
//     dbTransactions,
//     fileTransactions,
//     bonuses,
//     splits,
//   });
// };


// export const simulateHoldings = async (req, res) => {
//   try {
//     const file = req.files?.file;
//     const catalystApp = req.catalystApp;
// const zcql = catalystApp.zcql();

//     /* ---------- validations ---------- */
//     if (!file) {
//       return res.status(400).json({
//         success: false,
//         message: "CSV file is required (field name: file)",
//       });
//     }

//     if (!file.name.toLowerCase().endsWith(".csv")) {
//       return res.status(400).json({
//         success: false,
//         message: "Only CSV files are allowed",
//       });
//     }

//     if (!Buffer.isBuffer(file.data)) {
//       return res.status(400).json({
//         success: false,
//         message: "Invalid file buffer",
//       });
//     }

//     /* ---------- parse CSV ---------- */
//     const parsedRows = await parseCSVFromBuffer(file.data);

//     const fileTxns = normalizeFileTransactions(parsedRows);

//     const grouped = {};
//     for (const tx of fileTxns) {
//       const key = `${tx.WS_Account_code}__${tx.ISIN}`;
//       if (!grouped[key]) {
//         grouped[key] = {
//           accountCode: tx.WS_Account_code,
//           isin: tx.ISIN,
//           fileTransactions: [],
//         };
//       }
//       grouped[key].fileTransactions.push(tx);
//     }

//     const data = [];

//     for (const key of Object.keys(grouped)) {
//       const { accountCode, isin, fileTransactions } = grouped[key];

//       const rowWiseResult = await getHoldingValues(
//         zcql,
//         isin,
//         accountCode,
//         fileTransactions
//       );

//       data.push(...rowWiseResult);
//     }

//     /* ---------- response: only WS_Account_code, ISIN, Security_code, Security_Name, Tran_Type, oldQuantity, newQuantity ---------- */
//     return res.status(200).json({
//       success: true,
//       message: "Holdings simulated successfully",
//       count: data.length,
//       data,
//     });

//   } catch (error) {
//     console.error("simulateHoldings error:", error);
//     return res.status(500).json({
//       success: false,
//       message: "Simulation failed",
//       error: error.message,
//     });
//   }
// };

// const parseCSVFromBuffer = (buffer) => {
//   return new Promise((resolve, reject) => {
//     const results = [];

//     Readable.from(buffer)
//       .pipe(
//         csv({
//           mapHeaders: ({ header }) =>
//             header?.trim(), // remove extra spaces
//         })
//       )
//       .on("data", (row) => {
//         results.push(row);
//       })
//       .on("end", () => {
//         resolve(results);
//       })
//       .on("error", (err) => {
//         reject(err);
//       });
//   });
// };

// /**
//  * Reduce simulated holdings to one row per (UCC, ISIN) with latest newQuantity.
//  * Rows are in transaction order, so last row per key = final quantity.
//  */
// const latestQuantityPerAccountIsin = (simulatedHoldings = []) => {
//   const byKey = {};
//   for (const row of simulatedHoldings) {
//     const ucc = row.WS_Account_code ?? "";
//     const isin = row.ISIN ?? "";
//     const key = `${ucc}__${isin}`;
//     byKey[key] = {
//       UCC: ucc,
//       ISIN: isin,
//       newQuantity: Number(row.newQuantity) || 0,
//       Security_Name: row.Security_Name ?? "",
//     };
//   }
//   return byKey;
// };

// /**
//  * Custodian CSV: reuse CSV parser.
//  */
// const parseCustodianFromBuffer = (buffer) => {
//   return parseCSVFromBuffer(buffer);
// };

// /**
//  * Build comparison rows: match by UCC + ISIN.
//  * - Qty FA = newQuantity from our simulation
//  * - Qty Cust = NetBalance from custodian
//  * - Report Date = today; Holding Date = from custodian (LTPDate) or empty
//  * - Scheme Code = empty for now
//  */
// const buildComparisonData = (simulatedHoldings = [], custodianRows = []) => {
//   const ourByKey = latestQuantityPerAccountIsin(simulatedHoldings);
//   const reportDate = new Date().toISOString().split("T")[0];
//   const addedTime = new Date().toISOString();

//   const custodianByKey = {};
//   for (const r of custodianRows) {
//     const ucc = (r.UCC ?? "").toString().trim();
//     const isin = (r.ISIN ?? "").toString().trim();
//     if (!ucc && !isin) continue;
//     const key = `${ucc}__${isin}`;
//     custodianByKey[key] = {
//       UCC: ucc,
//       ISIN: isin,
//       NetBalance: parseFloat(r.NetBalance) || 0,
//       ClientName: (r.ClientName ?? r["Client Name"] ?? "").toString().trim(),
//       SecurityName: (r.SecurityName ?? "").toString().trim(),
//       LTPDate: (r.LTPDate ?? "").toString().trim(),
//     };
//   }

//   const allKeys = new Set([
//     ...Object.keys(ourByKey),
//     ...Object.keys(custodianByKey),
//   ]);

//   const rows = [];
//   for (const key of allKeys) {
//     const our = ourByKey[key];
//     const cust = custodianByKey[key];
//     const ucc = our?.UCC ?? cust?.UCC ?? "";
//     const isin = our?.ISIN ?? cust?.ISIN ?? "";
//     const qtyFA = our?.newQuantity ?? 0;
//     const qtyCust = cust?.NetBalance ?? 0;
//     const diff = qtyFA - qtyCust;
//     const matching = diff === 0 ? "Yes" : "No";
//     let mismatchReason = "";
//     if (diff !== 0) mismatchReason = "Qty Mismatch";
//     if (!our && cust) mismatchReason = "Only in Custodian";
//     if (our && !cust) mismatchReason = "Only in FA";

//     rows.push({
//       "Report Date": reportDate,
//       "Holding Date": cust?.LTPDate ?? "",
//       UCC: ucc,
//       ISIN: isin,
//       "Qty Cust": qtyCust,
//       "Qty FA": qtyFA,
//       Diff: diff,
//       "Mismatch Reason": mismatchReason,
//       Matching: matching,
//       Security: our?.Security_Name ?? cust?.SecurityName ?? "",
//       "Client name": cust?.ClientName ?? "",
//       "Added Time": addedTime,
//       Comments: "",
//       "Scheme Code": "",
//       "LTP Date": cust?.LTPDate ?? "",
//     });
//   }

//   return rows;
// };

// /**
//  * POST /transaction-uploader/compare-custodian
//  * Body: { simulatedHoldings: Array } (from /parse-data response data)
//  * File: custodian CSV (field name: file)
//  */
// export const compareWithCustodian = async (req, res) => {
//   try {
//     const file = req.files?.file;
//     let simulatedHoldings = req.body?.simulatedHoldings;
//     console.log("simulatedHoldings count:", Array.isArray(simulatedHoldings) ? simulatedHoldings.length : "not array");

//     if (!file) {
//       return res.status(400).json({
//         success: false,
//         message: "Custodian CSV file is required (field name: file)",
//       });
//     }

//     if (!file.name || !file.name.toLowerCase().endsWith(".csv")) {
//       return res.status(400).json({
//         success: false,
//         message: "Only CSV files are allowed",
//       });
//     }

//     if (!Buffer.isBuffer(file.data)) {
//       return res.status(400).json({
//         success: false,
//         message: "Invalid file buffer",
//       });
//     }

//     if (typeof simulatedHoldings === "string") {
//       try {
//         simulatedHoldings = JSON.parse(simulatedHoldings);
//       } catch {
//         simulatedHoldings = [];
//       }
//     }
//     if (!Array.isArray(simulatedHoldings)) simulatedHoldings = [];

//     const custodianRows = await parseCustodianFromBuffer(file.data);
//     const data = buildComparisonData(simulatedHoldings, custodianRows);

//     return res.status(200).json({
//       success: true,
//       message: "Comparison generated",
//       count: data.length,
//       data,
//     });
//   } catch (error) {
//     console.error("compareWithCustodian error:", error);
//     return res.status(500).json({
//       success: false,
//       message: "Comparison failed",
//       error: error.message,
//     });
//   }
// };

// export const uploadTransactionFileToStratus = async (req, res) => {
//   try {
//     /* ---------------- INIT ---------------- */
//     const catalystApp = req.catalystApp;
//     const stratus = catalystApp.stratus();
//     const datastore = catalystApp.datastore();
//     const bucket = stratus.bucket("upload-data-bucket");

//     /* ---------------- VALIDATE FILE ---------------- */
//     const file = req.files?.file;

//     if (!file) {
//       return res.status(400).json({ message: "CSV file is required" });
//     }

//     if (!file.name.endsWith(".csv")) {
//       return res.status(400).json({ message: "Only CSV files are allowed" });
//     }

//     /* ---------------- FILE NAME ---------------- */
//     const storedFileName = `Txn-${Date.now()}-${file.name}`;
//     console.log("storedFileName:", storedFileName);

//     /* ---------------- STREAM TO STRATUS ---------------- */
//     const passThrough = new PassThrough();

//     const uploadPromise = bucket.putObject(storedFileName, passThrough, {
//       overwrite: true,
//       contentType: "text/csv",
//     });

//     // Write file buffer to stream
//     passThrough.end(file.data);

//     // Wait for upload to complete
//     await uploadPromise;

//     const jobScheduling = catalystApp.jobScheduling();
// const jobName = `TXN_IMPORT_${Date.now()}`;

// await jobScheduling.JOB.submitJob({
//   job_name: "UptSecurity_Clients",
//   jobpool_name: "UpdateMasters",  // create "Import" job pool in Catalyst console if needed
//   target_name: "UpdateSecurity_ClientMaster",
//   target_type: "Function",
//   params: {
//     fileName: storedFileName,
//     jobName,
//   },
// })
    
//     /* ---------------- TRIGGER BULK IMPORT ---------------- */
//     const bulkWrite = datastore.table("Transaction").bulkJob("write");

//     const bulkWriteJob = await bulkWrite.createJob(
//       {
//         bucket_name: "upload-data-bucket",
//         object_key: storedFileName,
//       },
//       {
//         operation: "insert",
//       },
//     );

//     /* ---------------- RESPONSE ---------------- */
//     return res.status(200).json({
//       message: "File uploaded and bulk import job created",
//       fileName: storedFileName,
//       bucket: "upload-data-bucket",
//       jobId: bulkWriteJob.job_id,
//       status: bulkWriteJob.status,
//     });
//   } catch (error) {
//     console.error("Stratus upload or bulk import error:", error);

//     return res.status(500).json({
//       message: "Failed to upload file or create bulk import job",
//       error: error.message,
//     });
//   }
// };

// /**
//  * POST /transaction/import-bulk
//  * Create Catalyst Bulk Write Job from Stratus CSV (manual trigger, still available)
//  */
// export const triggerTransactionBulkImport = async (req, res) => {
//   try {
//     /* ---------------- INIT ---------------- */
//     const catalystApp = req.catalystApp;
//     const datastore = catalystApp.datastore();

//     const { stratusFileName } = req.body;

//     if (!stratusFileName) {
//       return res.status(400).json({
//         message: "stratusFileName is required",
//       });
//     }

//     /* ---------------- BULK WRITE INSTANCE ---------------- */
//     const bulkWrite = datastore.table("Transaction").bulkJob("write");

//     /* ---------------- STRATUS OBJECT DETAILS ---------------- */
//     const objectDetails = {
//       bucket_name: "upload-data-bucket",
//       object_key: stratusFileName,
//     };

//     /* ---------------- CREATE BULK WRITE JOB ---------------- */
//     const bulkWriteJob = await bulkWrite.createJob(objectDetails, {
//       operation: "insert",
//     });

//     /* ---------------- RESPONSE ---------------- */
//     return res.status(200).json({
//       message: "Bulk write job created successfully",
//       jobId: bulkWriteJob.job_id,
//       status: bulkWriteJob.status,
//     });
//   } catch (error) {
//     console.error("Bulk write error:", error);

//     return res.status(500).json({
//       message: "Failed to create bulk write job",
//       error: error.message,
//     });
//   }
// };
