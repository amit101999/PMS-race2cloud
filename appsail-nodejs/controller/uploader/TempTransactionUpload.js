import csv from "csv-parser";
import { PassThrough, Readable } from "stream";

const BUCKET_NAME = "temporary-files";
const TABLE_NAME = "Transaction";

const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

/**
 * Official row-1 headers: exact count, order, spelling, and case.
 * Validation compares the physical first line to this list (comma- or tab-separated).
 */
const EXPECTED_HEADERS_IN_ORDER = [
  "WS CLIENT ID",
  "WS ACCOUNT CODE",
  "CLIENT NAME",
  "TRANDATE",
  "SETDATE",
  "TRANTYPE",
  "TRAN DESC",
  "SECURITY TYPE",
  "SECURITY TYPE DESCRIPTION",
  "DETAILTYPENAME",
  "ISIN",
  "SECURITY CODE",
  "SECURITY NAME",
  "EXCHG",
  "BROKERCODE",
  "DEPOSITOY/REGISTRAR",
  "DPID/AMC",
  "DP CLIENT ID/FOLIO",
  "BANKCODE",
  "BANKACID",
  "QTY",
  "RATE",
  "BROKERAGE",
  "SERVICETAX",
  "NETRATE",
  "NET AMOUNT",
  "STT",
  "TRFDATE",
  "TRFRATE",
  "TRF AMOUNT",
  "TOTAL_TRXNFEE",
  "TOTAL_TRXNFEE_STAX",
  "TXN_REF_NO",
  "DESCMEMO",
  "CHEQUENO",
  "CHEQUEDTL",
  "PORTFOLIOID",
  "DELIVERYDATE",
  "PAYMENTDATE",
  "ACCRUEDINTEREST",
  "ISSUER",
  "ISSUERNAME",
  "TDSAMOUNT",
  "STAMPDUTY",
  "TPMSGAIN",
  "RMID",
  "RMNAME",
  "ADVISORID",
  "ADVISORNAME",
  "BRANCHID",
  "BRANCHNAME",
  "GROUPID",
  "GROUPNAME",
  "OWNERID",
  "OWNERNAME",
  "WEALTHADVISOR_NAME",
  "SCHEMEID",
  "SCHEMENAME",
];

/**
 * Row-1 template header (exact match to EXPECTED_HEADERS_IN_ORDER) → Transaction column for bulk CSV.
 * Template columns with no entry are omitted from the Stratus file (e.g. "CLIENT NAME" is not in Transaction).
 */
const HEADER_MAP = {
  "WS CLIENT ID": "WS_client_id",
  "WS ACCOUNT CODE": "WS_Account_code",
  TRANDATE: "TRANDATE",
  SETDATE: "SETDATE",
  TRANTYPE: "Tran_Type",
  "TRAN DESC": "Tran_Desc",
  "SECURITY TYPE": "Security_Type",
  "SECURITY TYPE DESCRIPTION": "Security_Type_Description",
  DETAILTYPENAME: "DETAILTYPENAME",
  ISIN: "ISIN",
  "SECURITY CODE": "Security_code",
  "SECURITY NAME": "Security_Name",
  EXCHG: "EXCHG",
  BROKERCODE: "BROKERCODE",
  "DEPOSITOY/REGISTRAR": "Depositoy_Registrar",
  "DPID/AMC": "DPID_AMC",
  "DP CLIENT ID/FOLIO": "Dp_Client_id_Folio",
  BANKCODE: "BANKCODE",
  BANKACID: "BANKACID",
  QTY: "QTY",
  RATE: "RATE",
  BROKERAGE: "BROKERAGE",
  SERVICETAX: "SERVICETAX",
  NETRATE: "NETRATE",
  "NET AMOUNT": "Net_Amount",
  STT: "STT",
  TRFDATE: "TRFDATE",
  TRFRATE: "TRFRATE",
  "TRF AMOUNT": "TRFAMT",
  TOTAL_TRXNFEE: "TOTAL_TRXNFEE",
  TOTAL_TRXNFEE_STAX: "TOTAL_TRXNFEE_STAX",
  TXN_REF_NO: "Txn_Ref_No",
  DESCMEMO: "DESCMEMO",
  CHEQUENO: "CHEQUENO",
  CHEQUEDTL: "CHEQUEDTL",
  PORTFOLIOID: "PORTFOLIOID",
  DELIVERYDATE: "DELIVERYDATE",
  PAYMENTDATE: "PAYMENTDATE",
  ACCRUEDINTEREST: "ACCRUEDINTEREST",
  ISSUER: "ISSUER",
  ISSUERNAME: "ISSUERNAME",
  TDSAMOUNT: "TDSAMOUNT",
  STAMPDUTY: "STAMPDUTY",
  TPMSGAIN: "TPMSGAIN",
  RMID: "RMID",
  RMNAME: "RMNAME",
  ADVISORID: "ADVISORID",
  ADVISORNAME: "ADVISORNAME",
  BRANCHID: "BRANCHID",
  BRANCHNAME: "BRANCHNAME",
  GROUPID: "GROUPID",
  GROUPNAME: "GROUPNAME",
  OWNERID: "OWNERID",
  OWNERNAME: "OWNERNAME",
  WEALTHADVISOR_NAME: "WEALTHADVISOR_NAME",
  SCHEMEID: "SCHEMEID",
  SCHEMENAME: "SCHEMENAME",
};

/** Must be yyyy-mm-dd when the cell is not blank / null / 0-like. */
const REQUIRED_DATE_COLUMNS = ["TRANDATE", "SETDATE"];

/** If non-empty, value must be yyyy-mm-dd. */
const OPTIONAL_DATE_COLUMNS = ["TRFDATE", "DELIVERYDATE", "PAYMENTDATE"];

function stripBom(text) {
  if (text.charCodeAt(0) === 0xfeff) {
    return text.slice(1);
  }
  return text;
}

function getCell(row, columnName) {
  const key = Object.keys(row).find((k) => k.trim() === columnName);
  if (!key) return undefined;
  const v = row[key];
  if (v == null) return "";
  return String(v).trim();
}

function getFirstLineText(fileBuffer) {
  const text = stripBom(fileBuffer.toString("utf8"));
  const nl = text.search(/\r?\n/);
  return nl === -1 ? text : text.slice(0, nl);
}

/** Tab if the header line contains tabs; otherwise comma. */
function detectSeparator(firstLine) {
  return (firstLine.match(/\t/g) || []).length > 0 ? "\t" : ",";
}

function splitHeaderCells(firstLine, separator) {
  return firstLine.split(separator).map((c) => c.trim());
}

/**
 * Row 1 must match EXPECTED_HEADERS_IN_ORDER exactly: same count, same order, same spelling/case.
 */
function validateExpectedHeaderOrder(fileBuffer) {
  const firstLine = getFirstLineText(fileBuffer);
  const separator = detectSeparator(firstLine);
  const cells = splitHeaderCells(firstLine, separator);
  const expected = EXPECTED_HEADERS_IN_ORDER;

  if (cells.length !== expected.length) {
    return {
      code: "WRONG_COLUMN_ORDER",
      kind: "COUNT",
      message: `Column order / count is wrong. Row 1 must have exactly ${expected.length} columns in the official template order; your file has ${cells.length}. Do not add, remove, or reorder columns.`,
      expectedCount: expected.length,
      actualCount: cells.length,
      expectedHeaders: [...expected],
      hint: "Restore the header row to match the template: same positions left-to-right.",
    };
  }

  const mismatches = [];
  for (let i = 0; i < cells.length; i++) {
    if (cells[i] !== expected[i]) {
      mismatches.push({
        columnIndex: i + 1,
        expected: expected[i],
        actual: cells[i],
      });
    }
  }

  if (mismatches.length > 0) {
    return {
      code: "WRONG_COLUMN_ORDER",
      kind: "ORDER_OR_NAME",
      message:
        "Column order is wrong (or a header name/spelling does not match the template). Row 1 headers must appear in the exact same order as the official list — shifting any column left or right will fail.",
      mismatches,
      expectedHeaders: [...expected],
      hint: `Check column position(s): ${mismatches.map((m) => m.columnIndex).join(", ")}. Expected vs actual is listed below.`,
    };
  }

  return null;
}

function isValidYyyyMmDd(value) {
  const s = String(value ?? "").trim();
  if (!ISO_DATE_RE.test(s)) return false;
  const [y, m, d] = s.split("-").map(Number);
  const dt = new Date(s + "T00:00:00");
  return (
    dt.getFullYear() === y &&
    dt.getMonth() + 1 === m &&
    dt.getDate() === d
  );
}

/** Missing date in CSV: empty, numeric 0, or common null tokens (case-insensitive). */
function isAbsentDateValue(value) {
  if (value === undefined) return true;
  const s = String(value).trim();
  if (s === "") return true;
  if (s === "0") return true;
  const lower = s.toLowerCase();
  return lower === "null" || lower === "na" || lower === "n/a" || s === "-";
}

/**
 * Rewrites CSV for bulk insert: maps template headers (HEADER_MAP) to Transaction column names,
 * trims cells, outputs comma-separated rows. Unmapped template columns are dropped.
 */
function normalizeCsvForBulkUpload(fileBuffer) {
  const text = stripBom(fileBuffer.toString("utf8"));
  const lines = text.split(/\r?\n/);
  if (!lines.length) return fileBuffer;

  const firstLine = lines[0] ?? "";
  const separator = detectSeparator(firstLine);
  const splitLine = (line) => line.split(separator).map((c) => c.trim());

  const originalHeaders = splitLine(lines[0]);
  const keepIndices = [];
  const newHeaders = [];

  for (let i = 0; i < originalHeaders.length; i++) {
    const raw = originalHeaders[i];
    const mapped = HEADER_MAP[raw];
    if (mapped) {
      keepIndices.push(i);
      newHeaders.push(mapped);
    }
  }

  const outSep = ",";
  const newLines = [newHeaders.join(outSep)];

  for (let r = 1; r < lines.length; r++) {
    const line = lines[r];
    if (!line.trim()) continue;
    const cells = splitLine(line);
    const kept = keepIndices.map((idx) => (idx < cells.length ? cells[idx] : ""));
    newLines.push(kept.join(outSep));
  }

  return Buffer.from(newLines.join("\n"), "utf8");
}

/** First N data rows parsed for date-format checks before upload. */
const SAMPLE_DATA_ROW_COUNT = 3;

/**
 * Parses the first SAMPLE_DATA_ROW_COUNT data rows from the CSV buffer.
 */
function parseTempTransactionSampleRows(fileBuffer) {
  return new Promise((resolve, reject) => {
    const text = stripBom(fileBuffer.toString("utf8"));
    const firstLine = getFirstLineText(fileBuffer);
    const separator = detectSeparator(firstLine);
    const rows = [];
    const source = Readable.from(text);
    const parser = csv({ separator });
    let settled = false;

    const settle = (err, result) => {
      if (settled) return;
      settled = true;
      source.removeAllListeners();
      parser.removeAllListeners();
      try {
        source.destroy();
        parser.destroy();
      } catch {
        /* ignore */
      }
      if (err) {
        reject(err);
        return;
      }
      resolve(result);
    };

    parser.on("data", (row) => {
      rows.push(row);
      if (rows.length >= SAMPLE_DATA_ROW_COUNT) {
        settle(null, rows);
      }
    });

    parser.on("end", () => settle(null, rows));
    parser.on("error", (e) => settle(e));
    source.on("error", (e) => settle(e));

    source.pipe(parser);
  });
}

/**
 * POST /api/transaction-uploader/upload-temp-file
 * 1. Validates row 1 matches EXPECTED_HEADERS_IN_ORDER (count + order + exact names).
 * 2. Validates the CSV parses and has at least one data row.
 * 3. Validates date columns on the first 3 data rows (blank / 0 / null allowed when empty).
 * 4. Rewrites CSV: maps template headers to Transaction columns (HEADER_MAP), tab → comma, trim cells.
 * 5. Uploads rewritten CSV to Stratus under temp-files/temp-transactions/.
 * 6. Triggers a Catalyst Bulk Write Job to insert into the Transaction table.
 */
export const uploadTempTransaction = async (req, res) => {
  try {
    /* ─── 1. VALIDATE REQUEST ─────────────────────────────────────── */
    const catalystApp = req.catalystApp;
    if (!catalystApp) {
      return res.status(500).json({
        success: false,
        message: "Catalyst not initialized",
      });
    }

    const file = req.files?.file;

    if (!file) {
      return res.status(400).json({
        success: false,
        message: "CSV file is required. Send it as form-data with key 'file'.",
      });
    }

    if (!file.name.toLowerCase().endsWith(".csv")) {
      return res.status(400).json({
        success: false,
        message: "Only CSV files are allowed",
      });
    }

    const orderErr = validateExpectedHeaderOrder(file.data);
    if (orderErr) {
      return res.status(400).json({
        success: false,
        ...orderErr,
      });
    }

    /* ─── 1b. PARSE CSV + DATE CHECK (before Stratus — must pass to continue) ─ */
    let sampleRows;
    try {
      sampleRows = await parseTempTransactionSampleRows(file.data);
    } catch (parseErr) {
      console.error(
        `[TempTransactionUpload] CSV parse error [${new Date().toISOString()}]:`,
        parseErr
      );
      return res.status(400).json({
        success: false,
        message: "Could not read the CSV file. Check the file and try again.",
      });
    }

    if (sampleRows.length === 0) {
      return res.status(400).json({
        success: false,
        message:
          "The CSV has no data rows. Add at least one data row before uploading.",
      });
    }

    for (let i = 0; i < sampleRows.length; i++) {
      const fileLineNumber = i + 2;
      const invalidDateFields = [];

      for (const col of REQUIRED_DATE_COLUMNS) {
        const v = getCell(sampleRows[i], col);
        if (!isAbsentDateValue(v) && !isValidYyyyMmDd(v)) {
          invalidDateFields.push(col);
        }
      }

      for (const col of OPTIONAL_DATE_COLUMNS) {
        const v = getCell(sampleRows[i], col);
        if (!isAbsentDateValue(v) && !isValidYyyyMmDd(v)) {
          invalidDateFields.push(col);
        }
      }

      if (invalidDateFields.length > 0) {
        return res.status(400).json({
          success: false,
          code: "INVALID_DATE_FORMAT",
          message: `Date format is invalid. Parsed the first ${SAMPLE_DATA_ROW_COUNT} data rows: problem in data row ${i + 1} of ${SAMPLE_DATA_ROW_COUNT} (CSV line ${fileLineNumber}). Columns must be yyyy-mm-dd: ${invalidDateFields.join(", ")}.`,
          fields: invalidDateFields,
          row: fileLineNumber,
          dataRowIndex: i + 1,
          headerNamesToFix: invalidDateFields,
          sampleRowsValidated: SAMPLE_DATA_ROW_COUNT,
          hint: "Use yyyy-mm-dd only (example: 2025-04-06). Empty dates: leave blank, 0, or null. Fix the file and re-upload — nothing was imported.",
        });
      }
    }

    /* ─── 2. NORMALIZE CSV & UPLOAD TO STRATUS ───────────────────── */
    const rewrittenCsv = normalizeCsvForBulkUpload(file.data);

    const bucket = catalystApp.stratus().bucket(BUCKET_NAME);
    const objectKey = `temp-files/temp-transactions/TxnUpload-${Date.now()}-${file.name}`;

    const passThrough = new PassThrough();
    const uploadPromise = bucket.putObject(objectKey, passThrough, {
      overwrite: true,
      contentType: "text/csv",
    });

    passThrough.end(rewrittenCsv);
    await uploadPromise;

    /* ─── 3. TRIGGER BULK WRITE JOB → Transaction TABLE ──────────── */
    const bulkJob = await catalystApp
      .datastore()
      .table(TABLE_NAME)
      .bulkJob("write")
      .createJob(
        {
          bucket_name: BUCKET_NAME,
          object_key: objectKey,
        },
        {
          operation: "insert",
        }
      );

    /* ─── 4. RESPOND ──────────────────────────────────────────────── */
    return res.status(200).json({
      success: true,
      message: "File uploaded to Stratus and bulk insert job started for Transaction table",
      fileName: file.name,
      objectKey,
      bucket: BUCKET_NAME,
      table: TABLE_NAME,
      jobId: bulkJob.job_id,
      jobStatus: bulkJob.status,
    });
  } catch (error) {
    console.error(`[TempTransactionUpload] [Error] [${new Date().toISOString()}] :`, error);

    return res.status(500).json({
      success: false,
      message: "Failed to upload file or start bulk insert job",
      error: error.message,
    });
  }
};
