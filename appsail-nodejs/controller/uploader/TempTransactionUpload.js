import csv from "csv-parser";
import { PassThrough, Readable } from "stream";

const BUCKET_NAME = "temporary-files";
const TABLE_NAME = "Transaction";

const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

/**
 * Headers must appear in this exact order. Name match is case-insensitive (trimmed).
 * All wrong, missing, or extra columns are collected and returned together.
 */
const REQUIRED_TRANSACTION_HEADERS = [
  "WS_client_id",
  "WS_Account_code",
  "TRANDATE",
  "SETDATE",
  "Tran_Type",
  "Tran_Desc",
  "Security_Type",
  "Security_Type_Description",
  "DETAILTYPENAME",
  "ISIN",
  "Security_code",
  "Security_Name",
  "EXCHG",
  "BROKERCODE",
  "Depositoy_Registrar",
  "DPID_AMC",
  "Dp_Client_id_Folio",
  "BANKCODE",
  "BANKACID",
  "QTY",
  "RATE",
  "BROKERAGE",
  "SERVICETAX",
  "NETRATE",
  "Net_Amount",
  "STT",
  "TRFDATE",
  "TRFRATE",
  "TRFAMT",
  "TOTAL_TRXNFEE",
  "TOTAL_TRXNFEE_STAX",
  "Txn_Ref_No",
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
  "executionPriority",
];

/** Must be yyyy-mm-dd on every sample row (non-empty). */
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
  const key = Object.keys(row).find(
    (k) => k.trim().toUpperCase() === columnName.toUpperCase()
  );
  if (!key) return undefined;
  const v = row[key];
  if (v == null) return "";
  return String(v).trim();
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

function normalizeHeaderKey(k) {
  return String(k).trim().toUpperCase();
}

/**
 * @returns {Array<{ type: 'wrongOrMissing' | 'extra', uploaded: string, expected: string | null, position: number }>}
 */
function collectAllHeaderMismatches(fileHeaders) {
  const required = REQUIRED_TRANSACTION_HEADERS;
  const mismatches = [];

  for (let i = 0; i < required.length; i++) {
    const expected = required[i];
    const raw = i < fileHeaders.length ? fileHeaders[i] : "";
    const uploaded = String(raw ?? "").trim();
    if (normalizeHeaderKey(uploaded) !== normalizeHeaderKey(expected)) {
      mismatches.push({
        type: "wrongOrMissing",
        uploaded: uploaded === "" ? "(missing)" : uploaded,
        expected,
        position: i + 1,
      });
    }
  }

  for (let i = required.length; i < fileHeaders.length; i++) {
    const uploaded = String(fileHeaders[i] ?? "").trim() || "(empty)";
    mismatches.push({
      type: "extra",
      uploaded,
      expected: null,
      position: i + 1,
    });
  }

  return mismatches;
}

function buildHeaderMismatchMessage(mismatches) {
  const templateCount = REQUIRED_TRANSACTION_HEADERS.length;
  const lines = mismatches.map((m) => {
    if (m.type === "extra") {
      return `Column ${m.position}: Unexpected header '${m.uploaded}' — the template has exactly ${templateCount} columns in order. Remove extra columns.`;
    }
    return `Column ${m.position}: Header '${m.uploaded}' in your file does not match the expected header '${m.expected}'. Please rename it to '${m.expected}'.`;
  });
  return `One or more headers do not match the template. Fix the issues below and re-upload the file.\n${lines.join("\n")}`;
}

const SAMPLE_DATA_ROW_COUNT = 5;

/**
 * Parses the first SAMPLE_DATA_ROW_COUNT data rows from the CSV buffer.
 */
function parseTempTransactionSampleRows(fileBuffer) {
  return new Promise((resolve, reject) => {
    const text = stripBom(fileBuffer.toString("utf8"));
    const rows = [];
    const source = Readable.from(text);
    const parser = csv();
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
 * 1. Validates the uploaded CSV file.
 * 2. Ensures CSV headers match the template in order; validates date columns on sample rows.
 * 3. Streams the file to Stratus under temp-files/temp-transactions/.
 * 4. Triggers a Catalyst Bulk Write Job to insert into the Transaction table.
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

    const headerKeys = Object.keys(sampleRows[0] || {});
    const headerMismatches = collectAllHeaderMismatches(headerKeys);
    if (headerMismatches.length > 0) {
      return res.status(400).json({
        success: false,
        message: buildHeaderMismatchMessage(headerMismatches),
        mismatches: headerMismatches,
      });
    }

    for (let i = 0; i < sampleRows.length; i++) {
      const rowNumber = i + 2;
      const invalidDateFields = [];

      for (const col of REQUIRED_DATE_COLUMNS) {
        const v = getCell(sampleRows[i], col);
        if (!isValidYyyyMmDd(v)) {
          invalidDateFields.push(col);
        }
      }

      for (const col of OPTIONAL_DATE_COLUMNS) {
        const v = getCell(sampleRows[i], col);
        if (v !== "" && !isValidYyyyMmDd(v)) {
          invalidDateFields.push(col);
        }
      }

      if (invalidDateFields.length > 0) {
        return res.status(400).json({
          success: false,
          message: `Invalid date format (use yyyy-mm-dd) in data row ${rowNumber} for: ${invalidDateFields.join(", ")}.`,
          fields: invalidDateFields,
          row: rowNumber,
        });
      }
    }

    /* ─── 2. UPLOAD TO STRATUS (only after dates validated above) ─── */
    const bucket = catalystApp.stratus().bucket(BUCKET_NAME);

    // Unique key so parallel uploads never collide
    const objectKey = `temp-files/temp-transactions/TxnUpload-${Date.now()}-${file.name}`;

    const passThrough = new PassThrough();

    // Start the upload before piping data so the stream is ready
    const uploadPromise = bucket.putObject(objectKey, passThrough, {
      overwrite: true,
      contentType: "text/csv",
    });

    passThrough.end(file.data);   // Push the file buffer into the stream
    await uploadPromise;           // Wait until Stratus confirms the upload

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
          operation: "insert",   // Use "upsert" if deduplication is needed
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
