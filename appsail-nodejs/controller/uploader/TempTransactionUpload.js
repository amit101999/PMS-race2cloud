import csv from "csv-parser";
import { PassThrough, Readable } from "stream";

const BUCKET_NAME = "temporary-files";
const TABLE_NAME = "Transaction";

const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

/**
 * CSV header (UPPERCASE key) → Transaction table column name.
 * Only mapped columns are kept; extra CSV columns are dropped before upload.
 */
const HEADER_MAP = {
  "BROKER CODE":      "BROKERCODE",
  "BROKERACID":       "WS_Account_code",
  "SYMBOLCODE":       "ISIN",
  "EXCHG":            "EXCHG",
  "TRANSTYPE":        "Tran_Type",
  "DATEPUR_ACQUI":    "TRANDATE",
  "SETDATE":          "SETDATE",
  "QUANTITY":         "QTY",
  "RATE":             "RATE",
  "NET RATE":         "NETRATE",
  "NET AMOUNT":       "Net_Amount",
  "BROKERAGEPERSHARE":"BROKERAGE",
  "SERVICETAX":       "SERVICETAX",
  "TRANEXPENSE":      "STT",
  "ACCRUEDINTEREST":  "ACCRUEDINTEREST",
  "DESCRIPTION":      "Tran_Desc",
  "CHEQUE NUMBER":    "CHEQUENO",
  "CHQDETAIL":        "CHEQUEDTL",
};

/** Must be yyyy-mm-dd on every sample row (non-empty). */
const REQUIRED_DATE_COLUMNS = ["DATEPUR_ACQUI", "SETDATE"];

/** If non-empty, value must be yyyy-mm-dd. */
const OPTIONAL_DATE_COLUMNS = ["CashSetdate"];

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

/**
 * Rewrites a CSV buffer: renames mapped headers to DB column names,
 * drops columns that have no mapping, returns a new Buffer.
 */
function rewriteCsvHeaders(fileBuffer) {
  const text = stripBom(fileBuffer.toString("utf8"));
  const lines = text.split(/\r?\n/);
  if (!lines.length) return fileBuffer;

  const originalHeaders = lines[0].split(",");
  const keepIndices = [];
  const newHeaders = [];

  for (let i = 0; i < originalHeaders.length; i++) {
    const raw = originalHeaders[i].trim();
    const mapped = HEADER_MAP[raw.toUpperCase()];
    if (mapped) {
      keepIndices.push(i);
      newHeaders.push(mapped);
    }
  }

  const newLines = [newHeaders.join(",")];

  for (let r = 1; r < lines.length; r++) {
    const line = lines[r];
    if (!line.trim()) continue;
    const cells = line.split(",");
    const kept = keepIndices.map((idx) => (idx < cells.length ? cells[idx] : ""));
    newLines.push(kept.join(","));
  }

  return Buffer.from(newLines.join("\n"), "utf8");
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
 * 1. Validates the CSV parses and has at least one data row.
 * 2. Validates date columns on sample rows (DATEPUR_ACQUI, SETDATE; optional CashSetdate).
 * 3. Rewrites CSV: renames mapped headers to DB column names, drops unmapped columns.
 * 4. Uploads rewritten CSV to Stratus under temp-files/temp-transactions/.
 * 5. Triggers a Catalyst Bulk Write Job to insert into the Transaction table.
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

    /* ─── 2. REWRITE CSV HEADERS & UPLOAD TO STRATUS ─────────────── */
    const rewrittenCsv = rewriteCsvHeaders(file.data);

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
