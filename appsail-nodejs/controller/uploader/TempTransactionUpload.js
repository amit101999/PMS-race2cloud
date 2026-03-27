import csv from "csv-parser";
import { PassThrough, Readable } from "stream";

const BUCKET_NAME = "temporary-files";
const TABLE_NAME = "Transaction";

const DATE_FORMAT_ERROR =
  "Date format in sheet is not yyyy-mm-dd. Please update the date format and re-upload it.";

const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

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

const SAMPLE_DATA_ROW_COUNT = 5;

/**
 * Parses the first SAMPLE_DATA_ROW_COUNT data rows and ensures TRANDATE and SETDATE
 * are present and each value matches yyyy-mm-dd (calendar-valid).
 */
function validateTempTransactionDateColumns(fileBuffer) {
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
 * POST /api/temp-transaction/upload
 *temporary-files
 * 1. Validates the uploaded CSV file.
 * 2. Parses sample data rows and validates TRANDATE / SETDATE as yyyy-mm-dd.
 *    (No Stratus or bulk job work runs until this passes — wrong dates return 400.)
 * 3. Streams the file to the Stratus bucket under temp-files/temp-transactions/.
 * 4. Triggers a Catalyst Bulk Write Job to insert the CSV rows into the Transaction table.
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
      sampleRows = await validateTempTransactionDateColumns(file.data);
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
          "The CSV has no data rows. Add at least one row with TRANDATE and SETDATE before uploading.",
      });
    }

    const headerKeys = Object.keys(sampleRows[0] || {});
    const hasTrandateCol = headerKeys.some(
      (k) => k.trim().toUpperCase() === "TRANDATE"
    );
    const hasSetdateCol = headerKeys.some(
      (k) => k.trim().toUpperCase() === "SETDATE"
    );
    if (!hasTrandateCol || !hasSetdateCol) {
      return res.status(400).json({
        success: false,
        message:
          "The CSV must include TRANDATE and SETDATE columns (header names).",
      });
    }

    for (let i = 0; i < sampleRows.length; i++) {
      const trandate = getCell(sampleRows[i], "TRANDATE");
      const setdate = getCell(sampleRows[i], "SETDATE");
      if (!isValidYyyyMmDd(trandate) || !isValidYyyyMmDd(setdate)) {
        return res.status(400).json({
          success: false,
          message: DATE_FORMAT_ERROR,
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
