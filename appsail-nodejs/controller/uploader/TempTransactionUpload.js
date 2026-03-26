import { PassThrough } from "stream";

const BUCKET_NAME = "temporary-files";
const TABLE_NAME = "Transaction";

/**
 * POST /api/temp-transaction/upload
 *temporary-files
 * 1. Validates the uploaded CSV file.
 * 2. Streams the file to the Stratus bucket under temp-files/temp-transactions/.
 * 3. Triggers a Catalyst Bulk Write Job to insert the CSV rows into the Transaction table.
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

    /* ─── 2. UPLOAD TO STRATUS ────────────────────────────────────── */
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
