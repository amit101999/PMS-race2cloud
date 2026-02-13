import { PassThrough } from "stream";

/**
 * POST /transaction-uploader/upload-transaction
 * 1. Upload CSV file to Stratus
 * 2. Create bulk write job to insert rows into Transaction table
 */
export const uploadTempTransactionFile = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const stratus = catalystApp.stratus();
    const datastore = catalystApp.datastore();
    const bucket = stratus.bucket("upload-data-bucket");

    const file = req.files?.file;
    if (!file) {
      return res.status(400).json({ message: "CSV file is required" });
    }
    if (!file.name.endsWith(".csv")) {
      return res.status(400).json({ message: "Only CSV files are allowed" });
    }

    const storedFileName = `transaction-${Date.now()}-${file.name}`;

    const passThrough = new PassThrough();
    const uploadPromise = bucket.putObject(storedFileName, passThrough, {
      overwrite: true,
      contentType: "text/csv",
    });
    passThrough.end(file.data);
    await uploadPromise;

    const bulkWrite = datastore.table("Transaction").bulkJob("write");
    const bulkWriteJob = await bulkWrite.createJob(
      {
        bucket_name: "upload-data-bucket",
        object_key: storedFileName,
      },
      { operation: "insert" }
    );

    return res.status(200).json({
      message: "File uploaded to Stratus and bulk insert to Transaction table started",
      fileName: storedFileName,
      bucket: "upload-data-bucket",
      jobId: bulkWriteJob.job_id,
      status: bulkWriteJob.status,
    });
  } catch (error) {
    console.error("Transaction upload error:", error);
    return res.status(500).json({
      message: "Failed to upload file or create bulk import job",
      error: error.message,
    });
  }
};


