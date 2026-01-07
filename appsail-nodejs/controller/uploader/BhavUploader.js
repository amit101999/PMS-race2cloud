import { PassThrough } from "stream";

/**
 * POST /bhav/upload-bhav
 */
export const uploadBhavFileToStratus = async (req, res) => {
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
    const storedFileName = `bhavcopy-${Date.now()}-${file.name}`;

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

    /* ---------------- TRIGGER BULK IMPORT ---------------- */
    const bulkWrite = datastore.table("Bhav_Copy").bulkJob("write");

    const bulkWriteJob = await bulkWrite.createJob(
      {
        bucket_name: "upload-data-bucket",
        object_key: storedFileName,
        // version_id: "" // optional, only if bucket versioning enabled
      },
      {
        operation: "insert", // insert | update | upsert
        // find_by: "TradDt", // OPTIONAL (enable if you want deduplication)
        // fk_mapping: []     // not required for bhav copy
      }
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
 * POST /bhav/import-bulk
 * Create Catalyst Bulk Write Job from Stratus CSV (manual trigger, still available)
 */
export const triggerBhavBulkImport = async (req, res) => {
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
    const bulkWrite = datastore.table("Bhav_Copy").bulkJob("write");

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