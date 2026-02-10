import express from "express";

import fileUpload from "express-fileupload";
import {
  getFifoPageHandler,
  getDiffPageHandler,
  uploadTempTransactionFile,
  uploadTempCustodianFile,
  startDifferentialExportJob,
  getDifferentialExportStatus,
  listDifferentialExportJobs,
} from "../../controller/uploader/TransactionUploader.js";

const router = express.Router();
router.use(fileUpload());

router.post("/upload-temp-transaction", uploadTempTransactionFile);
router.post("/upload-temp-custodian", uploadTempCustodianFile);
router.get("/fifo-page", getFifoPageHandler);
router.get("/diff-page", getDiffPageHandler);

router.post("/export-differential-report", startDifferentialExportJob);
router.get("/export-differential-report/status", getDifferentialExportStatus);
router.get("/export-differential-report/list", listDifferentialExportJobs);

export default router;
