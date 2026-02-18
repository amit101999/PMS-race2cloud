import express from "express";
import fileUpload from "express-fileupload";

import {
  uploadTempTransactionFile,
  loadHolding,
  startDifferentialReport,
  getDifferentialReportStatus,
  getDifferentialReportHistory,
  downloadDifferentialReport,
} from "../../controller/uploader/TransactionUploader.js";

const router = express.Router();

router.use(fileUpload());

router.post("/upload-transaction", uploadTempTransactionFile);
router.get("/load-holding", loadHolding);
router.post("/differential-report", startDifferentialReport);
router.get("/differential-report/status", getDifferentialReportStatus);
router.get("/differential-report/history", getDifferentialReportHistory);
router.get("/differential-report/download", downloadDifferentialReport);

export default router;
