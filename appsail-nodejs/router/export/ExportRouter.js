import express from "express";
import { exportDataPerAccount } from "../../controller/export/exportHolding/exportSingleClientHolding.js";
import {
  exportAllData,
  getExportAllJobStatus,
  downloadExportFile,
  getExportAllHistory,
} from "../../controller/export/exportHolding/ExportAllHolding.js";
import { exportTransactionPerAccount } from "../../controller/export/exportTransaction/exportSingleClient.js";

const router = express.Router();

// holding export
router.get("/export-all", exportAllData);
router.get("/export-single", exportDataPerAccount);
router.get("/check-status", getExportAllJobStatus);
router.get("/download", downloadExportFile);
router.get("/export-all/history", getExportAllHistory);

// transaction export
router.get("/transaction/export-single", exportTransactionPerAccount);

export default router;
