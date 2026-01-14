import express from "express";
import { exportDataPerAccount } from "../../controller/export/exportHolding/exportSingleClientHolding.js";
import {
  exportAllData,
  getExportAllJobStatus,
  downloadExportFile,
  getExportAllHistory,
} from "../../controller/export/exportHolding/ExportAllHolding.js";

const router = express.Router();

router.get("/export-all", exportAllData);
router.get("/export-single", exportDataPerAccount);
router.get("/check-status", getExportAllJobStatus);
router.get("/download", downloadExportFile);
router.get("/export-all/history", getExportAllHistory);

export default router;
