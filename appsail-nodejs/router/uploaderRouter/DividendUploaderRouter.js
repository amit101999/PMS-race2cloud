import express from "express";
import {
  previewStockDividend,
  getAllSecuritiesISINs,
  applyStockDividendMaster,
  getDividendApplyStatus,
  sendDividendReceivable,
} from "../../controller/uploader/DividendUploader.js";
import { compareDividendWithBenefit } from "../../controller/corporateAction/CustodianUploadController.js";
import { exportDividendPreviewFile, getDividendExportStatus, downloadDividendExportFile } from "../../controller/export/exportDividend/exportDividendFile.js";
const router = express.Router();
router.get("/getAllSecuritiesList", getAllSecuritiesISINs);
// The custodian file comes from the Custodian Upload page, so /preview takes JSON only.
// Stage 1: /preview with compare: true is the Ex-Entitlement comparison.
router.post("/preview", previewStockDividend);
router.post("/send-receivable", sendDividendReceivable);
// Stage 2: Receivable accounts vs the Corporate Benefit Collection file; /apply confirms.
router.post("/compare-benefit", compareDividendWithBenefit);
router.post("/apply", applyStockDividendMaster);
router.get("/apply-status", getDividendApplyStatus);
router.get("/export-preview", exportDividendPreviewFile);
router.get("/export-status", getDividendExportStatus);
router.get("/export-download", downloadDividendExportFile);





export default router;
