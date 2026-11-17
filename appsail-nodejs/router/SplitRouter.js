import express from "express";
import {
  addStockSplit,
  addStockSplitBypass,
  getAllSecuritiesISINs,
  previewStockSplit,
  getSplitApplyStatus,
  sendSplitReceivable,
} from "../controller/uploader/SplitController.js";
import { exportSplitPreviewFile } from "../controller/export/exportSplit/exportSplitFile.js";
import {
  compareWithBenefit,
  compareWithCustodian,
  downloadDifferenceFile,
  listDifferenceHistory,
} from "../controller/corporateAction/CustodianUploadController.js";
const router = express.Router();

router.post("/add", addStockSplit);
// Temporary: apply without the custodian files (no Receivable / Benefit check).
router.post("/apply-bypass", addStockSplitBypass);
router.post("/preview", previewStockSplit);
// Stage 1: vs the Ex-Entitlement file, then mark every account Receivable.
router.post("/compare-custodian", compareWithCustodian("split"));
router.post("/send-receivable", sendSplitReceivable);
// Stage 2: Receivable accounts vs the Corporate Benefit Collection file.
router.post("/compare-benefit", compareWithBenefit("split"));

router.get("/getAllSecuritiesList", getAllSecuritiesISINs);
router.get("/export-preview", exportSplitPreviewFile);
router.get("/job-status", getSplitApplyStatus);
router.get("/history", listDifferenceHistory("split"));
router.get("/history/download", downloadDifferenceFile("split"));
export default router;
