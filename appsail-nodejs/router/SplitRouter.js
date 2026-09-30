import express from "express";
import {
  addStockSplit,
  getAllSecuritiesISINs,
  previewStockSplit,
  getSplitApplyStatus,
  sendSplitReceivable,
  getSplitHistory,
  getSplitHistoryDownload,
} from "../controller/uploader/SplitController.js";
import { exportSplitPreviewFile } from "../controller/export/exportSplit/exportSplitFile.js";
import {
  compareWithCustodian,
  compareSplitWithBenefit,
} from "../controller/corporateAction/CustodianUploadController.js";
const router = express.Router();

router.post("/add", addStockSplit);
router.post("/preview", previewStockSplit);
// Stage 1: vs the Ex-Entitlement file, then mark every account Receivable.
router.post("/compare-custodian", compareWithCustodian("split"));
router.post("/send-receivable", sendSplitReceivable);
// Stage 2: Receivable accounts vs the Corporate Benefit Collection file.
router.post("/compare-benefit", compareSplitWithBenefit);

router.get("/getAllSecuritiesList", getAllSecuritiesISINs);
router.get("/export-preview", exportSplitPreviewFile);
router.get("/job-status", getSplitApplyStatus);
router.get("/history", getSplitHistory);
router.get("/history/download", getSplitHistoryDownload);
export default router;
