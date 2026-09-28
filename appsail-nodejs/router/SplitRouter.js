import express from "express";
import {
  addStockSplit,
  getAllSecuritiesISINs,
  previewStockSplit,
  getSplitApplyStatus,
} from "../controller/uploader/SplitController.js";
import { exportSplitPreviewFile } from "../controller/export/exportSplit/exportSplitFile.js";
import { compareWithCustodian } from "../controller/corporateAction/CustodianUploadController.js";
const router = express.Router();

router.post("/add", addStockSplit);
router.post("/preview", previewStockSplit);
router.post("/compare-custodian", compareWithCustodian("split"));

router.get("/getAllSecuritiesList", getAllSecuritiesISINs);
router.get("/export-preview", exportSplitPreviewFile);
router.get("/job-status", getSplitApplyStatus);
export default router;
