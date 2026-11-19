import express from "express";
import {
  getAllSecuritiesISINs,
  previewStockBonus,
  applyStockBonus,
  applyStockBonusBypass,
  getBonusApplyStatus,
  getBonusApplyProgress,
  sendBonusReceivable,
} from "../controller/BonusController.js";
import { exportBonusPreviewFile } from "../controller/export/exportBonus/exportBonusFile.js";
import {
  compareWithBenefit,
  compareWithCustodian,
  downloadDifferenceFile,
  listDifferenceHistory,
} from "../controller/corporateAction/CustodianUploadController.js";

const router = express.Router();

router.get("/getAllSecuritiesList", getAllSecuritiesISINs);
router.post("/preview", previewStockBonus);
// Stage 1: vs the Ex-Entitlement file, then mark every account Receivable.
router.post("/compare-custodian", compareWithCustodian("bonus"));
router.post("/send-receivable", sendBonusReceivable);
// Stage 2: Receivable accounts vs the Corporate Benefit Collection file; /apply confirms.
router.post("/compare-benefit", compareWithBenefit("bonus"));
router.get("/history", listDifferenceHistory("bonus"));
router.get("/history/download", downloadDifferenceFile("bonus"));
router.get("/export-preview", exportBonusPreviewFile);
router.post("/apply", applyStockBonus);
// Temporary: apply without the custodian files (no Receivable / Benefit check).
router.post("/apply-bypass", applyStockBonusBypass);
router.get("/apply-status", getBonusApplyStatus);
router.get("/apply-progress", getBonusApplyProgress);

export default router;
