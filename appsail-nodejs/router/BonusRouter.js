import express from "express";
import {
  getAllSecuritiesISINs,
  previewStockBonus,
  applyStockBonus,
  getBonusApplyStatus,
} from "../controller/BonusController.js";
import { exportBonusPreviewFile } from "../controller/export/exportBonus/exportBonusFile.js";

const router = express.Router();

router.get("/getAllSecuritiesList", getAllSecuritiesISINs);
router.post("/preview", previewStockBonus);
router.get("/export-preview", exportBonusPreviewFile);
router.post("/apply", applyStockBonus);
router.get("/apply-status", getBonusApplyStatus);

export default router;
