import express from "express";
import {
  getAllSecuritiesISINs,
  previewStockBonus,
  applyStockBonus,
} from "../controller/BonusController.js";

const router = express.Router();

router.get("/getAllSecuritiesList", getAllSecuritiesISINs);
router.post("/preview", previewStockBonus);
router.post("/apply", applyStockBonus);

export default router;
