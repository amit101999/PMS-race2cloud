import express from "express";
import {
  addStockBonus,
  getAllSecuritiesISINs,
} from "../controller/BonusController.js";

const router = express.Router();

router.post("/add", addStockBonus);

router.get("/getAllSecuritiesList", getAllSecuritiesISINs);

export default router;
