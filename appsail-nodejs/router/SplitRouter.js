import express from "express";
import {
  addStockSplit,
  getAllSecuritiesISINs,
  previewStockSplit,
} from "../controller/uploader/SplitController.js";

const router = express.Router();

router.post("/add", addStockSplit);
router.post("/preview", previewStockSplit);

router.get("/getAllSecuritiesList", getAllSecuritiesISINs);

export default router;
