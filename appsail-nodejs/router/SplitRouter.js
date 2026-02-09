import express from "express";
import {
  addStockSplit,
  getAllSecuritiesISINs,
  previewStockSplit,
} from "../controller/uploader/SplitController.js";
import { exportSplitPreviewFile } from "../controller/export/exportSplit/exportSplitFile.js";
const router = express.Router();

router.post("/add", addStockSplit);
router.post("/preview", previewStockSplit);

router.get("/getAllSecuritiesList", getAllSecuritiesISINs);
router.get("/export-preview", exportSplitPreviewFile);
export default router;
