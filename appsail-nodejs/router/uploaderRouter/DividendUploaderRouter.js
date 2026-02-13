import express from "express";
import { previewStockDividend, getAllSecuritiesISINs, applyStockDividendMaster} from "../../controller/uploader/DividendUploader.js";
import { exportDividendPreviewFile } from "../../controller/export/exportDividend/exportDividendFile.js";
const router = express.Router();
router.get("/getAllSecuritiesList", getAllSecuritiesISINs);
router.post("/preview", previewStockDividend);
router.post("/apply", applyStockDividendMaster);
router.get("/export-preview", exportDividendPreviewFile);





export default router;