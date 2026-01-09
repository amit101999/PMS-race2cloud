import express from "express";
import { exportDataPerAccount } from "../../controller/export/exportHolding/exportSingleClientHolding.js";
import { exportAllData } from "../../controller/export/exportHolding/ExportAllHolding.js";

const router = express.Router();

router.get("/export-all", exportAllData);
router.get("/export-single", exportDataPerAccount);

export default router;
