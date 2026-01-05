import express from "express";
import { exportAllData } from "../controller/ExportController.js";

const router = express.Router();

router.get("/export-all", exportAllData);

export default router;
