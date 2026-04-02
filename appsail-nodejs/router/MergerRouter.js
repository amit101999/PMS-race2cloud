import express from "express";
import { previewMerger, applyMerger } from "../controller/MergerController.js";

const router = express.Router();

router.post("/preview", previewMerger);
router.post("/apply", applyMerger);

export default router;
