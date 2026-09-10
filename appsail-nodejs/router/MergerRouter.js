import express from "express";
import {
  previewMerger,
  applyMerger,
  getMergerApplyStatus,
  getMergerApplyProgress,
} from "../controller/MergerController.js";

const router = express.Router();

router.post("/preview", previewMerger);
router.post("/apply", applyMerger);
router.get("/apply-status", getMergerApplyStatus);
router.get("/apply-progress", getMergerApplyProgress);

export default router;
