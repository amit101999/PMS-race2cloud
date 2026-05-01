import express from "express";
import {
  previewMerger,
  applyMerger,
  getMergerApplyStatus,
} from "../controller/MergerController.js";

const router = express.Router();

router.post("/preview", previewMerger);
router.post("/apply", applyMerger);
router.get("/apply-status", getMergerApplyStatus);

export default router;
