import express from "express";
import {
  previewDemerger,
  applyDemerger,
} from "../controller/uploader/DemergerController.js";

const router = express.Router();

router.post("/preview", previewDemerger);
router.post("/apply", applyDemerger);

export default router;
