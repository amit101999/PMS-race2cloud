import express from "express";
import {
  postUpdateIsin,
  getIsinUpdateJobStatus,
} from "../controller/isin/updateIsin.js";

const router = express.Router();

router.post("/update", postUpdateIsin);
router.get("/job-status", getIsinUpdateJobStatus);

export default router;
