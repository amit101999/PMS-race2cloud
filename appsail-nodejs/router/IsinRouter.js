import express from "express";
import {
  postUpdateIsin,
  getIsinUpdateJobStatus,
  getSecurityListIsins,
} from "../controller/isin/updateIsin.js";

const router = express.Router();

router.get("/security-list-isins", getSecurityListIsins);
router.post("/update", postUpdateIsin);
router.get("/job-status", getIsinUpdateJobStatus);

export default router;
