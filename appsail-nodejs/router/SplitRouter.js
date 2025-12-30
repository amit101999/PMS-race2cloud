import express from "express";
import { addStockSplit,getAllSecurityCodes } from "../controller/SplitController.js";

const router = express.Router();

router.post("/add", addStockSplit);

router.get("/getAllSecurityCodes", getAllSecurityCodes);

export default router;
