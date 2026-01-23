import express from "express";
import {
  addStockSplit,
  getAllSecuritiesISINs,
} from "../controller/SplitController.js";

const router = express.Router();

router.post("/add", addStockSplit);

router.get("/getAllSecuritiesList", getAllSecuritiesISINs);

export default router;
