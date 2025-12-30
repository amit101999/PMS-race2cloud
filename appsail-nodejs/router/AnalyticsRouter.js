import Express from "express";
import {
  getAllAccountCodes,
  getHoldingsSummarySimple,
} from "../controller/AnalyticsControllers.js";
const router = Express.Router();

router.get("/getAllAccountCodes", getAllAccountCodes);
router.get("/getHoldingsSummarySimple", getHoldingsSummarySimple);
router.get("/", async (req, res) => {
  res.status(200).json({
    status: "success",
    message: "Analytics API working",
  });
});

export default router;
