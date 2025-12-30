import Express from "express";
import { getAllTransactions } from "../controller/DashboardController.js";

const router = Express.Router();

router.get("/getAllTransactions", getAllTransactions);
router.get("/", async (req, res) => {
  res.status(200).json({
    status: "success",
    message: "Analytics API working",
  });
});

export default router;
