import Express from "express";
import {
  getStockTransactionHistory,
  getTotalBuyQty,
  getTotalSellQty,
  getCommitDates,
  getCommittedTransactions,
} from "../controller/TransactionController.js";
const router = Express.Router();

router.get("/getStockTransactionHistory", getStockTransactionHistory);
router.get("/getAllBuys", getTotalBuyQty);
router.get("/getAllSells", getTotalSellQty);
// Transaction Deletion page (read-only preview)
router.get("/commit-dates", getCommitDates);
router.get("/committed", getCommittedTransactions);
router.get("/", async (req, res) => {
  res.status(200).json({
    status: "success",
    message: "Analytics API working",
  });
});

export default router;
