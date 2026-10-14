import Express from "express";
import {
  getStockTransactionHistory,
  getTotalBuyQty,
  getTotalSellQty,
  getCommitDates,
  getCommittedTransactions,
  deleteTransactions,
  getDeletionDates,
  getDeletedTransactions,
} from "../controller/TransactionController.js";
const router = Express.Router();

router.get("/getStockTransactionHistory", getStockTransactionHistory);
router.get("/getAllBuys", getTotalBuyQty);
router.get("/getAllSells", getTotalSellQty);
// Transaction Deletion page
router.get("/commit-dates", getCommitDates);
router.get("/committed", getCommittedTransactions);
router.post("/delete", deleteTransactions);
router.get("/deletion-dates", getDeletionDates);
router.get("/deleted", getDeletedTransactions);
router.get("/", async (req, res) => {
  res.status(200).json({
    status: "success",
    message: "Analytics API working",
  });
});

export default router;
