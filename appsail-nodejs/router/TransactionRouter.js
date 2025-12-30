import Express from "express";
import {
  getStockTransactionHistory,
  getTotalBuyQty,
  getTotalSellQty,
} from "../controller/TransactionController.js";
const router = Express.Router();

router.get("/getStockTransactionHistory", getStockTransactionHistory);
router.get("/getAllBuys", getTotalBuyQty);
router.get("/getAllSells", getTotalSellQty);
router.get("/", async (req, res) => {
  res.status(200).json({
    status: "success",
    message: "Analytics API working",
  });
});

export default router;
