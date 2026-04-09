import Express from "express";
import { calculateCashBalanceJob } from "../../controller/cashBalance/calculateBalanceOnce.js";
import { getCashPassbook } from "../../controller/cashBalance/cashPassbookController.js";

const router = Express.Router();

router.get("/calculateCashbalanceOnce", calculateCashBalanceJob);
router.get("/passbook", getCashPassbook);

export default router;
