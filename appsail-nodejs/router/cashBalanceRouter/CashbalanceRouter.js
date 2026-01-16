import Express from "express";
import { calculateCashBalanceJob } from "../../controller/cashBalance/calculateBalanceOnce.js";

const router = Express.Router();

router.get("/calculateCashbalanceOnce", calculateCashBalanceJob);

export default router;
