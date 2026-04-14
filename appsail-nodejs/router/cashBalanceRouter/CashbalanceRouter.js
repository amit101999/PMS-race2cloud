import Express from "express";
import { calculateCashBalanceJob } from "../../controller/cashBalance/calculateBalanceOnce.js";
import { getCashPassbook, getIsinList } from "../../controller/cashBalance/cashPassbookController.js";
import { triggerCashBalExport, getCashBalExportStatus, downloadCashBalExport } from "../../controller/cashBalance/exportCashBalance.js";

const router = Express.Router();

router.get("/calculateCashbalanceOnce", calculateCashBalanceJob);
router.get("/passbook", getCashPassbook);
router.get("/isins", getIsinList);
router.get("/export", triggerCashBalExport);
router.get("/export/status", getCashBalExportStatus);
router.get("/export/download", downloadCashBalExport);

export default router;
