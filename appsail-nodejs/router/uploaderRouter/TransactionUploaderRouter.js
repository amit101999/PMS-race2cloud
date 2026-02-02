import express from "express";

import fileUpload from "express-fileupload";
import {
  simulateHoldings,
  triggerTransactionBulkImport,
  uploadTransactionFileToStratus,
  compareWithCustodian,
} from "../../controller/uploader/TransactionUploader.js";

const router = express.Router();
router.use(fileUpload());

router.post("/upload-transaction", uploadTransactionFileToStratus);
router.post("/trigger-transaction-import", triggerTransactionBulkImport);
router.post("/parse-data", simulateHoldings);
router.post("/compare-custodian", compareWithCustodian);

export default router;
