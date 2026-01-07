import express from "express";

import fileUpload from "express-fileupload";
import { triggerTransactionBulkImport, uploadTransactionFileToStratus } from "../../controller/uploader/TransactionUploader.js";

const router = express.Router();
router.use(fileUpload());

router.post("/upload-transaction", uploadTransactionFileToStratus);
router.post("/trigger-transaction-import", triggerTransactionBulkImport);

export default router;
