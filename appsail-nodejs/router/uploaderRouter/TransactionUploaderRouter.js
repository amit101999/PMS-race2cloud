import express from "express";
import fileUpload from "express-fileupload";

import { uploadTempTransactionFile } from "../../controller/uploader/TransactionUploader.js";

const router = express.Router();

router.use(fileUpload());

router.post("/upload-transaction", uploadTempTransactionFile);

export default router;
