import express from "express";
import fileUpload from "express-fileupload";

import { uploadTempTransaction } from "../../controller/uploader/TempTransactionUpload.js";

const router = express.Router();

// router.use(fileUpload());

// POST /api/transaction-uploader/upload-temp-file
router.post("/upload-temp-file", uploadTempTransaction);

export default router;
