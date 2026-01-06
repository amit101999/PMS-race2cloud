import express from "express";
import fileUpload from "express-fileupload";
import { uploadBhavFileToStratus, triggerBhavBulkImport } from "../controller/BhavController.js";
const router = express.Router();
router.use(fileUpload());

router.post("/upload-bhav", uploadBhavFileToStratus);
router.post("/trigger-bhav-import", triggerBhavBulkImport);

export default router;
