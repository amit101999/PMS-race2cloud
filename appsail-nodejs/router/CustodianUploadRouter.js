import express from "express";
import fileUpload from "express-fileupload";

import {
  deleteCustodianFile,
  downloadCustodianFile,
  getDay,
  getLatestForCorpAction,
  listHistory,
  uploadCustodianFile,
} from "../controller/corporateAction/CustodianUploadController.js";

const router = express.Router();

router.use((req, res, next) => {
  if (req.catalystApp) return next();
  return res
    .status(500)
    .json({ success: false, code: "CATALYST_NOT_INITIALIZED", message: "Catalyst not initialized" });
});

// abortOnLimit rejects an oversized file outright instead of silently truncating it.
router.use(
  fileUpload({
    limits: { fileSize: 25 * 1024 * 1024 },
    abortOnLimit: true,
    responseOnLimit: "File is larger than the 25 MB limit for a custodian file.",
  }),
);

// `kind` = entitlement (default) | allocation picks which of the two custodian files.

// GET /api/custodian-upload/day/:date?kind= — the newest file of that kind uploaded for the date
router.get("/day/:date", getDay);

// GET /api/custodian-upload/latest?caType=split|bonus|dividend&isin= — newest entitlement file + its matching row count
// GET /api/custodian-upload/latest?kind=allocation&isin= — newest allocation file + its row count for the ISIN
router.get("/latest", getLatestForCorpAction);

// GET /api/custodian-upload/history?kind= — the last 10 uploads of that kind, for Previous Uploads
router.get("/history", listHistory);

// GET /api/custodian-upload/download?key=... — presigned URL for one uploaded file
router.get("/download", downloadCustodianFile);

// POST /api/custodian-upload/file (form field kind) — check against the saved mapping and store
router.post("/file", uploadCustodianFile);

// DELETE /api/custodian-upload/file?key=... — remove the file (every upload of its kind for its date)
router.delete("/file", deleteCustodianFile);

export default router;
