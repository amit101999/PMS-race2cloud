import express from "express";
import fileUpload from "express-fileupload";

import {
  deleteCustodyFile,
  downloadFile,
  listCustodyHistory,
  listDifferenceReports,
  listFaHistory,
  uploadCustodyFile,
} from "../../controller/holdingComparison/HoldingComparisonUpload.js";
import {
  generateFaHoldings,
  getFaHoldingsStatus,
} from "../../controller/holdingComparison/HoldingComparisonFaHoldings.js";
import {
  exportExceptions,
  getDay,
  getExceptions,
  getRunStatus,
  startRun,
} from "../../controller/holdingComparison/HoldingComparisonRun.js";

const router = express.Router();

router.use((req, res, next) => {
  if (req.catalystApp) return next();
  return res
    .status(500)
    .json({ success: false, code: "CATALYST_NOT_INITIALIZED", message: "Catalyst not initialized" });
});

// Same limits as the custody-match router; abortOnLimit rejects an oversized
// file outright instead of silently truncating it.
router.use(
  fileUpload({
    limits: { fileSize: 25 * 1024 * 1024 },
    abortOnLimit: true,
    responseOnLimit: "File is larger than the 25 MB limit for a custodian holdings file.",
  }),
);

// GET /api/holding-comparison/day/:date — staged file, FA holdings status, latest run
router.get("/day/:date", getDay);

// POST /api/holding-comparison/custody-file — validate and stage the custodian file
router.post("/custody-file", uploadCustodyFile);

// DELETE /api/holding-comparison/custody-file?key=... — remove one uploaded custodian file
router.delete("/custody-file", deleteCustodyFile);

// POST /api/holding-comparison/fa-holdings/generate — start FA holdings generation
router.post("/fa-holdings/generate", generateFaHoldings);

// GET /api/holding-comparison/fa-holdings/:date/status — poll target for generation
router.get("/fa-holdings/:date/status", getFaHoldingsStatus);

// The three "previous files" lists (last 10 each) and their download
router.get("/custody-history", listCustodyHistory);
router.get("/fa-history", listFaHistory);
router.get("/reports", listDifferenceReports);
router.get("/download", downloadFile);

// POST /api/holding-comparison/run — Gate 1
router.post("/run", startRun);

// GET /api/holding-comparison/run/:runId/exceptions — first 10 rows of the report
router.get("/run/:runId/exceptions", getExceptions);

// GET /api/holding-comparison/run/:runId/export — full difference report
router.get("/run/:runId/export", exportExceptions);

// GET /api/holding-comparison/run/:runId — poll target
router.get("/run/:runId", getRunStatus);

export default router;
