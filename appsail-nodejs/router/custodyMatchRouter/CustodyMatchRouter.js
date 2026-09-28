import express from "express";
import fileUpload from "express-fileupload";

import {
  deleteDayFiles,
  deleteUploadedFile,
  downloadObject,
  health,
  listMatchDates,
  listCustodyUploads,
  listSchemes,
  listUploadedFiles,
  uploadCustodyFile,
  uploadFaFile,
} from "../../controller/custodyMatch/CustodyMatchUpload.js";
import {
  exportExceptions,
  getExceptions,
  getLatestRun,
  getRunStatus,
  listReports,
  proceedToImport,
  retryRun,
  startRun,
} from "../../controller/custodyMatch/CustodyMatchRun.js";
import {
  deleteFieldMap,
  getAllFieldMaps,
  getFieldMap,
  previewHeaders,
  saveFieldMap,
} from "../../controller/custodyMatch/CustodyMatchFieldMap.js";

const router = express.Router();

/**
 * File upload middleware, as the transaction uploader router already does.
 *
 * A 15,000-row CSV is roughly 3 MB, so 25 MB is generous headroom while still
 * rejecting an obviously wrong file before it is read into memory. abortOnLimit
 * makes that rejection immediate instead of silently truncating the buffer —
 * a truncated CSV parses cleanly and reconciles wrongly.
 */
router.use(
  fileUpload({
    limits: { fileSize: 25 * 1024 * 1024 },
    abortOnLimit: true,
    responseOnLimit: "File is larger than the 25 MB limit for a transaction or custodian file.",
  }),
);

// GET /api/custody-match/health — bucket reachability and read/write/delete probe
router.get("/health", health);

/*
 * Field mapping — which column in the user's file feeds which internal field.
 *
 * Registered BEFORE /day/:date and the run routes so "field-map" can never be
 * captured by a :param segment.
 */

// GET /api/custody-match/field-map — all three profiles plus their field catalogs
router.get("/field-map", getAllFieldMaps);

// POST /api/custody-match/field-map/preview — read a sample file's header row only
router.post("/field-map/preview", previewHeaders);

// GET /api/custody-match/field-map/:type — one profile
router.get("/field-map/:type", getFieldMap);

// PUT /api/custody-match/field-map/:type — save a profile (supersedes stored runs)
router.put("/field-map/:type", saveFieldMap);

// DELETE /api/custody-match/field-map/:type — remove a saved profile (supersedes stored runs)
router.delete("/field-map/:type", deleteFieldMap);

// GET /api/custody-match/schemes — schemes already used, with their layout
router.get("/schemes", listSchemes);

// GET /api/custody-match/dates — every date with active files, for the Match dialog
router.get("/dates", listMatchDates);

// GET /api/custody-match/files?uploadDate=&date= — files saved under an upload date
router.get("/files", listUploadedFiles);

// DELETE /api/custody-match/file?key=... — one staged file, either side
router.delete("/file", deleteUploadedFile);

// DELETE /api/custody-match/day/:date?side=fa|custody|all
router.delete("/day/:date", deleteDayFiles);

// POST /api/custody-match/fa-file — validate and stage the 26-column transaction file
router.post("/fa-file", uploadFaFile);

// POST /api/custody-match/custody-file — validate and store one custodian deal/allocation file
router.post("/custody-file", uploadCustodyFile);

// GET /api/custody-match/download?key=… — 1-hour presigned GET
router.get("/download", downloadObject);

// GET /api/custody-match/custody-history — the last few custodian uploads, for download
router.get("/custody-history", listCustodyUploads);

/* ── matching ──────────────────────────────────────────────────────────── */

// GET /api/custody-match/reports — past runs that found differences
router.get("/reports", listReports);

// GET /api/custody-match/latest-run/:date — restore the screen on reload
router.get("/latest-run/:date", getLatestRun);

// POST /api/custody-match/run — GATE 1
router.post("/run", startRun);

// GET /api/custody-match/run/:runId/exceptions?page=&size=10&exceptionType=&scheme=&status=&q=&from=&to=
router.get("/run/:runId/exceptions", getExceptions);

// GET /api/custody-match/run/:runId/exceptions/export
router.get("/run/:runId/exceptions/export", exportExceptions);

// POST /api/custody-match/run/:runId/retry — system failures only
router.post("/run/:runId/retry", retryRun);

// POST /api/custody-match/run/:runId/proceed — GATE 2
router.post("/run/:runId/proceed", proceedToImport);

// GET /api/custody-match/run/:runId — poll target
router.get("/run/:runId", getRunStatus);

export default router;
