/**
 * Holding Comparison — custodian file staging and the "previous files" lists.
 *
 * Staging only writes to the temporary-files bucket. Nothing in production is
 * touched, and replacing the file simply stages a newer one for the date.
 */

import {
  CUSTODY_PROFILE_TYPE,
  DATE_PATTERN,
  RUN_STATUS,
} from "../../util/holdingComparison/constants.js";
import { parseCustodyHoldings } from "../../util/holdingComparison/parse.js";
import { listRunsForDate, supersedeRuns } from "../../util/holdingComparison/runStore.js";
import {
  FOLDER,
  buildCustodyKey,
  getBucket,
  isModuleKey,
  listRecent,
  markCustodyCleared,
  parseKey,
  readFieldMapProfile,
} from "../../util/holdingComparison/storage.js";
import { STRATUS_GET_URL_OPTS, stratusSignedUrlToString } from "../../util/stratusSignedUrl.js";

const fail = (res, status, body) => res.status(status).json({ success: false, ...body });

/**
 * POST /api/holding-comparison/custody-file  (multipart: file, businessDate)
 *
 * Validates the whole file before storing it. A header mismatch comes back as
 * HEADER_MISMATCH, which the screen answers by opening the Field Mapping popup.
 */
export const uploadCustodyFile = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;

    const file = req.files?.file;
    if (!file) {
      return fail(res, 400, {
        code: "NO_FILE",
        message: "CSV file is required. Send it as form-data with key 'file'.",
      });
    }
    if (!/\.csv$/i.test(String(file.name || ""))) {
      return fail(res, 400, { code: "NOT_CSV", message: "Only CSV files are allowed." });
    }

    const businessDate = String(req.body?.businessDate || "").trim();
    if (!DATE_PATTERN.test(businessDate)) {
      return fail(res, 400, { code: "BAD_DATE", message: "businessDate must be yyyy-mm-dd." });
    }

    const bucket = getBucket(catalystApp);
    const profile = await readFieldMapProfile(bucket, CUSTODY_PROFILE_TYPE);
    const parsed = await parseCustodyHoldings(file.data, profile);

    if (parsed.fileError) {
      const status = parsed.fileError.code === "HEADER_MISMATCH" ? 422 : 400;
      return fail(res, status, { ...parsed.fileError, fileName: file.name });
    }
    if (!parsed.ok) {
      return fail(res, 422, {
        code: "ROW_VALIDATION_FAILED",
        message: `${parsed.rowErrorCount} problem(s) found in ${file.name}. Nothing was uploaded.`,
        fileName: file.name,
        rowErrors: parsed.rowErrors,
        rowErrorCount: parsed.rowErrorCount,
        moreRowErrors: Math.max(parsed.rowErrorCount - parsed.rowErrors.length, 0),
      });
    }
    if (parsed.rowCount === 0) {
      return fail(res, 400, { code: "NO_ROWS", message: `${file.name} has no holding rows.` });
    }

    const key = buildCustodyKey(businessDate, Date.now(), file.name);
    await bucket.putObject(key, file.data, { contentType: "text/csv", overwrite: true });

    // The staged file changed, so earlier answers for this date no longer describe it.
    const superseded = await supersedeRuns(catalystApp, businessDate);

    return res.status(201).json({
      success: true,
      message: `${file.name} staged for ${businessDate}.`,
      // Returned in full because Stratus listings lag a fresh write by ~20 seconds.
      file: { ...parseKey(key), sizeBytes: file.data.length },
      rowCount: parsed.rowCount,
      holdingCount: parsed.holdings.size,
      supersededRuns: superseded.runIds,
    });
  } catch (error) {
    console.error("[HoldingComparison] uploadCustodyFile:", error);
    return fail(res, 500, { message: error.message || "Could not upload the custodian file." });
  }
};

/**
 * DELETE /api/holding-comparison/custody-file?key=...
 *
 * For a file uploaded by mistake: deletes that upload and leaves the date with
 * no staged custodian file, so the correct one can be uploaded. Earlier uploads
 * for the date are kept (they stay in the history list) but are not staged
 * again. See markCustodyCleared.
 */
export const deleteCustodyFile = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const key = String(req.query?.key || "").trim();
    const parsed = parseKey(key);
    // Only custodian uploads can be removed here, never FA holdings or reports.
    if (!parsed || !key.startsWith(`${FOLDER.CUSTODY}/`)) {
      return fail(res, 400, { code: "BAD_KEY", message: "key must name an uploaded custodian holdings file." });
    }

    const runs = await listRunsForDate(catalystApp, parsed.businessDate);
    const inUse = runs.some((r) => r.status === RUN_STATUS.QUEUED || r.status === RUN_STATUS.RUNNING);
    if (inUse) {
      return fail(res, 409, {
        code: "FILE_IN_USE",
        message: `A comparison for ${parsed.businessDate} is running right now. Wait for it to finish, then remove the file.`,
      });
    }

    const bucket = getBucket(catalystApp);
    // Already gone (pressed twice) is fine: the outcome is the same.
    if (await bucket.headObject(key, { throwErr: false })) await bucket.deleteObject(key);
    // At or after the removed file's own time, so neither it nor anything older is staged again.
    await markCustodyCleared(bucket, parsed.businessDate, Math.max(Date.now(), parsed.timeMs));

    // The staged file changed, so earlier answers for this date no longer describe it.
    const superseded = await supersedeRuns(catalystApp, parsed.businessDate);

    return res.json({
      success: true,
      message: `Removed ${parsed.fileName}. Upload the correct custodian file for ${parsed.businessDate}.`,
      removedKeys: [key],
      custodyFile: null,
      supersededRuns: superseded.runIds,
    });
  } catch (error) {
    console.error("[HoldingComparison] deleteCustodyFile:", error);
    return fail(res, 500, { message: error.message || "Could not remove the custodian file." });
  }
};

const listFolder = (folder) => async (req, res) => {
  try {
    const items = await listRecent(getBucket(req.catalystApp), folder);
    return res.json({ success: true, items });
  } catch (error) {
    console.error(`[HoldingComparison] list ${folder}:`, error);
    return fail(res, 500, { message: error.message || "Could not list previous files." });
  }
};

/** GET /api/holding-comparison/custody-history — last 10 custodian uploads */
export const listCustodyHistory = listFolder(FOLDER.CUSTODY);

/** GET /api/holding-comparison/fa-history — last 10 FA holdings snapshots */
export const listFaHistory = listFolder(FOLDER.FA);

/** GET /api/holding-comparison/reports — last 10 difference reports */
export const listDifferenceReports = listFolder(FOLDER.DIFFERENCE);

/** GET /api/holding-comparison/download?key=... — one-hour presigned URL */
export const downloadFile = async (req, res) => {
  try {
    const key = String(req.query?.key || "");
    // Only this module's own files, so the endpoint cannot hand out arbitrary objects.
    if (!isModuleKey(key)) {
      return fail(res, 400, { code: "BAD_KEY", message: "That file cannot be downloaded here." });
    }
    const raw = await getBucket(req.catalystApp).generatePreSignedUrl(key, "GET", STRATUS_GET_URL_OPTS);
    const downloadUrl = stratusSignedUrlToString(raw);
    if (!downloadUrl) {
      return fail(res, 404, { code: "NOT_FOUND", message: "Could not prepare the download." });
    }
    return res.json({ success: true, key, downloadUrl, expiresInSeconds: 3600 });
  } catch (error) {
    console.error("[HoldingComparison] downloadFile:", error);
    return fail(res, 500, { message: error.message || "Could not prepare the download." });
  }
};
