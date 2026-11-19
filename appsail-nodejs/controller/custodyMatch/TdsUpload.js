/**
 * TDS tab of Transaction Upload — the mutual fund TDS files of a trade date.
 *
 *   GET    /api/custody-match/tds/download?key=…   1-hour download link for one file
 *   GET    /api/custody-match/tds/:date            the date's files, newest first
 *   POST   /api/custody-match/tds/:date            save one file (form-data: file)
 *   DELETE /api/custody-match/tds/:date?key=…      remove one file
 *
 * For now the files are only kept; nothing in our books reads them. They live
 * in their own bucket, one folder per trade date:
 *   tds-files/<trade date>/<uploadedAtMs>_<uploadedOn>_<file name>
 * The upload time in front of the name means a second file with the same name
 * is kept beside the first instead of replacing it.
 */

import { FIELD_CATALOG, PROFILE_TYPE, resolveFieldMap } from "../../util/custodyMatch/fieldMap.js";
import { detectSeparator, firstLineOf, splitHeaderCells } from "../../util/custodyMatch/normalize.js";
import {
  getBucket,
  listAllObjects,
  loadFieldMapProfiles,
  putBuffer,
  signedGetUrl,
} from "../../util/custodyMatch/storage.js";
import { getUploadedFile, isCsv, readUploadedOn } from "./CustodyMatchUpload.js";

const TDS_BUCKET = "tds-files";
const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;
const KEY_PATTERN = /^(\d{4}-\d{2}-\d{2})\/(\d+)_(\d{4}-\d{2}-\d{2})_(.+)$/;

const fail = (res, status, body) => res.status(status).json({ success: false, ...body });
const noApp = (res) => fail(res, 500, { message: "Catalyst app is not initialised." });

/* Today in India as yyyy-mm-dd, read by parts so no time zone shift creeps in. */
const indiaToday = () => {
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Asia/Kolkata",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(new Date());
  const part = (type) => parts.find((p) => p.type === type).value;
  return `${part("year")}-${part("month")}-${part("day")}`;
};

const displayDate = (date) => String(date).split("-").reverse().join("-");

/** The trade date in the URL: yyyy-mm-dd and not in the future. Returns an error message or null. */
const tradeDateProblem = (date) => {
  if (!DATE_PATTERN.test(date)) return "The trade date must be yyyy-mm-dd.";
  if (date > indiaToday()) return `The trade date ${displayDate(date)} is in the future.`;
  return null;
};

/**
 * The date's files, newest first. Stratus keeps listing a deleted object for a
 * short while, so each listed file is confirmed to still exist; otherwise a file
 * removed a moment ago would come straight back on the screen.
 */
const listTdsFiles = async (bucket, date) => {
  const files = [];
  for (const object of await listAllObjects(bucket, `${date}/`)) {
    const match = object.key.match(KEY_PATTERN);
    if (!match || match[1] !== date) continue;
    if (!(await bucket.headObject(object.key, { throwErr: false }))) continue;
    files.push({
      key: object.key,
      fileName: match[4],
      uploadedAtMs: Number(match[2]),
      uploadedOn: match[3],
      sizeBytes: object.sizeBytes,
    });
  }
  return files.sort((a, b) => b.uploadedAtMs - a.uploadedAtMs);
};

/** GET /api/custody-match/tds/:date */
export const listTdsDate = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);
    const date = String(req.params.date || "").trim();
    const problem = tradeDateProblem(date);
    if (problem) return fail(res, 400, { code: "BAD_DATE", message: problem });

    return res.json({ success: true, date, files: await listTdsFiles(getBucket(catalystApp, TDS_BUCKET), date) });
  } catch (error) {
    console.error("[TdsUpload] listTdsDate:", error);
    return fail(res, 500, { message: error.message || "Could not read the TDS files." });
  }
};

/**
 * POST /api/custody-match/tds/:date
 * form-data: file, uploadDate (the upload date picked in the tab bar; required, not in the future)
 *
 * The file must be a CSV whose headings give every required field of the saved
 * TDS mapping (FA_MUTUAL_FUND_TDS). The rows themselves are not read.
 */
export const uploadTdsFile = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);
    const date = String(req.params.date || "").trim();
    const problem = tradeDateProblem(date);
    if (problem) return fail(res, 400, { code: "BAD_DATE", message: problem });
    const uploadDay = readUploadedOn(req.body?.uploadDate);
    if (uploadDay.error) return fail(res, 400, { code: "BAD_UPLOAD_DATE", message: uploadDay.error });

    const file = getUploadedFile(req);
    if (!file) {
      return fail(res, 400, { code: "NO_FILE", message: "CSV file is required. Send it as form-data with key 'file'." });
    }
    if (!isCsv(file.name)) {
      return fail(res, 400, { code: "NOT_CSV", message: `${file.name} is not a CSV file.`, fileName: file.name });
    }

    const headerLine = firstLineOf(file.data);
    const headers = splitHeaderCells(headerLine, detectSeparator(headerLine));
    const profiles = await loadFieldMapProfiles(catalystApp);
    const { index } = resolveFieldMap(PROFILE_TYPE.FA_MUTUAL_FUND_TDS, profiles[PROFILE_TYPE.FA_MUTUAL_FUND_TDS], headers);
    const missing = FIELD_CATALOG[PROFILE_TYPE.FA_MUTUAL_FUND_TDS]
      .filter((entry) => entry.required && index[entry.field] === undefined)
      .map((entry) => entry.label);
    if (missing.length > 0) {
      return fail(res, 400, {
        code: "TDS_COLUMNS_MISSING",
        message: `${file.name} has no column for: ${missing.join(", ")}. Check the file or the TDS mapping.`,
        fileName: file.name,
        missing,
      });
    }

    const uploadedAtMs = Date.now();
    const safeName = String(file.name).replace(/[^A-Za-z0-9._ ()-]+/g, "_").slice(0, 120);
    const key = `${date}/${uploadedAtMs}_${uploadDay.uploadedOn}_${safeName}`;
    const bucket = getBucket(catalystApp, TDS_BUCKET);
    await putBuffer(bucket, key, file.data);

    return res.json({
      success: true,
      date,
      key,
      message: `TDS file ${file.name} saved for ${displayDate(date)}.`,
      files: await listTdsFiles(bucket, date),
    });
  } catch (error) {
    console.error("[TdsUpload] uploadTdsFile:", error);
    return fail(res, 500, { message: error.message || "Could not save the TDS file." });
  }
};

/** DELETE /api/custody-match/tds/:date?key=… — only a file of that date's folder. */
export const deleteTdsFile = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);
    const date = String(req.params.date || "").trim();
    const problem = tradeDateProblem(date);
    if (problem) return fail(res, 400, { code: "BAD_DATE", message: problem });

    const key = String(req.query.key || "");
    const match = key.match(KEY_PATTERN);
    if (!match || match[1] !== date) {
      return fail(res, 400, { code: "BAD_KEY", message: "That is not a TDS file of this date." });
    }

    const bucket = getBucket(catalystApp, TDS_BUCKET);
    if (!(await bucket.headObject(key, { throwErr: false }))) {
      return fail(res, 404, { code: "NOT_FOUND", message: "That TDS file is already gone." });
    }
    await bucket.deleteObject(key);

    return res.json({
      success: true,
      date,
      message: `TDS file ${match[4]} removed from ${displayDate(date)}.`,
      files: (await listTdsFiles(bucket, date)).filter((f) => f.key !== key),
    });
  } catch (error) {
    console.error("[TdsUpload] deleteTdsFile:", error);
    return fail(res, 500, { message: error.message || "Could not remove the TDS file." });
  }
};

/** GET /api/custody-match/tds/download?key=… */
export const downloadTdsFile = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);
    const key = String(req.query.key || "");
    if (!KEY_PATTERN.test(key)) return fail(res, 400, { code: "BAD_KEY", message: "That is not a TDS file." });

    const downloadUrl = await signedGetUrl(getBucket(catalystApp, TDS_BUCKET), key);
    if (!downloadUrl) return fail(res, 404, { code: "NOT_FOUND", message: "Could not prepare the download." });
    return res.json({ success: true, downloadUrl });
  } catch (error) {
    console.error("[TdsUpload] downloadTdsFile:", error);
    return fail(res, 500, { message: error.message || "Could not prepare the download." });
  }
};
