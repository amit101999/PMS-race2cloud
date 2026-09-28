/**
 * Corporate Actions -> Custodian Upload.
 *
 * Stores the custodian's corporate actions file for a date, once its columns
 * have been mapped on the Field Mapping popup. Nothing is applied yet: the file
 * is kept as uploaded, and a later step will read it through the saved mapping.
 *
 *   Custodian-CorpAction/<date>/<ms>-<name>.csv   (bucket "temporary-files")
 */

import { Readable } from "stream";
import csvParser from "csv-parser";

import { PROFILE_TYPE } from "../../util/custodyMatch/fieldMap.js";
import { DATE_PATTERN } from "../../util/holdingComparison/constants.js";
import { normHeader } from "../../util/holdingComparison/parse.js";
import { getBucket, listAll, readFieldMapProfile } from "../../util/holdingComparison/storage.js";

const FOLDER = "Custodian-CorpAction";
const KEY_PATTERN = /^Custodian-CorpAction\/(\d{4}-\d{2}-\d{2})\/(\d+)-([^/]+)$/;

const fail = (res, status, body) => res.status(status).json({ success: false, ...body });

/** Keep a file name safe to use inside an object key. */
const safeFileName = (name) =>
  String(name || "file.csv")
    .replace(/[\\/]+/g, "_")
    .replace(/[^\w.\- ()]+/g, "_")
    .slice(0, 150);

const parseKey = (key) => {
  const m = KEY_PATTERN.exec(key);
  return m ? { key, businessDate: m[1], timeMs: Number(m[2]), fileName: m[3] } : null;
};

/** Every row as a positional array, header row first. */
const readRows = (buffer) =>
  new Promise((resolve, reject) => {
    const rows = [];
    Readable.from(buffer)
      .pipe(csvParser({ headers: false }))
      .on("data", (row) => rows.push(Object.keys(row).map((k) => String(row[k] ?? "").trim())))
      .on("end", () => resolve(rows))
      .on("error", reject);
  });

/**
 * Check the file against the saved mapping.
 *
 * Every field is optional text, so no cell can fail a row. What can go wrong is
 * the file itself: no header, a column the mapping names that this file does
 * not have, or no data rows at all.
 *
 * @returns {{ fileError: object|null, rowCount: number }}
 */
const checkFile = async (buffer, profile) => {
  const rows = buffer.length > 0 ? await readRows(buffer) : [];
  const headers = rows[0] || [];
  if (headers.every((h) => h === "")) {
    return { fileError: { code: "NO_HEADER", message: "Row 1 must carry the column headings." }, rowCount: 0 };
  }

  const fileHeaders = new Set(headers.map(normHeader));
  const missing = [];
  for (const source of Object.values(profile)) {
    if (source?.by === "name" && !fileHeaders.has(normHeader(source.name))) missing.push(source.name);
    if (source?.by === "index" && !(source.index < headers.length)) missing.push(`column ${source.index + 1}`);
  }
  if (missing.length > 0) {
    return {
      fileError: {
        code: "HEADER_MISMATCH",
        message: `The file does not have the column(s) the saved field mapping expects: ${missing.join(", ")}.`,
        missing,
        hasSavedMapping: true,
      },
      rowCount: 0,
    };
  }

  const rowCount = rows.slice(1).filter((cells) => cells.some((c) => c !== "")).length;
  return { fileError: null, rowCount };
};

/**
 * POST /api/custodian-upload/file  (multipart: file, businessDate)
 */
export const uploadCustodianFile = async (req, res) => {
  try {
    const file = req.files?.file;
    if (!file) {
      return fail(res, 400, { code: "NO_FILE", message: "CSV file is required. Send it as form-data with key 'file'." });
    }
    if (!/\.csv$/i.test(String(file.name || ""))) {
      return fail(res, 400, { code: "NOT_CSV", message: "Only CSV files are allowed." });
    }

    const businessDate = String(req.body?.businessDate || "").trim();
    if (!DATE_PATTERN.test(businessDate)) {
      return fail(res, 400, { code: "BAD_DATE", message: "businessDate must be yyyy-mm-dd." });
    }

    const bucket = getBucket(req.catalystApp);
    // The popup will not offer Upload until a mapping is saved, so none means the screen was bypassed.
    const profile = await readFieldMapProfile(bucket, PROFILE_TYPE.CUSTODY_CORP_ACTION);
    if (!profile) {
      return fail(res, 400, { code: "NO_MAPPING", message: "Map and save this file's columns before uploading it." });
    }

    const { fileError, rowCount } = await checkFile(file.data, profile);
    if (fileError) {
      const status = fileError.code === "HEADER_MISMATCH" ? 422 : 400;
      return fail(res, status, { ...fileError, fileName: file.name });
    }
    if (rowCount === 0) {
      return fail(res, 400, { code: "NO_ROWS", message: `${file.name} has no data rows.` });
    }

    const key = `${FOLDER}/${businessDate}/${Date.now()}-${safeFileName(file.name)}`;
    await bucket.putObject(key, file.data, { contentType: "text/csv", overwrite: true });

    return res.status(201).json({
      success: true,
      message: `${file.name} uploaded for ${businessDate}.`,
      // Returned in full because Stratus listings lag a fresh write by ~20 seconds.
      file: { ...parseKey(key), sizeBytes: file.data.length },
      rowCount,
    });
  } catch (error) {
    console.error("[CustodianUpload] uploadCustodianFile:", error);
    return fail(res, 500, { message: error.message || "Could not upload the custodian file." });
  }
};

/** Every upload for a date, newest first. */
const listForDate = async (bucket, date) =>
  (await listAll(bucket, `${FOLDER}/${date}/`))
    .map((item) => {
      const parsed = parseKey(item.key);
      return parsed ? { ...parsed, sizeBytes: item.sizeBytes } : null;
    })
    .filter(Boolean)
    .sort((a, b) => b.timeMs - a.timeMs);

/**
 * GET /api/custodian-upload/day/:date — the newest file uploaded for a date, or null.
 */
export const getDay = async (req, res) => {
  try {
    const date = String(req.params.date || "");
    if (!DATE_PATTERN.test(date)) {
      return fail(res, 400, { code: "BAD_DATE", message: "date must be yyyy-mm-dd." });
    }

    const bucket = getBucket(req.catalystApp);
    // Stratus keeps listing a deleted object for about twenty seconds, so the
    // listed file is confirmed to still exist before it is shown.
    for (const item of await listForDate(bucket, date)) {
      if (await bucket.headObject(item.key, { throwErr: false })) {
        return res.json({ success: true, file: item });
      }
    }
    return res.json({ success: true, file: null });
  } catch (error) {
    console.error("[CustodianUpload] getDay:", error);
    return fail(res, 500, { message: error.message || "Could not read the uploaded files." });
  }
};

/**
 * DELETE /api/custodian-upload/file?key=...
 *
 * For a file uploaded by mistake. Every upload for that date is deleted, not
 * just this one, so an earlier upload does not quietly take its place: after
 * removal the date has no file until the correct one is uploaded.
 */
export const deleteCustodianFile = async (req, res) => {
  try {
    const parsed = parseKey(String(req.query?.key || "").trim());
    if (!parsed) {
      return fail(res, 400, { code: "BAD_KEY", message: "key must name an uploaded custodian file." });
    }

    const bucket = getBucket(req.catalystApp);
    const keys = new Set([parsed.key, ...(await listForDate(bucket, parsed.businessDate)).map((i) => i.key)]);
    for (const key of keys) {
      // Already gone (pressed twice, or a listing that lags a delete) is fine: the outcome is the same.
      if (await bucket.headObject(key, { throwErr: false })) await bucket.deleteObject(key);
    }

    return res.json({
      success: true,
      message: `Removed ${parsed.fileName}. Upload the correct custodian file for ${parsed.businessDate}.`,
    });
  } catch (error) {
    console.error("[CustodianUpload] deleteCustodianFile:", error);
    return fail(res, 500, { message: error.message || "Could not remove the custodian file." });
  }
};
