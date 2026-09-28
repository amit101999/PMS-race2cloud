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

import { computeBonusPreviewRows } from "../../util/analytics/bonusPreviewCalc.js";
import { computeSplitPreviewRows } from "../../util/analytics/splitPreviewCalc.js";
import { checkEvent, compareHoldings, comparisonToCsv } from "../../util/corporateAction/compareCustodian.js";
import { parseDate, num, stripIsinSuffix } from "../../util/custodian/parseCustodianCsv.js";
import { PROFILE_TYPE } from "../../util/custodyMatch/fieldMap.js";
import { STRATUS_GET_URL_OPTS, stratusSignedUrlToString } from "../../util/stratusSignedUrl.js";
import { DATE_PATTERN } from "../../util/holdingComparison/constants.js";
import { normHeader } from "../../util/holdingComparison/parse.js";
import { getBucket, getBuffer, listAll, readFieldMapProfile } from "../../util/holdingComparison/storage.js";

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

/*
 * "CA Type Descr" values in the custodian file for each corporate action page.
 * Values seen in the file: Bonus, Buyback, Demerger, Dividend In Cash, Split.
 */
const CA_TYPE_DESCR = {
  split: ["split"],
  bonus: ["bonus"],
};

/** The columns the Split / Bonus comparison reads (see CORP_ACTION_REQUIRED_FIELDS in fieldMap.js). */
const COMPARE_FIELDS = ["clientUccCode", "clientName", "isin", "isinName", "caTypeDescr", "recordDate", "ratio", "holdingQty"];
const OPTIONAL_COMPARE_FIELDS = ["clientName", "isinName"];

const MONTHS = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];

/**
 * yyyy-mm-dd from a custodian date cell, or null.
 * "10-Mar-2026" is read here directly: parseDate's Date() fallback would read it
 * as local midnight and can land on the previous day ahead of UTC.
 */
const toIsoDate = (value) => {
  const m = /^(\d{1,2})[-/ ]([A-Za-z]{3})[A-Za-z]*[-/ ](\d{4})$/.exec(String(value || "").trim());
  if (m) {
    const month = MONTHS.indexOf(m[2].toLowerCase()) + 1;
    if (month > 0) return `${m[3]}-${String(month).padStart(2, "0")}-${m[1].padStart(2, "0")}`;
  }
  return parseDate(value);
};

/** The newest custodian corporate actions file across every date that still exists, or null. */
const newestCorpActionFile = async (bucket) => {
  const items = (await listAll(bucket, `${FOLDER}/`))
    .map((item) => {
      const parsed = parseKey(item.key);
      return parsed ? { ...parsed, sizeBytes: item.sizeBytes } : null;
    })
    .filter(Boolean)
    .sort((a, b) => b.timeMs - a.timeMs);
  for (const item of items) {
    // A deleted object stays listed for about twenty seconds.
    if (await bucket.headObject(item.key, { throwErr: false })) return item;
  }
  return null;
};

/**
 * The newest custodian file's rows for one corporate action type and ISIN,
 * read through the saved field mapping.
 *
 * Throws an Error with `code` when the file cannot be used, so callers can
 * return it to the screen as-is.
 *
 * @param {object} catalystApp
 * @param {{ caType: "split"|"bonus", isin: string }} filter
 * @returns {Promise<{ file: object, rows: Array<{
 *   accountCode, clientName, isin, isinName, recordDate, ratio, holdingQty }> }>}
 */
export const readLatestCorpActionRows = async (catalystApp, { caType, isin }) => {
  const wantedTypes = CA_TYPE_DESCR[caType];
  if (!wantedTypes) throw Object.assign(new Error(`Unknown corporate action type "${caType}".`), { code: "BAD_TYPE" });

  const bucket = getBucket(catalystApp);
  const file = await newestCorpActionFile(bucket);
  if (!file) {
    throw Object.assign(new Error("No custodian file has been uploaded on the Custodian Upload page."), { code: "NO_FILE" });
  }

  const profile = await readFieldMapProfile(bucket, PROFILE_TYPE.CUSTODY_CORP_ACTION);
  if (!profile) throw Object.assign(new Error("The custodian file's columns have not been mapped."), { code: "NO_MAPPING" });

  const buffer = await getBuffer(bucket, file.key);
  const [headers = [], ...dataRows] = await readRows(buffer);
  const normHeaders = headers.map(normHeader);

  // Where each field sits in this file, according to the saved mapping.
  const columnOf = {};
  for (const field of COMPARE_FIELDS) {
    const source = profile[field];
    let index = -1;
    if (source?.by === "name") index = normHeaders.indexOf(normHeader(source.name));
    if (source?.by === "index") index = Number(source.index);
    if (index < 0 || index >= headers.length) {
      if (OPTIONAL_COMPARE_FIELDS.includes(field)) continue;
      throw Object.assign(
        new Error(`The custodian file ${file.fileName} has no column mapped for "${field}". Re-map and upload it again.`),
        { code: "FIELD_NOT_MAPPED" },
      );
    }
    columnOf[field] = index;
  }
  const cell = (cells, field) => (columnOf[field] === undefined ? "" : String(cells[columnOf[field]] ?? "").trim());

  const wantedIsin = stripIsinSuffix(isin).toUpperCase();
  const rows = [];
  for (const cells of dataRows) {
    const typeDescr = cell(cells, "caTypeDescr").toLowerCase();
    if (!wantedTypes.includes(typeDescr)) continue;
    if (stripIsinSuffix(cell(cells, "isin")).toUpperCase() !== wantedIsin) continue;
    rows.push({
      accountCode: cell(cells, "clientUccCode").toUpperCase(),
      clientName: cell(cells, "clientName"),
      isin: wantedIsin,
      isinName: cell(cells, "isinName"),
      recordDate: toIsoDate(cell(cells, "recordDate")),
      ratio: cell(cells, "ratio"),
      holdingQty: num(cell(cells, "holdingQty")),
    });
  }

  return { file, rows };
};

/**
 * GET /api/custodian-upload/latest?caType=split|bonus&isin=...
 * Which custodian file the Split / Bonus page will compare with, and how many of its rows apply.
 */
export const getLatestForCorpAction = async (req, res) => {
  try {
    const caType = String(req.query?.caType || "").trim().toLowerCase();
    const isin = String(req.query?.isin || "").trim();
    if (!isin) return fail(res, 400, { code: "BAD_ISIN", message: "isin is required." });

    const { file, rows } = await readLatestCorpActionRows(req.catalystApp, { caType, isin });
    return res.json({ success: true, file, rowCount: rows.length });
  } catch (error) {
    if (error.code) return fail(res, error.code === "BAD_TYPE" ? 400 : 404, { code: error.code, message: error.message });
    console.error("[CustodianUpload] getLatestForCorpAction:", error);
    return fail(res, 500, { message: error.message || "Could not read the custodian file." });
  }
};

const PREVIEW_ROWS = 10;
const DIFFERENCE_FOLDER = "CA-Differences";

/**
 * POST /api/split/compare-custodian and /api/bonus/compare-custodian
 * body: { isin, ratio1, ratio2, recordDate, securityName }
 *
 * Compares the page's preview (holding on the record date) with the newest
 * custodian file's rows for the same ISIN and action. Stops with 409 when the
 * record dates differ; a different ratio is returned as a warning.
 *
 * @param {"split"|"bonus"} caType
 */
export const compareWithCustodian = (caType) => async (req, res) => {
  try {
    const isin = String(req.body?.isin || "").trim();
    const recordDateISO = String(req.body?.recordDate || "").trim().slice(0, 10);
    const r1 = Number(req.body?.ratio1);
    const r2 = Number(req.body?.ratio2);
    if (!isin || !DATE_PATTERN.test(recordDateISO) || !(r1 > 0) || !(r2 > 0)) {
      return fail(res, 400, { code: "BAD_INPUT", message: "ISIN, ratio and a yyyy-mm-dd record date are required." });
    }

    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();

    /*
     * The custodian's ratio is "Propose : Held". On the Split page ratio1 is
     * "for every" (held) and ratio2 is "investor gets" (propose); on the Bonus
     * page ratio1 is the bonus shares (propose) and ratio2 is held.
     */
    const [previewRows, { file, rows: custodyRows }] = await Promise.all([
      caType === "split"
        ? computeSplitPreviewRows(zcql, { isin, r1, r2, recordDateISO })
        : computeBonusPreviewRows(zcql, { isin, r1, r2, recordDateISO }),
      readLatestCorpActionRows(catalystApp, { caType, isin }),
    ]);
    const pageRatio = caType === "split" ? r2 / r1 : r1 / r2;
    const pageRatioLabel = caType === "split" ? `${r2}:${r1}` : `${r1}:${r2}`;
    const typeLabel = caType === "split" ? "Split" : "Bonus";

    if (custodyRows.length === 0) {
      return fail(res, 409, {
        code: "NO_CUSTODIAN_ROWS",
        file,
        message: `The custodian file ${file.fileName} has no ${typeLabel} rows for ${isin}.`,
      });
    }

    const { error, warnings } = checkEvent({ custodyRows, recordDateISO, pageRatio, pageRatioLabel });
    if (error) return fail(res, 409, { code: "RECORD_DATE_MISMATCH", file, message: error });

    const { rows, counts } = compareHoldings({
      previewRows,
      custodyRows,
      isin,
      isinName: String(req.body?.securityName || ""),
      recordDateISO,
    });

    const bucket = getBucket(catalystApp);
    const reportKey = `${DIFFERENCE_FOLDER}/${safeFileName(isin)}/${caType}-${Date.now()}.csv`;
    await bucket.putObject(reportKey, comparisonToCsv(rows), { contentType: "text/csv", overwrite: true });
    const downloadUrl = stratusSignedUrlToString(
      await bucket.generatePreSignedUrl(reportKey, "GET", STRATUS_GET_URL_OPTS),
    );

    return res.json({
      success: true,
      file,
      counts,
      warnings,
      rows: rows.slice(0, PREVIEW_ROWS),
      downloadUrl,
    });
  } catch (error) {
    if (error.code) return fail(res, 409, { code: error.code, message: error.message });
    console.error(`[CustodianUpload] compareWithCustodian(${caType}):`, error);
    return fail(res, 500, { message: error.message || "Could not compare with the custodian file." });
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
