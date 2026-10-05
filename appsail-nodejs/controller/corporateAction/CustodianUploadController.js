/**
 * Corporate Actions -> Custodian Upload.
 *
 * Stores two custodian files for a date, once their columns have been mapped
 * on the Field Mapping popup. Modules read them through the saved mapping.
 *
 *   entitlement ("EX Entitlement Vs Benefit Collection combo"):
 *     CA-Ex-Entitlement/<date>/<ms>-<name>.csv
 *   allocation ("Corporate Benefit Collection"):
 *     CA-Benefit-Collection/<date>/<ms>-<name>.csv
 *
 * Both in bucket "custodian-ca-files". Their field mappings stay in
 * "temporary-files" with every other mapping (saved by the Field Mapping
 * popup), and so do the Split / Bonus difference reports.
 *
 * Each upload is also imported into a Datastore table (see custodianTables.js),
 * and modules read from there: one event (ISIN + CA type + record date) can be
 * in any uploaded file, not only the newest.
 */

import { createHash } from "crypto";
import { Readable } from "stream";
import csvParser from "csv-parser";

import { computeBonusPreviewRows } from "../../util/analytics/bonusPreviewCalc.js";
import { computeSplitPreviewRows, readSplitAccountRows, SPLIT_STATUS } from "../../util/analytics/splitPreviewCalc.js";
import { BONUS_STATUS, readBonusAccountRows } from "../../util/analytics/bonusAccountRows.js";
import {
  checkEvent,
  compareHoldings,
  compareReceipts,
  comparisonToCsv,
  receiptsToCsv,
} from "../../util/corporateAction/compareCustodian.js";
import { buildVirtualToActualMap } from "../../util/mapVirtualToActualCodes.js";
import {
  buildImportCsv,
  columnIndexes,
  dateOf,
  deleteDataRows,
  deleteUploadRow,
  fileLabel,
  FILES_BUCKET,
  findDuplicate,
  isImporting,
  listUploadKeys,
  missingRequiredFields,
  numberOrNull,
  readEventRows,
  settleImport,
  startImport,
} from "../../util/corporateAction/custodianTables.js";
import { PROFILE_TYPE } from "../../util/custodyMatch/fieldMap.js";
import { STRATUS_GET_URL_OPTS, stratusSignedUrlToString } from "../../util/stratusSignedUrl.js";
import { DATE_PATTERN } from "../../util/holdingComparison/constants.js";
import { normHeader } from "../../util/holdingComparison/parse.js";
import { getBucket, getBuffer, listAll, readFieldMapProfile } from "../../util/holdingComparison/storage.js";

/** The two custodian files this page stores: where each lives and which mapping reads it. */
const FILE_KINDS = {
  entitlement: { folder: "CA-Ex-Entitlement", profileType: PROFILE_TYPE.CUSTODY_CORP_ACTION },
  allocation: { folder: "CA-Benefit-Collection", profileType: PROFILE_TYPE.CUSTODY_CORP_ALLOCATION },
};
const filesBucket = (catalystApp) => getBucket(catalystApp, FILES_BUCKET);

const KEY_PATTERN = /^(CA-Ex-Entitlement|CA-Benefit-Collection)\/(\d{4}-\d{2}-\d{2})\/(\d+)-([^/]+)$/;

const fail = (res, status, body) => res.status(status).json({ success: false, ...body });

/** The requested file kind; the entitlement file when none is given, as before the allocation file existed. */
const kindFrom = (value) => {
  const kind = String(value || "entitlement").trim().toLowerCase();
  if (!FILE_KINDS[kind]) throw Object.assign(new Error(`Unknown custodian file kind "${kind}".`), { code: "BAD_KIND" });
  return kind;
};

/** Keep a file name safe to use inside an object key. */
const safeFileName = (name) =>
  String(name || "file.csv")
    .replace(/[\\/]+/g, "_")
    .replace(/[^\w.\- ()]+/g, "_")
    .slice(0, 150);

const parseKey = (key) => {
  const m = KEY_PATTERN.exec(key);
  if (!m) return null;
  const kind = Object.keys(FILE_KINDS).find((k) => FILE_KINDS[k].folder === m[1]);
  return { key, kind, businessDate: m[2], timeMs: Number(m[3]), fileName: m[4] };
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
const checkFile = (rows, profile) => {
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

const sha256 = (buffer) => createHash("sha256").update(buffer).digest("hex");

/** Turn a stored file into the table CSV and start its import. */
const importFile = async (catalystApp, { kind, key, rows, profile, fileHash }) => {
  const { businessDate, timeMs, fileName } = parseKey(key);
  const [headers = [], ...dataRows] = rows;
  const indexOf = columnIndexes(profile, headers.map(normHeader), normHeader);
  const { csv, rowCount } = buildImportCsv(kind, dataRows, indexOf, {
    sourceKey: key,
    reportDate: businessDate,
    uploadedMs: timeMs,
  });
  return startImport(catalystApp, { kind, key, businessDate, timeMs, fileName, fileHash, csv, rowCount });
};

/**
 * Delete uploaded files and their table data. Each upload's row goes first so
 * no module reads it any more; its data rows (up to 30k) are deleted in the background.
 */
const removeUploads = async (catalystApp, kind, keys) => {
  const bucket = filesBucket(catalystApp);
  const zcql = catalystApp.zcql();
  for (const key of keys) {
    // Already gone (pressed twice, or a listing that lags a delete) is fine: the outcome is the same.
    if (await bucket.headObject(key, { throwErr: false })) await bucket.deleteObject(key);
    await deleteUploadRow(zcql, key);
    void deleteDataRows(zcql, kind, key).catch((error) =>
      console.error(`[CustodianUpload] Could not delete the table rows of ${key}:`, error.message),
    );
  }
};

/**
 * POST /api/custodian-upload/file  (multipart: file, businessDate, kind = entitlement|allocation, replace)
 *
 * Exactly the same file as an earlier upload is refused with DUPLICATE_FILE;
 * sent again with replace=true, the earlier upload is removed and this one stored.
 */
export const uploadCustodianFile = async (req, res) => {
  try {
    const kind = kindFrom(req.body?.kind);
    const { folder, profileType } = FILE_KINDS[kind];
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

    // The popup will not offer Upload until a mapping is saved, so none means the screen was bypassed.
    const profile = await readFieldMapProfile(getBucket(req.catalystApp), profileType);
    if (!profile) {
      return fail(res, 400, { code: "NO_MAPPING", message: "Map and save this file's columns before uploading it." });
    }

    const rows = file.data.length > 0 ? await readRows(file.data) : [];
    const { fileError, rowCount } = checkFile(rows, profile);
    if (fileError) {
      const status = fileError.code === "HEADER_MISMATCH" ? 422 : 400;
      return fail(res, status, { ...fileError, fileName: file.name });
    }
    if (rowCount === 0) {
      return fail(res, 400, { code: "NO_ROWS", message: `${file.name} has no data rows.` });
    }

    // Modules cannot use a row without these, so the mapping must cover them.
    const missing = missingRequiredFields(kind, columnIndexes(profile, (rows[0] || []).map(normHeader), normHeader));
    if (missing.length > 0) {
      return fail(res, 400, {
        code: "FIELD_NOT_MAPPED",
        message: `Map these fields before uploading: ${missing.join(", ")}.`,
        missing,
      });
    }

    const catalystApp = req.catalystApp;
    const fileHash = sha256(file.data);
    const duplicate = await findDuplicate(catalystApp.zcql(), kind, fileHash);
    if (duplicate) {
      if (String(req.body?.replace) !== "true") {
        return fail(res, 409, {
          code: "DUPLICATE_FILE",
          message:
            `This file was already uploaded for ${duplicate.businessDate} as ${duplicate.fileName}. ` +
            "Replace it to remove that upload and its data, and store this one instead.",
          duplicate,
        });
      }
      await removeUploads(catalystApp, kind, [duplicate.key]);
    }

    const key = `${folder}/${businessDate}/${Date.now()}-${safeFileName(file.name)}`;
    await filesBucket(catalystApp).putObject(key, file.data, { contentType: "text/csv", overwrite: true });
    const upload = await importFile(catalystApp, { kind, key, rows, profile, fileHash });

    return res.status(201).json({
      success: true,
      message: `${file.name} uploaded for ${businessDate}. Its rows are being imported.`,
      // Returned in full because Stratus listings lag a fresh write by ~20 seconds.
      file: {
        ...parseKey(key),
        sizeBytes: file.data.length,
        importStatus: upload?.importStatus || "",
        importError: upload?.importError || "",
      },
      rowCount,
    });
  } catch (error) {
    if (error.code === "BAD_KIND") return fail(res, 400, { code: error.code, message: error.message });
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
  dividend: ["dividend in cash"],
};

/** Without a rate and an amount the Dividend reconciliation would compare against zeros. */
const DIVIDEND_REQUIRED_COLUMNS = [
  ["Dividend_Rate", "dividendRate"],
  ["CA_Amt", "caAmt"],
];
/** Split compares the CA quantity too; without it every client would show as a break. */
const SPLIT_REQUIRED_COLUMNS = [["CA_Qty", "caQty"]];

const TYPE_LABEL = { split: "Split", bonus: "Bonus", dividend: "Dividend" };

const coded = (code, message) => Object.assign(new Error(message), { code });

/** An upload described the way the pages show a file. */
const fileOf = (upload) => ({
  key: upload.key,
  kind: upload.kind,
  businessDate: upload.businessDate,
  timeMs: upload.timeMs,
  fileName: upload.fileName,
});

/**
 * One event's rows (ISIN + CA type + record date) from the custodian tables,
 * taken from the uploaded file with the latest report date that has it.
 *
 * Throws an Error with `code` when there are none, so callers can return it
 * to the screen as-is.
 */
const readEvent = async (catalystApp, kind, { caType, isin, recordDateISO }) => {
  const caTypes = CA_TYPE_DESCR[caType];
  if (!caTypes) throw coded("BAD_TYPE", `Unknown corporate action type "${caType}".`);
  if (!DATE_PATTERN.test(String(recordDateISO || ""))) throw coded("BAD_DATE", "A yyyy-mm-dd record date is required.");

  const found = await readEventRows(catalystApp, kind, { isin, caTypes, recordDateISO });
  if (found) return found;

  const importing = await isImporting(catalystApp.zcql(), kind);
  throw coded(
    "NO_CUSTODIAN_ROWS",
    `No ${TYPE_LABEL[caType]} rows for ${isin} with Record Date ${recordDateISO} in any uploaded ${fileLabel(kind)}.` +
      (importing ? " A file is still being imported; try again in a few minutes." : ""),
  );
};

/** A column empty in every row was not mapped in that file, and the comparison cannot work without it. */
const requireColumns = (rows, columns, upload) => {
  for (const [column, field] of columns) {
    if (rows.every((row) => numberOrNull(row[column]) === null)) {
      throw coded(
        "FIELD_NOT_MAPPED",
        `The ${fileLabel(upload.kind)} ${upload.fileName} has no values for "${field}". Re-map and upload it again.`,
      );
    }
  }
};

/**
 * The Ex-Entitlement rows for one corporate action type, ISIN and record date.
 *
 * @param {object} catalystApp
 * @param {{ caType: "split"|"bonus"|"dividend", isin: string, recordDateISO: string }} filter
 * @returns {Promise<{ file: object, rows: Array<{
 *   accountCode, clientName, isin, isinName, recordDate, ratio, holdingQty, caQty,
 *   caRefNo, exDate, paymentDate, dividendRate, caAmt }> }>}
 */
export const readLatestCorpActionRows = async (catalystApp, { caType, isin, recordDateISO }) => {
  const { upload, rows } = await readEvent(catalystApp, "entitlement", { caType, isin, recordDateISO });
  if (caType === "dividend") requireColumns(rows, DIVIDEND_REQUIRED_COLUMNS, upload);
  if (caType === "split") requireColumns(rows, SPLIT_REQUIRED_COLUMNS, upload);

  return {
    file: fileOf(upload),
    rows: rows.map((row) => ({
      accountCode: row.Account_Code || "",
      clientName: row.Client_Name || "",
      isin: row.ISIN,
      isinName: row.ISIN_Name || "",
      recordDate: dateOf(row.Record_Date),
      ratio: row.Ratio || "",
      holdingQty: numberOrNull(row.Holding_Qty) ?? 0,
      caQty: numberOrNull(row.CA_Qty) ?? 0,
      caRefNo: row.CA_Ref_No || "",
      exDate: dateOf(row.Ex_Date) || null,
      paymentDate: dateOf(row.Payment_Date) || null,
      dividendRate: numberOrNull(row.Dividend_Rate) ?? 0,
      caAmt: numberOrNull(row.CA_Amt) ?? 0,
    })),
  };
};

/**
 * The Corporate Benefit Collection rows of one split or bonus (ISIN + record
 * date), with the quantities and dates the stage-2 comparison reads.
 *
 * @param {{ caType: "split"|"bonus", isin: string, recordDateISO: string }} filter
 * @returns {Promise<{ file: object, rows: Array<{ accountCode, isinName, caRefNo, recordDate,
 *   caQty, receivedQty, balanceQty, receivedDate, eventStatus }> }>}
 */
export const readLatestReceiptRows = async (catalystApp, { caType, isin, recordDateISO }) => {
  const { upload, rows } = await readEvent(catalystApp, "allocation", { caType, isin, recordDateISO });
  return {
    file: fileOf(upload),
    rows: rows.map((row) => ({
      accountCode: row.Account_Code || "",
      isinName: row.ISIN_Name || "",
      caRefNo: row.CA_Ref_No || "",
      recordDate: dateOf(row.Record_Date),
      caQty: numberOrNull(row.CA_Qty) ?? 0,
      receivedQty: numberOrNull(row.Received_Qty) ?? 0,
      balanceQty: numberOrNull(row.Balance_Qty) ?? 0,
      receivedDate: dateOf(row.Received_Date),
      eventStatus: row.Event_Status || "",
    })),
  };
};

/**
 * GET /api/custodian-upload/latest?kind=entitlement|allocation&caType=split|bonus|dividend&isin=...&recordDate=yyyy-mm-dd
 * Which uploaded file a module will use for that event, and how many of its rows apply.
 * The allocation file is read for Split and Bonus; caType defaults to split there.
 */
export const getLatestForCorpAction = async (req, res) => {
  try {
    const kind = kindFrom(req.query?.kind);
    const caType = String(req.query?.caType || (kind === "allocation" ? "split" : "")).trim().toLowerCase();
    const isin = String(req.query?.isin || "").trim();
    const recordDateISO = String(req.query?.recordDate || "").trim().slice(0, 10);
    if (!isin) return fail(res, 400, { code: "BAD_ISIN", message: "isin is required." });

    const { upload, rows } = await readEvent(req.catalystApp, kind, { caType, isin, recordDateISO });
    return res.json({ success: true, file: fileOf(upload), rowCount: rows.length });
  } catch (error) {
    if (error.code) {
      const status = ["BAD_TYPE", "BAD_KIND", "BAD_DATE"].includes(error.code) ? 400 : 404;
      return fail(res, status, { code: error.code, message: error.message });
    }
    console.error("[CustodianUpload] getLatestForCorpAction:", error);
    return fail(res, 500, { message: error.message || "Could not read the custodian file." });
  }
};

const PREVIEW_ROWS = 10;
const DIFFERENCE_FOLDER = "CA-Differences";

/**
 * Split and Bonus keep one history per reconciliation stage, listed on their pages:
 *   CA-Differences/<split|bonus>-<entitlement|benefit>/<record date>/<ms>-<ISIN>.csv
 */
const DIFFERENCE_FOLDERS = {
  split: { entitlement: `${DIFFERENCE_FOLDER}/split-entitlement`, benefit: `${DIFFERENCE_FOLDER}/split-benefit` },
  bonus: { entitlement: `${DIFFERENCE_FOLDER}/bonus-entitlement`, benefit: `${DIFFERENCE_FOLDER}/bonus-benefit` },
};
const DIFFERENCE_KEY_PATTERN = /^CA-Differences\/(split|bonus)-(entitlement|benefit)\/(\d{4}-\d{2}-\d{2})\/(\d+)-([^/]+)$/;
const differenceKey = (caType, stage, recordDateISO, isin) =>
  `${DIFFERENCE_FOLDERS[caType][stage]}/${recordDateISO}/${Date.now()}-${safeFileName(isin)}.csv`;

/** Store a differential CSV and return a signed URL to download it. */
const saveDifferenceFile = async (bucket, key, csv) => {
  await bucket.putObject(key, csv, { contentType: "text/csv", overwrite: true });
  return stratusSignedUrlToString(await bucket.generatePreSignedUrl(key, "GET", STRATUS_GET_URL_OPTS));
};

/**
 * POST /api/split/compare-custodian and /api/bonus/compare-custodian
 * body: { isin, ratio1, ratio2, recordDate, securityName }
 *
 * Compares the page's preview (holding on the record date) with the custodian
 * rows for the same ISIN, action and record date. Stops with 409 when no
 * uploaded file has that event; a different ratio is returned as a warning.
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
      readLatestCorpActionRows(catalystApp, { caType, isin, recordDateISO }),
    ]);
    const pageRatio = caType === "split" ? r2 / r1 : r1 / r2;
    const pageRatioLabel = caType === "split" ? `${r2}:${r1}` : `${r1}:${r2}`;

    const { warnings } = checkEvent({ custodyRows, pageRatio, pageRatioLabel });

    /*
     * The CA quantity is compared too. The custodian's CA Qty is the total after
     * a split, but only the new shares for a bonus. A bonus that rounds down to
     * nothing is not in our preview, so the custodian's zero rows are left out
     * rather than reported as missing.
     */
    const { rows, counts } = compareHoldings({
      previewRows,
      custodyRows: caType === "bonus" ? custodyRows.filter((r) => r.caQty > 0) : custodyRows,
      isin,
      isinName: String(req.body?.securityName || ""),
      recordDateISO,
      withCaQty: true,
      faCaQtyField: caType === "bonus" ? "bonusShares" : "newHolding",
    });

    const reportKey = differenceKey(caType, "entitlement", recordDateISO, isin);
    const downloadUrl = await saveDifferenceFile(
      getBucket(catalystApp),
      reportKey,
      comparisonToCsv(rows, { withCaQty: true }),
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

/**
 * The accounts of one split / bonus still waiting for their shares, with the
 * quantity the custodian should report as received for each.
 */
const RECEIVABLE_ROWS = {
  split: async (zcql, event) =>
    (await readSplitAccountRows(zcql, event))
      .filter((r) => r.status === SPLIT_STATUS.RECEIVABLE)
      .map((r) => ({ accountCode: r.accountCode, expectedQty: r.newHolding })),
  bonus: async (zcql, event) =>
    (await readBonusAccountRows(zcql, event))
      .filter((r) => r.status === BONUS_STATUS.RECEIVABLE)
      .map((r) => ({ accountCode: r.accountCode, expectedQty: r.bonusShares })),
};

const NOT_RECEIVABLE_MESSAGES = {
  split: (isin, recordDateISO) =>
    `Record Date mismatch: ${isin} with Record Date ${recordDateISO} has not been compared with the ` +
    "Ex-Entitlement file yet. Compare with the entitlement file and send it as receivable first, then do this step.",
  bonus: (isin, recordDateISO) =>
    `This bonus (${isin}, Record Date ${recordDateISO}) has not been sent as Receivable yet. ` +
    "Compare with the Ex-Entitlement file and send it as receivable first, then do this step.",
};

/**
 * POST /api/split/compare-benefit and /api/bonus/compare-benefit
 * body: { isin, ratio1, ratio2, recordDate, securityName }
 *
 * Stage 2: the accounts marked Receivable for this split / bonus vs the
 * Corporate Benefit Collection rows for the same ISIN and record date. Only
 * reachable once stage 1 (Send as Receivable) has run for the same ISIN,
 * record date and ratio.
 *
 * @param {"split"|"bonus"} caType
 */
export const compareWithBenefit = (caType) => async (req, res) => {
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

    const receivable = await RECEIVABLE_ROWS[caType](zcql, { isin, recordDateISO, r1, r2 });
    if (receivable.length === 0) {
      return fail(res, 409, { code: "NOT_RECEIVABLE", message: NOT_RECEIVABLE_MESSAGES[caType](isin, recordDateISO) });
    }

    const { file, rows: allocationRows } = await readLatestReceiptRows(catalystApp, { caType, isin, recordDateISO });

    // Our rows are per virtual code; the custodian knows the actual (UCC) code.
    const virtualToActual = await buildVirtualToActualMap(
      zcql,
      receivable.map((r) => r.accountCode),
    );
    const { rows, counts } = compareReceipts({
      receivableRows: receivable.map((r) => ({
        accountCode: virtualToActual.get(r.accountCode) || r.accountCode,
        expectedQty: r.expectedQty,
      })),
      allocationRows,
      isin,
      isinName: String(req.body?.securityName || ""),
      recordDateISO,
    });

    const reportKey = differenceKey(caType, "benefit", recordDateISO, isin);
    const downloadUrl = await saveDifferenceFile(getBucket(catalystApp), reportKey, receiptsToCsv(rows));

    return res.json({
      success: true,
      file,
      counts,
      warnings: [],
      rows: rows.slice(0, PREVIEW_ROWS),
      downloadUrl,
    });
  } catch (error) {
    if (error.code) return fail(res, 409, { code: error.code, message: error.message });
    console.error(`[CustodianUpload] compareWithBenefit(${caType}):`, error);
    return fail(res, 500, { message: error.message || "Could not compare with the Corporate Benefit Collection file." });
  }
};

const DIFFERENCE_HISTORY_LIMIT = 10;

/**
 * GET /api/split/history?stage=entitlement|benefit (and /api/bonus/history)
 * The newest differential files of one reconciliation stage (every ISIN).
 *
 * @param {"split"|"bonus"} caType
 */
export const listDifferenceHistory = (caType) => async (req, res) => {
  try {
    const stage = String(req.query?.stage || "");
    const folder = DIFFERENCE_FOLDERS[caType][stage];
    if (!folder) return fail(res, 400, { message: "stage must be entitlement or benefit." });

    const items = (await listAll(getBucket(req.catalystApp), `${folder}/`))
      .map((item) => {
        const m = DIFFERENCE_KEY_PATTERN.exec(item.key);
        return m
          ? { key: item.key, businessDate: m[3], timeMs: Number(m[4]), fileName: m[5], sizeBytes: item.sizeBytes }
          : null;
      })
      .filter(Boolean)
      .sort((a, b) => b.timeMs - a.timeMs)
      .slice(0, DIFFERENCE_HISTORY_LIMIT);

    return res.json({ success: true, items });
  } catch (error) {
    console.error(`[CustodianUpload] listDifferenceHistory(${caType}):`, error);
    return fail(res, 500, { message: error.message || "Could not list the differential files." });
  }
};

/**
 * GET /api/split/history/download?key=... (and /api/bonus/history/download)
 * Only for keys listDifferenceHistory lists for the same corporate action.
 *
 * @param {"split"|"bonus"} caType
 */
export const downloadDifferenceFile = (caType) => async (req, res) => {
  try {
    const key = String(req.query?.key || "");
    const m = DIFFERENCE_KEY_PATTERN.exec(key);
    if (!m || m[1] !== caType) {
      return fail(res, 400, { message: `key must name a ${caType} differential file.` });
    }
    const downloadUrl = stratusSignedUrlToString(
      await getBucket(req.catalystApp).generatePreSignedUrl(key, "GET", STRATUS_GET_URL_OPTS),
    );
    return res.json({ success: true, downloadUrl });
  } catch (error) {
    console.error(`[CustodianUpload] downloadDifferenceFile(${caType}):`, error);
    return fail(res, 500, { message: error.message || "Could not create the download link." });
  }
};

/** Every upload of one kind for a date, newest first. */
const listForDate = async (bucket, kind, date) =>
  (await listAll(bucket, `${FILE_KINDS[kind].folder}/${date}/`))
    .map((item) => {
      const parsed = parseKey(item.key);
      return parsed ? { ...parsed, sizeBytes: item.sizeBytes } : null;
    })
    .filter(Boolean)
    .sort((a, b) => b.timeMs - a.timeMs);

/**
 * GET /api/custodian-upload/day/:date?kind=entitlement|allocation — the newest file of that kind uploaded for a date, or null.
 * The file carries its table import status (IMPORTING / DONE / FAILED, or "" when never imported);
 * the page asks again while it is IMPORTING.
 */
export const getDay = async (req, res) => {
  try {
    const kind = kindFrom(req.query?.kind);
    const date = String(req.params.date || "");
    if (!DATE_PATTERN.test(date)) {
      return fail(res, 400, { code: "BAD_DATE", message: "date must be yyyy-mm-dd." });
    }

    const bucket = filesBucket(req.catalystApp);
    // Stratus keeps listing a deleted object for about twenty seconds, so the
    // listed file is confirmed to still exist before it is shown.
    for (const item of await listForDate(bucket, kind, date)) {
      if (await bucket.headObject(item.key, { throwErr: false })) {
        const upload = await settleImport(req.catalystApp, item.key);
        return res.json({
          success: true,
          file: { ...item, importStatus: upload?.importStatus || "", importError: upload?.importError || "" },
        });
      }
    }
    return res.json({ success: true, file: null });
  } catch (error) {
    if (error.code === "BAD_KIND") return fail(res, 400, { code: error.code, message: error.message });
    console.error("[CustodianUpload] getDay:", error);
    return fail(res, 500, { message: error.message || "Could not read the uploaded files." });
  }
};

const HISTORY_LIMIT = 5;

/**
 * GET /api/custodian-upload/history?kind=entitlement|allocation
 * The newest uploads of one kind across every date, newest first, for download.
 * Replaced uploads for a date are listed too; removed ones are gone from storage.
 */
export const listHistory = async (req, res) => {
  try {
    const kind = kindFrom(req.query?.kind);
    const items = (await listAll(filesBucket(req.catalystApp), `${FILE_KINDS[kind].folder}/`))
      .map((item) => {
        const parsed = parseKey(item.key);
        return parsed ? { ...parsed, sizeBytes: item.sizeBytes } : null;
      })
      .filter(Boolean)
      .sort((a, b) => b.timeMs - a.timeMs)
      .slice(0, HISTORY_LIMIT);
    return res.json({ success: true, items });
  } catch (error) {
    if (error.code === "BAD_KIND") return fail(res, 400, { code: error.code, message: error.message });
    console.error("[CustodianUpload] listHistory:", error);
    return fail(res, 500, { message: error.message || "Could not list previous uploads." });
  }
};

/** GET /api/custodian-upload/download?key=... — one-hour presigned URL */
export const downloadCustodianFile = async (req, res) => {
  try {
    const parsed = parseKey(String(req.query?.key || "").trim());
    // Only this page's own files, so the endpoint cannot hand out arbitrary objects.
    if (!parsed) {
      return fail(res, 400, { code: "BAD_KEY", message: "That file cannot be downloaded here." });
    }
    const raw = await filesBucket(req.catalystApp).generatePreSignedUrl(parsed.key, "GET", STRATUS_GET_URL_OPTS);
    const downloadUrl = stratusSignedUrlToString(raw);
    if (!downloadUrl) {
      return fail(res, 404, { code: "NOT_FOUND", message: "Could not prepare the download." });
    }
    return res.json({ success: true, key: parsed.key, downloadUrl });
  } catch (error) {
    console.error("[CustodianUpload] downloadCustodianFile:", error);
    return fail(res, 500, { message: error.message || "Could not prepare the download." });
  }
};

/**
 * DELETE /api/custodian-upload/file?key=...
 *
 * For a file uploaded by mistake. Every upload of the same kind for that date
 * is deleted, not just this one, so an earlier upload does not quietly take its
 * place: after removal the date has no file of that kind until the correct one
 * is uploaded. The other kind's file is left alone.
 */
export const deleteCustodianFile = async (req, res) => {
  try {
    const parsed = parseKey(String(req.query?.key || "").trim());
    if (!parsed) {
      return fail(res, 400, { code: "BAD_KEY", message: "key must name an uploaded custodian file." });
    }

    const bucket = filesBucket(req.catalystApp);
    const keys = new Set([
      parsed.key,
      ...(await listForDate(bucket, parsed.kind, parsed.businessDate)).map((i) => i.key),
    ]);
    await removeUploads(req.catalystApp, parsed.kind, keys);

    return res.json({
      success: true,
      message: `Removed ${parsed.fileName}. Upload the correct custodian file for ${parsed.businessDate}.`,
    });
  } catch (error) {
    console.error("[CustodianUpload] deleteCustodianFile:", error);
    return fail(res, 500, { message: error.message || "Could not remove the custodian file." });
  }
};

/**
 * POST /api/custodian-upload/import-missing
 *
 * Imports every stored custodian file that has no table import yet: the files
 * uploaded before the tables existed, or after a deploy to a new environment.
 * Each is read through the field mapping saved now.
 */
export const importMissingFiles = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const bucket = filesBucket(catalystApp);
    const imported = await listUploadKeys(catalystApp.zcql());
    const started = [];
    const skipped = [];

    for (const kind of Object.keys(FILE_KINDS)) {
      const profile = await readFieldMapProfile(getBucket(catalystApp), FILE_KINDS[kind].profileType);
      const items = (await listAll(bucket, `${FILE_KINDS[kind].folder}/`))
        .map((item) => parseKey(item.key))
        .filter((item) => item && !imported.has(item.key));

      for (const item of items) {
        if (!profile) {
          skipped.push({ key: item.key, reason: "No saved field mapping for this file kind." });
          continue;
        }
        const buffer = await getBuffer(bucket, item.key);
        const rows = await readRows(buffer);
        const missing = missingRequiredFields(kind, columnIndexes(profile, (rows[0] || []).map(normHeader), normHeader));
        if (missing.length > 0) {
          skipped.push({ key: item.key, reason: `Not mapped: ${missing.join(", ")}.` });
          continue;
        }
        const upload = await importFile(catalystApp, { kind, key: item.key, rows, profile, fileHash: sha256(buffer) });
        started.push({ key: item.key, importStatus: upload?.importStatus || "" });
      }
    }

    return res.json({ success: true, started, skipped });
  } catch (error) {
    console.error("[CustodianUpload] importMissingFiles:", error);
    return fail(res, 500, { message: error.message || "Could not import the stored custodian files." });
  }
};
