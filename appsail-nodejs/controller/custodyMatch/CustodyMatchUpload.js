/**
 * Custody Matching — upload, validate, list and delete (stage S0).
 *
 * Validation runs HERE, synchronously, while the user is still looking at the
 * screen. That is the single most valuable placement decision in the design:
 * validating inside the job function instead would mean the user uploads, waits,
 * polls, and only then learns that row 412 has an 11-character ISIN — with a
 * failed run to clean up afterwards.
 *
 * Nothing invalid ever reaches the bucket, so the job function can assume its
 * inputs parse.
 *
 * Error bodies follow the shape controller/uploader/TempTransactionUpload.js
 * already uses ({ success:false, code, message, hint, ... }) so the upload page's
 * existing error renderer works without changes.
 */

import {
  FA_FILE_TYPE,
  FA_FILE_TYPE_LABEL,
  FILE_SIDE,
  PREDEFINED_SCHEMES,
  ROW_ERROR,
  RUN_STATUS,
} from "../../util/custodyMatch/constants.js";
import { CUSTODY_MODE, closedDateMessage, compareCustodyRows } from "../../util/custodyMatch/equityReload.js";
import { PROFILE_TYPE } from "../../util/custodyMatch/fieldMap.js";
import { detectFaFileType, parseFaFile, validateFaHeader } from "../../util/custodyMatch/parseFA.js";
import { parseCustodyFile } from "../../util/custodyMatch/parseCustody.js";
import {
  BUCKET_NAME,
  CUSTODY_FILE_TYPES,
  CUSTODY_HISTORY_LIMIT,
  FA_FILE_TYPES,
  SIDE_KEY,
  buildCustodyKey,
  buildFaRawKey,
  bucketExists,
  parseCustodyKey,
  parseFaRawKey,
  deleteDaySide,
  deleteKeys,
  getBucket,
  getBuffer,
  listDay,
  listCustodyHistory,
  listKnownSchemes,
  loadFieldMapProfiles,
  parseCustodyHistoryKey,
  putBuffer,
  saveToCustodyHistory,
  signedGetUrl,
} from "../../util/custodyMatch/storage.js";
import {
  deleteFileRows,
  listFiles,
  listRunsForDate,
  recordFile,
  supersedeRunsForDate,
} from "../../util/custodyMatch/runStore.js";
import { insertNotification } from "../../util/notifications.js";

/** Row errors are for reading, not for scrolling. Send the first page and say how many more. */
const ZCQL_PAGE = 300;
const MAX_REPORTED_ROW_ERRORS = 50;
const MAX_REPORTED_WARNINGS = 20;

const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

const fail = (res, status, body) =>
  res.status(status).json({ success: false, ...body });

const noApp = (res) =>
  fail(res, 500, {
    code: "CATALYST_NOT_INITIALIZED",
    message: "Catalyst not initialized",
  });

/** Trim the error list and report the remainder rather than shipping 4,000 rows. */
export const summariseIssues = (rowErrors = [], warnings = []) => ({
  rowErrors: rowErrors.slice(0, MAX_REPORTED_ROW_ERRORS),
  rowErrorCount: rowErrors.length,
  moreRowErrors: Math.max(rowErrors.length - MAX_REPORTED_ROW_ERRORS, 0),
  warnings: warnings.slice(0, MAX_REPORTED_WARNINGS),
  warningCount: warnings.length,
});

/**
 * The date the user picked for this file. Required: a file is filed under the
 * day its rows are for, so there is no sensible default to fall back on.
 */
export const readUploadDate = (requested) => {
  const given = String(requested || "").trim();
  if (!given) {
    return { error: "Choose the date this file is for before uploading it." };
  }
  if (!DATE_PATTERN.test(given)) {
    return { error: `businessDate must be yyyy-mm-dd; received "${given}".` };
  }
  return { businessDate: given };
};

/** Today in India. The server clock is UTC, so this is what "today" means to the user. */
const indiaToday = () => new Date(Date.now() + 330 * 60 * 1000).toISOString().slice(0, 10);

/**
 * The upload date the user picked at the top of the screen. Files are recorded
 * under it and Match offers them by it. Required, and not later than today.
 */
export const readUploadedOn = (requested) => {
  const given = String(requested || "").trim();
  if (!given) {
    return { error: "Choose the upload date at the top of the screen before uploading." };
  }
  if (!DATE_PATTERN.test(given)) {
    return { error: `uploadDate must be yyyy-mm-dd; received "${given}".` };
  }
  if (given > indiaToday()) {
    return { error: `The upload date ${given} is in the future.` };
  }
  return { uploadedOn: given };
};

/**
 * The files saved under one upload date — for one file date, or (no date) for
 * every file date. The file table decides which files; the Stratus listing of
 * each file date's folder supplies their sizes. No upload date means no files.
 */
export const uploadDayFiles = async (catalystApp, date, uploadDate) => {
  const rows = uploadDate
    ? await listFiles(catalystApp, { forDate: date || null, uploadedOn: uploadDate })
    : [];
  const fileDates = [...new Set(rows.map((r) => String(r.forDate)))];
  const days = await Promise.all(fileDates.map((d) => listDay(catalystApp, d)));
  const sizeByKey = new Map(
    days.flatMap((day) => [...day.fa.raw, ...day.custody.files]).map((f) => [f.key, f.sizeBytes]),
  );
  const describe = (row, parseKey) => {
    const parsed = parseKey(row.fileKey) || {};
    return {
      ...parsed,
      key: row.fileKey,
      fileType: row.fileType,
      scheme: row.scheme || parsed.scheme || "",
      originalFileName: row.originalFileName || parsed.originalFileName || "",
      // A just-uploaded object can be missing from the listing for a moment.
      sizeBytes: sizeByKey.get(row.fileKey) ?? null,
      uploadedOn: String(row.uploadedOn),
    };
  };

  // Newest file date first, so the all-dates view reads top-down by date.
  const byFileDateDesc = (a, b) => String(b.coveredFrom).localeCompare(String(a.coveredFrom));
  const faRaw = rows
    .filter((r) => r.side === FILE_SIDE.FA)
    .map((r) => describe(r, parseFaRawKey))
    .sort(byFileDateDesc);
  const custodyFiles = rows
    .filter((r) => r.side === FILE_SIDE.CUSTODY)
    .map((r) => describe(r, parseCustodyKey))
    .sort(byFileDateDesc);

  return {
    businessDate: date || "",
    uploadDate: uploadDate || "",
    fa: { raw: faRaw, hasFile: faRaw.length > 0 },
    custody: { files: custodyFiles, hasFiles: custodyFiles.length > 0 },
  };
};

/**
 * Save a new file, then drop the one it replaces.
 *
 * The new object and its row are written first: if either fails nothing that
 * was already there is lost. Only then are the replaced rows and their objects
 * deleted. If recording the row fails the new object is deleted again, since a
 * file in Stratus with no row is invisible to the cards and to Match.
 */
const storeReplacing = async (catalystApp, bucket, { key, data, row, replacing }) => {
  await putBuffer(bucket, key, data, "text/csv");
  try {
    await recordFile(catalystApp, { ...row, fileKey: key });
  } catch (err) {
    const undo = await deleteKeys(bucket, [key]);
    if (undo.failed.length > 0) {
      console.error("[CustodyMatchUpload] could not remove unrecorded file:", key);
    }
    throw new Error(`The file could not be recorded for matching, so it was not kept: ${err.message}`);
  }

  if (replacing.length > 0) {
    await deleteFileRows(catalystApp, replacing);
    const removed = await deleteKeys(bucket, replacing.map((r) => r.fileKey));
    if (removed.failed.length > 0) {
      // Harmless to Match (the rows are gone); the cleanup cron clears the objects.
      console.warn("[CustodyMatchUpload] replaced objects not deleted:", removed.failed);
    }
  }
};

/**
 * Every row must be dated the day the file is uploaded for. Blank and
 * unreadable dates are already row errors from the parser, so only rows that
 * parsed to a different date are reported here.
 */
const wrongDateErrors = (rows, businessDate, fileName) =>
  rows
    .filter((r) => r.tranDate !== businessDate)
    .map((r) => ({
      row: r.rowNumber,
      code: ROW_ERROR.WRONG_DATE,
      message: `This row is dated ${r.tranDate}, but the file is being uploaded for ${businessDate}. Every row must be dated ${businessDate}.`,
      column: null,
      value: r.tranDate,
      fileName,
    }));

/** Parser errors and wrong-date errors together, in file order. */
export const allRowErrors = (parsed, businessDate, fileName) =>
  [...parsed.rowErrors, ...wrongDateErrors(parsed.rows, businessDate, fileName)].sort(
    (a, b) => a.row - b.row,
  );

export const getUploadedFile = (req) => req.files?.file || null;

export const isCsv = (name) => String(name || "").toLowerCase().endsWith(".csv");

/**
 * GET /api/custody-match/files?uploadDate=yyyy-mm-dd[&date=yyyy-mm-dd]
 *
 * The files saved under an upload date — every file date, or just `date` when
 * given. Backs the file lists on the two upload cards.
 */
export const listUploadedFiles = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);

    const uploadDate = String(req.query.uploadDate || "").trim();
    const date = String(req.query.date || "").trim();
    for (const [name, value] of [["uploadDate", uploadDate], ["date", date]]) {
      if (value && !DATE_PATTERN.test(value)) {
        return fail(res, 400, {
          code: "BAD_DATE",
          message: `${name} must be yyyy-mm-dd; received "${value}".`,
        });
      }
    }

    const day = await uploadDayFiles(catalystApp, date, uploadDate);
    return res.status(200).json({ success: true, ...day });
  } catch (error) {
    console.error("[CustodyMatchUpload] listUploadedFiles:", error);
    return fail(res, 500, {
      message: error.message || "Could not read the files for that date.",
    });
  }
};

/**
 * POST /api/custody-match/fa-file
 * form-data: file, businessDate (every row must be dated this day), [fileType], [replace=true]
 *
 * Validates and stages the 26-column transaction file. Staging only — this does
 * NOT upload to client-transaction-files and does NOT import anything.
 */
/**
 * GET /api/custody-match/schemes
 *
 * The schemes the picker offers, each with the layout it was last filed under.
 *
 * Two sources, and the order matters. First the ones DISCOVERED from the stored
 * keys — a scheme exists there because a file for it exists, which is the only
 * definition that cannot drift, and those carry a real last-used date. Then the
 * configured ones that have never been filed, so a fresh system still offers
 * something to pick instead of an empty list.
 *
 * A configured scheme that HAS been filed is dropped from the second list: the
 * discovered entry knows the layout actually used and the date it was used on.
 */
export const listSchemes = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);

    const discovered = await listKnownSchemes(catalystApp);
    const filed = new Set(discovered.map((s) => s.scheme));

    const schemes = [
      ...discovered,
      ...PREDEFINED_SCHEMES.filter((s) => !filed.has(s.scheme)).map((s) => ({
        ...s,
        // Never filed, so there is no date to report.
        lastUsedDate: "",
      })),
    ];

    return res.json({ success: true, schemes, count: schemes.length });
  } catch (err) {
    console.error("[CustodyMatchUpload] listSchemes:", err);
    return fail(res, 500, {
      code: "SCHEME_LIST_FAILED",
      message: `Could not list the schemes: ${err.message}`,
    });
  }
};

/**
 * GET /api/custody-match/dates
 *
 * One entry per (upload date, file date) that has a file, newest file date
 * first, with what each side has. Backs the Match dialog and the summary: the
 * screen keeps the entries for the upload date picked at the top. A pair can
 * only be matched when both sides were uploaded under that upload date.
 */
export const listMatchDates = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);

    const byPair = new Map();
    for (const f of await listFiles(catalystApp)) {
      const uploadDate = String(f.uploadedOn);
      const date = String(f.forDate);
      const pairKey = `${uploadDate}|${date}`;
      if (!byPair.has(pairKey)) {
        byPair.set(pairKey, { uploadDate, date, faFiles: [], custodyFiles: [] });
      }
      const entry = byPair.get(pairKey);
      const file = {
        key: f.fileKey,
        fileType: f.fileType,
        scheme: f.scheme || "",
        fileName: f.originalFileName || "",
      };
      if (f.side === FILE_SIDE.FA) entry.faFiles.push(file);
      else entry.custodyFiles.push(file);
    }

    const dates = [...byPair.values()]
      .map((d) => ({ ...d, canMatch: d.faFiles.length > 0 && d.custodyFiles.length > 0 }))
      .sort((a, b) => b.date.localeCompare(a.date) || b.uploadDate.localeCompare(a.uploadDate));

    return res.json({ success: true, dates, count: dates.length });
  } catch (err) {
    console.error("[CustodyMatchUpload] listMatchDates:", err);
    return fail(res, 500, {
      code: "DATE_LIST_FAILED",
      message: `Could not list the dates that have files: ${err.message}`,
    });
  }
};

export const uploadFaFile = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);

    const file = getUploadedFile(req);
    if (!file) {
      return fail(res, 400, {
        code: "NO_FILE",
        message: "CSV file is required. Send it as form-data with key 'file'.",
      });
    }
    if (!isCsv(file.name)) {
      return fail(res, 400, {
        code: "NOT_CSV",
        message: "Only CSV files are allowed.",
      });
    }

    /*
     * Which transaction file this is. Mutual fund transactions arrive in their
     * own fund-accounting file, read through its own mapping — the two layouts
     * differ, so one mapping cannot describe both.
     */
    const faFileType = String(req.body?.fileType || FA_FILE_TYPE.REGULAR).toUpperCase();
    if (!FA_FILE_TYPES.includes(faFileType)) {
      return fail(res, 400, {
        code: "BAD_FILE_TYPE",
        message: `fileType must be one of ${FA_FILE_TYPES.join(", ")}; received "${req.body?.fileType || ""}".`,
      });
    }
    const profileType =
      faFileType === FA_FILE_TYPE.MUTUAL_FUND ? PROFILE_TYPE.FA_MUTUAL_FUND : PROFILE_TYPE.FA;

    // The saved mapping decides which column is which. Unsaved -> the built-in
    // 26-column template, i.e. exactly the behaviour before mapping existed.
    const profiles = await loadFieldMapProfiles(catalystApp);
    const faProfile = profiles[profileType];

    // Header contract first: a wrong header makes every row error meaningless.
    const headerError = validateFaHeader(file.data, { profile: faProfile, type: profileType });
    if (headerError) {
      return fail(res, 400, { ...headerError, fileName: file.name });
    }

    // The screen blocks this when the file is picked; checked again here so a
    // file sent any other way cannot land in the wrong slot.
    const fileProfileType = await detectFaFileType(file.data, { profiles });
    if (fileProfileType !== profileType) {
      return fail(res, 400, {
        code: "FA_FILE_TYPE_MISMATCH",
        message:
          profileType === PROFILE_TYPE.FA
            ? "Wrong file type — Equity selected, but this file has TDS / Stamp Duty amounts, so it is a Mutual Funds transaction file."
            : "Wrong file type — Mutual Funds selected, but TDS and Stamp Duty are empty in every row, so it is an Equity transaction file.",
        fileName: file.name,
      });
    }

    const forDate = readUploadDate(req.body?.businessDate);
    if (forDate.error) {
      return fail(res, 400, { code: "BAD_DATE", message: forDate.error });
    }
    const { businessDate } = forDate;

    const uploadDay = readUploadedOn(req.body?.uploadDate);
    if (uploadDay.error) {
      return fail(res, 400, { code: "BAD_UPLOAD_DATE", message: uploadDay.error });
    }
    const { uploadedOn } = uploadDay;

    const parsed = await parseFaFile(file.data, {
      businessDate,
      profile: faProfile,
      type: profileType,
    });
    if (parsed.fileError) {
      return fail(res, 400, { ...parsed.fileError, fileName: file.name });
    }

    const rowErrors = allRowErrors(parsed, businessDate, file.name);
    if (rowErrors.length > 0) {
      return fail(res, 400, {
        code: "VALIDATION_FAILED",
        message: `${rowErrors.length} row(s) in the transaction file are not valid for ${businessDate}. Nothing was uploaded.`,
        fileName: file.name,
        businessDate,
        ...summariseIssues(rowErrors, parsed.warnings),
        hint: `Every row must be dated ${businessDate}. Fix the rows listed above, or pick the date the file is for, then upload it again.`,
      });
    }

    const wantsReplace = String(req.body?.replace || "").toLowerCase() === "true";

    /*
     * Only a file of the SAME type, for the same file date AND the same upload
     * date, conflicts. The same file date under another upload date is a
     * separate file and is left alone. The regular and mutual fund files are
     * both expected for a date, so one never reads as the other.
     */
    const sameSlot = (await listFiles(catalystApp, { forDate: businessDate, uploadedOn })).filter(
      (r) => r.side === FILE_SIDE.FA && r.fileType === faFileType,
    );

    if (sameSlot.length > 0 && !wantsReplace) {
      return fail(res, 409, {
        code: "FILE_ALREADY_EXISTS",
        message: `A ${FA_FILE_TYPE_LABEL[faFileType]} file for ${businessDate} is already uploaded under ${uploadedOn}.`,
        businessDate,
        uploadDate: uploadedOn,
        fileType: faFileType,
        existing: sameSlot.map((r) => ({ key: r.fileKey, fileName: r.originalFileName })),
        hint: "Re-send with replace=true to delete the stored file and use this one instead.",
      });
    }

    const bucket = getBucket(catalystApp);

    // Every row is dated businessDate, so the file covers exactly that day.
    const uploadedAtMs = Date.now();
    const key = buildFaRawKey({
      businessDate,
      fileType: faFileType,
      coveredFrom: businessDate,
      coveredTo: businessDate,
      originalFileName: file.name,
      uploadedAtMs,
    });
    await storeReplacing(catalystApp, bucket, {
      key,
      data: file.data,
      row: {
        side: FILE_SIDE.FA,
        fileType: faFileType,
        forDate: businessDate,
        uploadedOn,
        originalFileName: file.name,
      },
      replacing: sameSlot,
    });

    // Earlier results for this upload date's files no longer describe them.
    const superseded = await supersedeRunsForDate(catalystApp, businessDate, uploadedOn);

    return res.status(200).json({
      success: true,
      message: `The ${FA_FILE_TYPE_LABEL[faFileType]} file is staged for ${businessDate}. It has not been imported.`,
      businessDate,
      key,
      fileType: faFileType,
      fileName: file.name,
      uploadedAtMs,
      sizeBytes: file.data.length,
      stats: parsed.stats,
      ...summariseIssues([], parsed.warnings),
      supersededRuns: superseded.runIds,
      staged: true,
      imported: false,
    });
  } catch (error) {
    console.error("[CustodyMatchUpload] uploadFaFile:", error);
    return fail(res, 500, {
      message: error.message || "Could not stage the transaction file.",
    });
  }
};

/**
 * POST /api/custody-match/custody-file
 * form-data: file, scheme, fileType (RESIDENTIAL|NDPMS|MUTUAL_FUND), businessDate, [replace=true]
 *
 * One deal-allocation file per scheme. fileType selects which custodian layout
 * to expect inside it, and which field mapping reads it.
 */
export const uploadCustodyFile = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);

    const file = getUploadedFile(req);
    if (!file) {
      return fail(res, 400, {
        code: "NO_FILE",
        message: "CSV file is required. Send it as form-data with key 'file'.",
      });
    }
    if (!isCsv(file.name)) {
      return fail(res, 400, { code: "NOT_CSV", message: "Only CSV files are allowed." });
    }

    const scheme = String(req.body?.scheme || "").trim();
    if (!scheme) {
      return fail(res, 400, {
        code: "NO_SCHEME",
        message:
          "scheme is required — it tags every exception this file produces so the user knows where to look.",
      });
    }

    const fileType = String(req.body?.fileType || "").trim().toUpperCase();
    if (!CUSTODY_FILE_TYPES.includes(fileType)) {
      return fail(res, 400, {
        code: "BAD_FILE_TYPE",
        message: `fileType must be one of ${CUSTODY_FILE_TYPES.join(", ")}; received "${req.body?.fileType || ""}".`,
      });
    }

    // Layout is detected from the columns, then cross-checked against the slot
    // the user dropped it into — a file in the wrong slot would mislabel the
    // scheme on every exception it produces.
    const profiles = await loadFieldMapProfiles(catalystApp);

    const forDate = readUploadDate(req.body?.businessDate);
    if (forDate.error) {
      return fail(res, 400, { code: "BAD_DATE", message: forDate.error });
    }
    const { businessDate } = forDate;

    const uploadDay = readUploadedOn(req.body?.uploadDate);
    if (uploadDay.error) {
      return fail(res, 400, { code: "BAD_UPLOAD_DATE", message: uploadDay.error });
    }
    const { uploadedOn } = uploadDay;

    const parsed = await parseCustodyFile(file.data, {
      scheme,
      fileType,
      fileName: file.name,
      // NDPMS files have no date column, so their rows take this date.
      businessDate,
      profiles,
    });
    if (parsed.fileError) {
      return fail(res, 400, { ...parsed.fileError, scheme, fileType });
    }

    const rowErrors = allRowErrors(parsed, businessDate, file.name);
    if (rowErrors.length > 0) {
      return fail(res, 400, {
        code: "VALIDATION_FAILED",
        message: `${rowErrors.length} row(s) in ${file.name} are not valid for ${businessDate}. Nothing was uploaded.`,
        fileName: file.name,
        scheme,
        fileType,
        businessDate,
        detectedFormat: parsed.format,
        ...summariseIssues(rowErrors, parsed.warnings),
        hint: `Every row must be dated ${businessDate}. Correct the rows listed above, or pick the date the file is for, then upload it again.`,
      });
    }

    // Every row is dated businessDate, so the file covers exactly that day.
    const coveredFrom = businessDate;
    const coveredTo = businessDate;

    // Build the key we would write, then read the scheme back out of it, so the
    // slot comparison uses storage.js's own sanitisation rather than a second
    // copy of the rule that could drift away from it.
    const prospectiveKey = buildCustodyKey({
      businessDate,
      scheme,
      fileType,
      coveredFrom,
      coveredTo,
      originalFileName: file.name,
      uploadedAtMs: 0,
    });
    const storedScheme = parseCustodyKey(prospectiveKey).scheme;

    /*
     * One file per scheme and type, per file date AND upload date. The same
     * file date under another upload date is a separate file and is left alone.
     *
     * Equity tab (EquityReload.js sets req.equityReload): a date can hold several
     * files of a scheme, whatever upload date they came under, and the new file's
     * lines decide what happens (compareCustodyRows).
     */
    const reload = req.equityReload || null;
    const slotFilter = reload ? { forDate: businessDate } : { forDate: businessDate, uploadedOn };
    const sameSlot = (await listFiles(catalystApp, slotFilter)).filter(
      (r) =>
        r.side === FILE_SIDE.CUSTODY && r.scheme === storedScheme && r.fileType === fileType,
    );
    const wantsReplace = String(req.body?.replace || "").toLowerCase() === "true";
    const bucket = getBucket(catalystApp);

    let reloadComparison = null;
    let replacesStoredFiles = !reload && sameSlot.length > 0;
    if (reload) {
      const storedRows = [];
      for (const stored of sameSlot) {
        const storedFile = await parseCustodyFile(await getBuffer(bucket, stored.fileKey), {
          scheme,
          fileType,
          fileName: stored.originalFileName,
          businessDate,
          profiles,
        });
        if (storedFile.fileError) {
          return fail(res, 409, {
            code: "STORED_FILE_UNREADABLE",
            message: `The stored file ${stored.originalFileName} no longer reads against the saved field mapping, so this file cannot be compared with it. Remove the stored file and upload it again.`,
          });
        }
        storedRows.push(...storedFile.rows);
      }

      reloadComparison = compareCustodyRows(parsed.rows, storedRows);
      const answer = {
        success: true,
        stored: false,
        mode: reloadComparison.mode,
        businessDate,
        fileName: file.name,
        scheme,
        fileType,
        counts: reloadComparison.counts,
        blankNumbers: reloadComparison.blankNumbers,
        lookAlikes: reloadComparison.lookAlikes,
        existing: sameSlot.map((r) => r.originalFileName),
      };

      if (reloadComparison.mode === CUSTODY_MODE.SKIP) {
        return res.status(200).json({
          ...answer,
          message: `Every line of ${file.name} is already uploaded for ${scheme} on ${businessDate}, so nothing was stored.`,
        });
      }
      if (reloadComparison.mode === CUSTODY_MODE.ASK) {
        if (!wantsReplace) {
          return res.status(200).json({
            ...answer,
            message: "This file overlaps the stored custodian lines. Replace the stored file(s), or cancel.",
          });
        }
        replacesStoredFiles = true;
        const runs = await listRunsForDate(catalystApp, businessDate, ZCQL_PAGE);
        if (runs.some((r) => r.status === RUN_STATUS.QUEUED || r.status === RUN_STATUS.RUNNING)) {
          return fail(res, 409, {
            code: "MATCH_RUNNING",
            message: `A match of ${businessDate} is running. Replace the file when it finishes.`,
          });
        }
      }
    }

    if (!reload && sameSlot.length > 0 && !wantsReplace) {
      return fail(res, 409, {
        code: "FILE_ALREADY_EXISTS",
        message: `A ${fileType} file for ${scheme} for ${businessDate} is already uploaded under ${uploadedOn}.`,
        businessDate,
        uploadDate: uploadedOn,
        scheme,
        fileType,
        existing: sameSlot.map((r) => ({
          key: r.fileKey,
          scheme: r.scheme,
          fileType: r.fileType,
          fileName: r.originalFileName,
        })),
        hint: `Re-send with replace=true. Only this file — ${scheme} ${fileType} for ${businessDate} under ${uploadedOn} — is replaced.`,
      });
    }

    const uploadedAtMs = Date.now();
    const key = buildCustodyKey({
      businessDate,
      scheme,
      fileType,
      coveredFrom,
      coveredTo,
      originalFileName: file.name,
      uploadedAtMs,
    });
    await storeReplacing(catalystApp, bucket, {
      key,
      data: file.data,
      row: {
        side: FILE_SIDE.CUSTODY,
        fileType,
        scheme: storedScheme,
        forDate: businessDate,
        uploadedOn,
        originalFileName: file.name,
      },
      replacing: replacesStoredFiles ? sameSlot : [],
    });

    /*
     * Keep a copy for Previous Uploads. Best-effort: the file IS stored for
     * matching by now, and failing the request over the history copy would tell
     * the user their upload did not work when it did.
     */
    let uploadHistory = null;
    try {
      const kept = await saveToCustodyHistory(
        catalystApp,
        {
          buffer: file.data,
          scheme,
          fileType,
          businessDate,
          originalFileName: file.name,
          uploadedAtMs,
        },
        { bucket },
      );
      uploadHistory = kept.map(toHistoryItem);
    } catch (historyErr) {
      console.error("[CustodyMatchUpload] could not save upload history copy:", historyErr);
    }

    // The Equity tab matches a date against all its custodian files, so every
    // run of the date is out of date, not only this upload date's.
    const superseded = await supersedeRunsForDate(catalystApp, businessDate, reload ? null : uploadedOn);

    // The file is in by now, and the runs above can no longer be pre-committed,
    // so a failed clean-up is reported rather than failing the upload.
    let cleanupError = "";
    if (reload && replacesStoredFiles) {
      try {
        await reload.clearMatchResults(businessDate);
      } catch (cleanupErr) {
        console.error("[CustodyMatchUpload] could not clear the match results:", cleanupErr);
        cleanupError = `The file was replaced, but the old match results of ${businessDate} could not all be removed: ${cleanupErr.message}`;
      }
    }

    const after = await uploadDayFiles(catalystApp, businessDate, uploadedOn);

    return res.status(200).json({
      success: true,
      ...(reload
        ? {
            stored: true,
            mode: replacesStoredFiles ? CUSTODY_MODE.REPLACE : reloadComparison.mode,
            counts: reloadComparison.counts,
            cleanupError,
          }
        : null),
      message: `File stored for ${scheme} for ${businessDate}.`,
      businessDate,
      key,
      fileName: file.name,
      scheme,
      fileType,
      coveredFrom,
      coveredTo,
      detectedFormat: parsed.format,
      uploadedAtMs,
      sizeBytes: file.data.length,
      stats: parsed.stats,
      ...summariseIssues([], parsed.warnings),
      replacedSlot: replacesStoredFiles,
      supersededRuns: superseded.runIds,
      files: after.custody.files,
      uploadHistory,
    });
  } catch (error) {
    console.error("[CustodyMatchUpload] uploadCustodyFile:", error);
    return fail(res, 500, {
      message: error.message || "Could not store the custodian file.",
    });
  }
};

/** One Previous Uploads row, as the screen reads it. */
const toHistoryItem = (item) => ({
  key: item.key,
  fileName: item.originalFileName,
  scheme: item.scheme,
  fileType: item.fileType,
  businessDate: item.businessDate,
  uploadedAtMs: item.uploadedAtMs,
  sizeBytes: item.sizeBytes,
});

/**
 * GET /api/custody-match/custody-history
 *
 * The most recent custodian uploads, newest first, for download. These are
 * copies kept outside the date folders, so they are still here after the
 * working file was replaced, removed or swept by the cleanup cron.
 */
export const listCustodyUploads = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);

    const items = (await listCustodyHistory(catalystApp))
      .slice(0, CUSTODY_HISTORY_LIMIT)
      .map(toHistoryItem);

    return res.json({ success: true, items, count: items.length });
  } catch (error) {
    console.error("[CustodyMatchUpload] listCustodyUploads:", error);
    return fail(res, 500, {
      message: error.message || "Could not read previous custodian uploads.",
    });
  }
};

/**
 * DELETE /api/custody-match/file?key=...
 *
 * Removes one staged file, either side. Both sides list a file per row with its
 * own remove button, so the button has to take that file and leave the rest of
 * the day — other schemes, other dates, the difference reports — where they are.
 */
/* How long to keep checking that a deleted file has left the Stratus listing. */
const REMOVAL_CHECK_EVERY_MS = 10 * 1000;
const REMOVAL_CHECK_FOR_MS = 5 * 60 * 1000;

const isListed = (day, key) =>
  day.custody.files.some((f) => f.key === key) || day.fa.raw.some((f) => f.key === key);

/**
 * Settle the bell's removal notice once the file is really gone.
 *
 * deleteObject returns before Stratus's listing catches up, so "removed" is only
 * reported once the key stops appearing in the date's listing. Runs after the
 * response has been sent, the same way the import's bulk-job poll does.
 */
const confirmRemoval = async (catalystApp, { key, businessDate, jobName, title, fileName }) => {
  const zcql = catalystApp.zcql();
  const deadline = Date.now() + REMOVAL_CHECK_FOR_MS;

  while (Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, REMOVAL_CHECK_EVERY_MS));
    try {
      if (!isListed(await listDay(catalystApp, businessDate), key)) {
        await insertNotification(zcql, {
          jobName,
          title,
          status: "completed",
          message: `${fileName} removed.`,
        });
        return;
      }
    } catch (err) {
      console.warn("[CustodyMatchUpload] confirmRemoval check failed:", err.message);
    }
  }

  // The object itself was deleted; only the listing is slow. Say so rather than failing.
  await insertNotification(zcql, {
    jobName,
    title,
    status: "completed",
    message: `${fileName} removed. It may still appear for a short while.`,
  });
};

export const deleteUploadedFile = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);

    const key = String(req.query.key || "").trim();
    /*
     * Parsing the key IS the validation. Only a custodian upload or an FA raw
     * upload parses, so no other kind of object — a merged final, a difference
     * report — can be reached through this route.
     */
    const parsed = key ? parseCustodyKey(key) || parseFaRawKey(key) : null;
    if (!parsed) {
      return fail(res, 400, {
        code: "BAD_KEY",
        message: "key must name a stored transaction or custodian file.",
      });
    }

    // A trade date past its change window is locked, removal included.
    const closed = closedDateMessage(parsed.businessDate);
    if (closed) return fail(res, 409, { code: "DATE_CLOSED", message: closed });

    const fileName = parsed.originalFileName || key.split("/").pop();
    const notice = { jobName: `RM-${Date.now()}`, title: "File removal" };
    const zcql = catalystApp.zcql();

    const bucket = getBucket(catalystApp);
    const result = await deleteKeys(bucket, [key]);
    if (result.failed.length > 0) {
      await insertNotification(zcql, {
        ...notice,
        status: "failed",
        message: `${fileName} could not be removed. Please try again.`,
      }).catch((e) => console.warn("[CustodyMatchUpload] notification failed:", e.message));
      return fail(res, 500, {
        code: "DELETE_FAILED",
        message: `Could not remove ${key.split("/").pop()}.`,
        failed: result.failed,
      });
    }

    // The row goes with the file. Its upload date says whose runs are now stale.
    const rows = (await listFiles(catalystApp, { forDate: parsed.businessDate })).filter(
      (r) => r.fileKey === key,
    );
    await deleteFileRows(catalystApp, rows);
    const uploadedOn = rows[0] ? String(rows[0].uploadedOn) : null;
    const superseded = uploadedOn
      ? await supersedeRunsForDate(catalystApp, parsed.businessDate, uploadedOn)
      : { runIds: [] };
    const after = await uploadDayFiles(catalystApp, parsed.businessDate, uploadedOn);

    // "In progress" until confirmRemoval sees the key leave the listing.
    try {
      await insertNotification(zcql, {
        ...notice,
        status: "in_progress",
        message: `Removing ${fileName}.`,
      });
      void confirmRemoval(catalystApp, {
        key,
        businessDate: parsed.businessDate,
        fileName,
        ...notice,
      }).catch((e) => console.error("[CustodyMatchUpload] confirmRemoval:", e.message));
    } catch (e) {
      // The file is removed either way; a missing bell entry must not fail the request.
      console.warn("[CustodyMatchUpload] removal notification failed:", e.message);
    }

    return res.json({
      success: true,
      message: `Removed ${parsed.originalFileName || `${parsed.scheme} ${parsed.fileType}`}.`,
      businessDate: parsed.businessDate,
      key,
      supersededRuns: superseded.runIds,
      files: after.custody.files,
      faFiles: after.fa.raw,
    });
  } catch (error) {
    console.error("[CustodyMatchUpload] deleteUploadedFile:", error);
    return fail(res, 500, {
      message: error.message || "Could not remove the file.",
    });
  }
};

/**
 * DELETE /api/custody-match/day/:date?side=fa|custody|all
 *
 * Clears a side wholesale so the user can restart a day — for every upload
 * date, since it empties the file date's folder.
 */
export const deleteDayFiles = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);

    const date = String(req.params.date || "").trim();
    if (!DATE_PATTERN.test(date)) {
      return fail(res, 400, {
        code: "BAD_DATE",
        message: `Business date must be yyyy-mm-dd; received "${date}".`,
      });
    }

    const side = String(req.query.side || "").trim().toLowerCase();
    if (![SIDE_KEY.FA, SIDE_KEY.CUSTODY, "all"].includes(side)) {
      return fail(res, 400, {
        code: "BAD_SIDE",
        message: `side must be "${SIDE_KEY.FA}", "${SIDE_KEY.CUSTODY}" or "all".`,
      });
    }

    const bucket = getBucket(catalystApp);
    const results = [];

    if (side === SIDE_KEY.FA || side === "all") {
      results.push(await deleteDaySide(catalystApp, date, SIDE_KEY.FA, { bucket }));
    }
    if (side === SIDE_KEY.CUSTODY || side === "all") {
      results.push(await deleteDaySide(catalystApp, date, SIDE_KEY.CUSTODY, { bucket }));
    }

    const clearedSides = new Set();
    if (side === SIDE_KEY.FA || side === "all") clearedSides.add(FILE_SIDE.FA);
    if (side === SIDE_KEY.CUSTODY || side === "all") clearedSides.add(FILE_SIDE.CUSTODY);
    await deleteFileRows(
      catalystApp,
      (await listFiles(catalystApp, { forDate: date })).filter((r) => clearedSides.has(r.side)),
    );

    const superseded = await supersedeRunsForDate(catalystApp, date);
    const failedDeletes = results.flatMap((r) => r.failed);

    return res.status(failedDeletes.length > 0 ? 207 : 200).json({
      success: failedDeletes.length === 0,
      businessDate: date,
      deleted: results.reduce((sum, r) => sum + r.count, 0),
      bySide: results.map((r) => ({ side: r.side, count: r.count })),
      failed: failedDeletes,
      supersededRuns: superseded.runIds,
      message:
        failedDeletes.length === 0
          ? `Cleared ${results.reduce((sum, r) => sum + r.count, 0)} file(s) for ${date}.`
          : `Cleared some files, but ${failedDeletes.length} could not be deleted.`,
    });
  } catch (error) {
    console.error("[CustodyMatchUpload] deleteDayFiles:", error);
    return fail(res, 500, {
      message: error.message || "Could not clear the files for that date.",
    });
  }
};

/**
 * GET /api/custody-match/download?key=…
 *
 * One-hour presigned GET. The key is checked against the two folders so this
 * cannot be pointed at another bucket path.
 */
export const downloadObject = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);

    const key = String(req.query.key || "").trim();
    const isDayObject = /^(FA-Transaction|Custodian-Transaction)\/\d{4}-\d{2}-\d{2}\//.test(key);
    if (!key || !(isDayObject || parseCustodyHistoryKey(key))) {
      return fail(res, 400, {
        code: "BAD_KEY",
        message: "key must be a custody-matching object key.",
      });
    }

    const url = await signedGetUrl(getBucket(catalystApp), key, 3600);
    if (!url) {
      return fail(res, 500, { message: "Could not create a download link." });
    }

    return res.status(200).json({ success: true, key, downloadUrl: url, expiresInSeconds: 3600 });
  } catch (error) {
    console.error("[CustodyMatchUpload] downloadObject:", error);
    return fail(res, 500, {
      message: error.message || "Could not create a download link.",
    });
  }
};

/**
 * GET /api/custody-match/health
 *
 * Confirms the bucket is reachable and that this app can write, read and delete
 * in it. Written because the storage layer was built before any live call had
 * been made against the real bucket — this is what closes that gap.
 */
export const health = async (req, res) => {
  const checks = {};
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);

    checks.bucketExists = await bucketExists(catalystApp, BUCKET_NAME);

    const bucket = getBucket(catalystApp);
    const probeKey = `FA-Transaction/1970-01-01/raw-${Date.now()}-healthcheck.csv`;

    await putBuffer(bucket, probeKey, Buffer.from("probe", "utf8"), "text/plain");
    checks.canWrite = true;

    const day = await listDay(catalystApp, "1970-01-01", { bucket });
    checks.canList = day.fa.raw.some((f) => f.key === probeKey);

    const removed = await deleteKeys(bucket, [probeKey]);
    checks.canDelete = removed.deleted.length === 1;

    const ok = Object.values(checks).every(Boolean);
    return res.status(ok ? 200 : 500).json({
      success: ok,
      bucket: BUCKET_NAME,
      checks,
      message: ok
        ? `Bucket "${BUCKET_NAME}" is reachable and writable.`
        : "One or more storage checks failed — see checks.",
    });
  } catch (error) {
    console.error("[CustodyMatchUpload] health:", error);
    return fail(res, 500, {
      bucket: BUCKET_NAME,
      checks,
      message: error.message || "Storage health check failed.",
    });
  }
};
