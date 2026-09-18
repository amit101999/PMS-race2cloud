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
  PREDEFINED_SCHEMES,
} from "../../util/custodyMatch/constants.js";
import { PROFILE_TYPE } from "../../util/custodyMatch/fieldMap.js";
import { parseFaFile, validateFaHeader } from "../../util/custodyMatch/parseFA.js";
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
  deleteFaRawOfType,
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
import { supersedeRunsForDate } from "../../util/custodyMatch/runStore.js";

/** Row errors are for reading, not for scrolling. Send the first page and say how many more. */
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
const summariseIssues = (rowErrors = [], warnings = []) => ({
  rowErrors: rowErrors.slice(0, MAX_REPORTED_ROW_ERRORS),
  rowErrorCount: rowErrors.length,
  moreRowErrors: Math.max(rowErrors.length - MAX_REPORTED_ROW_ERRORS, 0),
  warnings: warnings.slice(0, MAX_REPORTED_WARNINGS),
  warningCount: warnings.length,
});

/**
 * The date a file covers, taken from the file itself.
 *
 * The screen may not carry a date picker, and asking the user to type the date
 * of a file they just exported invites a mismatch nobody notices. The modal
 * transaction date is the file's own answer; it is returned to the caller so the
 * screen can show which day it is about to process.
 */
const inferBusinessDate = (rows) => {
  const counts = new Map();
  for (const r of rows) {
    if (!r.tranDate) continue;
    counts.set(r.tranDate, (counts.get(r.tranDate) || 0) + 1);
  }
  let best = null;
  let bestCount = -1;
  for (const [date, n] of counts) {
    if (n > bestCount || (n === bestCount && date > best)) {
      best = date;
      bestCount = n;
    }
  }
  return best;
};

/**
 * The span of days a file actually describes, first to last.
 *
 * A custodian file is identified by this range, not by one date: a file that
 * covers three days has no single "its" date, and picking one (the most common,
 * say) makes two genuinely different files look alike — the second then lands in
 * the first one's slot and silently replaces it.
 */
const coveredRange = (rows, fallbackDate) => {
  let from = null;
  let to = null;
  for (const r of rows) {
    if (!r.tranDate) continue;
    if (!from || r.tranDate < from) from = r.tranDate;
    if (!to || r.tranDate > to) to = r.tranDate;
  }
  return { coveredFrom: from || fallbackDate, coveredTo: to || fallbackDate };
};

const describeRange = (from, to) => (from === to ? from : `${from} to ${to}`);

const resolveBusinessDate = (requested, rows) => {
  const given = String(requested || "").trim();
  if (given) {
    if (!DATE_PATTERN.test(given)) {
      return { error: `businessDate must be yyyy-mm-dd; received "${given}".` };
    }
    return { businessDate: given, inferred: false };
  }
  const inferred = inferBusinessDate(rows);
  if (!inferred) {
    return {
      error:
        "Could not work out which business date this file covers, and none was supplied.",
    };
  }
  return { businessDate: inferred, inferred: true };
};

const getUploadedFile = (req) => req.files?.file || null;

const isCsv = (name) => String(name || "").toLowerCase().endsWith(".csv");

/**
 * GET /api/custody-match/day/:date
 *
 * Everything stored for a business date, described from the object keys alone.
 * Backs the replace prompt, the scheme rows, and restoring the screen on reload.
 */
export const getDay = async (req, res) => {
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

    const day = await listDay(catalystApp, date);
    return res.status(200).json({ success: true, ...day });
  } catch (error) {
    console.error("[CustodyMatchUpload] getDay:", error);
    return fail(res, 500, {
      message: error.message || "Could not read the files for that date.",
    });
  }
};

/**
 * POST /api/custody-match/fa-file
 * form-data: file, [businessDate], [replace=true]
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
 * GET /api/custody-match/day/:date/dates
 *
 * Which dates this run could compare, and what each one has on either side.
 * Backs the date picker shown when Match is pressed.
 *
 * The custodian side is free — each key carries the day it covers. The FA side
 * means reading the staged file, which is why this is its own endpoint rather
 * than part of the day view: the cost is paid when somebody opens the dialog,
 * not on every screen load.
 */
export const listRunDates = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);

    const date = String(req.params.date || "").trim();
    if (!DATE_PATTERN.test(date)) {
      return fail(res, 400, { code: "BAD_DATE", message: "date must be yyyy-mm-dd." });
    }

    const day = await listDay(catalystApp, date);
    const byDate = new Map();
    const touch = (d) => {
      if (!byDate.has(d)) byDate.set(d, { date: d, faRows: 0, custodyRows: 0, custodyFiles: 0 });
      return byDate.get(d);
    };

    const profiles = await loadFieldMapProfiles(catalystApp);
    const bucket = getBucket(catalystApp);

    /*
     * Both sides are counted from the rows themselves, never from the range on a
     * custodian key. A range says only where a file starts and ends: a file whose
     * first and last rows are years apart covers two days, not the thousands in
     * between, and offering every day in the span buries the real ones.
     */
    if (day.fa.latestRaw) {
      const buffer = await getBuffer(bucket, day.fa.latestRaw.key);
      const parsed = await parseFaFile(buffer, { profile: profiles[PROFILE_TYPE.FA] });
      for (const row of parsed.rows) {
        if (row.tranDate) touch(row.tranDate).faRows += 1;
      }
    }

    for (const f of day.custody.files) {
      let buffer;
      try {
        buffer = await getBuffer(bucket, f.key);
      } catch {
        // Stratus keeps listing an object for around twenty seconds after it is
        // deleted, so a file the user has just removed can still turn up here.
        // It is gone; leave its dates out rather than fail the whole dialog.
        continue;
      }

      const parsed = await parseCustodyFile(buffer, {
        scheme: f.scheme,
        fileType: f.fileType,
        fileName: f.originalFileName,
        profiles,
      });

      const daysInThisFile = new Set();
      for (const row of parsed.rows) {
        if (!row.tranDate) continue;
        touch(row.tranDate).custodyRows += 1;
        daysInThisFile.add(row.tranDate);
      }
      // A file counts once per day it has rows on, however many rows that is.
      for (const d of daysInThisFile) touch(d).custodyFiles += 1;
    }

    const dates = [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date));

    return res.json({
      success: true,
      businessDate: date,
      dates,
      count: dates.length,
      hasFaFile: Boolean(day.fa.latestRaw),
    });
  } catch (err) {
    console.error("[CustodyMatchUpload] listRunDates:", err);
    return fail(res, 500, {
      code: "DATE_LIST_FAILED",
      message: `Could not work out which dates these files cover: ${err.message}`,
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

    const firstPass = await parseFaFile(file.data, { profile: faProfile, type: profileType });
    if (firstPass.fileError) {
      return fail(res, 400, { ...firstPass.fileError, fileName: file.name });
    }

    const resolved = resolveBusinessDate(req.body?.businessDate, firstPass.rows);
    if (resolved.error) {
      return fail(res, 400, { code: "BAD_DATE", message: resolved.error });
    }
    const { businessDate, inferred } = resolved;

    // Re-parse against the resolved date so off-date rows are warned about.
    const parsed = await parseFaFile(file.data, {
      businessDate,
      profile: faProfile,
      type: profileType,
    });
    if (!parsed.ok) {
      return fail(res, 400, {
        code: "VALIDATION_FAILED",
        message: `${parsed.rowErrors.length} row(s) in the transaction file could not be read. Nothing was uploaded.`,
        fileName: file.name,
        businessDate,
        ...summariseIssues(parsed.rowErrors, parsed.warnings),
        hint: "Fix the rows listed above in the source file and upload it again.",
      });
    }

    const day = await listDay(catalystApp, businessDate);
    const wantsReplace = String(req.body?.replace || "").toLowerCase() === "true";

    /*
     * Only a file of the SAME type conflicts. The regular and mutual fund
     * transaction files are both expected for a date, so the presence of one
     * must not read as the other already being there.
     */
    const existingOfType = day.fa.latestByType?.[faFileType] || null;

    if (existingOfType && !wantsReplace) {
      return fail(res, 409, {
        code: "FILE_ALREADY_EXISTS",
        message: `A ${FA_FILE_TYPE_LABEL[faFileType]} file is already stored for ${businessDate}.`,
        businessDate,
        fileType: faFileType,
        existing: [
          {
            key: existingOfType.key,
            fileName: existingOfType.originalFileName,
            uploadedAtMs: existingOfType.uploadedAtMs,
            sizeBytes: existingOfType.sizeBytes,
          },
        ],
        hint: "Re-send with replace=true to delete the stored file and use this one instead.",
      });
    }

    const bucket = getBucket(catalystApp);

    if (existingOfType) {
      // Clears this type's raw file plus the final and difference files — a
      // stale final must never be paired with a new raw file.
      await deleteFaRawOfType(catalystApp, businessDate, faFileType, { bucket });
    }

    // The span of transaction dates the rows cover, stored on the key so the
    // screen can name the file by what is in it without reopening it.
    const { coveredFrom, coveredTo } = coveredRange(parsed.rows, businessDate);

    const uploadedAtMs = Date.now();
    const key = buildFaRawKey({
      businessDate,
      fileType: faFileType,
      coveredFrom,
      coveredTo,
      originalFileName: file.name,
      uploadedAtMs,
    });
    await putBuffer(bucket, key, file.data, "text/csv");

    // Any earlier clean result was reached against files that are no longer here.
    const superseded = await supersedeRunsForDate(catalystApp, businessDate);

    return res.status(200).json({
      success: true,
      message: `The ${FA_FILE_TYPE_LABEL[faFileType]} file is staged for ${businessDate}. It has not been imported.`,
      businessDate,
      businessDateInferred: inferred,
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
 * form-data: file, scheme, fileType (RESIDENTIAL|NDPMS|MUTUAL_FUND), [businessDate], [replace=true]
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

    const requestedDate = String(req.body?.businessDate || "").trim();
    if (requestedDate && !DATE_PATTERN.test(requestedDate)) {
      return fail(res, 400, {
        code: "BAD_DATE",
        message: `businessDate must be yyyy-mm-dd; received "${requestedDate}".`,
      });
    }

    const parsed = await parseCustodyFile(file.data, {
      scheme,
      fileType,
      fileName: file.name,
      // Settles an NDPMS ContractNo whose first 8 digits read as a valid date
      // in both orders; the run's own date is the only outside fact we have.
      businessDate: requestedDate || null,
      profiles,
    });
    if (parsed.fileError) {
      return fail(res, 400, { ...parsed.fileError, scheme, fileType });
    }

    const resolved = resolveBusinessDate(requestedDate, parsed.rows);
    if (resolved.error) {
      return fail(res, 400, { code: "BAD_DATE", message: resolved.error });
    }
    const { businessDate, inferred } = resolved;

    if (!parsed.ok) {
      return fail(res, 400, {
        code: "VALIDATION_FAILED",
        message: `${parsed.rowErrors.length} row(s) in ${file.name} could not be read. Nothing was uploaded.`,
        fileName: file.name,
        scheme,
        fileType,
        businessDate,
        detectedFormat: parsed.format,
        ...summariseIssues(parsed.rowErrors, parsed.warnings),
        hint: "Ask the custodian to correct the rows listed above, then upload the file again.",
      });
    }

    /*
     * Two different things, and conflating them is what limited a run to one day.
     *
     *   businessDate            — the run this file belongs to. Decides the
     *                             folder, so everything for a run stays together.
     *   coveredFrom..coveredTo  — the days the rows inside describe, read from
     *                             the file. Decides the slot, so a scheme can
     *                             hold as many files as the run needs.
     */
    const { coveredFrom, coveredTo } = coveredRange(parsed.rows, businessDate);
    const range = describeRange(coveredFrom, coveredTo);

    const day = await listDay(catalystApp, businessDate);

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

    const sameSlot = day.custody.files.filter(
      (f) =>
        f.scheme === storedScheme &&
        f.fileType === fileType &&
        f.coveredFrom === coveredFrom &&
        f.coveredTo === coveredTo,
    );
    const wantsReplace = String(req.body?.replace || "").toLowerCase() === "true";

    if (sameSlot.length > 0 && !wantsReplace) {
      return fail(res, 409, {
        code: "FILE_ALREADY_EXISTS",
        message: `A ${fileType} file for ${scheme} covering ${range} is already stored.`,
        businessDate,
        coveredFrom,
        coveredTo,
        scheme,
        fileType,
        existing: sameSlot.map((f) => ({
          key: f.key,
          scheme: f.scheme,
          fileType: f.fileType,
          coveredFrom: f.coveredFrom,
          coveredTo: f.coveredTo,
          uploadedAtMs: f.uploadedAtMs,
          sizeBytes: f.sizeBytes,
        })),
        allCustodyFiles: day.custody.files.length,
        hint: `Re-send with replace=true. Only this file — ${scheme} ${fileType} for ${range} — is replaced; the other schemes and dates in this run are left alone.`,
      });
    }

    const bucket = getBucket(catalystApp);

    /*
     * Replace only this slot. The old rule cleared the whole custodian date,
     * which was safe when a date held one file per scheme — with several dates
     * in a run it would silently delete the other days' files.
     */
    if (sameSlot.length > 0) {
      await deleteKeys(bucket, sameSlot.map((f) => f.key));
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
    await putBuffer(bucket, key, file.data, "text/csv");

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

    /*
     * A file whose range straddles another file's is stored — the user asked for
     * anything but an exact repeat to be added — but the days they share would
     * then be counted twice in the comparison, so say so.
     */
    const replacedKeys = new Set(sameSlot.map((f) => f.key));
    const overlapping = day.custody.files.filter(
      (f) =>
        !replacedKeys.has(f.key) &&
        f.scheme === storedScheme &&
        f.fileType === fileType &&
        f.coveredFrom <= coveredTo &&
        f.coveredTo >= coveredFrom,
    );
    const overlapWarning =
      overlapping.length > 0
        ? `This file shares dates with ${overlapping.length} other ${fileType} file(s) already stored for ${scheme}. Rows on the shared dates will be counted from both.`
        : "";

    const superseded = await supersedeRunsForDate(catalystApp, businessDate);
    const after = await listDay(catalystApp, businessDate);

    return res.status(200).json({
      success: true,
      message: `File stored for ${scheme} covering ${range}.`,
      businessDate,
      businessDateInferred: inferred,
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
      overlapWarning,
      replacedSlot: sameSlot.length > 0,
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

    const bucket = getBucket(catalystApp);
    const result = await deleteKeys(bucket, [key]);
    if (result.failed.length > 0) {
      return fail(res, 500, {
        code: "DELETE_FAILED",
        message: `Could not remove ${key.split("/").pop()}.`,
        failed: result.failed,
      });
    }

    const superseded = await supersedeRunsForDate(catalystApp, parsed.businessDate);
    const after = await listDay(catalystApp, parsed.businessDate);

    /*
     * The object is gone the moment deleteObject returns, but Stratus's listing
     * index catches up around twenty seconds later — so this listing almost
     * always still contains the key we just removed. Reporting it back would
     * put the row straight back on screen, which is what made a removed file
     * look as though it needed a page refresh to disappear.
     */
    const stillThere = (f) => f.key !== key;

    return res.json({
      success: true,
      message: `Removed ${parsed.originalFileName || `${parsed.scheme} ${parsed.fileType}`}.`,
      businessDate: parsed.businessDate,
      key,
      supersededRuns: superseded.runIds,
      files: after.custody.files.filter(stillThere),
      faFiles: after.fa.raw.filter(stillThere),
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
 * Clears a side wholesale so the user can restart a day.
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
