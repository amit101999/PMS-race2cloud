/**
 * OPI/OPO tab of Transaction Upload — shares moved in (OPI) or out (OPO) from
 * another system, for example when a client's account comes over from another
 * PMS.
 *
 *   POST /api/custody-match/opi-opo/file             read one file and keep it until Commit
 *   GET  /api/custody-match/opi-opo/:fileId          the file's summary and Commit progress
 *   POST /api/custody-match/opi-opo/:fileId/commit   add its rows to Transaction, rebuild holdings
 *   POST /api/custody-match/opi-opo/:fileId/retry    continue a Commit that stopped
 *
 * Unlike the Equity and Mutual Funds tabs (EquityReload.js), which work one
 * trade date at a time, this tab works one file at a time: a file may hold many
 * trade and settlement dates, and one Commit adds all of its rows. Rows carry no
 * Transaction ID. Upload only reads the file and saves it, refusing a file
 * already uploaded (same content), with no Transaction look-up, so it stays
 * quick. Commit is refused when any of the file's rows is already in our books
 * (same account, ISIN, OPI/OPO, trade date, settlement date, quantity and rate:
 * `fingerprint`, across both sub-tabs), so the same entries can never be booked
 * twice. A Commit only adds rows; it never removes any.
 *
 * Two sub-tabs, told apart by `assetKind` (SUB_TAB below):
 *   EQUITY       the Equity layout; mutual fund rows (INF on DIR) are refused
 *   MUTUAL_FUND  the same plus TDS and Stamp Duty; every row is booked as a
 *                mutual fund, and an OPI's cost is QTY × RATE + Stamp Duty
 *
 * fileId is the upload time in ms, which is also part of the raw file's key
 * (FA/<upload date>/raw-<OPI_OPO|OPI_OPO_MF>-<first>-<last>-<fileId>-<name>).
 * The rest lives in Stratus temporary-files opi-opo/<fileId>/:
 *   file.json      summary: sub-tab, rows, dates, affected clients and pairs, fingerprints, fileHash, committed
 *   rows.csv       the rows as the 26 template columns (+ TDS, Stamp Duty for mutual funds), which the import reads
 *   progress.json  the Commit's step and counts
 * The Commit's lock is the Jobs row RELOAD_OPI_<fileId>; its holdings jobs are
 * RELOAD_OPI_<fileId>_H_<n>. OPI / OPO move shares, not cash, so the cash
 * passbook is never rebuilt. Once progress.json exists the file can no longer
 * be removed from its card (deleteUploadedFile in CustodyMatchUpload.js).
 */

import { createHash } from "crypto";
import { FA_FILE_TYPE, FILE_SIDE, OPI_OPO_FILE_TYPES } from "../../util/custodyMatch/constants.js";
import { PROFILE_TYPE } from "../../util/custodyMatch/fieldMap.js";
import {
  faRowTemplateCells,
  hasDeductionColumns,
  parseFaFile,
  validateFaHeader,
} from "../../util/custodyMatch/parseFA.js";
import {
  buildFaRawKey,
  deleteKeys,
  getBucket,
  getBuffer,
  getJson,
  loadFieldMapProfiles,
  objectExists,
  parseFaRawKey,
  putBuffer,
  putJson,
  putText,
} from "../../util/custodyMatch/storage.js";
import { deleteFileRows, escapeZcql as esc, listFiles, recordFile } from "../../util/custodyMatch/runStore.js";
import {
  affectedOf,
  bookedTradeTab,
  fileProblems,
  fingerprint,
  tradeFromBookedRow,
  tradeFromCells,
  tradesToOpiOpoCsv,
  withMutualFundValues,
} from "../../util/custodyMatch/equityReload.js";
import { getUploadedFile, isCsv, readUploadedOn, summariseIssues } from "./CustodyMatchUpload.js";
import { busyElsewhere, readJobStatuses, submitRebuildJobs, waitForBulkWrite } from "./EquityReload.js";
import {
  BUCKET_NAME as TXN_BUCKET,
  TABLE_NAME as TXN_TABLE,
  ensureJobsRow,
  normalizeCsvForBulkUpload,
  updateJobsStatus,
} from "../uploader/TempTransactionUpload.js";
import { insertNotification } from "../../util/notifications.js";

const FOLDER = "opi-opo";
const LOCK_PREFIX = "RELOAD_OPI_";

/*
 * The two sub-tabs. assetKind is the Asset_Kind of their Commit history rows;
 * a file saved before the sub-tabs existed has no subTab and is Equity's.
 */
const SUB_TAB = {
  EQUITY: {
    key: "EQUITY",
    label: "Equity",
    profileType: PROFILE_TYPE.FA_OPI_OPO,
    fileType: FA_FILE_TYPE.OPI_OPO,
    assetKind: "OPI_OPO",
    mutualFund: false,
  },
  MUTUAL_FUND: {
    key: "MUTUAL_FUND",
    label: "Mutual Funds",
    profileType: PROFILE_TYPE.FA_OPI_OPO_MF,
    fileType: FA_FILE_TYPE.OPI_OPO_MF,
    assetKind: "OPI_OPO_MF",
    mutualFund: true,
  },
};
const subTabOf = (info) => SUB_TAB[info.subTab] || SUB_TAB.EQUITY;
const HISTORY_TABLE = "Transaction_Commit_History";
const FILE_ID_PATTERN = /^\d{10,16}$/;
const ZCQL_PAGE = 300;
/* ZCQL removes at most 300 rows per DELETE; ROWID IN lists stay below that. */
const DELETE_BATCH = 200;
const MAX_INSERT_ATTEMPTS = 3;
const BULK_POLL_MS = 15_000;
/* The holdings jobs report COMPLETED only on success; one still RUNNING after this is taken as failed. */
const REBUILD_GIVE_UP_MS = 2 * 60 * 60 * 1000;
/* A RUNNING Commit not saved for this long has lost its server (restart); Try again may continue it. */
const STALLED_AFTER_MS = 15 * 60 * 1000;
const JOB_CONFIG = { number_of_retries: 5, retry_interval: 60 * 1000 };

const STEP = { INSERT: "INSERT", REBUILD: "REBUILD", WAITING: "WAITING", DONE: "DONE" };
const STATUS = { RUNNING: "RUNNING", FAILED: "FAILED", COMPLETED: "COMPLETED" };

/* Commits running in this server, so a double click cannot start two loops for one file. */
const activeCommits = new Set();

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const fail = (res, status, body) => res.status(status).json({ success: false, ...body });
const noApp = (res) => fail(res, 500, { code: "CATALYST_NOT_INITIALIZED", message: "Catalyst not initialized" });
const unwrap = (rows, table) => (rows || []).map((r) => r[table] || r);
const stateKey = (fileId, name) => `${FOLDER}/${fileId}/${name}`;
const lockName = (fileId) => `${LOCK_PREFIX}${fileId}`;
/* 2026-10-28 → 28-10-2026, the way the screen shows dates. */
const displayDate = (date) => String(date).split("-").reverse().join("-");

const readJsonOr = async (bucket, key, fallback) => {
  try {
    return await getJson(bucket, key);
  } catch {
    return fallback;
  }
};

/* ───────────────────────── rows already in our books ────────────────────── */

/** fingerprint -> how many times it occurs. */
const countPrints = (prints) => {
  const counts = new Map();
  for (const p of prints) counts.set(p, (counts.get(p) || 0) + 1);
  return counts;
};

/* How many trade dates one query asks for; keeps the IN list short. */
const DATES_PER_QUERY = 50;
/* Tran_Type is stored as the file wrote it, so both spellings are asked for; bookedTradeTab decides. */
const OPI_OPO_TYPES_SQL = "'OPI', 'OPO', 'opi', 'opo'";

/**
 * The OPI/OPO rows already in our books on the given trade dates, as
 * { rowId, print }. Only OPI/OPO rows are read, many dates per query, paged by
 * ROWID (ZCQL OFFSET repeats rows).
 */
const bookedOpiOpoRows = async (zcql, dates) => {
  const found = [];
  for (let i = 0; i < dates.length; i += DATES_PER_QUERY) {
    const dateList = dates.slice(i, i + DATES_PER_QUERY).map((d) => `'${esc(d)}'`).join(", ");
    let lastRowId = "0";
    for (;;) {
      const batch = unwrap(
        await zcql.executeZCQLQuery(
          `SELECT ROWID, WS_Account_code, ISIN, EXCHG, Security_Type, Tran_Type, TRANDATE, SETDATE, QTY, RATE FROM ${TXN_TABLE} WHERE TRANDATE IN (${dateList}) AND Tran_Type IN (${OPI_OPO_TYPES_SQL}) AND ROWID > ${lastRowId} ORDER BY ROWID ASC LIMIT ${ZCQL_PAGE}`,
        ),
        TXN_TABLE,
      );
      for (const row of batch) {
        if (bookedTradeTab(row) !== "OPI_OPO") continue;
        found.push({ rowId: String(row.ROWID), print: fingerprint(tradeFromBookedRow(row)) });
      }
      if (batch.length < ZCQL_PAGE) break;
      lastRowId = String(batch[batch.length - 1].ROWID);
    }
  }
  return found;
};

/* ──────────────────────────── same file uploaded again ──────────────────── */

const sha256 = (buffer) => createHash("sha256").update(buffer).digest("hex");

/**
 * The OPI/OPO files already uploaded (on any upload date) whose content is
 * exactly this file's, as { row, fileId, fileName, uploadDate, committed }.
 * A file uploaded before fileHash was saved is hashed from its stored copy.
 */
const sameContentFiles = async (catalystApp, bucket, fileHash) => {
  const rows = (await listFiles(catalystApp)).filter((r) => OPI_OPO_FILE_TYPES.includes(r.fileType));
  const found = [];
  for (const row of rows) {
    const fileId = String(parseFaRawKey(row.fileKey)?.uploadedAtMs || "");
    if (!fileId) continue;
    const info = await readJsonOr(bucket, stateKey(fileId, "file.json"), null);
    if (!info) continue;
    const storedHash = info.fileHash || sha256(await getBuffer(bucket, row.fileKey));
    if (storedHash !== fileHash) continue;
    // Once a Commit has started the file can no longer be removed, so it counts as committed.
    const committed = Boolean(info.committed) || (await objectExists(bucket, stateKey(fileId, "progress.json")));
    found.push({ row, fileId, fileName: info.fileName, uploadDate: info.uploadDate, committed });
  }
  return found;
};

/** Removes uncommitted copies; the file row goes first, so Commit refuses them from then on. */
const removeCopies = async (catalystApp, bucket, copies) => {
  await deleteFileRows(catalystApp, copies.map((c) => c.row));
  const keys = copies.flatMap((c) => [c.row.fileKey, stateKey(c.fileId, "rows.csv"), stateKey(c.fileId, "file.json")]);
  const result = await deleteKeys(bucket, keys);
  if (result.failed.length > 0) {
    // The rows are gone, so the screen no longer lists these copies; only storage is left over.
    console.warn("[OpiOpoUpload] could not delete replaced copies:", result.failed);
  }
};

/* ─────────────────────────────────── upload ─────────────────────────────── */

/** Mutual Funds sub-tab: TDS, Stamp Duty and TRANEXPENSE, when filled, must be numbers of zero or more. */
const deductionProblems = (trades) => {
  const problems = [];
  for (const t of trades) {
    for (const [field, label] of [
      ["tds", "TDS"],
      ["stampDuty", "Stamp Duty"],
      ["stt", "TRANEXPENSE"],
    ]) {
      if (t[field] === "" || t[field] === undefined) continue;
      const value = Number(t[field]);
      if (!Number.isFinite(value) || value < 0) {
        problems.push({
          row: t.rowNumber,
          code: "BAD_NUMBER",
          message: `${label} must be a number of zero or more; found "${t[field]}".`,
          column: label,
          value: t[field],
        });
      }
    }
  }
  return problems;
};

/**
 * POST /api/custody-match/opi-opo/file   form-data: file, uploadDate, assetKind (EQUITY | MUTUAL_FUND), replace
 *
 * Reads the file and keeps it for Commit. Nothing is booked here and the
 * Transaction table is not read, so this is quick. Every row must be OPI or OPO
 * with readable dates; there is no file date, because one file may hold many
 * trade and settlement dates. Rows already in our books are found at Commit.
 *
 * Exactly the same file as an earlier upload is refused: with DUPLICATE_FILE
 * while that copy is not committed (sent again with replace=true, the copy is
 * removed and this one kept), and with ALREADY_COMMITTED once it is.
 */
export const uploadOpiOpoFile = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);

    const file = getUploadedFile(req);
    if (!file) {
      return fail(res, 400, { code: "NO_FILE", message: "CSV file is required. Send it as form-data with key 'file'." });
    }
    if (!isCsv(file.name)) return fail(res, 400, { code: "NOT_CSV", message: "Only CSV files are allowed." });
    const uploadDay = readUploadedOn(req.body?.uploadDate);
    if (uploadDay.error) return fail(res, 400, { code: "BAD_UPLOAD_DATE", message: uploadDay.error });
    const { uploadedOn } = uploadDay;
    const subTab = SUB_TAB[String(req.body?.assetKind || SUB_TAB.EQUITY.key).trim().toUpperCase()];
    if (!subTab) return fail(res, 400, { code: "BAD_KIND", message: "assetKind must be EQUITY or MUTUAL_FUND." });

    const type = subTab.profileType;
    const profiles = await loadFieldMapProfiles(catalystApp);
    const profile = profiles[type];
    const headerError = validateFaHeader(file.data, { profile, type });
    if (headerError) return fail(res, 400, { ...headerError, fileName: file.name });
    /*
     * Each sub-tab takes only its own layout: the mutual fund file is the one with
     * TDS and Stamp Duty columns, read through the Mutual Funds sub-tab's mapping
     * (its rows may leave both blank). The mapping window detects it the same way.
     */
    const mfType = SUB_TAB.MUTUAL_FUND.profileType;
    const mutualFundLayout = hasDeductionColumns(file.data, { type: mfType, profile: profiles[mfType] });
    if (subTab.mutualFund !== mutualFundLayout) {
      return fail(res, 400, {
        code: "FA_FILE_TYPE_MISMATCH",
        message: subTab.mutualFund
          ? "This file has no TDS / Stamp Duty columns, so it is an Equity OPI/OPO file. Upload it in the Equity sub-tab."
          : "This file has TDS / Stamp Duty columns, so it is a Mutual Funds OPI/OPO file. Upload it in the Mutual Funds sub-tab.",
        fileName: file.name,
      });
    }

    // No businessDate: the rows may be dated on any day.
    const parsed = await parseFaFile(file.data, { profile, type });
    if (parsed.fileError) return fail(res, 400, { ...parsed.fileError, fileName: file.name });

    // The 26 template cells; there is no Transaction ID (column 27 stays blank).
    const trades = parsed.rows.map((row) => {
      const trade = tradeFromCells([...faRowTemplateCells(row), ""], { rowNumber: row.rowNumber });
      return subTab.mutualFund ? withMutualFundValues(trade, [row.mapped?.tds, row.mapped?.stampDuty]) : trade;
    });
    const rowErrors = [
      ...parsed.rowErrors.map((e) => ({ ...e, fileName: file.name })),
      ...fileProblems(trades, { opiOpo: true, mutualFund: subTab.mutualFund }).map((p) => ({ ...p, fileName: file.name })),
      ...(subTab.mutualFund ? deductionProblems(trades) : []).map((p) => ({ ...p, fileName: file.name })),
    ].sort((a, b) => a.row - b.row);
    if (rowErrors.length > 0) {
      return fail(res, 400, {
        code: "VALIDATION_FAILED",
        message: `${rowErrors.length} row(s) in the OPI/OPO file cannot be used. Nothing was uploaded.`,
        fileName: file.name,
        ...summariseIssues(rowErrors, parsed.warnings),
        hint: "Every row must be an OPI or OPO entry with readable dates.",
      });
    }
    if (trades.length === 0) {
      return fail(res, 400, { code: "EMPTY_FILE", message: "The file has no rows.", fileName: file.name });
    }

    const bucket = getBucket(catalystApp);

    // The same file uploaded again would give a second Commit block for the same entries.
    const fileHash = sha256(file.data);
    const copies = await sameContentFiles(catalystApp, bucket, fileHash);
    const committedCopy = copies.find((c) => c.committed);
    if (committedCopy) {
      return fail(res, 409, {
        code: "ALREADY_COMMITTED",
        message: `This file was already uploaded as ${committedCopy.fileName} on ${displayDate(committedCopy.uploadDate)} and committed, so its entries are already in our books. It cannot be uploaded again.`,
        fileName: file.name,
      });
    }
    if (copies.length > 0) {
      if (String(req.body?.replace || "").toLowerCase() !== "true") {
        const first = copies[0];
        return fail(res, 409, {
          code: "DUPLICATE_FILE",
          message: `This file is already uploaded as ${first.fileName} on ${displayDate(first.uploadDate)} and is not committed yet. Do you want to replace it?`,
          fileName: file.name,
          existing: copies.map((c) => ({ fileId: c.fileId, fileName: c.fileName, uploadDate: c.uploadDate })),
        });
      }
      await removeCopies(catalystApp, bucket, copies);
    }

    const dates = [...new Set(trades.map((t) => t.tranDate))].sort();

    const uploadedAtMs = Date.now();
    const fileId = String(uploadedAtMs);
    const key = buildFaRawKey({
      businessDate: uploadedOn,
      fileType: subTab.fileType,
      coveredFrom: dates[0],
      coveredTo: dates[dates.length - 1],
      originalFileName: file.name,
      uploadedAtMs,
    });
    const affected = affectedOf([], trades);
    const info = {
      fileId,
      subTab: subTab.key,
      fileKey: key,
      fileName: file.name,
      uploadDate: uploadedOn,
      uploadedAtMs,
      rowCount: trades.length,
      opiCount: trades.filter((t) => t.tranType === "OPI").length,
      opoCount: trades.filter((t) => t.tranType === "OPO").length,
      dates,
      affected,
      fingerprints: trades.map(fingerprint),
      fileHash,
      committed: false,
    };

    // The state first and the file row last: a card only lists a file once its row exists.
    await putBuffer(bucket, key, file.data, "text/csv");
    const rowsCsv = tradesToOpiOpoCsv(trades, { withDeductions: subTab.mutualFund });
    await putText(bucket, stateKey(fileId, "rows.csv"), rowsCsv, "text/csv");
    await putJson(bucket, stateKey(fileId, "file.json"), info);
    try {
      await recordFile(catalystApp, {
        fileKey: key,
        side: FILE_SIDE.FA,
        fileType: subTab.fileType,
        forDate: uploadedOn,
        uploadedOn,
        originalFileName: file.name,
      });
    } catch (err) {
      await deleteKeys(bucket, [key, stateKey(fileId, "rows.csv"), stateKey(fileId, "file.json")]);
      throw new Error(`The file could not be recorded, so it was not kept: ${err.message}`);
    }

    return res.status(200).json({
      success: true,
      stored: true,
      fileId,
      key,
      fileName: file.name,
      file: fileView(info),
      ...summariseIssues([], parsed.warnings),
      message: `${file.name} is ready to commit: ${trades.length} entries from ${displayDate(dates[0])} to ${displayDate(dates[dates.length - 1])}. Nothing is booked until Commit.`,
    });
  } catch (error) {
    console.error("[OpiOpoUpload] uploadOpiOpoFile:", error);
    return fail(res, 500, { code: error.code, message: error.message || "Could not keep the OPI/OPO file." });
  }
};

/* ─────────────────────────────── progress ───────────────────────────────── */

/** What the screen shows about a file (fingerprints stay on the server). */
const fileView = (info) => ({
  fileId: info.fileId,
  fileName: info.fileName,
  uploadDate: info.uploadDate,
  rowCount: info.rowCount,
  opiCount: info.opiCount,
  opoCount: info.opoCount,
  firstDate: info.dates[0],
  lastDate: info.dates[info.dates.length - 1],
  dateCount: info.dates.length,
  clients: info.affected.clients.length,
  subTab: subTabOf(info).key,
  committed: Boolean(info.committed),
});

const canRetry = (p) =>
  Boolean(p) &&
  (p.status === STATUS.FAILED ||
    (p.status === STATUS.RUNNING && p.step !== STEP.WAITING && Date.now() - (p.updatedAtMs || 0) > STALLED_AFTER_MS));

const progressView = (p) =>
  p && {
    status: p.status,
    step: p.step,
    startedAtMs: p.startedAtMs,
    finishedAtMs: p.finishedAtMs || null,
    expectedCount: p.expectedCount,
    inserted: p.insert?.inserted ?? null,
    rebuild: p.rebuild ? { clients: p.rebuild.clients, total: p.rebuild.jobs.length, done: p.rebuild.done || 0 } : null,
    error: p.error || "",
    canRetry: canRetry(p),
  };

const saveProgress = async (bucket, progress) => {
  progress.updatedAtMs = Date.now();
  await putJson(bucket, stateKey(progress.fileId, "progress.json"), progress);
};

/* The commit itself is what matters, so a history row that cannot be updated only logs. */
const updateCommitHistory = (catalystApp, progress, patch) =>
  catalystApp
    .datastore()
    .table(HISTORY_TABLE)
    .updateRow({ ROWID: progress.historyRowId, ...patch })
    .catch((err) => console.warn("[OpiOpoUpload] commit history update failed:", err.message));

const notify = (catalystApp, progress, status, message) =>
  insertNotification(catalystApp.zcql(), {
    jobName: `${lockName(progress.fileId)}_${progress.startedAtMs}`,
    title: `OPI/OPO commit — ${progress.fileName}`,
    status,
    message,
  }).catch((err) => console.warn("[OpiOpoUpload] notification failed:", err.message));

/**
 * While the holdings jobs run, the progress is brought up to date whenever it
 * is read: finished once every job row says COMPLETED, failed once it has
 * waited REBUILD_GIVE_UP_MS.
 */
const refreshWaiting = async (catalystApp, bucket, progress) => {
  if (!progress || progress.step !== STEP.WAITING || progress.status !== STATUS.RUNNING) return progress;
  const names = progress.rebuild.jobs.map((j) => j.name);
  const statuses = await readJobStatuses(catalystApp.zcql(), names);
  const done = names.filter((n) => statuses.get(n) === "COMPLETED").length;
  progress.rebuild.done = done;

  if (done === names.length) {
    progress.status = STATUS.COMPLETED;
    progress.step = STEP.DONE;
    progress.finishedAtMs = Date.now();
    await saveProgress(bucket, progress);
    const info = await getJson(bucket, stateKey(progress.fileId, "file.json"));
    await putJson(bucket, stateKey(progress.fileId, "file.json"), { ...info, committed: true });
    await updateJobsStatus(catalystApp.zcql(), lockName(progress.fileId), STATUS.COMPLETED);
    await updateCommitHistory(catalystApp, progress, { status: STATUS.COMPLETED, finishedAtMs: progress.finishedAtMs });
    await notify(
      catalystApp,
      progress,
      "completed",
      `${progress.insert.inserted} entries committed. Holdings updated for ${progress.rebuild.clients} client(s).`,
    );
    return progress;
  }

  if (Date.now() - progress.rebuild.queuedAtMs > REBUILD_GIVE_UP_MS) {
    progress.status = STATUS.FAILED;
    progress.error = `${names.length - done} of ${names.length} holdings rebuilds did not finish. Press Try again to run them again.`;
    await saveProgress(bucket, progress);
    await updateJobsStatus(catalystApp.zcql(), lockName(progress.fileId), STATUS.FAILED);
    await updateCommitHistory(catalystApp, progress, { status: STATUS.FAILED });
    await notify(catalystApp, progress, "failed", "Holdings update did not finish. Press Try again.");
    return progress;
  }

  await saveProgress(bucket, progress);
  return progress;
};

/** The file's summary, or a refusal already sent. */
const readFileOr404 = async (req, res, bucket) => {
  const fileId = String(req.params.fileId || "").trim();
  if (!FILE_ID_PATTERN.test(fileId)) {
    return { refused: fail(res, 400, { code: "BAD_FILE_ID", message: "fileId must be the file's upload time in ms." }) };
  }
  const info = await readJsonOr(bucket, stateKey(fileId, "file.json"), null);
  if (!info) return { refused: fail(res, 404, { code: "FILE_NOT_FOUND", message: "This OPI/OPO file is not known." }) };
  return { fileId, info };
};

/** GET /api/custody-match/opi-opo/:fileId */
export const getOpiOpoFile = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);
    const bucket = getBucket(catalystApp);
    const found = await readFileOr404(req, res, bucket);
    if (found.refused) return found.refused;
    const progress = await refreshWaiting(
      catalystApp,
      bucket,
      await readJsonOr(bucket, stateKey(found.fileId, "progress.json"), null),
    );
    return res.json({ success: true, file: fileView(found.info), progress: progressView(progress) });
  } catch (error) {
    console.error("[OpiOpoUpload] getOpiOpoFile:", error);
    return fail(res, 500, { message: error.message || "Could not read the OPI/OPO file." });
  }
};

/* ──────────────────────────────────── commit ────────────────────────────── */

/**
 * The ROWIDs of this file's rows already in Transaction: OPI/OPO rows on its
 * dates whose fingerprint is one of the file's, at most as many times as the
 * file has it. Upload and Commit refuse a file whose rows were already booked,
 * so every match comes from this file's own load.
 */
const loadedRowIds = async (zcql, info) => {
  const wanted = countPrints(info.fingerprints);
  const ids = [];
  for (const row of await bookedOpiOpoRows(zcql, info.dates)) {
    const left = wanted.get(row.print) || 0;
    if (left === 0) continue;
    wanted.set(row.print, left - 1);
    ids.push(row.rowId);
  }
  return ids;
};

const deleteRows = async (zcql, ids) => {
  for (let i = 0; i < ids.length; i += DELETE_BATCH) {
    await zcql.executeZCQLQuery(`DELETE FROM ${TXN_TABLE} WHERE ROWID IN (${ids.slice(i, i + DELETE_BATCH).join(", ")})`);
  }
};

/** POST /api/custody-match/opi-opo/:fileId/commit   body: { startedBy } */
export const commitOpiOpoFile = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);
    const bucket = getBucket(catalystApp);
    const found = await readFileOr404(req, res, bucket);
    if (found.refused) return found.refused;
    const { fileId, info } = found;
    const zcql = catalystApp.zcql();

    const progress = await refreshWaiting(catalystApp, bucket, await readJsonOr(bucket, stateKey(fileId, "progress.json"), null));
    if (progress?.status === STATUS.COMPLETED || info.committed) {
      return fail(res, 409, { code: "ALREADY_COMMITTED", message: `${info.fileName} is already committed. A file is committed only once.` });
    }
    if (progress) {
      return fail(res, 409, {
        code: "COMMIT_STARTED",
        message:
          progress.status === STATUS.FAILED
            ? `The Commit of ${info.fileName} did not finish. Press Try again.`
            : `${info.fileName} is being committed.`,
      });
    }

    const stillListed = (await listFiles(catalystApp, { forDate: info.uploadDate })).some((r) => r.fileKey === info.fileKey);
    if (!stillListed) {
      return fail(res, 409, { code: "FILE_REMOVED", message: `${info.fileName} was removed, so there is nothing to commit.` });
    }

    const busy = await busyElsewhere(catalystApp, lockName(fileId));
    if (busy) return fail(res, 409, { code: "IMPORT_STILL_PROCESSING", message: busy });

    // The same entries may already be booked (an earlier file, or opening data). Booking them
    // again would count them twice, and the count check below relies on there being none.
    const alreadyBooked = (await loadedRowIds(zcql, info)).length;
    if (alreadyBooked > 0) {
      return fail(res, 409, {
        code: "ALREADY_BOOKED",
        message: `${alreadyBooked} of the ${info.rowCount} entries in ${info.fileName} are already in our books (same account, ISIN, OPI/OPO, dates, quantity and rate), so it cannot be committed. Remove the file, or upload one without those entries.`,
      });
    }

    const startedAtMs = Date.now();
    // Before the lock: without this row the commit does not start, and nothing has changed.
    const historyRow = await catalystApp.datastore().table(HISTORY_TABLE).insertRow({
      businessDate: info.dates[0],
      commitType: "FIRST",
      status: STATUS.RUNNING,
      runId: `opi-${fileId}`,
      tradesBefore: 0,
      tradesAfter: info.rowCount,
      clients: info.affected.clients.length,
      fileNames: info.fileName,
      startedAtMs,
      Asset_Kind: subTabOf(info).assetKind,
    });
    const fresh = {
      fileId,
      fileName: info.fileName,
      historyRowId: historyRow.ROWID,
      status: STATUS.RUNNING,
      step: STEP.INSERT,
      startedBy: String(req.body?.startedBy || req.headers?.["x-user"] || ""),
      startedAtMs,
      expectedCount: info.rowCount,
      insert: { attempts: 0 },
      rebuild: null,
      error: "",
    };
    await ensureJobsRow(zcql, lockName(fileId), STATUS.RUNNING);
    await saveProgress(bucket, fresh);
    await notify(catalystApp, fresh, "in_progress", `Adding ${info.rowCount} entries.`);

    void runCommit(catalystApp, fileId);
    return res.status(202).json({ success: true, fileId, progress: progressView(fresh), message: "Commit started." });
  } catch (error) {
    console.error("[OpiOpoUpload] commitOpiOpoFile:", error);
    return fail(res, 500, { code: error.code, message: error.message || "Could not start the Commit." });
  }
};

/** POST /api/custody-match/opi-opo/:fileId/retry — continue from the saved step. */
export const retryOpiOpoFile = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);
    const bucket = getBucket(catalystApp);
    const found = await readFileOr404(req, res, bucket);
    if (found.refused) return found.refused;
    const { fileId } = found;

    const progress = await refreshWaiting(catalystApp, bucket, await readJsonOr(bucket, stateKey(fileId, "progress.json"), null));
    if (!canRetry(progress) || activeCommits.has(fileId)) {
      return fail(res, 409, { code: "NOT_RETRYABLE", message: "This Commit is not stopped, so there is nothing to try again." });
    }

    progress.status = STATUS.RUNNING;
    progress.error = "";
    await ensureJobsRow(catalystApp.zcql(), lockName(fileId), STATUS.RUNNING);
    await updateCommitHistory(catalystApp, progress, { status: STATUS.RUNNING });

    if (progress.step === STEP.WAITING) {
      // Only the holdings jobs that never reported COMPLETED are queued again.
      const statuses = await readJobStatuses(catalystApp.zcql(), progress.rebuild.jobs.map((j) => j.name));
      await submitRebuildJobs(catalystApp, progress.rebuild.jobs.filter((j) => statuses.get(j.name) !== "COMPLETED"));
      progress.rebuild.queuedAtMs = Date.now();
      await saveProgress(bucket, progress);
    } else {
      await saveProgress(bucket, progress);
      void runCommit(catalystApp, fileId);
    }
    return res.status(202).json({ success: true, fileId, progress: progressView(progress), message: "Continuing." });
  } catch (error) {
    console.error("[OpiOpoUpload] retryOpiOpoFile:", error);
    return fail(res, 500, { message: error.message || "Could not continue the Commit." });
  }
};

/**
 * The Commit itself, after the reply has gone. Reads the saved step and carries
 * on from there, so a crash, a failed bulk write or Try again take the same path.
 *
 *   INSERT   remove whatever an earlier attempt loaded, bulk write rows.csv,
 *            check the count (load again on a wrong count)
 *   REBUILD  queue the masters job (new accounts and ISINs) and one holdings
 *            rebuild per affected client
 *   WAITING  finished by refreshWaiting once every holdings job is COMPLETED
 */
const runCommit = async (catalystApp, fileId) => {
  if (activeCommits.has(fileId)) return;
  activeCommits.add(fileId);
  const bucket = getBucket(catalystApp);
  const zcql = catalystApp.zcql();
  const p = await getJson(bucket, stateKey(fileId, "progress.json"));

  try {
    const info = await getJson(bucket, stateKey(fileId, "file.json"));

    if (p.step === STEP.INSERT) {
      const bulkWrite = catalystApp.datastore().table(TXN_TABLE).bulkJob("write");
      const txnBucket = catalystApp.stratus().bucket(TXN_BUCKET);
      for (;;) {
        if (p.insert.attempts >= MAX_INSERT_ATTEMPTS) {
          throw new Error(`The entries could not be loaded after ${MAX_INSERT_ATTEMPTS} attempts.`);
        }
        p.insert.attempts++;
        // Go forward: whatever a failed or half-finished load left behind goes first.
        await deleteRows(zcql, await loadedRowIds(zcql, info));
        await saveProgress(bucket, p);

        const importStartedAtMs = Date.now();
        // "transactions/" is the prefix the masters job accepts.
        const objectKey = `transactions/TxnReload-OPI-${fileId}-${importStartedAtMs}.csv`;
        // Mutual funds: Security_Type 'm', TDS and Stamp Duty kept, and the amount (an OPI's cost) includes Stamp Duty.
        const rewritten = normalizeCsvForBulkUpload(await getBuffer(bucket, stateKey(fileId, "rows.csv")), {
          mutualFund: subTabOf(info).mutualFund,
        });
        await txnBucket.putObject(objectKey, rewritten, { overwrite: true, contentType: "text/csv" });
        const job = await bulkWrite.createJob({ bucket_name: TXN_BUCKET, object_key: objectKey }, { operation: "insert" });
        const jobId = job?.job_id || job?.id;
        Object.assign(p.insert, { objectKey, bulkJobId: jobId, importStartedAtMs });
        await saveProgress(bucket, p);

        const ok = await waitForBulkWrite(bulkWrite, jobId, () => saveProgress(bucket, p));
        let actual = (await loadedRowIds(zcql, info)).length;
        if (ok && actual !== p.expectedCount) {
          await sleep(BULK_POLL_MS);
          actual = (await loadedRowIds(zcql, info)).length;
        }
        if (ok && actual === p.expectedCount) {
          p.insert.inserted = actual;
          break;
        }
        console.warn(`[OpiOpoUpload] ${fileId}: load ok=${ok}, ${actual} of ${p.expectedCount} entries; loading again`);
      }

      // A copy for Commit history's download link.
      const copyKey = `transactions-original/TxnUpload-${p.insert.importStartedAtMs}-${subTabOf(info).mutualFund ? "OpiOpoMf" : "OpiOpo"}-${info.dates[0]}.csv`;
      await txnBucket
        .putObject(copyKey, await getBuffer(bucket, stateKey(fileId, "rows.csv")), { overwrite: true, contentType: "text/csv" })
        .then(() => updateCommitHistory(catalystApp, p, { fileKey: copyKey }))
        .catch((err) => console.warn("[OpiOpoUpload] history copy failed:", err.message));

      p.step = STEP.REBUILD;
      await saveProgress(bucket, p);
    }

    if (p.step === STEP.REBUILD) {
      await catalystApp.jobScheduling().JOB.submitJob({
        job_name: `USCM_${Date.now()}`.slice(0, 20),
        jobpool_name: "UpdateMasters",
        target_name: "UpdatesSecurity_ClientMasters",
        target_type: "Function",
        job_config: JOB_CONFIG,
        params: {
          source: "TxnReload",
          bucketName: TXN_BUCKET,
          objectKey: p.insert.objectKey,
          importStartedAtMs: String(p.insert.importStartedAtMs || ""),
        },
      });

      const isinsByClient = new Map();
      for (const [account, isin] of info.affected.pairs) {
        if (!isinsByClient.has(account)) isinsByClient.set(account, []);
        isinsByClient.get(account).push(isin);
      }
      // Holdings only: OPI / OPO move shares, not cash.
      const jobs = [...isinsByClient.entries()].map(([account, isins], i) => ({
        name: `${lockName(fileId)}_H_${i}`,
        kind: "H",
        account,
        isins,
      }));
      await submitRebuildJobs(catalystApp, jobs);
      Object.assign(p, {
        step: STEP.WAITING,
        rebuild: { clients: isinsByClient.size, jobs, done: 0, queuedAtMs: Date.now() },
      });
      await saveProgress(bucket, p);
    }
  } catch (error) {
    console.error(`[OpiOpoUpload] Commit ${fileId} stopped at ${p.step}:`, error);
    p.status = STATUS.FAILED;
    p.error = `Stopped at ${String(p.step).toLowerCase()}: ${error.message}`;
    await saveProgress(bucket, p).catch(() => {});
    await updateJobsStatus(zcql, lockName(fileId), STATUS.FAILED);
    await updateCommitHistory(catalystApp, p, { status: STATUS.FAILED });
    await notify(catalystApp, p, "failed", "Commit did not finish. Press Try again.");
  } finally {
    activeCommits.delete(fileId);
  }
};
