/**
 * OPI/OPO tab of Transaction Upload — shares moved in (OPI) or out (OPO) from
 * another system, for example when a client's account comes over from another
 * PMS.
 *
 *   POST /api/custody-match/opi-opo/file             check one file and keep it until Commit
 *   GET  /api/custody-match/opi-opo/:fileId          the file's summary and Commit progress
 *   POST /api/custody-match/opi-opo/:fileId/commit   add its rows to Transaction, rebuild holdings
 *   POST /api/custody-match/opi-opo/:fileId/retry    continue a Commit that stopped
 *
 * Unlike the Equity and Mutual Funds tabs (EquityReload.js), which work one
 * trade date at a time, this tab works one file at a time: a file may hold many
 * trade and settlement dates, and one Commit adds all of its rows. Rows carry no
 * Transaction ID. A file is refused when any of its rows is already in our books
 * or in another file waiting for Commit (same account, ISIN, OPI/OPO, trade
 * date, settlement date, quantity and rate: `fingerprint`), so the same file
 * can never be booked twice. A Commit only adds rows; it never removes any.
 *
 * fileId is the upload time in ms, which is also part of the raw file's key
 * (FA/<upload date>/raw-OPI_OPO-<first>-<last>-<fileId>-<name>). The rest lives
 * in Stratus temporary-files opi-opo/<fileId>/:
 *   file.json      summary: rows, dates, affected clients and pairs, fingerprints, committed
 *   rows.csv       the rows as the 26 template columns, which the import reads
 *   progress.json  the Commit's step and counts
 * The Commit's lock is the Jobs row RELOAD_OPI_<fileId>; its holdings jobs are
 * RELOAD_OPI_<fileId>_H_<n>. OPI / OPO move shares, not cash, so the cash
 * passbook is never rebuilt. Once progress.json exists the file can no longer
 * be removed from its card (deleteUploadedFile in CustodyMatchUpload.js).
 */

import { FA_FILE_TYPE, FILE_SIDE } from "../../util/custodyMatch/constants.js";
import { PROFILE_TYPE } from "../../util/custodyMatch/fieldMap.js";
import {
  detectFaFileType,
  faRowTemplateCells,
  parseFaFile,
  validateFaHeader,
} from "../../util/custodyMatch/parseFA.js";
import {
  buildFaRawKey,
  deleteKeys,
  getBucket,
  getBuffer,
  getJson,
  listAllObjects,
  loadFieldMapProfiles,
  putBuffer,
  putJson,
  putText,
} from "../../util/custodyMatch/storage.js";
import { escapeZcql as esc, listFiles, recordFile } from "../../util/custodyMatch/runStore.js";
import {
  affectedOf,
  bookedTradeTab,
  fileProblems,
  fingerprint,
  tradeFromBookedRow,
  tradeFromCells,
  tradesToOpiOpoCsv,
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
const ASSET_KIND = "OPI_OPO";
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

/* ─────────────────────────────── duplicates ─────────────────────────────── */

/** fingerprint -> how many times it occurs. */
const countPrints = (prints) => {
  const counts = new Map();
  for (const p of prints) counts.set(p, (counts.get(p) || 0) + 1);
  return counts;
};

/**
 * The OPI/OPO rows already in our books on the given trade dates, as
 * { rowId, print }. Paged by ROWID (ZCQL OFFSET repeats rows).
 */
const bookedOpiOpoRows = async (zcql, dates) => {
  const found = [];
  for (const date of dates) {
    let lastRowId = "0";
    for (;;) {
      const batch = unwrap(
        await zcql.executeZCQLQuery(
          `SELECT ROWID, WS_Account_code, ISIN, EXCHG, Security_Type, Tran_Type, TRANDATE, SETDATE, QTY, RATE FROM ${TXN_TABLE} WHERE TRANDATE = '${esc(date)}' AND ROWID > ${lastRowId} ORDER BY ROWID ASC LIMIT ${ZCQL_PAGE}`,
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

/**
 * The other OPI/OPO files still waiting for Commit: not committed, and their
 * raw file still on a card (removing a file, or the nightly clean-up, takes it
 * out of the running).
 */
const waitingFiles = async (catalystApp, bucket, exceptFileId = "") => {
  const rawKeys = new Set(
    (await listFiles(catalystApp))
      .filter((r) => r.fileType === FA_FILE_TYPE.OPI_OPO)
      .map((r) => r.fileKey),
  );
  const objects = await listAllObjects(bucket, `${FOLDER}/`);
  const files = [];
  for (const object of objects) {
    if (!object.key.endsWith("/file.json")) continue;
    const info = await readJsonOr(bucket, object.key, null);
    if (!info || info.committed || info.fileId === exceptFileId || !rawKeys.has(info.fileKey)) continue;
    files.push(info);
  }
  return files;
};

/**
 * One row error per row of the file that is already in our books, or already in
 * a file waiting for Commit. Any of them refuses the whole file.
 */
const duplicateRowErrors = async (catalystApp, bucket, trades, dates, fileName) => {
  const booked = new Set((await bookedOpiOpoRows(catalystApp.zcql(), dates)).map((r) => r.print));
  const waitingByPrint = new Map();
  for (const other of await waitingFiles(catalystApp, bucket)) {
    for (const print of other.fingerprints) waitingByPrint.set(print, other.fileName);
  }
  const errors = [];
  for (const t of trades) {
    const print = fingerprint(t);
    if (booked.has(print)) {
      errors.push({
        row: t.rowNumber,
        code: "ALREADY_BOOKED",
        message: `This ${t.tranType} of ${t.quantity} ${t.isin} for ${t.account} on ${displayDate(t.tranDate)} is already in our books.`,
        column: null,
        value: t.account,
        fileName,
      });
    } else if (waitingByPrint.has(print)) {
      errors.push({
        row: t.rowNumber,
        code: "IN_WAITING_FILE",
        message: `This ${t.tranType} of ${t.quantity} ${t.isin} for ${t.account} on ${displayDate(t.tranDate)} is already in ${waitingByPrint.get(print)}, which is waiting for Commit.`,
        column: null,
        value: t.account,
        fileName,
      });
    }
  }
  return errors;
};

/* ─────────────────────────────────── upload ─────────────────────────────── */

/**
 * POST /api/custody-match/opi-opo/file   form-data: file, uploadDate
 *
 * Checks the file and keeps it for Commit. Nothing is booked here. Every row
 * must be OPI or OPO with readable dates; there is no file date, because one
 * file may hold many trade and settlement dates.
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

    const type = PROFILE_TYPE.FA_OPI_OPO;
    const profiles = await loadFieldMapProfiles(catalystApp);
    const profile = profiles[type];
    const headerError = validateFaHeader(file.data, { profile, type });
    if (headerError) return fail(res, 400, { ...headerError, fileName: file.name });
    if ((await detectFaFileType(file.data, { profiles })) !== PROFILE_TYPE.FA) {
      return fail(res, 400, {
        code: "FA_FILE_TYPE_MISMATCH",
        message: "This file has TDS / Stamp Duty amounts, so it is a Mutual Funds transaction file, not an OPI/OPO file.",
        fileName: file.name,
      });
    }

    // No businessDate: the rows may be dated on any day.
    const parsed = await parseFaFile(file.data, { profile, type });
    if (parsed.fileError) return fail(res, 400, { ...parsed.fileError, fileName: file.name });

    // The 26 template cells; there is no Transaction ID (column 27 stays blank).
    const trades = parsed.rows.map((row) => tradeFromCells([...faRowTemplateCells(row), ""], { rowNumber: row.rowNumber }));
    const rowErrors = [
      ...parsed.rowErrors.map((e) => ({ ...e, fileName: file.name })),
      ...fileProblems(trades, { opiOpo: true }).map((p) => ({ ...p, fileName: file.name })),
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
    const dates = [...new Set(trades.map((t) => t.tranDate))].sort();
    const duplicates = await duplicateRowErrors(catalystApp, bucket, trades, dates, file.name);
    if (duplicates.length > 0) {
      return fail(res, 409, {
        code: "DUPLICATE_ROWS",
        message: `${duplicates.length} row(s) of this file are already in our books or in another file waiting for Commit. A file can be uploaded only once. Nothing was uploaded.`,
        fileName: file.name,
        ...summariseIssues(duplicates, []),
        hint: "If this is a corrected file, remove the waiting one first. Entries already committed cannot be uploaded again.",
      });
    }

    const uploadedAtMs = Date.now();
    const fileId = String(uploadedAtMs);
    const key = buildFaRawKey({
      businessDate: uploadedOn,
      fileType: FA_FILE_TYPE.OPI_OPO,
      coveredFrom: dates[0],
      coveredTo: dates[dates.length - 1],
      originalFileName: file.name,
      uploadedAtMs,
    });
    const affected = affectedOf([], trades);
    const info = {
      fileId,
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
      committed: false,
    };

    // The state first and the file row last: a card only lists a file once its row exists.
    await putBuffer(bucket, key, file.data, "text/csv");
    await putText(bucket, stateKey(fileId, "rows.csv"), tradesToOpiOpoCsv(trades), "text/csv");
    await putJson(bucket, stateKey(fileId, "file.json"), info);
    try {
      await recordFile(catalystApp, {
        fileKey: key,
        side: FILE_SIDE.FA,
        fileType: FA_FILE_TYPE.OPI_OPO,
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

    // Another file with the same rows may have been committed since this one was uploaded.
    if ((await loadedRowIds(zcql, info)).length > 0) {
      return fail(res, 409, {
        code: "ALREADY_BOOKED",
        message: `Some rows of ${info.fileName} are already in our books, so it cannot be committed. Remove it.`,
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
      Asset_Kind: ASSET_KIND,
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
        const rewritten = normalizeCsvForBulkUpload(await getBuffer(bucket, stateKey(fileId, "rows.csv")));
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
      const copyKey = `transactions-original/TxnUpload-${p.insert.importStartedAtMs}-OpiOpo-${info.dates[0]}.csv`;
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
