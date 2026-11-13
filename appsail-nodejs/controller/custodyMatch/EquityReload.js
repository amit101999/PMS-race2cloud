/**
 * Equity tab of Transaction Upload — upload, match and Pre-commit a trading date
 * so that our books always equal the latest FA files for that date.
 *
 *   POST /api/custody-match/reload/fa-file         check IDs, decide First/Add/Replace/Ask, badges
 *   POST /api/custody-match/reload/custody-file    custodian file: First/Add/Skip/Ask by its lines, Replace on request
 *   POST /api/custody-match/reload/run             build the date's final list, then match it
 *   GET  /api/custody-match/reload/consolidated-files  the file each date's trades were last built from
 *   GET  /api/custody-match/reload/commit-history  every commit, newest first
 *   GET  /api/custody-match/reload/:date           pending files, match summary, Pre-commit progress
 *   POST /api/custody-match/reload/:date/pre-commit  clean up the date, insert again, rebuild
 *   POST /api/custody-match/reload/:date/retry     continue a Pre-commit that stopped
 *
 * Uploading and matching never change our books. Only Pre-commit does, in this
 * order, each step saved before the next starts so a failure continues where it
 * stopped ("failures go forward"):
 *
 *   TRANSACTION  delete the date's equity trades
 *   HOLDINGS     delete Holdings of every affected (client, ISIN)
 *   CASH         delete the cash passbook of every affected client
 *   MASTERS      delete automatic master rows (stubs) that no trade uses any more
 *   INSERT       delete the date's equity trades again, bulk write the consolidated
 *                file, check the count (repeat on a wrong count)
 *   REBUILD      queue the masters job and, per client, RebuildHoldingtable + Cal_CB_Per_TNX
 *   WAITING      wait until every rebuild job reports COMPLETED, then unlock the date
 *
 * A date's first commit (no history row and no equity trades booked) has nothing to
 * clean up, so it starts at INSERT. The rebuild jobs still run: they clear and
 * rebuild the holdings and cash they cover, so a back-dated trade lands in order.
 *
 * Every commit gets a row in Transaction_Commit_History. The rest of the state
 * lives in Stratus (bucket temporary-files):
 *   reload/<date>/state.json         which FA files are pending, with their mode
 *   reload/<date>/consolidated.csv   the date's final list: 26 template columns + Transaction ID
 *   reload/<date>/affected.json      every (client, ISIN) of the old and new trades
 *   reload/<date>/summary.json       the match's badges and the run it belongs to
 *   reload/<date>/progress.json      the Pre-commit step and counts
 * The date's lock is the Jobs row RELOAD_<date> (RUNNING until finished).
 *
 * Plan: docs/transaction upload/transient-wibbling-eclipse.md.
 */

import { FA_FILE_TYPE, FILE_SIDE, RUN_STATUS } from "../../util/custodyMatch/constants.js";
import { FA_CONSOLIDATED_PROFILE, PROFILE_TYPE } from "../../util/custodyMatch/fieldMap.js";
import {
  detectFaFileType,
  faRowTemplateCells,
  parseFaFile,
  validateFaHeader,
} from "../../util/custodyMatch/parseFA.js";
import {
  buildFaRawKey,
  deleteKeys,
  deleteMatchFilesForDate,
  getBucket,
  getBuffer,
  getJson,
  listAllObjects,
  loadFieldMapProfiles,
  objectExists,
  putBuffer,
  putJson,
  putText,
} from "../../util/custodyMatch/storage.js";
import {
  IMPORTED_MARKER,
  createRun,
  deleteExceptionsForDate,
  deleteFileRows,
  deleteRunsForDate,
  escapeZcql as esc,
  failRun,
  getRun,
  listFiles,
  listRunsForDate,
  newRunId,
  recordFile,
  setStageDetail,
  supersedeRunsForDate,
} from "../../util/custodyMatch/runStore.js";
import {
  BOOKED_COLUMNS,
  MODE,
  affectedOf,
  buildDateList,
  compareWithList,
  decideMode,
  fileProblems,
  isBigRemoval,
  closedDateMessage,
  DEFAULT_WINDOW_DAYS,
  dateWindowView,
  parseWindowDays,
  isMutualFundTrade,
  tradeFromBookedRow,
  tradeFromCells,
  tradesToTemplateCsv,
} from "../../util/custodyMatch/equityReload.js";
import { readDateWindow, saveDateWindow } from "../../util/custodyMatch/changeWindow.js";
import {
  allRowErrors,
  getUploadedFile,
  isCsv,
  readUploadDate,
  readUploadedOn,
  summariseIssues,
  uploadCustodyFile,
} from "./CustodyMatchUpload.js";
import {
  BUCKET_NAME as TXN_BUCKET,
  TABLE_NAME as TXN_TABLE,
  ensureJobsRow,
  findImportsStillProcessing,
  isBulkFailedStatus,
  isBulkSuccessStatus,
  normalizeCsvForBulkUpload,
  timeOfDay,
  updateJobsStatus,
} from "../uploader/TempTransactionUpload.js";
import { insertNotification } from "../../util/notifications.js";

const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;
const ZCQL_PAGE = 300;
/* ZCQL removes at most 300 rows per DELETE; ROWID IN lists are kept below that. */
const DELETE_BATCH = 200;
const ZCQL_DELETE_LIMIT = 300;
const CONSOLIDATED_FILES_SHOWN = 30;
const HISTORY_TABLE = "Transaction_Commit_History";
const COMMIT_HISTORY_SHOWN = 50;
const COMMIT_TYPE = { FIRST: "FIRST", RECOMMIT: "RECOMMIT" };

const STEP = {
  TRANSACTION: "TRANSACTION",
  HOLDINGS: "HOLDINGS",
  CASH: "CASH",
  MASTERS: "MASTERS",
  INSERT: "INSERT",
  REBUILD: "REBUILD",
  WAITING: "WAITING",
  DONE: "DONE",
};

const STATUS = { RUNNING: "RUNNING", FAILED: "FAILED", COMPLETED: "COMPLETED" };

/** A wrong count after the bulk write means clean the date and insert again, at most this often. */
const MAX_INSERT_ATTEMPTS = 3;
const BULK_POLL_MS = 15_000;
const BULK_MAX_WAIT_MS = 45 * 60 * 1000;
/* The rebuild jobs report COMPLETED only on success; one still RUNNING after this is taken as failed. */
const REBUILD_GIVE_UP_MS = 2 * 60 * 60 * 1000;
/* A RUNNING Pre-commit not saved for this long has lost its server (restart); Try again may continue it. */
const STALLED_AFTER_MS = 15 * 60 * 1000;
const JOB_CONFIG = { number_of_retries: 5, retry_interval: 60 * 1000 };

/* Pre-commits running in this server, so a double click cannot start two loops for a date. */
const activeReloads = new Set();

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const fail = (res, status, body) => res.status(status).json({ success: false, ...body });

const noApp = (res) =>
  fail(res, 500, { code: "CATALYST_NOT_INITIALIZED", message: "Catalyst not initialized" });

const reloadKey = (date, name) => `reload/${date}/${name}`;
const lockJobName = (date) => `RELOAD_${date}`;
const rebuildJobName = (date, kind, index) => `RELOAD_${date}_${kind}_${index}`;

const unwrap = (rows, table) => (rows || []).map((r) => r[table] || r);

const readJsonOr = async (bucket, key, fallback) => {
  try {
    return await getJson(bucket, key);
  } catch {
    return fallback;
  }
};

/* ───────────────────────────── pending files ───────────────────────────── */

/** Which FA files of the date are pending (uploaded in the Equity tab since the last Pre-commit). */
const readState = (bucket, date) => readJsonOr(bucket, reloadKey(date, "state.json"), { files: {} });

const writeState = (bucket, date, state) => putJson(bucket, reloadKey(date, "state.json"), state);

/**
 * The pending FA files of the date, oldest upload first, each with its mode.
 * A file row not in state.json is already in our books (an earlier Pre-commit, or
 * the old upload screen). It stays listed on the screen and is left out here.
 */
const pendingFaFiles = async (catalystApp, bucket, date) => {
  const state = await readState(bucket, date);
  const rows = await listFiles(catalystApp, { forDate: date });
  return rows
    .filter((r) => r.side === FILE_SIDE.FA && state.files[r.fileKey])
    .map((r) => ({ ...r, mode: state.files[r.fileKey].mode }));
};

const custodyFilesOfDate = async (catalystApp, date) =>
  (await listFiles(catalystApp, { forDate: date })).filter((r) => r.side === FILE_SIDE.CUSTODY);

/** FA files a Replace has overtaken: their rows and objects go, and a copy is kept for audit. */
const retireFaFiles = async (catalystApp, bucket, date, files) => {
  for (const file of files) {
    const name = file.fileKey.split("/").pop();
    await putBuffer(bucket, reloadKey(date, `replaced/${name}`), await getBuffer(bucket, file.fileKey));
  }
  await deleteFileRows(catalystApp, files);
  await deleteKeys(bucket, files.map((f) => f.fileKey));
};

/** A parsed FA file as trades. */
const tradesOfParsedFile = (parsed) =>
  parsed.rows.map((row) =>
    // The 26 template cells, then the Transaction ID the mapping found (column 27).
    tradeFromCells([...faRowTemplateCells(row), row.mapped?.transactionId ?? ""], { rowNumber: row.rowNumber }),
  );

/** Re-read the pending files, oldest first, as { mode, trades }. */
const loadPendingTrades = async (bucket, files, date, profile) => {
  const loaded = [];
  for (const f of files) {
    const parsed = await parseFaFile(await getBuffer(bucket, f.fileKey), {
      businessDate: date,
      profile,
      type: PROFILE_TYPE.FA,
    });
    if (!parsed.ok) {
      throw Object.assign(
        new Error(
          `The stored file ${f.originalFileName} no longer reads against the saved field mapping. Remove it and upload it again.`,
        ),
        { code: "PENDING_FILE_UNREADABLE" },
      );
    }
    loaded.push({ mode: f.mode, trades: tradesOfParsedFile(parsed) });
  }
  return loaded;
};

/* ─────────────────────────────── our books ─────────────────────────────── */

const BOOKED_SELECT = [
  "ROWID",
  ...new Set([...BOOKED_COLUMNS.filter(Boolean), "WS_client_id", "Security_Name"]),
].join(", ");

/*
 * Reads on Transaction now and then fail with "Error occurred during query
 * processing" and pass when sent again, so a read is tried up to three times.
 * Only SELECTs go through here.
 */
const READ_ATTEMPTS = 3;
const selectWithRetry = async (zcql, query) => {
  for (let attempt = 1; ; attempt++) {
    try {
      return await zcql.executeZCQLQuery(query);
    } catch (err) {
      if (attempt >= READ_ATTEMPTS) throw err;
      console.warn(`[EquityReload] read failed (attempt ${attempt}), trying again: ${err.message}`);
      await sleep(500 * attempt);
    }
  }
};

/** The date's equity trades in Transaction, as trades. Paged by ROWID (OFFSET repeats rows). */
const readBookedEquityTrades = async (zcql, date) => {
  const trades = [];
  let lastRowId = "0";
  for (;;) {
    const batch = unwrap(
      await selectWithRetry(
        zcql,
        `SELECT ${BOOKED_SELECT} FROM ${TXN_TABLE} WHERE TRANDATE = '${esc(date)}' AND ROWID > ${lastRowId} ORDER BY ROWID ASC LIMIT ${ZCQL_PAGE}`,
      ),
      TXN_TABLE,
    );
    for (const row of batch) {
      if (!isMutualFundTrade(row.ISIN, row.EXCHG)) trades.push(tradeFromBookedRow(row));
    }
    if (batch.length < ZCQL_PAGE) break;
    lastRowId = String(batch[batch.length - 1].ROWID);
  }
  return trades;
};

/** ROWIDs of the date's equity trades. Mutual fund trades of the date are never touched. */
const equityRowIdsOfDate = async (zcql, date) => {
  const ids = [];
  let lastRowId = "0";
  for (;;) {
    const batch = unwrap(
      await selectWithRetry(
        zcql,
        `SELECT ROWID, ISIN, EXCHG FROM ${TXN_TABLE} WHERE TRANDATE = '${esc(date)}' AND ROWID > ${lastRowId} ORDER BY ROWID ASC LIMIT ${ZCQL_PAGE}`,
      ),
      TXN_TABLE,
    );
    for (const row of batch) {
      if (!isMutualFundTrade(row.ISIN, row.EXCHG)) ids.push(String(row.ROWID));
    }
    if (batch.length < ZCQL_PAGE) break;
    lastRowId = String(batch[batch.length - 1].ROWID);
  }
  return ids;
};

/**
 * A date's first commit: no equity trades of the date in our books, so there is
 * nothing to clean up. A date committed before and then deleted counts as first
 * again; trades booked by the old upload screen do not.
 */
const isFirstCommit = async (zcql, date) => (await equityRowIdsOfDate(zcql, date)).length === 0;

/** Repeating it is safe: a second run finds nothing left to delete. */
const deleteEquityTradesOfDate = async (zcql, date) => {
  const ids = await equityRowIdsOfDate(zcql, date);
  for (let i = 0; i < ids.length; i += DELETE_BATCH) {
    await zcql.executeZCQLQuery(
      `DELETE FROM ${TXN_TABLE} WHERE ROWID IN (${ids.slice(i, i + DELETE_BATCH).join(", ")})`,
    );
  }
  return ids.length;
};

/* Same loop as deleteHoldingsForPair in functions/RebuildHoldingtable. */
const deleteHoldingsForPair = async (zcql, account, isin) => {
  for (;;) {
    const result = await zcql.executeZCQLQuery(
      `DELETE FROM Holdings WHERE WS_Account_code = '${esc(account)}' AND ISIN = '${esc(isin)}'`,
    );
    const deleted = Number(result?.[0]?.Holdings?.DELETED_ROWS_COUNT) || 0;
    if (deleted < ZCQL_DELETE_LIMIT) return;
  }
};

const countPassbookRows = async (zcql, account) => {
  const rows = await zcql.executeZCQLQuery(
    `SELECT COUNT(ROWID) FROM Cash_Balance_Per_Transaction WHERE Account_Code = '${esc(account)}'`,
  );
  const r = rows?.[0]?.Cash_Balance_Per_Transaction || rows?.[0] || {};
  return Number(r["COUNT(ROWID)"] ?? Object.values(r)[0] ?? 0);
};

/* Same rule as deleteCashPassbookForAccount in functions/Cal_CB_Per_TNX: repeat until empty. */
const deletePassbook = async (zcql, account) => {
  let remaining = await countPassbookRows(zcql, account);
  for (let round = 0; remaining > 0; round++) {
    if (round >= 500) throw new Error(`Cash passbook of ${account} could not be emptied.`);
    await zcql.executeZCQLQuery(
      `DELETE FROM Cash_Balance_Per_Transaction WHERE Account_Code = '${esc(account)}'`,
    );
    const after = await countPassbookRows(zcql, account);
    if (after >= remaining) throw new Error(`Cash passbook delete stalled for ${account}.`);
    remaining = after;
  }
};

const isBlank = (v) => v === null || v === undefined || String(v).trim() === "" || String(v).trim().toLowerCase() === "null";

/**
 * Transaction.Transaction_ID is unique, so an ID already booked on another date
 * would be dropped by the bulk write without an error. Such a file is refused.
 */
const idsBookedOnOtherDates = async (zcql, ids, date) => {
  const unique = [...new Set(ids.filter(Boolean))];
  const found = [];
  for (let i = 0; i < unique.length; i += 100) {
    const list = unique.slice(i, i + 100).map((id) => `'${esc(id)}'`).join(", ");
    const rows = unwrap(
      await selectWithRetry(
        zcql,
        `SELECT Transaction_ID, TRANDATE FROM ${TXN_TABLE} WHERE Transaction_ID IN (${list}) AND TRANDATE != '${esc(date)}' LIMIT ${ZCQL_PAGE}`,
      ),
      TXN_TABLE,
    );
    found.push(...rows.map((r) => ({ txnId: String(r.Transaction_ID), tranDate: String(r.TRANDATE).slice(0, 10) })));
  }
  return found;
};

const anyTradeUses = async (zcql, column, value) =>
  unwrap(
    await zcql.executeZCQLQuery(
      `SELECT ROWID FROM ${TXN_TABLE} WHERE ${column} = '${esc(value)}' LIMIT 1`,
    ),
    TXN_TABLE,
  ).length > 0;

/**
 * Master rows are shared by every date and mostly filled from the master files,
 * so only a stub is removed: a row the trade upload added on its own (only the
 * ISIN / account code filled) that no trade on any date uses any more. Values in
 * the new file are kept, because the masters job adds them back straight after.
 */
const deleteUnusedStubMasters = async (zcql, affected, finalTrades) => {
  const keepIsins = new Set(finalTrades.map((t) => t.isin));
  const keepAccounts = new Set(finalTrades.map((t) => t.account));
  let securities = 0;
  let clients = 0;

  for (const isin of affected.isins) {
    if (keepIsins.has(isin) || (await anyTradeUses(zcql, "ISIN", isin))) continue;
    const rows = unwrap(
      await zcql.executeZCQLQuery(
        `SELECT ROWID, Security_Code, Security_Name FROM Security_List WHERE ISIN = '${esc(isin)}'`,
      ),
      "Security_List",
    );
    for (const row of rows) {
      if (!isBlank(row.Security_Code) || !isBlank(row.Security_Name)) continue;
      await zcql.executeZCQLQuery(`DELETE FROM Security_List WHERE ROWID = ${row.ROWID}`);
      securities++;
    }
  }

  for (const account of affected.clients) {
    if (keepAccounts.has(account) || (await anyTradeUses(zcql, "WS_Account_code", account))) continue;
    const rows = unwrap(
      await zcql.executeZCQLQuery(
        `SELECT ROWID, Actual_Code FROM clientIds WHERE WS_Account_code = '${esc(account)}'`,
      ),
      "clientIds",
    );
    for (const row of rows) {
      if (!isBlank(row.Actual_Code)) continue;
      await zcql.executeZCQLQuery(`DELETE FROM clientIds WHERE ROWID = ${row.ROWID}`);
      clients++;
    }
  }
  return { securities, clients };
};

/* ───────────────────────────── progress and lock ───────────────────────── */

const readProgress = (bucket, date) => readJsonOr(bucket, reloadKey(date, "progress.json"), null);

const saveProgress = async (bucket, progress) => {
  progress.updatedAtMs = Date.now();
  await putJson(bucket, reloadKey(progress.date, "progress.json"), progress);
};

const readJobStatuses = async (zcql, names) => {
  const statusByName = new Map();
  for (let i = 0; i < names.length; i += 50) {
    const list = names.slice(i, i + 50).map((n) => `'${esc(n)}'`).join(", ");
    const rows = unwrap(
      await zcql.executeZCQLQuery(`SELECT jobName, status FROM Jobs WHERE jobName IN (${list})`),
      "Jobs",
    );
    for (const row of rows) statusByName.set(row.jobName, String(row.status || "").toUpperCase());
  }
  return statusByName;
};

/* The commit itself is what matters, so a history row that cannot be updated only logs. */
const updateCommitHistory = (catalystApp, progress, patch) =>
  catalystApp
    .datastore()
    .table(HISTORY_TABLE)
    .updateRow({ ROWID: progress.historyRowId, ...patch })
    .catch((err) => console.warn("[EquityReload] commit history update failed:", err.message));

/* 2026-10-28 → 28-10-2026, the way the screen shows dates. */
const displayDate = (date) => String(date).split("-").reverse().join("-");

const notify = (catalystApp, progress, status, message) =>
  insertNotification(catalystApp.zcql(), {
    jobName: `RELOAD_${progress.date}_${progress.startedAtMs}`,
    title: `Commit — ${displayDate(progress.date)}`,
    status,
    message,
  }).catch((err) => console.warn("[EquityReload] notification failed:", err.message));

/**
 * While the rebuild jobs run, the progress is brought up to date whenever it is
 * read: finished once every job row says COMPLETED, failed once it has waited
 * REBUILD_GIVE_UP_MS. Nothing has to stay running in this server for that.
 */
const refreshWaiting = async (catalystApp, bucket, progress) => {
  if (!progress || progress.step !== STEP.WAITING || progress.status !== STATUS.RUNNING) {
    return progress;
  }
  const names = progress.rebuild.jobs.map((j) => j.name);
  const statuses = await readJobStatuses(catalystApp.zcql(), names);
  const done = names.filter((n) => statuses.get(n) === "COMPLETED").length;
  progress.rebuild.done = done;

  if (done === names.length) {
    progress.status = STATUS.COMPLETED;
    progress.step = STEP.DONE;
    progress.finishedAtMs = Date.now();
    await saveProgress(bucket, progress);
    await updateJobsStatus(catalystApp.zcql(), lockJobName(progress.date), STATUS.COMPLETED);
    await updateCommitHistory(catalystApp, progress, { status: STATUS.COMPLETED, finishedAtMs: progress.finishedAtMs });
    await notify(
      catalystApp,
      progress,
      "completed",
      `${progress.insert.inserted} trades committed. Holdings and cash updated for ${progress.rebuild.clients} client(s).`,
    );
    return progress;
  }

  if (Date.now() - progress.rebuild.queuedAtMs > REBUILD_GIVE_UP_MS) {
    const pending = names.length - done;
    progress.status = STATUS.FAILED;
    progress.error = `${pending} of ${names.length} holdings / cash rebuilds did not finish. Press Try again to run them again.`;
    await saveProgress(bucket, progress);
    await updateJobsStatus(catalystApp.zcql(), lockJobName(progress.date), STATUS.FAILED);
    await updateCommitHistory(catalystApp, progress, { status: STATUS.FAILED });
    await notify(catalystApp, progress, "failed", "Holdings and cash update did not finish. Press Try again.");
    return progress;
  }

  await saveProgress(bucket, progress);
  return progress;
};

/** The date is locked from the first Pre-commit until it has COMPLETED. */
const currentProgress = async (catalystApp, bucket, date) =>
  refreshWaiting(catalystApp, bucket, await readProgress(bucket, date));

const isLocked = (progress) => Boolean(progress) && progress.status !== STATUS.COMPLETED;

/**
 * A trade date past its working-day change window is locked from upload to commit
 * (file removal is refused in CustodyMatchUpload.js). Returns the refusal, or
 * null while the date is open. A Commit that started in time can still be
 * finished with Try again, which does not check this, so the books are never
 * left half done.
 */
const closedDateResponse = async (res, catalystApp, date) => {
  const message = closedDateMessage(date, await readDateWindow(catalystApp, date));
  return message ? fail(res, 409, { code: "DATE_CLOSED", message }) : null;
};

const lockedMessage = (date, progress) =>
  progress.status === STATUS.FAILED
    ? `The Commit of ${date} did not finish. Press Try again on the Equity tab before changing this date.`
    : `${date} is being rebuilt. Try again when it finishes.`;

/* ─────────────────────────────── FA upload ─────────────────────────────── */

/** The date's current list: our books plus the pending files. Uploads are compared with it. */
const currentDateList = async (catalystApp, bucket, date, profile) => {
  const booked = await readBookedEquityTrades(catalystApp.zcql(), date);
  const pending = await pendingFaFiles(catalystApp, bucket, date);
  const pendingTrades = await loadPendingTrades(bucket, pending, date, profile);
  return { booked, pending, ...buildDateList(booked, pendingTrades) };
};

/**
 * The file's trades already in our books with the same values, saved as a CSV in
 * the upload's 27 columns so the screen can offer it for download. It sits beside
 * the date's FA files, a folder the download route accepts. The upload goes on
 * if it cannot be saved; only the download link is missing then.
 */
const withDuplicatesFile = async (bucket, date, comparison) => {
  const { duplicateTrades, ...rest } = comparison;
  if (duplicateTrades.length === 0) return { ...rest, duplicatesKey: "" };
  const key = `FA-Transaction/${date}/duplicates-${date}-${Date.now()}.csv`;
  try {
    await putText(bucket, key, tradesToTemplateCsv(duplicateTrades), "text/csv");
    return { ...rest, duplicatesKey: key };
  } catch (err) {
    console.warn("[EquityReload] duplicates file not saved:", err.message);
    return { ...rest, duplicatesKey: "" };
  }
};

/**
 * POST /api/custody-match/reload/fa-file
 * form-data: file, businessDate, uploadDate, [choice=ADD|REPLACE]
 *
 * choice is sent only after the system asked (mode ASK came back). Nothing is
 * stored while the answer is ASK.
 */
export const uploadEquityFaFile = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);

    const file = getUploadedFile(req);
    if (!file) {
      return fail(res, 400, { code: "NO_FILE", message: "CSV file is required. Send it as form-data with key 'file'." });
    }
    if (!isCsv(file.name)) return fail(res, 400, { code: "NOT_CSV", message: "Only CSV files are allowed." });

    const choice = String(req.body?.choice || "").trim().toUpperCase();
    if (choice && choice !== MODE.ADD && choice !== MODE.REPLACE) {
      return fail(res, 400, { code: "BAD_CHOICE", message: `choice must be ADD or REPLACE; received "${choice}".` });
    }

    const forDate = readUploadDate(req.body?.businessDate);
    if (forDate.error) return fail(res, 400, { code: "BAD_DATE", message: forDate.error });
    const { businessDate } = forDate;
    const closed = await closedDateResponse(res, catalystApp, businessDate);
    if (closed) return closed;
    const uploadDay = readUploadedOn(req.body?.uploadDate);
    if (uploadDay.error) return fail(res, 400, { code: "BAD_UPLOAD_DATE", message: uploadDay.error });
    const { uploadedOn } = uploadDay;

    const profiles = await loadFieldMapProfiles(catalystApp);
    const profile = profiles[PROFILE_TYPE.FA];

    const headerError = validateFaHeader(file.data, { profile, type: PROFILE_TYPE.FA });
    if (headerError) return fail(res, 400, { ...headerError, fileName: file.name });

    if ((await detectFaFileType(file.data, { profiles })) !== PROFILE_TYPE.FA) {
      return fail(res, 400, {
        code: "FA_FILE_TYPE_MISMATCH",
        message: "This file has TDS / Stamp Duty amounts, so it is a Mutual Funds transaction file. Upload it in the Mutual Funds tab.",
        fileName: file.name,
      });
    }

    const parsed = await parseFaFile(file.data, { businessDate, profile, type: PROFILE_TYPE.FA });
    if (parsed.fileError) return fail(res, 400, { ...parsed.fileError, fileName: file.name });

    const newTrades = tradesOfParsedFile(parsed);
    const rowErrors = [
      ...allRowErrors(parsed, businessDate, file.name),
      ...fileProblems(newTrades).map((p) => ({ ...p, fileName: file.name })),
    ].sort((a, b) => a.row - b.row);
    if (rowErrors.length > 0) {
      return fail(res, 400, {
        code: "VALIDATION_FAILED",
        message: `${rowErrors.length} row(s) in the transaction file cannot be used for ${businessDate}. Nothing was uploaded.`,
        fileName: file.name,
        businessDate,
        ...summariseIssues(rowErrors, parsed.warnings),
        hint: `Every row must be dated ${businessDate}, carry its own transaction ID, and each ID may appear once with one set of values.`,
      });
    }

    const bucket = getBucket(catalystApp);
    const progress = await currentProgress(catalystApp, bucket, businessDate);
    if (isLocked(progress)) {
      return fail(res, 409, { code: "DATE_LOCKED", message: lockedMessage(businessDate, progress) });
    }

    const elsewhere = await idsBookedOnOtherDates(catalystApp.zcql(), newTrades.map((t) => t.txnId), businessDate);
    if (elsewhere.length > 0) {
      return fail(res, 400, {
        code: "TRANSACTION_ID_ON_OTHER_DATE",
        message: `${elsewhere.length} transaction ID(s) in this file are already booked on another date, and a transaction ID can be used only once. Nothing was uploaded.`,
        fileName: file.name,
        ...summariseIssues(
          elsewhere.map((e) => ({
            row: newTrades.find((t) => t.txnId === e.txnId)?.rowNumber ?? null,
            code: "TRANSACTION_ID_ON_OTHER_DATE",
            message: `Transaction ID ${e.txnId} is already booked on ${e.tranDate}.`,
            column: "Transaction ID",
            value: e.txnId,
            fileName: file.name,
          })),
          [],
        ),
        hint: "Check the Transaction ID column, or correct the trade on its own date first.",
      });
    }

    const current = await currentDateList(catalystApp, bucket, businessDate, profile);
    const decided = choice
      ? { mode: choice, reason: null }
      : decideMode(newTrades, current.trades);

    // Duplicates (already in our books) are the same trades for Replace and Add.
    const markBooked = { markBooked: true };
    if (decided.mode === MODE.ASK) {
      const replace = await withDuplicatesFile(
        bucket,
        businessDate,
        compareWithList(newTrades, current.trades, MODE.REPLACE, markBooked),
      );
      const { duplicateTrades, ...add } = compareWithList(newTrades, current.trades, MODE.ADD, markBooked);
      return res.status(200).json({
        success: true,
        stored: false,
        mode: MODE.ASK,
        reason: decided.reason,
        businessDate,
        fileName: file.name,
        currentCount: current.trades.length,
        options: { replace, add: { ...add, duplicatesKey: replace.duplicatesKey } },
        message: "Choose whether this file replaces the date's trades or is added to them.",
      });
    }

    const comparison = await withDuplicatesFile(
      bucket,
      businessDate,
      compareWithList(newTrades, current.trades, decided.mode === MODE.REPLACE ? MODE.REPLACE : MODE.ADD, markBooked),
    );

    // Saved first, so nothing already there is lost if storing fails.
    const uploadedAtMs = Date.now();
    const key = buildFaRawKey({
      businessDate,
      fileType: FA_FILE_TYPE.REGULAR,
      coveredFrom: businessDate,
      coveredTo: businessDate,
      originalFileName: file.name,
      uploadedAtMs,
    });
    await putBuffer(bucket, key, file.data, "text/csv");
    try {
      await recordFile(catalystApp, {
        fileKey: key,
        side: FILE_SIDE.FA,
        fileType: FA_FILE_TYPE.REGULAR,
        forDate: businessDate,
        uploadedOn,
        originalFileName: file.name,
      });
    } catch (err) {
      await deleteKeys(bucket, [key]);
      throw new Error(`The file could not be recorded, so it was not kept: ${err.message}`);
    }

    // A Replace takes every older equity FA file of the date off the card at once,
    // also those already in our books (their trades stay until the Pre-commit).
    const replacedFiles =
      decided.mode === MODE.REPLACE
        ? (await listFiles(catalystApp, { forDate: businessDate })).filter(
            (r) => r.side === FILE_SIDE.FA && r.fileType === FA_FILE_TYPE.REGULAR && r.fileKey !== key,
          )
        : [];

    const state = await readState(bucket, businessDate);
    for (const old of replacedFiles) delete state.files[old.fileKey];
    state.files[key] = { mode: decided.mode, uploadedAtMs, fileName: file.name };
    await writeState(bucket, businessDate, state);

    // After the state: the new file already decides the date, so a failed tidy-up
    // only leaves an older file listed.
    await retireFaFiles(catalystApp, bucket, businessDate, replacedFiles).catch((err) =>
      console.warn("[EquityReload] could not remove the replaced FA files:", err.message),
    );

    const superseded = await supersedeRunsForDate(catalystApp, businessDate);

    return res.status(200).json({
      success: true,
      stored: true,
      mode: decided.mode,
      businessDate,
      key,
      fileName: file.name,
      comparison,
      bigRemoval: isBigRemoval(comparison),
      stats: parsed.stats,
      ...summariseIssues([], parsed.warnings),
      supersededRuns: superseded.runIds,
      message: `The file is staged for ${businessDate} (${decided.mode.toLowerCase()}). Nothing is booked until Commit.`,
    });
  } catch (error) {
    console.error("[EquityReload] uploadEquityFaFile:", error);
    return fail(res, 500, { code: error.code, message: error.message || "Could not stage the transaction file." });
  }
};

/**
 * A replaced custodian file makes the date's match results meaningless. Only this
 * date's are removed, and our books are not touched. One run covers every scheme
 * of the date, so the next match recreates the other schemes' results.
 */
const clearMatchResults = async (catalystApp, bucket, date) => {
  await deleteExceptionsForDate(catalystApp, date);
  const removed = await deleteMatchFilesForDate(bucket, date);
  const summaryKey = reloadKey(date, "summary.json");
  const summary = (await objectExists(bucket, summaryKey)) ? await deleteKeys(bucket, [summaryKey]) : { failed: [] };
  const failed = [...removed.failed, ...summary.failed];
  if (failed.length > 0) {
    throw new Error(`${failed.length} stored match file(s) could not be deleted, starting with ${failed[0].key}.`);
  }
  await deleteRunsForDate(catalystApp, date);
};

/**
 * POST /api/custody-match/reload/custody-file — same form as /custody-file, and
 * replace=true only after the system asked (mode ASK came back). The file's lines
 * are compared with the files stored for its date, scheme and type: all new is
 * added, all already there is skipped, and anything else needs the operator's
 * Replace (compareCustodyRows).
 */
export const uploadEquityCustodyFile = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);

    const bucket = getBucket(catalystApp);
    const businessDate = String(req.body?.businessDate || "").trim();
    if (DATE_PATTERN.test(businessDate)) {
      const closed = await closedDateResponse(res, catalystApp, businessDate);
      if (closed) return closed;
      const progress = await currentProgress(catalystApp, bucket, businessDate);
      if (isLocked(progress)) {
        return fail(res, 409, { code: "DATE_LOCKED", message: lockedMessage(businessDate, progress) });
      }
    }
    req.equityReload = { clearMatchResults: (date) => clearMatchResults(catalystApp, bucket, date) };
    return uploadCustodyFile(req, res);
  } catch (error) {
    console.error("[EquityReload] uploadEquityCustodyFile:", error);
    return fail(res, 500, { message: error.message || "Could not store the custodian file." });
  }
};

/* ────────────────────────────────── match ──────────────────────────────── */

const pendingKeysOf = (pending) => pending.map((f) => f.fileKey).sort();

/**
 * POST /api/custody-match/reload/run   body: { businessDate, uploadDate }
 *
 * Builds the date's final list (books + pending files, duplicates merged) and
 * the affected pairs NOW, before anything is deleted, then starts the existing
 * match job on that list against every custodian file of the date.
 */
export const startEquityRun = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);

    const businessDate = String(req.body?.businessDate || "").trim();
    const uploadDate = String(req.body?.uploadDate || "").trim();
    if (!DATE_PATTERN.test(businessDate) || !DATE_PATTERN.test(uploadDate)) {
      return fail(res, 400, { code: "BAD_DATE", message: "businessDate and uploadDate must be yyyy-mm-dd." });
    }

    const closed = await closedDateResponse(res, catalystApp, businessDate);
    if (closed) return closed;

    const bucket = getBucket(catalystApp);
    const progress = await currentProgress(catalystApp, bucket, businessDate);
    if (isLocked(progress)) {
      return fail(res, 409, { code: "DATE_LOCKED", message: lockedMessage(businessDate, progress) });
    }

    const pending = await pendingFaFiles(catalystApp, bucket, businessDate);
    const custodyFiles = await custodyFilesOfDate(catalystApp, businessDate);
    const blocked = (blockers) =>
      fail(res, 409, {
        code: "GATE_1_BLOCKED",
        message: `${businessDate} cannot be matched yet. ${blockers.map((b) => b.message).join(" ")}`,
        blockers,
      });
    if (custodyFiles.length === 0) {
      return blocked([{ code: "NO_CUSTODY_FILES", message: "Custodian file is not present for this date." }]);
    }

    // The same pending files and custodian files give the same answer; reuse it.
    const summary = await readJsonOr(bucket, reloadKey(businessDate, "summary.json"), null);
    const runs = await listRunsForDate(catalystApp, businessDate, 50);
    const reusable = runs.find(
      (r) =>
        (r.status === RUN_STATUS.COMPLETED_CLEAN || r.status === RUN_STATUS.COMPLETED_WITH_EXCEPTIONS) &&
        summary?.runId === r.runId,
    );
    if (reusable) {
      return res.status(200).json({
        success: true,
        reused: true,
        runId: reusable.runId,
        businessDate,
        uploadDate,
        status: reusable.status,
        message: `${businessDate} has already been matched with these files. Showing that report.`,
      });
    }

    const profiles = await loadFieldMapProfiles(catalystApp);
    const booked = await readBookedEquityTrades(catalystApp.zcql(), businessDate);
    // With no pending FA file the match simply checks our books against the
    // custodian files, for example after a custodian file was added or replaced.
    if (pending.length === 0 && booked.length === 0) {
      return blocked([{ code: "NO_TRANSACTION_FILE", message: "FA file is not present for this date." }]);
    }
    const pendingTrades = await loadPendingTrades(bucket, pending, businessDate, profiles[PROFILE_TYPE.FA]);
    const { trades: finalTrades, merged } = buildDateList(booked, pendingTrades);

    // The consolidated file must read back cleanly, or the match and the
    // import would both stumble on it. A booked trade with a blank date is the
    // usual reason.
    const csv = tradesToTemplateCsv(finalTrades);
    const check = await parseFaFile(Buffer.from(csv, "utf8"), { businessDate, profile: FA_CONSOLIDATED_PROFILE });
    if (!check.ok) {
      return fail(res, 409, {
        code: "CONSOLIDATED_UNREADABLE",
        message: `The date's final list cannot be read back (${check.rowErrors.length} row(s)). These are usually trades already in our books with a blank date or rate.`,
        ...summariseIssues(check.rowErrors, []),
      });
    }

    const comparison = compareWithList(finalTrades, booked, MODE.REPLACE);
    const affected = affectedOf(booked, finalTrades);
    const runId = newRunId();

    await putText(bucket, reloadKey(businessDate, "consolidated.csv"), csv, "text/csv");
    await putJson(bucket, reloadKey(businessDate, "affected.json"), affected);
    await putJson(bucket, reloadKey(businessDate, "summary.json"), {
      runId,
      businessDate,
      uploadDate,
      builtAtMs: Date.now(),
      pendingKeys: pendingKeysOf(pending),
      bookedCount: booked.length,
      finalCount: finalTrades.length,
      duplicatesMerged: merged,
      counts: comparison.counts,
      samples: comparison.samples,
      bigRemoval: isBigRemoval(comparison),
      affectedClients: affected.clients.length,
      affectedPairs: affected.pairs.length,
    });

    await createRun(catalystApp, {
      runId,
      businessDate,
      uploadDate,
      startedBy: String(req.body?.startedBy || req.headers?.["x-user"] || ""),
      faRawKey: pending[0]?.fileKey || "",
      custodyFiles: custodyFiles.map((f) => ({ scheme: f.scheme, fileType: f.fileType, key: f.fileKey })),
    });

    try {
      await catalystApp.jobScheduling().JOB.submitJob({
        job_name: `CM_${Date.now()}`.slice(0, 20),
        jobpool_name: "CustodyMatch",
        target_name: "CustodyMatchRunner",
        target_type: "Function",
        job_config: { number_of_retries: 3, retry_interval: 60 * 1000 },
        // consolidatedKey makes the runner match the final list against every
        // custodian file of the date, instead of the files of one upload date.
        params: { runId, businessDate, uploadDate, consolidatedKey: reloadKey(businessDate, "consolidated.csv") },
      });
    } catch (submitErr) {
      await failRun(catalystApp, runId, {
        errorCode: "JOB_SUBMIT_FAILED",
        errorMessage: `Could not queue the job: ${submitErr.message}`,
      });
      return fail(res, 502, { code: "JOB_SUBMIT_FAILED", runId, message: `The match could not be queued: ${submitErr.message}` });
    }

    return res.status(202).json({
      success: true,
      runId,
      businessDate,
      uploadDate,
      status: RUN_STATUS.QUEUED,
      message: "Matching started.",
    });
  } catch (error) {
    console.error("[EquityReload] startEquityRun:", error);
    return fail(res, 500, { code: error.code, message: error.message || "Could not start matching." });
  }
};

/* ──────────────────────────── consolidated files ───────────────────────── */

/**
 * GET /api/custody-match/reload/consolidated-files
 *
 * The file each date's trades were last built from: the copy of consolidated.csv
 * Pre-commit keeps, not reload/<date>/consolidated.csv, which the next match
 * overwrites before anything is loaded. A date's earlier Pre-commits are left out.
 */
export const listConsolidatedFiles = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);

    const objects = await listAllObjects(catalystApp.stratus().bucket(TXN_BUCKET), "transactions-original/TxnUpload-");
    const latestByDate = new Map();
    for (const object of objects) {
      const match = object.key.match(/\/TxnUpload-(\d+)-(Equity-(\d{4}-\d{2}-\d{2})\.csv)$/);
      if (!match) continue;
      const preCommittedAtMs = Number(match[1]);
      const date = match[3];
      if (preCommittedAtMs > (latestByDate.get(date)?.preCommittedAtMs || 0)) {
        latestByDate.set(date, {
          key: object.key,
          date,
          fileName: match[2],
          preCommittedAtMs,
          sizeBytes: object.sizeBytes,
        });
      }
    }

    const items = [...latestByDate.values()]
      .sort((a, b) => b.date.localeCompare(a.date))
      .slice(0, CONSOLIDATED_FILES_SHOWN);
    return res.json({ success: true, items });
  } catch (error) {
    console.error("[EquityReload] listConsolidatedFiles:", error);
    return fail(res, 500, { message: error.message || "Could not list the consolidated files." });
  }
};

/* ──────────────────────────── commit history ───────────────────────────── */

/** GET /api/custody-match/reload/commit-history */
export const listCommitHistory = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);

    const rows = unwrap(
      await catalystApp
        .zcql()
        .executeZCQLQuery(`SELECT * FROM ${HISTORY_TABLE} ORDER BY startedAtMs DESC LIMIT ${COMMIT_HISTORY_SHOWN}`),
      HISTORY_TABLE,
    );
    const items = rows.map((row) => ({
      id: String(row.ROWID),
      date: String(row.businessDate).slice(0, 10),
      commitType: row.commitType,
      status: row.status,
      tradesBefore: Number(row.tradesBefore) || 0,
      tradesAfter: Number(row.tradesAfter) || 0,
      clients: Number(row.clients) || 0,
      fileNames: row.fileNames || "",
      key: row.fileKey || "",
      startedAtMs: Number(row.startedAtMs) || null,
    }));
    return res.json({ success: true, items });
  } catch (error) {
    console.error("[EquityReload] listCommitHistory:", error);
    return fail(res, 500, { message: error.message || "Could not read the commit history." });
  }
};

/* ───────────────────────────── change window ───────────────────────────── */

/** Most dates the screen asks about at once (the dates of one upload date). */
const MAX_WINDOW_DATES = 60;

/** GET /api/custody-match/reload/windows?dates=2026-10-28,2026-10-29 — each date's T+N window. */
export const getDateWindows = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);
    const dates = [...new Set(String(req.query.dates || "").split(",").map((d) => d.trim()).filter(Boolean))];
    if (dates.length > MAX_WINDOW_DATES || dates.some((d) => !DATE_PATTERN.test(d))) {
      return fail(res, 400, { code: "BAD_DATES", message: `dates must be up to ${MAX_WINDOW_DATES} yyyy-mm-dd values.` });
    }
    const views = await Promise.all(dates.map(async (d) => dateWindowView(d, await readDateWindow(catalystApp, d))));
    return res.json({ success: true, windows: Object.fromEntries(views.map((v) => [v.date, v])) });
  } catch (error) {
    console.error("[EquityReload] getDateWindows:", error);
    return fail(res, 500, { message: error.message || "Could not read the change windows." });
  }
};

/**
 * PUT /api/custody-match/reload/:date/window   body: { days }
 *
 * Chooses the trade date's window. Allowed only until the date's first commit,
 * which fixes it; after that the window can never change.
 */
export const setDateWindow = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);
    const date = String(req.params.date || "").trim();
    if (!DATE_PATTERN.test(date)) return fail(res, 400, { code: "BAD_DATE", message: "Date must be yyyy-mm-dd." });
    const days = parseWindowDays(req.body?.days);
    if (!days) return fail(res, 400, { code: "BAD_WINDOW", message: "The window must be a whole number of days from 1 to 150." });

    const current = await readDateWindow(catalystApp, date);
    if (current?.fixed) {
      return fail(res, 409, {
        code: "WINDOW_FIXED",
        message: `The window of ${date} was fixed at its first commit and cannot change.`,
        window: dateWindowView(date, current),
      });
    }
    const record = { days, fixed: false };
    await saveDateWindow(catalystApp, date, record);
    return res.json({ success: true, window: dateWindowView(date, record) });
  } catch (error) {
    console.error("[EquityReload] setDateWindow:", error);
    return fail(res, 500, { message: error.message || "Could not save the window." });
  }
};

/* ──────────────────────────────── status ───────────────────────────────── */

/** What the screen shows about a Pre-commit. */
const progressView = (p) =>
  p && {
    status: p.status,
    step: p.step,
    runId: p.runId,
    startedBy: p.startedBy,
    startedAtMs: p.startedAtMs,
    firstCommit: Boolean(p.firstCommit),
    updatedAtMs: p.updatedAtMs,
    finishedAtMs: p.finishedAtMs || null,
    expectedCount: p.expectedCount,
    deleted: p.deleted,
    inserted: p.insert?.inserted ?? null,
    rebuild: p.rebuild ? { clients: p.rebuild.clients, total: p.rebuild.jobs.length, done: p.rebuild.done || 0 } : null,
    error: p.error || "",
    canRetry: canRetry(p),
  };

const canRetry = (p) =>
  Boolean(p) &&
  (p.status === STATUS.FAILED ||
    (p.status === STATUS.RUNNING &&
      p.step !== STEP.WAITING &&
      Date.now() - (p.updatedAtMs || 0) > STALLED_AFTER_MS));

/** GET /api/custody-match/reload/:date */
export const getEquityDate = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);
    const date = String(req.params.date || "").trim();
    if (!DATE_PATTERN.test(date)) return fail(res, 400, { code: "BAD_DATE", message: "Date must be yyyy-mm-dd." });

    const bucket = getBucket(catalystApp);
    const progress = await currentProgress(catalystApp, bucket, date);
    const pending = await pendingFaFiles(catalystApp, bucket, date);
    const summary = await readJsonOr(bucket, reloadKey(date, "summary.json"), null);
    const window = dateWindowView(date, await readDateWindow(catalystApp, date));

    return res.json({
      success: true,
      date,
      window,
      locked: isLocked(progress),
      progress: progressView(progress),
      pendingFiles: pending.map((f) => ({
        key: f.fileKey,
        fileName: f.originalFileName,
        mode: f.mode,
        uploadedOn: String(f.uploadedOn),
      })),
      // A summary describes the pending files it was built from; older ones say nothing now.
      summary: summary && summary.pendingKeys?.join() === pendingKeysOf(pending).join() ? summary : null,
    });
  } catch (error) {
    console.error("[EquityReload] getEquityDate:", error);
    return fail(res, 500, { message: error.message || "Could not read that date." });
  }
};

/* ─────────────────────────────── pre-commit ────────────────────────────── */

/** Is another import or Pre-commit still running? Holdings and cash jobs must not overlap. */
const busyElsewhere = async (catalystApp, date) => {
  const { running } = await findImportsStillProcessing(catalystApp);
  if (running.length > 0) {
    return `The ${running[0].what} for the import started at ${timeOfDay(running[0].startedAtMs)} is still running. Try again when it finishes.`;
  }
  const rows = unwrap(
    await catalystApp
      .zcql()
      .executeZCQLQuery("SELECT jobName FROM Jobs WHERE status = 'RUNNING' AND jobName LIKE 'RELOAD_*' LIMIT 200"),
    "Jobs",
  );
  const other = rows
    .map((r) => String(r.jobName || ""))
    .find((name) => /^RELOAD_\d{4}-\d{2}-\d{2}$/.test(name) && name !== lockJobName(date));
  return other ? `The Commit of ${other.slice(7)} is still running. Try again when it finishes.` : "";
};

/**
 * POST /api/custody-match/reload/:date/pre-commit
 * body: { runId, confirmBigRemoval, startedBy }
 */
export const preCommitEquityDate = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);
    const date = String(req.params.date || "").trim();
    if (!DATE_PATTERN.test(date)) return fail(res, 400, { code: "BAD_DATE", message: "Date must be yyyy-mm-dd." });
    const runId = String(req.body?.runId || "").trim();

    const bucket = getBucket(catalystApp);
    const progress = await currentProgress(catalystApp, bucket, date);
    if (isLocked(progress)) return fail(res, 409, { code: "DATE_LOCKED", message: lockedMessage(date, progress) });
    const closed = await closedDateResponse(res, catalystApp, date);
    if (closed) return closed;

    const busy = await busyElsewhere(catalystApp, date);
    if (busy) return fail(res, 409, { code: "IMPORT_STILL_PROCESSING", message: busy });

    const run = runId ? await getRun(catalystApp, runId) : null;
    if (!run || run.businessDate !== date) {
      return fail(res, 409, { code: "RUN_MISSING", message: "Run the match first." });
    }
    if (run.status === RUN_STATUS.SUPERSEDED) {
      return fail(res, 409, { code: "RUN_SUPERSEDED", message: "Files changed after the match. Run the match again." });
    }
    if (run.status !== RUN_STATUS.COMPLETED_CLEAN && run.status !== RUN_STATUS.COMPLETED_WITH_EXCEPTIONS) {
      return fail(res, 409, { code: "RUN_NOT_FINISHED", message: "Wait for the match to finish, then Commit." });
    }

    const summary = await readJsonOr(bucket, reloadKey(date, "summary.json"), null);
    const pending = await pendingFaFiles(catalystApp, bucket, date);
    if (!summary || summary.runId !== runId || summary.pendingKeys.join() !== pendingKeysOf(pending).join()) {
      return fail(res, 409, { code: "RUN_SUPERSEDED", message: "Files changed after the match. Run the match again." });
    }
    if (summary.pendingKeys.length === 0) {
      return fail(res, 409, {
        code: "NOTHING_TO_PRE_COMMIT",
        message: `No new FA file has been uploaded for ${date}, so there is nothing to load. The match only checked our books.`,
      });
    }
    const affected = await readJsonOr(bucket, reloadKey(date, "affected.json"), null);
    if (!affected) return fail(res, 409, { code: "CONSOLIDATED_MISSING", message: "Run the match again." });

    if (summary.bigRemoval && String(req.body?.confirmBigRemoval) !== "true") {
      return fail(res, 409, {
        code: "CONFIRM_BIG_REMOVAL",
        message: `${summary.counts.removed} of ${summary.bookedCount} trades will be removed. Are you sure?`,
        removed: summary.counts.removed,
        bookedCount: summary.bookedCount,
      });
    }

    // The first commit fixes the date's window; from now on it can never change.
    const window = await readDateWindow(catalystApp, date);
    if (!window?.fixed) await saveDateWindow(catalystApp, date, { days: window?.days || DEFAULT_WINDOW_DAYS, fixed: true });

    const startedAtMs = Date.now();
    const firstCommit = await isFirstCommit(catalystApp.zcql(), date);
    // Before the lock: without this row the commit does not start, and nothing has changed.
    const historyRow = await catalystApp.datastore().table(HISTORY_TABLE).insertRow({
      businessDate: date,
      commitType: firstCommit ? COMMIT_TYPE.FIRST : COMMIT_TYPE.RECOMMIT,
      status: STATUS.RUNNING,
      runId,
      tradesBefore: summary.bookedCount,
      tradesAfter: summary.finalCount,
      clients: affected.clients.length,
      fileNames: pending.map((f) => f.originalFileName).join(", "),
      startedAtMs,
    });
    const fresh = {
      date,
      runId,
      historyRowId: historyRow.ROWID,
      status: STATUS.RUNNING,
      step: firstCommit ? STEP.INSERT : STEP.TRANSACTION,
      firstCommit,
      startedBy: String(req.body?.startedBy || req.headers?.["x-user"] || ""),
      startedAtMs,
      expectedCount: summary.finalCount,
      deleted: {},
      insert: { attempts: 0 },
      rebuild: null,
      error: "",
    };
    await ensureJobsRow(catalystApp.zcql(), lockJobName(date), STATUS.RUNNING);
    await saveProgress(bucket, fresh);
    await notify(
      catalystApp,
      fresh,
      "in_progress",
      firstCommit ? `Adding ${summary.finalCount} trades.` : `Replacing the date's trades with ${summary.finalCount} trades.`,
    );

    void runPreCommit(catalystApp, date);
    return res.status(202).json({ success: true, date, progress: progressView(fresh), message: "Commit started." });
  } catch (error) {
    console.error("[EquityReload] preCommitEquityDate:", error);
    return fail(res, 500, { message: error.message || "Could not start the Commit." });
  }
};

/** POST /api/custody-match/reload/:date/retry — continue from the saved step. */
export const retryEquityDate = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);
    const date = String(req.params.date || "").trim();
    if (!DATE_PATTERN.test(date)) return fail(res, 400, { code: "BAD_DATE", message: "Date must be yyyy-mm-dd." });

    const bucket = getBucket(catalystApp);
    const progress = await currentProgress(catalystApp, bucket, date);
    if (!canRetry(progress) || activeReloads.has(date)) {
      return fail(res, 409, { code: "NOT_RETRYABLE", message: "This Commit is not stopped, so there is nothing to try again." });
    }

    progress.status = STATUS.RUNNING;
    progress.error = "";
    await ensureJobsRow(catalystApp.zcql(), lockJobName(date), STATUS.RUNNING);
    await updateCommitHistory(catalystApp, progress, { status: STATUS.RUNNING });

    if (progress.step === STEP.WAITING) {
      // Only the rebuilds that never reported COMPLETED are queued again.
      const statuses = await readJobStatuses(catalystApp.zcql(), progress.rebuild.jobs.map((j) => j.name));
      const unfinished = progress.rebuild.jobs.filter((j) => statuses.get(j.name) !== "COMPLETED");
      await submitRebuildJobs(catalystApp, unfinished);
      progress.rebuild.queuedAtMs = Date.now();
      await saveProgress(bucket, progress);
    } else {
      await saveProgress(bucket, progress);
      void runPreCommit(catalystApp, date);
    }
    return res.status(202).json({ success: true, date, progress: progressView(progress), message: "Continuing." });
  } catch (error) {
    console.error("[EquityReload] retryEquityDate:", error);
    return fail(res, 500, { message: error.message || "Could not continue the Commit." });
  }
};

/** Wait for a bulk write to finish. true = success, false = failed or gave up. */
const waitForBulkWrite = async (bulkWrite, jobId, onTick) => {
  const start = Date.now();
  while (Date.now() - start < BULK_MAX_WAIT_MS) {
    await sleep(BULK_POLL_MS);
    try {
      const result = await bulkWrite.getStatus(jobId);
      const status = result?.status ?? result?.data?.status ?? "";
      if (isBulkSuccessStatus(status)) return true;
      if (isBulkFailedStatus(status)) {
        console.error(`[EquityReload] bulk write ${jobId} failed:`, JSON.stringify(result));
        return false;
      }
    } catch (err) {
      console.warn(`[EquityReload] bulk status ${jobId}:`, err.message);
    }
    await onTick();
  }
  return false;
};

const submitRebuildJobs = async (catalystApp, jobs) => {
  const zcql = catalystApp.zcql();
  const scheduling = catalystApp.jobScheduling();
  for (const job of jobs) {
    await ensureJobsRow(zcql, job.name, STATUS.RUNNING);
    const holdings = job.kind === "H";
    await scheduling.JOB.submitJob({
      job_name: `${holdings ? "TRH" : "TRC"}${Date.now()}`.slice(0, 20),
      jobpool_name: holdings ? "CorporateActions" : "UpdateMasters",
      target_name: holdings ? "RebuildHoldingtable" : "Cal_CB_Per_TNX",
      target_type: "Function",
      job_config: JOB_CONFIG,
      params: {
        accountCodesJson: JSON.stringify([job.account]),
        ...(holdings ? { isinsJson: JSON.stringify(job.isins), keepCaRecords: "true" } : {}),
        source: "TxnReload",
        // The job sets this Jobs row COMPLETED when it succeeds.
        importJobRowName: job.name,
      },
    });
  }
};

/**
 * The Pre-commit itself. Reads the saved step and carries on from there, so a
 * crash, a failed bulk write or a "Try again" all take the same path. Runs after
 * the reply has gone, like the import's bulk-write poll does.
 */
const runPreCommit = async (catalystApp, date) => {
  if (activeReloads.has(date)) return;
  activeReloads.add(date);
  const bucket = getBucket(catalystApp);
  const zcql = catalystApp.zcql();
  let p = await readProgress(bucket, date);

  try {
    const affected = await getJson(bucket, reloadKey(date, "affected.json"));
    const save = (step, patch = {}) => {
      Object.assign(p, patch, { step });
      return saveProgress(bucket, p);
    };

    /* ── phase 1: clean up all five tables ── */
    if (p.step === STEP.TRANSACTION) {
      const transactions = await deleteEquityTradesOfDate(zcql, date);
      await save(STEP.HOLDINGS, { deleted: { ...p.deleted, transactions } });
    }
    if (p.step === STEP.HOLDINGS) {
      for (const [account, isin] of affected.pairs) await deleteHoldingsForPair(zcql, account, isin);
      await save(STEP.CASH, { deleted: { ...p.deleted, holdingsPairs: affected.pairs.length } });
    }
    if (p.step === STEP.CASH) {
      for (const account of affected.clients) await deletePassbook(zcql, account);
      await save(STEP.MASTERS, { deleted: { ...p.deleted, cashClients: affected.clients.length } });
    }
    if (p.step === STEP.MASTERS) {
      const consolidated = await getBuffer(bucket, reloadKey(date, "consolidated.csv"));
      const finalTrades = tradesOfParsedFile(await parseFaFile(consolidated, { businessDate: date, profile: FA_CONSOLIDATED_PROFILE }));
      const masters = await deleteUnusedStubMasters(zcql, affected, finalTrades);
      await save(STEP.INSERT, { deleted: { ...p.deleted, ...masters } });
    }

    /* ── phase 2: insert, check, rebuild ── */
    if (p.step === STEP.INSERT) {
      const bulkWrite = catalystApp.datastore().table(TXN_TABLE).bulkJob("write");
      const txnBucket = catalystApp.stratus().bucket(TXN_BUCKET);
      for (;;) {
        if (p.insert.attempts >= MAX_INSERT_ATTEMPTS) {
          throw new Error(`The trades could not be loaded after ${MAX_INSERT_ATTEMPTS} attempts.`);
        }
        p.insert.attempts++;
        // Go forward: whatever a failed or half-finished load left behind goes first.
        // On a first commit's first attempt there is nothing to remove.
        await deleteEquityTradesOfDate(zcql, date);
        await saveProgress(bucket, p);
        if (p.expectedCount === 0) {
          p.insert.inserted = 0;
          break;
        }

        const importStartedAtMs = Date.now();
        // "transactions/" is the prefix the masters job accepts.
        const objectKey = `transactions/TxnReload-${date}-${importStartedAtMs}.csv`;
        const rewritten = normalizeCsvForBulkUpload(await getBuffer(bucket, reloadKey(date, "consolidated.csv")));
        await txnBucket.putObject(objectKey, rewritten, { overwrite: true, contentType: "text/csv" });
        const job = await bulkWrite.createJob({ bucket_name: TXN_BUCKET, object_key: objectKey }, { operation: "insert" });
        const jobId = job?.job_id || job?.id;
        Object.assign(p.insert, { objectKey, bulkJobId: jobId, importStartedAtMs });
        await saveProgress(bucket, p);

        const ok = await waitForBulkWrite(bulkWrite, jobId, () => saveProgress(bucket, p));
        let actual = (await equityRowIdsOfDate(zcql, date)).length;
        if (ok && actual !== p.expectedCount) {
          await sleep(BULK_POLL_MS);
          actual = (await equityRowIdsOfDate(zcql, date)).length;
        }
        if (ok && actual === p.expectedCount) {
          p.insert.inserted = actual;
          break;
        }
        console.warn(`[EquityReload] ${date}: load ok=${ok}, ${actual} of ${p.expectedCount} trades; loading again`);
      }

      // A copy for "Recent uploads" on the screen, which lists transactions-original/.
      if (p.insert.objectKey) {
        const copyKey = `transactions-original/TxnUpload-${p.insert.importStartedAtMs}-Equity-${date}.csv`;
        await txnBucket
          .putObject(copyKey, await getBuffer(bucket, reloadKey(date, "consolidated.csv")), {
            overwrite: true,
            contentType: "text/csv",
          })
          .then(() => updateCommitHistory(catalystApp, p, { fileKey: copyKey }))
          .catch((err) => console.warn("[EquityReload] history copy failed:", err.message));
      }

      // Our books now equal the final list, so the pending files are used up. They
      // stay on the screen as the files behind the date's trades.
      await writeState(bucket, date, { files: {} });
      await setStageDetail(catalystApp, p.runId, `${IMPORTED_MARKER} — committed ${p.insert.inserted} trades`).catch(
        (err) => console.warn("[EquityReload] run mark failed:", err.message),
      );
      await save(STEP.REBUILD);
    }

    if (p.step === STEP.REBUILD) {
      if (p.insert.objectKey) {
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
      }

      const isinsByClient = new Map();
      for (const [account, isin] of affected.pairs) {
        if (!isinsByClient.has(account)) isinsByClient.set(account, []);
        isinsByClient.get(account).push(isin);
      }
      const jobs = [];
      [...isinsByClient.entries()].forEach(([account, isins], i) => {
        jobs.push({ name: rebuildJobName(date, "H", i), kind: "H", account, isins });
        jobs.push({ name: rebuildJobName(date, "C", i), kind: "C", account });
      });
      await submitRebuildJobs(catalystApp, jobs);
      await save(STEP.WAITING, { rebuild: { clients: isinsByClient.size, jobs, done: 0, queuedAtMs: Date.now() } });
    }
  } catch (error) {
    console.error(`[EquityReload] Pre-commit ${date} stopped at ${p?.step}:`, error);
    if (p) {
      p.status = STATUS.FAILED;
      p.error = `Stopped at ${String(p.step).toLowerCase()}: ${error.message}`;
      await saveProgress(bucket, p).catch(() => {});
      await updateJobsStatus(zcql, lockJobName(date), STATUS.FAILED);
      await updateCommitHistory(catalystApp, p, { status: STATUS.FAILED });
      await notify(catalystApp, p, "failed", "Commit did not finish. Press Try again.");
    }
  } finally {
    activeReloads.delete(date);
  }
};
