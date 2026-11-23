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
 *   reload/<date>/consolidated.csv   the date's final list: 26 template columns + Transaction ID + Column1–5
 *   reload/<date>/affected.json      every (client, ISIN) of the old and new trades
 *   reload/<date>/summary.json       the match's badges and the run it belongs to
 *   reload/<date>/progress.json      the Pre-commit step and counts
 * The date's lock is the Jobs row RELOAD_<date> (RUNNING until finished).
 *
 * The Mutual Funds tab uses the same engine with kind = MUTUAL_FUND (body or
 * query). It has no custodian file and no match: its Commit builds the date's
 * final list itself. It keeps its own folder (reload-mf/<date>/), lock
 * (RELOAD_MF_<date>), window and history rows, reads its files through the
 * Mutual Funds mapping, and touches only mutual fund trades (Security_Type 'm'
 * or INF + DIR). Equity touches only the other trades. See KIND below.
 *
 * OPI and OPO rows belong to the OPI/OPO tab, which commits one file at a time
 * (OpiOpoUpload.js). Neither tab here reads, deletes or re-books them
 * (bookedTradeTab), and an Equity file with an OPI or OPO row is refused.
 *
 * Plan: docs/transaction upload/transient-wibbling-eclipse.md.
 */

import { FA_FILE_TYPE, FILE_SIDE, RUN_STATUS } from "../../util/custodyMatch/constants.js";
import {
  FA_CONSOLIDATED_PROFILE,
  PROFILE_TYPE,
  resolveFieldMap,
} from "../../util/custodyMatch/fieldMap.js";
import { detectSeparator, firstLineOf, splitHeaderCells } from "../../util/custodyMatch/normalize.js";
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
  bookedTradeTab,
  EXTRA_BOOKED_COLUMNS,
  MUTUAL_FUND_BOOKED_COLUMNS,
  mutualFundTradeProblems,
  tradeFromBookedRow,
  tradeFromCells,
  tradesToMutualFundCsv,
  tradesToTemplateCsv,
  withExtraColumnValues,
  withMutualFundValues,
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
/* Both tabs share the table, so more rows are read than one tab shows. */
const COMMIT_HISTORY_READ = 200;
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

/*
 * The two tabs that share this engine. Each has its own state folder, lock,
 * window and history rows, reads its files through its own mapping and touches
 * only its own booked trades. A request names its tab with kind (body or query);
 * no kind means Equity.
 */
const KIND = {
  EQUITY: {
    key: "EQUITY",
    label: "Equity",
    folder: "reload",
    lockPrefix: "RELOAD_",
    profileType: PROFILE_TYPE.FA,
    faFileType: FA_FILE_TYPE.REGULAR,
    mutualFund: false,
    // Name part of the committed copy kept for download (transactions-original/).
    fileLabel: "Equity",
  },
  MUTUAL_FUND: {
    key: "MUTUAL_FUND",
    label: "Mutual Funds",
    folder: "reload-mf",
    lockPrefix: "RELOAD_MF_",
    profileType: PROFILE_TYPE.FA_MUTUAL_FUND,
    faFileType: FA_FILE_TYPE.MUTUAL_FUND,
    mutualFund: true,
    fileLabel: "MutualFunds",
  },
};

/**
 * Asset_Kind of the OPI/OPO tab's rows in Transaction_Commit_History (written by
 * OpiOpoUpload.js): OPI_OPO for Equity files, OPI_OPO_MF for Mutual Funds files.
 * The tab shows both in one History list.
 */
const OPI_OPO_ASSET_KINDS = ["OPI_OPO", "OPI_OPO_MF"];

/** The tab a request is for, or null when it names an unknown one. */
const kindOfRequest = (req) => {
  const value = String(req.body?.kind || req.query?.kind || "").trim().toUpperCase();
  if (!value) return KIND.EQUITY;
  return KIND[value] || null;
};

const badKind = (res) =>
  fail(res, 400, { code: "BAD_KIND", message: "kind must be EQUITY or MUTUAL_FUND." });

/** The tab's own booked trades: mutual funds for the Mutual Funds tab, the rest (apart from OPI/OPO) for Equity. */
const isOwnBookedRow = (kind, row) => bookedTradeTab(row) === kind.key;

/* Pre-commits running in this server ("<kind>:<date>"), so a double click cannot start two loops for a date. */
const activeReloads = new Set();

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const fail = (res, status, body) => res.status(status).json({ success: false, ...body });

const noApp = (res) =>
  fail(res, 500, { code: "CATALYST_NOT_INITIALIZED", message: "Catalyst not initialized" });

const reloadKey = (kind, date, name) => `${kind.folder}/${date}/${name}`;
const lockJobName = (kind, date) => `${kind.lockPrefix}${date}`;
const rebuildJobName = (kind, date, jobKind, index) => `${kind.lockPrefix}${date}_${jobKind}_${index}`;

const unwrap = (rows, table) => (rows || []).map((r) => r[table] || r);

const readJsonOr = async (bucket, key, fallback) => {
  try {
    return await getJson(bucket, key);
  } catch {
    return fallback;
  }
};

/* ───────────────────────────── pending files ───────────────────────────── */

/** Which FA files of the date are pending (uploaded in the tab since its last Pre-commit). */
const readState = (kind, bucket, date) => readJsonOr(bucket, reloadKey(kind, date, "state.json"), { files: {} });

const writeState = (kind, bucket, date, state) => putJson(bucket, reloadKey(kind, date, "state.json"), state);

/**
 * The tab's pending FA files of the date, oldest upload first, each with its mode.
 * A file row not in state.json is already in our books (an earlier Pre-commit, or
 * the old upload screen). It stays listed on the screen and is left out here.
 */
const pendingFaFiles = async (kind, catalystApp, bucket, date) => {
  const state = await readState(kind, bucket, date);
  const rows = await listFiles(catalystApp, { forDate: date });
  return rows
    .filter((r) => r.side === FILE_SIDE.FA && r.fileType === kind.faFileType && state.files[r.fileKey])
    .map((r) => ({ ...r, mode: state.files[r.fileKey].mode }));
};

const custodyFilesOfDate = async (catalystApp, date) =>
  (await listFiles(catalystApp, { forDate: date })).filter((r) => r.side === FILE_SIDE.CUSTODY);

/** FA files a Replace has overtaken: their rows and objects go, and a copy is kept for audit. */
const retireFaFiles = async (kind, catalystApp, bucket, date, files) => {
  for (const file of files) {
    const name = file.fileKey.split("/").pop();
    await putBuffer(bucket, reloadKey(kind, date, `replaced/${name}`), await getBuffer(bucket, file.fileKey));
  }
  await deleteFileRows(catalystApp, files);
  await deleteKeys(bucket, files.map((f) => f.fileKey));
};

/**
 * A parsed FA file as trades. Every trade carries Column1–5 (the extra columns
 * added on the mapping screen); a mutual fund file's trades also carry TDS,
 * Stamp Duty and TOTAL AMOUNT.
 */
const tradesOfParsedFile = (parsed, kind = KIND.EQUITY) =>
  parsed.rows.map((row) => {
    // The 26 template cells, then the Transaction ID the mapping found (column 27).
    const trade = tradeFromCells([...faRowTemplateCells(row), row.mapped?.transactionId ?? ""], {
      rowNumber: row.rowNumber,
    });
    const m = row.mapped || {};
    if (!kind.mutualFund) {
      return withExtraColumnValues(trade, [m.column1, m.column2, m.column3, m.column4, m.column5]);
    }
    return withMutualFundValues(
      trade,
      [m.tds, m.stampDuty, m.column1, m.column2, m.column3, m.column4, m.column5],
      m.totalAmount,
    );
  });

/** The trades as the file the import reads: 32 columns for Equity, 35 for mutual funds. */
const csvOfTrades = (kind, trades) => (kind.mutualFund ? tradesToMutualFundCsv(trades) : tradesToTemplateCsv(trades));

/** Re-read the tab's pending files, oldest first, as { mode, trades }. */
const loadPendingTrades = async (kind, bucket, files, date, profile) => {
  const loaded = [];
  for (const f of files) {
    const parsed = await parseFaFile(await getBuffer(bucket, f.fileKey), {
      businessDate: date,
      profile,
      type: kind.profileType,
    });
    if (!parsed.ok) {
      throw Object.assign(
        new Error(
          `The stored file ${f.originalFileName} no longer reads against the saved field mapping. Remove it and upload it again.`,
        ),
        { code: "PENDING_FILE_UNREADABLE" },
      );
    }
    loaded.push({ mode: f.mode, trades: tradesOfParsedFile(parsed, kind) });
  }
  return loaded;
};

/* ─────────────────────────────── our books ─────────────────────────────── */

/*
 * ZCQL allows at most 30 columns in one SELECT, and this list is exactly 30.
 * Each tab's extra columns are therefore read by a second query over the same
 * rows (extraSelectOf) and joined by ROWID.
 */
const BOOKED_SELECT = [
  "ROWID",
  ...new Set([...BOOKED_COLUMNS.filter(Boolean), "WS_client_id", "Security_Name", "Security_Type"]),
].join(", ");

// Mutual Funds: Net_Amount is the booked amount, which a re-commit writes back as TOTAL AMOUNT.
const extraSelectOf = (kind) =>
  (kind.mutualFund
    ? ["ROWID", ...MUTUAL_FUND_BOOKED_COLUMNS, "Net_Amount"]
    : ["ROWID", ...EXTRA_BOOKED_COLUMNS]
  ).join(", ");

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

/** The date's trades of the tab in Transaction, as trades. Paged by ROWID (OFFSET repeats rows). */
const readBookedTrades = async (kind, zcql, date) => {
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
    // The same rows' extra columns, by ROWID (see BOOKED_SELECT).
    const extrasById = new Map();
    if (batch.length > 0) {
      const lastOfBatch = String(batch[batch.length - 1].ROWID);
      const extras = unwrap(
        await selectWithRetry(
          zcql,
          `SELECT ${extraSelectOf(kind)} FROM ${TXN_TABLE} WHERE TRANDATE = '${esc(date)}' AND ROWID > ${lastRowId} AND ROWID <= ${lastOfBatch} ORDER BY ROWID ASC LIMIT ${ZCQL_PAGE}`,
        ),
        TXN_TABLE,
      );
      for (const row of extras) extrasById.set(String(row.ROWID), row);
    }
    for (const row of batch) {
      if (!isOwnBookedRow(kind, row)) continue;
      const full = { ...extrasById.get(String(row.ROWID)), ...row };
      trades.push(tradeFromBookedRow(full, { mutualFund: kind.mutualFund }));
    }
    if (batch.length < ZCQL_PAGE) break;
    lastRowId = String(batch[batch.length - 1].ROWID);
  }
  return trades;
};

/** ROWIDs of the date's trades of the tab. The other tabs' trades of the date are never touched (Tran_Type tells OPI/OPO apart). */
const rowIdsOfDate = async (kind, zcql, date) => {
  const ids = [];
  let lastRowId = "0";
  for (;;) {
    const batch = unwrap(
      await selectWithRetry(
        zcql,
        `SELECT ROWID, ISIN, EXCHG, Security_Type, Tran_Type FROM ${TXN_TABLE} WHERE TRANDATE = '${esc(date)}' AND ROWID > ${lastRowId} ORDER BY ROWID ASC LIMIT ${ZCQL_PAGE}`,
      ),
      TXN_TABLE,
    );
    for (const row of batch) {
      if (isOwnBookedRow(kind, row)) ids.push(String(row.ROWID));
    }
    if (batch.length < ZCQL_PAGE) break;
    lastRowId = String(batch[batch.length - 1].ROWID);
  }
  return ids;
};

/**
 * A date's first commit on a tab: none of the tab's trades of the date in our
 * books, so there is nothing to clean up. A date committed before and then
 * deleted counts as first again; trades booked by the old upload screen do not.
 */
const isFirstCommit = async (kind, zcql, date) => (await rowIdsOfDate(kind, zcql, date)).length === 0;

/** Repeating it is safe: a second run finds nothing left to delete. */
const deleteTradesOfDate = async (kind, zcql, date) => {
  const ids = await rowIdsOfDate(kind, zcql, date);
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
const deleteUnusedStubMasters = async (zcql, affected, keep) => {
  const keepIsins = new Set(keep.isins);
  const keepAccounts = new Set(keep.clients);
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

/** The tab a saved Pre-commit belongs to. Progress saved before the Mutual Funds tab existed is Equity's. */
const kindOfProgress = (progress) => KIND[progress?.kind] || KIND.EQUITY;

const readProgress = (kind, bucket, date) => readJsonOr(bucket, reloadKey(kind, date, "progress.json"), null);

const saveProgress = async (bucket, progress) => {
  progress.updatedAtMs = Date.now();
  await putJson(bucket, reloadKey(kindOfProgress(progress), progress.date, "progress.json"), progress);
};

/* Shared with OpiOpoUpload.js. */
export const readJobStatuses = async (zcql, names) => {
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
    jobName: `${kindOfProgress(progress).lockPrefix}${progress.date}_${progress.startedAtMs}`,
    title: `${kindOfProgress(progress).mutualFund ? "Mutual Funds commit" : "Commit"} — ${displayDate(progress.date)}`,
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
    await updateJobsStatus(catalystApp.zcql(), lockJobName(kindOfProgress(progress), progress.date), STATUS.COMPLETED);
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
    await updateJobsStatus(catalystApp.zcql(), lockJobName(kindOfProgress(progress), progress.date), STATUS.FAILED);
    await updateCommitHistory(catalystApp, progress, { status: STATUS.FAILED });
    await notify(catalystApp, progress, "failed", "Holdings and cash update did not finish. Press Try again.");
    return progress;
  }

  await saveProgress(bucket, progress);
  return progress;
};

/** The date is locked on the tab from its first Pre-commit until that has COMPLETED. */
const currentProgress = async (kind, catalystApp, bucket, date) =>
  refreshWaiting(catalystApp, bucket, await readProgress(kind, bucket, date));

const isLocked = (progress) => Boolean(progress) && progress.status !== STATUS.COMPLETED;

/**
 * A trade date past its working-day change window is locked from upload to commit
 * (file removal is refused in CustodyMatchUpload.js). Returns the refusal, or
 * null while the date is open. A Commit that started in time can still be
 * finished with Try again, which does not check this, so the books are never
 * left half done.
 */
const closedDateResponse = async (kind, res, catalystApp, date) => {
  const message = closedDateMessage(date, await readDateWindow(catalystApp, date, { mutualFund: kind.mutualFund }));
  return message ? fail(res, 409, { code: "DATE_CLOSED", message }) : null;
};

const lockedMessage = (date, progress) =>
  progress.status === STATUS.FAILED
    ? `The Commit of ${date} did not finish. Press Try again on the ${kindOfProgress(progress).label} tab before changing this date.`
    : `${date} is being rebuilt. Try again when it finishes.`;

/* ─────────────────────────────── FA upload ─────────────────────────────── */

/** The tab's current list of the date: our books plus the pending files. Uploads are compared with it. */
const currentDateList = async (kind, catalystApp, bucket, date, profile) => {
  const booked = await readBookedTrades(kind, catalystApp.zcql(), date);
  const pending = await pendingFaFiles(kind, catalystApp, bucket, date);
  const pendingTrades = await loadPendingTrades(kind, bucket, pending, date, profile);
  return { booked, pending, ...buildDateList(booked, pendingTrades) };
};

/**
 * The file's trades already in our books with the same values, saved as a CSV in
 * the tab's consolidated columns so the screen can offer it for download. It sits beside
 * the date's FA files, a folder the download route accepts. The upload goes on
 * if it cannot be saved; only the download link is missing then.
 */
const withDuplicatesFile = async (kind, bucket, date, comparison) => {
  const { duplicateTrades, ...rest } = comparison;
  if (duplicateTrades.length === 0) return { ...rest, duplicatesKey: "" };
  const key = `FA-Transaction/${date}/duplicates-${date}-${Date.now()}.csv`;
  try {
    await putText(bucket, key, csvOfTrades(kind, duplicateTrades), "text/csv");
    return { ...rest, duplicatesKey: key };
  } catch (err) {
    console.warn("[EquityReload] duplicates file not saved:", err.message);
    return { ...rest, duplicatesKey: "" };
  }
};

/**
 * A mutual fund file has at least one of the TOTAL AMOUNT, TDS and Stamp Duty
 * columns; the 26-column Equity file has none. All three are optional on the
 * mapping, so without this check an Equity file would be accepted here.
 * Returns the refusal, or null.
 */
const notMutualFundFileError = (buffer, profile) => {
  const headerLine = firstLineOf(buffer);
  const headers = splitHeaderCells(headerLine, detectSeparator(headerLine));
  const { index } = resolveFieldMap(PROFILE_TYPE.FA_MUTUAL_FUND, profile, headers);
  if (["totalAmount", "tds", "stampDuty"].some((field) => index[field] !== undefined)) return null;
  return {
    code: "NOT_A_MUTUAL_FUND_FILE",
    message:
      "This file has no TOTAL AMOUNT, TDS or Stamp Duty column, so it is not a mutual fund transaction file. Upload Equity files on the Equity tab.",
  };
};

/**
 * POST /api/custody-match/reload/fa-file
 * form-data: file, businessDate, uploadDate, [choice=ADD|REPLACE], [kind=MUTUAL_FUND]
 *
 * choice is sent only after the system asked (mode ASK came back). Nothing is
 * stored while the answer is ASK. kind = MUTUAL_FUND is the Mutual Funds tab:
 * same rules, its own mapping, checks and list of the date's trades.
 */
export const uploadEquityFaFile = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);
    const kind = kindOfRequest(req);
    if (!kind) return badKind(res);

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
    const closed = await closedDateResponse(kind, res, catalystApp, businessDate);
    if (closed) return closed;
    const uploadDay = readUploadedOn(req.body?.uploadDate);
    if (uploadDay.error) return fail(res, 400, { code: "BAD_UPLOAD_DATE", message: uploadDay.error });
    const { uploadedOn } = uploadDay;

    const profiles = await loadFieldMapProfiles(catalystApp);
    const profile = profiles[kind.profileType];

    const headerError = validateFaHeader(file.data, { profile, type: kind.profileType });
    if (headerError) return fail(res, 400, { ...headerError, fileName: file.name });

    if (kind.mutualFund) {
      const notMutualFund = notMutualFundFileError(file.data, profile);
      if (notMutualFund) return fail(res, 400, { ...notMutualFund, fileName: file.name });
    } else if ((await detectFaFileType(file.data, { profiles })) !== PROFILE_TYPE.FA) {
      return fail(res, 400, {
        code: "FA_FILE_TYPE_MISMATCH",
        message: "This file has TDS / Stamp Duty amounts, so it is a Mutual Funds transaction file. Upload it in the Mutual Funds tab.",
        fileName: file.name,
      });
    }

    const parsed = await parseFaFile(file.data, { businessDate, profile, type: kind.profileType });
    if (parsed.fileError) return fail(res, 400, { ...parsed.fileError, fileName: file.name });

    const newTrades = tradesOfParsedFile(parsed, kind);
    const rowErrors = [
      ...allRowErrors(parsed, businessDate, file.name),
      ...fileProblems(newTrades, { mutualFund: kind.mutualFund }).map((p) => ({ ...p, fileName: file.name })),
      ...(kind.mutualFund ? mutualFundTradeProblems(newTrades) : []).map((p) => ({ ...p, fileName: file.name })),
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
    const progress = await currentProgress(kind, catalystApp, bucket, businessDate);
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

    const current = await currentDateList(kind, catalystApp, bucket, businessDate, profile);
    const decided = choice
      ? { mode: choice, reason: null }
      : decideMode(newTrades, current.trades);

    // Duplicates (already in our books) are the same trades for Replace and Add.
    const markBooked = { markBooked: true };
    if (decided.mode === MODE.ASK) {
      const replace = await withDuplicatesFile(
        kind,
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
      kind,
      bucket,
      businessDate,
      compareWithList(newTrades, current.trades, decided.mode === MODE.REPLACE ? MODE.REPLACE : MODE.ADD, markBooked),
    );

    // Saved first, so nothing already there is lost if storing fails.
    const uploadedAtMs = Date.now();
    const key = buildFaRawKey({
      businessDate,
      fileType: kind.faFileType,
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
        fileType: kind.faFileType,
        forDate: businessDate,
        uploadedOn,
        originalFileName: file.name,
      });
    } catch (err) {
      await deleteKeys(bucket, [key]);
      throw new Error(`The file could not be recorded, so it was not kept: ${err.message}`);
    }

    // A Replace takes every older FA file of the tab and date off the card at once,
    // also those already in our books (their trades stay until the Pre-commit).
    const replacedFiles =
      decided.mode === MODE.REPLACE
        ? (await listFiles(catalystApp, { forDate: businessDate })).filter(
            (r) => r.side === FILE_SIDE.FA && r.fileType === kind.faFileType && r.fileKey !== key,
          )
        : [];

    const state = await readState(kind, bucket, businessDate);
    for (const old of replacedFiles) delete state.files[old.fileKey];
    state.files[key] = { mode: decided.mode, uploadedAtMs, fileName: file.name };
    await writeState(kind, bucket, businessDate, state);

    // After the state: the new file already decides the date, so a failed tidy-up
    // only leaves an older file listed.
    await retireFaFiles(kind, catalystApp, bucket, businessDate, replacedFiles).catch((err) =>
      console.warn("[EquityReload] could not remove the replaced FA files:", err.message),
    );

    // Only Equity files are matched, so a mutual fund upload leaves the date's match as it is.
    const superseded = kind.mutualFund ? { runIds: [] } : await supersedeRunsForDate(catalystApp, businessDate);

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
  const summaryKey = reloadKey(KIND.EQUITY, date, "summary.json");
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
      const closed = await closedDateResponse(KIND.EQUITY, res, catalystApp, businessDate);
      if (closed) return closed;
      const progress = await currentProgress(KIND.EQUITY, catalystApp, bucket, businessDate);
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

    const kind = KIND.EQUITY;
    const closed = await closedDateResponse(kind, res, catalystApp, businessDate);
    if (closed) return closed;

    const bucket = getBucket(catalystApp);
    const progress = await currentProgress(kind, catalystApp, bucket, businessDate);
    if (isLocked(progress)) {
      return fail(res, 409, { code: "DATE_LOCKED", message: lockedMessage(businessDate, progress) });
    }

    const pending = await pendingFaFiles(kind, catalystApp, bucket, businessDate);
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
    const summary = await readJsonOr(bucket, reloadKey(kind, businessDate, "summary.json"), null);
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
    const booked = await readBookedTrades(kind, catalystApp.zcql(), businessDate);
    // With no pending FA file the match simply checks our books against the
    // custodian files, for example after a custodian file was added or replaced.
    if (pending.length === 0 && booked.length === 0) {
      return blocked([{ code: "NO_TRANSACTION_FILE", message: "FA file is not present for this date." }]);
    }
    const pendingTrades = await loadPendingTrades(kind, bucket, pending, businessDate, profiles[PROFILE_TYPE.FA]);
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

    await putText(bucket, reloadKey(kind, businessDate, "consolidated.csv"), csv, "text/csv");
    await putJson(bucket, reloadKey(kind, businessDate, "affected.json"), affected);
    await putJson(bucket, reloadKey(kind, businessDate, "summary.json"), {
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
        params: { runId, businessDate, uploadDate, consolidatedKey: reloadKey(kind, businessDate, "consolidated.csv") },
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
 * GET /api/custody-match/reload/consolidated-files[?kind=MUTUAL_FUND]
 *
 * The file each date's trades were last built from: the copy of consolidated.csv
 * Pre-commit keeps, not reload/<date>/consolidated.csv, which the next match
 * overwrites before anything is loaded. A date's earlier Pre-commits are left out.
 */
export const listConsolidatedFiles = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);
    const kind = kindOfRequest(req);
    if (!kind) return badKind(res);

    const objects = await listAllObjects(catalystApp.stratus().bucket(TXN_BUCKET), "transactions-original/TxnUpload-");
    const namePattern = new RegExp(`/TxnUpload-(\\d+)-(${kind.fileLabel}-(\\d{4}-\\d{2}-\\d{2})\\.csv)$`);
    const latestByDate = new Map();
    for (const object of objects) {
      const match = object.key.match(namePattern);
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

/**
 * GET /api/custody-match/reload/commit-history[?kind=MUTUAL_FUND|OPI_OPO]
 *
 * Only Mutual Funds and OPI/OPO commits fill Asset_Kind; a row without it is
 * Equity's, so Equity rows read the same whether or not the column exists yet.
 */
export const listCommitHistory = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);
    // The OPI/OPO tab (OpiOpoUpload.js) writes its commits to the same table.
    const assetKind = String(req.query?.kind || KIND.EQUITY.key).trim().toUpperCase();
    if (![KIND.EQUITY.key, KIND.MUTUAL_FUND.key, "OPI_OPO"].includes(assetKind)) {
      return fail(res, 400, { code: "BAD_KIND", message: "kind must be EQUITY, MUTUAL_FUND or OPI_OPO." });
    }
    // OPI/OPO lists its Equity and Mutual Funds files' commits together.
    const wanted = assetKind === "OPI_OPO" ? OPI_OPO_ASSET_KINDS : [assetKind];

    const rows = unwrap(
      await catalystApp
        .zcql()
        .executeZCQLQuery(`SELECT * FROM ${HISTORY_TABLE} ORDER BY startedAtMs DESC LIMIT ${COMMIT_HISTORY_READ}`),
      HISTORY_TABLE,
    );
    const ownRows = rows
      .filter((row) => wanted.includes(String(row.Asset_Kind || "").trim().toUpperCase() || KIND.EQUITY.key))
      .slice(0, COMMIT_HISTORY_SHOWN);
    const items = ownRows.map((row) => ({
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

/** GET /api/custody-match/reload/windows?dates=2026-10-28,2026-10-29[&kind=MUTUAL_FUND] — each date's T+N window on the tab. */
export const getDateWindows = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);
    const kind = kindOfRequest(req);
    if (!kind) return badKind(res);
    const opts = { mutualFund: kind.mutualFund };
    const dates = [...new Set(String(req.query.dates || "").split(",").map((d) => d.trim()).filter(Boolean))];
    if (dates.length > MAX_WINDOW_DATES || dates.some((d) => !DATE_PATTERN.test(d))) {
      return fail(res, 400, { code: "BAD_DATES", message: `dates must be up to ${MAX_WINDOW_DATES} yyyy-mm-dd values.` });
    }
    const views = await Promise.all(dates.map(async (d) => dateWindowView(d, await readDateWindow(catalystApp, d, opts))));
    return res.json({ success: true, windows: Object.fromEntries(views.map((v) => [v.date, v])) });
  } catch (error) {
    console.error("[EquityReload] getDateWindows:", error);
    return fail(res, 500, { message: error.message || "Could not read the change windows." });
  }
};

/**
 * PUT /api/custody-match/reload/:date/window   body: { days, [kind] }
 *
 * Chooses the trade date's window on the tab. Allowed only until the tab's first
 * commit of the date, which fixes it; after that the window can never change.
 */
export const setDateWindow = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);
    const kind = kindOfRequest(req);
    if (!kind) return badKind(res);
    const opts = { mutualFund: kind.mutualFund };
    const date = String(req.params.date || "").trim();
    if (!DATE_PATTERN.test(date)) return fail(res, 400, { code: "BAD_DATE", message: "Date must be yyyy-mm-dd." });
    const days = parseWindowDays(req.body?.days);
    if (!days) return fail(res, 400, { code: "BAD_WINDOW", message: "The window must be a whole number of days from 1 to 150." });

    const current = await readDateWindow(catalystApp, date, opts);
    if (current?.fixed) {
      return fail(res, 409, {
        code: "WINDOW_FIXED",
        message: `The window of ${date} was fixed at its first commit and cannot change.`,
        window: dateWindowView(date, current),
      });
    }
    const record = { days, fixed: false };
    await saveDateWindow(catalystApp, date, record, opts);
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

/**
 * The date's final list for the Mutual Funds tab, built now from our books and
 * the pending files, with the counts the Commit confirmation states. There is
 * no match on this tab, so this is what Equity's match builds (startEquityRun).
 */
const buildMutualFundList = async (catalystApp, bucket, date, pending) => {
  const kind = KIND.MUTUAL_FUND;
  const profiles = await loadFieldMapProfiles(catalystApp);
  const booked = await readBookedTrades(kind, catalystApp.zcql(), date);
  const pendingTrades = await loadPendingTrades(kind, bucket, pending, date, profiles[kind.profileType]);
  const { trades: finalTrades, merged } = buildDateList(booked, pendingTrades);
  const comparison = compareWithList(finalTrades, booked, MODE.REPLACE);
  const affected = affectedOf(booked, finalTrades);
  const summary = {
    businessDate: date,
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
  };
  return { finalTrades, affected, summary };
};

/** GET /api/custody-match/reload/:date[?kind=MUTUAL_FUND] */
export const getEquityDate = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);
    const kind = kindOfRequest(req);
    if (!kind) return badKind(res);
    const date = String(req.params.date || "").trim();
    if (!DATE_PATTERN.test(date)) return fail(res, 400, { code: "BAD_DATE", message: "Date must be yyyy-mm-dd." });

    const bucket = getBucket(catalystApp);
    const progress = await currentProgress(kind, catalystApp, bucket, date);
    const pending = await pendingFaFiles(kind, catalystApp, bucket, date);
    let summary = null;
    if (!kind.mutualFund) {
      summary = await readJsonOr(bucket, reloadKey(kind, date, "summary.json"), null);
    } else if (pending.length > 0) {
      // No match on this tab: the counts are worked out from the books and files now.
      summary = (await buildMutualFundList(catalystApp, bucket, date, pending)).summary;
    }
    const window = dateWindowView(date, await readDateWindow(catalystApp, date, { mutualFund: kind.mutualFund }));

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

/**
 * Is another import or Pre-commit still running, on any tab? Holdings and cash
 * jobs must not overlap, and the tabs rebuild the same holdings and cash.
 * ownLock is the caller's own lock row, which does not count. Also used by the
 * OPI/OPO tab (OpiOpoUpload.js), whose lock is RELOAD_OPI_<fileId>.
 */
export const busyElsewhere = async (catalystApp, ownLock) => {
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
  // Commit locks only (RELOAD_<date>, RELOAD_MF_<date>, RELOAD_OPI_<fileId>); the rebuild job rows carry a suffix.
  const other = rows
    .map((r) => String(r.jobName || ""))
    .find((name) => /^RELOAD_(MF_)?\d{4}-\d{2}-\d{2}$|^RELOAD_OPI_\d+$/.test(name) && name !== ownLock);
  if (!other) return "";
  if (other.startsWith("RELOAD_OPI_")) return "An OPI/OPO Commit is still running. Try again when it finishes.";
  const tab = other.startsWith(KIND.MUTUAL_FUND.lockPrefix) ? KIND.MUTUAL_FUND : KIND.EQUITY;
  return `The ${tab.label} Commit of ${other.slice(tab.lockPrefix.length)} is still running. Try again when it finishes.`;
};

/**
 * The Equity tab's Commit needs a finished match of the files now pending; the
 * match built the date's final list. Returns { summary, affected } or the refusal.
 */
const equityListFromMatch = async (res, catalystApp, bucket, date, runId, pending) => {
  const kind = KIND.EQUITY;
  const run = runId ? await getRun(catalystApp, runId) : null;
  if (!run || run.businessDate !== date) {
    return { refused: fail(res, 409, { code: "RUN_MISSING", message: "Run the match first." }) };
  }
  if (run.status === RUN_STATUS.SUPERSEDED) {
    return { refused: fail(res, 409, { code: "RUN_SUPERSEDED", message: "Files changed after the match. Run the match again." }) };
  }
  if (run.status !== RUN_STATUS.COMPLETED_CLEAN && run.status !== RUN_STATUS.COMPLETED_WITH_EXCEPTIONS) {
    return { refused: fail(res, 409, { code: "RUN_NOT_FINISHED", message: "Wait for the match to finish, then Commit." }) };
  }

  const summary = await readJsonOr(bucket, reloadKey(kind, date, "summary.json"), null);
  if (!summary || summary.runId !== runId || summary.pendingKeys.join() !== pendingKeysOf(pending).join()) {
    return { refused: fail(res, 409, { code: "RUN_SUPERSEDED", message: "Files changed after the match. Run the match again." }) };
  }
  if (summary.pendingKeys.length === 0) {
    return {
      refused: fail(res, 409, {
        code: "NOTHING_TO_PRE_COMMIT",
        message: `No new FA file has been uploaded for ${date}, so there is nothing to load. The match only checked our books.`,
      }),
    };
  }
  const affected = await readJsonOr(bucket, reloadKey(kind, date, "affected.json"), null);
  if (!affected) return { refused: fail(res, 409, { code: "CONSOLIDATED_MISSING", message: "Run the match again." }) };
  return { summary, affected };
};

/**
 * The Mutual Funds tab has no match, so its Commit builds the date's final list
 * now from our books and the pending files, and saves it where the Pre-commit
 * steps read it. affected.keep lists the ISINs and clients of the final list, for
 * the MASTERS step (Equity's re-reads its consolidated file instead).
 */
const mutualFundListNow = async (res, catalystApp, bucket, date, pending) => {
  const kind = KIND.MUTUAL_FUND;
  if (pending.length === 0) {
    return {
      refused: fail(res, 409, {
        code: "NOTHING_TO_PRE_COMMIT",
        message: `No new mutual fund file has been uploaded for ${date}, so there is nothing to commit.`,
      }),
    };
  }
  const { finalTrades, affected, summary } = await buildMutualFundList(catalystApp, bucket, date, pending);
  const withKeep = {
    ...affected,
    keep: {
      isins: [...new Set(finalTrades.map((t) => t.isin))],
      clients: [...new Set(finalTrades.map((t) => t.account))],
    },
  };
  await putText(bucket, reloadKey(kind, date, "consolidated.csv"), tradesToMutualFundCsv(finalTrades), "text/csv");
  await putJson(bucket, reloadKey(kind, date, "affected.json"), withKeep);
  await putJson(bucket, reloadKey(kind, date, "summary.json"), summary);
  return { summary, affected: withKeep };
};

/**
 * POST /api/custody-match/reload/:date/pre-commit
 * body: { runId, confirmBigRemoval, startedBy, [kind] }
 * runId is the Equity tab's match; the Mutual Funds tab sends none.
 */
export const preCommitEquityDate = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);
    const kind = kindOfRequest(req);
    if (!kind) return badKind(res);
    const date = String(req.params.date || "").trim();
    if (!DATE_PATTERN.test(date)) return fail(res, 400, { code: "BAD_DATE", message: "Date must be yyyy-mm-dd." });
    const runId = kind.mutualFund ? "" : String(req.body?.runId || "").trim();

    const bucket = getBucket(catalystApp);
    const progress = await currentProgress(kind, catalystApp, bucket, date);
    if (isLocked(progress)) return fail(res, 409, { code: "DATE_LOCKED", message: lockedMessage(date, progress) });
    const closed = await closedDateResponse(kind, res, catalystApp, date);
    if (closed) return closed;

    const busy = await busyElsewhere(catalystApp, lockJobName(kind, date));
    if (busy) return fail(res, 409, { code: "IMPORT_STILL_PROCESSING", message: busy });

    const pending = await pendingFaFiles(kind, catalystApp, bucket, date);
    const list = kind.mutualFund
      ? await mutualFundListNow(res, catalystApp, bucket, date, pending)
      : await equityListFromMatch(res, catalystApp, bucket, date, runId, pending);
    if (list.refused) return list.refused;
    const { summary, affected } = list;

    if (summary.bigRemoval && String(req.body?.confirmBigRemoval) !== "true") {
      return fail(res, 409, {
        code: "CONFIRM_BIG_REMOVAL",
        message: `${summary.counts.removed} of ${summary.bookedCount} trades will be removed. Are you sure?`,
        removed: summary.counts.removed,
        bookedCount: summary.bookedCount,
      });
    }

    // The tab's first commit fixes the date's window on that tab; from now on it can never change.
    const windowOpts = { mutualFund: kind.mutualFund };
    const window = await readDateWindow(catalystApp, date, windowOpts);
    if (!window?.fixed) {
      await saveDateWindow(catalystApp, date, { days: window?.days || DEFAULT_WINDOW_DAYS, fixed: true }, windowOpts);
    }

    const startedAtMs = Date.now();
    const firstCommit = await isFirstCommit(kind, catalystApp.zcql(), date);
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
      // Only Mutual Funds rows fill it (OPI/OPO rows are written by OpiOpoUpload.js); a row without it is Equity's.
      ...(kind.mutualFund ? { Asset_Kind: kind.key } : {}),
    });
    const fresh = {
      kind: kind.key,
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
    await ensureJobsRow(catalystApp.zcql(), lockJobName(kind, date), STATUS.RUNNING);
    await saveProgress(bucket, fresh);
    await notify(
      catalystApp,
      fresh,
      "in_progress",
      firstCommit ? `Adding ${summary.finalCount} trades.` : `Replacing the date's trades with ${summary.finalCount} trades.`,
    );

    void runPreCommit(kind, catalystApp, date);
    return res.status(202).json({ success: true, date, progress: progressView(fresh), message: "Commit started." });
  } catch (error) {
    console.error("[EquityReload] preCommitEquityDate:", error);
    return fail(res, 500, { code: error.code, message: error.message || "Could not start the Commit." });
  }
};

/** POST /api/custody-match/reload/:date/retry   body: { [kind] } — continue from the saved step. */
export const retryEquityDate = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) return noApp(res);
    const kind = kindOfRequest(req);
    if (!kind) return badKind(res);
    const date = String(req.params.date || "").trim();
    if (!DATE_PATTERN.test(date)) return fail(res, 400, { code: "BAD_DATE", message: "Date must be yyyy-mm-dd." });

    const bucket = getBucket(catalystApp);
    const progress = await currentProgress(kind, catalystApp, bucket, date);
    if (!canRetry(progress) || activeReloads.has(`${kind.key}:${date}`)) {
      return fail(res, 409, { code: "NOT_RETRYABLE", message: "This Commit is not stopped, so there is nothing to try again." });
    }

    progress.status = STATUS.RUNNING;
    progress.error = "";
    await ensureJobsRow(catalystApp.zcql(), lockJobName(kind, date), STATUS.RUNNING);
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
      void runPreCommit(kind, catalystApp, date);
    }
    return res.status(202).json({ success: true, date, progress: progressView(progress), message: "Continuing." });
  } catch (error) {
    console.error("[EquityReload] retryEquityDate:", error);
    return fail(res, 500, { message: error.message || "Could not continue the Commit." });
  }
};

/*
 * A status read that keeps failing (seen: "invalid oauth token" once the
 * Commit request's token expired) would otherwise be retried for the whole
 * BULK_MAX_WAIT_MS. After this many failures in a row the wait gives up; the
 * caller then counts the rows itself, or stops so Try again can continue with
 * a fresh token.
 */
const BULK_STATUS_MAX_ERRORS = 4;

/** Wait for a bulk write to finish. true = success, false = failed or gave up. Shared with OpiOpoUpload.js. */
export const waitForBulkWrite = async (bulkWrite, jobId, onTick) => {
  const start = Date.now();
  let errorsInARow = 0;
  while (Date.now() - start < BULK_MAX_WAIT_MS) {
    await sleep(BULK_POLL_MS);
    try {
      const result = await bulkWrite.getStatus(jobId);
      errorsInARow = 0;
      const status = result?.status ?? result?.data?.status ?? "";
      if (isBulkSuccessStatus(status)) return true;
      if (isBulkFailedStatus(status)) {
        console.error(`[EquityReload] bulk write ${jobId} failed:`, JSON.stringify(result));
        return false;
      }
    } catch (err) {
      errorsInARow++;
      console.warn(`[EquityReload] bulk status ${jobId} (${errorsInARow} of ${BULK_STATUS_MAX_ERRORS}):`, err.message);
      if (errorsInARow >= BULK_STATUS_MAX_ERRORS) return false;
    }
    await onTick();
  }
  return false;
};

/* Shared with OpiOpoUpload.js, which queues holdings jobs (kind "H") only. */
export const submitRebuildJobs = async (catalystApp, jobs) => {
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
const runPreCommit = async (kind, catalystApp, date) => {
  const activeKey = `${kind.key}:${date}`;
  if (activeReloads.has(activeKey)) return;
  activeReloads.add(activeKey);
  const bucket = getBucket(catalystApp);
  const zcql = catalystApp.zcql();
  let p = await readProgress(kind, bucket, date);

  try {
    const affected = await getJson(bucket, reloadKey(kind, date, "affected.json"));
    const save = (step, patch = {}) => {
      Object.assign(p, patch, { step });
      return saveProgress(bucket, p);
    };

    /* ── phase 1: clean up all five tables ── */
    if (p.step === STEP.TRANSACTION) {
      const transactions = await deleteTradesOfDate(kind, zcql, date);
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
      // The Mutual Funds Commit saved the final list's ISINs and clients; Equity's are read from its file.
      let keep = affected.keep;
      if (!keep) {
        const consolidated = await getBuffer(bucket, reloadKey(kind, date, "consolidated.csv"));
        const finalTrades = tradesOfParsedFile(await parseFaFile(consolidated, { businessDate: date, profile: FA_CONSOLIDATED_PROFILE }));
        keep = { isins: finalTrades.map((t) => t.isin), clients: finalTrades.map((t) => t.account) };
      }
      const masters = await deleteUnusedStubMasters(zcql, affected, keep);
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
        await deleteTradesOfDate(kind, zcql, date);
        await saveProgress(bucket, p);
        if (p.expectedCount === 0) {
          p.insert.inserted = 0;
          break;
        }

        const importStartedAtMs = Date.now();
        // "transactions/" is the prefix the masters job accepts.
        const objectKey = `transactions/TxnReload-${date}-${importStartedAtMs}.csv`;
        // A mutual fund row gets its amount worked out and is marked Security_Type 'm' here.
        const rewritten = normalizeCsvForBulkUpload(await getBuffer(bucket, reloadKey(kind, date, "consolidated.csv")), {
          mutualFund: kind.mutualFund,
        });
        await txnBucket.putObject(objectKey, rewritten, { overwrite: true, contentType: "text/csv" });
        const job = await bulkWrite.createJob({ bucket_name: TXN_BUCKET, object_key: objectKey }, { operation: "insert" });
        const jobId = job?.job_id || job?.id;
        Object.assign(p.insert, { objectKey, bulkJobId: jobId, importStartedAtMs });
        await saveProgress(bucket, p);

        const ok = await waitForBulkWrite(bulkWrite, jobId, () => saveProgress(bucket, p));
        let actual = (await rowIdsOfDate(kind, zcql, date)).length;
        if (actual !== p.expectedCount) {
          await sleep(BULK_POLL_MS);
          actual = (await rowIdsOfDate(kind, zcql, date)).length;
        }
        // Every trade is in the table, even when the job's status could not be read.
        if (actual === p.expectedCount) {
          if (!ok) console.warn(`[EquityReload] ${date}: load status unknown, but all ${actual} trades are in; going on`);
          p.insert.inserted = actual;
          break;
        }
        console.warn(`[EquityReload] ${date}: load ok=${ok}, ${actual} of ${p.expectedCount} trades; loading again`);
      }

      // A copy for "Recent uploads" on the screen, which lists transactions-original/.
      if (p.insert.objectKey) {
        const copyKey = `transactions-original/TxnUpload-${p.insert.importStartedAtMs}-${kind.fileLabel}-${date}.csv`;
        await txnBucket
          .putObject(copyKey, await getBuffer(bucket, reloadKey(kind, date, "consolidated.csv")), {
            overwrite: true,
            contentType: "text/csv",
          })
          .then(() => updateCommitHistory(catalystApp, p, { fileKey: copyKey }))
          .catch((err) => console.warn("[EquityReload] history copy failed:", err.message));
      }

      // Our books now equal the final list, so the pending files are used up. They
      // stay on the screen as the files behind the date's trades.
      await writeState(kind, bucket, date, { files: {} });
      if (p.runId) {
        await setStageDetail(catalystApp, p.runId, `${IMPORTED_MARKER} — committed ${p.insert.inserted} trades`).catch(
          (err) => console.warn("[EquityReload] run mark failed:", err.message),
        );
      }
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
        jobs.push({ name: rebuildJobName(kind, date, "H", i), kind: "H", account, isins });
        jobs.push({ name: rebuildJobName(kind, date, "C", i), kind: "C", account });
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
      await updateJobsStatus(zcql, lockJobName(kind, date), STATUS.FAILED);
      await updateCommitHistory(catalystApp, p, { status: STATUS.FAILED });
      await notify(catalystApp, p, "failed", "Commit did not finish. Press Try again.");
    }
  } finally {
    activeReloads.delete(activeKey);
  }
};
