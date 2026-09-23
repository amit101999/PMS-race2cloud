/**
 * Custody Matching — Data Store access for Custody_Match_Run / Custody_Match_Exception.
 *
 * Three Catalyst constraints drive the shape of this file:
 *
 *   1. ZCQL returns at most 300 rows per query. It does not error or paginate for
 *      you — it silently returns 300. Every read here loops on OFFSET.
 *   2. ZCQL allows at most 20 columns per SELECT. Custody_Match_Run has 23, so it
 *      is read as two queries merged on ROWID — the same split DashboardController.js
 *      already uses on the Transaction table.
 *   3. insertRows accepts at most 200 rows per call, so exceptions are chunked.
 *
 * Takes catalystApp as a parameter, so the same file serves AppSail and the job
 * function. Inside the job function that app MUST be
 * catalyst.initialize(context, { scope: 'admin' }) or every call here stalls ~60s
 * and the job dies on the 15-minute timeout with nothing useful in the logs.
 */

import { EXCEPTION_STATUS, EXCEPTION_TYPE, RUN_STATUS, STAGE } from "./constants.js";

export const TABLE = {
  RUN: "Custody_Match_Run",
  EXCEPTION: "Custody_Match_Exception",
};

/** ZCQL page size. 300 is the hard server cap, not a tuning choice. */
const ZCQL_PAGE = 300;

/** insertRows hard cap. */
const INSERT_CHUNK = 200;

/** Catalyst `text` columns hold 10,000 characters. */
const TEXT_MAX = 10000;

/** Single quotes are the only string delimiter in ZCQL, so doubling them is the escape. */
const esc = (s) => String(s ?? "").replace(/'/g, "''");

/** Never interpolate an unvalidated value where a number belongs. */
const int = (v, fallback = 0) => {
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : fallback;
};

const dbl = (v, fallback = 0) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
};

const clip = (v, max = TEXT_MAX) => {
  const s = String(v ?? "");
  return s.length > max ? `${s.slice(0, max - 3)}...` : s;
};

/** ZCQL wraps every row under its table name. Forgetting this is the classic silent bug. */
const unwrap = (result, tableName) =>
  (result || []).map((r) => r?.[tableName] ?? r).filter(Boolean);

/**
 * Run a SELECT across as many pages as it takes.
 *
 * @param {(limit:number, offset:number)=>string} buildSql
 */
const selectAll = async (zcql, tableName, buildSql, { maxPages = 200 } = {}) => {
  const rows = [];
  // ZCQL's OFFSET is 1-based, so each page repeats the previous page's last
  // row; skip it by ROWID (every caller selects ROWID).
  const seenRowIds = new Set();
  for (let page = 0; page < maxPages; page++) {
    const offset = page * ZCQL_PAGE;
    const result = await zcql.executeZCQLQuery(buildSql(ZCQL_PAGE, offset));
    const batch = unwrap(result, tableName);
    for (const row of batch) {
      if (row.ROWID != null) {
        if (seenRowIds.has(row.ROWID)) continue;
        seenRowIds.add(row.ROWID);
      }
      rows.push(row);
    }
    if (batch.length < ZCQL_PAGE) break;
  }
  return rows;
};

// ============================================================ Custody_Match_Run

/**
 * The run table has 23 columns and ZCQL allows 20 per SELECT, so reads are split
 * into two groups and merged on ROWID.
 */
const RUN_COLS_A = [
  "ROWID",
  "runId",
  "businessDate",
  "status",
  "stage",
  "stageDetail",
  "faRawKey",
  "faFinalKey",
  "custodyFinalKey",
  "differenceKey",
  "custodyFileList",
  "startedBy",
];

const RUN_COLS_B = [
  "ROWID",
  "faRowCount",
  "custodyRowCount",
  "resolvedCount",
  "unmappedCount",
  "totalKeyCount",
  "comparedKeyCount",
  "exceptionCount",
  "errorCode",
  "errorMessage",
  "attemptCount",
  "startedAtMs",
  "finishedAtMs",
];

const NUMERIC_RUN_FIELDS = new Set([
  "faRowCount",
  "custodyRowCount",
  "resolvedCount",
  "unmappedCount",
  "totalKeyCount",
  "comparedKeyCount",
  "exceptionCount",
  "attemptCount",
  "startedAtMs",
  "finishedAtMs",
]);

/** Coerce the strings ZCQL hands back into the types callers expect. */
const shapeRun = (row) => {
  if (!row) return null;
  const out = { ...row };
  for (const field of NUMERIC_RUN_FIELDS) {
    if (field in out) out[field] = out[field] == null ? null : Number(out[field]);
  }
  if ("custodyFileList" in out) {
    try {
      out.custodyFiles = out.custodyFileList ? JSON.parse(out.custodyFileList) : [];
    } catch {
      out.custodyFiles = [];
    }
  }
  return out;
};

/** Merge the two column groups by ROWID. */
const mergeRunGroups = (groupA, groupB) => {
  const byRowId = new Map(groupB.map((r) => [String(r.ROWID), r]));
  return groupA.map((a) => shapeRun({ ...a, ...(byRowId.get(String(a.ROWID)) || {}) }));
};

const runSelect = (cols, where, order = "", limit = ZCQL_PAGE, offset = 0) =>
  `SELECT ${cols.join(", ")} FROM ${TABLE.RUN}${where ? ` WHERE ${where}` : ""}${
    order ? ` ORDER BY ${order}` : ""
  } LIMIT ${int(limit, ZCQL_PAGE)} OFFSET ${int(offset, 0)}`;

const fetchRunsWhere = async (zcql, where, order = "ROWID DESC", limit = ZCQL_PAGE) => {
  const groupA = unwrap(
    await zcql.executeZCQLQuery(runSelect(RUN_COLS_A, where, order, limit, 0)),
    TABLE.RUN,
  );
  if (groupA.length === 0) return [];

  // ROWID is a bigint, so the IN list is unquoted — same as DashboardController.js:100.
  // Digits-only filtering is what keeps an unquoted interpolation safe.
  const rowIds = groupA
    .map((r) => String(r.ROWID))
    .filter((id) => /^\d+$/.test(id))
    .join(",");
  if (!rowIds) return groupA.map(shapeRun);

  const groupB = unwrap(
    await zcql.executeZCQLQuery(
      `SELECT ${RUN_COLS_B.join(", ")} FROM ${TABLE.RUN} WHERE ROWID IN (${rowIds}) LIMIT ${ZCQL_PAGE}`,
    ),
    TABLE.RUN,
  );

  return mergeRunGroups(groupA, groupB);
};

/** @returns {Promise<object|null>} */
export const getRun = async (catalystApp, runId) => {
  const rows = await fetchRunsWhere(catalystApp.zcql(), `runId = '${esc(runId)}'`, "ROWID DESC", 1);
  return rows[0] || null;
};

/** Latest run for a business date — what the screen shows on reload. */
export const getLatestRunForDate = async (catalystApp, businessDate) => {
  const rows = await fetchRunsWhere(
    catalystApp.zcql(),
    `businessDate = '${esc(businessDate)}'`,
    "ROWID DESC",
    1,
  );
  return rows[0] || null;
};

export const listRunsForDate = async (catalystApp, businessDate, limit = 20) =>
  fetchRunsWhere(
    catalystApp.zcql(),
    `businessDate = '${esc(businessDate)}'`,
    "ROWID DESC",
    limit,
  );

/**
 * Finished runs that found differences, newest first — the report history.
 *
 * Only runs with something to show: a clean run produces no report worth
 * revisiting, and a queued or running one has no answer yet. Not scoped to a
 * date, because the point of the list is to look back across days.
 */
/**
 * Runs that produced a report, newest first.
 *
 * SUPERSEDED runs are included on purpose. Superseding says a result may no
 * longer be IMPORTED — the files behind it may have moved on — but the report
 * itself is history, and it was true of the files it ran against. Excluding
 * them made a date matched in two passes (the 20th, then the 21st) show only
 * the second: starting the second run superseded the first, and its report
 * disappeared from the list even though the file was still there.
 */
export const listRunsWithExceptions = async (catalystApp, limit = 20) =>
  fetchRunsWhere(
    catalystApp.zcql(),
    `exceptionCount > 0 AND (status = '${esc(RUN_STATUS.COMPLETED_WITH_EXCEPTIONS)}' ` +
      `OR status = '${esc(RUN_STATUS.SUPERSEDED)}')`,
    "ROWID DESC",
    limit,
  );

/**
 * The run that already imported this date, if any.
 *
 * The import takes the whole FA file for the date, so it must happen once per
 * date — not once per run. With several runs over one date (the 20th, then the
 * 21st) a per-run guard alone would let the same file in twice.
 */
export const findImportedRunForDate = async (catalystApp, businessDate, marker) => {
  const runs = await listRunsForDate(catalystApp, businessDate, ZCQL_PAGE);
  return runs.find((r) => String(r.stageDetail || "").startsWith(marker)) || null;
};

/** ROWID for a runId — every update needs it, and runId alone will not do. */
const findRunRowId = async (catalystApp, runId) => {
  const rows = unwrap(
    await catalystApp
      .zcql()
      .executeZCQLQuery(
        `SELECT ROWID FROM ${TABLE.RUN} WHERE runId = '${esc(runId)}' LIMIT 1`,
      ),
    TABLE.RUN,
  );
  return rows[0]?.ROWID || null;
};

/** `CM-<epochms>` — sorts chronologically and is unique enough for one screen. */
export const newRunId = () => `CM-${Date.now()}`;

export const createRun = async (
  catalystApp,
  { runId = newRunId(), businessDate, startedBy = "", faRawKey = "", custodyFiles = [] },
) => {
  const table = catalystApp.datastore().table(TABLE.RUN);
  const now = Date.now();

  const row = await table.insertRow({
    runId,
    businessDate,
    status: RUN_STATUS.QUEUED,
    stage: STAGE.VALIDATE,
    stageDetail: "Queued",
    faRawKey,
    faFinalKey: "",
    custodyFinalKey: "",
    differenceKey: "",
    custodyFileList: clip(JSON.stringify(custodyFiles || [])),
    faRowCount: 0,
    custodyRowCount: 0,
    resolvedCount: 0,
    unmappedCount: 0,
    totalKeyCount: 0,
    comparedKeyCount: 0,
    exceptionCount: 0,
    errorCode: "",
    errorMessage: "",
    attemptCount: 0,
    startedAtMs: now,
    finishedAtMs: 0,
    startedBy,
  });

  return { ...shapeRun(row), runId, ROWID: row?.ROWID };
};

/** Patch a run by runId. Unknown keys are dropped rather than sent to the SDK. */
const updateRun = async (catalystApp, runId, patch) => {
  const rowId = await findRunRowId(catalystApp, runId);
  if (!rowId) throw new Error(`No ${TABLE.RUN} row for runId "${runId}".`);

  const allowed = new Set([...RUN_COLS_A, ...RUN_COLS_B].filter((c) => c !== "ROWID"));
  const clean = { ROWID: rowId };
  for (const [k, v] of Object.entries(patch)) {
    if (!allowed.has(k)) continue;
    if (NUMERIC_RUN_FIELDS.has(k)) clean[k] = int(v, 0);
    else if (k === "errorMessage" || k === "custodyFileList") clean[k] = clip(v);
    else clean[k] = v == null ? "" : String(v);
  }

  const updated = await catalystApp.datastore().table(TABLE.RUN).updateRow(clean);
  return shapeRun(updated);
};

/**
 * Move the run to a stage. Written BEFORE the stage's work starts, so a crash
 * leaves `stage` pointing at the step that needs re-running, not the one that
 * finished.
 */
export const setStage = (catalystApp, runId, stage, stageDetail = "") =>
  updateRun(catalystApp, runId, {
    stage,
    stageDetail,
    status: RUN_STATUS.RUNNING,
  });

export const setStageDetail = (catalystApp, runId, stageDetail) =>
  updateRun(catalystApp, runId, { stageDetail });

export const setObjectKeys = (catalystApp, runId, keys) =>
  updateRun(catalystApp, runId, keys);

export const setCounts = (catalystApp, runId, counts) =>
  updateRun(catalystApp, runId, counts);

/** The S4 resume checkpoint. */
export const checkpointCompare = (catalystApp, runId, comparedKeyCount, totalKeyCount) =>
  updateRun(catalystApp, runId, {
    comparedKeyCount,
    totalKeyCount,
    stage: STAGE.COMPARE,
    status: RUN_STATUS.RUNNING,
    stageDetail: `Compared ${int(comparedKeyCount)} of ${int(totalKeyCount)} keys`,
  });

export const incrementAttempt = async (catalystApp, runId) => {
  const run = await getRun(catalystApp, runId);
  if (!run) throw new Error(`No ${TABLE.RUN} row for runId "${runId}".`);
  return updateRun(catalystApp, runId, { attemptCount: int(run.attemptCount, 0) + 1 });
};

/**
 * Terminal success. Zero exceptions is COMPLETED_CLEAN — the only status that
 * opens the upload gate. Anything else is COMPLETED_WITH_EXCEPTIONS, which is a
 * successful run reporting a real disagreement, NOT a failure.
 */
export const finishRun = (catalystApp, runId, { exceptionCount = 0, ...counts } = {}) =>
  updateRun(catalystApp, runId, {
    ...counts,
    exceptionCount,
    stage: STAGE.DONE,
    status:
      int(exceptionCount, 0) === 0
        ? RUN_STATUS.COMPLETED_CLEAN
        : RUN_STATUS.COMPLETED_WITH_EXCEPTIONS,
    stageDetail:
      int(exceptionCount, 0) === 0
        ? "Matched — no differences"
        : `${int(exceptionCount)} exception(s) found`,
    finishedAtMs: Date.now(),
  });

/** System failure — a timeout or crash, not a data difference. */
export const failRun = (catalystApp, runId, { errorCode = "UNKNOWN", errorMessage = "" }) =>
  updateRun(catalystApp, runId, {
    status: RUN_STATUS.FAILED,
    errorCode,
    errorMessage,
    stageDetail: `Failed: ${clip(errorMessage, 200)}`,
    finishedAtMs: Date.now(),
  });

/**
 * Retire every open run for a date.
 *
 * Called when files for that date are replaced: without it, a clean result from
 * before the swap would still be sitting there able to open the upload gate for
 * files it never saw.
 */
/**
 * Marks every finished run for a date as no longer usable.
 *
 * Called ONLY when the inputs change — a file uploaded, removed, or a mapping
 * saved. It is deliberately not called when a new run starts: starting a match
 * changes no file, so an earlier run's answer about the dates IT compared is
 * still true. Conflating the two meant a date matched in two passes lost the
 * first report, and left no way to tell a stale run from a merely older one.
 */
export const supersedeRunsForDate = async (catalystApp, businessDate) => {
  const runs = await listRunsForDate(catalystApp, businessDate, ZCQL_PAGE);
  const open = runs.filter(
    (r) =>
      r.status === RUN_STATUS.QUEUED ||
      r.status === RUN_STATUS.RUNNING ||
      r.status === RUN_STATUS.COMPLETED_CLEAN ||
      r.status === RUN_STATUS.COMPLETED_WITH_EXCEPTIONS,
  );
  if (open.length === 0) return { superseded: 0, runIds: [] };

  await catalystApp
    .datastore()
    .table(TABLE.RUN)
    .updateRows(
      open.map((r) => ({
        ROWID: r.ROWID,
        status: RUN_STATUS.SUPERSEDED,
        stageDetail: "Superseded — source files were replaced",
      })),
    );

  return { superseded: open.length, runIds: open.map((r) => r.runId) };
};

/** Only a clean run opens Gate 2. Re-checked server-side; the disabled button is a courtesy. */
export const isGateOpen = (run) =>
  Boolean(
    run &&
      run.status === RUN_STATUS.COMPLETED_CLEAN &&
      int(run.exceptionCount, -1) === 0,
  );

// ====================================================== Custody_Match_Exception

const EXCEPTION_COLS = [
  "ROWID",
  "runId",
  "businessDate",
  "actualCode",
  "isin",
  "tranDate",
  "tranType",
  "virtualCodes",
  "scheme",
  "faQuantity",
  "custodyQuantity",
  "difference",
  "exceptionType",
  "status",
  "faRef",
  "custodyRef",
  "sortKey",
];

/**
 * Project a comparison result onto the table's columns.
 *
 * compare.js carries extra reference fields (source row numbers, rate, brokerage,
 * settlement date) for the difference CSV. Those belong in the file, not in the
 * table — the grid never filters or sorts on them.
 */
export const toExceptionRow = (ex, runId, businessDate) => ({
  runId,
  businessDate,
  actualCode: String(ex.actualCode || "").slice(0, 64),
  isin: String(ex.isin || "").slice(0, 12),
  tranDate: String(ex.tranDate || "").slice(0, 10),
  tranType: String(ex.tranType || "").slice(0, 8),
  virtualCodes: String(ex.virtualCodes || "").slice(0, 255),
  scheme: String(ex.scheme || "").slice(0, 128),
  faQuantity: dbl(ex.faQuantity, 0),
  custodyQuantity: dbl(ex.custodyQuantity, 0),
  difference: dbl(ex.difference, 0),
  exceptionType: String(ex.exceptionType || "").slice(0, 32),
  status: String(ex.status || EXCEPTION_STATUS.OPEN).slice(0, 16),
  faRef: String(ex.faRef || "").slice(0, 255),
  custodyRef: String(ex.custodyRef || "").slice(0, 255),
  sortKey: String(ex.sortKey || "").slice(0, 160),
});

/**
 * Remove this run's exceptions.
 *
 * Called before S5 writes. Catalyst auto-retries job functions, so without this a
 * single transient timeout in S5 produces a run reporting 940 exceptions where
 * there were 470 — every one of them a duplicate.
 */
export const clearExceptions = async (catalystApp, runId) => {
  const zcql = catalystApp.zcql();
  const table = catalystApp.datastore().table(TABLE.EXCEPTION);
  let removed = 0;

  // Always read the first page: rows are deleted as we go, so the window slides
  // down to us rather than us walking past it with an OFFSET.
  for (let guard = 0; guard < 500; guard++) {
    const rows = unwrap(
      await zcql.executeZCQLQuery(
        `SELECT ROWID FROM ${TABLE.EXCEPTION} WHERE runId = '${esc(runId)}' LIMIT ${ZCQL_PAGE}`,
      ),
      TABLE.EXCEPTION,
    );
    if (rows.length === 0) break;
    await table.deleteRows(rows.map((r) => r.ROWID));
    removed += rows.length;
    if (rows.length < ZCQL_PAGE) break;
  }

  return { removed };
};

/**
 * Write exceptions in chunks of 200 (the insertRows cap).
 *
 * One call per row would do to S5 exactly what an N+1 lookup does to S1.
 */
export const insertExceptions = async (
  catalystApp,
  runId,
  businessDate,
  exceptions,
  { onProgress = null } = {},
) => {
  if (!exceptions || exceptions.length === 0) return { inserted: 0 };

  const table = catalystApp.datastore().table(TABLE.EXCEPTION);
  const rows = exceptions.map((ex) => toExceptionRow(ex, runId, businessDate));
  let inserted = 0;

  for (let i = 0; i < rows.length; i += INSERT_CHUNK) {
    const chunk = rows.slice(i, i + INSERT_CHUNK);
    await table.insertRows(chunk);
    inserted += chunk.length;
    if (onProgress) onProgress(inserted, rows.length);
  }

  return { inserted };
};

/** Build the WHERE clause for the exception grid. Every value is escaped. */
const buildExceptionWhere = (runId, filters = {}) => {
  const clauses = [`runId = '${esc(runId)}'`];

  const { exceptionType, scheme, status, q, from, to } = filters;

  if (exceptionType && EXCEPTION_TYPE[exceptionType]) {
    clauses.push(`exceptionType = '${esc(exceptionType)}'`);
  }
  if (scheme) clauses.push(`scheme = '${esc(scheme)}'`);
  if (status && EXCEPTION_STATUS[status]) clauses.push(`status = '${esc(status)}'`);
  if (from) clauses.push(`tranDate >= '${esc(from)}'`);
  if (to) clauses.push(`tranDate <= '${esc(to)}'`);
  if (q) {
    const term = esc(String(q).trim());
    if (term) {
      clauses.push(
        `(actualCode LIKE '%${term}%' OR scheme LIKE '%${term}%' OR isin LIKE '%${term}%' OR virtualCodes LIKE '%${term}%')`,
      );
    }
  }

  return clauses.join(" AND ");
};

export const countExceptions = async (catalystApp, runId, filters = {}) => {
  const where = buildExceptionWhere(runId, filters);
  const rows = unwrap(
    await catalystApp
      .zcql()
      .executeZCQLQuery(`SELECT COUNT(ROWID) AS total FROM ${TABLE.EXCEPTION} WHERE ${where}`),
    TABLE.EXCEPTION,
  );
  return int(rows[0]?.total, 0);
};

/**
 * One page of the grid. Ten rows per response by default.
 *
 * Ordered by the precomputed sortKey rather than four separate columns, so paging
 * is stable and the database is not sorting on a composite every request.
 */
export const pageExceptions = async (
  catalystApp,
  runId,
  { page = 1, size = 10, ...filters } = {},
) => {
  const safeSize = Math.min(Math.max(int(size, 10), 1), 100);
  const safePage = Math.max(int(page, 1), 1);
  const offset = (safePage - 1) * safeSize;
  const where = buildExceptionWhere(runId, filters);

  const [rows, total] = await Promise.all([
    catalystApp
      .zcql()
      .executeZCQLQuery(
        `SELECT ${EXCEPTION_COLS.join(", ")} FROM ${TABLE.EXCEPTION} WHERE ${where} ORDER BY sortKey ASC, ROWID ASC LIMIT ${safeSize} OFFSET ${offset}`,
      )
      .then((r) => unwrap(r, TABLE.EXCEPTION)),
    countExceptions(catalystApp, runId, filters),
  ]);

  return {
    rows: rows.map((r) => ({
      ...r,
      faQuantity: Number(r.faQuantity),
      custodyQuantity: Number(r.custodyQuantity),
      difference: Number(r.difference),
    })),
    page: safePage,
    size: safeSize,
    total,
    totalPages: Math.max(Math.ceil(total / safeSize), 1),
    hasMore: offset + rows.length < total,
  };
};

/** Every exception for a run, paged past the 300 cap. Used to rebuild the difference CSV. */
export const allExceptions = (catalystApp, runId, filters = {}) => {
  const where = buildExceptionWhere(runId, filters);
  return selectAll(
    catalystApp.zcql(),
    TABLE.EXCEPTION,
    (limit, offset) =>
      `SELECT ${EXCEPTION_COLS.join(", ")} FROM ${TABLE.EXCEPTION} WHERE ${where} ORDER BY sortKey ASC, ROWID ASC LIMIT ${limit} OFFSET ${offset}`,
  );
};

/** Counts per exception type — the breakdown under the result banner. */
export const exceptionTypeCounts = async (catalystApp, runId) => {
  const rows = unwrap(
    await catalystApp
      .zcql()
      .executeZCQLQuery(
        `SELECT exceptionType, COUNT(ROWID) AS total FROM ${TABLE.EXCEPTION} WHERE runId = '${esc(runId)}' GROUP BY exceptionType`,
      ),
    TABLE.EXCEPTION,
  );

  const counts = {};
  for (const r of rows) counts[r.exceptionType] = int(r.total, 0);
  return counts;
};


export const setExceptionStatus = async (catalystApp, rowIds, status) => {
  const valid = EXCEPTION_STATUS[status] ? status : EXCEPTION_STATUS.OPEN;
  const ids = (Array.isArray(rowIds) ? rowIds : [rowIds]).filter(Boolean);
  if (ids.length === 0) return { updated: 0 };

  await catalystApp
    .datastore()
    .table(TABLE.EXCEPTION)
    .updateRows(ids.map((ROWID) => ({ ROWID, status: valid })));

  return { updated: ids.length };
};

/**
 * Used by the cleanup cron alongside the bucket sweep.
 *
 * `skipDates` must carry every date whose FILES were preserved. Deleting the run
 * row for a preserved date is worse than useless: it removes the very record that
 * caused the files to be kept, so the next sweep deletes them as orphans — and if
 * the run was still QUEUED or RUNNING, its next status write throws.
 *
 * @param {string[]} [skipDates] business dates to leave completely alone
 */
export const deleteRunsBefore = async (
  catalystApp,
  businessDateExclusive,
  { skipDates = [] } = {},
) => {
  const zcql = catalystApp.zcql();
  const skip = new Set(skipDates.map(String));
  const all = await selectAll(
    zcql,
    TABLE.RUN,
    (limit, offset) =>
      `SELECT ROWID, runId, businessDate FROM ${TABLE.RUN} WHERE businessDate < '${esc(businessDateExclusive)}' LIMIT ${limit} OFFSET ${offset}`,
  );
  const runs = all.filter((r) => !skip.has(String(r.businessDate)));
  if (runs.length === 0) return { runs: 0, exceptions: 0, skipped: all.length - runs.length };

  let exceptions = 0;
  for (const run of runs) {
    const { removed } = await clearExceptions(catalystApp, run.runId);
    exceptions += removed;
  }

  await catalystApp
    .datastore()
    .table(TABLE.RUN)
    .deleteRows(runs.map((r) => r.ROWID));

  return { runs: runs.length, exceptions, skipped: all.length - runs.length };
};

export { esc as escapeZcql };
