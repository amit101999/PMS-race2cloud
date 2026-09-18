/**
 * Holding Comparison — Data Store access for Holding_Comparison_Run.
 *
 * Takes catalystApp as a parameter so the same file serves AppSail and the job
 * functions. Inside a job that app must be initialised with { scope: "admin" }.
 *
 * ZCQL limits that shape this file: at most 300 rows per query and at most 20
 * columns per SELECT (RUN_COLS is exactly 20, including ROWID). updateRows
 * accepts at most 200 rows per call.
 */

import { RUN_STATUS, STAGE } from "./constants.js";

export const RUN_TABLE = "Holding_Comparison_Run";

const ZCQL_PAGE = 300;
const UPDATE_CHUNK = 200;

const RUN_COLS = [
  "ROWID",
  "runId",
  "businessDate",
  "status",
  "stage",
  "stageDetail",
  "custodyKey",
  "faHoldingsKey",
  "differenceKey",
  "custodyRowCount",
  "faRowCount",
  "totalKeyCount",
  "exceptionCount",
  "quantityMismatchCount",
  "missingInCustodyCount",
  "missingInFaCount",
  "errorCode",
  "errorMessage",
  "startedAtMs",
  "finishedAtMs",
];

const NUMERIC_FIELDS = new Set([
  "custodyRowCount",
  "faRowCount",
  "totalKeyCount",
  "exceptionCount",
  "quantityMismatchCount",
  "missingInCustodyCount",
  "missingInFaCount",
  "startedAtMs",
  "finishedAtMs",
]);

/** Single quotes are ZCQL's only string delimiter, so doubling them is the escape. */
const esc = (s) => String(s ?? "").replace(/'/g, "''");

const toInt = (v) => {
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : 0;
};

/** Catalyst text columns hold 10,000 characters. */
const clip = (v, max = 10000) => {
  const s = String(v ?? "");
  return s.length > max ? `${s.slice(0, max - 3)}...` : s;
};

const shapeRun = (row) => {
  if (!row) return null;
  const run = { ...row };
  for (const field of NUMERIC_FIELDS) run[field] = toInt(run[field]);
  return run;
};

/** ZCQL wraps every row under its table name. */
const unwrap = (result) => (result || []).map((r) => r?.[RUN_TABLE] ?? r).filter(Boolean);

const selectRuns = async (catalystApp, where, limit = ZCQL_PAGE) => {
  const sql = `SELECT ${RUN_COLS.join(", ")} FROM ${RUN_TABLE} WHERE ${where} ORDER BY ROWID DESC LIMIT ${toInt(limit)}`;
  return unwrap(await catalystApp.zcql().executeZCQLQuery(sql)).map(shapeRun);
};

export const newRunId = () => `HC-${Date.now()}`;

export const getRun = async (catalystApp, runId) =>
  (await selectRuns(catalystApp, `runId = '${esc(runId)}'`, 1))[0] || null;

export const listRunsForDate = (catalystApp, businessDate, limit = 50) =>
  selectRuns(catalystApp, `businessDate = '${esc(businessDate)}'`, limit);

export const createRun = async (catalystApp, { runId, businessDate, custodyKey, faHoldingsKey }) => {
  const row = await catalystApp.datastore().table(RUN_TABLE).insertRow({
    runId,
    businessDate,
    status: RUN_STATUS.QUEUED,
    stage: STAGE.READ_CUSTODY,
    stageDetail: "Queued",
    custodyKey,
    faHoldingsKey,
    differenceKey: "",
    custodyRowCount: 0,
    faRowCount: 0,
    totalKeyCount: 0,
    exceptionCount: 0,
    quantityMismatchCount: 0,
    missingInCustodyCount: 0,
    missingInFaCount: 0,
    errorCode: "",
    errorMessage: "",
    startedAtMs: Date.now(),
    finishedAtMs: 0,
  });
  return shapeRun(row);
};

/** Patch a run by runId. Unknown keys are dropped rather than sent to the SDK. */
const updateRun = async (catalystApp, runId, patch) => {
  const run = await getRun(catalystApp, runId);
  if (!run) throw new Error(`No ${RUN_TABLE} row for runId "${runId}".`);

  const row = { ROWID: run.ROWID };
  for (const [key, value] of Object.entries(patch)) {
    if (!RUN_COLS.includes(key) || key === "ROWID") continue;
    if (NUMERIC_FIELDS.has(key)) row[key] = toInt(value);
    else row[key] = clip(value);
  }
  return shapeRun(await catalystApp.datastore().table(RUN_TABLE).updateRow(row));
};

/** Written BEFORE a stage starts, so a crash leaves the stage that needs re-running. */
export const setStage = (catalystApp, runId, stage, stageDetail) =>
  updateRun(catalystApp, runId, { stage, stageDetail, status: RUN_STATUS.RUNNING });

export const finishRun = async (catalystApp, runId, { exceptionCount, ...counts }) => {
  const patch = {
    ...counts,
    exceptionCount,
    stage: STAGE.DONE,
    status: exceptionCount === 0 ? RUN_STATUS.COMPLETED_CLEAN : RUN_STATUS.COMPLETED_WITH_EXCEPTIONS,
    stageDetail: exceptionCount === 0 ? "Holdings agree" : `${exceptionCount} difference(s) found`,
    finishedAtMs: Date.now(),
  };

  // An input changed while this run was working. Its report is still written,
  // but it must stay SUPERSEDED so it is never handed back as the current answer.
  const current = await getRun(catalystApp, runId);
  if (current?.status === RUN_STATUS.SUPERSEDED) {
    patch.status = RUN_STATUS.SUPERSEDED;
    patch.stageDetail = `Superseded while running — ${patch.stageDetail.toLowerCase()}`;
  }

  return updateRun(catalystApp, runId, patch);
};

export const failRun = (catalystApp, runId, { errorCode, errorMessage }) =>
  updateRun(catalystApp, runId, {
    status: RUN_STATUS.FAILED,
    errorCode: errorCode || "UNEXPECTED_ERROR",
    errorMessage: errorMessage || "",
    stageDetail: "Failed",
    finishedAtMs: Date.now(),
  });

/**
 * Mark runs whose inputs have changed as SUPERSEDED.
 *
 * With a date: runs for that date (a custodian file was uploaded, or new FA
 * holdings were generated). Without: every run (the custodian field mapping
 * changed, which affects how every stored file is read).
 *
 * FAILED runs are left alone; there is no answer on them to retire.
 */
export const supersedeRuns = async (catalystApp, businessDate = null) => {
  const liveStatuses = [
    RUN_STATUS.QUEUED,
    RUN_STATUS.RUNNING,
    RUN_STATUS.COMPLETED_CLEAN,
    RUN_STATUS.COMPLETED_WITH_EXCEPTIONS,
  ];
  const statusClause = `(${liveStatuses.map((s) => `status = '${s}'`).join(" OR ")})`;
  const where = businessDate
    ? `businessDate = '${esc(businessDate)}' AND ${statusClause}`
    : statusClause;

  const runIds = [];
  // Each pass updates what it read, so the next query no longer matches those
  // rows; loop until nothing live is left rather than paging with OFFSET.
  for (let pass = 0; pass < 100; pass++) {
    const runs = await selectRuns(catalystApp, where, UPDATE_CHUNK);
    if (runs.length === 0) break;

    await catalystApp
      .datastore()
      .table(RUN_TABLE)
      .updateRows(
        runs.map((r) => ({
          ROWID: r.ROWID,
          status: RUN_STATUS.SUPERSEDED,
          stageDetail: "Superseded — the custodian file, FA holdings or field mapping changed",
        })),
      );
    runIds.push(...runs.map((r) => r.runId));
    if (runs.length < UPDATE_CHUNK) break;
  }

  return { superseded: runIds.length, runIds };
};
