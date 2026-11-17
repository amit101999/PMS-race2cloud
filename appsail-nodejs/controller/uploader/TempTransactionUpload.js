import csv from "csv-parser";
import { PassThrough, Readable } from "stream";

import { stratusSignedUrlToString } from "../../util/stratusSignedUrl.js";
import { releaseImportMarker } from "../../util/custodyMatch/runStore.js";
import { MUTUAL_FUND_TOTAL_AMOUNT_HEADER, mutualFundAmount } from "../../util/custodyMatch/equityReload.js";

export const BUCKET_NAME = "client-transaction-files";
export const TABLE_NAME = "Transaction";

/** AppSail public base URL — used as the bulk-write callback target. */
const APPSAIL_BASE_URL =
  process.env.CATALYST_APPSAIL_URL ||
  "https://backend-50039746698.development.catalystappsail.in";
  // "http://localhost:3001";

const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

/** Read a Stratus object fully into a UTF-8 string. */
async function readStratusObjectAsString(bucket, objectKey) {
  const raw = await bucket.getObject(objectKey);
  if (raw == null) return "";
  if (typeof raw === "string") return raw;
  if (Buffer.isBuffer(raw)) return raw.toString("utf8");
  if (raw instanceof ArrayBuffer) return Buffer.from(raw).toString("utf8");
  if (typeof raw === "object" && typeof raw.pipe === "function") {
    const chunks = [];
    for await (const chunk of raw) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    }
    return Buffer.concat(chunks).toString("utf8");
  }
  return "";
}

function escSql(s) {
  return String(s ?? "").replace(/'/g, "''");
}

export async function ensureJobsRow(zcql, jobName, status) {
  try {
    await zcql.executeZCQLQuery(`
      INSERT INTO Jobs (jobName, status) VALUES ('${escSql(jobName)}', '${escSql(status)}')
    `);
  } catch {
    try {
      await zcql.executeZCQLQuery(`
        UPDATE Jobs SET status = '${escSql(status)}' WHERE jobName = '${escSql(jobName)}'
      `);
    } catch (e2) {
      console.warn(
        `[TempTransactionUpload] Jobs upsert failed for ${jobName}:`,
        e2.message,
      );
    }
  }
}

export async function updateJobsStatus(zcql, jobName, status) {
  try {
    await zcql.executeZCQLQuery(`
      UPDATE Jobs SET status = '${escSql(status)}' WHERE jobName = '${escSql(jobName)}'
    `);
  } catch (e) {
    console.warn(
      `[TempTransactionUpload] Jobs status update failed for ${jobName}:`,
      e.message,
    );
  }
}

/**
 * Bulk-write status poll: first check after upload response, then every 30s.
 * A load still going after 45 min is marked TIMED_OUT — not FAILED, because it
 * may still finish — and checked every 5 min until 3 h have passed.
 */
const POLL_INITIAL_DELAY_MS = 15_000;
const POLL_INTERVAL_MS = 30_000;
const POLL_MAX_WAIT_MS = 45 * 60 * 1000;
const POLL_SLOW_INTERVAL_MS = 5 * 60 * 1000;
const POLL_GIVE_UP_MS = 3 * 60 * 60 * 1000;

/*
 * The load row's status while nobody knows whether the bulk write will finish.
 * Still counts as running for a new commit, and a late finish is still
 * completed (holdings and cash queued) from it.
 */
const LOAD_TIMED_OUT = "TIMED_OUT";

/** Pause between each downstream job submit to reduce Dev COMPONENT concurrency burst. */
const JOB_DISPATCH_DELAY_MS = 10_000;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function normalizeBulkStatus(raw) {
  return String(raw ?? "")
    .toUpperCase()
    .replace(/-/g, "_");
}

export function isBulkSuccessStatus(status) {
  const s = normalizeBulkStatus(status);
  return s === "COMPLETED" || s === "SUCCESS" || s === "SUCCESSFUL";
}

export function isBulkFailedStatus(status) {
  return normalizeBulkStatus(status) === "FAILED";
}

function isBulkInProgressStatus(status) {
  const s = normalizeBulkStatus(status);
  return (
    s === "IN_PROGRESS" || s === "RUNNING" || s === "PENDING" || s === "QUEUED"
  );
}

async function getPipelineJobStatus(zcql, pipelineJobName) {
  if (!pipelineJobName) return "";
  try {
    const rows = await zcql.executeZCQLQuery(`
      SELECT status FROM Jobs
      WHERE jobName = '${escSql(pipelineJobName)}'
      LIMIT 1
    `);
    const row = rows?.[0]?.Jobs || rows?.[0];
    return String(row?.status ?? "").trim().toUpperCase();
  } catch (e) {
    console.warn(`[TxnUploadPipeline] Jobs read failed:`, e.message);
    return "";
  }
}

async function failTxnUploadPipeline(catalystApp, pipelineJobName, reason) {
  console.error(`[TxnUploadPipeline] FAILED: ${reason}`);
  if (!pipelineJobName) return;
  await updateJobsStatus(catalystApp.zcql(), pipelineJobName, "FAILED");
}

/**
 * The bulk job reported FAILED: take the "Imported" mark off the matching run so
 * the date can be committed again. Not called on a poll timeout — the job may
 * still finish, and unlocking then could import the date twice.
 */
async function releaseCustodyRun(catalystApp, custodyRunId) {
  // A commit of several dates carries every run id, joined with commas.
  const runIds = String(custodyRunId || "")
    .split(",")
    .map((id) => id.trim())
    .filter(Boolean);
  for (const runId of runIds) {
    try {
      await releaseImportMarker(catalystApp, runId);
    } catch (e) {
      console.warn(`[TxnUploadPipeline] could not reopen run ${runId}:`, e.message);
    }
  }
}

/*
 * Jobs rows that say an import is still being processed. The holdings and cash
 * jobs pick their "new" trades as everything loaded since their import started,
 * so a second import loaded while they run gets counted by both. A commit waits
 * until these rows leave RUNNING.
 *
 * The names carry the import's start time (epoch ms) after the prefix. The two
 * functions build the same names from their importStartedAtMs param, so keep
 * them in step with functions/CalculateHoldingMaster and
 * functions/Cal_CB_Append_TxnUpload. A client rebuilt because of a late trade
 * gets its own row, HOLD_<ms>_R… (CalculateHoldingWorkers) or CASH_<ms>_R_…
 * (Cal_CB_Append_TxnUpload), and a commit waits for those too.
 */
const IMPORT_JOB_PREFIX = { LOAD: "TxnCsv_", HOLDINGS: "HOLD_", CASH: "CASH_" };
const holdingsJobRowName = (importStartedAtMs) => `${IMPORT_JOB_PREFIX.HOLDINGS}${importStartedAtMs}`;
const cashJobRowName = (importStartedAtMs) => `${IMPORT_JOB_PREFIX.CASH}${importStartedAtMs}`;

/*
 * A row still RUNNING (or a load still TIMED_OUT) this long after its import
 * started is taken as dead: the job and all its retries died without recording
 * an end, or the load poll gave up (POLL_GIVE_UP_MS, the same 3 h). The largest
 * import the holdings job accepts finishes well inside it.
 */
const IMPORT_JOB_DEAD_AFTER_MS = 3 * 60 * 60 * 1000;
/* Dead rows older than this are old history, not worth a warning. */
const IMPORT_JOB_WARN_WINDOW_MS = 24 * 60 * 60 * 1000;

export const timeOfDay = (ms) =>
  new Date(ms).toLocaleTimeString("en-IN", {
    timeZone: "Asia/Kolkata",
    hour: "2-digit",
    minute: "2-digit",
  });

const IMPORT_JOB_LABEL = {
  [IMPORT_JOB_PREFIX.LOAD]: "loading of the trades",
  [IMPORT_JOB_PREFIX.HOLDINGS]: "holdings update",
  [IMPORT_JOB_PREFIX.CASH]: "cash passbook update",
};

/**
 * Is an earlier import still being processed?
 *
 * Ages come from the start time in each row's name, read against this server's
 * clock — the same clock that wrote it — so the database's timezone cannot skew them.
 *
 * @returns {Promise<{running: {jobName:string, what:string, startedAtMs:number}[],
 *                    dead: {jobName:string, what:string, startedAtMs:number}[]}>}
 */
export async function findImportsStillProcessing(catalystApp) {
  const rows = await catalystApp.zcql().executeZCQLQuery(`
    SELECT jobName FROM Jobs
    WHERE (status = 'RUNNING' OR status = '${LOAD_TIMED_OUT}')
      AND (jobName LIKE '${IMPORT_JOB_PREFIX.LOAD}*'
        OR jobName LIKE '${IMPORT_JOB_PREFIX.HOLDINGS}*'
        OR jobName LIKE '${IMPORT_JOB_PREFIX.CASH}*')
    ORDER BY CREATEDTIME DESC
    LIMIT 200
  `);

  const running = [];
  const dead = [];
  for (const row of rows || []) {
    const jobName = String((row.Jobs || row).jobName || "");
    const prefix = Object.values(IMPORT_JOB_PREFIX).find((p) => jobName.startsWith(p));
    // Digits only: a rebuild handed off for a late trade adds "_R…" after them.
    const startedAtMs = Number(jobName.slice(prefix ? prefix.length : 0).match(/^\d+/)?.[0]);
    if (!prefix || !Number.isFinite(startedAtMs) || startedAtMs <= 0) continue;

    const age = Date.now() - startedAtMs;
    const entry = { jobName, what: IMPORT_JOB_LABEL[prefix], startedAtMs };
    if (age < IMPORT_JOB_DEAD_AFTER_MS) running.push(entry);
    else if (age < IMPORT_JOB_WARN_WINDOW_MS) dead.push(entry);
  }
  return { running, dead };
}

/**
 * Queue security master + holdings + cash after bulk import succeeds.
 * Idempotent: skips if Jobs row is no longer RUNNING (callback + poll safe).
 */
async function completeTxnUploadPipeline(catalystApp, meta, logPrefix = "[TxnUploadPipeline]") {
  const {
    pipelineJobName,
    holdingsPairsKey,
    bucketName,
    importStartedAtMs,
    objectKey,
    hasPairs,
  } = meta;

  const zcql = catalystApp.zcql();
  const current = await getPipelineJobStatus(zcql, pipelineJobName);

  // TIMED_OUT is a load that finished late: its trades are in, so holdings and
  // cash must still be queued for them.
  if (current && current !== "RUNNING" && current !== LOAD_TIMED_OUT) {
    console.log(
      `${logPrefix} skip — ${pipelineJobName} already ${current} (idempotent)`,
    );
    return;
  }

  // The holdings and cash rows go to RUNNING BEFORE the load row leaves it, so a
  // commit never sees a moment where this import looks finished. The jobs
  // themselves set COMPLETED at their real end.
  const holdingsRow = holdingsJobRowName(importStartedAtMs);
  const cashRow = cashJobRowName(importStartedAtMs);
  if (hasPairs) {
    await ensureJobsRow(zcql, holdingsRow, "RUNNING");
    await ensureJobsRow(zcql, cashRow, "RUNNING");
  }

  if (pipelineJobName) {
    await updateJobsStatus(zcql, pipelineJobName, "COMPLETED");
  }

  const jobScheduling = catalystApp.jobScheduling();

  try {
    await jobScheduling.JOB.submitJob({
      job_name: `USCM_${Date.now()}`.slice(0, 20),
      jobpool_name: "UpdateMasters",
      target_name: "UpdatesSecurity_ClientMasters",
      target_type: "Function",
      job_config: { number_of_retries: 5, retry_interval: 60 * 1000 },
      params: {
        source: "TxnUpload",
        bucketName,
        objectKey,
        importStartedAtMs: String(importStartedAtMs || ""),
      },
    });
    console.log(`${logPrefix} Queued UpdatesSecurity_ClientMasters.`);
  } catch (err) {
    console.error(
      `${logPrefix} Failed to queue UpdatesSecurity_ClientMasters:`,
      err.message,
    );
  }

  if (!hasPairs) {
    console.warn(`${logPrefix} No pairs — skipping Holdings and Cash jobs.`);
    return;
  }

  console.log(
    `${logPrefix} waiting ${JOB_DISPATCH_DELAY_MS}ms before CalculateHoldingMaster`,
  );
  await sleep(JOB_DISPATCH_DELAY_MS);

  try {
    await jobScheduling.JOB.submitJob({
      job_name: `CHM_${Date.now()}`.slice(0, 20),
      jobpool_name: "UpdateMasters",
      target_name: "CalculateHoldingMaster",
      target_type: "Function",
      job_config: { number_of_retries: 5, retry_interval: 60 * 1000 },
      params: {
        source: "TxnUpload",
        pairsObjectKey: holdingsPairsKey,
        bucketName,
        importStartedAtMs,
      },
    });
    console.log(`${logPrefix} Queued CalculateHoldingMaster.`);
  } catch (err) {
    console.error(
      `${logPrefix} Failed to queue CalculateHoldingMaster:`,
      err.message,
    );
    // Nothing will run to end the row, so end it here.
    await updateJobsStatus(zcql, holdingsRow, "FAILED");
  }

  console.log(
    `${logPrefix} waiting ${JOB_DISPATCH_DELAY_MS}ms before Cal_CB_Append_TxnUpload`,
  );
  await sleep(JOB_DISPATCH_DELAY_MS);

  try {
    await jobScheduling.JOB.submitJob({
      job_name: `CCB_${Date.now()}`.slice(0, 20),
      jobpool_name: "UpdateMasters",
      target_name: "Cal_CB_Append_TxnUpload",
      target_type: "Function",
      job_config: { number_of_retries: 5, retry_interval: 60 * 1000 },
      params: {
        source: "TxnUpload",
        importStartedAtMs,
        lastAccount: "",
      },
    });
    console.log(`${logPrefix} Queued Cal_CB_Append_TxnUpload.`);
  } catch (err) {
    console.error(
      `${logPrefix} Failed to queue Cal_CB_Append_TxnUpload:`,
      err.message,
    );
    // Nothing will run to end the row, so end it here.
    await updateJobsStatus(zcql, cashRow, "FAILED");
  }
}

/** Poll Catalyst bulk-write job status when automatic callback is not delivered. */
async function pollBulkWriteAndComplete(catalystApp, bulkJobId, meta) {
  const logPrefix = `[BulkPoll ${bulkJobId}]`;
  const bulkWrite = catalystApp.datastore().table(TABLE_NAME).bulkJob("write");
  const start = Date.now();

  console.log(
    `${logPrefix} waiting ${POLL_INITIAL_DELAY_MS}ms before first status check`,
  );
  await sleep(POLL_INITIAL_DELAY_MS);

  let markedTimedOut = false;

  while (Date.now() - start < POLL_GIVE_UP_MS) {
    if (!markedTimedOut && Date.now() - start >= POLL_MAX_WAIT_MS) {
      markedTimedOut = true;
      // Only a load still RUNNING: the callback may already have finished it.
      if ((await getPipelineJobStatus(catalystApp.zcql(), meta.pipelineJobName)) === "RUNNING") {
        console.warn(`${logPrefix} still running after ${POLL_MAX_WAIT_MS}ms — marking ${LOAD_TIMED_OUT}`);
        await updateJobsStatus(catalystApp.zcql(), meta.pipelineJobName, LOAD_TIMED_OUT);
      }
    }

    try {
      const res = await bulkWrite.getStatus(bulkJobId);
      const st = res?.status ?? res?.data?.status ?? "";
      const norm = normalizeBulkStatus(st);

      console.log(`${logPrefix} status=${norm}`);

      if (isBulkSuccessStatus(st)) {
        await completeTxnUploadPipeline(catalystApp, meta, logPrefix);
        return;
      }
      if (isBulkFailedStatus(st)) {
        await failTxnUploadPipeline(
          catalystApp,
          meta.pipelineJobName,
          JSON.stringify(res),
        );
        await releaseCustodyRun(catalystApp, meta.custodyRunId);
        return;
      }
      if (!isBulkInProgressStatus(st) && norm) {
        console.warn(`${logPrefix} unexpected status ${norm} — keep polling`);
      }
    } catch (err) {
      console.error(`${logPrefix} getStatus error:`, err.message);
    }

    await sleep(markedTimedOut ? POLL_SLOW_INTERVAL_MS : POLL_INTERVAL_MS);
  }

  // Still not finished after 3 h. Left TIMED_OUT rather than FAILED: the runs
  // stay locked (unlocking could book the dates twice) and a late callback can
  // still complete it. Someone has to check the Transaction table.
  console.error(
    `${logPrefix} [TxnUploadPipeline] no result after ${POLL_GIVE_UP_MS}ms; ` +
      `${meta.pipelineJobName} left ${LOAD_TIMED_OUT}. Check the Transaction table for this import.`,
  );
}

/**
 * Bank / broker export: row-1 headers — exact count (26), order, spelling, and case.
 * Three literal `filler` headers after BankRef match the broker file; by position they are Sub Folio,
 * Transaction Fee and Transaction Fee Service Tax. SETDATEFLAG is layout-only (not inserted).
 */
const EXPECTED_HEADERS_IN_ORDER = [
  "Broker Code",
  "BROKERACID",
  "SYMBOLCODE",
  "EXCHG",
  "TRANSTYPE",
  "DATEPUR_ACQUI",
  "SETDATE",
  "QUANTITY",
  "RATE",
  "BROKERAGEPERSHARE",
  "SERVICETAX",
  "SETDATEFLAG",
  "MKTRATE",
  "CASHSYMBOLCODE",
  "TRANEXPENSE",
  "ACCRUEDINTEREST",
  "BLOCKID",
  "TRANSREF",
  "Description",
  "Cheque number",
  "CHQDETAIL",
  "BankRef",
  "filler",
  "filler",
  "filler",
  "CashSetdate",
];

/** 0-based column indices of literal `filler` in the template, in order. */
const FILLER_TEMPLATE_INDICES = EXPECTED_HEADERS_IN_ORDER.map((h, i) =>
  h === "filler" ? i : -1,
).filter((i) => i >= 0);

/**
 * CSV header (exact) → Transaction column name(s) for bulk insert.
 * String or string[] (duplicated value). Unmapped columns (SETDATEFLAG) dropped in rewrite.
 * The three `filler` columns are mapped by position in mapSourceColumnToTargets.
 */
const HEADER_MAP = {
  "Broker Code": ["WS_client_id", "BROKERCODE"],
  BROKERACID: "WS_Account_code",
  SYMBOLCODE: "ISIN",
  EXCHG: "EXCHG",
  TRANSTYPE: "Tran_Type",
  DATEPUR_ACQUI: "TRANDATE",
  SETDATE: "SETDATE",
  QUANTITY: "QTY",
  RATE: "RATE",
  BROKERAGEPERSHARE: "BROKERAGE",
  SERVICETAX: "SERVICETAX",
  MKTRATE: "MKTRATE",
  CASHSYMBOLCODE: "Security_code",
  TRANEXPENSE: "STT",
  ACCRUEDINTEREST: "ACCRUEDINTEREST",
  BLOCKID: "PORTFOLIOID",
  TRANSREF: "Txn_Ref_No",
  Description: ["Security_Name", "Tran_Desc"],
  "Cheque number": "CHEQUENO",
  CHQDETAIL: "CHEQUEDTL",
  BankRef: "BANKACID",
  CashSetdate: "PAYMENTDATE",
  TDS: "TDSAMOUNT",
  "Stamp Duty": "STAMPDUTY",
  // Column 27 of the Equity tab's consolidated file (FA_TRANSACTION_ID_HEADER in fieldMap.js).
  "Transaction ID": "Transaction_ID",
  // The extra columns added on the Mutual Funds mapping screen.
  Column1: "Column1",
  Column2: "Column2",
  Column3: "Column3",
  Column4: "Column4",
  Column5: "Column5",
};

/**
 * Optional trailing columns, allowed only as a complete pair after the 26.
 * The mutual fund transaction file carries them; the regular file never does,
 * so a 26-column upload is validated and imported exactly as before.
 */
const OPTIONAL_TRAILING_HEADERS = ["TDS", "Stamp Duty"];

/** How a mutual fund row is marked in Transaction. The inception data already uses these values. */
const MUTUAL_FUND_SECURITY_TYPE = "m";
const MUTUAL_FUND_SECURITY_TYPE_DESCRIPTION = "Mutual Funds";

/**
 * @param {string} raw Header cell from row 1 (trimmed).
 * @param {number} columnIndex 0-based column index (must match validated template order).
 * @returns {string | string[] | null} null = omit from bulk CSV.
 */
function mapSourceColumnToTargets(raw, columnIndex) {
  if (Object.prototype.hasOwnProperty.call(HEADER_MAP, raw)) {
    return HEADER_MAP[raw];
  }
  if (raw !== "filler") return null;
  const ord = FILLER_TEMPLATE_INDICES.indexOf(columnIndex);
  if (ord === 0) return "SUB_FOLIO";
  if (ord === 1) return "TOTAL_TRXNFEE";
  if (ord === 2) return "TOTAL_TRXNFEE_STAX";
  return null;
}

/** Must be yyyy-mm-dd when the cell is not blank / null / 0-like. */
const REQUIRED_DATE_COLUMNS = ["DATEPUR_ACQUI", "SETDATE"];

/** If non-empty, value must be yyyy-mm-dd. */
const OPTIONAL_DATE_COLUMNS = ["CashSetdate"];

function stripBom(text) {
  if (text.charCodeAt(0) === 0xfeff) {
    return text.slice(1);
  }
  return text;
}

function getCell(row, columnName) {
  const key = Object.keys(row).find((k) => k.trim() === columnName);
  if (!key) return undefined;
  const v = row[key];
  if (v == null) return "";
  return String(v).trim();
}

function getFirstLineText(fileBuffer) {
  const text = stripBom(fileBuffer.toString("utf8"));
  const nl = text.search(/\r?\n/);
  return nl === -1 ? text : text.slice(0, nl);
}

/** Tab if the header line contains tabs; otherwise comma. */
function detectSeparator(firstLine) {
  return (firstLine.match(/\t/g) || []).length > 0 ? "\t" : ",";
}

function splitHeaderCells(firstLine, separator) {
  return firstLine.split(separator).map((c) =>
    c
      .trim()
      .replace(/^\uFEFF/, "")
      .trim(),
  );
}

/**
 * Row 1 must match EXPECTED_HEADERS_IN_ORDER exactly: same count, same order, same spelling/case.
 */
function validateExpectedHeaderOrder(fileBuffer) {
  const firstLine = getFirstLineText(fileBuffer);
  const separator = detectSeparator(firstLine);
  const cells = splitHeaderCells(firstLine, separator);
  const withTrailing = [...EXPECTED_HEADERS_IN_ORDER, ...OPTIONAL_TRAILING_HEADERS];
  const expected =
    cells.length === withTrailing.length ? withTrailing : EXPECTED_HEADERS_IN_ORDER;

  if (cells.length !== expected.length) {
    return {
      code: "WRONG_COLUMN_ORDER",
      kind: "COUNT",
      message: `Column order / count is wrong. Row 1 must have exactly ${expected.length} columns in the official template order; your file has ${cells.length}. Do not add, remove, or reorder columns.`,
      expectedCount: expected.length,
      actualCount: cells.length,
      expectedHeaders: [...expected],
      hint: "Restore the header row to match the template: same positions left-to-right.",
    };
  }

  const mismatches = [];
  for (let i = 0; i < cells.length; i++) {
    if (cells[i] !== expected[i]) {
      mismatches.push({
        columnIndex: i + 1,
        expected: expected[i],
        actual: cells[i],
      });
    }
  }

  if (mismatches.length > 0) {
    return {
      code: "WRONG_COLUMN_ORDER",
      kind: "ORDER_OR_NAME",
      message:
        "Column order is wrong (or a header name/spelling does not match the template). Row 1 headers must appear in the exact same order as the official list — shifting any column left or right will fail.",
      mismatches,
      expectedHeaders: [...expected],
      hint: `Check column position(s): ${mismatches.map((m) => m.columnIndex).join(", ")}. Expected vs actual is listed below.`,
    };
  }

  return null;
}

function isValidYyyyMmDd(value) {
  const s = String(value ?? "").trim();
  if (!ISO_DATE_RE.test(s)) return false;
  const [y, m, d] = s.split("-").map(Number);
  const dt = new Date(s + "T00:00:00");
  return (
    dt.getFullYear() === y && dt.getMonth() + 1 === m && dt.getDate() === d
  );
}

/** Missing date in CSV: empty, numeric 0, or common null tokens (case-insensitive). */
function isAbsentDateValue(value) {
  if (value === undefined) return true;
  const s = String(value).trim();
  if (s === "") return true;
  if (s === "0") return true;
  const lower = s.toLowerCase();
  return lower === "null" || lower === "na" || lower === "n/a" || s === "-";
}

function targetsForHeader(mapped) {
  return Array.isArray(mapped) ? mapped : [mapped];
}

/** Round to 4 dp to keep derived NETRATE / Net_Amount free of float noise. */
function round4(n) {
  return Math.round((Number(n) + Number.EPSILON) * 1e4) / 1e4;
}

/** Buy-side trade types: brokerage adds to cost. Everything else: brokerage reduces proceeds. */
function isBuyTransType(tranType) {
  return /^BY-|SQB|OPI/i.test(String(tranType ?? "").trim());
}

/**
 * Derive NETRATE / Net_Amount from the raw broker columns. The file has no
 * column for either, so every row is calculated here.
 * Convention matches the historical rows already in `Transaction`:
 *   NETRATE    = RATE ± BROKERAGEPERSHARE   (+ for buys, − for sells)
 *   Net_Amount = QTY × NETRATE
 * STT (TRANEXPENSE) and SERVICETAX stay in their own columns — not folded in.
 * Returns "" for both when RATE is absent (nothing to derive).
 */
function deriveNetRateAndAmount({ tranType, qty, rate, brokeragePerShare }) {
  const r = Number(rate) || 0;
  if (!r) return { netRate: "", netAmount: "" };
  const b = Number(brokeragePerShare) || 0;
  const q = Math.abs(Number(qty) || 0);
  const netRate = round4(isBuyTransType(tranType) ? r + b : r - b);
  const netAmount = round4(q * netRate);
  return { netRate: String(netRate), netAmount: String(netAmount) };
}

/**
 * Mutual fund rows:
 *   Net_Amount = TOTAL AMOUNT when the cell is filled, exactly as given;
 *                otherwise worked out by mutualFundAmount
 *                (buy: QTY × RATE + Stamp Duty, sell: QTY × RATE − STT)
 *   NETRATE    = Net_Amount ÷ QTY
 * TDS is stored but does not change the amount. Net_Amount is the final money
 * moved, so the cash jobs use it as it is for these rows (no second ± STT).
 * Returns "" for both when there is no amount or no quantity.
 */
function deriveMutualFundAmount({ tranType, qty, rate, stampDuty, stt, totalAmount }) {
  const q = Math.abs(Number(qty) || 0);
  const given = String(totalAmount ?? "").trim();
  const amount = given !== "" ? Number(given) : mutualFundAmount({ tranType, qty, rate, stampDuty, stt });
  if (!q || amount === null || !Number.isFinite(amount)) return { netRate: "", netAmount: "" };
  return { netRate: String(round4(amount / q)), netAmount: given !== "" ? given : String(amount) };
}

/**
 * Splits CSV text into rows of cells, honouring double quotes: a quoted cell may
 * hold the separator, a line break, or a doubled "" for a literal quote. Cutting
 * each line at every comma instead shifts every column after a value such as
 * "TATA MOTORS LTD, DVR" into the wrong Transaction field.
 */
function parseCsvRows(text, separator) {
  const rows = [];
  let row = [];
  let cell = "";
  let inQuotes = false;

  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (inQuotes) {
      if (ch === '"' && text[i + 1] === '"') {
        cell += '"';
        i++;
      } else if (ch === '"') {
        inQuotes = false;
      } else {
        cell += ch;
      }
    } else if (ch === '"') {
      inQuotes = true;
    } else if (ch === separator) {
      row.push(cell);
      cell = "";
    } else if (ch === "\n" || ch === "\r") {
      if (ch === "\r" && text[i + 1] === "\n") i++;
      row.push(cell);
      rows.push(row);
      row = [];
      cell = "";
    } else {
      cell += ch;
    }
  }
  row.push(cell);
  rows.push(row);
  return rows;
}

/*
 * Codes stored in capitals, whatever case the file used. Holdings, cash and the
 * security list key on these values, so "inzztest0054" and "INZZTEST0054" would
 * otherwise become two different securities.
 */
const UPPERCASE_SOURCE_COLUMNS = new Set(["BROKERACID", "SYMBOLCODE", "EXCHG"]);

/** CSV cell for the bulk-write file: quoted when it holds a comma, quote or line break. */
function csvCell(value) {
  const s = String(value ?? "");
  return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

/**
 * Rewrites CSV for bulk insert: maps template headers to Transaction column names;
 * one source column can fill multiple DB columns. Unmapped template columns (SETDATEFLAG) dropped.
 * NETRATE and Net_Amount are appended to every row, always derived from
 * RATE / BROKERAGEPERSHARE / QTY / TRANSTYPE (see deriveNetRateAndAmount).
 */
export function normalizeCsvForBulkUpload(fileBuffer, { mutualFund = false } = {}) {
  const text = stripBom(fileBuffer.toString("utf8"));
  const separator = detectSeparator(getFirstLineText(fileBuffer));
  const rows = parseCsvRows(text, separator).map((cells) =>
    cells.map((c) => c.trim().replace(/^\uFEFF/, "")),
  );

  const originalHeaders = rows[0];
  const headerMappings = [];
  for (let i = 0; i < originalHeaders.length; i++) {
    const raw = originalHeaders[i];
    const mapped = mapSourceColumnToTargets(raw, i);
    if (mapped) {
      headerMappings.push({ idx: i, targets: targetsForHeader(mapped) });
    }
  }

  // Source column positions for auto-deriving NETRATE / Net_Amount. The header is
  // validated against the fixed template before this runs, so these always exist.
  const srcIdx = (name) => originalHeaders.indexOf(name);
  const typeIdx = srcIdx("TRANSTYPE");
  const qtyIdx = srcIdx("QUANTITY");
  const rateIdx = srcIdx("RATE");
  const brokIdx = srcIdx("BROKERAGEPERSHARE");
  const stampIdx = srcIdx("Stamp Duty");
  const sttIdx = srcIdx("TRANEXPENSE");
  // Mutual funds only; it has no Transaction column of its own (not in HEADER_MAP).
  const totalAmountIdx = srcIdx(MUTUAL_FUND_TOTAL_AMOUNT_HEADER);

  const newHeaders = [...headerMappings.flatMap((m) => m.targets), "NETRATE", "Net_Amount"];
  if (mutualFund) newHeaders.push("Security_Type", "Security_Type_Description");
  const outSep = ",";
  const newLines = [newHeaders.join(outSep)];

  for (let r = 1; r < rows.length; r++) {
    const cells = rows[r];
    // A blank line reads as one empty cell.
    if (cells.length === 1 && cells[0] === "") continue;
    const cellAt = (i) => (i >= 0 && i < cells.length ? cells[i] : "");

    const derived = mutualFund
      ? deriveMutualFundAmount({
          tranType: cellAt(typeIdx),
          qty: cellAt(qtyIdx),
          rate: cellAt(rateIdx),
          stampDuty: cellAt(stampIdx),
          stt: cellAt(sttIdx),
          totalAmount: cellAt(totalAmountIdx),
        })
      : deriveNetRateAndAmount({
          tranType: cellAt(typeIdx),
          qty: cellAt(qtyIdx),
          rate: cellAt(rateIdx),
          brokeragePerShare: cellAt(brokIdx),
        });

    const outCells = [];
    for (const m of headerMappings) {
      let raw = m.idx < cells.length ? cells[m.idx] : "";
      if (UPPERCASE_SOURCE_COLUMNS.has(originalHeaders[m.idx])) raw = raw.toUpperCase();
      for (let t = 0; t < m.targets.length; t++) outCells.push(csvCell(raw));
    }
    outCells.push(derived.netRate, derived.netAmount);
    if (mutualFund) {
      outCells.push(MUTUAL_FUND_SECURITY_TYPE, MUTUAL_FUND_SECURITY_TYPE_DESCRIPTION);
    }
    newLines.push(outCells.join(outSep));
  }

  return Buffer.from(newLines.join("\n"), "utf8");
}

/** Max distinct (BROKERACID, SYMBOLCODE) pairs collected from one CSV for scoped Holdings job. */
const MAX_HOLDINGS_SCOPE_PAIRS = 25000;

/** Soft cap on serialized pairsJson for Catalyst job params (~1MB safety). */
const MAX_PAIRS_JSON_CHARS = 900000;

/** First N data rows parsed for date-format checks before upload. */
const SAMPLE_DATA_ROW_COUNT = 3;

/**
 * Parses the first SAMPLE_DATA_ROW_COUNT data rows from the CSV buffer.
 */
function parseTempTransactionSampleRows(fileBuffer) {
  return new Promise((resolve, reject) => {
    const text = stripBom(fileBuffer.toString("utf8"));
    const firstLine = getFirstLineText(fileBuffer);
    const separator = detectSeparator(firstLine);
    const rows = [];
    const source = Readable.from(text);
    const parser = csv({ separator });
    let settled = false;

    const settle = (err, result) => {
      if (settled) return;
      settled = true;
      source.removeAllListeners();
      parser.removeAllListeners();
      try {
        source.destroy();
        parser.destroy();
      } catch {
        /* ignore */
      }
      if (err) {
        reject(err);
        return;
      }
      resolve(result);
    };

    parser.on("data", (row) => {
      rows.push(row);
      if (rows.length >= SAMPLE_DATA_ROW_COUNT) {
        settle(null, rows);
      }
    });

    parser.on("end", () => settle(null, rows));
    parser.on("error", (e) => settle(e));
    source.on("error", (e) => settle(e));

    source.pipe(parser);
  });
}

/**
 * Reads every data row; collects unique (BROKERACID, SYMBOLCODE) for scoped Holdings rebuild.
 */
function extractDistinctBrokerAccountIsinPairsFromCsv(fileBuffer) {
  return new Promise((resolve, reject) => {
    const text = stripBom(fileBuffer.toString("utf8"));
    const firstLine = getFirstLineText(fileBuffer);
    const separator = detectSeparator(firstLine);
    const seen = new Set();
    const pairs = [];
    let truncated = false;
    const source = Readable.from(text);
    const parser = csv({ separator });
    let settled = false;

    const settle = (err, result) => {
      if (settled) return;
      settled = true;
      source.removeAllListeners();
      parser.removeAllListeners();
      try {
        source.destroy();
        parser.destroy();
      } catch {
        /* ignore */
      }
      if (err) {
        reject(err);
        return;
      }
      resolve(result);
    };

    parser.on("data", (row) => {
      // Capitals, as normalizeCsvForBulkUpload stores them.
      const acc = String(getCell(row, "BROKERACID") ?? "").trim().toUpperCase();
      const isin = String(getCell(row, "SYMBOLCODE") ?? "").trim().toUpperCase();
      if (!acc || !isin) return;
      const key = `${acc}\t${isin}`;
      if (seen.has(key)) return;
      seen.add(key);
      pairs.push([acc, isin]);
      if (pairs.length >= MAX_HOLDINGS_SCOPE_PAIRS) {
        truncated = true;
      }
    });

    parser.on("end", () => settle(null, { pairs, truncated }));
    parser.on("error", (e) => settle(e));
    source.on("error", (e) => settle(e));

    source.pipe(parser);
  });
}

/**
 * POST /api/transaction-uploader/upload-temp-file
 * 1. Validates row 1 matches EXPECTED_HEADERS_IN_ORDER (26 columns, count + order + exact names).
 * 2. Validates the CSV parses and has at least one data row.
 * 3. Validates date columns on the first 3 data rows (blank / 0 / null allowed when empty).
 * 4. Rewrites CSV: maps template headers to Transaction columns (HEADER_MAP), tab → comma, trim cells.
 * 5. Uploads rewritten CSV + original CSV to Stratus. Writes pairs JSON + account codes JSON to Stratus.
 * 6. Creates Datastore bulk write job (Transaction table) WITH a callback URL.
 * 7. Returns immediately; background poll checks bulk job status every 30s (fallback when
 *    Catalyst does not POST to /bulk-callback). Callback or poll queues downstream jobs.
 */
export const uploadTempTransaction = async (req, res) => {
  let pipelineJobName = null;
  let zcqlForJob = null;

  try {
    /* ─── 1. VALIDATE REQUEST ─────────────────────────────────────── */
    const catalystApp = req.catalystApp;
    if (!catalystApp) {
      return res.status(500).json({
        success: false,
        message: "Catalyst not initialized",
      });
    }

    const file = req.files?.file;

    if (!file) {
      return res.status(400).json({
        success: false,
        message: "CSV file is required. Send it as form-data with key 'file'.",
      });
    }

    if (!file.name.toLowerCase().endsWith(".csv")) {
      return res.status(400).json({
        success: false,
        message: "Only CSV files are allowed",
      });
    }

    const orderErr = validateExpectedHeaderOrder(file.data);
    if (orderErr) {
      return res.status(400).json({
        success: false,
        ...orderErr,
      });
    }

    /* ─── 1b. PARSE CSV + DATE CHECK (before Stratus — must pass to continue) ─ */
    let sampleRows;
    try {
      sampleRows = await parseTempTransactionSampleRows(file.data);
    } catch (parseErr) {
      console.error(
        `[TempTransactionUpload] CSV parse error [${new Date().toISOString()}]:`,
        parseErr,
      );
      return res.status(400).json({
        success: false,
        message: "Could not read the CSV file. Check the file and try again.",
      });
    }

    if (sampleRows.length === 0) {
      return res.status(400).json({
        success: false,
        message:
          "The CSV has no data rows. Add at least one data row before uploading.",
      });
    }

    for (let i = 0; i < sampleRows.length; i++) {
      const fileLineNumber = i + 2;
      const invalidDateFields = [];

      for (const col of REQUIRED_DATE_COLUMNS) {
        const v = getCell(sampleRows[i], col);
        if (!isAbsentDateValue(v) && !isValidYyyyMmDd(v)) {
          invalidDateFields.push(col);
        }
      }

      for (const col of OPTIONAL_DATE_COLUMNS) {
        const v = getCell(sampleRows[i], col);
        if (!isAbsentDateValue(v) && !isValidYyyyMmDd(v)) {
          invalidDateFields.push(col);
        }
      }

      if (invalidDateFields.length > 0) {
        return res.status(400).json({
          success: false,
          code: "INVALID_DATE_FORMAT",
          message: `Date format is invalid. Parsed the first ${SAMPLE_DATA_ROW_COUNT} data rows: problem in data row ${i + 1} of ${SAMPLE_DATA_ROW_COUNT} (CSV line ${fileLineNumber}). Columns must be yyyy-mm-dd: ${invalidDateFields.join(", ")}.`,
          fields: invalidDateFields,
          row: fileLineNumber,
          dataRowIndex: i + 1,
          headerNamesToFix: invalidDateFields,
          sampleRowsValidated: SAMPLE_DATA_ROW_COUNT,
          hint: "Use yyyy-mm-dd only (example: 2025-04-06). Empty dates: leave blank, 0, or null. Fix the file and re-upload — nothing was imported.",
        });
      }
    }

    /* ─── 1b2. ONE IMPORT AT A TIME ───────────────────────────────── */
    // Checked here, not by the callers, so every route into the import obeys it.
    const { running: busyImports, dead: deadImports } =
      await findImportsStillProcessing(catalystApp);
    if (busyImports.length > 0) {
      const busy = busyImports[0];
      return res.status(409).json({
        success: false,
        code: "IMPORT_STILL_PROCESSING",
        message: `The ${busy.what} for the import started at ${timeOfDay(busy.startedAtMs)} is still running. Importing now could count trades twice in holdings or cash. Try again when it finishes.`,
        running: busyImports.map((r) => r.jobName),
      });
    }
    // Said with the reply, never a reason to refuse: the job is gone, so it
    // cannot overlap this import.
    const importWarning =
      deadImports.length > 0
        ? `The ${deadImports[0].what} for the import started at ${timeOfDay(deadImports[0].startedAtMs)} never finished. Ask the developer to check it and rebuild holdings / cash if needed.`
        : "";

    /* ─── 1c. DISTINCT (BROKERACID, SYMBOLCODE) FOR SCOPED HOLDINGS JOB ─ */
    let scopePairsResult = { pairs: [], truncated: false };
    try {
      scopePairsResult = await extractDistinctBrokerAccountIsinPairsFromCsv(
        file.data,
      );
    } catch (pairErr) {
      console.error(
        `[TempTransactionUpload] pair-list CSV parse [${new Date().toISOString()}]:`,
        pairErr,
      );
      return res.status(400).json({
        success: false,
        message:
          "Could not scan the CSV for broker account / ISIN pairs. Check the file and try again.",
      });
    }

    /* ─── 2. NORMALIZE CSV & UPLOAD TO STRATUS ───────────────────── */
    const rewrittenCsv = normalizeCsvForBulkUpload(file.data);

    const uploadMs = Date.now();
    const objectKey = `transactions/TxnUpload-${uploadMs}-${file.name}`;
    const originalObjectKey = `transactions-original/TxnUpload-${uploadMs}-${file.name}`;
    pipelineJobName = `TxnCsv_${uploadMs}`.slice(0, 48);
    // Set when the commit comes from custody matching (CustodyMatchRun.js).
    const custodyRunId = String(req.body?.custodyRunId || "").trim();

    const bucket = catalystApp.stratus().bucket(BUCKET_NAME);

    const passThrough = new PassThrough();
    const uploadPromise = bucket.putObject(objectKey, passThrough, {
      overwrite: true,
      contentType: "text/csv",
    });
    passThrough.end(rewrittenCsv);

    const originalPassThrough = new PassThrough();
    const originalUploadPromise = bucket.putObject(
      originalObjectKey,
      originalPassThrough,
      {
        overwrite: true,
        contentType: "text/csv",
      },
    );
    originalPassThrough.end(file.data);

    await Promise.all([uploadPromise, originalUploadPromise]);

    /* ─── 3. Write pairs manifest + account codes to Stratus ────────── */
    const hasPairs = scopePairsResult.pairs.length > 0;

    const holdingsPairsKey = `transactions-meta/holdings-pairs-${uploadMs}.json`;
    const accountCodesKey = `transactions-meta/account-codes-${uploadMs}.json`;

    if (hasPairs) {
      const accountCodesForCash = [
        ...new Set(
          scopePairsResult.pairs
            .map(([acc]) => String(acc ?? "").trim())
            .filter(Boolean),
        ),
      ].sort((a, b) => a.localeCompare(b));

      const pairsMeta = new PassThrough();
      const pairsUpload = bucket.putObject(holdingsPairsKey, pairsMeta, {
        overwrite: true,
        contentType: "application/json",
      });
      pairsMeta.end(
        Buffer.from(JSON.stringify(scopePairsResult.pairs), "utf8"),
      );
      await pairsUpload;

      const codesMeta = new PassThrough();
      const codesUpload = bucket.putObject(accountCodesKey, codesMeta, {
        overwrite: true,
        contentType: "application/json",
      });
      codesMeta.end(Buffer.from(JSON.stringify(accountCodesForCash), "utf8"));
      await codesUpload;

      console.log(
        `[TempTransactionUpload] Wrote ${scopePairsResult.pairs.length} pairs and ` +
          `${accountCodesForCash.length} account codes to Stratus.`,
      );
    }

    /* ─── 4. Mark job RUNNING + create bulk write job WITH callback ── */
    zcqlForJob = catalystApp.zcql();
    await ensureJobsRow(zcqlForJob, pipelineJobName, "RUNNING");

    const datastore = catalystApp.datastore();
    const bulkWrite = datastore.table(TABLE_NAME).bulkJob("write");

    // Pipeline metadata the callback needs. Sent BOTH in the URL query string
    // (reliable channel — Catalyst has been observed to drop the callback `params`
    // object) and as `params` below. handleBulkCallback reads the query first.
    const callbackParams = {
      pipelineJobName,
      holdingsPairsKey,
      accountCodesKey,
      bucketName: BUCKET_NAME,
      importStartedAtMs: String(uploadMs),
      objectKey,
      hasPairs: String(hasPairs),
      custodyRunId,
    };
    const callbackUrl =
      `${APPSAIL_BASE_URL}/api/transaction-uploader/bulk-callback` +
      `?${new URLSearchParams(callbackParams).toString()}`;

    let bulkJob;
    try {
      bulkJob = await bulkWrite.createJob(
        {
          bucket_name: BUCKET_NAME,
          object_key: objectKey,
        },
        {
          operation: "insert",
          callback: {
            url: callbackUrl,
            params: callbackParams,
          },
        },
      );
    } catch (bulkCreateErr) {
      await updateJobsStatus(zcqlForJob, pipelineJobName, "FAILED");
      throw bulkCreateErr;
    }

    console.log(
      `[TempTransactionUpload] Bulk write job created: ${bulkJob.job_id}. ` +
        `Callback registered at ${callbackUrl}`,
    );

    const pipelineMeta = {
      pipelineJobName,
      holdingsPairsKey,
      accountCodesKey,
      bucketName: BUCKET_NAME,
      importStartedAtMs: String(uploadMs),
      objectKey,
      hasPairs,
      custodyRunId,
    };

    void pollBulkWriteAndComplete(catalystApp, bulkJob.job_id, pipelineMeta).catch(
      (err) => {
        console.error(
          `[TempTransactionUpload] pollBulkWriteAndComplete fatal:`,
          err.message,
        );
      },
    );

    /* ─── 5. RESPOND IMMEDIATELY ─────────────────────────────────── */
    return res.status(200).json({
      success: true,
      message:
        "File uploaded successfully. Transaction import is running in the background. " +
        "Holdings and cash balance will be updated automatically once import completes.",
      pipelineJobName,
      bulkJobId: bulkJob.job_id,
      fileName: file.name,
      objectKey,
      bucket: BUCKET_NAME,
      table: TABLE_NAME,
      holdingsPairsCount: scopePairsResult.pairs.length,
      status: "PROCESSING",
      ...(importWarning ? { warning: importWarning } : {}),
    });
  } catch (error) {
    console.error(
      `[TempTransactionUpload] [Error] [${new Date().toISOString()}] :`,
      error,
    );

    if (pipelineJobName && zcqlForJob) {
      await updateJobsStatus(zcqlForJob, pipelineJobName, "FAILED");
    }

    return res.status(500).json({
      success: false,
      message: "Failed to upload file or complete bulk insert into Transaction",
      error: error.message,
      ...(pipelineJobName ? { pipelineJobName } : {}),
    });
  }
};

/* ──────────────────────── BULK WRITE CALLBACK ─────────────────── */

/**
 * POST /api/transaction-uploader/bulk-callback
 *
 * Catalyst may POST here when the bulk write job completes. Polling is the primary
 * fallback when automatic callback delivery fails. Both paths call
 * completeTxnUploadPipeline (idempotent).
 */
export const handleBulkCallback = async (req, res) => {
  res.status(200).json({ received: true });

  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) {
      console.error("[BulkCallback] Catalyst app not initialized");
      return;
    }

    const body = req.body || {};

    console.log(
      "[BulkCallback RAW] body=",
      JSON.stringify(body),
      "query=",
      JSON.stringify(req.query || {}),
      "contentType=",
      req.headers?.["content-type"] || "",
    );

    const status = String(body.status || "").toUpperCase();
    const p = { ...(body.params || {}), ...(req.query || {}) };

    const meta = {
      pipelineJobName: String(p.pipelineJobName || "").trim(),
      holdingsPairsKey: String(p.holdingsPairsKey || "").trim(),
      accountCodesKey: String(p.accountCodesKey || "").trim(),
      bucketName: String(p.bucketName || BUCKET_NAME).trim(),
      importStartedAtMs: String(p.importStartedAtMs || "").trim(),
      objectKey: String(p.objectKey || "").trim(),
      hasPairs: p.hasPairs === "true" || p.hasPairs === true,
      custodyRunId: String(p.custodyRunId || "").trim(),
    };

    console.log(
      `[BulkCallback] status=${status} pipelineJobName=${meta.pipelineJobName} hasPairs=${meta.hasPairs}`,
    );

    if (status === "FAILED") {
      await failTxnUploadPipeline(
        catalystApp,
        meta.pipelineJobName,
        "bulk callback FAILED",
      );
      await releaseCustodyRun(catalystApp, meta.custodyRunId);
      return;
    }

    if (!isBulkSuccessStatus(status)) {
      console.warn(`[BulkCallback] Unexpected status "${status}" — ignoring.`);
      return;
    }

    await completeTxnUploadPipeline(catalystApp, meta, "[BulkCallback]");
  } catch (error) {
    console.error("[BulkCallback] Unexpected error:", error.message);
  }
};

/* ──────────────────────── UPLOAD HISTORY ──────────────────────── */

/**
 * Stratus object keys produced by `uploadTempTransaction` look like
 *   transactions/TxnUpload-<ms>-<originalFileName>
 * (older keys may use `Txn-<ms>-...`). The capture groups give us the
 * upload time and the user's original filename without needing a separate
 * tracking table.
 */
const STRATUS_TXN_KEY_PATTERN =
  /^transactions(?:-original)?\/Txn(?:Upload)?-(\d+)-(.+)$/;

export function parseStratusTransactionUploadKey(key) {
  if (!key || typeof key !== "string") return null;
  const m = STRATUS_TXN_KEY_PATTERN.exec(key.trim());
  if (!m) return null;
  const uploadedAtMs = Number(m[1]);
  if (!Number.isFinite(uploadedAtMs)) return null;
  return {
    originalFileName: m[2],
    uploadedAtIso: new Date(uploadedAtMs).toISOString(),
    uploadedAtMs,
  };
}

/**
 * GET /api/transaction-uploader/upload-history?limit=&cursor=&search=
 * Lists transaction uploads under the `transactions/` prefix, newest first.
 * Returns minimal metadata derived from the object key (no separate DB).
 */
export const listUploadHistory = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) {
      return res
        .status(500)
        .json({ success: false, message: "Catalyst not initialized" });
    }

    const limit = Math.min(100, Math.max(1, Number(req.query.limit) || 5));
    const cursor = req.query.cursor ? String(req.query.cursor) : null;
    const search = String(req.query.search || "")
      .trim()
      .toLowerCase();

    const bucket = catalystApp.stratus().bucket(BUCKET_NAME);
    const listOpts = {
      maxKeys: limit,
      prefix: "transactions-original/",
      orderBy: "desc",
    };
    if (cursor) listOpts.continuationToken = cursor;

    const result = await bucket.listPagedObjects(listOpts);

    // The SDK can return either StratusObject instances (metadata on
    // `keyDetails`) or plain objects with the fields at the top level.
    // Normalize both shapes here.
    const rawContents = result.contents || [];
    const items = [];

    for (const entry of rawContents) {
      const o =
        entry &&
        typeof entry === "object" &&
        "keyDetails" in entry &&
        entry.keyDetails
          ? entry.keyDetails
          : entry;
      const key = o.key;
      if (!key || typeof key !== "string") continue;

      if (o.key_type && o.key_type !== "file") continue;
      const parsed = parseStratusTransactionUploadKey(key);
      if (!parsed) continue;
      if (search && !parsed.originalFileName.toLowerCase().includes(search)) {
        continue;
      }
      items.push({
        key,
        originalFileName: parsed.originalFileName,
        uploadedAt: parsed.uploadedAtIso,
        uploadedAtMs: parsed.uploadedAtMs,
        sizeBytes: Number(o.size) || 0,
        contentType: o.content_type || "text/csv",
        lastModified: o.last_modified ?? null,
      });
    }

    items.sort((a, b) => b.uploadedAtMs - a.uploadedAtMs);

    const truncated =
      result.truncated === true ||
      String(result.truncated || "").toLowerCase() === "true";
    const nextCursor = truncated
      ? result.next_continuation_token || result.nextContinuationToken || null
      : null;

    return res.status(200).json({ success: true, items, nextCursor });
  } catch (error) {
    console.error("[TempTransactionUpload] listUploadHistory:", error);
    return res.status(500).json({
      success: false,
      message: error.message || "Failed to list upload history",
    });
  }
};

/**
 * GET /api/transaction-uploader/upload-history/download?key=…
 * Validates the key shape, then returns a 1-hour Stratus presigned GET URL.
 */
export const downloadUploadHistory = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    if (!catalystApp) {
      return res
        .status(500)
        .json({ success: false, message: "Catalyst not initialized" });
    }

    const key = String(req.query.key || "").trim();
    if (!key.startsWith("transactions-original/")) {
      return res
        .status(400)
        .json({ success: false, message: "Invalid object key" });
    }
    const parsed = parseStratusTransactionUploadKey(key);
    if (!parsed) {
      return res.status(400).json({
        success: false,
        message: "Unsupported upload key format",
      });
    }

    const bucket = catalystApp.stratus().bucket(BUCKET_NAME);
    const rawUrl = await bucket.generatePreSignedUrl(key, "GET", {
      expiresIn: 3600,
    });
    const downloadUrl = stratusSignedUrlToString(rawUrl);
    if (!downloadUrl) {
      return res.status(500).json({
        success: false,
        message: "Could not generate download URL",
      });
    }

    return res.status(200).json({
      success: true,
      downloadUrl,
      expiresAtIso: new Date(Date.now() + 3600 * 1000).toISOString(),
      fileName: parsed.originalFileName,
      key,
    });
  } catch (error) {
    console.error("[TempTransactionUpload] downloadUploadHistory:", error);
    return res.status(500).json({
      success: false,
      message: error.message || "Failed to generate download link",
    });
  }
};
