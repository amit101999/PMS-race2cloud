/**
 * CalculateHoldingWorkers (Catalyst Job Function) — TxnUpload append-only
 *
 * Single responsibility: incrementally append Holdings rows after a transaction
 * CSV upload. Full rebuilds (DELETE + INSERT all) are NOT handled here — they
 * live in a separate dedicated job. This function never deletes Holdings rows.
 *
 * Required job params (otherwise the function exits as a no-op):
 *   - `source` = "TxnUpload"
 *   - `pairsJson` = JSON array of `[accountCode, ISIN]` pairs touched by the upload
 *   - `importStartedAtMs` = epoch ms; only Transaction rows with
 *     `CREATEDTIME >= import floor` are considered new
 *
 * Per pair logic:
 *   - Buys-only new txns → cheap append. Continue qty / cost / WAP from the last
 *     Holdings row and INSERT one Holdings row per new buy (no queue rebuild).
 *   - Any sell present → read existing Holdings rows for the pair, replay them
 *     once to rebuild the active buy queue in memory, then apply each new txn
 *     FIFO-style and INSERT one Holdings row per new txn. Prior Holdings rows
 *     are never touched. The Transaction table is not re-read here.
 *   - A new txn dated before a row already saved for its pair (a late trade, or
 *     dates committed out of order) → nothing is appended for that account; its
 *     ISINs go to RebuildHoldingtable, which redoes them from all trades.
 *
 * Holdings columns written:
 *   WS_Account_code, TRANSACTION_DATE, SETTLEMENT_DATE, TYPE, ISIN,
 *   QUANTITY, PRICE, TOTAL_AMOUNT, HOLDING, WAP, HOLDING_VALUE, P_L, STATUS
 *
 * Row order within a pair is preserved by INSERT order. Reads must use the
 * same ORDER BY as AppSail `HOLDINGS_FIFO_ORDER_BY_SQL`
 * (`CREATEDTIME ASC, ROWID ASC`), not settlement-only.
 *
 * Bonus / Split / Demerger / Merger are intentionally not loaded here — they
 * are applied via separate methods.
 *
 * Optional: pass non-empty `jobName` via `jobRequest.getAllJobParams()` so the
 * function writes `Jobs` / `JobStatusPerAccount` for retries and UX; omit it
 * for legacy / smoke runs and no Job tables are touched.
 */

const catalyst = require("zcatalyst-sdk-node");

/* ============================== CONFIG ============================== */

/** Hard cap on number of (account, ISIN) pairs processed; 0 = unlimited. Useful for first event-fire tests. */
const MAX_PAIRS = 0;

/** YYYY-MM-DD inclusive cutoff for source data, or null for full history. */
const AS_ON_DATE = null;

/** When true, computes everything but does not DELETE/INSERT into Holdings (safe smoke test). */
const DRY_RUN = false;

/** ZCQL paging size. */
const BATCH = 250;

/* ============================== HELPERS ============================== */

const esc = (s) => String(s ?? "").replace(/'/g, "''");

/**
 * Quantity epsilon for normal (non-corporate-action) FIFO math. Floating point
 * subtraction on fractional share quantities can leave tiny residue (e.g.
 * 0.533 - 0.533 = 1e-16) which would otherwise linger in the buy queue and
 * drift the running holding. snapQty() treats anything below QTY_EPS as exactly
 * zero. Genuine fractional quantities (e.g. 5.533) are preserved untouched.
 * Corporate-action rounding (Math.floor on demerger/merger, split/bonus ratios)
 * is intentional business logic and is NOT affected by this.
 */
const QTY_EPS = 1e-6;
const snapQty = (n) => {
  const v = Number(n) || 0;
  return Math.abs(v) < QTY_EPS ? 0 : v;
};

/**
 * Decimals are stored in binary, so 190.88 - 190 gives 0.8799999999999955 and
 * 0.88 x 1000 gives 879.9999999999955. Rounding every quantity (6 decimals) and
 * amount (4 decimals) as it is calculated keeps the saved figures equal to a hand
 * calculation, so all screens and exports agree. Same rule as the AppSail FIFO
 * engine (util/analytics/transactionHistory/fifo.js). `=== 0` turns -0 into 0.
 */
const roundQty = (n) => {
  const v = Math.round((Number(n) || 0) * 1e6) / 1e6;
  return v === 0 ? 0 : v;
};
const roundAmt = (n) => {
  const v = Math.round((Number(n) || 0) * 1e4) / 1e4;
  return v === 0 ? 0 : v;
};

/** When tracking is enabled, written to JobStatusPerAccount.jobType unless params.jobType overrides. */
const HOLDINGS_JOB_TYPE_DEFAULT = "HOLDINGS_FULL_REBUILD";

/** AppSail upload-temp-file sets this — job must not fall back to full DB rebuild. */
const HOLDINGS_UPLOAD_SOURCE = "TxnUpload";

/* ---------------- Optional Jobs / JobStatusPerAccount (see file header) ---------------- */

function extractJobParams(jobRequest) {
  try {
    if (jobRequest && typeof jobRequest.getAllJobParams === "function") {
      const p = jobRequest.getAllJobParams() || {};
      return {
        jobName: String(p.jobName ?? "").trim(),
        jobType: String(p.jobType ?? "").trim() || HOLDINGS_JOB_TYPE_DEFAULT,
      };
    }
  } catch (e) {
    console.warn("extractJobParams:", e.message);
  }
  return { jobName: "", jobType: HOLDINGS_JOB_TYPE_DEFAULT };
}

/** Default Stratus bucket holding the pairs manifest (overridable via `bucketName` param). */
const DEFAULT_META_BUCKET = "client-transaction-files";

/** Normalize a raw array into deduped [WS_Account_code, ISIN] pairs. */
function normalizePairs(arr) {
  if (!Array.isArray(arr) || arr.length === 0) return [];
  const out = [];
  const seen = new Set();
  for (const item of arr) {
    let a = "";
    let i = "";
    if (Array.isArray(item) && item.length >= 2) {
      a = String(item[0] ?? "").trim();
      i = String(item[1] ?? "").trim();
    } else if (item && typeof item === "object") {
      a = String(item.wsAccountCode ?? item.account ?? "").trim();
      i = String(item.isin ?? "").trim();
    }
    if (!a || !i) continue;
    const k = `${a}\t${i}`;
    if (seen.has(k)) continue;
    seen.add(k);
    out.push([a, i]);
  }
  return out;
}

/**
 * Legacy inline mode: parses `pairsJson` job param (JSON array of [WS_Account_code, ISIN]
 * or { wsAccountCode, isin }). Kept for small uploads / backward compatibility.
 * @returns {Array<[string, string]>}
 */
function parseScopedPairsFromJob(jobRequest) {
  try {
    if (!jobRequest || typeof jobRequest.getAllJobParams !== "function") {
      return [];
    }
    const p = jobRequest.getAllJobParams() || {};
    const raw = p.pairsJson ?? p.pairs_json;
    if (raw == null || raw === "") return [];
    const arr = typeof raw === "string" ? JSON.parse(raw) : raw;
    return normalizePairs(arr);
  } catch (e) {
    console.warn("parseScopedPairsFromJob:", e.message);
    return [];
  }
}

function rawToUtf8(raw) {
  if (raw == null) return "";
  if (Buffer.isBuffer(raw)) return raw.toString("utf8");
  if (raw instanceof ArrayBuffer) return Buffer.from(raw).toString("utf8");
  if (typeof raw === "string") return raw;
  return null;
}

async function streamToString(stream) {
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks).toString("utf8");
}

/** Read a Stratus object fully into a UTF-8 string (handles stream / buffer / string). */
async function readStratusObjectAsString(app, bucketName, objectKey) {
  const bucket = app.stratus().bucket(bucketName);
  const raw = await bucket.getObject(objectKey);
  if (raw != null && typeof raw === "object" && typeof raw.pipe === "function") {
    return await streamToString(raw);
  }
  const s = rawToUtf8(raw);
  return s == null ? "" : s;
}

/**
 * Resolve this invocation's scoped pairs. Two modes:
 *   - Slave mode (preferred): `pairsObjectKey` points to a Stratus JSON manifest of
 *     all pairs; `chunkStart` / `chunkCount` select this slave's slice.
 *   - Inline mode (legacy): `pairsJson` carries the pairs directly.
 * @returns {Promise<Array<[string, string]>>}
 */
async function resolveScopedPairs(app, jobRequest) {
  const p =
    jobRequest && typeof jobRequest.getAllJobParams === "function"
      ? jobRequest.getAllJobParams() || {}
      : {};

  const objectKey = String(p.pairsObjectKey ?? p.pairs_object_key ?? "").trim();
  if (!objectKey) {
    return parseScopedPairsFromJob(jobRequest);
  }

  const bucketName =
    String(p.bucketName ?? p.bucket_name ?? "").trim() || DEFAULT_META_BUCKET;

  let text = "";
  try {
    text = await readStratusObjectAsString(app, bucketName, objectKey);
  } catch (e) {
    console.error(
      `resolveScopedPairs: failed to read ${bucketName}/${objectKey}:`,
      e.message,
    );
    return [];
  }

  let arr;
  try {
    arr = JSON.parse(text);
  } catch (e) {
    console.error(`resolveScopedPairs: invalid JSON in ${objectKey}:`, e.message);
    return [];
  }
  if (!Array.isArray(arr) || arr.length === 0) return [];

  const start = Math.max(0, Math.floor(Number(p.chunkStart ?? p.chunk_start) || 0));
  const rawCount = Number(p.chunkCount ?? p.chunk_count);
  const count =
    Number.isFinite(rawCount) && rawCount > 0 ? Math.floor(rawCount) : arr.length;
  const slice = arr.slice(start, start + count);

  console.log(
    `resolveScopedPairs: ${bucketName}/${objectKey} total=${arr.length} ` +
      `slice=[${start}, ${start + count}) -> ${slice.length} item(s)`,
  );

  return normalizePairs(slice);
}

function getJobSource(jobRequest) {
  try {
    if (jobRequest && typeof jobRequest.getAllJobParams === "function") {
      const p = jobRequest.getAllJobParams() || {};
      return String(p.source ?? "").trim();
    }
  } catch (e) {
    console.warn("getJobSource:", e.message);
  }
  return "";
}

function parseImportStartedAtMs(jobRequest) {
  try {
    if (!jobRequest || typeof jobRequest.getAllJobParams !== "function") {
      return 0;
    }
    const p = jobRequest.getAllJobParams() || {};
    const raw = p.importStartedAtMs ?? p.import_started_at_ms;
    const n = Number(raw);
    return Number.isFinite(n) && n > 0 ? Math.floor(n) : 0;
  } catch (e) {
    console.warn("parseImportStartedAtMs:", e.message);
    return 0;
  }
}

/** This slave's first index in the pairs manifest; 0 in inline mode. */
function parseChunkStart(jobRequest) {
  try {
    const p = jobRequest.getAllJobParams() || {};
    return Math.max(0, Math.floor(Number(p.chunkStart ?? p.chunk_start) || 0));
  } catch {
    return 0;
  }
}

function rowFromJobStatusRow(r) {
  if (!r) return null;
  return r.JobStatusPerAccount || r;
}

async function ensureJobsRowRunning(zcql, jobName) {
  try {
    await zcql.executeZCQLQuery(
      `INSERT INTO Jobs (jobName, status) VALUES ('${esc(jobName)}', 'RUNNING')`,
    );
  } catch (insertErr) {
    try {
      await zcql.executeZCQLQuery(
        `UPDATE Jobs SET status = 'RUNNING' WHERE jobName = '${esc(jobName)}'`,
      );
    } catch (upErr) {
      console.warn(`[Jobs] ensure RUNNING failed for ${jobName}:`, upErr.message);
    }
  }
}

async function finalizeJobsRow(zcql, jobName, status) {
  if (!jobName) return;
  try {
    await zcql.executeZCQLQuery(
      `UPDATE Jobs SET status = '${esc(status)}' WHERE jobName = '${esc(jobName)}'`,
    );
  } catch (e) {
    console.error(`[Jobs] finalize failed for ${jobName}:`, e.message);
  }
}

async function getPerAccountJobStatus(zcql, jobName, accountCode) {
  try {
    const rows = await zcql.executeZCQLQuery(`
      SELECT status FROM JobStatusPerAccount
      WHERE jobName = '${esc(jobName)}' AND WS_Account_code = '${esc(accountCode)}'
      LIMIT 1
    `);
    if (!rows?.length) return "";
    const st = rowFromJobStatusRow(rows[0]);
    return String(st?.status ?? "").trim();
  } catch (e) {
    console.warn("[JobStatusPerAccount] read status failed:", e.message);
    return "";
  }
}

async function upsertAccountRowRunning(zcql, jobName, jobType, accountCode) {
  try {
    await zcql.executeZCQLQuery(`
      INSERT INTO JobStatusPerAccount (jobType, WS_Account_code, status, lastError, jobName)
      VALUES (
        '${esc(jobType)}',
        '${esc(accountCode)}',
        'RUNNING',
        '',
        '${esc(jobName)}'
      )
    `);
  } catch (insertErr) {
    try {
      await zcql.executeZCQLQuery(`
        UPDATE JobStatusPerAccount
        SET status = 'RUNNING', lastError = '', jobType = '${esc(jobType)}'
        WHERE jobName = '${esc(jobName)}' AND WS_Account_code = '${esc(accountCode)}'
      `);
    } catch (upErr) {
      console.warn(
        `[JobStatusPerAccount] upsert RUNNING failed ${accountCode}:`,
        upErr.message,
      );
    }
  }
}

async function markAccountSuccess(zcql, jobName, accountCode) {
  try {
    await zcql.executeZCQLQuery(`
      UPDATE JobStatusPerAccount
      SET status = 'SUCCESS', lastError = ''
      WHERE jobName = '${esc(jobName)}' AND WS_Account_code = '${esc(accountCode)}'
    `);
  } catch (e) {
    console.warn(`[JobStatusPerAccount] mark SUCCESS failed ${accountCode}:`, e.message);
  }
}

async function markAccountFailed(zcql, jobName, accountCode, errMsg) {
  const msg = esc(String(errMsg || "UNKNOWN").slice(0, 500));
  try {
    await zcql.executeZCQLQuery(`
      UPDATE JobStatusPerAccount
      SET status = 'FAILED', lastError = '${msg}'
      WHERE jobName = '${esc(jobName)}' AND WS_Account_code = '${esc(accountCode)}'
    `);
  } catch (e) {
    console.warn(`[JobStatusPerAccount] mark FAILED failed ${accountCode}:`, e.message);
  }
}

const sqlDate = (v) => {
  const s = String(v ?? "").trim().slice(0, 10);
  return s && /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : "";
};

const isBuyType = (type) => /^BY-|SQB|OPI/i.test(String(type || ""));

const isSellType = (type) => /^SL\+|SQS|OPO|NF-/i.test(String(type || ""));

/** Build CREATEDTIME floor string for "since this import" filter (matches cash append). */
function createdTimeFloorFromMs(ms) {
  const d = new Date(ms - 15_000);
  const pad = (n) => String(n).padStart(2, "0");
  return (
    `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ` +
    `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}:000`
  );
}

/* ===== TRADE_DATE_MIGRATION: OLD (settlement basis for ALL buys) — retained, do not delete =====
const getEffectiveDate = (r) => {
  const setDate = r.SETDATE || r.setdate;
  const tradeDate = r.TRANDATE || r.trandate;
  return isBuyType(r.Tran_Type || r.tranType)
    ? setDate || tradeDate
    : tradeDate || setDate;
};
   ============================================================================================= */

/* ===== TRADE_DATE_MIGRATION: NEW (trade basis; OPI keeps settlement) =====
   OPI rows are inception/opening positions: TRANDATE is the client's original
   acquisition date (can be years earlier), SETDATE is the date the position was
   onboarded into PMS. Those are two different events, so OPI stays on SETDATE.
   Real broker buys (BY-, SQB) settle T+1 and move to TRANDATE. Sells unchanged. */
const isOpiType = (type) => /^OPI/i.test(String(type || ""));

const getEffectiveDate = (r) => {
  const setDate = r.SETDATE || r.setdate;
  const tradeDate = r.TRANDATE || r.trandate;
  return isOpiType(r.Tran_Type || r.tranType)
    ? setDate || tradeDate
    : tradeDate || setDate;
};

/* ============================== FETCH (batched per pair) ============================== */

/** New transactions for one pair since CSV import (CREATEDTIME floor). */
async function fetchNewTxnsForPair(zcql, accountCode, isin, importStartedAtMs) {
  const createdFloor = createdTimeFloorFromMs(importStartedAtMs);
  const rows = [];
  const seen = new Set();
  let offset = 0;
  while (true) {
    try {
      const batch = await zcql.executeZCQLQuery(`
        SELECT TRANDATE, SETDATE, Tran_Type, QTY, NETRATE, Net_Amount, ISIN, ROWID
        FROM Transaction
        WHERE WS_Account_code = '${esc(accountCode)}'
          AND ISIN = '${esc(isin)}'
          AND CREATEDTIME >= '${createdFloor}'
        ORDER BY SETDATE ASC, ROWID ASC
        LIMIT ${BATCH} OFFSET ${offset}
      `);
      if (!batch || batch.length === 0) break;
      for (const row of batch) {
        const r = row.Transaction || row;
        if (r.ROWID && seen.has(r.ROWID)) continue;
        if (r.ROWID) seen.add(r.ROWID);
        rows.push(r);
      }
      if (batch.length < BATCH) break;
      offset += BATCH;
    } catch (err) {
      console.error(`fetchNewTxnsForPair[${accountCode}/${isin}] offset=${offset}:`, err.message);
      break;
    }
  }
  return rows;
}

/** Last Holdings row for one pair — checkpoint for cheap-append. */
async function fetchLastHoldingsRowForPair(zcql, accountCode, isin) {
  try {
    const rows = await zcql.executeZCQLQuery(`
      SELECT TRANSACTION_DATE, SETTLEMENT_DATE, TYPE, QUANTITY, PRICE, TOTAL_AMOUNT,
             HOLDING, WAP, HOLDING_VALUE, P_L, STATUS
      FROM Holdings
      WHERE WS_Account_code = '${esc(accountCode)}' AND ISIN = '${esc(isin)}'
      ORDER BY CREATEDTIME DESC, ROWID DESC
      LIMIT 1
    `);
    if (!rows || rows.length === 0) return null;
    return rows[0].Holdings || rows[0];
  } catch (e) {
    console.warn(`fetchLastHoldingsRowForPair[${accountCode}/${isin}]:`, e.message);
    return null;
  }
}

/** All Holdings rows for one pair, in canonical FIFO order (insert order). */
async function fetchAllHoldingsRowsForPair(zcql, accountCode, isin) {
  const rows = [];
  // ZCQL's OFFSET is 1-based, so each page repeats the previous page's last
  // row; skip it so that trade is not counted twice.
  const seenRowIds = new Set();
  let offset = 0;
  while (true) {
    try {
      const batch = await zcql.executeZCQLQuery(`
        SELECT TRANSACTION_DATE, SETTLEMENT_DATE, TYPE, QUANTITY, PRICE,
               HOLDING, WAP, HOLDING_VALUE, CA_DATE, ROWID
        FROM Holdings
        WHERE WS_Account_code = '${esc(accountCode)}' AND ISIN = '${esc(isin)}'
        ORDER BY CREATEDTIME ASC, ROWID ASC
        LIMIT ${BATCH} OFFSET ${offset}
      `);
      if (!batch || batch.length === 0) break;
      for (const row of batch) {
        const r = row.Holdings || row;
        if (r.ROWID != null) {
          if (seenRowIds.has(r.ROWID)) continue;
          seenRowIds.add(r.ROWID);
        }
        rows.push(r);
      }
      if (batch.length < BATCH) break;
      offset += BATCH;
    } catch (err) {
      console.error(
        `fetchAllHoldingsRowsForPair[${accountCode}/${isin}] offset=${offset}:`,
        err.message,
      );
      break;
    }
  }
  return rows;
}

/* ============================== LATE TRADES ============================== */

/** ZCQL returns at most this many rows per query. */
const ZCQL_ROW_CAP = 300;

/**
 * Holdings rows are read back in the order they were saved, each carrying the
 * running HOLDING / WAP. A trade dated before rows already saved therefore
 * cannot be added at the end: every later row's running figures would leave it
 * out, and FIFO would use up the lots in the wrong order. Such an account is
 * rebuilt from its trades instead (handOffAccountToRebuild).
 *
 * Same date rule as `holdingsEffectiveDate` in appsail-nodejs
 * util/analytics/holdingsFromTable.js.
 */
const holdingsRowEffectiveDate = (r) => {
  const type = String(r.TYPE || "").trim().toUpperCase();
  const setD = String(r.SETTLEMENT_DATE || "").slice(0, 10);
  const txD = String(r.TRANSACTION_DATE || "").slice(0, 10);
  const caD = String(r.CA_DATE || "").slice(0, 10);
  if ((type === "MERGER" || type === "DEMERGER") && caD) return caD;
  return isOpiType(type) ? setD || txD : txD || setD;
};

/** The earliest date among new trades, or "" when none carries a date. */
const earliestEffectiveDate = (txns) =>
  txns
    .map((t) => String(getEffectiveDate(t) || "").slice(0, 10))
    .filter(Boolean)
    .sort()[0] || "";

/** Does the pair hold a row, saved before this import, dated after `date`? */
async function hasHoldingsRowsDatedAfter(zcql, accountCode, isin, date, createdFloor) {
  const d = esc(date);
  const rows = await zcql.executeZCQLQuery(`
    SELECT TYPE, TRANSACTION_DATE, SETTLEMENT_DATE, CA_DATE
    FROM Holdings
    WHERE WS_Account_code = '${esc(accountCode)}' AND ISIN = '${esc(isin)}'
      AND CREATEDTIME < '${createdFloor}'
      AND (TRANSACTION_DATE > '${d}' OR SETTLEMENT_DATE > '${d}' OR CA_DATE > '${d}')
    LIMIT ${ZCQL_ROW_CAP}
  `);
  const list = (rows || []).map((r) => r.Holdings || r);
  // A full page is taken as "yes" without checking further: a rebuild that was
  // not needed still gives the right holdings.
  if (list.length >= ZCQL_ROW_CAP) return true;
  return list.some((r) => holdingsRowEffectiveDate(r) > date);
}

/**
 * Jobs row for one account this chunk handed to RebuildHoldingtable. It starts
 * with HOLD_<importStartedAtMs>, so a new transaction commit waits for it the
 * same way it waits for the holdings row (findImportsStillProcessing in
 * appsail-nodejs TempTransactionUpload.js). The chunk start keeps two chunks
 * holding ISINs of the same account apart, and lets a retry of this chunk find
 * its own row.
 */
const rebuildJobRowName = (importStartedAtMs, chunkStart, accountCode) =>
  `HOLD_${importStartedAtMs}_R${chunkStart}_${accountCode}`;

async function getJobsRowStatus(zcql, jobName) {
  const rows = await zcql.executeZCQLQuery(
    `SELECT status FROM Jobs WHERE jobName = '${esc(jobName)}' LIMIT 1`,
  );
  if (!rows || rows.length === 0) return "";
  return String((rows[0].Jobs || rows[0]).status || "").trim().toUpperCase();
}

/** Same retries as the rebuilds Transaction Deletion queues. */
const REBUILD_JOB_CONFIG = { number_of_retries: 5, retry_interval: 60 * 1000 };

/**
 * Queue the rebuild of these ISINs for one account: the same rebuild
 * Transaction Deletion uses. It reads every trade of the account, this
 * import's included, so nothing is appended for the account here. Corporate
 * action records stay as saved (keepCaRecords). RebuildHoldingtable sets the
 * row COMPLETED when it finishes.
 */
async function handOffAccountToRebuild(app, accountCode, isins, jobRowName) {
  const zcql = app.zcql();
  await ensureJobsRowRunning(zcql, jobRowName);
  try {
    await app.jobScheduling().JOB.submitJob({
      job_name: `HRB${Date.now()}`.slice(0, 20),
      jobpool_name: "CorporateActions",
      target_name: "RebuildHoldingtable",
      target_type: "Function",
      job_config: REBUILD_JOB_CONFIG,
      params: {
        accountCodesJson: JSON.stringify([accountCode]),
        isinsJson: JSON.stringify(isins),
        source: "TxnUploadLateTrades",
        keepCaRecords: "true",
        importJobRowName: jobRowName,
      },
    });
  } catch (err) {
    // Nothing will end the row; FAILED also tells a retry of this chunk to submit again.
    await finalizeJobsRow(zcql, jobRowName, "FAILED");
    throw err;
  }
}

/* ============================== QUEUE RECONSTRUCTION ============================== */

/**
 * Replay existing Holdings rows (in insert order) to rebuild the active buy
 * queue at the end of recorded history.
 *
 * Row → queue effect:
 *   BY- / SQB / OPI / BONUS                 → push lot {qty, price}
 *   SPLIT / DEMERGER / MERGER               → first row of an event clears the
 *                                              queue; subsequent rows of the
 *                                              same event add a lot each
 *   SL+ / SQS / OPO / NF-                   → consume FIFO from the oldest
 *                                              active lot(s)
 *
 * Returns the queue plus its qty / cost totals — the seed for applying new
 * transactions on top.
 */
function reconstructBuyQueueFromHoldings(rows) {
  const queue = [];
  let lastReplaceKey = null;

  for (const r of rows) {
    const type = String(r.TYPE || "").trim();
    const upper = type.toUpperCase();
    const qty = Number(r.QUANTITY) || 0;
    const price = Number(r.PRICE) || 0;
    // Group all lots of one corporate action together. DEMERGER/MERGER write a
    // shared record date to CA_DATE while each surviving lot carries its own
    // per-lot TRANSACTION_DATE, so keying by TRANSACTION_DATE would wipe the
    // queue on every lot and keep only the last one. SPLIT writes no CA_DATE,
    // so it falls back to TRANSACTION_DATE and behaves exactly as before.
    const caDate = String(r.CA_DATE || "").trim();
    const txnDate = String(r.TRANSACTION_DATE || "").trim();
    const groupDate = caDate || txnDate;

    if (upper === "SPLIT" || upper === "DEMERGER" || upper === "MERGER") {
      const key = `${upper}|${groupDate}`;
      if (key !== lastReplaceKey) queue.length = 0;
      lastReplaceKey = key;
      // Same rule as `recomputeHoldingsSnapshot`: a merger writes one row per
      // lot RECEIVED on the surviving ISIN, and one row on the merged-away ISIN
      // for the quantity that LEFT the account. Both are TYPE='MERGER' with a
      // positive QUANTITY; only the stored HOLDING tells them apart (0 on the
      // closing row). Seeding the queue from a closing row would resurrect a
      // position that no longer exists, and — unlike the read-side snapshot —
      // every Holdings row appended after it would PERSIST that phantom
      // balance. DEMERGER/SPLIT are unaffected: their rows keep a non-zero
      // HOLDING.
      if ((Number(r.HOLDING) || 0) <= 0) continue;
      if (qty > 0) queue.push({ qty, price });
      continue;
    }
    lastReplaceKey = null;

    if (upper === "BONUS" || isBuyType(type)) {
      if (qty > 0) queue.push({ qty, price });
      continue;
    }

    if (isSellType(type)) {
      const holdingBefore = queue.reduce((s, l) => s + l.qty, 0);
      let remaining = Math.min(qty, holdingBefore);
      while (remaining > 0 && queue.length) {
        const lot = queue[0];
        const used = Math.min(lot.qty, remaining);
        lot.qty = roundQty(lot.qty - used);
        remaining = roundQty(remaining - used);
        if (snapQty(lot.qty) === 0) queue.shift();
      }
    }
  }

  const holding = roundQty(queue.reduce((s, l) => s + l.qty, 0));
  const cost = roundAmt(queue.reduce((s, l) => s + l.qty * l.price, 0));
  return { queue, holding, cost };
}

/* ============================== HOLDINGS WRITER ============================== */

async function insertHoldingsRow(zcql, accountCode, row, displayIsin) {
  const txD = sqlDate(row.originalTrandate || row.trandate);
  const setD = sqlDate(row.setdate || row.trandate);
  const typ = String(row.tranType ?? "").trim();
  const qty = Number(row.qty) || 0;
  const price = Number(row.price) || 0;
  const totalAmt = Number(row.netAmount) || 0;
  const holding = Number(row.holdings) || 0;
  const wap = Number(row.averageCostOfHoldings) || 0;
  const hv = Number(row.costOfHoldings) || 0;
  const pl =
    row.profitLoss === null || row.profitLoss === undefined
      ? "NULL"
      : Number(row.profitLoss);
  const status = row.isActive ? "true" : "false";

  await zcql.executeZCQLQuery(`
    INSERT INTO Holdings (
      WS_Account_code, TRANSACTION_DATE, SETTLEMENT_DATE, TYPE, ISIN,
      QUANTITY, PRICE, TOTAL_AMOUNT, HOLDING, WAP, HOLDING_VALUE, P_L, STATUS
    ) VALUES (
      '${esc(accountCode)}',
      '${esc(txD)}',
      '${esc(setD)}',
      '${esc(typ)}',
      '${esc(displayIsin)}',
      ${qty},
      ${price},
      ${totalAmt},
      ${holding},
      ${wap},
      ${hv},
      ${pl},
      ${status}
    )
  `);
}

/* ============================== CHEAP BUY-APPEND ============================== */

/**
 * Append rows for new buys without running the FIFO engine. Uses last Holdings
 * row as checkpoint and updates running qty / cost / WAP per buy.
 *
 * Caller must ensure `newBuyTxns` contains only buy-type transactions.
 *
 * @returns {Promise<number>} appended row count
 */
async function appendBuyRowsCheap(zcql, accountCode, isin, newBuyTxns, counters) {
  const tail = await fetchLastHoldingsRowForPair(zcql, accountCode, isin);
  let holding = Number(tail?.HOLDING) || 0;
  let cost = Number(tail?.HOLDING_VALUE) || 0;

  /* ===== TRADE_DATE_MIGRATION =====
     Previously this loop consumed the SQL `ORDER BY SETDATE ASC` order directly,
     which was correct only because every buy type was settlement-dated. Buys are
     now split (OPI -> SETDATE, BY-/SQB -> TRANDATE) and no single SQL column can
     order a mixed batch, so the authoritative sort happens here instead. The SQL
     ORDER BY is now only a stable pre-sort. */
  const sortedBuyTxns = [...newBuyTxns].sort((a, b) => {
    const eA = String(getEffectiveDate(a) || "");
    const eB = String(getEffectiveDate(b) || "");
    if (eA !== eB) return eA.localeCompare(eB);
    return String(a.ROWID || "").localeCompare(String(b.ROWID || ""));
  });

  let appended = 0;
  for (const t of sortedBuyTxns) {
    const tranType = t.Tran_Type;
    const qty = Math.abs(Number(t.QTY) || 0);
    if (!qty) continue;

    const netRate = Number(t.NETRATE) || 0;
    const netAmount = Number(t.Net_Amount) || 0;
    const price = netRate || (netAmount && qty ? netAmount / qty : 0);

    if (
      String(tranType).toUpperCase() === "OPI" &&
      qty === 1 &&
      price === 0 &&
      netAmount === 0
    ) {
      continue;
    }

    holding = roundQty(holding + qty);
    cost = roundAmt(cost + qty * price);
    const wap = holding > 0 ? roundAmt(cost / holding) : 0;

    const row = {
      tranType,
      trandate: t.TRANDATE,
      originalTrandate: t.TRANDATE,
      setdate: t.SETDATE || t.TRANDATE,
      qty,
      price,
      netAmount,
      holdings: holding,
      costOfHoldings: cost,
      averageCostOfHoldings: wap,
      profitLoss: null,
      isActive: true,
      isin: t.ISIN || isin,
    };

    await insertHoldingsRow(zcql, accountCode, row, isin);
    appended++;
  }

  counters.rows += appended;
  return appended;
}

/**
 * Sell-path append. Rebuilds the active buy queue from existing Holdings,
 * applies each new txn (FIFO consume on sells, push lot on buys), and INSERTs
 * one Holdings row per new non-zero txn. Prior Holdings rows are never touched.
 *
 * @returns {Promise<number>} appended row count
 */
async function appendTxnsWithQueueReconstruct(
  zcql,
  accountCode,
  isin,
  newTxns,
  counters,
) {
  const existingHoldings = await fetchAllHoldingsRowsForPair(
    zcql,
    accountCode,
    isin,
  );
  const { queue, holding: initialHolding, cost: initialCost } =
    reconstructBuyQueueFromHoldings(existingHoldings);

  let holding = initialHolding;
  let cost = initialCost;

  const sortedTxns = [...newTxns].sort((a, b) => {
    const eA = String(getEffectiveDate(a) || "");
    const eB = String(getEffectiveDate(b) || "");
    if (eA !== eB) return eA.localeCompare(eB);
    return String(a.ROWID || "").localeCompare(String(b.ROWID || ""));
  });

  let appended = 0;
  for (const t of sortedTxns) {
    const tranType = t.Tran_Type;
    const qty = Math.abs(Number(t.QTY) || 0);
    if (!qty) continue;

    const netRate = Number(t.NETRATE) || 0;
    const netAmount = Number(t.Net_Amount) || 0;
    const price = netRate || (netAmount && qty ? netAmount / qty : 0);

    if (
      String(tranType).toUpperCase() === "OPI" &&
      qty === 1 &&
      price === 0 &&
      netAmount === 0
    ) {
      continue;
    }

    let profitLoss = null;
    let isActive = true;

    if (isBuyType(tranType)) {
      queue.push({ qty, price });
      holding = roundQty(holding + qty);
      cost = roundAmt(cost + qty * price);
    } else if (isSellType(tranType)) {
      const sellQty = Math.min(qty, holding);
      let remaining = sellQty;
      let fifoCost = 0;
      while (remaining > 0 && queue.length) {
        const lot = queue[0];
        const used = Math.min(lot.qty, remaining);
        fifoCost = roundAmt(fifoCost + used * lot.price);
        lot.qty = roundQty(lot.qty - used);
        remaining = roundQty(remaining - used);
        if (snapQty(lot.qty) === 0) queue.shift();
      }
      holding = roundQty(holding - sellQty);
      cost = roundAmt(cost - fifoCost);
      if (holding === 0) cost = 0;
      profitLoss = roundAmt(sellQty * price - fifoCost);
      isActive = false;
    } else {
      continue;
    }

    const wap = holding > 0 ? roundAmt(cost / holding) : 0;

    const row = {
      tranType,
      trandate: t.TRANDATE,
      originalTrandate: t.TRANDATE,
      setdate: t.SETDATE || t.TRANDATE,
      qty,
      price,
      netAmount,
      holdings: holding,
      costOfHoldings: cost,
      averageCostOfHoldings: wap,
      profitLoss,
      isActive,
      isin: t.ISIN || isin,
    };

    await insertHoldingsRow(zcql, accountCode, row, isin);
    appended++;
  }

  counters.rows += appended;
  return appended;
}

/**
 * Per-(account, ISIN) processing for TxnUpload mode, given this import's new
 * transactions for the pair (none of them dated before rows already saved).
 *   - Buys-only → cheap-append (read last Holdings row, append).
 *   - Any sell present → reconstruct buy queue from existing Holdings, then
 *     append one Holdings row per new txn.
 */
async function processPairTxnUpload(zcql, accountCode, isin, newTxns, counters) {
  counters.pairs++;

  const hasSell = newTxns.some((t) => isSellType(t.Tran_Type));

  if (!hasSell) {
    if (DRY_RUN) {
      console.log(
        `[DRY_RUN][${accountCode}/${isin}] would cheap-append ${newTxns.length} buy txn(s)`,
      );
      counters.rows += newTxns.length;
      return;
    }

    try {
      const appended = await appendBuyRowsCheap(
        zcql,
        accountCode,
        isin,
        newTxns,
        counters,
      );
      console.log(
        `[${accountCode}/${isin}] cheap-append: ${appended} buy row(s) from ${newTxns.length} new txn(s)`,
      );
      return;
    } catch (err) {
      console.error(
        `[${accountCode}/${isin}] cheap-append failed — falling back to queue reconstruct:`,
        err.message,
      );
    }
  } else {
    console.log(
      `[${accountCode}/${isin}] sell in upload — reconstruct queue from Holdings`,
    );
  }

  if (DRY_RUN) {
    console.log(
      `[DRY_RUN][${accountCode}/${isin}] would reconstruct queue + append ${newTxns.length} txn(s)`,
    );
    return;
  }

  try {
    const appended = await appendTxnsWithQueueReconstruct(
      zcql,
      accountCode,
      isin,
      newTxns,
      counters,
    );
    console.log(
      `[${accountCode}/${isin}] appended ${appended} row(s) from ${newTxns.length} new txn(s)`,
    );
  } catch (err) {
    console.error(
      `[${accountCode}/${isin}] queue reconstruct failed:`,
      err.message,
    );
    counters.errors++;
  }
}

/* ============================== PER-ACCOUNT DISPATCH (TxnUpload only) ============================== */

/**
 * Dispatches per-pair processing for one account in TxnUpload mode only.
 *
 * Decided for the whole account: if any new trade is dated before a row already
 * saved for its pair, none is appended and the account's ISINs from this chunk
 * go to RebuildHoldingtable instead. Deciding per account keeps a retry of this
 * chunk simple: the account was either handed off (its Jobs row exists) or not.
 *
 * Caller must guarantee: importStartedAtMs > 0 and scopedIsinsSet has entries.
 */
async function rebuildHoldingsForAccount(
  app,
  zcql,
  accountCode,
  counters,
  scopedIsinsSet,
  importStartedAtMs,
  chunkStart,
) {
  const t0 = Date.now();
  const targetIsins = [...scopedIsinsSet]
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));

  console.log(
    `[${accountCode}] TxnUpload: ${targetIsins.length} ISIN(s) | ` +
      `importStartedAtMs=${importStartedAtMs} (start in ${Date.now() - t0}ms)`,
  );

  // An earlier attempt of this chunk already handed the account off. Appending
  // now would count its trades a second time on top of the rebuild.
  const jobRowName = rebuildJobRowName(importStartedAtMs, chunkStart, accountCode);
  const handOffStatus = await getJobsRowStatus(zcql, jobRowName);
  if (handOffStatus && handOffStatus !== "FAILED") {
    console.log(`[${accountCode}] already handed to RebuildHoldingtable (${jobRowName} ${handOffStatus}) — skip`);
    return;
  }

  const createdFloor = createdTimeFloorFromMs(importStartedAtMs);
  const newTxnsByIsin = new Map();
  let hasLateTrade = false;
  for (const isin of targetIsins) {
    if (MAX_PAIRS > 0 && counters.pairs + newTxnsByIsin.size >= MAX_PAIRS) {
      console.log(`MAX_PAIRS=${MAX_PAIRS} reached, stopping early`);
      break;
    }
    const newTxns = await fetchNewTxnsForPair(zcql, accountCode, isin, importStartedAtMs);
    if (newTxns.length === 0) {
      console.log(`[${accountCode}/${isin}] no new txns since import — skip`);
      continue;
    }
    newTxnsByIsin.set(isin, newTxns);
    if (!hasLateTrade) {
      const earliest = earliestEffectiveDate(newTxns);
      hasLateTrade =
        earliest !== "" &&
        (await hasHoldingsRowsDatedAfter(zcql, accountCode, isin, earliest, createdFloor));
    }
  }

  if (hasLateTrade) {
    const isins = [...newTxnsByIsin.keys()];
    console.log(
      `[${accountCode}] a new trade is dated before saved Holdings rows — ` +
        `rebuilding ${isins.length} ISIN(s) instead of appending (${jobRowName})`,
    );
    if (DRY_RUN) return;
    await handOffAccountToRebuild(app, accountCode, isins, jobRowName);
    counters.pairs += isins.length;
    counters.handedOff++;
    return;
  }

  for (const [isin, newTxns] of newTxnsByIsin) {
    try {
      await processPairTxnUpload(zcql, accountCode, isin, newTxns, counters);
    } catch (err) {
      console.error(
        `[${accountCode}/${isin}] processPairTxnUpload failed:`,
        err.message,
      );
      counters.errors++;
    }
  }
}

/* ============================== ENTRY ============================== */

module.exports = async (jobRequest, context) => {
  const app = catalyst.initialize(context);
  const zcql = app.zcql();

  const { jobName: trackingJobName, jobType: trackingJobType } =
    extractJobParams(jobRequest);
  const trackingOn = Boolean(trackingJobName);

  const startedAt = Date.now();

  try {
    const scopedPairs = await resolveScopedPairs(app, jobRequest);
    const fromTxnUpload = getJobSource(jobRequest) === HOLDINGS_UPLOAD_SOURCE;
    const importStartedAtMs = parseImportStartedAtMs(jobRequest);
    const chunkStart = parseChunkStart(jobRequest);
    // handedOff: accounts sent to RebuildHoldingtable because of a late trade.
    const counters = { pairs: 0, rows: 0, errors: 0, handedOff: 0 };

    // Hard guard: this job is TxnUpload-only. Full rebuilds live in a separate job.
    if (!fromTxnUpload) {
      console.warn(
        `CalculateHoldingWorkers: ignoring invocation — source must be "${HOLDINGS_UPLOAD_SOURCE}". ` +
          `Full rebuilds are handled by a dedicated job.`,
      );
      context.closeWithSuccess();
      return;
    }
    if (scopedPairs.length === 0) {
      console.warn(
        `CalculateHoldingWorkers: source=${HOLDINGS_UPLOAD_SOURCE} but no pairs to process ` +
          `(empty slice or manifest) — nothing to do.`,
      );
      context.closeWithSuccess();
      return;
    }
    if (!importStartedAtMs) {
      console.warn(
        `CalculateHoldingWorkers: source=${HOLDINGS_UPLOAD_SOURCE} but importStartedAtMs missing — nothing to do.`,
      );
      context.closeWithSuccess();
      return;
    }

    /** @type {Map<string, Set<string>>} */
    const byAccount = new Map();
    for (const [acc, isin] of scopedPairs) {
      if (!byAccount.has(acc)) byAccount.set(acc, new Set());
      byAccount.get(acc).add(isin);
    }

    console.log(
      `CalculateHoldingWorkers TxnUpload: ${scopedPairs.length} distinct pair(s) across ${byAccount.size} account(s) | ` +
        `AS_ON_DATE=${AS_ON_DATE ?? "null"} | DRY_RUN=${DRY_RUN} | ` +
        `MAX_PAIRS=${MAX_PAIRS} | importStartedAtMs=${importStartedAtMs} | ` +
        `jobTracking=${trackingOn ? `"${trackingJobName}"` : "off"}`,
    );

    if (trackingOn) {
      await ensureJobsRowRunning(zcql, trackingJobName);
    }

    let ai = 0;
    for (const [accountCode, isinSet] of byAccount) {
      ai++;
      if (MAX_PAIRS > 0 && counters.pairs >= MAX_PAIRS) break;
      console.log(
        `Scoped account ${ai}/${byAccount.size} ${accountCode} (${isinSet.size} ISIN(s))`,
      );

      if (trackingOn) {
        const prev = await getPerAccountJobStatus(
          zcql,
          trackingJobName,
          accountCode,
        );
        if (prev === "SUCCESS") {
          console.log(`[${accountCode}] skip — JobStatusPerAccount already SUCCESS`);
          continue;
        }
        await upsertAccountRowRunning(
          zcql,
          trackingJobName,
          trackingJobType,
          accountCode,
        );
      }

      const errsBeforeAccount = counters.errors;
      try {
        await rebuildHoldingsForAccount(
          app,
          zcql,
          accountCode,
          counters,
          isinSet,
          importStartedAtMs,
          chunkStart,
        );
        if (trackingOn) {
          if (counters.errors > errsBeforeAccount) {
            await markAccountFailed(
              zcql,
              trackingJobName,
              accountCode,
              "One or more scoped ISIN appends logged errors — see Catalyst logs.",
            );
          } else {
            await markAccountSuccess(zcql, trackingJobName, accountCode);
          }
        }
      } catch (err) {
        console.error(
          `[${accountCode}] scoped account append failed:`,
          err.message,
        );
        counters.errors++;
        if (trackingOn) {
          await markAccountFailed(
            zcql,
            trackingJobName,
            accountCode,
            err.message,
          );
        }
      }
    }

    console.log(
      `CalculateHoldingWorkers TxnUpload done in ${Date.now() - startedAt}ms: ` +
        `${counters.pairs} pair(s), ${counters.rows} row(s), ${counters.errors} error(s), ` +
        `${counters.handedOff} account(s) handed to RebuildHoldingtable.`,
    );

    if (trackingOn) {
      await finalizeJobsRow(
        zcql,
        trackingJobName,
        counters.errors > 0 ? "FAILED" : "COMPLETED",
      );
    }

    context.closeWithSuccess();
  } catch (err) {
    console.error("CalculateHoldingWorkers failed:", err);
    if (trackingOn) {
      await finalizeJobsRow(zcql, trackingJobName, "FAILED");
    }
    context.closeWithFailure();
  }
};
