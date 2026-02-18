"use strict";

const { PassThrough } = require("stream");
const { runFifoEngine } = require("./fifo.js");

const BATCH = 250;
const ACCOUNT_CHUNK = 25;
const esc = (s) => String(s).replace(/'/g, "''");

/* -------------------------------------------------------- */
/* -------------------- FETCH HELPERS --------------------- */
/* -------------------------------------------------------- */

/**
 * Generic paginated fetch: appends LIMIT/OFFSET to the query,
 * maps each raw row through mapFn, returns flat array.
 */
async function fetchAll(zcql, baseQuery, mapFn) {
  const out = [];
  let offset = 0;
  while (true) {
    const rows = await zcql.executeZCQLQuery(
      `${baseQuery} LIMIT ${BATCH} OFFSET ${offset}`
    );
    if (!rows || rows.length === 0) break;
    for (const r of rows) {
      out.push(mapFn(r));
    }
    offset += rows.length;
    if (rows.length < BATCH) break;
  }
  return out;
}

/** Normalize a raw Transaction / Temp_Transaction row. */
function normalizeTxnRow(r) {
  return {
    WS_Account_code: (r.WS_Account_code ?? "").toString().trim(),
    ISIN: (r.ISIN ?? "").toString().trim(),
    Security_Name: r.Security_Name ?? "",
    Tran_Type: r.Tran_Type ?? "",
    QTY: Number(r.QTY) || 0,
    TRANDATE: r.TRANDATE ?? "",
    NETRATE: Number(r.NETRATE) || 0,
  };
}

/* -------------------------------------------------------- */
/* -------------- STEP 1a: SMALL TABLE FETCHES ------------ */
/* -------------------------------------------------------- */

/** Fetch all Temp_Transaction rows (~3-4K, small). */
async function fetchTempTransactions(zcql) {
  return fetchAll(
    zcql,
    `SELECT ROWID, WS_Account_code, ISIN, Security_Name, Security_code,
            Tran_Type, QTY, TRANDATE, NETRATE
     FROM Temp_Transaction
     ORDER BY ROWID ASC`,
    (r) => normalizeTxnRow(r.Temp_Transaction || r)
  );
}

/** Fetch all Temp_Custodian rows (~15K) → Map<"account__isin", custodian data>. */
async function fetchCustodian(zcql) {
  const map = new Map();
  const rows = await fetchAll(
    zcql,
    `SELECT UCC, ISIN, NetBalance, ClientName, SecurityName, LTPDate, LTPPrice
     FROM Temp_Custodian
     ORDER BY ROWID ASC`,
    (r) => r.Temp_Custodian || r
  );
  for (const r of rows) {
    const ucc = (r.UCC ?? "").toString().trim();
    const isin = (r.ISIN ?? "").toString().trim();
    if (!ucc || !isin) continue;
    map.set(`${ucc}__${isin}`, {
      NetBalance: Number(r.NetBalance) || 0,
      ClientName: (r.ClientName ?? "").toString().trim(),
      SecurityName: (r.SecurityName ?? "").toString().trim(),
      LTPDate: (r.LTPDate ?? "").toString().trim(),
      LTPPrice: r.LTPPrice != null && String(r.LTPPrice).trim() !== ""
        ? Number(r.LTPPrice)
        : "",
    });
  }
  return map;
}

/** Fetch all Bonus rows (small table). */
async function fetchAllBonuses(zcql) {
  return fetchAll(
    zcql,
    `SELECT ROWID, WS_Account_code, ISIN, BonusShare, ExDate
     FROM Bonus
     ORDER BY ROWID ASC`,
    (r) => {
      const b = r.Bonus || r;
      return {
        WS_Account_code: (b.WS_Account_code ?? "").toString().trim(),
        ISIN: (b.ISIN ?? "").toString().trim(),
        BonusShare: b.BonusShare,
        ExDate: b.ExDate ?? "",
      };
    }
  );
}

/** Fetch all Split rows (small table). */
async function fetchAllSplits(zcql) {
  return fetchAll(
    zcql,
    `SELECT ROWID, ISIN, Issue_Date, Ratio1, Ratio2
     FROM Split
     ORDER BY ROWID ASC`,
    (r) => {
      const s = r.Split || r;
      return {
        isin: (s.ISIN ?? "").toString().trim(),
        issueDate: s.Issue_Date ?? "",
        ratio1: s.Ratio1,
        ratio2: s.Ratio2,
      };
    }
  );
}

/* -------------------------------------------------------- */
/* -------------- STEP 1b: TARGETED TRANSACTION FETCH ----- */
/* -------------------------------------------------------- */

/**
 * Extract unique account codes from Temp_Transaction rows and Temp_Custodian map.
 * These are the ONLY accounts we need Transaction history for.
 */
function getRelevantAccountCodes(tempTransactionRows, custodianMap) {
  const set = new Set();
  for (const r of tempTransactionRows) {
    if (r.WS_Account_code) set.add(r.WS_Account_code);
  }
  for (const key of custodianMap.keys()) {
    const account = key.split("__")[0];
    if (account) set.add(account);
  }
  return Array.from(set).sort();
}

/**
 * Fetch Transaction rows ONLY for the given account codes.
 * Uses batched WHERE IN (...) to avoid scanning the full 2 lakh+ table.
 *
 * With ~2000 accounts at 25 per batch = ~80 batched queries,
 * each paginated = ~150-250 total DB calls (vs 800+ for full table scan).
 */
async function fetchTransactionsForAccounts(zcql, accountCodes) {
  if (!accountCodes.length) return [];

  const out = [];
  let batchesDone = 0;

  for (let i = 0; i < accountCodes.length; i += ACCOUNT_CHUNK) {
    const chunk = accountCodes.slice(i, i + ACCOUNT_CHUNK);
    const inClause = chunk.map((a) => `'${esc(a)}'`).join(",");
    batchesDone++;

    let offset = 0;
    while (true) {
      const rows = await zcql.executeZCQLQuery(`
        SELECT ROWID, WS_Account_code, ISIN, Security_Name, Security_code,
               Tran_Type, QTY, TRANDATE, NETRATE
        FROM Transaction
        WHERE WS_Account_code IN (${inClause})
        ORDER BY ROWID ASC
        LIMIT ${BATCH} OFFSET ${offset}
      `);
      if (!rows || rows.length === 0) break;
      for (const r of rows) {
        out.push(normalizeTxnRow(r.Transaction || r));
      }
      offset += rows.length;
      if (rows.length < BATCH) break;
    }

    if (batchesDone % 10 === 0) {
      console.log(`[Diff]     ... account batches fetched: ${batchesDone}/${Math.ceil(accountCodes.length / ACCOUNT_CHUNK)}, rows so far: ${out.length}`);
    }
  }

  return out;
}

/* -------------------------------------------------------- */
/* -------------------- GROUPING HELPERS ------------------ */
/* -------------------------------------------------------- */

/** Group rows into Map<string, row[]> using a key function. Skips null keys. */
function groupBy(rows, keyFn) {
  const map = new Map();
  for (const row of rows) {
    const key = keyFn(row);
    if (!key) continue;
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(row);
  }
  return map;
}

/**
 * Merge Transaction + Temp_Transaction into one Map<account__isin, txn[]>,
 * with each group sorted by TRANDATE ASC.
 */
function buildMergedFAMap(transactionRows, tempTransactionRows) {
  const all = [...transactionRows, ...tempTransactionRows];
  const map = groupBy(all, (r) => {
    return r.WS_Account_code && r.ISIN
      ? `${r.WS_Account_code}__${r.ISIN}`
      : null;
  });
  for (const [, list] of map) {
    list.sort((a, b) => (a.TRANDATE || "").localeCompare(b.TRANDATE || ""));
  }
  return map;
}

/** Group bonuses by account__isin. */
function buildBonusMap(bonusRows) {
  return groupBy(bonusRows, (b) => {
    return b.WS_Account_code && b.ISIN
      ? `${b.WS_Account_code}__${b.ISIN}`
      : null;
  });
}

/** Group splits by ISIN (splits are per-ISIN, not per-account). */
function buildSplitMap(splitRows) {
  return groupBy(splitRows, (s) => s.isin || null);
}

/**
 * Build union of ALL keys from Transaction, Temp_Transaction, and Temp_Custodian.
 */
function buildUnionKeys(transactionRows, tempTransactionRows, custodianMap) {
  const set = new Set();
  for (const r of transactionRows) {
    if (r.WS_Account_code && r.ISIN) {
      set.add(`${r.WS_Account_code}__${r.ISIN}`);
    }
  }
  for (const r of tempTransactionRows) {
    if (r.WS_Account_code && r.ISIN) {
      set.add(`${r.WS_Account_code}__${r.ISIN}`);
    }
  }
  for (const key of custodianMap.keys()) {
    set.add(key);
  }
  return Array.from(set).sort();
}

/* -------------------------------------------------------- */
/* -------------------- RECONCILIATION -------------------- */
/* -------------------------------------------------------- */

/**
 * For each key in the reconciliation universe:
 *   1. Get merged FA transactions from mergedFAMap
 *   2. Get bonuses from bonusMap (keyed by account__isin)
 *   3. Get splits from splitMap (keyed by isin)
 *   4. Run runFifoEngine → Qty_FA
 *   5. Get custodian from custodianMap → Qty_Cust
 *   6. Compute Diff, Matching, Mismatch_Reason
 *
 * ZERO database calls — everything from in-memory maps.
 */
function generateDifferentialRows(mergedFAMap, custodianMap, bonusMap, splitMap, unionKeys) {
  const rows = [];

  for (const key of unionKeys) {
    const [accountCode, isin] = key.split("__");

    const faTxns = mergedFAMap.get(key) || [];
    const bonuses = bonusMap.get(key) || [];
    const splits = splitMap.get(isin) || [];

    const fifoResult = runFifoEngine(faTxns, bonuses, splits, true);
    const qtyFA = fifoResult?.holdings ?? 0;

    const cust = custodianMap.get(key) || null;
    const qtyCust = cust ? cust.NetBalance : 0;

    const diff = qtyFA - qtyCust;
    const hasFA = faTxns.length > 0;
    const hasCust = cust !== null;

    let matching = "Yes";
    let mismatchReason = "";

    if (diff !== 0) {
      matching = "No";
      if (hasFA && hasCust) mismatchReason = "Qty Mismatch";
      else if (hasFA && !hasCust) mismatchReason = "Not in Custodian";
      else if (!hasFA && hasCust) mismatchReason = "Not in FA";
    }

    const security =
      (faTxns.length > 0 ? faTxns[faTxns.length - 1].Security_Name : "").trim() ||
      (cust?.SecurityName ?? "").trim();

    rows.push({
      UCC: accountCode,
      ISIN: isin,
      Qty_FA: qtyFA,
      Qty_Cust: qtyCust,
      Diff: diff,
      Matching: matching,
      Mismatch_Reason: mismatchReason,
      Security: security,
      ClientName: cust?.ClientName ?? "",
      LTPDate: cust?.LTPDate ?? "",
      LTPPrice: cust?.LTPPrice ?? "",
    });
  }

  return rows;
}

/* -------------------------------------------------------- */
/* ---------------------- CSV HELPERS --------------------- */
/* -------------------------------------------------------- */

const CSV_HEADER =
  "UCC,ISIN,Qty_FA,Qty_Cust,Diff,Matching,Mismatch_Reason,Security,ClientName,LTPDate,LTPPrice";

function escapeCsv(val) {
  const s = val == null ? "" : String(val);
  return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

function generateCsvStream(rows) {
  const stream = new PassThrough();
  stream.write(CSV_HEADER + "\n");
  for (const r of rows) {
    const line = [
      r.UCC, r.ISIN, r.Qty_FA, r.Qty_Cust, r.Diff,
      r.Matching, r.Mismatch_Reason, r.Security,
      r.ClientName, r.LTPDate, r.LTPPrice,
    ].map(escapeCsv).join(",");
    stream.write(line + "\n");
  }
  stream.end();
  return stream;
}

/* -------------------------------------------------------- */
/* -------------------- MAIN ENTRY POINT ----------------- */
/* -------------------------------------------------------- */

/**
 * Optimized differential export pipeline:
 *
 *   STEP 1 — Fetch small tables first (Temp_Transaction, Temp_Custodian, Bonus, Split)
 *   STEP 2 — Extract unique account codes from Temp_Transaction + Temp_Custodian
 *   STEP 3 — Fetch Transaction rows ONLY for those accounts (batched WHERE IN)
 *            → Skips scanning the full 2 lakh+ table
 *   STEP 4 — Build in-memory maps
 *   STEP 5 — Build union of ALL keys (reconciliation universe)
 *   STEP 6 — Reconcile (FIFO per key, no DB calls)
 *   STEP 7 — Stream CSV to Stratus
 */
async function startDifferentialExportJob(catalystApp, options = {}) {
  const zcql = catalystApp.zcql();
  const bucket = catalystApp.stratus().bucket("upload-data-bucket");

  const timestamp = Date.now();
  const jobName = options.jobName || `DifferentialReport_${timestamp}`;
  const fileName = options.fileName || `temp-files/differential/differential-report-${timestamp}.csv`;

  console.log(`[Diff] Job started: ${jobName}`);

  // STEP 1: Fetch small tables first
  console.log("[Diff] STEP 1: Fetching small tables...");

  const tempTransactionRows = await fetchTempTransactions(zcql);
  console.log(`[Diff]   Temp_Transaction rows: ${tempTransactionRows.length}`);

  const custodianMap = await fetchCustodian(zcql);
  console.log(`[Diff]   Temp_Custodian keys: ${custodianMap.size}`);

  const bonusRows = await fetchAllBonuses(zcql);
  console.log(`[Diff]   Bonus rows: ${bonusRows.length}`);

  const splitRows = await fetchAllSplits(zcql);
  console.log(`[Diff]   Split rows: ${splitRows.length}`);

  // STEP 2: Get relevant account codes (only accounts that matter)
  const accountCodes = getRelevantAccountCodes(tempTransactionRows, custodianMap);
  console.log(`[Diff] STEP 2: Relevant account codes: ${accountCodes.length}`);

  // STEP 3: Fetch Transaction rows ONLY for those accounts (batched)
  console.log(`[Diff] STEP 3: Fetching Transaction rows for ${accountCodes.length} accounts (${Math.ceil(accountCodes.length / ACCOUNT_CHUNK)} batches)...`);
  const transactionRows = await fetchTransactionsForAccounts(zcql, accountCodes);
  console.log(`[Diff]   Transaction rows fetched: ${transactionRows.length}`);

  // STEP 4: Build in-memory maps
  console.log("[Diff] STEP 4: Building in-memory maps...");
  const mergedFAMap = buildMergedFAMap(transactionRows, tempTransactionRows);
  console.log(`[Diff]   Merged FA groups: ${mergedFAMap.size}`);
  const bonusMap = buildBonusMap(bonusRows);
  console.log(`[Diff]   Bonus groups: ${bonusMap.size}`);
  const splitMap = buildSplitMap(splitRows);
  console.log(`[Diff]   Split groups (by ISIN): ${splitMap.size}`);

  // STEP 5: Build reconciliation universe (union of ALL keys)
  const unionKeys = buildUnionKeys(transactionRows, tempTransactionRows, custodianMap);
  console.log(`[Diff] STEP 5: Union keys (reconciliation universe): ${unionKeys.length}`);

  // STEP 6: Reconcile — no DB calls, purely in-memory
  console.log("[Diff] STEP 6: Generating differential rows...");
  const rows = generateDifferentialRows(mergedFAMap, custodianMap, bonusMap, splitMap, unionKeys);
  const matched = rows.filter((r) => r.Matching === "Yes").length;
  const mismatched = rows.length - matched;
  console.log(`[Diff]   Total rows: ${rows.length} | Matched: ${matched} | Mismatched: ${mismatched}`);

  // STEP 7: Stream CSV to Stratus
  console.log("[Diff] STEP 7: Uploading CSV to Stratus...");
  const csvStream = generateCsvStream(rows);
  await bucket.putObject(fileName, csvStream, {
    overwrite: true,
    contentType: "text/csv",
  });
  console.log(`[Diff] DONE — file: ${fileName}`);

  return {
    success: true,
    jobName,
    fileName,
    status: "COMPLETED",
  };
}

module.exports = { startDifferentialExportJob };
