"use strict";

const { runFifoEngine } = require("./fifo.js");

const MAX_ZCQL_ROWS = 300;
const BATCH = Math.min(250, MAX_ZCQL_ROWS);
const esc = (s) => String(s).replace(/'/g, "''");

/* ------------------------------------------------ */
/* ----------------- DB HELPERS ------------------- */
/* ------------------------------------------------ */

const toFifoTxnRow = (r) => {
  const t = r.Transaction || r.Temp_Transaction || r;
  return {
    WS_Account_code: t.WS_Account_code,
    ISIN: t.ISIN,
    Security_code: t.Security_code ?? "",
    Security_Name: t.Security_Name ?? "",
    Tran_Type: t.Tran_Type,
    QTY: Number(t.QTY) || 0,
    TRANDATE: t.TRANDATE ?? "",
    NETRATE: Number(t.NETRATE) || 0,
  };
};

const fetchTxnRows = async (zcql, tableName, isin, accountCode) => {
  const out = [];
  let offset = 0;

  while (true) {
    const rows = await zcql.executeZCQLQuery(`
      SELECT ROWID, Security_Name, Security_code, Tran_Type, QTY, TRANDATE, NETRATE, ISIN, WS_Account_code
      FROM ${tableName}
      WHERE ISIN='${esc(isin)}' AND WS_Account_code='${esc(accountCode)}'
      ORDER BY ROWID ASC
      LIMIT ${BATCH} OFFSET ${offset}
    `);

    if (!rows?.length) break;
    rows.forEach((r) => out.push(toFifoTxnRow(r)));
    if (rows.length < BATCH) break;
    offset += BATCH;
  }

  return out;
};

const fetchBonusRows = async (zcql, isin, accountCode) => {
  const out = [];
  let offset = 0;

  while (true) {
    const rows = await zcql.executeZCQLQuery(`
      SELECT ROWID, BonusShare, ExDate, ISIN
      FROM Bonus
      WHERE ISIN='${esc(isin)}' AND WS_Account_code='${esc(accountCode)}'
      ORDER BY ROWID ASC
      LIMIT ${BATCH} OFFSET ${offset}
    `);

    if (!rows?.length) break;

    rows.forEach((r) => {
      const b = r.Bonus || r;
      out.push({
        ROWID: b.ROWID,
        BonusShare: b.BonusShare,
        ExDate: b.ExDate,
        ISIN: b.ISIN,
      });
    });

    if (rows.length < BATCH) break;
    offset += BATCH;
  }

  return out;
};

const fetchSplitRows = async (zcql, isin) => {
  const out = [];
  let offset = 0;

  while (true) {
    const rows = await zcql.executeZCQLQuery(`
      SELECT ROWID, Issue_Date, Ratio1, Ratio2, ISIN
      FROM Split
      WHERE ISIN='${esc(isin)}'
      ORDER BY ROWID ASC
      LIMIT ${BATCH} OFFSET ${offset}
    `);

    if (!rows?.length) break;

    rows.forEach((r) => {
      const s = r.Split || r;
      out.push({
        issueDate: s.Issue_Date,
        ratio1: s.Ratio1,
        ratio2: s.Ratio2,
        isin: s.ISIN,
      });
    });

    if (rows.length < BATCH) break;
    offset += BATCH;
  }

  return out;
};

/* ------------------------------------------------ */
/* -------- GET UNIQUE GROUPS FROM Temp_Transaction */
/* ------------------------------------------------ */
/**
 * Scans ONLY Temp_Transaction to build the list of unique (accountCode, isin)
 * groups, counting how many rows each group has.
 *
 * ── WHY THE OLD CODE GAVE 104 ROWS INSTEAD OF 699 ──
 * The previous version of getAllGroups also scanned Transaction and
 * Temp_Custodian tables. For any group that existed in those tables but had
 * ZERO rows in Temp_Transaction, it pushed exactly ONE summary row per group
 * instead of one-row-per-temp-transaction. With ~595 such collapsed groups the
 * total came out at ~104 instead of ~699.
 *
 * Fix: only scan Temp_Transaction — identical to what the UI does.
 */
const getAllGroups = async (zcql) => {
  const groupCounts = new Map();
  let offset = 0;

  while (true) {
    const rows = await zcql.executeZCQLQuery(`
      SELECT WS_Account_code, ISIN
      FROM Temp_Transaction
      ORDER BY WS_Account_code, ISIN, ROWID
      LIMIT ${MAX_ZCQL_ROWS} OFFSET ${offset}
    `);

    if (!rows?.length) break;

    for (const r of rows) {
      const t = r.Temp_Transaction || r;
      const accountCode = (t.WS_Account_code ?? "").toString().trim();
      const isin = (t.ISIN ?? "").toString().trim();
      if (!accountCode || !isin) continue;

      const key = `${accountCode}__${isin}`;
      const existing = groupCounts.get(key);
      if (existing) {
        existing.count += 1;
      } else {
        groupCounts.set(key, { accountCode, isin, count: 1 });
      }
    }

    if (rows.length < MAX_ZCQL_ROWS) break;
    offset += MAX_ZCQL_ROWS;
  }

  return Array.from(groupCounts.values())
    .filter((g) => g.count > 0)
    .sort((a, b) =>
      (a.accountCode + a.isin).localeCompare(b.accountCode + b.isin)
    );
};

/* ------------------------------------------------ */
/* -------- FINAL HOLDINGS (CORRECT FIFO) -------- */
/* ------------------------------------------------ */
/**
 * Combines DB transactions + already-fetched temp transactions and runs FIFO.
 *
 * tempTransactions are PASSED IN from the caller — never re-fetched here.
 * This halves DB calls per group and prevents Catalyst function timeouts.
 */
const getFinalHoldingsForGroup = async (
  zcql,
  accountCode,
  isin,
  tempTransactions  // passed in — do NOT re-fetch inside
) => {
  const dbTransactions = await fetchTxnRows(zcql, "Transaction", isin, accountCode);
  const bonuses = await fetchBonusRows(zcql, isin, accountCode);
  const splits = await fetchSplitRows(zcql, isin);

  const allTransactions = [...dbTransactions, ...tempTransactions];
  const fifoResult = runFifoEngine(allTransactions, bonuses, splits, true);

  const security =
    tempTransactions[tempTransactions.length - 1]?.Security_Name ||
    dbTransactions[dbTransactions.length - 1]?.Security_Name ||
    "";

  return {
    Qty_FA: fifoResult?.holdings || 0,
    Security: security,
  };
};

/* ------------------------------------------------ */
/* -------- GET CUSTODIAN ROW -------------------- */
/* ------------------------------------------------ */

const getCustodianRow = async (zcql, ucc, isin) => {
  const rows = await zcql.executeZCQLQuery(`
    SELECT UCC, ISIN, NetBalance, ClientName, SecurityName, LTPDate, LTPPrice
    FROM Temp_Custodian
    WHERE UCC='${esc(ucc)}' AND ISIN='${esc(isin)}'
    LIMIT 1
  `);

  const r = rows?.[0]?.Temp_Custodian ?? rows?.[0];
  if (!r) return null;

  const hasKey =
    (r.UCC != null && String(r.UCC).trim() !== "") ||
    (r.ISIN != null && String(r.ISIN).trim() !== "");
  if (!hasKey) return null;

  return {
    NetBalance: parseFloat(r.NetBalance) || 0,
    ClientName: (r.ClientName ?? "").trim(),
    SecurityName: (r.SecurityName ?? "").trim(),
    LTPDate: (r.LTPDate ?? "").trim(),
    LTPPrice: parseFloat(r.LTPPrice) != null ? parseFloat(r.LTPPrice) : "",
  };
};

/* ------------------------------------------------ */
/* -------- MAIN EXPORT LOGIC -------------------- */
/* ------------------------------------------------ */
/**
 * Produces ONE output row per Temp_Transaction row — exactly matching the
 * "Comparison (FA vs Custodian)" table in the UI.
 *
 * Per group:
 *   1. fetchTxnRows(Temp_Transaction) → N rows → N CSV output rows
 *   2. getFinalHoldingsForGroup — reuses those N rows + fetches DB rows via FIFO
 *   3. getCustodianRow → single custodian balance for the comparison columns
 *   4. Emit one entry per temp row (all share the same computed Qty_FA / Qty_Cust / Diff)
 */
async function getAllDiffRows(zcql) {
  const groups = await getAllGroups(zcql);
  const data = [];

  console.log(`[DiffLogic] ${groups.length} unique (UCC, ISIN) groups found`);

  for (const g of groups) {
    // ── 1. Fetch all Temp_Transaction rows for this group ONCE ───────────────
    const tempTransactions = await fetchTxnRows(
      zcql,
      "Temp_Transaction",
      g.isin,
      g.accountCode
    );

    if (!tempTransactions.length) continue;

    // ── 2. Compute final FIFO holding (reuses tempTransactions — no re-fetch) ─
    const fa = await getFinalHoldingsForGroup(
      zcql,
      g.accountCode,
      g.isin,
      tempTransactions
    );

    // ── 3. Get custodian balance ──────────────────────────────────────────────
    const cust = await getCustodianRow(zcql, g.accountCode, g.isin);

    const qtyFA = fa.Qty_FA;
    const qtyCust = cust ? cust.NetBalance : 0;
    const diff = qtyFA - qtyCust;

    const Mismatch_Reason = !cust
      ? "Not in custodian file"
      : diff !== 0
      ? "Qty Mismatch"
      : "";

    // ── 4. One CSV row per uploaded transaction row ───────────────────────────
    for (const row of tempTransactions) {
      data.push({
        UCC: g.accountCode,
        ISIN: g.isin,
        Qty_FA: qtyFA,
        Qty_Cust: qtyCust,
        Diff: diff,
        Matching: diff === 0 ? "Yes" : "No",
        Mismatch_Reason,
        Security:
          (row.Security_Name ?? "").trim() ||
          fa.Security ||
          cust?.SecurityName ||
          "",
        ClientName: cust?.ClientName || "",
        LTPDate: cust?.LTPDate || "",
        LTPPrice: cust?.LTPPrice ?? "",
      });
    }
  }

  console.log(`[DiffLogic] Total export rows: ${data.length}`);
  return data;
}

/* ------------------------------------------------ */
/* ---------------- CSV HELPERS ------------------ */
/* ------------------------------------------------ */

const CSV_HEADER =
  "UCC,ISIN,Qty_FA,Qty_Cust,Diff,Matching,Mismatch_Reason,Security,ClientName,LTPDate,LTPPrice";

function escapeCsv(val) {
  const s = val == null ? "" : String(val);
  return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

function rowsToCsvLines(rows) {
  return [
    CSV_HEADER,
    ...rows.map((r) =>
      [
        r.UCC,
        r.ISIN,
        r.Qty_FA,
        r.Qty_Cust,
        r.Diff,
        r.Matching,
        r.Mismatch_Reason,
        r.Security,
        r.ClientName,
        r.LTPDate,
        r.LTPPrice,
      ]
        .map(escapeCsv)
        .join(",")
    ),
  ].join("\n");
}

module.exports = { getAllDiffRows, rowsToCsvLines, CSV_HEADER };