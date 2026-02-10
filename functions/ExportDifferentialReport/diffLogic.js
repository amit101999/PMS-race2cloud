"use strict";

const { runFifoEngine } = require("./fifo.js");

const BATCH = 250;

function toFifoTxnRow(r) {
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
}

async function fetchTransactionRows(zcql, isin, accountCode) {
  const out = [];
  let offset = 0;
  const esc = (s) => String(s).replace(/'/g, "''");
  while (true) {
    const rows = await zcql.executeZCQLQuery(`
      SELECT ROWID, Security_Name, Security_code, Tran_Type, QTY, TRANDATE, NETRATE, ISIN, WS_Account_code
      FROM Transaction
      WHERE ISIN = '${esc(isin)}' AND WS_Account_code = '${esc(accountCode)}'
      ORDER BY ROWID ASC
      LIMIT ${BATCH} OFFSET ${offset}
    `);
    if (!rows?.length) break;
    rows.forEach((r) => out.push(toFifoTxnRow(r)));
    if (rows.length < BATCH) break;
    offset += BATCH;
  }
  return out;
}

async function fetchTempTransactionRows(zcql, isin, accountCode) {
  const out = [];
  let offset = 0;
  const esc = (s) => String(s).replace(/'/g, "''");
  while (true) {
    const rows = await zcql.executeZCQLQuery(`
      SELECT ROWID, Security_Name, Security_code, Tran_Type, QTY, TRANDATE, NETRATE, ISIN, WS_Account_code
      FROM Temp_Transaction
      WHERE ISIN = '${esc(isin)}' AND WS_Account_code = '${esc(accountCode)}'
      ORDER BY ROWID ASC
      LIMIT ${BATCH} OFFSET ${offset}
    `);
    if (!rows?.length) break;
    rows.forEach((r) => out.push(toFifoTxnRow(r)));
    if (rows.length < BATCH) break;
    offset += BATCH;
  }
  return out;
}

async function fetchBonusRows(zcql, isin, accountCode) {
  const out = [];
  let offset = 0;
  const esc = (s) => String(s).replace(/'/g, "''");
  while (true) {
    const rows = await zcql.executeZCQLQuery(`
      SELECT ROWID, BonusShare, ExDate, ISIN
      FROM Bonus
      WHERE ISIN = '${esc(isin)}' AND WS_Account_code = '${esc(accountCode)}'
      ORDER BY ROWID ASC
      LIMIT ${BATCH} OFFSET ${offset}
    `);
    if (!rows?.length) break;
    rows.forEach((r) => {
      const b = r.Bonus || r;
      out.push({ ROWID: b.ROWID, BonusShare: b.BonusShare, ExDate: b.ExDate, ISIN: b.ISIN });
    });
    if (rows.length < BATCH) break;
    offset += BATCH;
  }
  return out;
}

async function fetchSplitRows(zcql, isin) {
  const out = [];
  let offset = 0;
  const esc = (s) => String(s).replace(/'/g, "''");
  while (true) {
    const rows = await zcql.executeZCQLQuery(`
      SELECT ROWID, Issue_Date, Ratio1, Ratio2, ISIN
      FROM Split
      WHERE ISIN = '${esc(isin)}'
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
}

function runPerRowSimulationFull(dbTransactions, fileTransactions, bonuses, splits) {
  let currentTransactions = [...dbTransactions];
  let prevFifo = runFifoEngine(currentTransactions, bonuses, splits, true);
  let lastRow = null;
  for (let i = 0; i < fileTransactions.length; i++) {
    const row = fileTransactions[i];
    currentTransactions.push(row);
    const afterFifo = runFifoEngine(currentTransactions, bonuses, splits, true);
    const afterHolding = afterFifo?.holdings || 0;
    lastRow = {
      WS_Account_code: row.WS_Account_code,
      ISIN: row.ISIN,
      Security_Name: row.Security_Name ?? "",
      newQuantity: afterHolding,
    };
    prevFifo = afterFifo;
  }
  return lastRow;
}

async function getFinalHoldingsForGroup(zcql, accountCode, isin) {
  const dbTransactions = await fetchTransactionRows(zcql, isin, accountCode);
  const tempTransactions = await fetchTempTransactionRows(zcql, isin, accountCode);
  const bonuses = await fetchBonusRows(zcql, isin, accountCode);
  const splits = await fetchSplitRows(zcql, isin);
  const last = runPerRowSimulationFull(dbTransactions, tempTransactions, bonuses, splits);
  return {
    UCC: accountCode,
    ISIN: isin,
    Qty_FA: last ? Number(last.newQuantity) || 0 : 0,
    Security: last?.Security_Name ?? "",
    fromTemp: tempTransactions.length > 0,
  };
}

async function getOrderedDiffKeys(zcql) {
  const keys = new Map();
  const add = (ucc, isin) => {
    const u = (ucc ?? "").toString().trim();
    const i = (isin ?? "").toString().trim();
    if (u && i) keys.set(`${u}__${i}`, { UCC: u, ISIN: i });
  };
  let offset = 0;
  while (true) {
    const rows = await zcql.executeZCQLQuery(`
      SELECT WS_Account_code, ISIN FROM Temp_Transaction ORDER BY WS_Account_code, ISIN LIMIT ${BATCH} OFFSET ${offset}
    `);
    if (!rows?.length) break;
    rows.forEach((r) => {
      const t = r.Temp_Transaction || r;
      add(t.WS_Account_code, t.ISIN);
    });
    if (rows.length < BATCH) break;
    offset += BATCH;
  }
  offset = 0;
  while (true) {
    const rows = await zcql.executeZCQLQuery(`
      SELECT UCC, ISIN FROM Temp_Custodian ORDER BY UCC, ISIN LIMIT ${BATCH} OFFSET ${offset}
    `);
    if (!rows?.length) break;
    rows.forEach((r) => {
      const t = r.Temp_Custodian || r;
      add(t.UCC, t.ISIN);
    });
    if (rows.length < BATCH) break;
    offset += BATCH;
  }
  return Array.from(keys.values()).sort((a, b) => (a.UCC + a.ISIN).localeCompare(b.UCC + b.ISIN));
}

async function getCustodianRow(zcql, ucc, isin) {
  const esc = (s) => String(s).replace(/'/g, "''");
  const rows = await zcql.executeZCQLQuery(`
    SELECT UCC, ISIN, NetBalance, ClientName, SecurityName, LTPDate, LTPPrice
    FROM Temp_Custodian WHERE UCC = '${esc(ucc)}' AND ISIN = '${esc(isin)}' LIMIT 1
  `);
  const r = rows?.[0]?.Temp_Custodian ?? rows?.[0];
  if (!r) return null;
  return {
    UCC: r.UCC ?? ucc,
    ISIN: r.ISIN ?? isin,
    NetBalance: parseFloat(r.NetBalance) || 0,
    ClientName: (r.ClientName ?? "").toString().trim(),
    SecurityName: (r.SecurityName ?? "").toString().trim(),
    LTPDate: (r.LTPDate ?? "").toString().trim(),
    LTPPrice: parseFloat(r.LTPPrice) != null ? parseFloat(r.LTPPrice) : null,
  };
}

function escapeCsv(val) {
  const s = val == null ? "" : String(val);
  if (/[",\r\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

const CSV_HEADER =
  "UCC,ISIN,Qty_FA,Qty_Cust,Diff,Matching,Mismatch_Reason,Security,ClientName,LTPDate,LTPPrice";

async function getAllDiffRows(zcql) {
  const allKeys = await getOrderedDiffKeys(zcql);
  const rows = [];
  for (const k of allKeys) {
    const fa = await getFinalHoldingsForGroup(zcql, k.UCC, k.ISIN);
    const cust = await getCustodianRow(zcql, k.UCC, k.ISIN);
    const qtyFA = fa.Qty_FA;
    const qtyCust = cust ? cust.NetBalance : 0;
    const diff = qtyFA - qtyCust;
    let Mismatch_Reason = "";
    if (!cust) Mismatch_Reason = "Only in FA";
    else if (cust && !fa.fromTemp) Mismatch_Reason = "Only in Custodian";
    else if (diff !== 0) Mismatch_Reason = "Qty Mismatch";
    rows.push({
      UCC: k.UCC,
      ISIN: k.ISIN,
      Qty_FA: qtyFA,
      Qty_Cust: qtyCust,
      Diff: diff,
      Matching: diff === 0 ? "Yes" : "No",
      Mismatch_Reason,
      Security: fa.Security || (cust && cust.SecurityName) || "",
      ClientName: cust ? cust.ClientName : "",
      LTPDate: cust ? cust.LTPDate : "",
      LTPPrice: cust && cust.LTPPrice != null ? cust.LTPPrice : "",
    });
  }
  return rows;
}

function rowsToCsvLines(rows) {
  const lines = [CSV_HEADER];
  for (const r of rows) {
    lines.push(
      [
        escapeCsv(r.UCC),
        escapeCsv(r.ISIN),
        escapeCsv(r.Qty_FA),
        escapeCsv(r.Qty_Cust),
        escapeCsv(r.Diff),
        escapeCsv(r.Matching),
        escapeCsv(r.Mismatch_Reason),
        escapeCsv(r.Security),
        escapeCsv(r.ClientName),
        escapeCsv(r.LTPDate),
        escapeCsv(r.LTPPrice),
      ].join(",")
    );
  }
  return lines.join("\n");
}

module.exports = {
  getAllDiffRows,
  rowsToCsvLines,
  CSV_HEADER,
};
