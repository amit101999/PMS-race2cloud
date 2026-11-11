/*
 * Copied from appsail-nodejs/util/analytics/holdingsFromTable.js, only the parts
 * this app needs. If the date rule, the buy/sell tests or the Holdings columns
 * change there, change them here too.
 */

const BATCH = 250;

function escHoldingsSql(s) {
  return String(s ?? "").replace(/'/g, "''");
}

function holdingsDateFilterClause(asOnDate) {
  if (!asOnDate || !/^\d{4}-\d{2}-\d{2}$/.test(String(asOnDate).trim())) return "";
  const nextDay = new Date(asOnDate);
  nextDay.setDate(nextDay.getDate() + 1);
  const cutoff = nextDay.toISOString().split("T")[0];
  return ` AND (SETTLEMENT_DATE < '${escHoldingsSql(cutoff)}' OR TRANSACTION_DATE < '${escHoldingsSql(cutoff)}')`;
}

function isHoldingsBuyType(type) {
  return /^BY-|SQB|OPI/i.test(String(type || ""));
}

function isHoldingsSellType(type) {
  return /^SL\+|SQS|OPO|NF-/i.test(String(type || "").trim());
}

function isHoldingsOpiType(type) {
  return /^OPI/i.test(String(type || "").trim());
}

function isHoldingsReplaceType(type) {
  const t = String(type || "").trim().toUpperCase();
  return t === "MERGER" || t === "DEMERGER";
}

/**
 * The date a Holdings row takes effect: the trade date, except OPI (settlement
 * date) and merger / demerger rows, which keep each lot's old trade date and
 * carry the event's date in CA_DATE.
 */
function holdingsEffectiveDate(row) {
  const setD = String(row.SETTLEMENT_DATE || "").slice(0, 10);
  const txD = String(row.TRANSACTION_DATE || "").slice(0, 10);
  if (isHoldingsReplaceType(row.TYPE)) {
    const caD = String(row.CA_DATE || "").slice(0, 10);
    if (caD) return caD;
  }
  return isHoldingsOpiType(row.TYPE) ? setD || txD : txD || setD;
}

/** Numeric compare of ROWID strings (they exceed Number precision). */
function compareRowIds(a, b) {
  const ra = String(a ?? "0");
  const rb = String(b ?? "0");
  if (ra.length !== rb.length) return ra.length - rb.length;
  return ra < rb ? -1 : ra > rb ? 1 : 0;
}

function compareFifoOrder(a, b) {
  const ta = String(a.CREATEDTIME ?? "");
  const tb = String(b.CREATEDTIME ?? "");
  if (ta !== tb) return ta < tb ? -1 : 1;
  return compareRowIds(a.ROWID, b.ROWID);
}

/**
 * All Holdings rows for an account in the order they were created, which is the
 * order HOLDING runs in. Paged by ROWID, not OFFSET: ZCQL's OFFSET repeats the
 * row at each page boundary, and a repeated sell would look like a shortfall.
 */
async function fetchHoldingsRowsPaged(zcql, accountCode, asOnDate, extraSql = "") {
  const clause = holdingsDateFilterClause(asOnDate);
  const rows = [];
  let lastRowId = "0";
  const acc = escHoldingsSql(accountCode);
  while (true) {
    const batch = await zcql.executeZCQLQuery(`
      SELECT ROWID, TRANSACTION_DATE, SETTLEMENT_DATE, TYPE, ISIN,
             QUANTITY, PRICE, TOTAL_AMOUNT, HOLDING, WAP, HOLDING_VALUE, P_L, STATUS,
             CA_DATE, CREATEDTIME
      FROM Holdings
      WHERE WS_Account_code = '${acc}'
        AND ROWID > ${lastRowId}
      ${clause}
      ${extraSql}
      ORDER BY ROWID ASC
      LIMIT ${BATCH}
    `);
    if (!batch || batch.length === 0) break;
    let maxRowId = lastRowId;
    for (const r of batch) {
      const h = r.Holdings || r;
      rows.push(h);
      if (compareRowIds(h.ROWID, maxRowId) > 0) maxRowId = String(h.ROWID);
    }
    // Guard against a non-advancing cursor so a bad ROWID can't loop forever.
    if (maxRowId === lastRowId) break;
    lastRowId = maxRowId;
    if (batch.length < BATCH) break;
  }
  rows.sort(compareFifoOrder);

  // The SQL date clause is loose (it ORs both dates), so apply the real rule.
  const trimmed = String(asOnDate ?? "").trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    const nextDay = new Date(trimmed);
    nextDay.setDate(nextDay.getDate() + 1);
    const cutoff = nextDay.toISOString().split("T")[0];
    return rows.filter((r) => {
      const d = holdingsEffectiveDate(r);
      return !d || d < cutoff;
    });
  }

  return rows;
}

async function fetchHoldingsRowsForAccountIsin(zcql, accountCode, isin, asOnDate) {
  const isinTrim = String(isin ?? "").trim();
  if (!isinTrim) return [];
  const extra = ` AND ISIN = '${escHoldingsSql(isinTrim)}' `;
  return fetchHoldingsRowsPaged(zcql, accountCode, asOnDate, extra);
}

async function fetchSecurityListByIsins(zcql, isins) {
  const map = Object.create(null);
  if (!isins || !isins.length) return map;
  for (let i = 0; i < isins.length; i += 200) {
    const part = isins.slice(i, i + 200);
    const inClause = part.map((isin) => `'${escHoldingsSql(isin)}'`).join(",");
    let offset = 0;
    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT ISIN, Security_Code, Security_Name FROM Security_List
        WHERE ISIN IN (${inClause})
        ORDER BY ROWID ASC
        LIMIT ${BATCH} OFFSET ${offset}
      `);
      if (!batch || batch.length === 0) break;
      for (const r of batch) {
        const row = r.Security_List || r;
        if (row.ISIN) {
          map[String(row.ISIN).trim()] = {
            securityCode: row.Security_Code || "",
            securityName: row.Security_Name || "",
          };
        }
      }
      if (batch.length < BATCH) break;
      offset += BATCH;
    }
  }
  return map;
}

/** Holdings row → the shape the history popup expects. */
function mapHoldingsRowToStockHistoryDto(h, isinFromQuery) {
  const plRaw = h.P_L;
  const plNum = plRaw === null || plRaw === undefined || plRaw === "" ? null : Number(plRaw);

  let isActive = true;
  const st = h.STATUS;
  if (typeof st === "boolean") isActive = st;
  else if (String(st).toLowerCase() === "false") isActive = false;

  const txD = String(h.TRANSACTION_DATE || "").slice(0, 10);
  const setD = String(h.SETTLEMENT_DATE || "").slice(0, 10);

  return {
    rowId: h.ROWID != null ? String(h.ROWID) : null,
    trandate: txD || setD || null,
    originalTrandate: txD || setD || null,
    setdate: setD || txD || null,
    tranType: String(h.TYPE || "").trim() || "",
    qty: Number(h.QUANTITY) || 0,
    price: Number(h.PRICE) || 0,
    netAmount: Number(h.TOTAL_AMOUNT) || 0,
    holdings: Number(h.HOLDING) || 0,
    costOfHoldings: Number(h.HOLDING_VALUE) || 0,
    averageCostOfHoldings: Number(h.WAP) || 0,
    profitLoss: Number.isFinite(plNum) ? plNum : null,
    isActive,
    isin: String(h.ISIN || isinFromQuery || "").trim(),
    caDate: String(h.CA_DATE || "").slice(0, 10) || null,
  };
}

module.exports = {
  holdingsEffectiveDate,
  isHoldingsBuyType,
  isHoldingsSellType,
  fetchHoldingsRowsPaged,
  fetchHoldingsRowsForAccountIsin,
  fetchSecurityListByIsins,
  mapHoldingsRowToStockHistoryDto,
};
