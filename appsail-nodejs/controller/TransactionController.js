import {
  fetchHoldingsRowsForAccountIsin,
  mapHoldingsRowToStockHistoryDto,
  isHoldingsBuyType,
  isHoldingsSellType,
} from "../util/analytics/holdingsFromTable.js";

export const getStockTransactionHistory = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) throw new Error("Catalyst missing");

    const accountCode = String(req.query.accountCode || "").trim();
    const isin = decodeURIComponent(req.query.isin || "").trim();

    if (!accountCode || !isin) {
      return res.status(400).json({
        message: "accountCode and isin are required",
      });
    }

    const zcql = app.zcql();
    const asOnDate = req.query.asOnDate;

    const rows = await fetchHoldingsRowsForAccountIsin(
      zcql,
      accountCode,
      isin,
      asOnDate,
    );

    // Buy/sell totals come from these same rows, so they ride along instead of
    // making the client re-request the identical scan through getAllBuys /
    // getAllSells. Those routes stay for other callers; the predicates below are
    // the shared ones, so all three endpoints keep reporting the same figures.
    // Deliberately two independent `if`s, not else-if — matching the standalone
    // endpoints, where a type could satisfy both tests.
    let totalBuyQty = 0;
    let totalSellQty = 0;
    for (const h of rows) {
      const typ = h.TYPE || h.type;
      const qty = Math.abs(Number(h.QUANTITY) || 0);
      if (isHoldingsBuyType(typ) || String(typ || "").toUpperCase() === "BONUS") {
        totalBuyQty += qty;
      }
      if (isHoldingsSellType(typ)) {
        totalSellQty += qty;
      }
    }

    const result = rows.map((h) => mapHoldingsRowToStockHistoryDto(h, isin));
    return res.json({ rows: result, totalBuyQty, totalSellQty });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
};

export const getTotalBuyQty = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app missing" });
    }

    const accountCode = String(req.query.accountCode || "").trim();
    const isin = decodeURIComponent(req.query.isin || "").trim();
    const asOnDate = req.query.asOnDate;

    if (!accountCode || !isin) {
      return res.status(400).json({
        message: "accountCode and isin are required",
      });
    }

    const zcql = app.zcql();
    const rows = await fetchHoldingsRowsForAccountIsin(
      zcql,
      accountCode,
      isin,
      asOnDate,
    );

    let totalBuyQty = 0;
    for (const h of rows) {
      const typ = h.TYPE || h.type;
      if (isHoldingsBuyType(typ) || String(typ || "").toUpperCase() === "BONUS") {
        totalBuyQty += Math.abs(Number(h.QUANTITY) || 0);
      }
    }

    return res.status(200).json({
      accountCode,
      isin,
      asOnDate: asOnDate || null,
      totalBuyQty,
    });
  } catch (err) {
    console.error("[getTotalBuyQty]", err);
    return res.status(500).json({
      message: "Failed to calculate total buy quantity",
      error: err.message,
    });
  }
};

export const getTotalSellQty = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app missing" });
    }

    const accountCode = String(req.query.accountCode || "").trim();
    const isin = decodeURIComponent(req.query.isin || "").trim();
    const asOnDate = req.query.asOnDate;

    if (!accountCode || !isin) {
      return res.status(400).json({
        message: "accountCode and isin are required",
      });
    }

    const zcql = app.zcql();
    const rows = await fetchHoldingsRowsForAccountIsin(
      zcql,
      accountCode,
      isin,
      asOnDate,
    );

    let totalSellQty = 0;
    for (const h of rows) {
      const typ = h.TYPE || h.type;
      if (isHoldingsSellType(typ)) {
        totalSellQty += Math.abs(Number(h.QUANTITY) || 0);
      }
    }

    return res.status(200).json({
      accountCode,
      isin,
      totalSellQty,
      asOnDate: asOnDate || null,
    });
  } catch (err) {
    console.error("[getTotalSellQty]", err);
    return res.status(500).json({
      message: "Failed to calculate total sell quantity",
      error: err.message,
    });
  }
};

/* ── Transaction Deletion (preview only — these handlers never modify data) ── */

const esc = (s) => String(s ?? "").replace(/'/g, "''");

const isIsoDate = (s) => /^\d{4}-\d{2}-\d{2}$/.test(s);

/** "2026-10-05" → "2026-10-06" (pure calendar math, independent of server TZ). */
function nextIsoDay(isoDay) {
  const [y, m, d] = isoDay.split("-").map(Number);
  return new Date(Date.UTC(y, m - 1, d + 1)).toISOString().slice(0, 10);
}

/**
 * CREATEDTIME is an IST wall-clock string ("2026-10-05 17:41:28:675"), so a
 * commit day is just its first 10 characters and a day range compares strings
 * in the same format the cash/holdings jobs already use.
 */
function commitDayCondition(isoDay) {
  return (
    `CREATEDTIME >= '${isoDay} 00:00:00:000' ` +
    `AND CREATEDTIME < '${nextIsoDay(isoDay)} 00:00:00:000'`
  );
}

function readCount(rows) {
  const r = rows?.[0]?.Transaction || rows?.[0] || {};
  return Number(r["COUNT(ROWID)"] ?? Object.values(r)[0] ?? 0) || 0;
}

/** Upper bound on commit days walked, so a bad CREATEDTIME value can't loop forever. */
const MAX_COMMIT_DAYS = 3000;

/**
 * GET /api/transaction/commit-dates
 * Every day on which rows were committed to Transaction, with row counts, newest first.
 * Walks forward one commit day at a time (first row at/after the cursor, then a
 * COUNT for that day), so it only costs two queries per day that has data.
 */
export const getCommitDates = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app missing" });
    }
    const zcql = app.zcql();

    const dates = [];
    let cursorDay = null;
    while (dates.length < MAX_COMMIT_DAYS) {
      const where = cursorDay
        ? `WHERE CREATEDTIME >= '${cursorDay} 00:00:00:000'`
        : "";
      const firstRows = await zcql.executeZCQLQuery(
        `SELECT CREATEDTIME FROM Transaction ${where} ORDER BY CREATEDTIME ASC LIMIT 1`,
      );
      if (!firstRows || firstRows.length === 0) break;

      const createdTime = String(
        (firstRows[0].Transaction || firstRows[0]).CREATEDTIME || "",
      );
      const day = createdTime.slice(0, 10);
      if (!isIsoDate(day)) {
        throw new Error(`Unexpected CREATEDTIME format: "${createdTime}"`);
      }

      const countRows = await zcql.executeZCQLQuery(
        `SELECT COUNT(ROWID) FROM Transaction WHERE ${commitDayCondition(day)}`,
      );
      dates.push({ date: day, count: readCount(countRows) });
      cursorDay = nextIsoDay(day);
    }

    dates.reverse();
    return res.status(200).json({ dates });
  } catch (err) {
    console.error("[getCommitDates]", err);
    return res.status(500).json({
      message: "Failed to load commit dates",
      error: err.message,
    });
  }
};

const COMMITTED_COLUMNS = [
  "ROWID",
  "CREATEDTIME",
  "WS_Account_code",
  "TRANDATE",
  "SETDATE",
  "Tran_Type",
  "ISIN",
  "Security_code",
  "Security_Name",
  "QTY",
  "RATE",
  "Net_Amount",
];

/**
 * GET /api/transaction/committed?date=&page=&pageSize=&isin=&securityCode=&clientCode=&tranDate=&setDate=
 * One page of Transaction rows committed on `date` (system commit day), plus the total.
 */
export const getCommittedTransactions = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app missing" });
    }

    const date = String(req.query.date || "").trim();
    const isin = String(req.query.isin || "").trim();
    const securityCode = String(req.query.securityCode || "").trim();
    const clientCode = String(req.query.clientCode || "").trim();
    const tranDate = String(req.query.tranDate || "").trim();
    const setDate = String(req.query.setDate || "").trim();
    const page = Math.max(1, Math.floor(Number(req.query.page) || 1));
    const pageSize = Math.min(
      100,
      Math.max(1, Math.floor(Number(req.query.pageSize) || 50)),
    );

    if (!isIsoDate(date)) {
      return res.status(400).json({ message: "date is required (yyyy-mm-dd)" });
    }
    if ((tranDate && !isIsoDate(tranDate)) || (setDate && !isIsoDate(setDate))) {
      return res
        .status(400)
        .json({ message: "tranDate and setDate must be yyyy-mm-dd" });
    }

    const conditions = [commitDayCondition(date)];
    if (isin) {
      conditions.push(
        `(ISIN LIKE '*${esc(isin)}*' OR Security_Name LIKE '*${esc(isin)}*')`,
      );
    }
    if (securityCode) conditions.push(`Security_code = '${esc(securityCode)}'`);
    if (clientCode) conditions.push(`WS_Account_code = '${esc(clientCode)}'`);
    if (tranDate) conditions.push(`TRANDATE = '${tranDate}'`);
    if (setDate) conditions.push(`SETDATE = '${setDate}'`);
    const where = conditions.join(" AND ");

    const zcql = app.zcql();
    const countRows = await zcql.executeZCQLQuery(
      `SELECT COUNT(ROWID) FROM Transaction WHERE ${where}`,
    );
    const total = readCount(countRows);

    // ZCQL's OFFSET is 1-based (OFFSET k skips only k-1 rows), so skipping
    // `skip` rows needs OFFSET skip + 1; otherwise each page repeats the
    // previous page's last row.
    const skip = (page - 1) * pageSize;
    const offsetClause = skip > 0 ? ` OFFSET ${skip + 1}` : "";
    const pageRows = await zcql.executeZCQLQuery(
      `SELECT ${COMMITTED_COLUMNS.join(", ")} FROM Transaction WHERE ${where} ` +
        `ORDER BY ROWID ASC LIMIT ${pageSize}${offsetClause}`,
    );

    const rows = (pageRows || []).map((r) => r.Transaction || r);
    return res.status(200).json({ rows, total, page, pageSize });
  } catch (err) {
    console.error("[getCommittedTransactions]", err);
    return res.status(500).json({
      message: "Failed to load committed transactions",
      error: err.message,
    });
  }
};
