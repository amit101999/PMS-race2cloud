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

/* ── Transaction Deletion ──
 * Deleting removes the Transaction rows, keeps a copy of each in
 * Transaction_Deletion_Log (the page's Deletion History), and queues the
 * existing holdings and cash passbook rebuilds for the affected clients.
 * Corporate action tables (Bonus, Split, Merger, Demerger, Dividend records)
 * are not changed; the holdings rebuild replays them as saved. The only
 * exception is cash: dividend credits earned through a deleted buy are left
 * out of the passbook (Cal_CB_Per_TNX reads Transaction_Deletion_Log). */

const LOG_TABLE = "Transaction_Deletion_Log";

const esc = (s) => String(s ?? "").replace(/'/g, "''");

const isIsoDate = (s) => /^\d{4}-\d{2}-\d{2}$/.test(s);

/** "2026-10-05" → "2026-10-06" (pure calendar math, independent of server TZ). */
function nextIsoDay(isoDay) {
  const [y, m, d] = isoDay.split("-").map(Number);
  return new Date(Date.UTC(y, m - 1, d + 1)).toISOString().slice(0, 10);
}

/**
 * CREATEDTIME is an IST wall-clock string ("2026-10-05 17:41:28:675"), so a
 * day is just its first 10 characters and a day range compares strings in the
 * same format the cash/holdings jobs already use. For Transaction that day is
 * the commit day; for the deletion log it is the day the rows were deleted.
 */
function createdOnDayCondition(isoDay) {
  return (
    `CREATEDTIME >= '${isoDay} 00:00:00:000' ` +
    `AND CREATEDTIME < '${nextIsoDay(isoDay)} 00:00:00:000'`
  );
}

function readCount(rows, tableName) {
  const r = rows?.[0]?.[tableName] || rows?.[0] || {};
  return Number(r["COUNT(ROWID)"] ?? Object.values(r)[0] ?? 0) || 0;
}

/** Upper bound on days walked, so a bad CREATEDTIME value can't loop forever. */
const MAX_CREATED_DAYS = 3000;

/**
 * Every day on which rows were created in `tableName`, with row counts, newest
 * first. Walks forward one day at a time (first row at/after the cursor, then a
 * COUNT for that day), so it only costs two queries per day that has data.
 */
async function countRowsPerCreatedDay(zcql, tableName) {
  const dates = [];
  let cursorDay = null;
  while (dates.length < MAX_CREATED_DAYS) {
    const where = cursorDay
      ? `WHERE CREATEDTIME >= '${cursorDay} 00:00:00:000'`
      : "";
    const firstRows = await zcql.executeZCQLQuery(
      `SELECT CREATEDTIME FROM ${tableName} ${where} ORDER BY CREATEDTIME ASC LIMIT 1`,
    );
    if (!firstRows || firstRows.length === 0) break;

    const createdTime = String(
      (firstRows[0][tableName] || firstRows[0]).CREATEDTIME || "",
    );
    const day = createdTime.slice(0, 10);
    if (!isIsoDate(day)) {
      throw new Error(`Unexpected CREATEDTIME format: "${createdTime}"`);
    }

    const countRows = await zcql.executeZCQLQuery(
      `SELECT COUNT(ROWID) FROM ${tableName} WHERE ${createdOnDayCondition(day)}`,
    );
    dates.push({ date: day, count: readCount(countRows, tableName) });
    cursorDay = nextIsoDay(day);
  }
  return dates.reverse();
}

/**
 * GET /api/transaction/commit-dates
 * Every day on which rows were committed to Transaction, with row counts, newest first.
 */
export const getCommitDates = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app missing" });
    }
    const dates = await countRowsPerCreatedDay(app.zcql(), "Transaction");
    return res.status(200).json({ dates });
  } catch (err) {
    console.error("[getCommitDates]", err);
    return res.status(500).json({
      message: "Failed to load commit dates",
      error: err.message,
    });
  }
};

/** Columns the page shows. The deletion log stores them under the same names. */
const DISPLAY_COLUMNS = [
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
 * WHERE clause for "Transaction rows committed on `date` matching the page's
 * filters". `source` is req.query (list) or req.body (delete everything
 * marked), so the delete always reads the same rows the page showed.
 * Returns { error } when the input is invalid.
 */
function buildCommittedWhere(source) {
  const date = String(source.date || "").trim();
  const isin = String(source.isin || "").trim();
  const securityCode = String(source.securityCode || "").trim();
  const clientCode = String(source.clientCode || "").trim();
  const tranDate = String(source.tranDate || "").trim();
  const setDate = String(source.setDate || "").trim();

  if (!isIsoDate(date)) {
    return { error: "date is required (yyyy-mm-dd)" };
  }
  if ((tranDate && !isIsoDate(tranDate)) || (setDate && !isIsoDate(setDate))) {
    return { error: "tranDate and setDate must be yyyy-mm-dd" };
  }

  const conditions = [createdOnDayCondition(date)];
  if (isin) {
    conditions.push(
      `(ISIN LIKE '*${esc(isin)}*' OR Security_Name LIKE '*${esc(isin)}*')`,
    );
  }
  if (securityCode) conditions.push(`Security_code = '${esc(securityCode)}'`);
  if (clientCode) conditions.push(`WS_Account_code = '${esc(clientCode)}'`);
  if (tranDate) conditions.push(`TRANDATE = '${tranDate}'`);
  if (setDate) conditions.push(`SETDATE = '${setDate}'`);
  return { where: conditions.join(" AND ") };
}

function parsePaging(query) {
  return {
    page: Math.max(1, Math.floor(Number(query.page) || 1)),
    pageSize: Math.min(100, Math.max(1, Math.floor(Number(query.pageSize) || 50))),
  };
}

/** One page of `tableName` rows matching `where`, plus the total count. */
async function readPage(zcql, tableName, columns, where, page, pageSize) {
  const countRows = await zcql.executeZCQLQuery(
    `SELECT COUNT(ROWID) FROM ${tableName} WHERE ${where}`,
  );
  const total = readCount(countRows, tableName);

  // ZCQL's OFFSET is 1-based (OFFSET k skips only k-1 rows), so skipping
  // `skip` rows needs OFFSET skip + 1; otherwise each page repeats the
  // previous page's last row.
  const skip = (page - 1) * pageSize;
  const offsetClause = skip > 0 ? ` OFFSET ${skip + 1}` : "";
  const pageRows = await zcql.executeZCQLQuery(
    `SELECT ${columns.join(", ")} FROM ${tableName} WHERE ${where} ` +
      `ORDER BY ROWID ASC LIMIT ${pageSize}${offsetClause}`,
  );

  const rows = (pageRows || []).map((r) => r[tableName] || r);
  return { rows, total };
}

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

    const { where, error } = buildCommittedWhere(req.query);
    if (error) {
      return res.status(400).json({ message: error });
    }
    const { page, pageSize } = parsePaging(req.query);

    const { rows, total } = await readPage(
      app.zcql(),
      "Transaction",
      ["ROWID", ...DISPLAY_COLUMNS],
      where,
      page,
      pageSize,
    );
    return res.status(200).json({ rows, total, page, pageSize });
  } catch (err) {
    console.error("[getCommittedTransactions]", err);
    return res.status(500).json({
      message: "Failed to load committed transactions",
      error: err.message,
    });
  }
};

/** Most rows one delete may remove; bigger selections must be narrowed with filters. */
const DELETE_MAX_ROWS = 5000;

/** Rows per round: within ZCQL's 300-row DELETE cap and insertRows' 200-row limit. */
const DELETE_BATCH = 200;

const ROW_ID_PATTERN = /^\d+$/;

const DELETE_SOURCE_COLUMNS = ["ROWID", "CREATEDTIME", ...DISPLAY_COLUMNS].join(", ");

const tooManyRowsMessage = (count) =>
  `${count.toLocaleString("en-IN")} rows are selected. One delete can remove at most ` +
  `${DELETE_MAX_ROWS.toLocaleString("en-IN")} rows; narrow the filters and delete in parts.`;

/** The requested rows as they are now. ROWIDs that no longer exist are skipped. */
async function fetchRowsByIds(zcql, rowIds) {
  const rows = [];
  for (let i = 0; i < rowIds.length; i += DELETE_BATCH) {
    const ids = rowIds.slice(i, i + DELETE_BATCH).join(", ");
    const result = await zcql.executeZCQLQuery(
      `SELECT ${DELETE_SOURCE_COLUMNS} FROM Transaction WHERE ROWID IN (${ids})`,
    );
    for (const r of result || []) rows.push(r.Transaction || r);
  }
  return rows;
}

/** Every row matching `where`, paged by ROWID (ZCQL's OFFSET would repeat rows). */
async function fetchRowsByWhere(zcql, where) {
  const rows = [];
  let lastRowId = "0";
  for (;;) {
    const result = await zcql.executeZCQLQuery(
      `SELECT ${DELETE_SOURCE_COLUMNS} FROM Transaction ` +
        `WHERE ${where} AND ROWID > ${lastRowId} ORDER BY ROWID ASC LIMIT ${DELETE_BATCH}`,
    );
    const batch = (result || []).map((r) => r.Transaction || r);
    rows.push(...batch);
    if (batch.length < DELETE_BATCH || rows.length > DELETE_MAX_ROWS) break;
    lastRowId = String(batch[batch.length - 1].ROWID);
  }
  return rows;
}

function toLogRow(row) {
  const logRow = {
    Original_ROWID: String(row.ROWID),
    Committed_At: String(row.CREATEDTIME || ""),
  };
  // Empty values are left out so date and number columns stay null instead of
  // receiving an empty string.
  for (const column of DISPLAY_COLUMNS) {
    const value = row[column];
    if (value !== null && value !== undefined && value !== "") {
      logRow[column] = value;
    }
  }
  return logRow;
}

/**
 * Per batch: copy the rows into the deletion log, then delete them from
 * Transaction. If a delete fails, that batch's log copies are removed again so
 * the history only lists rows that were really deleted, and the loop stops.
 * Returns the rows deleted so far and the error, if any.
 */
async function deleteRowsWithLog(app, rows) {
  const zcql = app.zcql();
  const logTable = app.datastore().table(LOG_TABLE);
  const deleted = [];

  for (let i = 0; i < rows.length; i += DELETE_BATCH) {
    const batch = rows.slice(i, i + DELETE_BATCH);
    let logged;
    try {
      logged = await logTable.insertRows(batch.map(toLogRow));
    } catch (err) {
      return { deleted, error: err };
    }

    try {
      await zcql.executeZCQLQuery(
        `DELETE FROM Transaction WHERE ROWID IN (${batch.map((r) => r.ROWID).join(", ")})`,
      );
    } catch (err) {
      const logIds = (logged || []).map((r) => r.ROWID).filter(Boolean);
      if (logIds.length > 0) {
        try {
          await zcql.executeZCQLQuery(
            `DELETE FROM ${LOG_TABLE} WHERE ROWID IN (${logIds.join(", ")})`,
          );
        } catch (cleanupErr) {
          console.error("[deleteTransactions] log cleanup failed:", cleanupErr.message);
        }
      }
      return { deleted, error: err };
    }

    deleted.push(...batch);
  }
  return { deleted, error: null };
}

const JOB_CONFIG = { number_of_retries: 5, retry_interval: 60 * 1000 };

/**
 * Queues the existing rebuild jobs for every client that lost trades:
 * - RebuildHoldingtable, limited to the ISINs that lost trades. An ISIN with
 *   no trades left gets its Holdings rows removed by the rebuild.
 *   keepCaRecords stops it from rewriting Split_Account.
 * - Cal_CB_Per_TNX, which rebuilds the client's whole cash passbook, because
 *   every passbook row carries the running balance. It also leaves out the
 *   dividend credits that a deleted buy made the client eligible for.
 * One job per client keeps each job short. Returns clients whose jobs could
 * not be queued, so the page can say so.
 */
async function queueRebuilds(app, deletedRows) {
  const isinsByAccount = new Map();
  for (const row of deletedRows) {
    const account = String(row.WS_Account_code || "").trim();
    if (!account) continue;
    if (!isinsByAccount.has(account)) isinsByAccount.set(account, new Set());
    const isin = String(row.ISIN || "").trim();
    if (isin) isinsByAccount.get(account).add(isin);
  }

  const scheduling = app.jobScheduling();
  const failedAccounts = [];
  let jobIndex = 0;
  for (const [account, isins] of isinsByAccount) {
    jobIndex++;
    const stamp = `${Date.now()}${jobIndex}`;
    try {
      if (isins.size > 0) {
        await scheduling.JOB.submitJob({
          job_name: `TDH${stamp}`.slice(0, 20),
          jobpool_name: "CorporateActions",
          target_name: "RebuildHoldingtable",
          target_type: "Function",
          job_config: JOB_CONFIG,
          params: {
            accountCodesJson: JSON.stringify([account]),
            isinsJson: JSON.stringify([...isins]),
            source: "TransactionDeletion",
            keepCaRecords: "true",
          },
        });
      }
      await scheduling.JOB.submitJob({
        job_name: `TDC${stamp}`.slice(0, 20),
        jobpool_name: "UpdateMasters",
        target_name: "Cal_CB_Per_TNX",
        target_type: "Function",
        job_config: JOB_CONFIG,
        params: {
          accountCodesJson: JSON.stringify([account]),
          source: "TransactionDeletion",
        },
      });
    } catch (err) {
      console.error(`[deleteTransactions] could not queue rebuild for ${account}:`, err.message);
      failedAccounts.push(account);
    }
  }
  return { clientCount: isinsByAccount.size, failedAccounts };
}

/**
 * POST /api/transaction/delete
 * Body: { rowIds: [...] } for rows marked one by one, or
 *       { all: true, date, isin, securityCode, clientCode, tranDate, setDate }
 *       for every row of a commit day matching the filters.
 */
export const deleteTransactions = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app missing" });
    }
    const zcql = app.zcql();
    const body = req.body || {};

    let rows;
    if (body.all === true) {
      const { where, error } = buildCommittedWhere(body);
      if (error) {
        return res.status(400).json({ message: error });
      }
      const total = readCount(
        await zcql.executeZCQLQuery(`SELECT COUNT(ROWID) FROM Transaction WHERE ${where}`),
        "Transaction",
      );
      if (total > DELETE_MAX_ROWS) {
        return res.status(400).json({ message: tooManyRowsMessage(total) });
      }
      rows = await fetchRowsByWhere(zcql, where);
    } else {
      const rowIds = Array.isArray(body.rowIds)
        ? [...new Set(body.rowIds.map((id) => String(id).trim()))]
        : [];
      if (rowIds.length === 0 || rowIds.some((id) => !ROW_ID_PATTERN.test(id))) {
        return res.status(400).json({ message: "rowIds must be a non-empty list of row IDs" });
      }
      if (rowIds.length > DELETE_MAX_ROWS) {
        return res.status(400).json({ message: tooManyRowsMessage(rowIds.length) });
      }
      rows = await fetchRowsByIds(zcql, rowIds);
    }

    // Rows committed between the count and the read could push it over.
    if (rows.length > DELETE_MAX_ROWS) {
      return res.status(400).json({ message: tooManyRowsMessage(rows.length) });
    }
    if (rows.length === 0) {
      return res.status(404).json({
        message: "None of the selected transactions exist any more. They may already have been deleted.",
      });
    }

    const { deleted, error } = await deleteRowsWithLog(app, rows);
    const { clientCount, failedAccounts } = await queueRebuilds(app, deleted);

    if (error) {
      console.error("[deleteTransactions] stopped part-way:", error);
      return res.status(500).json({
        message:
          `Deleted ${deleted.length} of ${rows.length} rows, then failed: ${error.message}. ` +
          "Holdings and cash recalculation was started for the rows that were deleted.",
        deletedCount: deleted.length,
        clientCount,
        failedAccounts,
      });
    }

    return res.status(200).json({
      deletedCount: deleted.length,
      clientCount,
      failedAccounts,
    });
  } catch (err) {
    console.error("[deleteTransactions]", err);
    return res.status(500).json({
      message: "Failed to delete transactions",
      error: err.message,
    });
  }
};

/**
 * GET /api/transaction/deletion-dates
 * Every day on which transactions were deleted, with row counts, newest first.
 */
export const getDeletionDates = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app missing" });
    }
    const dates = await countRowsPerCreatedDay(app.zcql(), LOG_TABLE);
    return res.status(200).json({ dates });
  } catch (err) {
    console.error("[getDeletionDates]", err);
    return res.status(500).json({
      message: "Failed to load deletion dates",
      error: err.message,
    });
  }
};

/**
 * GET /api/transaction/deleted?date=&page=&pageSize=
 * One page of the transactions deleted on `date`, plus the total.
 */
export const getDeletedTransactions = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app missing" });
    }

    const date = String(req.query.date || "").trim();
    if (!isIsoDate(date)) {
      return res.status(400).json({ message: "date is required (yyyy-mm-dd)" });
    }
    const { page, pageSize } = parsePaging(req.query);

    const { rows, total } = await readPage(
      app.zcql(),
      LOG_TABLE,
      ["ROWID", ...DISPLAY_COLUMNS],
      createdOnDayCondition(date),
      page,
      pageSize,
    );
    return res.status(200).json({ rows, total, page, pageSize });
  } catch (err) {
    console.error("[getDeletedTransactions]", err);
    return res.status(500).json({
      message: "Failed to load deleted transactions",
      error: err.message,
    });
  }
};
