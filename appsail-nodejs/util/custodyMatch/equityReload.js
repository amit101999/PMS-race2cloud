/**
 * Equity transaction reload — the rules that decide what a re-uploaded FA file
 * does to a date's trades. Pure functions only: no database, no Stratus, so the
 * rules can be tested on their own (equityReload.test.js).
 *
 * Every trading date is handled as one box: the date's equity trades in our
 * books, plus the FA files uploaded for it since the last Pre-commit. A trade is
 * identified by its own transaction ID: the FA column mapped to "Transaction ID"
 * on the Field Mapping screen, stored in Transaction.Transaction_ID. It is not
 * TRANSREF (the transaction reference number, Txn_Ref_No), which is compared
 * like any other value. Trades booked before IDs existed have none; they are
 * matched by their fingerprint instead (account + ISIN + buy/sell + dates +
 * quantity + price).
 *
 * Plan: docs/transaction upload/transient-wibbling-eclipse.md.
 */

import { FA_HEADERS } from "./constants.js";
import { FA_TRANSACTION_ID_HEADER } from "./fieldMap.js";

export const MODE = {
  FIRST: "FIRST",
  ADD: "ADD",
  REPLACE: "REPLACE",
  ASK: "ASK",
};

/** Why the operator is asked instead of the system deciding. */
export const ASK_REASON = {
  NO_IDS: "NO_IDS",
  NEW_IDS_LOOK_OLD: "NEW_IDS_LOOK_OLD",
  PARTIAL: "PARTIAL",
};

/** A file holding at least this share of the date's IDs is a full corrected file. */
export const REPLACE_SHARE = 0.9;

/*
 * The change window of a trade date: its trades can be re-uploaded and
 * re-committed until N working days after the trade date (T+N), that day
 * included. Each trade date has its own N, picked on the Fund Accounting card
 * (default T+150, or any number from 1 to 150). N can be changed until the date's
 * first commit, which fixes it. Saturday and Sunday are not working days;
 * holidays are not counted yet.
 *
 * Only a committed date locks: a date never committed stays open, so a late file
 * can still be booked. A date first committed after its window has passed is
 * locked straight after that commit. Once locked, a date is booked for good.
 * The record per date is kept by util/custodyMatch/changeWindow.js.
 */
export const WINDOW_PRESETS = [1, 2, 3, 150];
export const DEFAULT_WINDOW_DAYS = 150;
export const MAX_WINDOW_DAYS = 150;

/** N from a request or a stored record, or null when it is not a whole number from 1 to 150. */
export const parseWindowDays = (value) => {
  const days = Number(value);
  return Number.isInteger(days) && days >= 1 && days <= MAX_WINDOW_DAYS ? days : null;
};

// Noon UTC, so adding days never crosses into another date.
const dayOf = (iso) => {
  const [y, m, d] = String(iso).split("-").map(Number);
  return new Date(Date.UTC(y, m - 1, d, 12));
};
const isoOf = (day) => {
  const p = (n) => String(n).padStart(2, "0");
  return `${day.getUTCFullYear()}-${p(day.getUTCMonth() + 1)}-${p(day.getUTCDate())}`;
};

/** The last day a trade date can still be changed under a T+days window, as YYYY-MM-DD. */
export const lastChangeDate = (tradeDate, days) => {
  const day = dayOf(tradeDate);
  let counted = 0;
  while (counted < days) {
    day.setUTCDate(day.getUTCDate() + 1);
    const weekday = day.getUTCDay();
    if (weekday !== 0 && weekday !== 6) counted++;
  }
  return isoOf(day);
};

/** Today in India, as YYYY-MM-DD; the server's own clock may be in another time zone. */
export const todayInIndia = () => new Date().toLocaleDateString("en-CA", { timeZone: "Asia/Kolkata" });

const show = (iso) => iso.split("-").reverse().join("-");

/**
 * What the screen needs to know about one trade date's window.
 *
 * @param {string} tradeDate YYYY-MM-DD
 * @param {{ days: number, fixed: boolean }|null} record null when nothing is chosen or committed yet
 */
export const dateWindowView = (tradeDate, record, today = todayInIndia()) => {
  const days = parseWindowDays(record?.days) || DEFAULT_WINDOW_DAYS;
  const fixed = Boolean(record?.fixed);
  const lastDay = lastChangeDate(tradeDate, days);
  const locked = fixed && today > lastDay;
  let message = "";
  if (locked) {
    message = `${show(tradeDate)} is locked permanently. Its window (T+${days}) ended on ${show(lastDay)}.`;
  } else if (fixed) {
    message = `Re-upload of ${show(tradeDate)} is open till ${show(lastDay)} (T+${days}).`;
  }
  return { date: tradeDate, days, fixed, lastDay, locked, message };
};

/**
 * The refusal text for a locked trade date, or "" while it is open.
 * Locked from upload to commit: uploads, file removal, matching and Commit.
 */
export const closedDateMessage = (tradeDate, record, today = todayInIndia()) => {
  const view = dateWindowView(tradeDate, record, today);
  return view.locked ? view.message : "";
};

/** Removing more than this share of the date's trades needs a second confirmation. */
export const BIG_REMOVAL_SHARE = 0.1;

/**
 * Positions in the consolidated layout: the 26 template columns (FA_HEADERS),
 * then the Transaction ID as column 27.
 */
const COL = {
  brokerCode: 0,
  account: 1,
  isin: 2,
  exchange: 3,
  tranType: 4,
  tranDate: 5,
  setDate: 6,
  quantity: 7,
  rate: 8,
  brokerage: 9,
  serviceTax: 10,
  stt: 14,
  ref: 17,
  cashSetdate: 25,
  txnId: FA_HEADERS.length,
};

/*
 * Transaction columns in consolidated order, so a booked trade can be written
 * back into the same 27 columns an uploaded file is turned into. The import maps them the other
 * way (HEADER_MAP in TempTransactionUpload.js); keep the two in step.
 * SETDATEFLAG is never stored, so it comes back blank.
 */
export const BOOKED_COLUMNS = [
  "BROKERCODE",
  "WS_Account_code",
  "ISIN",
  "EXCHG",
  "Tran_Type",
  "TRANDATE",
  "SETDATE",
  "QTY",
  "RATE",
  "BROKERAGE",
  "SERVICETAX",
  null,
  "MKTRATE",
  "Security_code",
  "STT",
  "ACCRUEDINTEREST",
  "PORTFOLIOID",
  "Txn_Ref_No",
  "Tran_Desc",
  "CHEQUENO",
  "CHEQUEDTL",
  "BANKACID",
  "SUB_FOLIO",
  "TOTAL_TRXNFEE",
  "TOTAL_TRXNFEE_STAX",
  "PAYMENTDATE",
  "Transaction_ID",
];

const DATE_COLUMNS = new Set(["TRANDATE", "SETDATE", "PAYMENTDATE"]);

const text = (v) => (v === null || v === undefined ? "" : String(v).trim());
const upper = (v) => text(v).toUpperCase();

/** Numbers compare by value: "10", "10.0" and 10 are the same quantity. */
const numText = (v) => {
  const s = text(v);
  if (s === "") return "";
  const n = Number(s);
  return Number.isFinite(n) ? String(Math.round(n * 1e6) / 1e6) : upper(s);
};

/**
 * Mutual fund trades are bought directly from the fund: ISIN starting INF and
 * exchange DIR. An INF ISIN on NSE/BSE is an ETF, which is equity, so the ISIN
 * alone cannot decide it.
 */
export const isMutualFundTrade = (isin, exchange) =>
  upper(isin).startsWith("INF") && upper(exchange) === "DIR";

/** One trade, from its 27 consolidated cells. Both sides of every comparison use this shape. */
export const tradeFromCells = (cells, extra = {}) => ({
  ...extra,
  cells,
  txnId: text(cells[COL.txnId]),
  ref: text(cells[COL.ref]),
  account: upper(cells[COL.account]),
  isin: upper(cells[COL.isin]),
  exchange: upper(cells[COL.exchange]),
  tranType: upper(cells[COL.tranType]),
  tranDate: text(cells[COL.tranDate]).slice(0, 10),
  setDate: text(cells[COL.setDate]).slice(0, 10),
  quantity: numText(cells[COL.quantity]),
  rate: numText(cells[COL.rate]),
  brokerage: numText(cells[COL.brokerage]),
  serviceTax: numText(cells[COL.serviceTax]),
  stt: numText(cells[COL.stt]),
  cashSetdate: text(cells[COL.cashSetdate]).slice(0, 10),
});

/**
 * A Transaction row as a trade. Description comes from Tran_Desc, or from
 * Security_Name for older rows that never filled Tran_Desc. Dates are cut to
 * YYYY-MM-DD; a blank cell stays blank.
 */
export const tradeFromBookedRow = (row) => {
  const cells = BOOKED_COLUMNS.map((column) => {
    if (!column) return "";
    let value = row[column];
    if (column === "BROKERCODE") value = value ?? row.WS_client_id;
    if (column === "Tran_Desc") value = value ?? row.Security_Name;
    const cell = text(value);
    if (cell.toLowerCase() === "null") return "";
    return DATE_COLUMNS.has(column) ? cell.slice(0, 10) : cell;
  });
  return tradeFromCells(cells, { rowId: text(row.ROWID), booked: true });
};

/** The values that say whether a trade changed. */
const COMPARED_FIELDS = [
  "account",
  "isin",
  "exchange",
  "tranType",
  "tranDate",
  "setDate",
  "quantity",
  "rate",
  "brokerage",
  "serviceTax",
  "stt",
  "cashSetdate",
  "ref",
];

export const sameValues = (a, b) => COMPARED_FIELDS.every((f) => a[f] === b[f]);

/** The fields that differ, for the "Changed" list on screen. */
export const changedFields = (oldTrade, newTrade) =>
  COMPARED_FIELDS.filter((f) => oldTrade[f] !== newTrade[f]).map((f) => ({
    field: f,
    from: oldTrade[f],
    to: newTrade[f],
  }));

/** Used only when a trade has no ID. */
export const fingerprint = (t) =>
  [t.account, t.isin, t.tranType, t.tranDate, t.setDate, t.quantity, t.rate].join("|");

/**
 * Upload checks on the rows of one new file. Each returned problem refuses the
 * whole file, because the system cannot tell a new trade from a changed one
 * without the ID, nor which of two different rows with one ID is right.
 */
export const fileProblems = (trades) => {
  const problems = [];
  const firstById = new Map();
  for (const t of trades) {
    if (!t.txnId) {
      problems.push({
        row: t.rowNumber,
        code: "MISSING_TRANSACTION_ID",
        message:
          "Transaction ID is blank. Every trade must carry its own ID. If the file has the column, map it to Transaction ID on the Field Mapping screen.",
        column: FA_TRANSACTION_ID_HEADER,
        value: "",
      });
      continue;
    }
    if (isMutualFundTrade(t.isin, t.exchange)) {
      problems.push({
        row: t.rowNumber,
        code: "MUTUAL_FUND_ROW",
        message: `${t.isin} on exchange DIR is a mutual fund trade. Upload it in the Mutual Funds tab.`,
        column: "SYMBOLCODE",
        value: t.isin,
      });
      continue;
    }
    const first = firstById.get(t.txnId);
    if (!first) {
      firstById.set(t.txnId, t);
    } else if (!sameValues(first, t)) {
      problems.push({
        row: t.rowNumber,
        code: "SAME_ID_DIFFERENT_VALUES",
        message: `Transaction ID ${t.txnId} is also on row ${first.rowNumber} with different values. Keep only the right row.`,
        column: FA_TRANSACTION_ID_HEADER,
        value: t.txnId,
      });
    }
  }
  return problems;
};

/**
 * FIRST, ADD, REPLACE or ASK for a new file against the date's current list.
 *
 *   current list empty                     → FIRST
 *   some current trades have no ID         → ASK (matched by fingerprint, never auto-added)
 *   no ID of the date in the file:
 *     most rows look like current trades   → ASK (probably re-exported with new IDs)
 *     otherwise                            → ADD
 *   ≥ 90% of the date's IDs in the file    → REPLACE
 *   anything in between                    → ASK
 *
 * @returns {{ mode: string, reason: string|null, share: number }}
 */
export const decideMode = (newTrades, current) => {
  if (current.length === 0) return { mode: MODE.FIRST, reason: null, share: 0 };
  if (current.some((t) => !t.txnId)) {
    return { mode: MODE.ASK, reason: ASK_REASON.NO_IDS, share: 0 };
  }

  const currentIds = new Set(current.map((t) => t.txnId));
  const found = new Set(newTrades.map((t) => t.txnId).filter((id) => currentIds.has(id)));
  const share = found.size / currentIds.size;

  if (share === 0) {
    const currentPrints = new Set(current.map(fingerprint));
    const lookAlike = newTrades.filter((t) => currentPrints.has(fingerprint(t))).length;
    return lookAlike > newTrades.length / 2
      ? { mode: MODE.ASK, reason: ASK_REASON.NEW_IDS_LOOK_OLD, share }
      : { mode: MODE.ADD, reason: null, share };
  }
  if (share >= REPLACE_SHARE) return { mode: MODE.REPLACE, reason: null, share };
  return { mode: MODE.ASK, reason: ASK_REASON.PARTIAL, share };
};

/** How many samples of each badge go to the screen. The counts are always complete. */
const SAMPLE_SIZE = 20;

const sampleOf = (t) => ({
  txnId: t.txnId,
  account: t.account,
  isin: t.isin,
  tranType: t.tranType,
  quantity: t.quantity,
  rate: t.rate,
});

/**
 * The badges for a new file against the date's current list.
 *
 * Same      – ID already there, same values
 * Changed   – ID already there, some value different (the new file wins)
 * New       – ID not there
 * Duplicate – (uploads, markBooked) already in our books with the same values
 * Repeated  – the same ID again in this file, or (Add) a repeat of a pending trade
 * Removed   – (Replace) a current trade the new file does not have
 *
 * Without markBooked (the match against our books) a booked trade with the same
 * values counts as Same, and Repeated is the only kind of repeat.
 * A current trade without an ID is matched by fingerprint, once.
 */
export const compareWithList = (newTrades, current, mode, { markBooked = false } = {}) => {
  const byId = new Map();
  const noIdByPrint = new Map();
  for (const t of current) {
    if (t.txnId) byId.set(t.txnId, t);
    else {
      const key = fingerprint(t);
      if (!noIdByPrint.has(key)) noIdByPrint.set(key, []);
      noIdByPrint.get(key).push(t);
    }
  }

  const result = { same: [], changed: [], new: [], duplicate: [], repeated: [], removed: [] };
  const seenIds = new Set();
  const matchedNoId = new Set();
  // A trade the new file repeats unchanged.
  const unchanged = (old, t) => {
    if (markBooked && old.booked) result.duplicate.push(t);
    else if (mode === MODE.ADD) result.repeated.push(t);
    else result.same.push(t);
  };

  for (const t of newTrades) {
    if (seenIds.has(t.txnId)) {
      result.repeated.push(t);
      continue;
    }
    seenIds.add(t.txnId);

    const old = byId.get(t.txnId);
    if (old) {
      if (!sameValues(old, t)) result.changed.push({ old, row: t });
      else unchanged(old, t);
      continue;
    }

    const lookAlikes = noIdByPrint.get(fingerprint(t));
    if (lookAlikes && lookAlikes.length > 0) {
      const match = lookAlikes.shift();
      matchedNoId.add(match);
      unchanged(match, t);
      continue;
    }
    result.new.push(t);
  }

  if (mode === MODE.REPLACE) {
    for (const t of current) {
      if (t.txnId ? !seenIds.has(t.txnId) : !matchedNoId.has(t)) result.removed.push(t);
    }
  }

  // Replace keeps every distinct trade of the file; Add keeps the list plus the new ones.
  const totalAfter =
    mode === MODE.REPLACE
      ? newTrades.length - result.repeated.length
      : current.length + result.new.length;

  return {
    mode,
    counts: {
      same: result.same.length,
      changed: result.changed.length,
      new: result.new.length,
      duplicate: result.duplicate.length,
      repeated: result.repeated.length,
      removed: result.removed.length,
    },
    totalBefore: current.length,
    totalAfter,
    samples: {
      changed: result.changed.slice(0, SAMPLE_SIZE).map(({ old, row }) => ({
        ...sampleOf(row),
        fields: changedFields(old, row),
      })),
      new: result.new.slice(0, SAMPLE_SIZE).map(sampleOf),
      duplicate: result.duplicate.slice(0, SAMPLE_SIZE).map(sampleOf),
      repeated: result.repeated.slice(0, SAMPLE_SIZE).map(sampleOf),
      removed: result.removed.slice(0, SAMPLE_SIZE).map(sampleOf),
    },
    // Every duplicate, for the export; the controller writes them and drops this.
    duplicateTrades: result.duplicate,
  };
};

/** True when Replace would remove more than BIG_REMOVAL_SHARE of the date's trades. */
export const isBigRemoval = (comparison) =>
  comparison.totalBefore > 0 &&
  comparison.counts.removed / comparison.totalBefore > BIG_REMOVAL_SHARE;

/**
 * Adds one file's trades to a list. The newer row wins for an ID already there;
 * a trade without an ID that has the same fingerprint is a repeat and is left out.
 * The same ID twice in the file collapses to one row.
 */
const addTrades = (list, trades) => {
  const next = [...list];
  const indexById = new Map();
  next.forEach((t, i) => {
    if (t.txnId) indexById.set(t.txnId, i);
  });
  const noIdByPrint = new Map();
  for (const t of next) {
    if (t.txnId) continue;
    const key = fingerprint(t);
    noIdByPrint.set(key, (noIdByPrint.get(key) || 0) + 1);
  }

  let merged = 0;
  for (const t of trades) {
    if (indexById.has(t.txnId)) {
      next[indexById.get(t.txnId)] = t;
      merged++;
      continue;
    }
    const key = fingerprint(t);
    if ((noIdByPrint.get(key) || 0) > 0) {
      noIdByPrint.set(key, noIdByPrint.get(key) - 1);
      merged++;
      continue;
    }
    indexById.set(t.txnId, next.length);
    next.push(t);
  }
  return { trades: next, merged };
};

/**
 * The date's final trade list: our books, then every pending file in upload
 * order. A Replace file starts the list again from itself; Add and First files
 * add to it. Rows that were merged away (repeats, older versions of an ID) are
 * counted so the screen can say how many duplicates were folded in.
 *
 * @param {object[]} booked the date's equity trades in our books
 * @param {{ mode: string, trades: object[] }[]} files pending files, oldest first
 */
export const buildDateList = (booked, files) => {
  let trades = booked;
  let merged = 0;
  for (const file of files) {
    const start = file.mode === MODE.REPLACE ? [] : trades;
    const added = addTrades(start, file.trades);
    trades = added.trades;
    merged += added.merged;
  }
  return { trades, merged };
};

/**
 * Every (client, ISIN) the date touches, old side and new side. Holdings and
 * cash are rebuilt for all of them, so a trade that moved to another account or
 * ISIN is corrected on both.
 */
export const affectedOf = (booked, finalTrades) => {
  const pairs = new Map();
  for (const t of [...booked, ...finalTrades]) {
    if (!t.account || !t.isin) continue;
    pairs.set(`${t.account}\t${t.isin}`, [t.account, t.isin]);
  }
  const list = [...pairs.values()].sort((a, b) => a.join().localeCompare(b.join()));
  return {
    pairs: list,
    clients: [...new Set(list.map(([account]) => account))].sort(),
    isins: [...new Set(list.map(([, isin]) => isin))].sort(),
  };
};

/* ───────────────────────────── custodian files ─────────────────────────── */

/** What a new custodian file does to the files stored for its date, scheme and type. */
export const CUSTODY_MODE = {
  FIRST: "FIRST",
  ADD: "ADD",
  SKIP: "SKIP",
  ASK: "ASK",
  REPLACE: "REPLACE",
};

/**
 * A custodian line has no FA transaction ID. It is identified by its own number
 * (TRADENO, ContractNo or REFNo) with the client, ISIN and buy/sell, because one
 * number may cover several clients or shares.
 */
const custodyLineId = (row) => [row.ref, row.actualCode, row.isin, row.tranType].map(upper).join("|");

const custodyLineValues = (row) =>
  [
    ...[row.quantity, row.rate, row.brokerage, row.stt, row.netAmount].map(numText),
    ...[row.tranDate, row.settlementDate, row.exchange, row.brokerCode].map(upper),
  ].join("|");

/** The same trade under a different number: a corrected file may be numbered afresh. */
const custodyFingerprint = (row) =>
  [row.actualCode, row.isin, row.tranType, numText(row.quantity)].map(upper).join("|");

/** Rows with one ID are one line; their values are sorted so the row order does not matter. */
const custodyLines = (rows) => {
  const valuesById = new Map();
  for (const row of rows) {
    const id = custodyLineId(row);
    if (!valuesById.has(id)) valuesById.set(id, []);
    valuesById.get(id).push(custodyLineValues(row));
  }
  return new Map([...valuesById].map(([id, values]) => [id, values.sort().join(";")]));
};

/**
 * FIRST, ADD, SKIP or ASK for a new custodian file against the rows stored for
 * its date, scheme and type. The row count decides nothing: a small file can be
 * new trades or a cut-down copy of the old file.
 *
 *   nothing stored                         → FIRST
 *   no new or changed line                 → SKIP (already uploaded)
 *   every line new, all with a number:
 *     most rows look like stored rows      → ASK (probably re-exported with new numbers)
 *     otherwise                            → ADD
 *   anything else                          → ASK (only the operator can say)
 *
 * Storing an overlapping file next to the old one is never offered, because the
 * match adds quantities together and would count the repeated lines twice.
 */
export const compareCustodyRows = (newRows, storedRows) => {
  const incoming = custodyLines(newRows);
  const stored = custodyLines(storedRows);
  const counts = { same: 0, changed: 0, new: 0, removed: 0 };

  for (const [id, values] of incoming) {
    if (!stored.has(id)) counts.new++;
    else if (stored.get(id) === values) counts.same++;
    else counts.changed++;
  }
  for (const id of stored.keys()) {
    if (!incoming.has(id)) counts.removed++;
  }
  const blankNumbers = newRows.filter((row) => !text(row.ref)).length;

  const storedPrints = new Set(storedRows.map(custodyFingerprint));
  const lookAlikes = newRows.filter((row) => storedPrints.has(custodyFingerprint(row))).length;

  let mode = CUSTODY_MODE.ASK;
  if (stored.size === 0) mode = CUSTODY_MODE.FIRST;
  else if (counts.new === 0 && counts.changed === 0) mode = CUSTODY_MODE.SKIP;
  else if (
    counts.same === 0 &&
    counts.changed === 0 &&
    blankNumbers === 0 &&
    lookAlikes <= newRows.length / 2
  ) {
    mode = CUSTODY_MODE.ADD;
  }

  return { mode, counts, blankNumbers, lookAlikes };
};

/** RFC4180 field: quoted when it holds a comma, quote or line break. */
const csvField = (v) => {
  const s = text(v);
  return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
};

/** The trades as the 27-column consolidated CSV (template + Transaction ID), which the import reads. */
export const tradesToTemplateCsv = (trades) =>
  [[...FA_HEADERS, FA_TRANSACTION_ID_HEADER], ...trades.map((t) => t.cells)]
    .map((cells) => cells.map(csvField).join(","))
    .join("\n");
