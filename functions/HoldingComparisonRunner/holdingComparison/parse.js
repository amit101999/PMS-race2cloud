import { Readable } from "stream";
import csvParser from "csv-parser";

import { CUSTODY_COLUMNS, CUSTODY_REQUIRED_FIELDS, FA_HEADERS } from "./constants.js";

/** Row errors returned to the screen. The total is still reported. */
const MAX_ROW_ERRORS = 50;

const ISIN_PATTERN = /^[A-Z]{2}[A-Z0-9]{9}[0-9]$/;

/** Same header normalisation the Field Mapping screen uses, so a saved name matches the same column. */
export const normHeader = (h) =>
  String(h || "")
    .trim()
    .toLowerCase()
    .replace(/[\s._\-/()]+/g, "");

const text = (v) => String(v ?? "").trim();

const toNumber = (v) => {
  const s = text(v);
  if (s === "") return null;
  const n = Number(s.replace(/[,\s]/g, ""));
  return Number.isFinite(n) ? n : null;
};

const normCode = (v) => text(v).toUpperCase();

/** Custodians sometimes append a currency suffix: INE467B01029-INR -> INE467B01029. */
const normIsin = (v) => {
  const s = text(v).toUpperCase();
  const dash = s.indexOf("-");
  return dash > 0 ? s.slice(0, dash) : s;
};

const MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"];

/**
 * A custodian date as YYYY-MM-DD, or null when it is not a real date.
 *
 * Accepts 2026-08-20, 20-Aug-26, 20-Aug-2026, 20 Aug 2026, 20/08/2026 and
 * 20-08-2026, with an optional time after the date. Numeric dates are read day
 * first, as Indian custodians write them. Two-digit years are 20xx.
 */
export const toIsoDate = (value) => {
  const s = text(value).replace(/[ T]\d{1,2}:\d{2}.*$/, "");
  let year;
  let month;
  let day;

  let m = /^(\d{4})-(\d{1,2})-(\d{1,2})$/.exec(s);
  if (m) {
    [, year, month, day] = m.map(Number);
  } else if ((m = /^(\d{1,2})[-/ .]([A-Za-z]{3})[A-Za-z]*[-/ .](\d{2}|\d{4})$/.exec(s))) {
    day = Number(m[1]);
    month = MONTHS.indexOf(m[2].toUpperCase()) + 1;
    year = Number(m[3]);
  } else if ((m = /^(\d{1,2})[-/.](\d{1,2})[-/.](\d{2}|\d{4})$/.exec(s))) {
    [, day, month, year] = m.map(Number);
  } else {
    return null;
  }

  if (year < 100) year += 2000;
  // Rejects month 0 / 13 and days that do not exist, such as 31-Sep or 29-Feb-2026.
  const date = new Date(Date.UTC(year, month - 1, day));
  if (month < 1 || date.getUTCMonth() !== month - 1 || date.getUTCDate() !== day) return null;
  return date.toISOString().slice(0, 10);
};

export const holdingKey = (accountCode, isin) => `${accountCode}|${isin}`;

/**
 * Every row as a positional array, header row first.
 *
 * Positional (headers: false) so a file with duplicate or blank headings is still
 * read column by column, and so a saved mapping by column position works.
 */
const readRows = (buffer) =>
  new Promise((resolve, reject) => {
    const rows = [];
    Readable.from(buffer)
      .pipe(csvParser({ headers: false }))
      .on("data", (row) => rows.push(Object.keys(row).map((k) => row[k])))
      .on("end", () => resolve(rows))
      .on("error", reject);
  });

const isBlankRow = (cells) => cells.every((c) => text(c) === "");

/** How many leading rows may be report titles before the header row. */
const MAX_TITLE_ROWS = 10;

/**
 * Position of the header row.
 *
 * Custodian exports can open with a title row such as
 * "HOLDING REPORT AS ON:20/08/2026,,,,". A title fills one cell at most, while
 * a header row names many columns, so the header is the first row with at least
 * two non-blank cells.
 */
const findHeaderRow = (rows) => {
  const limit = Math.min(rows.length, MAX_TITLE_ROWS + 1);
  for (let i = 0; i < limit; i++) {
    if (rows[i].filter((c) => text(c) !== "").length >= 2) return i;
  }
  return -1;
};

/**
 * The same file with any leading title rows removed, so the Field Mapping
 * preview reads the real header row. Uses the same rule as findHeaderRow.
 */
export const stripTitleRows = (buffer) => {
  const content = buffer.toString("utf8");
  const lines = content.split(/\r?\n/);
  const limit = Math.min(lines.length, MAX_TITLE_ROWS + 1);
  for (let i = 0; i < limit; i++) {
    // \s also matches a byte-order mark, so a BOM-only first cell counts as blank.
    const filledCells = lines[i].split(",").filter((c) => c.replace(/["\s]/g, "") !== "").length;
    if (filledCells >= 2) return i === 0 ? buffer : Buffer.from(lines.slice(i).join("\n"), "utf8");
  }
  return buffer;
};

/**
 * field -> column position for the custodian file.
 *
 * `profile` is the saved Field Mapping ({ field: { by, name | index } }) or null.
 * A column is found by its standard name when there is no saved mapping, or
 * through the mapping when there is one. Only CUSTODY_REQUIRED_FIELDS are
 * reported missing; the rest of the template is optional.
 *
 * @returns {{ index: object, missing: Array<{name:string, position:number}> }}
 */
export const resolveCustodyColumns = (headers, profile) => {
  const positionByName = new Map();
  headers.forEach((h, i) => {
    const key = normHeader(h);
    if (key && !positionByName.has(key)) positionByName.set(key, i);
  });

  const index = {};
  const missing = [];

  CUSTODY_COLUMNS.forEach(({ field, header }, templatePosition) => {
    // A field the saved mapping has no answer for falls back to the standard name.
    const source = profile?.[field] || { by: "name", name: header };

    if (source.by === "index" && Number.isInteger(source.index) && source.index < headers.length) {
      index[field] = source.index;
    } else if (source.by === "name" && positionByName.has(normHeader(source.name))) {
      index[field] = positionByName.get(normHeader(source.name));
    } else if (CUSTODY_REQUIRED_FIELDS.includes(field)) {
      const name = source.by === "index" ? `column ${Number(source.index) + 1}` : source.name;
      missing.push({ name, position: templatePosition + 1 });
    }
  });

  return { index, missing };
};

/** Add a row's quantity to its account + ISIN total. */
const addHolding = (holdings, row) => {
  const key = holdingKey(row.accountCode, row.isin);
  const existing = holdings.get(key);
  if (existing) {
    existing.quantity += row.quantity;
    existing.asOfDate = existing.asOfDate || row.asOfDate;
    existing.securityName = existing.securityName || row.securityName;
  } else {
    holdings.set(key, { ...row });
  }
};

/**
 * Parse and validate the custodian consolidated holdings file.
 *
 * Quantities for the same UCC + ISIN are summed: the custodian can report one
 * holding over several rows (for example by ISIN sub-type).
 *
 * @param {Buffer} buffer
 * @param {object|null} profile saved Field Mapping, or null for the standard template
 * @returns {Promise<{
 *   ok: boolean,
 *   fileError: object|null,
 *   rowErrors: object[], rowErrorCount: number,
 *   rowCount: number,
 *   holdings: Map<string, object>
 * }>}
 */
export const parseCustodyHoldings = async (buffer, profile = null) => {
  const result = {
    ok: false,
    fileError: null,
    rowErrors: [],
    rowErrorCount: 0,
    rowCount: 0,
    holdings: new Map(),
  };

  const rows = buffer && buffer.length > 0 ? await readRows(buffer) : [];
  const headerRow = findHeaderRow(rows);
  if (headerRow === -1) {
    result.fileError = { code: "NO_HEADER", message: "No header row was found at the top of the file." };
    return result;
  }

  const headers = rows[headerRow].map(text);
  const { index, missing } = resolveCustodyColumns(headers, profile);
  if (missing.length > 0) {
    result.fileError = {
      code: "HEADER_MISMATCH",
      message: profile
        ? `The file does not have the column(s) the saved field mapping expects: ${missing.map((m) => m.name).join(", ")}.`
        : `The file is missing required column(s): ${missing.map((m) => m.name).join(", ")}. ` +
          "Account code (UCC), ISIN, NetBalance and LTPDate are needed for the comparison.",
      missing,
      hasSavedMapping: Boolean(profile),
    };
    return result;
  }

  const addError = (error) => {
    result.rowErrorCount += 1;
    if (result.rowErrors.length < MAX_ROW_ERRORS) result.rowErrors.push(error);
  };

  for (let i = headerRow + 1; i < rows.length; i++) {
    const cells = rows[i];
    if (isBlankRow(cells)) continue;

    // Line numbers as a spreadsheet shows them, counting any title rows.
    const line = i + 1;
    const cell = (field) => (index[field] === undefined ? "" : cells[index[field]]);

    const accountCode = normCode(cell("accountCode"));
    const isin = normIsin(cell("isin"));
    const quantity = toNumber(cell("quantity"));
    // Blank is allowed: some holdings (unlisted funds, for example) have no last traded date.
    const rawDate = text(cell("asOfDate"));
    const asOfDate = rawDate === "" ? "" : toIsoDate(rawDate);

    let rowOk = true;
    if (!accountCode) {
      addError({ row: line, column: headers[index.accountCode], value: "", message: "Account code (UCC) is blank." });
      rowOk = false;
    }
    if (!ISIN_PATTERN.test(isin)) {
      addError({ row: line, column: headers[index.isin], value: cell("isin"), message: "Not a valid 12-character ISIN." });
      rowOk = false;
    }
    if (quantity === null) {
      addError({ row: line, column: headers[index.quantity], value: cell("quantity"), message: "Quantity is not a number." });
      rowOk = false;
    }
    if (asOfDate === null) {
      addError({ row: line, column: headers[index.asOfDate], value: rawDate, message: "Not a recognised date." });
      rowOk = false;
    }
    if (!rowOk) continue;

    result.rowCount += 1;
    addHolding(result.holdings, {
      accountCode,
      isin,
      quantity,
      asOfDate,
      securityName: text(cell("securityName")),
    });
  }

  result.ok = result.rowErrorCount === 0;
  return result;
};

/**
 * Parse the FA holdings snapshot written by FAHoldingsGenerator.
 *
 * The file is produced by our own job, so a wrong header means the wrong file
 * was picked, and it is reported as a file error rather than guessed around.
 */
export const parseFaHoldings = async (buffer) => {
  const result = { ok: false, fileError: null, rowErrorCount: 0, rowCount: 0, holdings: new Map() };

  const rows = buffer && buffer.length > 0 ? await readRows(buffer) : [];
  const headers = (rows[0] || []).map(normHeader);
  const expected = FA_HEADERS.map(normHeader);
  if (headers.length < expected.length || expected.some((h, i) => headers[i] !== h)) {
    result.fileError = {
      code: "FA_HEADER_MISMATCH",
      message: `The FA holdings file does not have the expected columns: ${FA_HEADERS.join(", ")}.`,
    };
    return result;
  }

  const col = Object.fromEntries(FA_HEADERS.map((h, i) => [h, i]));

  for (let i = 1; i < rows.length; i++) {
    const cells = rows[i];
    if (isBlankRow(cells)) continue;

    const accountCode = normCode(cells[col.ACCOUNT_CODE]);
    const isin = normIsin(cells[col.ISIN]);
    const quantity = toNumber(cells[col.HOLDING]);
    if (!accountCode || !isin || quantity === null) {
      result.rowErrorCount += 1;
      continue;
    }

    result.rowCount += 1;
    addHolding(result.holdings, {
      accountCode,
      isin,
      quantity,
      asOfDate: text(cells[col.AS_ON_DATE]),
      securityName: text(cells[col.SECURITY_NAME]),
    });
  }

  result.ok = result.rowErrorCount === 0;
  return result;
};

/** Read a difference report back into row objects, keyed by DIFFERENCE_HEADERS. */
export const parseDifferenceCsv = (buffer) =>
  new Promise((resolve, reject) => {
    const rows = [];
    Readable.from(buffer)
      .pipe(csvParser())
      .on("data", (row) => rows.push(row))
      .on("end", () => resolve(rows))
      .on("error", reject);
  });
