/**
 * Custody Matching — FA (daily transaction file) parser.
 *
 * Reads the same 26-column file the Transaction Upload screen already accepts.
 * The format is not changed by custody matching; this only reads from it.
 *
 * Parsed positionally rather than by header name on purpose: the template has
 * three columns literally named "filler", and any name-keyed parser collapses
 * them into one. Row 1 is validated for exact count/order/spelling first, so
 * position is guaranteed by the time the rows are read.
 *
 * Codes are left as they appear in the file (virtual OR actual). Resolving them
 * onto actual codes is resolveActual.js's job, which needs a clientIds map and
 * therefore cannot live in a pure parser.
 */

import { Readable } from "stream";
import csvParser from "csv-parser";

import {
  DEFAULT_OPTIONS,
  FA_DEDUCTION_HEADERS,
  FA_HEADERS,
  FILE_ERROR,
  ROW_ERROR,
  SIDE,
  WARNING,
} from "./constants.js";
import {
  detectSeparator,
  displayNum,
  firstLineOf,
  normCode,
  normIsin,
  normTranType,
  num,
  parseDate,
  splitHeaderCells,
  text,
} from "./normalize.js";
import {
  EXTRA_COLUMN_FIELDS,
  FA_TEMPLATE_SLOT,
  FA_TRANSACTION_ID_HEADER,
  PROFILE_TYPE,
  TRANSFORM,
  columnNamesFor,
  isDefaultProfile,
  resolveFieldMap,
} from "./fieldMap.js";

/**
 * Above this many missing columns, the file is treated as the wrong file rather
 * than a renamed one — the advice changes accordingly.
 */
const TOO_MANY_MISMATCHED = 5;

/**
 * 0-based positions in FA_HEADERS — the DEFAULT layout only.
 *
 * Kept as the canonical template slots (a remapped file is rewritten back into
 * these positions before import). A file's actual positions come from the
 * resolved field map, which equals this table when the profile is unedited.
 */
const COL = {
  brokerCode: 0,
  accountCode: 1, // BROKERACID — virtual or actual
  isin: 2, // SYMBOLCODE
  exchange: 3, // EXCHG
  tranType: 4, // TRANSTYPE
  tranDate: 5, // DATEPUR_ACQUI
  settlementDate: 6, // SETDATE
  quantity: 7, // QUANTITY
  rate: 8, // RATE
  brokerage: 9, // BROKERAGEPERSHARE
  serviceTax: 10, // SERVICETAX
  stt: 14, // TRANEXPENSE -> STT
  ref: 17, // TRANSREF
  description: 18,
};

/**
 * Row 1 must match FA_HEADERS exactly — same count, same order, same spelling and case.
 * Same contract the existing upload controller enforces, so a file that passes
 * custody matching is guaranteed to pass the import that follows it.
 *
 * @returns {object|null} file error, or null when the header is good.
 */
export const validateFaHeader = (buffer, { profile = null, type = PROFILE_TYPE.FA } = {}) => {
  if (!buffer || buffer.length === 0) {
    return {
      code: FILE_ERROR.EMPTY_FILE,
      message: "The file is empty.",
      hint: "Export the transaction file again and re-upload it.",
    };
  }

  const headerLine = firstLineOf(buffer);
  const separator = detectSeparator(headerLine);
  const cells = splitHeaderCells(headerLine, separator);

  /*
   * A custom mapping means the file is deliberately NOT the template, so the
   * exact count/order/spelling check cannot apply to it. The real requirement
   * is narrower and is what resolveFieldMap enforces: every field the match key
   * is built from must be locatable in this file.
   *
   * The mutual fund transaction file is never held to the template contract
   * either: it renames one heading (SYMBOLCODE/ISIN CODE), so an exact spelling
   * check would reject it before its own mapping could be read.
   */
  if (type !== PROFILE_TYPE.FA || !isDefaultProfile(type, profile)) {
    // Strict, exactly as the template path is strict: every heading the mapping
    // names has to be in the file.
    const resolved = resolveFieldMap(type, profile, cells, { strict: true });
    if (resolved.ok) return null;
    const missing = resolved.errors.map((e) => e.expected || e.label).filter(Boolean);
    const mappedTotal = Object.keys(resolved.index).length + missing.length;

    // Same rule as the custodian side: a few missing columns is a rename, most
    // of them missing is the wrong file.
    if (missing.length > TOO_MANY_MISMATCHED) {
      return {
        code: FILE_ERROR.HEADER_MAPPING_MISMATCH,
        message: `Unknown column names — ${missing.join(", ")}. Check the mapping.`,
        missing,
        mappedTotal,
        tooMany: true,
        mapErrors: resolved.errors,
        rawHeaders: cells,
      };
    }

    return {
      code: FILE_ERROR.HEADER_MAPPING_MISMATCH,
      message: `Unknown column names — ${missing.join(", ")}. Check the mapping.`,
      missing,
      mapErrors: resolved.errors,
      rawHeaders: cells,
    };
  }

  if (cells.length !== FA_HEADERS.length) {
    return {
      code: FILE_ERROR.WRONG_COLUMN_ORDER,
      message: `Wrong number of columns — expected ${FA_HEADERS.length}, found ${cells.length}. Check the mapping.`,
      expectedCount: FA_HEADERS.length,
      actualCount: cells.length,
      expectedHeaders: [...FA_HEADERS],
    };
  }

  const mismatches = [];
  for (let i = 0; i < cells.length; i++) {
    if (cells[i] !== FA_HEADERS[i]) {
      mismatches.push({
        columnIndex: i + 1,
        expected: FA_HEADERS[i],
        actual: cells[i],
      });
    }
  }

  if (mismatches.length > 0) {
    return {
      code: FILE_ERROR.WRONG_COLUMN_ORDER,
      message: `Unknown column names — ${mismatches.map((m) => m.actual || "(blank)").join(", ")}. Check the mapping.`,
      mismatches,
      expectedHeaders: [...FA_HEADERS],
    };
  }

  return null;
};

/**
 * Parse and validate the FA transaction file.
 *
 * @param {Buffer} buffer
 * @param {object}  opts
 * @param {string} [opts.businessDate] yyyy-mm-dd. Rows dated differently are warned about, not rejected.
 * @param {object} [opts.options]      overrides for DEFAULT_OPTIONS.
 * @param {object} [opts.profile]      user-defined field mapping; null uses the built-in template.
 * @param {string} [opts.type]         which transaction profile reads the file: FA or FA_MUTUAL_FUND.
 * @returns {Promise<{ok:boolean, fileError:object|null, rows:object[], rowErrors:object[], warnings:object[], stats:object}>}
 */
export const parseFaFile = (
  buffer,
  { businessDate = null, options = {}, profile = null, type = PROFILE_TYPE.FA } = {},
) => {
  const cfg = { ...DEFAULT_OPTIONS, ...options };

  const fileError = validateFaHeader(buffer, { profile, type });
  if (fileError) {
    return Promise.resolve({
      ok: false,
      fileError,
      rows: [],
      rowErrors: [],
      warnings: [],
      stats: { totalRows: 0, validRows: 0, rejectedRows: 0 },
    });
  }

  const headerLine = firstLineOf(buffer);
  const separator = detectSeparator(headerLine);
  const rawHeaders = splitHeaderCells(headerLine, separator);
  const usingDefault = type === PROFILE_TYPE.FA && isDefaultProfile(type, profile);

  // field -> 0-based position in THIS file. Identical to COL when unedited.
  const resolved = resolveFieldMap(type, profile, rawHeaders);
  const at = resolved.index;
  // Errors must name the column as it appears in the user's own file.
  const colName = columnNamesFor(resolved, rawHeaders);

  /*
   * With the template the header is exact, so there is nothing to report. A
   * custom mapping is the loose case: a column the mapping does not reach is
   * silently ignored, and a field with no column is silently blank. Both are
   * reported as warnings so the upload says what it did with the file.
   */
  const headerWarnings = [];
  if (!usingDefault) {
    const templateColumns = new Set(FA_HEADERS.map((h) => h.trim().toLowerCase()));
    const usedIndexes = new Set(Object.values(resolved.index));
    rawHeaders.forEach((header, i) => {
      if (usedIndexes.has(i) || !String(header || "").trim()) return;
      // Template columns left unmapped (an unanswered "filler", SETDATEFLAG …)
      // are expected, not worth a warning.
      if (templateColumns.has(String(header).trim().toLowerCase())) return;
      headerWarnings.push({
        row: null,
        code: WARNING.UNUSED_FILE_COLUMN,
        message: `Column "${header}" is not mapped to anything, so its values are ignored.`,
        column: header,
      });
    });
  }

  return new Promise((resolve, reject) => {
    const rows = [];
    const rowErrors = [];
    const warnings = [...headerWarnings];
    let totalRows = 0;
    let lineNumber = 1; // row 1 is the header

    const fail = (rowNumber, code, message, column, value) => {
      rowErrors.push({ row: rowNumber, code, message, column, value });
    };

    Readable.from(buffer)
      .pipe(csvParser({ separator, headers: false, skipLines: 1 }))
      .on("data", (arr) => {
        lineNumber += 1;
        const rowNumber = lineNumber;

        const cell = (i) => text(arr[i]);
        /** Read a mapped field. Unmapped optional fields read as blank. */
        const get = (field) => (field in at ? text(arr[at[field]]) : "");

        // A trailing blank line is not a data row.
        const anyValue = Object.values(arr).some((v) => text(v) !== "");
        if (!anyValue) return;

        totalRows += 1;

        /*
         * Column count must match the header exactly.
         *
         * This parser addresses cells by POSITION, so a row with the wrong number
         * of fields does not fail — it silently shifts every column after the
         * offending one. An unescaped comma in Description is enough to do it,
         * and the result is a row whose quantity or net amount is read from the
         * wrong cell while everything still looks valid. That has to be an error.
         */
        const expectedCount = usingDefault ? FA_HEADERS.length : rawHeaders.length;
        const fieldCount = Object.keys(arr).length;
        if (fieldCount !== expectedCount) {
          fail(
            rowNumber,
            ROW_ERROR.COLUMN_COUNT_MISMATCH,
            `This row has ${fieldCount} values but the file's header has ${expectedCount} columns. ` +
              "A comma inside a field must be wrapped in double quotes, or every column after it is read from the wrong place.",
            null,
            null,
          );
          return;
        }

        const accountCode = normCode(get("accountCode"));
        const isin = normIsin(get("isin"));
        const rawType = get("tranType");
        const rawDate = get("tranDate");
        const rawQty = get("quantity");

        let rejected = false;

        if (!accountCode) {
          fail(
            rowNumber,
            ROW_ERROR.MISSING_ACCOUNT_CODE,
            `${colName.accountCode} is blank. Every trade must carry an account code.`,
            colName.accountCode,
            get("accountCode"),
          );
          rejected = true;
        }

        if (!isin) {
          fail(rowNumber, ROW_ERROR.MISSING_ISIN, `${colName.isin} is blank.`, colName.isin, "");
          rejected = true;
        }

        const tranType = normTranType(rawType, SIDE.FA);
        if (!rawType) {
          fail(rowNumber, ROW_ERROR.MISSING_TRAN_TYPE, `${colName.tranType} is blank.`, colName.tranType, "");
          rejected = true;
        } else if (!tranType) {
          fail(
            rowNumber,
            ROW_ERROR.UNKNOWN_TRAN_TYPE,
            `"${rawType}" is not a recognised transaction type. Expected BY-*, BY+, SQB, OPI, BUY or B for a buy; SELL, SL+, SL, S or OPO for a sell.`,
            colName.tranType,
            rawType,
          );
          rejected = true;
        }

        /*
         * Dates may arrive as 20-08-2026, 20-Aug-2026, 2026-08-20 and so on
         * (see parseDate). Each one is converted to YYYY-MM-DD here and written
         * back into the row below, because the import accepts nothing else and
         * stores the value it is given as it is.
         */
        const isoDates = {};
        const readDate = (field) => {
          const raw = get(field);
          if (!raw) {
            fail(rowNumber, ROW_ERROR.MISSING_DATE, `${colName[field]} is blank.`, colName[field], "");
            return null;
          }
          const iso = parseDate(raw);
          if (!iso) {
            fail(
              rowNumber,
              ROW_ERROR.BAD_DATE,
              `${colName[field]} "${raw}" is not a readable date. Use a format such as 2026-08-20 or 20-08-2026.`,
              colName[field],
              raw,
            );
            return null;
          }
          isoDates[field] = iso;
          return iso;
        };

        const tranDate = readDate("tranDate");
        if (!tranDate) rejected = true;

        // The settlement dates are required too. Placeholders such as 0, null
        // or NA are not dates, so they fail like any other unreadable value.
        // OPI / OPO move shares, not cash, so their cash settlement date may be
        // blank; the import already stores a blank one as empty.
        const movesNoCash = ["OPI", "OPO"].includes(String(rawType || "").trim().toUpperCase());
        for (const field of ["settlementDate", "cashSetdate"]) {
          if (!(field in at)) continue;
          if (field === "cashSetdate" && movesNoCash && !get(field)) continue;
          if (!readDate(field)) rejected = true;
        }
        /*
         * A row dated differently from the run is NOT a problem: a run covers
         * every date its file contains, and each row is compared on its own
         * date. Warning per row meant a legitimate multi-day file produced one
         * note per row — thousands of them saying nothing.
         */

        const quantity = num(rawQty);
        if (quantity === null) {
          fail(
            rowNumber,
            ROW_ERROR.BAD_QUANTITY,
            `"${rawQty}" is not a number.`,
            colName.quantity,
            rawQty,
          );
          rejected = true;
        } else if (quantity <= 0) {
          fail(
            rowNumber,
            ROW_ERROR.ZERO_QUANTITY,
            `Quantity must be greater than zero; found ${quantity}. Direction is carried by ${colName.tranType}, not by the sign.`,
            colName.quantity,
            rawQty,
          );
          rejected = true;
        }

        /*
         * Rate must be filled in: the import works out net rate and net amount
         * from it, and a blank rate would store the trade at zero cost. Zero is
         * allowed, as on the custodian files. The fee columns are optional, so
         * only a filled cell is checked.
         */
        const amountChecks = [
          { field: "rate", required: true },
          // Stored in a number column, so text there would fail the import.
          { field: "mktRate", required: false },
          // Both sit under a heading of "filler", so the message names the field too.
          { field: "transactionFee", required: false, name: "Transaction Fee" },
          { field: "transactionFeeServiceTax", required: false, name: "Transaction Fee Service Tax" },
        ];
        for (const { field, required, name } of amountChecks) {
          if (!(field in at)) continue;
          const column = name ? `${colName[field]} (${name})` : colName[field];
          const raw = get(field);
          const value = num(raw);
          if (!raw) {
            if (!required) continue;
            fail(rowNumber, ROW_ERROR.MISSING_VALUE, `${column} is blank.`, column, "");
            rejected = true;
          } else if (value === null) {
            fail(rowNumber, ROW_ERROR.BAD_NUMBER, `${column} "${raw}" is not a number.`, column, raw);
            rejected = true;
          } else if (value < 0) {
            fail(
              rowNumber,
              ROW_ERROR.NEGATIVE_NUMBER,
              `${column} cannot be negative; found ${raw}.`,
              column,
              raw,
            );
            rejected = true;
          }
        }

        if (rejected) return;

        // The original 26 cells, so the consolidated file can be produced by
        // swapping ONE column rather than rebuilding a row from parsed fields.
        // Anything we did not parse still reaches the import untouched.
        const cells = rawHeaders.map((_, i) => cell(i));
        // Every mapped field, raw. When the file is NOT the 26-column template
        // this is what the consolidated file is rebuilt from, so a remapped
        // upload still reaches the import in the layout the import expects.
        const mapped = Object.fromEntries(Object.keys(at).map((f) => [f, get(f)]));
        // Except the dates, which reach the import already converted.
        for (const [field, iso] of Object.entries(isoDates)) {
          cells[at[field]] = iso;
          mapped[field] = iso;
        }

        rows.push({
          rowNumber,
          side: SIDE.FA,
          cells,
          mapped,
          usedTemplateLayout: usingDefault,
          accountCode,
          isin,
          tranDate,
          tranType,
          quantity,
          // Display only — carried to the exception row and difference file, never compared.
          rate: displayNum(get("rate")),
          brokerage: displayNum(get("brokerage")),
          stt: displayNum(get("stt")),
          netAmount: displayNum(get("netAmount")),
          // Only the mutual fund file carries these; they read blank elsewhere.
          tds: displayNum(get("tds")),
          stampDuty: displayNum(get("stampDuty")),
          settlementDate: isoDates.settlementDate || null,
          ref: get("ref"),
          exchange: get("exchange"),
          brokerCode: get("brokerCode"),
          description: get("description"),
        });
      })
      .on("end", () => {
        if (totalRows === 0) {
          resolve({
            ok: false,
            fileError: {
              code: FILE_ERROR.NO_DATA_ROWS,
              message: "The file has a valid header but no transaction rows.",
              hint: "Check the export covers the intended date.",
            },
            rows: [],
            rowErrors,
            warnings,
            stats: { totalRows: 0, validRows: 0, rejectedRows: 0 },
          });
          return;
        }

        resolve({
          ok: rowErrors.length === 0,
          fileError: null,
          rows,
          rowErrors,
          warnings,
          stats: {
            totalRows,
            validRows: rows.length,
            rejectedRows: totalRows - rows.length,
          },
        });
      })
      .on("error", (err) =>
        reject(
          Object.assign(new Error(`Could not read the transaction file: ${err.message}`), {
            code: FILE_ERROR.PARSE_FAILED,
          }),
        ),
      );
  });
};

/**
 * Is this transaction file the Equity file or the Mutual Funds file?
 *
 * Only the mutual fund file reports deductions, so a number in its TDS or Stamp
 * Duty column on any row marks it as the Mutual Funds file. The columns are
 * found through the saved Mutual Funds mapping; an Equity file usually has no
 * such columns at all and always reads as Equity.
 *
 * @param {Buffer} buffer
 * @param {object} [opts]
 * @param {object} [opts.profiles] saved field mappings, keyed by PROFILE_TYPE.
 * @returns {Promise<string>} PROFILE_TYPE.FA or PROFILE_TYPE.FA_MUTUAL_FUND
 */
export const detectFaFileType = (buffer, { profiles = null } = {}) => {
  if (!buffer || buffer.length === 0) return Promise.resolve(PROFILE_TYPE.FA);

  const headerLine = firstLineOf(buffer);
  const separator = detectSeparator(headerLine);
  const rawHeaders = splitHeaderCells(headerLine, separator);
  const { index } = resolveFieldMap(
    PROFILE_TYPE.FA_MUTUAL_FUND,
    profiles?.[PROFILE_TYPE.FA_MUTUAL_FUND] || null,
    rawHeaders,
  );
  const deductionColumns = [index.tds, index.stampDuty].filter((i) => i !== undefined);
  if (deductionColumns.length === 0) return Promise.resolve(PROFILE_TYPE.FA);

  return new Promise((resolve, reject) => {
    let found = false;
    const stream = Readable.from(buffer).pipe(csvParser({ separator, headers: false, skipLines: 1 }));
    stream
      .on("data", (arr) => {
        if (found) return;
        if (deductionColumns.some((i) => num(arr[i]) !== null)) {
          found = true;
          stream.destroy();
          resolve(PROFILE_TYPE.FA_MUTUAL_FUND);
        }
      })
      .on("end", () => {
        if (!found) resolve(PROFILE_TYPE.FA);
      })
      .on("error", (err) => {
        if (!found) reject(err);
      });
  });
};

/**
 * Does this file carry the TDS and Stamp Duty columns, as the given mutual fund
 * mapping places them? Unlike detectFaFileType this looks at the columns, not
 * their values: a mutual fund OPI/OPO file may leave both blank on every row.
 *
 * @param {Buffer} buffer
 * @param {object} opts
 * @param {string} opts.type     the mutual fund profile type the file is read with
 * @param {object} [opts.profile] its saved mapping; null uses the built-in one
 */
export const hasDeductionColumns = (buffer, { type, profile = null }) => {
  const headerLine = firstLineOf(buffer);
  const rawHeaders = splitHeaderCells(headerLine, detectSeparator(headerLine));
  const { index } = resolveFieldMap(type, profile, rawHeaders);
  return index.tds !== undefined && index.stampDuty !== undefined;
};

/** Import headings of the extra columns, the same as their Transaction column names. */
export const EXTRA_COLUMN_HEADERS = EXTRA_COLUMN_FIELDS.map((_, i) => `Column${i + 1}`);

/** RFC4180 field: quote when the value contains a comma, quote or newline. */
const csvField = (v) => {
  const str = v === null || v === undefined ? "" : String(v);
  return /[",\n\r]/.test(str) ? `"${str.replace(/"/g, '""')}"` : str;
};

/**
 * Stage S1 output — the FA file rewritten onto actual codes.
 *
 * Identical 26-column header, one output row per input row. Only BROKERACID
 * changes; every other cell is passed through exactly as it arrived, so the file
 * that eventually reaches the existing import is the user's file with one column
 * corrected, not a reconstruction of it.
 *
 * Rows are NOT aggregated here. Summing onto match keys is the comparison's job;
 * this file has to stay row-for-row so it remains a faithful transaction file.
 *
 * @param {object[]} rows rows from parseFaFile, after resolveActualCodes
 * @param {object} [opts]
 * @param {boolean} [opts.withDeductions] append the TDS and Stamp Duty columns.
 *        Only the import file asks for them, and only when the mutual fund file
 *        is part of it; the file the matcher re-reads stays at 26 columns.
 * @param {boolean} [opts.withTransactionId] append the Transaction ID column (Mutual Funds commit).
 * @param {boolean} [opts.withExtraColumns] append Column1 ... Column5, the extra
 *        columns added on the mapping screen. Only the Mutual Funds commit asks for them.
 */
export const faRowsToConsolidatedCsv = (
  rows,
  { withDeductions = false, withTransactionId = false, withExtraColumns = false } = {},
) => {
  const headers = withDeductions ? [...FA_HEADERS, ...FA_DEDUCTION_HEADERS] : [...FA_HEADERS];
  if (withTransactionId) headers.push(FA_TRANSACTION_ID_HEADER);
  if (withExtraColumns) headers.push(...EXTRA_COLUMN_HEADERS);
  const lines = [headers.map(csvField).join(",")];

  for (const row of rows) {
    const cells = faRowTemplateCells(row);
    if (withDeductions) {
      // Regular-file rows have no such columns and go in blank.
      cells.push(row.mapped?.tds ?? "", row.mapped?.stampDuty ?? "");
    }
    if (withTransactionId) cells.push(row.mapped?.transactionId ?? "");
    if (withExtraColumns) {
      // An extra column the user did not add goes in blank.
      cells.push(...EXTRA_COLUMN_FIELDS.map((field) => row.mapped?.[field] ?? ""));
    }
    lines.push(cells.map(csvField).join(","));
  }

  return lines.join("\n");
};

/** One row as the 26 template cells the import is handed (see faRowsToConsolidatedCsv). */
export const faRowTemplateCells = (row) => {
  /*
   * Two paths, and the choice is made per row so a mixed set cannot go wrong.
   *
   * Template layout -> copy the original cells and swap ONE column, so every
   *                    value the parser never looked at still reaches the
   *                    import exactly as the user exported it.
   * Remapped layout -> the original cells are in the USER's order, which the
   *                    import cannot read. Rebuild the 26 template columns
   *                    from the mapped values instead. Columns the profile
   *                    does not map come out blank, exactly as an empty cell
   *                    in the template does today.
   */
  let cells;
  if (row.usedTemplateLayout === false) {
    cells = new Array(FA_HEADERS.length).fill("");
    for (const [field, slot] of Object.entries(FA_TEMPLATE_SLOT)) {
      const value = row.mapped ? row.mapped[field] : undefined;
      if (value !== undefined && value !== null) cells[slot] = value;
    }
  } else {
    cells = row.cells ? [...row.cells] : new Array(FA_HEADERS.length).fill("");
    // A short row must still emit 26 columns or the import rejects the file.
    while (cells.length < FA_HEADERS.length) cells.push("");
    cells.length = FA_HEADERS.length;
  }

  cells[COL.accountCode] = row.actualCode || row.accountCode || "";
  return cells;
};

/**
 * The values the import stores for a row in Transaction (WS_Account_code, ISIN,
 * Tran_Type, TRANDATE, SETDATE, QTY, RATE), taken from the cells it is handed.
 * Lets a commit tell whether a trade is already booked.
 */
export const faRowStoredValues = (row) => {
  const cells = faRowTemplateCells(row);
  return {
    account: cells[COL.accountCode],
    isin: cells[COL.isin],
    tranType: cells[COL.tranType],
    tranDate: cells[COL.tranDate],
    setDate: cells[COL.settlementDate],
    quantity: cells[COL.quantity],
    rate: cells[COL.rate],
  };
};
