/**
 * Custody Matching — custodian deal / allocation parser.
 *
 * Three layouts arrive and one canonical shape comes out. After this module,
 * nothing downstream knows or cares which format a row came from, apart from
 * the fileType used to keep mutual funds in their own comparison.
 *
 * Each scheme sends ONE file. Which layout it uses depends on the account type:
 *
 *   Format 1 (Resident, RESIDENTIAL) — keyed on CLIENT_CODE, trade date in TRADE_DATE.
 *   Format 2 (NRE/NRO, NDPMS)        — keyed on CPCode_UCC, trade date derived
 *                                      from the first 8 digits of ContractNo.
 *   Format 3 (Mutual Funds)          — keyed on UCC, trade date in TradeDate,
 *                                      direction as Subscription / Redemption.
 *
 * Both already carry actual codes, so no virtual->actual mapping is needed here.
 * Column order is not enforced (unlike the FA template) — custodians reorder
 * their exports — but every required column must be present.
 */

import { Readable } from "stream";
import csvParser from "csv-parser";

import {
  CUSTODY_F1_HEADERS,
  CUSTODY_F2_HEADERS,
  CUSTODY_F3_HEADERS,
  CUSTODY_FORMAT,
  CUSTODY_TYPE_TO_FORMAT,
  DEFAULT_OPTIONS,
  FILE_ERROR,
  ROW_ERROR,
  SIDE,
  WARNING,
} from "./constants.js";
import {
  contractNoDatePart,
  detectSeparator,
  displayNum,
  firstLineOf,
  normCode,
  normIsin,
  normMutualFundTranType,
  normTranType,
  num,
  parseContractNoDate,
  parseDate,
  splitHeaderCells,
  text,
} from "./normalize.js";
import {
  FORMAT_TO_PROFILE_TYPE,
  TRANSFORM,
  columnNamesFor,
  resolveFieldMap,
} from "./fieldMap.js";

/**
 * Above this many missing columns, the file is treated as the wrong file rather
 * than a renamed one — the advice changes from "rename these" to "check the
 * type and the file".
 */
const TOO_MANY_MISMATCHED = 5;

/** Amount columns: every row must carry a number, zero or more. */
const AMOUNT_FIELDS = ["rate", "brokerage", "stt", "netAmount"];

/** Text columns that must be filled in on every row; any value is accepted. */
const REQUIRED_TEXT_FIELDS = ["ref", "exchange", "brokerCode"];

/** Case / space / punctuation-insensitive header form, so "Client Code" and "CLIENT_CODE" collapse together. */
const normHeader = (h) =>
  String(h || "")
    .trim()
    .toLowerCase()
    .replace(/[\s._\-/()]+/g, "");

/** The header that identifies each layout, when no profile overrides it. */
const FORMAT_SIGNATURE = {
  [CUSTODY_FORMAT.RESIDENT]: "CLIENT_CODE",
  [CUSTODY_FORMAT.NDPMS]: "CPCode_UCC",
  [CUSTODY_FORMAT.MUTUAL_FUND]: "UCC",
};

/** Human name of each layout, for error messages. */
const LAYOUT_LABEL = {
  [CUSTODY_FORMAT.RESIDENT]: "Resident",
  [CUSTODY_FORMAT.NDPMS]: "NRE/NRO/NDPMS",
  [CUSTODY_FORMAT.MUTUAL_FUND]: "Mutual Funds",
};

/** The file type each layout belongs to, for the wrong-slot message. */
const FORMAT_TO_FILE_TYPE = Object.fromEntries(
  Object.entries(CUSTODY_TYPE_TO_FORMAT).map(([fileType, format]) => [format, fileType]),
);

/** Standard headers of each layout; ones we never read do not trigger warnings. */
const LAYOUT_HEADERS = {
  [CUSTODY_FORMAT.RESIDENT]: CUSTODY_F1_HEADERS,
  [CUSTODY_FORMAT.NDPMS]: CUSTODY_F2_HEADERS,
  [CUSTODY_FORMAT.MUTUAL_FUND]: CUSTODY_F3_HEADERS,
};

/**
 * The column that identifies a layout is whichever one the profile reads the
 * account code from — CLIENT_CODE, CPCode_UCC and UCC by default, but a
 * custodian who renames it must still have their file recognised.
 */
const signatureFor = (format, profiles) => {
  const source = profiles?.[FORMAT_TO_PROFILE_TYPE[format]]?.actualCode;
  if (source && source.by === "name" && String(source.name || "").trim()) return source.name;
  return FORMAT_SIGNATURE[format];
};

/**
 * Work out which layout a custodian file uses, from its header row.
 *
 * @returns {{format:string|null, headerIndex:object, rawHeaders:string[], separator:string, fileError:object|null}}
 */
export const detectCustodyFormat = (buffer, { profiles = null } = {}) => {
  const empty = {
    format: null,
    headerIndex: {},
    rawHeaders: [],
    separator: ",",
  };

  if (!buffer || buffer.length === 0) {
    return {
      ...empty,
      fileError: {
        code: FILE_ERROR.EMPTY_FILE,
        message: "The file is empty.",
        hint: "Ask the custodian to resend the file.",
      },
    };
  }

  const headerLine = firstLineOf(buffer);
  const separator = detectSeparator(headerLine);
  const rawHeaders = splitHeaderCells(headerLine, separator).filter(
    (h) => h !== "",
  );

  if (rawHeaders.length === 0) {
    return {
      ...empty,
      separator,
      fileError: {
        code: FILE_ERROR.NO_HEADER,
        message: "The first row is blank, so the columns cannot be identified.",
      },
    };
  }

  const headerIndex = {};
  const seen = new Set();
  const duplicates = [];
  for (const raw of rawHeaders) {
    const key = normHeader(raw);
    if (seen.has(key)) duplicates.push(raw);
    seen.add(key);
    if (!(key in headerIndex)) headerIndex[key] = raw;
  }

  if (duplicates.length > 0) {
    return {
      ...empty,
      separator,
      rawHeaders,
      fileError: {
        code: FILE_ERROR.DUPLICATE_HEADERS,
        message: `The header row repeats ${duplicates.length} column name(s): ${duplicates.join(", ")}.`,
        duplicates,
        hint: "Each column must appear once.",
      },
    };
  }

  // Exactly one layout's identifying column must be present.
  const signatures = Object.values(CUSTODY_FORMAT).map((f) => ({
    format: f,
    signature: signatureFor(f, profiles),
  }));
  const found = signatures.filter((s) => normHeader(s.signature) in headerIndex);

  if (found.length !== 1) {
    const expected = signatures.map((s) => s.signature);
    return {
      ...empty,
      separator,
      rawHeaders,
      fileError: {
        code: FILE_ERROR.UNKNOWN_CUSTODY_FORMAT,
        message:
          found.length > 1
            ? `Unknown column names — this file has ${found.map((s) => s.signature).join(" and ")}. Check the mapping.`
            : `Unknown column names — expected one of ${expected.join(", ")}. Check the mapping.`,
        rawHeaders,
        expected,
      },
    };
  }

  const format = found[0].format;
  const profileType = FORMAT_TO_PROFILE_TYPE[format];
  const profile = profiles?.[profileType] || null;

  /*
   * Strict for Residential and NRE/NRO: every field the mapping names must be
   * present. The mapping is the contract between us and the custodian's export —
   * if it says the quantity lives in QTY and the file calls it Quantity, that is
   * a disagreement to fix before the numbers are trusted.
   *
   * Mutual Funds only require the five match-key columns; a missing display
   * column (Amount, REFNo …) reads as blank in the report.
   */
  const strict = format !== CUSTODY_FORMAT.MUTUAL_FUND;
  const resolved = resolveFieldMap(profileType, profile, rawHeaders, { strict });
  if (!resolved.ok) {
    const missing = resolved.errors.map((e) => e.expected || e.label).filter(Boolean);
    const layout = LAYOUT_LABEL[format];
    const mappedTotal = Object.keys(resolved.index).length + missing.length;

    /*
     * A couple of missing columns is a renamed export. Most of them missing is
     * a different file or the wrong account type, and telling somebody to
     * rename ten columns is the wrong instruction for that.
     */
    if (missing.length > TOO_MANY_MISMATCHED) {
      return {
        ...empty,
        separator,
        rawHeaders,
        format,
        fileError: {
          code: FILE_ERROR.HEADER_MAPPING_MISMATCH,
          message: `Unknown column names — ${missing.join(", ")}. Check the mapping.`,
          missing,
          mappedTotal,
          tooMany: true,
          layout,
          rawHeaders,
          mapErrors: resolved.errors,
        },
      };
    }

    return {
      ...empty,
      separator,
      rawHeaders,
      format,
      fileError: {
        code: FILE_ERROR.HEADER_MAPPING_MISMATCH,
        message: `Unknown column names — ${missing.join(", ")}. Check the mapping.`,
        missing,
        rawHeaders,
        mapErrors: resolved.errors,
      },
    };
  }

  return { format, headerIndex, rawHeaders, separator, resolved, fileError: null };
};

/**
 * Parse one custodian file into canonical rows.
 *
 * @param {Buffer} buffer
 * @param {object} opts
 * @param {string} [opts.scheme]       scheme tag from the filename; stamped on every row and every exception.
 * @param {string} [opts.fileType]     RESIDENTIAL | NDPMS | MUTUAL_FUND, from the filename suffix. Cross-checked against the detected layout.
 * @param {string} [opts.fileName]     for error messages.
 * @param {string} [opts.businessDate] yyyy-mm-dd
 * @param {object} [opts.options]      overrides for DEFAULT_OPTIONS.
 * @param {object} [opts.profiles]     user-defined field mappings, keyed by PROFILE_TYPE.
 */
export const parseCustodyFile = (
  buffer,
  {
    scheme = "",
    fileType = null,
    fileName = "",
    businessDate = null,
    options = {},
    profiles = null,
  } = {},
) => {
  const cfg = { ...DEFAULT_OPTIONS, ...options };
  const detected = detectCustodyFormat(buffer, { profiles });

  const emptyResult = (fileError) => ({
    ok: false,
    fileError: { ...fileError, fileName },
    format: detected.format,
    scheme,
    fileType,
    fileName,
    rows: [],
    rowErrors: [],
    warnings: [],
    stats: { totalRows: 0, validRows: 0, rejectedRows: 0 },
  });

  if (detected.fileError) return Promise.resolve(emptyResult(detected.fileError));

  // The filename suffix decides the scheme tag on every exception row, so a file
  // dropped into the wrong slot would mislabel its exceptions. Stop rather than warn.
  if (fileType && CUSTODY_TYPE_TO_FORMAT[fileType] !== detected.format) {
    return Promise.resolve(
      emptyResult({
        code: FILE_ERROR.FORMAT_TYPE_MISMATCH,
        message: `Wrong account type — ${fileType} selected, ${FORMAT_TO_FILE_TYPE[detected.format]} file`,
        declaredType: fileType,
        detectedFormat: detected.format,
      }),
    );
  }

  const { format, separator, rawHeaders, resolved } = detected;

  /*
   * field -> the raw header it resolved to in THIS file, or null when the file
   * does not carry it. Built from the saved profile rather than a fixed table,
   * so a custodian renaming a column is a mapping edit, not a code change.
   */
  const names = columnNamesFor(resolved, rawHeaders);
  const columnOf = {};
  for (const field of Object.keys(resolved.index)) columnOf[field] = names[field] ?? null;

  /*
   * Column order and extra columns are not enforced here — custodians reorder
   * and extend their exports, and rejecting a file for that would be wrong.
   * Silence is wrong too: a renamed optional column reads as a blank in the
   * report with nothing to explain it. So both directions are reported as
   * warnings, which surface on the upload without failing it.
   */
  const headerWarnings = [];
  /*
   * Both standard layouts carry columns we deliberately never read
   * (EXECABCON_DATE, Segment, Column1/2/3 …). Warning about those would fire on
   * every correct file and drown the one case worth seeing: a column this
   * custodian sends that the format does not describe at all.
   */
  const expectedButUnused = new Set(LAYOUT_HEADERS[format].map(normHeader));
  const usedIndexes = new Set(Object.values(resolved.index));
  rawHeaders.forEach((header, i) => {
    if (usedIndexes.has(i) || !String(header || "").trim()) return;
    if (expectedButUnused.has(normHeader(header))) return;
    headerWarnings.push({
      row: null,
      code: WARNING.UNUSED_FILE_COLUMN,
      message: `Column "${header}" is not part of the ${LAYOUT_LABEL[format]} layout, so its values are ignored.`,
      column: header,
      fileName,
    });
  });

  /* How each field's raw value is interpreted. NDPMS has no date column at all;
   * its trade date is the first 8 characters of ContractNo. */
  const transformOf = resolved.transforms;

  // Mutual Funds rows use Subscription/Redemption and may leave display cells blank.
  const isMutualFundLayout = format === CUSTODY_FORMAT.MUTUAL_FUND;

  /*
   * The Mutual Funds columns beyond the match key and the shared display set.
   * Carried as plain text — none is compared, so none may reject a row.
   */
  const MF_EXTRA_FIELDS = ["folioNo", "schemeName", "amcName", "firstHolderName"];

  return new Promise((resolve, reject) => {
    const rows = [];
    const rowErrors = [];
    const warnings = [...headerWarnings];
    let totalRows = 0;
    let lineNumber = 1; // row 1 is the header

    const fail = (rowNumber, code, message, column, value) => {
      rowErrors.push({ row: rowNumber, code, message, column, value, fileName });
    };

    Readable.from(buffer)
      .pipe(csvParser({ separator }))
      .on("data", (record) => {
        lineNumber += 1;
        const rowNumber = lineNumber;

        const cell = (field) => {
          const col = columnOf[field];
          return col ? text(record[col]) : "";
        };

        const anyValue = Object.values(record).some((v) => text(v) !== "");
        if (!anyValue) return;

        totalRows += 1;

        let rejected = false;

        // ---- account code -------------------------------------------------
        let actualCode = normCode(cell("actualCode"));
        if (!actualCode && cfg.clientCodeFallback && columnOf.fallbackCode) {
          const fallback = normCode(cell("fallbackCode"));
          if (fallback) {
            actualCode = fallback;
            warnings.push({
              row: rowNumber,
              code: WARNING.NON_NUMERIC_DISPLAY_FIELD,
              message: `${columnOf.actualCode} was blank; fell back to ${columnOf.fallbackCode} "${fallback}".`,
              column: columnOf.actualCode,
              value: fallback,
              fileName,
            });
          }
        }
        if (!actualCode) {
          fail(
            rowNumber,
            ROW_ERROR.MISSING_ACCOUNT_CODE,
            `${columnOf.actualCode} is blank. Every custodian row must carry an account code.`,
            columnOf.actualCode,
            "",
          );
          rejected = true;
        }

        // ---- ISIN ---------------------------------------------------------
        const isin = normIsin(cell("isin"));
        if (!isin) {
          fail(rowNumber, ROW_ERROR.MISSING_ISIN, `${columnOf.isin} is blank.`, columnOf.isin, "");
          rejected = true;
        }

        // ---- transaction date ---------------------------------------------
        let tranDate = null;
        const dateCol = columnOf.tranDate;
        if (transformOf.tranDate !== TRANSFORM.DATE_FROM_FIRST_8) {
          const rawDate = cell("tranDate");
          // Every layout accepts any format parseDate reads (20-08-2026,
          // 20-Aug-2026, 2026-08-20, ...) and carries it on as YYYY-MM-DD.
          tranDate = parseDate(rawDate);
          if (!rawDate) {
            fail(rowNumber, ROW_ERROR.MISSING_DATE, `${dateCol} is blank.`, dateCol, "");
            rejected = true;
          } else if (!tranDate) {
            fail(
              rowNumber,
              ROW_ERROR.BAD_DATE,
              `"${rawDate}" is not a readable date. Use a format such as 2026-08-20 or 20-08-2026.`,
              dateCol,
              rawDate,
            );
            rejected = true;
          }
        } else {
          const rawContract = cell("tranDate");
          const digits = contractNoDatePart(rawContract);
          if (!digits) {
            fail(
              rowNumber,
              ROW_ERROR.BAD_CONTRACT_NO,
              `${dateCol} "${rawContract}" does not start with an 8-digit date.`,
              dateCol,
              rawContract,
            );
            rejected = true;
          } else {
            tranDate = parseContractNoDate(rawContract);
            if (!tranDate) {
              fail(
                rowNumber,
                ROW_ERROR.BAD_DATE,
                `The first 8 digits of ${dateCol} ("${digits}") are not a valid ddmmyyyy date.`,
                dateCol,
                rawContract,
              );
              rejected = true;
            }
          }
        }

        /*
         * No warning when a row's date differs from the run's. A run covers
         * every date in its files and compares each row on its own date, so
         * this only ever produced one useless note per row.
         */

        // ---- direction ------------------------------------------------------
        const rawType = cell("tranType");
        const tranType = isMutualFundLayout
          ? normMutualFundTranType(rawType)
          : normTranType(rawType, SIDE.CUSTODY);
        if (!rawType) {
          fail(
            rowNumber,
            ROW_ERROR.MISSING_TRAN_TYPE,
            `${columnOf.tranType} is blank.`,
            columnOf.tranType,
            "",
          );
          rejected = true;
        } else if (!tranType) {
          fail(
            rowNumber,
            ROW_ERROR.UNKNOWN_TRAN_TYPE,
            isMutualFundLayout
              ? `"${rawType}" is not a recognised transaction type. Expected Subscription for a buy or Redemption for a sell.`
              : `"${rawType}" is not a recognised transaction type. Expected BUY, B, BY+ or BY-* for a buy; SELL, SL+, SL or S for a sell.`,
            columnOf.tranType,
            rawType,
          );
          rejected = true;
        }

        // ---- quantity -------------------------------------------------------
        const rawQty = cell("quantity");
        const quantity = num(rawQty);
        if (quantity === null) {
          fail(
            rowNumber,
            ROW_ERROR.BAD_QUANTITY,
            `"${rawQty}" is not a number.`,
            columnOf.quantity,
            rawQty,
          );
          rejected = true;
        } else if (quantity <= 0) {
          fail(
            rowNumber,
            ROW_ERROR.ZERO_QUANTITY,
            `Quantity must be greater than zero; found ${quantity}.`,
            columnOf.quantity,
            rawQty,
          );
          rejected = true;
        }

        // ---- the columns shown in the report --------------------------------
        /*
         * Not part of the match, but carried into the difference report, where a
         * blank or unreadable value would otherwise show up as a silent 0 or an
         * empty cell. A field with no column (an older mapping that marked it as
         * not present) has nothing to check.
         *
         * Mutual Funds files may leave these cells blank; a filled one is still
         * checked, so a typo does not reach the report as a silent 0.
         */
        for (const field of AMOUNT_FIELDS) {
          const column = columnOf[field];
          if (!column) continue;
          const raw = cell(field);
          const value = num(raw);
          if (!raw) {
            if (isMutualFundLayout) continue;
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

        if (columnOf.settlementDate) {
          const column = columnOf.settlementDate;
          const raw = cell("settlementDate");
          if (!raw) {
            if (!isMutualFundLayout) {
              fail(rowNumber, ROW_ERROR.MISSING_VALUE, `${column} is blank.`, column, "");
              rejected = true;
            }
          } else if (!parseDate(raw)) {
            fail(rowNumber, ROW_ERROR.BAD_DATE, `${column} "${raw}" is not a readable date.`, column, raw);
            rejected = true;
          }
        }

        for (const field of REQUIRED_TEXT_FIELDS) {
          const column = columnOf[field];
          if (!column || isMutualFundLayout) continue;
          if (!cell(field)) {
            fail(rowNumber, ROW_ERROR.MISSING_VALUE, `${column} is blank.`, column, "");
            rejected = true;
          }
        }

        if (rejected) return;

        rows.push({
          rowNumber,
          side: SIDE.CUSTODY,
          format,
          scheme,
          fileType,
          fileName,
          actualCode,
          isin,
          tranDate,
          tranType,
          quantity,
          // Display only — never compared.
          rate: displayNum(cell("rate")),
          brokerage: displayNum(cell("brokerage")),
          stt: displayNum(cell("stt")),
          netAmount: displayNum(cell("netAmount")),
          settlementDate: parseDate(cell("settlementDate")),
          ref: cell("ref"),
          exchange: cell("exchange"),
          brokerCode: cell("brokerCode"),
          ...(isMutualFundLayout
            ? Object.fromEntries(MF_EXTRA_FIELDS.map((f) => [f, cell(f)]))
            : null),
        });
      })
      .on("end", () => {
        if (totalRows === 0) {
          resolve(
            emptyResult({
              code: FILE_ERROR.NO_DATA_ROWS,
              message: "The file has a valid header but no trade rows.",
            }),
          );
          return;
        }

        resolve({
          ok: rowErrors.length === 0,
          fileError: null,
          format,
          scheme,
          fileType,
          fileName,
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
          Object.assign(
            new Error(`Could not read ${fileName || "the custodian file"}: ${err.message}`),
            { code: FILE_ERROR.PARSE_FAILED, fileName },
          ),
        ),
      );
  });
};

/**
 * Stage S2 — merge every scheme's custodian file for the day into one dataset.
 *
 * One file per scheme, so every row from every file counts. Files are parsed
 * independently so one bad file names itself in the error rather than failing
 * the batch anonymously.
 *
 * @param {Array<{buffer:Buffer, scheme?:string, fileType?:string, fileName?:string}>} files
 * @param {object} opts same businessDate / options as parseCustodyFile
 */
export const mergeCustodyFiles = async (
  files,
  { businessDate = null, options = {}, profiles = null } = {},
) => {
  const rows = [];
  const rowErrors = [];
  const warnings = [];
  const fileErrors = [];
  const perFile = [];

  for (const file of files) {
    const result = await parseCustodyFile(file.buffer, {
      scheme: file.scheme,
      fileType: file.fileType,
      fileName: file.fileName,
      businessDate,
      options,
      profiles,
    });

    perFile.push({
      fileName: file.fileName,
      scheme: file.scheme,
      fileType: file.fileType,
      format: result.format,
      ok: result.ok,
      stats: result.stats,
    });

    if (result.fileError) {
      fileErrors.push(result.fileError);
      continue;
    }

    rows.push(...result.rows);
    rowErrors.push(...result.rowErrors);
    warnings.push(...result.warnings);
  }

  return {
    ok: fileErrors.length === 0 && rowErrors.length === 0,
    fileErrors,
    rows,
    rowErrors,
    warnings,
    perFile,
    stats: {
      fileCount: files.length,
      totalRows: perFile.reduce((sum, f) => sum + (f.stats?.totalRows || 0), 0),
      validRows: rows.length,
    },
  };
};

/**
 * Stage S2 output — the merged custodian dataset, in one canonical layout.
 *
 * Written as CSV because the user asked for `final-<date>-Custodian.csv`, and
 * read back by S4. Persisting it is what lets the comparison resume after a
 * crash without re-parsing every custodian file, and lets a disputed result be
 * reproduced months later from exactly what was compared.
 */
export const CUSTODY_FINAL_COLUMNS = [
  "scheme",
  "fileType",
  "format",
  "fileName",
  "sourceRow",
  "actualCode",
  "isin",
  "tranDate",
  "tranType",
  "quantity",
  "rate",
  "brokerage",
  "stt",
  "netAmount",
  "settlementDate",
  "ref",
  "exchange",
  "brokerCode",
];

const csvField = (v) => {
  const str = v === null || v === undefined ? "" : String(v);
  return /[",\n\r]/.test(str) ? `"${str.replace(/"/g, '""')}"` : str;
};

export const custodyRowsToCsv = (rows) => {
  const lines = [CUSTODY_FINAL_COLUMNS.join(",")];
  for (const row of rows) {
    lines.push(
      CUSTODY_FINAL_COLUMNS.map((c) =>
        csvField(c === "sourceRow" ? row.rowNumber : row[c]),
      ).join(","),
    );
  }
  return lines.join("\n");
};

/**
 * Read back a file written by custodyRowsToCsv.
 *
 * Deliberately does NOT re-validate: these rows already passed validation on the
 * way in, and re-running the row rules here would let a stricter future rule
 * silently drop rows from a comparison that is resuming mid-flight.
 */
export const parseCustodyFinalCsv = (buffer) =>
  new Promise((resolve, reject) => {
    const rows = [];
    Readable.from(buffer)
      .pipe(csvParser())
      .on("data", (record) => {
        const actualCode = text(record.actualCode);
        if (!actualCode) return;
        rows.push({
          side: SIDE.CUSTODY,
          scheme: text(record.scheme),
          fileType: text(record.fileType),
          format: text(record.format),
          fileName: text(record.fileName),
          rowNumber: Number(record.sourceRow) || 0,
          actualCode,
          isin: text(record.isin),
          tranDate: text(record.tranDate),
          tranType: text(record.tranType),
          quantity: displayNum(record.quantity),
          rate: displayNum(record.rate),
          brokerage: displayNum(record.brokerage),
          stt: displayNum(record.stt),
          netAmount: displayNum(record.netAmount),
          settlementDate: text(record.settlementDate) || null,
          ref: text(record.ref),
          exchange: text(record.exchange),
          brokerCode: text(record.brokerCode),
        });
      })
      .on("end", () => resolve(rows))
      .on("error", reject);
  });
