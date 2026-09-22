/**
 * Custody Matching — normalisation primitives.
 *
 * Every value that takes part in the match key passes through here, so FA and
 * custodian rows are compared in the same shape regardless of how each file
 * spelled them. Dependency-free: this file is vendored into the job function.
 */

import {
  CUSTODY_FILE_TYPE,
  MF_FA_EXCHANGE,
  MF_ISIN_PREFIX,
  SIDE,
  TRAN_TYPE,
} from "./constants.js";

/** Strip a UTF-8 BOM from decoded text. */
export const stripBom = (text) =>
  text.charCodeAt(0) === 0xfeff ? text.slice(1) : text;

/** First physical line of a buffer, BOM removed. */
export const firstLineOf = (buffer) => {
  const text = stripBom(buffer.toString("utf8"));
  const nl = text.search(/\r?\n/);
  return nl === -1 ? text : text.slice(0, nl);
};

/**
 * Tab if the header line contains tabs, otherwise comma.
 * Matches detectSeparator() in TempTransactionUpload.js — broker exports arrive
 * both ways and the header row is the only reliable place to tell.
 */
export const detectSeparator = (headerLine) =>
  (headerLine.match(/\t/g) || []).length > 0 ? "\t" : ",";

/** Split a header line into trimmed, BOM-free cells. */
export const splitHeaderCells = (headerLine, separator) =>
  headerLine.split(separator).map((c) => c.trim().replace(/^\uFEFF/, "").trim());

/** Trim to a string, treating null/undefined as empty. */
export const text = (v) => (v == null ? "" : String(v).trim());

/**
 * Tolerant number: strips thousands separators and spaces.
 * Returns null (not 0) when the cell cannot be read as a number, so callers can
 * tell "absent" from "zero" — the difference matters for quantity.
 */
export const num = (v) => {
  const s = text(v);
  if (s === "") return null;
  const n = Number(s.replace(/[,\s]/g, ""));
  return Number.isFinite(n) ? n : null;
};

/** Display-only numeric: same parse, but an unreadable cell degrades to 0. */
export const displayNum = (v) => {
  const n = num(v);
  return n === null ? 0 : n;
};

/** Account / client code: uppercased and trimmed. Codes are case-insensitive upstream. */
export const normCode = (v) => text(v).toUpperCase();

/**
 * ISIN: uppercase, trimmed, currency suffix removed (INE467B01029-INR -> INE467B01029).
 * The ISIN format itself is not validated — callers only reject a blank value.
 */
export const normIsin = (v) => {
  const s = text(v).toUpperCase();
  if (!s) return "";
  const dash = s.indexOf("-");
  return dash > 0 ? s.slice(0, dash) : s;
};

const pad2 = (n) => String(n).padStart(2, "0");

/** True when y-m-d is a real calendar date (rejects 2026-02-30). */
const isRealDate = (y, m, d) => {
  if (m < 1 || m > 12 || d < 1 || d > 31) return false;
  const dt = new Date(Date.UTC(y, m - 1, d));
  return (
    dt.getUTCFullYear() === y &&
    dt.getUTCMonth() === m - 1 &&
    dt.getUTCDate() === d
  );
};

/**
 * Parse a date cell to ISO yyyy-mm-dd, or null.
 *
 * Accepts yyyy-mm-dd, yyyy/mm/dd, dd-mm-yyyy, dd/mm/yyyy and yyyymmdd.
 * Deliberately does NOT fall back to `new Date(str)`: that quietly reads
 * "03/07/2026" as 3 July or 7 March depending on the host locale, which would
 * move trades between match keys in a way nobody could reproduce.
 */
export const parseDate = (v) => {
  const s = text(v);
  if (!s) return null;

  // Trailing time component, if any, is not part of a trade date.
  const head = s.split(/[T\s]/)[0];

  let m = head.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$/);
  if (m) {
    const [, y, mo, d] = m.map(Number);
    return isRealDate(y, mo, d) ? `${y}-${pad2(mo)}-${pad2(d)}` : null;
  }

  m = head.match(/^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$/);
  if (m) {
    const d = Number(m[1]);
    const mo = Number(m[2]);
    const y = Number(m[3]);
    return isRealDate(y, mo, d) ? `${y}-${pad2(mo)}-${pad2(d)}` : null;
  }

  m = head.match(/^(\d{8})$/);
  if (m) return parseCompactDate(m[1]).date;

  return null;
};

const ISO_DATE_SHAPE = /^(\d{4})-(\d{2})-(\d{2})$/;

/**
 * Parse a date cell that must be exactly yyyy-mm-dd, or null.
 *
 * The strict counterpart of parseDate, for the columns where the standard
 * format is required: the FA file's dates (the import accepts nothing else) and
 * the Residential custodian trade date.
 */
export const parseIsoDate = (v) => {
  const s = text(v);
  const m = s.match(ISO_DATE_SHAPE);
  if (!m) return null;
  const [, y, mo, d] = m.map(Number);
  return isRealDate(y, mo, d) ? s : null;
};

/** Row-error message for a value parseIsoDate refused. */
export const isoDateErrorMessage = (column, raw) =>
  ISO_DATE_SHAPE.test(text(raw))
    ? `${column} "${raw}" is not a real calendar date.`
    : `${column} "${raw}" is not in the standard date format. Use YYYY-MM-DD, for example 2026-07-03.`;

/**
 * Parse an 8-digit compact date, resolving yyyymmdd vs ddmmyyyy.
 *
 * The digit order in ContractNo is still unconfirmed, so this reads it from the
 * value rather than assuming: a leading 19xx/20xx can only be a year, and a
 * trailing 19xx/20xx can only be a year. Only when both readings are valid is
 * the result flagged ambiguous — the caller then cross-checks against the run's
 * business date instead of guessing.
 *
 * @returns {{ date: string|null, ambiguous: boolean, order: "YMD"|"DMY"|null }}
 */
export const parseCompactDate = (digits) => {
  const s = text(digits);
  if (!/^\d{8}$/.test(s)) return { date: null, ambiguous: false, order: null };

  const asYmd = (() => {
    const y = Number(s.slice(0, 4));
    const mo = Number(s.slice(4, 6));
    const d = Number(s.slice(6, 8));
    return y >= 1900 && y <= 2999 && isRealDate(y, mo, d)
      ? `${y}-${pad2(mo)}-${pad2(d)}`
      : null;
  })();

  const asDmy = (() => {
    const d = Number(s.slice(0, 2));
    const mo = Number(s.slice(2, 4));
    const y = Number(s.slice(4, 8));
    return y >= 1900 && y <= 2999 && isRealDate(y, mo, d)
      ? `${y}-${pad2(mo)}-${pad2(d)}`
      : null;
  })();

  if (asYmd && asDmy) {
    // Both readings parse. Prefer yyyymmdd, but tell the caller it was a coin toss.
    return { date: asYmd, ambiguous: asYmd !== asDmy, order: "YMD" };
  }
  if (asYmd) return { date: asYmd, ambiguous: false, order: "YMD" };
  if (asDmy) return { date: asDmy, ambiguous: false, order: "DMY" };
  return { date: null, ambiguous: false, order: null };
};

/** Leading 8 digits of a contract number, or "" when there aren't 8. */
export const contractNoDatePart = (v) => {
  const s = text(v);
  const m = s.match(/^(\d{8})/);
  return m ? m[1] : "";
};

/**
 * The trade date carried in an NDPMS contract number, as ISO yyyy-mm-dd, or null.
 *
 * The first 8 digits are always ddmmyyyy - 210820261 is 21 Aug 2026. Read in
 * that order only: guessing between orders let a value like 01022026 land on
 * the wrong date whenever both readings happened to be real calendar dates.
 */
export const parseContractNoDate = (v) => {
  const digits = contractNoDatePart(v);
  if (!digits) return null;
  const d = Number(digits.slice(0, 2));
  const mo = Number(digits.slice(2, 4));
  const y = Number(digits.slice(4, 8));
  return y >= 1900 && y <= 2999 && isRealDate(y, mo, d) ? `${y}-${pad2(mo)}-${pad2(d)}` : null;
};

/**
 * Normalise a transaction type to BUY / SELL, or null when unrecognised.
 *
 * ORDER IS LOAD-BEARING. Rule 2 must run before anything that inspects the
 * first character: SQB is a BUY code in the FA file and it starts with "S".
 * A first-character rule turns every SQB buy into a sell — the run completes,
 * the report looks plausible, and every quantity sits on the wrong key.
 *
 * Unrecognised tokens return null and become a row error. Never guessed.
 *
 * @param {string} raw
 * @param {"FA"|"CUSTODY"} side SQB / OPI are FA-only codes.
 */
export const normTranType = (raw, side = SIDE.FA) => {
  const t = text(raw).toUpperCase();
  if (!t) return null;

  if (t.startsWith("BY-")) return TRAN_TYPE.BUY;
  if (side === SIDE.FA && (t === "SQB" || t === "OPI")) return TRAN_TYPE.BUY;
  if (t === "BUY" || t === "B" || t === "BY+") return TRAN_TYPE.BUY;
  if (t === "SELL" || t === "SL+" || t === "SL" || t === "S") return TRAN_TYPE.SELL;

  return null;
};

/**
 * Direction in a Mutual Funds custodian file. Only the two fund-house actions
 * are valid there; the equity codes above are not, and vice versa.
 */
export const normMutualFundTranType = (raw) => {
  const t = text(raw).toUpperCase();
  if (t === "SUBSCRIPTION") return TRAN_TYPE.BUY;
  if (t === "REDEMPTION") return TRAN_TYPE.SELL;
  return null;
};

/**
 * Is this parsed FA row a mutual fund transaction?
 *
 * INF ISIN + exchange DIR + a recognised buy/sell. Mutual fund rows are compared
 * only against the Mutual Funds custodian files, everything else only against
 * the Residential and NRE/NRO files.
 *
 * Read from the row rather than from the file it was uploaded in, because the
 * comparison works on the consolidated file S1 writes — a single 26-column
 * rewrite of every transaction file, in which the upload each row came from is
 * no longer recorded. Mutual fund rows only ever reach it from the mutual fund
 * file, so the two readings agree; this one also survives the rewrite.
 */
export const isMutualFundFaRow = (row) =>
  text(row.isin).toUpperCase().startsWith(MF_ISIN_PREFIX) &&
  text(row.exchange).toUpperCase() === MF_FA_EXCHANGE &&
  (row.tranType === TRAN_TYPE.BUY || row.tranType === TRAN_TYPE.SELL);

/** Did this custodian row come from a Mutual Funds file? */
export const isMutualFundCustodyRow = (row) =>
  row.fileType === CUSTODY_FILE_TYPE.MUTUAL_FUND;

/** The match key: actual code, ISIN, transaction date, direction. Quantity is the compared value, never part of the key. */
export const buildMatchKey = ({ actualCode, isin, tranDate, tranType }) =>
  `${actualCode}|${isin}|${tranDate}|${tranType}`;

/** Secondary key used only to re-pair orphans: same trade, disagreeing date or direction. */
export const buildOrphanKey = ({ actualCode, isin, quantity }) =>
  `${actualCode}|${isin}|${quantity}`;

/** Split a match key back into its parts, for report rows. */
export const parseMatchKey = (key) => {
  const [actualCode = "", isin = "", tranDate = "", tranType = ""] =
    String(key).split("|");
  return { actualCode, isin, tranDate, tranType };
};
