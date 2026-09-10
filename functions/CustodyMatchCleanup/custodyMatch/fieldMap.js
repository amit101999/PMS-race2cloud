/**
 * Custody Matching — user-defined field mapping.
 *
 * Until now every column name lived in code: FA_HEADERS (26 exact strings, exact
 * order), CUSTODY_F1_HEADERS, CUSTODY_F2_HEADERS. A custodian renaming QTY to
 * Quantity meant a code change. This module moves that decision to the user.
 *
 * The CANONICAL FIELDS are still fixed — they are what the match key is built
 * from, so they cannot be user-defined without the comparison losing meaning.
 * What becomes dynamic is only which column in *their* file feeds each one.
 *
 * Two details drive the shape of a mapping entry, and neither is incidental:
 *
 *   1. Sources address a column by NAME or by INDEX. The FA template has three
 *      columns literally named "filler" and the second one is Net_Amount, so a
 *      name-only model cannot express it.
 *   2. A source may carry a TRANSFORM. NDPMS files have no date column at all —
 *      the trade date is the first 8 characters of ContractNo.
 *
 * DEFAULT_PROFILES reproduce today's hardcoded behaviour exactly, so nothing
 * changes until somebody edits a profile.
 */

import { CUSTODY_FORMAT, FA_HEADERS } from "./constants.js";
import { detectSeparator, firstLineOf, splitHeaderCells } from "./normalize.js";

/** Which file a profile describes. One profile per type, shared across schemes. */
export const PROFILE_TYPE = {
  FA: "FA",
  CUSTODY_RESIDENTIAL: "CUSTODY_RESIDENTIAL",
  CUSTODY_NDPMS: "CUSTODY_NDPMS",
};

export const PROFILE_TYPES = Object.values(PROFILE_TYPE);

/** Stored blobs carry this so a later shape change can migrate rather than misread. */
export const PROFILE_VERSION = 1;

/** How a mapping entry addresses a column in the user's file. */
export const SOURCE_BY = {
  NAME: "name",
  INDEX: "index",
};

/** Value-level rewrites applied after a column is located. */
export const TRANSFORM = {
  NONE: "NONE",
  /** First 8 characters, read as a compact date. NDPMS ContractNo -> trade date. */
  DATE_FROM_FIRST_8: "DATE_FROM_FIRST_8",
};

export const TRANSFORMS = Object.values(TRANSFORM);

/** Errors this module reports. Kept distinct from FILE_ERROR — these are config faults, not file faults. */
export const MAP_ERROR = {
  UNKNOWN_TYPE: "UNKNOWN_TYPE",
  UNKNOWN_FIELD: "UNKNOWN_FIELD",
  MISSING_REQUIRED: "MISSING_REQUIRED",
  BAD_SOURCE: "BAD_SOURCE",
  BAD_TRANSFORM: "BAD_TRANSFORM",
  TRANSFORM_NOT_ALLOWED: "TRANSFORM_NOT_ALLOWED",
  COLUMN_NOT_IN_FILE: "COLUMN_NOT_IN_FILE",
  INDEX_OUT_OF_RANGE: "INDEX_OUT_OF_RANGE",
};

/**
 * The canonical fields, per profile type.
 *
 * `required` marks the five that build the match key (Actual Code + ISIN +
 * Transaction Date + Transaction Type, compared on Quantity). A profile missing
 * any of them is rejected at save time rather than producing silent nonsense.
 *
 * `kind` is a hint for the UI and for per-field validation; it does not change
 * how the column is located.
 */
export const FIELD_CATALOG = {
  [PROFILE_TYPE.FA]: [
    { field: "accountCode", label: "Account Code", kind: "code", required: true },
    { field: "isin", label: "ISIN", kind: "isin", required: true },
    { field: "tranDate", label: "Transaction Date", kind: "date", required: true },
    { field: "tranType", label: "Transaction Type", kind: "type", required: true },
    { field: "quantity", label: "Quantity", kind: "number", required: true },

    { field: "brokerCode", label: "Broker Code", kind: "code", required: false },
    { field: "exchange", label: "Exchange", kind: "text", required: false },
    { field: "settlementDate", label: "Settlement Date", kind: "date", required: false },
    { field: "rate", label: "Rate", kind: "number", required: false },
    { field: "brokerage", label: "Brokerage", kind: "number", required: false },
    { field: "serviceTax", label: "Service Tax", kind: "number", required: false },
    { field: "stt", label: "STT", kind: "number", required: false },
    { field: "ref", label: "Transaction Ref", kind: "text", required: false },
    { field: "description", label: "Description", kind: "text", required: false },
    { field: "netAmount", label: "Net Amount", kind: "number", required: false },

    // Not used by matching, but carried through to the production import. A
    // remapped file that cannot fill these would silently lose Security_code
    // and PORTFOLIOID on every row it writes to the Transaction table.
    { field: "setDateFlag", label: "Settlement Date Flag", kind: "text", required: false },
    { field: "mktRate", label: "Market Rate", kind: "number", required: false },
    { field: "cashSymbolCode", label: "Cash Symbol Code", kind: "code", required: false },
    { field: "accruedInterest", label: "Accrued Interest", kind: "number", required: false },
    { field: "blockId", label: "Block / Portfolio ID", kind: "code", required: false },
    { field: "chequeNumber", label: "Cheque Number", kind: "text", required: false },
    { field: "chequeDetail", label: "Cheque Detail", kind: "text", required: false },
    { field: "bankRef", label: "Bank Ref", kind: "text", required: false },
    { field: "netRate", label: "Net Rate", kind: "number", required: false },
    { field: "cashSetdate", label: "Cash Settlement Date", kind: "date", required: false },
  ],

  [PROFILE_TYPE.CUSTODY_RESIDENTIAL]: [
    { field: "actualCode", label: "Actual Code", kind: "code", required: true },
    { field: "isin", label: "ISIN", kind: "isin", required: true },
    { field: "tranDate", label: "Transaction Date", kind: "date", required: true },
    { field: "tranType", label: "Transaction Type", kind: "type", required: true },
    { field: "quantity", label: "Quantity", kind: "number", required: true },

    { field: "fallbackCode", label: "Fallback Code", kind: "code", required: false },
    { field: "rate", label: "Rate", kind: "number", required: false },
    { field: "brokerage", label: "Brokerage", kind: "number", required: false },
    { field: "stt", label: "STT", kind: "number", required: false },
    { field: "netAmount", label: "Net Amount", kind: "number", required: false },
    { field: "settlementDate", label: "Settlement Date", kind: "date", required: false },
    { field: "ref", label: "Trade / Contract Ref", kind: "text", required: false },
    { field: "exchange", label: "Exchange", kind: "text", required: false },
    { field: "brokerCode", label: "Broker Code", kind: "code", required: false },
  ],

  [PROFILE_TYPE.CUSTODY_NDPMS]: [
    { field: "actualCode", label: "Actual Code", kind: "code", required: true },
    { field: "isin", label: "ISIN", kind: "isin", required: true },
    {
      field: "tranDate",
      label: "Transaction Date",
      kind: "date",
      required: true,
      // NDPMS carries no date column; it is derived from ContractNo.
      allowTransforms: [TRANSFORM.NONE, TRANSFORM.DATE_FROM_FIRST_8],
    },
    { field: "tranType", label: "Transaction Type", kind: "type", required: true },
    { field: "quantity", label: "Quantity", kind: "number", required: true },

    { field: "rate", label: "Rate", kind: "number", required: false },
    { field: "brokerage", label: "Brokerage", kind: "number", required: false },
    { field: "stt", label: "STT", kind: "number", required: false },
    { field: "netAmount", label: "Net Amount", kind: "number", required: false },
    { field: "settlementDate", label: "Settlement Date", kind: "date", required: false },
    { field: "ref", label: "Trade / Contract Ref", kind: "text", required: false },
    { field: "exchange", label: "Exchange", kind: "text", required: false },
    { field: "brokerCode", label: "Broker Code", kind: "code", required: false },
  ],
};

/** Custodian layout -> profile type, so callers holding a CUSTODY_FORMAT can find the profile. */
export const FORMAT_TO_PROFILE_TYPE = {
  [CUSTODY_FORMAT.RESIDENT]: PROFILE_TYPE.CUSTODY_RESIDENTIAL,
  [CUSTODY_FORMAT.NDPMS]: PROFILE_TYPE.CUSTODY_NDPMS,
};

const byName = (name) => ({ by: SOURCE_BY.NAME, name, transform: TRANSFORM.NONE });
const byIndex = (index) => ({ by: SOURCE_BY.INDEX, index, transform: TRANSFORM.NONE });

/**
 * The built-in profiles — byte-for-byte today's behaviour.
 *
 * FA is addressed by INDEX because of the three "filler" columns; the header row
 * is validated for exact order before rows are read, so index is safe there.
 * Custodian files are addressed by NAME because custodians reorder their exports.
 */
export const DEFAULT_PROFILES = {
  [PROFILE_TYPE.FA]: {
    accountCode: byIndex(1), // BROKERACID
    isin: byIndex(2), // SYMBOLCODE
    tranDate: byIndex(5), // DATEPUR_ACQUI
    tranType: byIndex(4), // TRANSTYPE
    quantity: byIndex(7), // QUANTITY

    brokerCode: byIndex(0),
    exchange: byIndex(3),
    settlementDate: byIndex(6),
    rate: byIndex(8),
    brokerage: byIndex(9),
    serviceTax: byIndex(10),
    stt: byIndex(14), // TRANEXPENSE
    ref: byIndex(17), // TRANSREF
    description: byIndex(18),
    netAmount: byIndex(23), // the SECOND "filler"

    setDateFlag: byIndex(11),
    mktRate: byIndex(12),
    cashSymbolCode: byIndex(13),
    accruedInterest: byIndex(15),
    blockId: byIndex(16),
    chequeNumber: byIndex(19),
    chequeDetail: byIndex(20),
    bankRef: byIndex(21),
    netRate: byIndex(22), // the FIRST "filler"
    cashSetdate: byIndex(25),
  },

  [PROFILE_TYPE.CUSTODY_RESIDENTIAL]: {
    actualCode: byName("CLIENT_CODE"),
    isin: byName("ISIN"),
    tranDate: byName("TRADE_DATE"),
    tranType: byName("B_S"),
    quantity: byName("QTY"),

    fallbackCode: byName("UCC_CODE"),
    rate: byName("RATE"),
    brokerage: byName("BROKERAGE"),
    stt: byName("STT"),
    netAmount: byName("NET"),
    settlementDate: byName("SETTLEMENT_DATE"),
    ref: byName("TRADENO"),
    exchange: byName("EXCHANGE"),
    brokerCode: byName("BROKERCODE"),
  },

  [PROFILE_TYPE.CUSTODY_NDPMS]: {
    actualCode: byName("CPCode_UCC"),
    isin: byName("ISIN"),
    tranDate: {
      by: SOURCE_BY.NAME,
      name: "ContractNo",
      transform: TRANSFORM.DATE_FROM_FIRST_8,
    },
    tranType: byName("TranType"),
    quantity: byName("Quantity"),

    rate: byName("MarketRate"),
    brokerage: byName("Brokerage"),
    stt: byName("STT"),
    netAmount: byName("NetAmount"),
    settlementDate: byName("SettlementDate"),
    ref: byName("ContractNo"),
    exchange: byName("Exchange"),
    brokerCode: byName("BrokerRegNo"),
  },
};

/** Case / space / punctuation-insensitive header form — same rule parseCustody.js uses. */
export const normHeader = (h) =>
  String(h || "")
    .trim()
    .toLowerCase()
    .replace(/[\s._\-/()]+/g, "");

const catalogFor = (type) => FIELD_CATALOG[type] || null;

/** @returns {object|null} the catalog entry for one field, or null when the field is not in the catalog. */
export const catalogEntry = (type, field) =>
  (catalogFor(type) || []).find((e) => e.field === field) || null;

/** @returns {string[]} the required field names for a type. */
export const requiredFields = (type) =>
  (catalogFor(type) || []).filter((e) => e.required).map((e) => e.field);

/** A deep-enough copy that callers can edit a default without mutating it. */
export const defaultProfile = (type) => {
  const src = DEFAULT_PROFILES[type];
  if (!src) return null;
  const out = {};
  for (const [field, source] of Object.entries(src)) out[field] = { ...source };
  return out;
};

const allowedTransforms = (entry) =>
  entry && Array.isArray(entry.allowTransforms) ? entry.allowTransforms : [TRANSFORM.NONE];

/**
 * Check a profile in isolation — shape, unknown fields, required coverage.
 *
 * Deliberately does NOT need the file: this is what the save endpoint calls, and
 * a profile must be storable before the next day's file exists. Checks that need
 * the actual header row happen in resolveFieldMap.
 *
 * @param {string} type   one of PROFILE_TYPE
 * @param {object} profile  { field: {by, name|index, transform} }
 * @returns {{ok:boolean, errors:Array<{code:string,field?:string,message:string}>}}
 */
export const validateProfile = (type, profile) => {
  const errors = [];
  const catalog = catalogFor(type);

  if (!catalog) {
    return {
      ok: false,
      errors: [
        {
          code: MAP_ERROR.UNKNOWN_TYPE,
          message: `"${type}" is not a known file type. Expected one of: ${PROFILE_TYPES.join(", ")}.`,
        },
      ],
    };
  }

  const entries = profile && typeof profile === "object" ? profile : {};

  for (const [field, source] of Object.entries(entries)) {
    const entry = catalogEntry(type, field);

    if (!entry) {
      errors.push({
        code: MAP_ERROR.UNKNOWN_FIELD,
        field,
        message: `"${field}" is not a field on the ${type} layout.`,
      });
      continue;
    }

    if (!source || typeof source !== "object") {
      errors.push({
        code: MAP_ERROR.BAD_SOURCE,
        field,
        message: `${entry.label} has no column selected.`,
      });
      continue;
    }

    const transform = source.transform || TRANSFORM.NONE;
    if (!TRANSFORMS.includes(transform)) {
      errors.push({
        code: MAP_ERROR.BAD_TRANSFORM,
        field,
        message: `${entry.label} uses an unknown transform "${transform}".`,
      });
    } else if (!allowedTransforms(entry).includes(transform)) {
      errors.push({
        code: MAP_ERROR.TRANSFORM_NOT_ALLOWED,
        field,
        message: `${entry.label} cannot use the "${transform}" transform.`,
      });
    }

    if (source.by === SOURCE_BY.NAME) {
      const name = String(source.name ?? "").trim();
      if (!name) {
        errors.push({
          code: MAP_ERROR.BAD_SOURCE,
          field,
          message: `${entry.label} has an empty column name.`,
        });
        continue;
      }
    } else if (source.by === SOURCE_BY.INDEX) {
      const index = source.index;
      if (!Number.isInteger(index) || index < 0) {
        errors.push({
          code: MAP_ERROR.BAD_SOURCE,
          field,
          message: `${entry.label} has an invalid column position.`,
        });
        continue;
      }
    } else {
      errors.push({
        code: MAP_ERROR.BAD_SOURCE,
        field,
        message: `${entry.label} must be mapped by column name or by position.`,
      });
    }
  }

  for (const field of requiredFields(type)) {
    if (!entries[field]) {
      const entry = catalogEntry(type, field);
      errors.push({
        code: MAP_ERROR.MISSING_REQUIRED,
        field,
        // No heading was ever chosen, so the field's own name is what the
        // message can name.
        label: entry.label,
        required: true,
        message: `${entry.label} must be mapped — it is part of the match key.`,
      });
    }
  }

  return { ok: errors.length === 0, errors };
};

/**
 * Read a file's header row so the mapping screen can offer real columns.
 *
 * Columns are returned WITH their position and a disambiguated label, because
 * the FA template repeats the name "filler" three times and picking the right
 * one is the whole point.
 *
 * @param {Buffer} buffer
 * @returns {{separator:string, columns:Array<{index:number,name:string,label:string,duplicate:boolean}>}}
 */
export const readHeaderColumns = (buffer) => {
  if (!buffer || buffer.length === 0) return { separator: ",", columns: [] };

  const headerLine = firstLineOf(buffer);
  const separator = detectSeparator(headerLine);
  const cells = splitHeaderCells(headerLine, separator);

  // A header line that is present but entirely blank identifies no columns at
  // all. Reporting one nameless column would let the mapping screen offer it.
  if (cells.every((c) => String(c ?? "").trim() === "")) return { separator, columns: [] };

  const counts = new Map();
  for (const cell of cells) {
    const key = normHeader(cell);
    counts.set(key, (counts.get(key) || 0) + 1);
  }

  const columns = cells.map((name, index) => {
    const duplicate = (counts.get(normHeader(name)) || 0) > 1;
    return {
      index,
      name,
      // A repeated name is useless on its own, so show where it sits.
      label: duplicate ? `${name} (column ${index + 1})` : name,
      duplicate,
    };
  });

  return { separator, columns };
};

/**
 * Bind a profile to one file's actual header row.
 *
 * @param {string} type
 * @param {object} profile     validated profile; pass null to use the default
 * @param {string[]} rawHeaders  the file's row-1 cells, in order
 * @param {object} [opts]
 * @param {boolean} [opts.strict] treat EVERY mapped field as required. The
 *        mapping is a contract: if it names a heading, the file must carry it.
 *        Off, only the match-key fields have to resolve.
 * @returns {{ok:boolean, index:object, transforms:object, errors:Array, unmappedOptional:string[]}}
 *          `index` is field -> 0-based column position, ready for positional reads.
 */
export const resolveFieldMap = (type, profile, rawHeaders, { strict = false } = {}) => {
  const effective = profile || defaultProfile(type);
  const base = validateProfile(type, effective);
  if (!base.ok) {
    return { ok: false, index: {}, transforms: {}, errors: base.errors, unmappedOptional: [] };
  }

  const headers = Array.isArray(rawHeaders) ? rawHeaders : [];
  const nameToIndex = new Map();
  headers.forEach((h, i) => {
    const key = normHeader(h);
    // First occurrence wins; duplicates are only addressable by index, by design.
    if (!nameToIndex.has(key)) nameToIndex.set(key, i);
  });

  const index = {};
  const transforms = {};
  const errors = [];
  const unmappedOptional = [];

  for (const [field, source] of Object.entries(effective)) {
    const entry = catalogEntry(type, field);
    let resolved = -1;

    if (source.by === SOURCE_BY.NAME) {
      const key = normHeader(source.name);
      resolved = nameToIndex.has(key) ? nameToIndex.get(key) : -1;
      if (resolved === -1 && (entry.required || strict)) {
        errors.push({
          code: MAP_ERROR.COLUMN_NOT_IN_FILE,
          field,
          label: entry.label,
          expected: source.name,
          required: Boolean(entry.required),
          message: `${entry.label} is mapped to a column named "${source.name}", which this file does not have.`,
          hint: "Rename the column in the file to match, or open Field Mapping and change the mapping to the name the file uses.",
        });
      }
    } else {
      resolved = source.index < headers.length ? source.index : -1;
      /*
       * The contract is what the user NAMED. An index is a position in the
       * standard template, so for an optional field it says nothing about a
       * file of a different shape — enforcing it would refuse an upload over
       * fields nobody chose, naming column numbers instead of headings.
       * Required fields are still enforced either way.
       */
      if (resolved === -1 && entry.required) {
        errors.push({
          code: MAP_ERROR.INDEX_OUT_OF_RANGE,
          field,
          label: entry.label,
          expected: `column ${source.index + 1}`,
          required: Boolean(entry.required),
          message: `${entry.label} is mapped to column ${source.index + 1}, but this file has only ${headers.length}.`,
          hint: "Open Field Mapping and re-pick the column.",
        });
      }
    }

    if (resolved === -1) {
      if (!entry.required) unmappedOptional.push(field);
      continue;
    }

    index[field] = resolved;
    transforms[field] = source.transform || TRANSFORM.NONE;
  }

  return { ok: errors.length === 0, index, transforms, errors, unmappedOptional };
};

/** A stored profile blob, ready for Stratus. */
export const toStoredProfile = (type, profile, { updatedBy = "", updatedAtMs = Date.now() } = {}) => ({
  version: PROFILE_VERSION,
  type,
  profile,
  updatedBy: String(updatedBy || "").slice(0, 255),
  updatedAtMs,
});

/**
 * Read a stored blob back, falling back to the default when it is absent or
 * unreadable. A corrupt config must not take the upload screen down with it.
 *
 * @returns {{profile:object, isDefault:boolean, updatedAtMs:number|null, updatedBy:string}}
 */
export const fromStoredProfile = (type, blob) => {
  const fallback = {
    profile: defaultProfile(type),
    isDefault: true,
    updatedAtMs: null,
    updatedBy: "",
  };

  if (!blob || typeof blob !== "object" || !blob.profile) return fallback;
  if (blob.version !== PROFILE_VERSION) return fallback;
  if (!validateProfile(type, blob.profile).ok) return fallback;

  return {
    profile: blob.profile,
    isDefault: false,
    updatedAtMs: Number(blob.updatedAtMs) || null,
    updatedBy: String(blob.updatedBy || ""),
  };
};

/** The full FA header list, exposed so the mapping UI can seed a profile with no sample file. */
export const FA_DEFAULT_COLUMNS = FA_HEADERS.map((name, index) => ({
  index,
  name,
  label: FA_HEADERS.filter((h) => h === name).length > 1 ? `${name} (column ${index + 1})` : name,
  duplicate: FA_HEADERS.filter((h) => h === name).length > 1,
}));

/**
 * Canonical field -> its position in the 26-column FA template.
 *
 * The inverse of the FA default profile, and the thing that lets a file in ANY
 * layout be rewritten into the template the production import still expects.
 * Column 24 (the third "filler") has no field: the import drops it anyway.
 */
export const FA_TEMPLATE_SLOT = Object.fromEntries(
  Object.entries(DEFAULT_PROFILES[PROFILE_TYPE.FA]).map(([field, source]) => [field, source.index]),
);

/**
 * Is this profile the shipped default?
 *
 * Callers use it to keep the strict path strict: an unedited FA profile still
 * gets the exact 26-column header check, so nothing about today's behaviour
 * changes until somebody actually edits the mapping.
 */
export const isDefaultProfile = (type, profile) => {
  const base = DEFAULT_PROFILES[type];
  if (!base) return false;
  if (!profile) return true;

  const fields = new Set([...Object.keys(base), ...Object.keys(profile)]);
  for (const field of fields) {
    const a = base[field];
    const b = profile[field];
    if (!a || !b) return false;
    if (a.by !== b.by) return false;
    if (a.by === SOURCE_BY.NAME && normHeader(a.name) !== normHeader(b.name)) return false;
    if (a.by === SOURCE_BY.INDEX && a.index !== b.index) return false;
    if ((a.transform || TRANSFORM.NONE) !== (b.transform || TRANSFORM.NONE)) return false;
  }
  return true;
};

/**
 * field -> the raw header it resolved to, for error messages.
 *
 * Errors have to name the column as it appears in the USER's file. Saying
 * "BROKERACID is blank" about a file whose column is called "Account" is worse
 * than useless.
 */
export const columnNamesFor = (resolved, rawHeaders) => {
  const out = {};
  const headers = Array.isArray(rawHeaders) ? rawHeaders : [];
  for (const [field, index] of Object.entries(resolved.index || {})) {
    out[field] = headers[index] ?? `column ${index + 1}`;
  }
  return out;
};
