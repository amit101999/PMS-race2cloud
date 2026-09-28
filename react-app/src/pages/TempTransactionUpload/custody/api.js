import { BASE_URL } from "../../../constant";

const ROOT = `${BASE_URL}/custody-match`;

/**
 * Error carrying the server's structured body.
 *
 * The upload endpoints answer with a rich payload — per-row errors, header
 * mismatches, the list of blockers — and the screen renders all of it. A plain
 * `throw new Error(message)` would drop everything except the first line.
 */
export class ApiError extends Error {
  constructor(message, { status = 0, detail = null } = {}) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.detail = detail;
  }
}

const NETWORK_MESSAGE = "Could not reach the server. Check that the backend is running.";

async function parse(res) {
  const text = await res.text();
  let body = {};
  if (text) {
    try {
      body = JSON.parse(text);
    } catch {
      throw new ApiError(`The server did not return JSON (${res.status}).`, {
        status: res.status,
      });
    }
  }

  if (!res.ok || body?.success === false) {
    throw new ApiError(body?.message || `Request failed (${res.status}).`, {
      status: res.status,
      detail: body,
    });
  }

  return body;
}

async function request(path, options) {
  let res;
  try {
    res = await fetch(`${ROOT}${path}`, options);
  } catch (err) {
    // fetch rejects with TypeError when the host is unreachable; every other
    // failure already came back as a response.
    throw new ApiError(NETWORK_MESSAGE, { detail: { code: "NETWORK", cause: String(err) } });
  }
  return parse(res);
}

const json = (path, method, body) =>
  request(path, {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });

const form = (path, fields) => {
  const fd = new FormData();
  for (const [k, v] of Object.entries(fields)) {
    if (v !== undefined && v !== null) fd.append(k, v);
  }
  return request(path, { method: "POST", body: fd });
};

/* ── files ────────────────────────────────────────────────────────────── */

export const getDay = (date) => request(`/day/${date}`);

/** Schemes already used, each with the layout it was last filed under. */
export const getSchemes = () => request("/schemes");

/** The dates a run could compare, with what each has on either side. */
export const getRunDates = (date) => request(`/day/${date}/dates`);

export const uploadFaFile = ({
  file,
  businessDate,
  fileType = FA_FILE_TYPE.REGULAR,
  replace = false,
}) => form("/fa-file", { file, businessDate, fileType, replace: replace ? "true" : "" });

export const uploadCustodyFile = ({ file, scheme, fileType, businessDate, replace = false }) =>
  form("/custody-file", {
    file,
    scheme,
    fileType,
    businessDate,
    replace: replace ? "true" : "",
  });

/** Removes one staged file, either side, leaving the rest of the day alone. */
export const deleteUploadedFile = (key) =>
  request(`/file?key=${encodeURIComponent(key)}`, { method: "DELETE" });

/** side: "fa" | "custody" | "all" */
export const clearDay = (date, side) =>
  request(`/day/${date}?side=${side}`, { method: "DELETE" });

/** The last few custodian uploads, newest first, kept for download. */
export const getCustodyHistory = () => request("/custody-history");

export const getDownloadUrl = (key) =>
  request(`/download?key=${encodeURIComponent(key)}`);

/* ── matching ─────────────────────────────────────────────────────────── */

export const startRun = (businessDate, startedBy, dates) =>
  json("/run", "POST", { businessDate, startedBy, dates });

export const getRun = (runId) => request(`/run/${runId}`);

/** Past runs that found differences — the report history. */
export const getReports = (limit = 10) => request(`/reports?limit=${limit}`);

export const getLatestRun = (date) => request(`/latest-run/${date}`);

export const getExceptions = (runId, params = {}) => {
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null && v !== "") qs.set(k, v);
  }
  const q = qs.toString();
  return request(`/run/${runId}/exceptions${q ? `?${q}` : ""}`);
};

export const exportExceptions = (runId) =>
  request(`/run/${runId}/exceptions/export`);

export const retryRun = (runId) => json(`/run/${runId}/retry`, "POST");

export const proceedToImport = (runId) => json(`/run/${runId}/proceed`, "POST");

/* ---------------------------------------------------------------- field mapping */

/** All three profiles plus their field catalogs, in one call. */
export const getFieldMaps = () => request("/field-map");

export const saveFieldMap = (type, profile, updatedBy) =>
  json(`/field-map/${type}`, "PUT", { profile, updatedBy });

/** Deletes the saved mapping for one file type, so every field is mapped again. */
export const deleteFieldMap = (type) => request(`/field-map/${type}`, { method: "DELETE" });

/**
 * Read a file's columns without uploading it.
 *
 * Comes back with each column's position, a disambiguated label, a few sample
 * values and what those values can be used as — everything the mapping
 * dropdowns need. The file is not stored. Every row is read to decide what a
 * column holds, so a bad value anywhere in it is caught here.
 */
export const previewHeaders = (file, type) => form("/field-map/preview", { file, type });

export const PROFILE_TYPE = {
  FA: "FA",
  FA_MUTUAL_FUND: "FA_MUTUAL_FUND",
  CUSTODY_RESIDENTIAL: "CUSTODY_RESIDENTIAL",
  CUSTODY_NDPMS: "CUSTODY_NDPMS",
  CUSTODY_MUTUAL_FUND: "CUSTODY_MUTUAL_FUND",
  CUSTODY_HOLDINGS: "CUSTODY_HOLDINGS",
  CUSTODY_CORP_ACTION: "CUSTODY_CORP_ACTION",
};

export const PROFILE_LABEL = {
  FA: "Transaction file (FA)",
  FA_MUTUAL_FUND: "Transaction file (FA) — Mutual Funds",
  CUSTODY_RESIDENTIAL: "Custodian — Residential",
  CUSTODY_NDPMS: "Custodian — NRE/NRO/NDPMS",
  CUSTODY_MUTUAL_FUND: "Custodian — Mutual Funds",
  CUSTODY_HOLDINGS: "Custodian — Consolidated Holdings",
  CUSTODY_CORP_ACTION: "Custodian — Corporate Actions",
};

export const PROFILE_HELP = {
  FA: "The daily transaction file. Whatever layout you map here is rewritten into the standard 26-column format before it is imported, so the import is unaffected.",
  FA_MUTUAL_FUND:
    "The mutual fund fund-accounting file. Same 26 fields as the regular transaction file — it renames one heading (SYMBOLCODE/ISIN CODE), which is why it is mapped separately. Its rows are matched only against the Mutual Funds custodian files. Dates must be YYYY-MM-DD.",
  CUSTODY_RESIDENTIAL: "The deal allocation file for Resident accounts.",
  CUSTODY_NDPMS: "The deal allocation file for NRE/NRO/NDPMS accounts.",
  CUSTODY_MUTUAL_FUND:
    "The Mutual Funds transaction file (UCC, SourceMFISIN, TradeDate, TransactionType Subscription / Redemption, Units). Matched against the rows from the mutual fund fund-accounting file.",
  CUSTODY_HOLDINGS:
    "The custodian's consolidated holdings file. Its column names differ from the standard 29-column template, so tell us which column holds each field.",
  CUSTODY_CORP_ACTION:
    "The custodian's corporate actions file. Tell us which column holds each field; every field is optional.",
};

/** Which transaction file is being uploaded. A day may hold one of each. */
export const FA_FILE_TYPE = {
  REGULAR: "REGULAR",
  MUTUAL_FUND: "MUTUAL_FUND",
};

export const FA_FILE_TYPE_LABEL = {
  REGULAR: "Equity",
  MUTUAL_FUND: "Mutual Funds",
};

export const SOURCE_BY = { NAME: "name", INDEX: "index", ABSENT: "absent" };

/**
 * Can this column feed this field, under this transform?
 *
 * The type rules themselves live on the server — it reads the file and decides
 * what each column can be used as. This only reads off that answer, so the two
 * cannot disagree about a verdict.
 */
export const isColumnCompatible = (column, entry, transform = "NONE") => {
  if (!column || !entry) return false;
  const kinds =
    transform && transform !== "NONE"
      ? column.transformCompatible?.[transform] || []
      : column.compatibleKinds || [];
  return kinds.includes(entry.kind);
};

/** Plain-language description of what a column holds, for the "why not" line. */
export const KIND_BLURB = {
  isin: "ISINs",
  isoDate: "dates in YYYY-MM-DD format",
  date: "dates",
  type: "buy/sell codes",
  number: "numbers",
  code: "short codes",
  text: "free text",
};

/** Short form for a one-line error: `"abc" is not a number`. */
export const KIND_SHORT = {
  isin: "an ISIN",
  isoDate: "YYYY-MM-DD",
  date: "a date",
  type: "a buy/sell code",
  number: "a number",
  code: "a code",
  text: "text",
};

/** Same normalisation the server uses to match a saved heading to a column. */
export const normHeader = (h) =>
  String(h || "")
    .trim()
    .toLowerCase()
    .replace(/[\s._\-/()]+/g, "");

export const TRANSFORM_LABEL = {
  NONE: "Use the value as-is",
  DATE_FROM_FIRST_8: "Read the date from the first 8 characters",
};

/* ── helpers ──────────────────────────────────────────────────────────── */

export const CUSTODY_FILE_TYPE = {
  RESIDENTIAL: "RESIDENTIAL",
  NDPMS: "NDPMS",
  MUTUAL_FUND: "MUTUAL_FUND",
};

/** Every account type sends one deal-allocation file; only the layout inside differs. */
export const FILE_TYPE_LABEL = {
  RESIDENTIAL: "Residential",
  NDPMS: "NRE/NRO/NDPMS",
  MUTUAL_FUND: "Mutual Funds",
};

/** Exception types, in the order they should appear in the filter. */
export const EXCEPTION_TYPES = [
  "QUANTITY_MISMATCH",
  "MISSING_IN_CUSTODY",
  "MISSING_IN_FA",
  "DATE_MISMATCH",
  "TYPE_MISMATCH",
  "UNMAPPED_CODE",
];

/** Plain-language labels. The grid is read by people chasing a break, not by the system. */
export const EXCEPTION_LABEL = {
  QUANTITY_MISMATCH: "Quantity mismatch",
  MISSING_IN_CUSTODY: "Missing in custody",
  MISSING_IN_FA: "Missing in FA",
  DATE_MISMATCH: "Date mismatch",
  TYPE_MISMATCH: "Buy/sell mismatch",
  UNMAPPED_CODE: "Unmapped code",
};

/** yyyy-mm-dd for today, in local time — toISOString would shift the date across the day boundary. */
export const todayIso = () => {
  const d = new Date();
  const p = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`;
};

export function formatBytes(bytes) {
  if (bytes == null || Number.isNaN(Number(bytes))) return "—";
  const n = Number(bytes);
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(2)} MB`;
}
