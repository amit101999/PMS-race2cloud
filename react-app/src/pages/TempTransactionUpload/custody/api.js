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

/** The files saved under an upload date: every file date, or only `date` when given. */
export const getUploadedFiles = (uploadDate, date = "") =>
  request(
    `/files?uploadDate=${encodeURIComponent(uploadDate || "")}&date=${encodeURIComponent(date || "")}`,
  );

/** Schemes already used, each with the layout it was last filed under. */
export const getSchemes = () => request("/schemes");

/** Every date with uploaded files, newest first, with what each side has. */
export const getMatchDates = () => request("/dates");

/*
 * businessDate — the day the file's rows are for.
 * uploadDate   — the upload date picked at the top; the file is recorded under it.
 */
export const uploadFaFile = ({
  file,
  businessDate,
  uploadDate,
  fileType = FA_FILE_TYPE.REGULAR,
  replace = false,
}) =>
  form("/fa-file", { file, businessDate, uploadDate, fileType, replace: replace ? "true" : "" });

export const uploadCustodyFile = ({
  file,
  scheme,
  fileType,
  businessDate,
  uploadDate,
  replace = false,
}) =>
  form("/custody-file", {
    file,
    scheme,
    fileType,
    businessDate,
    uploadDate,
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

/** Starts the match for one file date's files under one upload date. */
export const startRun = (businessDate, uploadDate, startedBy) =>
  json("/run", "POST", { businessDate, uploadDate, startedBy });

export const getRun = (runId) => request(`/run/${runId}`);

/** Past runs that found differences — the report history. */
export const getReports = (limit = 10) => request(`/reports?limit=${limit}`);

export const getLatestRun = (date, uploadDate) =>
  request(`/latest-run/${date}?uploadDate=${encodeURIComponent(uploadDate || "")}`);

/* The exception, export and commit calls cover every date on screen: one run per date. */
export const getExceptions = (runIds, params = {}) => {
  const qs = new URLSearchParams({ runIds: runIds.join(",") });
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null && v !== "") qs.set(k, v);
  }
  return request(`/runs/exceptions?${qs.toString()}`);
};

export const exportExceptions = (runIds) =>
  request(`/runs/exceptions/export?${new URLSearchParams({ runIds: runIds.join(",") })}`);

export const retryRun = (runId) => json(`/run/${runId}/retry`, "POST");

export const proceedToImport = (runIds) => json("/runs/proceed", "POST", { runIds });

/* ── Equity and Mutual Funds tabs: upload, match and Pre-commit a date ── */

/*
 * The two tabs share these routes. kind says which tab a call is for; the
 * server keeps each tab's files, window, Commit and history apart. The Mutual
 * Funds tab has no custodian file and no match.
 */
/* OPI_OPO and OPI_OPO_MF are the OPI/OPO tab's Equity and Mutual Funds sub-tabs. */
export const KIND = { EQUITY: "EQUITY", MUTUAL_FUND: "MUTUAL_FUND", OPI_OPO: "OPI_OPO", OPI_OPO_MF: "OPI_OPO_MF" };

const kindQuery = (kind) => (kind && kind !== KIND.EQUITY ? `kind=${encodeURIComponent(kind)}` : "");
const withKind = (path, kind) => {
  const q = kindQuery(kind);
  if (!q) return path;
  return `${path}${path.includes("?") ? "&" : "?"}${q}`;
};

/* choice ("ADD" | "REPLACE") is sent only after the server answered mode "ASK". */
export const uploadEquityFaFile = ({ file, businessDate, uploadDate, choice = "", kind = KIND.EQUITY }) =>
  form("/reload/fa-file", { file, businessDate, uploadDate, choice, kind });

export const uploadEquityCustodyFile = ({ file, scheme, fileType, businessDate, uploadDate, replace = false }) =>
  form("/reload/custody-file", {
    file,
    scheme,
    fileType,
    businessDate,
    uploadDate,
    replace: replace ? "true" : "",
  });

export const startEquityRun = (businessDate, uploadDate, startedBy) =>
  json("/reload/run", "POST", { businessDate, uploadDate, startedBy });

/** Pending files, the commit summary and the Pre-commit progress of one date on a tab. */
export const getEquityDate = (date, kind = KIND.EQUITY) => request(withKind(`/reload/${date}`, kind));

export const preCommitEquityDate = (date, { runId = "", confirmBigRemoval = false, kind = KIND.EQUITY }) =>
  json(`/reload/${date}/pre-commit`, "POST", { runId, confirmBigRemoval: confirmBigRemoval ? "true" : "", kind });

export const retryEquityDate = (date, kind = KIND.EQUITY) => json(`/reload/${date}/retry`, "POST", { kind });

export const getCommitHistory = (kind = KIND.EQUITY) => request(withKind("/reload/commit-history", kind));

/* TDS tab: a trade date's TDS files, only kept for now. Each call answers with the date's files. */
export const getTdsFiles = (date) => request(`/tds/${date}`);
export const uploadTdsFile = ({ file, date, uploadDate }) => form(`/tds/${date}`, { file, uploadDate });
export const deleteTdsFile = (date, key) =>
  request(`/tds/${date}?key=${encodeURIComponent(key)}`, { method: "DELETE" });
export const getTdsDownloadUrl = (key) => request(`/tds/download?key=${encodeURIComponent(key)}`);

/* OPI/OPO tab: one file (any number of dates) is one Commit. fileId is the file's uploadedAtMs. */
/* assetKind: the sub-tab, EQUITY or MUTUAL_FUND. */
/* replace: the operator chose to replace an earlier upload of the same file (DUPLICATE_FILE). */
export const uploadOpiOpoFile = ({ file, uploadDate, assetKind, replace = false }) =>
  form("/opi-opo/file", { file, uploadDate, assetKind, replace: replace ? "true" : "" });
export const getOpiOpoFile = (fileId) => request(`/opi-opo/${encodeURIComponent(fileId)}`);
export const commitOpiOpoFile = (fileId, { startedBy = "" } = {}) =>
  json(`/opi-opo/${encodeURIComponent(fileId)}/commit`, "POST", { startedBy });
export const retryOpiOpoFile = (fileId) => json(`/opi-opo/${encodeURIComponent(fileId)}/retry`, "POST", {});

export const getConsolidatedFiles = (kind = KIND.EQUITY) => request(withKind("/reload/consolidated-files", kind));

export const MODE_LABEL = {
  FIRST: "First upload",
  ADD: "Added as extra file",
  REPLACE: "Replaces the date's trades",
  ASK: "Your choice needed",
};

export const CUSTODY_MODE_LABEL = {
  FIRST: "First custodian file",
  ADD: "Added as extra file",
  REPLACE: "Replaced the earlier file(s)",
  SKIP: "Already uploaded",
};

/* ---------------------------------------------------------------- field mapping */

/** All three profiles plus their field catalogs, in one call. */
export const getFieldMaps = () => request("/field-map");

/** `labels` = names of the extra columns added on the mapping screen. */
export const saveFieldMap = (type, profile, updatedBy, labels = {}) =>
  json(`/field-map/${type}`, "PUT", { profile, updatedBy, labels });

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
  FA_OPI_OPO: "FA_OPI_OPO",
  FA_OPI_OPO_MF: "FA_OPI_OPO_MF",
  FA_MUTUAL_FUND_TDS: "FA_MUTUAL_FUND_TDS",
  CUSTODY_RESIDENTIAL: "CUSTODY_RESIDENTIAL",
  CUSTODY_NDPMS: "CUSTODY_NDPMS",
  CUSTODY_MUTUAL_FUND: "CUSTODY_MUTUAL_FUND",
  CUSTODY_HOLDINGS: "CUSTODY_HOLDINGS",
  CUSTODY_CORP_ACTION: "CUSTODY_CORP_ACTION",
  CUSTODY_CORP_ALLOCATION: "CUSTODY_CORP_ALLOCATION",
};

export const PROFILE_LABEL = {
  FA: "Transaction file (FA)",
  FA_MUTUAL_FUND: "Transaction file (FA) — Mutual Funds",
  FA_OPI_OPO: "Transaction file (FA) — OPI/OPO Equity",
  FA_OPI_OPO_MF: "Transaction file (FA) — OPI/OPO Mutual Funds",
  FA_MUTUAL_FUND_TDS: "TDS file — Mutual Funds",
  CUSTODY_RESIDENTIAL: "Custodian — Residential",
  CUSTODY_NDPMS: "Custodian — NRE/NRO/NDPMS",
  CUSTODY_MUTUAL_FUND: "Custodian — Mutual Funds",
  CUSTODY_HOLDINGS: "Custodian — Consolidated Holdings",
  CUSTODY_CORP_ACTION: "Custodian — Corporate Actions",
  CUSTODY_CORP_ALLOCATION: "Custodian — Corporate Benefit Collection",
};

export const PROFILE_HELP = {
  FA: "The daily transaction file. Whatever layout you map here is rewritten into the standard 26-column format before it is imported, so the import is unaffected.",
  FA_MUTUAL_FUND:
    "The mutual fund fund-accounting file. Same 26 fields as the regular transaction file — it renames one heading (SYMBOLCODE/ISIN CODE), which is why it is mapped separately. Its rows are matched only against the Mutual Funds custodian files. Dates may be in any common format (2026-08-20, 20-08-2026, 20-Aug-2026) and are stored as YYYY-MM-DD.",
  FA_OPI_OPO:
    "The OPI/OPO Equity file: shares moved in (OPI) or out (OPO) from another system. Same columns as the Equity transaction file, without a Transaction ID. Saved separately, so it never changes the Equity mapping.",
  FA_OPI_OPO_MF:
    "The OPI/OPO Mutual Funds file: units moved in (OPI) or out (OPO) from another system. The OPI/OPO Equity columns plus TDS and Stamp Duty. Saved separately, so it never changes any other mapping.",
  FA_MUTUAL_FUND_TDS:
    "The mutual fund TDS file (TDI / TDO rows per account). Broker acc id, Transaction type, Transaction date and AMT must be mapped. For now the file is only kept; it does not change cash or holdings.",
  CUSTODY_RESIDENTIAL: "The deal allocation file for Resident accounts.",
  CUSTODY_NDPMS: "The deal allocation file for NRE/NRO/NDPMS accounts.",
  CUSTODY_MUTUAL_FUND:
    "The Mutual Funds transaction file (UCC, SourceMFISIN, TradeDate, TransactionType Subscription / Redemption, Units). Matched against the rows from the mutual fund fund-accounting file.",
  CUSTODY_HOLDINGS:
    "The custodian's consolidated holdings file. Its column names differ from the standard 29-column template, so tell us which column holds each field.",
  CUSTODY_CORP_ACTION:
    "The custodian's corporate actions file. Tell us which column holds each field; every field is optional.",
  CUSTODY_CORP_ALLOCATION:
    "The custodian's Corporate Benefit Collection file. Tell us which column holds each field; every field is optional.",
};

/** Which transaction file is being uploaded. A day may hold one of each. */
export const FA_FILE_TYPE = {
  REGULAR: "REGULAR",
  MUTUAL_FUND: "MUTUAL_FUND",
  /* The OPI/OPO tab's files: Equity sub-tab (no TDS / Stamp Duty) and Mutual Funds sub-tab. */
  OPI_OPO: "OPI_OPO",
  OPI_OPO_MF: "OPI_OPO_MF",
};

export const FA_FILE_TYPE_LABEL = {
  REGULAR: "Equity",
  MUTUAL_FUND: "Mutual Funds",
  OPI_OPO: "Equity",
  OPI_OPO_MF: "Mutual Funds",
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

/*
 * Equity tab: each trade date has its own change window (T+N), chosen on the Fund
 * Accounting card until the date's first commit fixes it. The server works out
 * whether a date is open or locked (util/custodyMatch/equityReload.js) and sends
 * the text to show, so the screen holds no copy of the rule.
 */
export const WINDOW_PRESETS = [1, 2, 3, 150];
export const DEFAULT_WINDOW_DAYS = 150;
export const MAX_WINDOW_DAYS = 150;

/** { [date]: { days, fixed, lastDay, locked, message } } for the given trade dates on a tab. */
export const getDateWindows = async (dates, kind = KIND.EQUITY) => {
  const unique = [...new Set(dates.filter(Boolean))].sort();
  if (unique.length === 0) return {};
  const res = await request(withKind(`/reload/windows?dates=${encodeURIComponent(unique.join(","))}`, kind));
  return res.windows || {};
};

export const saveDateWindow = (date, days, kind = KIND.EQUITY) => json(`/reload/${date}/window`, "PUT", { days, kind });

export function formatBytes(bytes) {
  if (bytes == null || Number.isNaN(Number(bytes))) return "—";
  const n = Number(bytes);
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(2)} MB`;
}
