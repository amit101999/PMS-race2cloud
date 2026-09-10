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

export const uploadFaFile = ({ file, businessDate, replace = false }) =>
  form("/fa-file", { file, businessDate, replace: replace ? "true" : "" });

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

export const PROFILE_TYPE = {
  FA: "FA",
  CUSTODY_RESIDENTIAL: "CUSTODY_RESIDENTIAL",
  CUSTODY_NDPMS: "CUSTODY_NDPMS",
};

export const PROFILE_LABEL = {
  FA: "Transaction file (FA)",
  CUSTODY_RESIDENTIAL: "Custodian — Residential",
  CUSTODY_NDPMS: "Custodian — NRE/NRO (NDPMS)",
};

export const PROFILE_HELP = {
  FA: "The daily transaction file. Whatever layout you map here is rewritten into the standard 26-column format before it is imported, so the import is unaffected.",
  CUSTODY_RESIDENTIAL: "The deal allocation file for Resident accounts.",
  CUSTODY_NDPMS: "The deal allocation file for NRE / NRO (NDPMS) accounts.",
};

export const SOURCE_BY = { NAME: "name", INDEX: "index" };

export const TRANSFORM_LABEL = {
  NONE: "Use the value as-is",
  DATE_FROM_FIRST_8: "Read the date from the first 8 characters",
};

/* ── helpers ──────────────────────────────────────────────────────────── */

export const CUSTODY_FILE_TYPE = {
  RESIDENTIAL: "RESIDENTIAL",
  NDPMS: "NDPMS",
};

/** Both account types send one deal-allocation file; only the layout inside differs. */
export const FILE_TYPE_LABEL = {
  RESIDENTIAL: "Residential",
  NDPMS: "NRE / NRO (NDPMS)",
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
