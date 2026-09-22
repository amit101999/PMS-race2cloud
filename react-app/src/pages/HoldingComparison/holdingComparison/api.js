import { BASE_URL } from "../../../constant";
import { ApiError } from "../../TempTransactionUpload/custody/api";

export { ApiError };

const ROOT = `${BASE_URL}/holding-comparison`;

const NETWORK_MESSAGE = "Could not reach the server. Check that the backend is running.";

/** fetch + JSON, throwing ApiError with the server's structured body attached. */
async function request(path, options) {
  let res;
  try {
    res = await fetch(`${ROOT}${path}`, options);
  } catch (err) {
    throw new ApiError(NETWORK_MESSAGE, { detail: { code: "NETWORK", cause: String(err) } });
  }

  const text = await res.text();
  let body = {};
  if (text) {
    try {
      body = JSON.parse(text);
    } catch {
      throw new ApiError(`The server did not return JSON (${res.status}).`, { status: res.status });
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

const postJson = (path, body) =>
  request(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });

export const getDay = (date) => request(`/day/${date}`);

export const uploadCustodyFile = (file, businessDate) => {
  const form = new FormData();
  form.append("file", file);
  form.append("businessDate", businessDate);
  return request("/custody-file", { method: "POST", body: form });
};

export const deleteCustodyFile = (key) =>
  request(`/custody-file?key=${encodeURIComponent(key)}`, { method: "DELETE" });

export const generateFaHoldings = (businessDate) =>
  postJson("/fa-holdings/generate", { businessDate });

export const getFaStatus = (date) => request(`/fa-holdings/${date}/status`);

export const startRun = (businessDate, custodyKey) => postJson("/run", { businessDate, custodyKey });

export const getRun = (runId) => request(`/run/${runId}`);

export const getExceptions = (runId) => request(`/run/${runId}/exceptions`);

export const exportExceptions = (runId) => request(`/run/${runId}/export`);

export const getCustodyHistory = () => request("/custody-history");
export const getFaHistory = () => request("/fa-history");
export const getDifferenceReports = () => request("/reports");

export const getDownloadUrl = (key) => request(`/download?key=${encodeURIComponent(key)}`);

/** The Field Mapping profile type for the custodian holdings file. */
export const CUSTODY_PROFILE_TYPE = "CUSTODY_HOLDINGS";

export const EXCEPTION_LABEL = {
  QUANTITY_MISMATCH: "Quantity mismatch",
  MISSING_IN_CUSTODY: "Missing in custody",
  MISSING_IN_FA: "Missing in FA",
};

/** FA holdings generation states that are still moving. */
export const FA_IN_PROGRESS = ["PENDING", "RUNNING"];

export const FA_STATUS_LABEL = {
  NOT_STARTED: "Not generated yet",
  COMPLETED: "Generated",
  FAILED: "Failed",
};
