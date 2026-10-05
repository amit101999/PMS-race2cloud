/**
 * The custodian files from the Custodian Upload page, copied into Datastore
 * tables so a module can find one event (ISIN + CA type + record date) in any
 * upload, not just the newest file.
 *
 *   CA_Custodian_Upload    one row per uploaded file: date, hash, import status
 *   CA_Ex_Entitlement      the Ex-Entitlement file's rows
 *   CA_Benefit_Collection  the Corporate Benefit Collection file's rows
 *
 * Every data row carries the Stratus key of the file it came from (Source_Key)
 * and that file's Custodian Upload date (Report_Date). Reports overlap, so one
 * event is always read from a single file: the one with the latest report date.
 *
 * Files are 25–30k rows, so they go in with a Catalyst bulk write job. The job
 * runs in the background; the upload row says whether it has finished.
 */

import { parseDate } from "../custodyMatch/normalize.js";
import { num, stripIsinSuffix } from "../custodian/parseCustodianCsv.js";

export const UPLOAD_TABLE = "CA_Custodian_Upload";
export const IMPORT_STATUS = { IMPORTING: "IMPORTING", DONE: "DONE", FAILED: "FAILED" };

/** Bucket holding the uploaded Entitlement and Allocation files. */
export const FILES_BUCKET = "custodian-ca-files";

/**
 * Where the CSVs handed to the bulk write job are kept, inside the files bucket.
 * Each one is only read by its job, so it is deleted once the import ends.
 */
const IMPORT_FOLDER = "CA-Import";
const importKeyOf = (kind, timeMs) => `${IMPORT_FOLDER}/${kind}/${timeMs}.csv`;

const PAGE = 300;

/*
 * Each table's columns: [column, mapping field, type, max length for text].
 * Types: text, upper (upper-cased text), isin, catype (lower-cased), date, number.
 */
const COMMON_HEAD = [
  ["ISIN", "isin", "isin", 20],
  ["Record_Date", "recordDate", "date"],
  ["Account_Code", "clientUccCode", "upper", 50],
  ["ISIN_Name", "isinName", "text", 255],
  ["CA_Ref_No", "caRefNo", "text", 100],
  ["CA_Qty", "caQty", "number"],
];

const KINDS = {
  entitlement: {
    table: "CA_Ex_Entitlement",
    label: "Ex-Entitlement file",
    columns: [
      ...COMMON_HEAD,
      ["CA_Type", "caTypeDescr", "catype", 50],
      ["Client_Name", "clientName", "text", 255],
      ["Ratio", "ratio", "text", 50],
      ["Holding_Qty", "holdingQty", "number"],
      ["Ex_Date", "exDate", "date"],
      ["Payment_Date", "tentativePaymentDate", "date"],
      ["Dividend_Rate", "dividendRate", "number"],
      ["CA_Amt", "caAmt", "number"],
    ],
    // Without these no comparison can use a row (see CORP_ACTION_REQUIRED_FIELDS in fieldMap.js).
    required: ["clientUccCode", "isin", "caTypeDescr", "recordDate", "ratio", "holdingQty"],
  },
  allocation: {
    table: "CA_Benefit_Collection",
    label: "Corporate Benefit Collection file",
    columns: [
      ...COMMON_HEAD,
      ["CA_Type", "caTypeDescription", "catype", 50],
      ["Received_Qty", "receivedQty", "number"],
      ["Balance_Qty", "balanceQty", "number"],
      ["Received_Date", "receivedDate", "date"],
      ["Received_Amount", "receivedAmount", "number"],
      ["Event_Status", "eventStatus", "text", 100],
    ],
    // The Split stage-2 comparison cannot work without these.
    required: ["clientUccCode", "isin", "caTypeDescription", "recordDate", "receivedQty"],
  },
};

export const fileLabel = (kind) => KINDS[kind].label;

export const escSql = (value) => String(value ?? "").replace(/'/g, "''");

/** ZCQL nests each row under its table name. */
const unwrap = (rows, table) => (rows || []).map((row) => row[table] || row);

/** Datastore returns an empty number as null or the text "null". */
export const numberOrNull = (value) => {
  if (value === null || value === undefined || value === "" || value === "null") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};

const dateOf = (value) => (value && value !== "null" ? String(value).slice(0, 10) : "");

const csvCell = (value) => {
  const s = String(value ?? "");
  return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
};

const cellValue = (raw, type, maxLength) => {
  const text = String(raw ?? "").trim();
  if (text === "") return "";
  switch (type) {
    case "date":
      return parseDate(text) || "";
    case "number":
      return num(text);
    case "isin":
      return stripIsinSuffix(text).toUpperCase().slice(0, maxLength);
    case "upper":
      return text.toUpperCase().slice(0, maxLength);
    case "catype":
      return text.toLowerCase().slice(0, maxLength);
    default:
      return text.slice(0, maxLength);
  }
};

/**
 * Where each mapped field sits in this file, according to the saved mapping.
 * @returns {Record<string, number>} field → column index (unmapped fields left out)
 */
export const columnIndexes = (profile, normHeaders, normHeader) => {
  const indexOf = {};
  for (const [field, source] of Object.entries(profile || {})) {
    let index = -1;
    if (source?.by === "name") index = normHeaders.indexOf(normHeader(source.name));
    if (source?.by === "index") index = Number(source.index);
    if (index >= 0 && index < normHeaders.length) indexOf[field] = index;
  }
  return indexOf;
};

/** The required fields this file's mapping does not cover. */
export const missingRequiredFields = (kind, indexOf) => KINDS[kind].required.filter((f) => indexOf[f] === undefined);

/**
 * The CSV the bulk write job imports: the table's column names, then one line
 * per non-empty data row of the file.
 */
export const buildImportCsv = (kind, dataRows, indexOf, { sourceKey, reportDate, uploadedMs }) => {
  const { columns } = KINDS[kind];
  const lines = [["Source_Key", "Report_Date", "Uploaded_Ms", ...columns.map(([column]) => column)].join(",")];
  let rowCount = 0;
  for (const cells of dataRows) {
    if (!cells.some((c) => c !== "")) continue;
    const values = columns.map(([, field, type, maxLength]) =>
      indexOf[field] === undefined ? "" : cellValue(cells[indexOf[field]], type, maxLength),
    );
    lines.push([sourceKey, reportDate, uploadedMs, ...values].map(csvCell).join(","));
    rowCount += 1;
  }
  return { csv: `${lines.join("\n")}\n`, rowCount };
};

/* ------------------------------ upload rows ------------------------------ */

const toUpload = (row) => ({
  rowId: row.ROWID,
  key: row.Source_Key,
  kind: row.Kind,
  businessDate: dateOf(row.Report_Date),
  timeMs: Number(row.Uploaded_Ms),
  fileName: row.File_Name || "",
  fileHash: row.File_Hash || "",
  rowCount: Number(row.Row_Count) || 0,
  jobId: row.Job_Id || "",
  importStatus: row.Import_Status || "",
  importError: row.Import_Error || "",
});

const selectUploads = async (zcql, where) =>
  unwrap(await zcql.executeZCQLQuery(`SELECT * FROM ${UPLOAD_TABLE} WHERE ${where}`), UPLOAD_TABLE).map(toUpload);

export const findUpload = async (zcql, key) =>
  (await selectUploads(zcql, `Source_Key = '${escSql(key)}'`))[0] || null;

/** An earlier upload of exactly the same file of this kind (a failed import does not count). */
export const findDuplicate = async (zcql, kind, fileHash) =>
  (
    await selectUploads(
      zcql,
      `Kind = '${escSql(kind)}' AND File_Hash = '${escSql(fileHash)}' AND Import_Status != '${IMPORT_STATUS.FAILED}'`,
    )
  )[0] || null;

/** Uploads not on this list (stored before the tables existed) are imported by importMissing. */
export const listUploadKeys = async (zcql) => {
  const keys = new Set();
  let lastRowId = "0";
  for (;;) {
    const page = unwrap(
      await zcql.executeZCQLQuery(
        `SELECT ROWID, Source_Key FROM ${UPLOAD_TABLE} WHERE ROWID > ${lastRowId} ORDER BY ROWID LIMIT ${PAGE}`,
      ),
      UPLOAD_TABLE,
    );
    for (const row of page) keys.add(row.Source_Key);
    if (page.length < PAGE) return keys;
    lastRowId = page[page.length - 1].ROWID;
  }
};

const setImportResult = async (zcql, key, status, error = "") => {
  await zcql.executeZCQLQuery(`
    UPDATE ${UPLOAD_TABLE}
    SET Import_Status = '${status}', Import_Error = '${escSql(String(error).slice(0, 2000))}'
    WHERE Source_Key = '${escSql(key)}'
  `);
};

/* -------------------------------- import -------------------------------- */

/**
 * Delete an upload's import CSV once its import has ended (done or failed).
 * The outcome is already recorded, so a failed delete is only logged; a file
 * that is already gone (two status checks finishing together) is fine.
 */
const removeImportFile = async (catalystApp, kind, timeMs) => {
  const importKey = importKeyOf(kind, timeMs);
  try {
    const bucket = catalystApp.stratus().bucket(FILES_BUCKET);
    if (await bucket.headObject(importKey, { throwErr: false })) await bucket.deleteObject(importKey);
  } catch (error) {
    console.error(`[CustodianImport] Could not delete ${importKey}:`, error.message);
  }
};

/**
 * Record the upload, store the import CSV next to the file and start the bulk
 * write job. Returns the upload as stored; the job finishes in the background.
 */
export const startImport = async (catalystApp, { kind, key, businessDate, timeMs, fileName, fileHash, csv, rowCount }) => {
  const zcql = catalystApp.zcql();
  await catalystApp.datastore().table(UPLOAD_TABLE).insertRow({
    Source_Key: key,
    Kind: kind,
    Report_Date: businessDate,
    Uploaded_Ms: timeMs,
    File_Name: String(fileName).slice(0, 255),
    File_Hash: fileHash,
    Row_Count: rowCount,
    Import_Status: IMPORT_STATUS.IMPORTING,
  });

  try {
    const importKey = importKeyOf(kind, timeMs);
    await catalystApp.stratus().bucket(FILES_BUCKET).putObject(importKey, csv, { contentType: "text/csv", overwrite: true });
    const job = await catalystApp
      .datastore()
      .table(KINDS[kind].table)
      .bulkJob("write")
      .createJob({ bucket_name: FILES_BUCKET, object_key: importKey }, { operation: "insert" });
    const jobId = String(job?.job_id ?? "");
    if (!jobId) throw new Error(`The bulk write job did not start: ${JSON.stringify(job)}`);
    await zcql.executeZCQLQuery(
      `UPDATE ${UPLOAD_TABLE} SET Job_Id = '${escSql(jobId)}' WHERE Source_Key = '${escSql(key)}'`,
    );
    void watchImport(catalystApp, key);
  } catch (error) {
    console.error(`[CustodianImport] ${key} could not start:`, error.message);
    await setImportResult(zcql, key, IMPORT_STATUS.FAILED, error.message);
    await removeImportFile(catalystApp, kind, timeMs);
  }

  return findUpload(zcql, key);
};

const countRows = async (zcql, kind, key) => {
  const { table } = KINDS[kind];
  const rows = await zcql.executeZCQLQuery(`SELECT COUNT(ROWID) FROM ${table} WHERE Source_Key = '${escSql(key)}'`);
  const first = unwrap(rows, table)[0] || {};
  return Number(Object.values(first)[0]) || 0;
};

/**
 * Check a running import once and record the outcome. Safe to call from
 * several places at once: it only ever sets the final status.
 *
 * An import counts as done only when every row of the file is in the table,
 * so a job that skipped rows is reported as failed instead of giving a
 * comparison with clients missing.
 *
 * @returns {Promise<object|null>} the upload, with its status after the check
 */
export const settleImport = async (catalystApp, key) => {
  const zcql = catalystApp.zcql();
  const upload = await findUpload(zcql, key);
  if (!upload || upload.importStatus !== IMPORT_STATUS.IMPORTING || !upload.jobId) return upload;

  const result = await catalystApp.datastore().table(KINDS[upload.kind].table).bulkJob("write").getStatus(upload.jobId);
  const status = String(result?.status ?? result?.data?.status ?? "").toUpperCase().replace(/-/g, "_");
  if (status === "FAILED") {
    await setImportResult(zcql, key, IMPORT_STATUS.FAILED, `Bulk write failed: ${JSON.stringify(result?.results ?? result)}`);
  } else if (["COMPLETED", "SUCCESS", "SUCCESSFUL"].includes(status)) {
    const imported = await countRows(zcql, upload.kind, key);
    if (imported === upload.rowCount) {
      await setImportResult(zcql, key, IMPORT_STATUS.DONE);
    } else {
      await setImportResult(
        zcql,
        key,
        IMPORT_STATUS.FAILED,
        `Imported ${imported} of ${upload.rowCount} rows. Remove the file and upload it again.`,
      );
    }
  }
  const settled = await findUpload(zcql, key);
  if (settled && settled.importStatus !== IMPORT_STATUS.IMPORTING) {
    await removeImportFile(catalystApp, settled.kind, settled.timeMs);
  }
  return settled;
};

const WATCH_FIRST_MS = 10_000;
const WATCH_EVERY_MS = 15_000;
const WATCH_MAX_MS = 30 * 60 * 1000;

/** Keep checking an import until it finishes (the page and the readers check too, in case the server restarts). */
const watchImport = async (catalystApp, key) => {
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const start = Date.now();
  await sleep(WATCH_FIRST_MS);
  while (Date.now() - start < WATCH_MAX_MS) {
    try {
      const upload = await settleImport(catalystApp, key);
      if (!upload || upload.importStatus !== IMPORT_STATUS.IMPORTING) return;
    } catch (error) {
      console.error(`[CustodianImport] ${key} status check failed:`, error.message);
    }
    await sleep(WATCH_EVERY_MS);
  }
};

/** Forget an upload: once its row is gone no reader can pick its data any more. */
export const deleteUploadRow = async (zcql, key) => {
  await zcql.executeZCQLQuery(`DELETE FROM ${UPLOAD_TABLE} WHERE Source_Key = '${escSql(key)}'`);
};

/**
 * Delete a forgotten upload's data rows (up to 30k, so callers run it in the
 * background). ZCQL may delete in batches, so it repeats until none are left.
 */
export const deleteDataRows = async (zcql, kind, key) => {
  const { table } = KINDS[kind];
  for (let i = 0; i < 1000; i += 1) {
    const result = unwrap(
      await zcql.executeZCQLQuery(`DELETE FROM ${table} WHERE Source_Key = '${escSql(key)}'`),
      table,
    )[0];
    if (!(Number(result?.DELETED_ROWS_COUNT) > 0)) return;
  }
};

/* -------------------------------- reading -------------------------------- */

/** The upload that stands for a report date: the newest one uploaded for it. */
const activeUploadFor = async (catalystApp, kind, reportDate) => {
  const zcql = catalystApp.zcql();
  const [upload] = await selectUploads(
    zcql,
    `Kind = '${escSql(kind)}' AND Report_Date = '${reportDate}' ORDER BY Uploaded_Ms DESC LIMIT 1`,
  );
  if (upload?.importStatus === IMPORT_STATUS.IMPORTING) return settleImport(catalystApp, upload.key);
  return upload || null;
};

/**
 * One event's rows (ISIN + CA type + record date) from the uploaded file with
 * the latest report date that has it.
 *
 * A report date counts only through its newest upload (uploading again for a
 * date replaces the earlier file, as on the page), and only once that upload's
 * import has finished.
 *
 * @returns {Promise<{ upload: object, rows: object[] } | null>} raw table rows
 */
export const readEventRows = async (catalystApp, kind, { isin, caTypes, recordDateISO }) => {
  const zcql = catalystApp.zcql();
  const { table } = KINDS[kind];
  const eventWhere =
    `ISIN = '${escSql(stripIsinSuffix(isin).toUpperCase())}' AND Record_Date = '${escSql(recordDateISO)}' ` +
    `AND CA_Type IN (${caTypes.map((t) => `'${escSql(t)}'`).join(", ")})`;

  // Each upload holding the event, latest report first.
  const holders = unwrap(
    await zcql.executeZCQLQuery(`
      SELECT Source_Key, Report_Date, Uploaded_Ms FROM ${table}
      WHERE ${eventWhere}
      GROUP BY Source_Key, Report_Date, Uploaded_Ms
      ORDER BY Report_Date DESC, Uploaded_Ms DESC
      LIMIT ${PAGE}
    `),
    table,
  );
  const holderKeys = new Set(holders.map((h) => h.Source_Key));
  const reportDates = [...new Set(holders.map((h) => dateOf(h.Report_Date)))];

  for (const reportDate of reportDates) {
    const active = await activeUploadFor(catalystApp, kind, reportDate);
    if (!active || active.importStatus !== IMPORT_STATUS.DONE || !holderKeys.has(active.key)) continue;

    const rows = [];
    let lastRowId = "0";
    for (;;) {
      const page = unwrap(
        await zcql.executeZCQLQuery(`
          SELECT * FROM ${table}
          WHERE Source_Key = '${escSql(active.key)}' AND ${eventWhere} AND ROWID > ${lastRowId}
          ORDER BY ROWID LIMIT ${PAGE}
        `),
        table,
      );
      rows.push(...page);
      if (page.length < PAGE) break;
      lastRowId = page[page.length - 1].ROWID;
    }
    return { upload: active, rows };
  }
  return null;
};

/** Whether a file of this kind is still being imported, to explain a missing event. */
export const isImporting = async (zcql, kind) =>
  (
    await selectUploads(
      zcql,
      `Kind = '${escSql(kind)}' AND Import_Status = '${IMPORT_STATUS.IMPORTING}' LIMIT 1`,
    )
  ).length > 0;

export { dateOf };
