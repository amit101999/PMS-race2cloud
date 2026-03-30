"use strict";

const catalyst = require("zcatalyst-sdk-node");

const esc = (v) => String(v ?? "").replace(/'/g, "''");

const isValid = (v) =>
  v != null && v !== "" && String(v).toLowerCase() !== "null";

/** Only process transaction CSV uploads from these Stratus key prefixes. */
const ALLOWED_CSV_PREFIXES = [
  "temp-files/temp-transactions/",
  "temp-files/transactions/",
];

function isAllowedObjectKey(key) {
  if (!key || typeof key !== "string") return false;
  const lower = key.toLowerCase();
  if (!lower.endsWith(".csv")) return false;
  return ALLOWED_CSV_PREFIXES.some((p) => lower.startsWith(p.toLowerCase()));
}

function parseRawEvent(event) {
  let raw;
  if (event != null && typeof event.getRawData === "function") {
    raw = event.getRawData();
  }
  if (raw === undefined || raw === null) {
    raw = event?.data ?? event?.payload ?? event;
  }

  if (raw && typeof raw === "object" && raw === event) {
    const hasStratusShape =
      raw.bucket_details != null ||
      raw.bucket_name != null ||
      raw.object_details != null;
    if (!hasStratusShape && raw.body != null) {
      raw = raw.body;
    }
  }

  if (Buffer.isBuffer(raw)) {
    raw = raw.toString("utf8");
  }
  if (typeof raw === "string") {
    try {
      raw = JSON.parse(raw);
    } catch {
      return null;
    }
  }
  return raw && typeof raw === "object" ? raw : null;
}

/** Collect nested objects Catalyst may wrap the Stratus payload in. */
function payloadRoots(raw) {
  const out = [];
  const seen = new Set();
  const push = (o) => {
    if (!o || typeof o !== "object" || seen.has(o)) return;
    seen.add(o);
    out.push(o);
  };
  push(raw);
  const unwrapKeys = [
    "data",
    "payload",
    "body",
    "event",
    "message",
    "context",
    "stratus",
    "details",
    "record",
    "records",
  ];
  const firstPass = [...out];
  for (const r of firstPass) {
    for (const k of unwrapKeys) {
      const v = r[k];
      if (v && typeof v === "object" && !Array.isArray(v)) {
        push(v);
      }
    }
    if (Array.isArray(r.records)) {
      for (const rec of r.records) {
        if (rec && typeof rec === "object") push(rec);
      }
    }
    if (Array.isArray(r.events)) {
      for (const ev of r.events) {
        if (ev && typeof ev === "object") push(ev);
        if (ev?.data && typeof ev.data === "object") push(ev.data);
      }
    }
  }
  return out;
}

function objectKeyFromEntry(o) {
  if (o == null) return null;
  if (typeof o === "string") return o;
  if (typeof o !== "object") return null;
  const key =
    o.key ??
    o.Key ??
    o.object_key ??
    o.objectKey ??
    o.name ??
    o.Name;
  if (key != null && String(key).trim() !== "") return String(key);
  if (o.object && typeof o.object === "object") {
    return objectKeyFromEntry(o.object);
  }
  if (o.object_details?.[0]) {
    return objectKeyFromEntry(o.object_details[0]);
  }
  return null;
}

function normalizeObjectList(list) {
  if (list == null) return [];
  if (Array.isArray(list)) return list;
  if (typeof list === "object") return [list];
  return [];
}

/**
 * Stratus object-created payload: bucket_details.bucket_name + object_details[].key
 * (plus Catalyst wrappers / alternate property names.)
 */
function extractBucketAndKeys(raw) {
  const seeds = Array.isArray(raw)
    ? raw.filter((x) => x && typeof x === "object")
    : [raw];

  for (const seed of seeds) {
    if (!seed || typeof seed !== "object") continue;
    for (const r of payloadRoots(seed)) {
    const bucket =
      r.bucket_details?.bucket_name ??
      r.bucket_details?.bucketName ??
      r.bucket_name ??
      r.bucketName ??
      null;

    const list = normalizeObjectList(
      r.object_details ??
        r.objectDetails ??
        r.objects ??
        r.object_list ??
        r.objectList,
    );

    const keys = [];
    for (const o of list) {
      const k = objectKeyFromEntry(o);
      if (k) keys.push(k);
    }

    if (bucket && keys.length) {
      return [{ bucket: String(bucket), keys }];
    }
    }
  }

  return [];
}

function summarizePayloadForLog(raw) {
  try {
    if (!raw || typeof raw !== "object") {
      return String(raw);
    }
    const s = JSON.stringify(raw);
    return s.length > 2000 ? `${s.slice(0, 2000)}…` : s;
  } catch {
    return "[unserializable]";
  }
}

function parseCsvRow(line) {
  const out = [];
  let cur = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (inQuotes) {
      if (c === '"') {
        if (line[i + 1] === '"') {
          cur += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        cur += c;
      }
    } else if (c === '"') {
      inQuotes = true;
    } else if (c === ",") {
      out.push(cur);
      cur = "";
    } else {
      cur += c;
    }
  }
  out.push(cur);
  return out;
}

function csvTextToRows(text) {
  const lines = text.split(/\r?\n/);
  const nonEmpty = [];
  for (const line of lines) {
    if (line.length > 0) nonEmpty.push(line);
  }
  if (nonEmpty.length < 2) return [];
  const headers = parseCsvRow(nonEmpty[0]).map((h) => h.trim());
  const rows = [];
  for (let i = 1; i < nonEmpty.length; i++) {
    const cells = parseCsvRow(nonEmpty[i]);
    const row = {};
    for (let j = 0; j < headers.length; j++) {
      const h = headers[j];
      row[h] =
        cells[j] !== undefined ? String(cells[j]).trim() : "";
    }
    rows.push(row);
  }
  return rows;
}

function getCell(row, ...candidates) {
  const keys = Object.keys(row);
  for (const name of candidates) {
    if (!name) continue;
    if (
      row[name] !== undefined &&
      String(row[name]).trim() !== ""
    ) {
      return String(row[name]).trim();
    }
    const lower = name.toLowerCase();
    for (const k of keys) {
      if (
        k.toLowerCase() === lower &&
        String(row[k]).trim() !== ""
      ) {
        return String(row[k]).trim();
      }
    }
  }
  return "";
}

function rawToUtf8(raw) {
  if (raw == null) return "";
  if (Buffer.isBuffer(raw)) return raw.toString("utf8");
  if (raw instanceof ArrayBuffer) {
    return Buffer.from(raw).toString("utf8");
  }
  if (typeof raw === "string") return raw;
  return String(raw);
}

function streamToString(stream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (c) => chunks.push(c));
    stream.on("end", () =>
      resolve(Buffer.concat(chunks).toString("utf8")),
    );
    stream.on("error", reject);
  });
}

async function readObjectAsUtf8(bucket, objectKey) {
  const raw = await bucket.getObject(objectKey);
  if (raw != null && typeof raw === "object" && typeof raw.pipe === "function") {
    return streamToString(raw);
  }
  return rawToUtf8(raw);
}

async function ensureClientIds(zcql, wsClientId, wsAccountCode) {
  if (!isValid(wsClientId) || !isValid(wsAccountCode)) return;

  const existingClient = await zcql.executeZCQLQuery(`
    SELECT ROWID FROM clientIds
    WHERE WS_client_id = '${esc(wsClientId)}'
  `);

  if (!existingClient.length) {
    await zcql.executeZCQLQuery(`
      INSERT INTO clientIds (WS_client_id, WS_Account_code)
      VALUES ('${esc(wsClientId)}', '${esc(wsAccountCode)}')
    `);
  }
}

async function ensureSecurityList(zcql, securityCode, isin, securityName) {
  if (!isValid(securityCode) && !isValid(isin)) return;

  const existingSecurity = await zcql.executeZCQLQuery(`
    SELECT ROWID FROM Security_List
    WHERE Security_Code = '${esc(securityCode || "")}'
  `);

  if (!existingSecurity.length) {
    await zcql.executeZCQLQuery(`
      INSERT INTO Security_List (Security_Code, ISIN, Security_Name)
      VALUES (
        '${esc(securityCode || "")}',
        '${esc(isin || "")}',
        '${esc(securityName || "")}'
      )
    `);
  }
}

async function processCsvRows(zcql, rows) {
  for (const row of rows) {
    const wsClientId = getCell(row, "WS_client_id", "ws_client_id");
    const wsAccountCode = getCell(row, "WS_Account_code", "ws_account_code");
    const securityCode = getCell(
      row,
      "Security_code",
      "Security_Code",
      "security_code",
    );
    const isin = getCell(row, "ISIN", "isin");
    const securityName = getCell(
      row,
      "Security_Name",
      "security_name",
    );

    await ensureClientIds(zcql, wsClientId, wsAccountCode);
    await ensureSecurityList(zcql, securityCode, isin, securityName);
  }
}

module.exports = async (event, context) => {
  try {
    const raw = parseRawEvent(event);
    if (!raw) {
      console.warn(
        "[UpdateSecurity_ClientMaster] Missing or invalid event payload",
      );
      context.closeWithSuccess();
      return;
    }

    const jobs = extractBucketAndKeys(raw);
    if (!jobs.length) {
      console.warn(
        "[UpdateSecurity_ClientMaster] No bucket/object_details in payload. Raw:",
        summarizePayloadForLog(raw),
      );
      context.closeWithSuccess();
      return;
    }

    const catalystApp = catalyst.initialize(context);
    const zcql = catalystApp.zcql();

    for (const { bucket, keys } of jobs) {
      const stratusBucket = catalystApp.stratus().bucket(bucket);
      for (const objectKey of keys) {
        if (!isAllowedObjectKey(objectKey)) {
          console.log(
            "[UpdateSecurity_ClientMaster] Skip object (not allowed path):",
            objectKey,
          );
          continue;
        }

        console.log(
          "[UpdateSecurity_ClientMaster] Processing",
          bucket,
          objectKey,
        );

        const text = await readObjectAsUtf8(stratusBucket, objectKey);
        const rows = csvTextToRows(text);
        if (!rows.length) {
          console.warn(
            "[UpdateSecurity_ClientMaster] No data rows in CSV:",
            objectKey,
          );
          continue;
        }

        await processCsvRows(zcql, rows);
      }
    }

    context.closeWithSuccess();

  } catch (err) {
    console.error("UpdateSecurity_ClientMaster error:", err);
    context.closeWithFailure();
  }
};

/*
 * =============================================================================
 * COMMENTED OUT — Purana flow: RAW_DATA.events[].data + executionPriority.
 * Ab Stratus object event se CSV read karke clientIds / Security_List update.
 * Zarurat ho to neeche map + Transaction UPDATE wapas use karo (e.g. CSV Tran_Type).
 * =============================================================================
 *
 * // Tran_Type -> executionPriority
 * const EXECUTION_PRIORITY_MAP = {
 *   "CS-": 0, "NF-": 0,
 *   "CS+": 1, "CSI": 1, "CUS": 1, "MGE": 1, "MGF": 1, "SQB": 1, "SQS": 1, "PRF": 1,
 *   "BY-": 2, "IN+": 2, "IN1": 2, "TDI": 2, "DI1": 2, "OI1": 2, "OPI": 2, "OPO": 2,
 *   "E01": 2, "E10": 2, "E22": 2, "E23": 2,
 *   "SL+": 3, "TDO": 3, "DIO": 3, "RD0": 3,
 * };
 *
 * const getExecutionPriority = (tranType) => {
 *   if (!isValid(tranType)) return null;
 *   const key = String(tranType).trim().toUpperCase();
 *   return EXECUTION_PRIORITY_MAP[key] ?? null;
 * };
 *
 * // --- events-based client/security + priority (purana) ---
 * // const RAW_DATA = event.getRawData();
 * // const events = RAW_DATA?.events || [];
 * // const processedTranTypes = new Set();
 * //
 * // for (const ev of events) {
 * //   const d = ev?.data || {};
 * //   const wsClientId = d.WS_client_id ?? d.ws_client_id;
 * //   const wsAccountCode = d.WS_Account_code ?? d.ws_account_code;
 * //   const securityCode = d.Security_code ?? d.Security_Code;
 * //   const isin = d.ISIN ?? d.isin;
 * //   const securityName = d.Security_Name ?? d.security_name;
 * //   const tranType = d.Tran_Type ?? d.tran_type;
 * //
 * //   if (isValid(wsClientId) && isValid(wsAccountCode)) { ... INSERT clientIds ... }
 * //   if (isValid(securityCode) || isValid(isin)) { ... INSERT Security_List ... }
 * //
 * //   if (isValid(tranType)) {
 * //     const typeKey = String(tranType).trim().toUpperCase();
 * //     const priority = getExecutionPriority(typeKey);
 * //     if (priority !== null && !processedTranTypes.has(typeKey)) {
 * //       let hasMore = true;
 * //       while (hasMore) {
 * //         const rows = await zcql.executeZCQLQuery(`
 * //           SELECT ROWID FROM Transaction
 * //           WHERE Tran_Type = '${esc(typeKey)}'
 * //           AND executionPriority IS NULL
 * //           LIMIT 300
 * //         `);
 * //         if (!rows || rows.length === 0) { hasMore = false; break; }
 * //         for (const row of rows) {
 * //           const rowId = row.Transaction.ROWID;
 * //           await zcql.executeZCQLQuery(`
 * //             UPDATE Transaction
 * //             SET executionPriority = ${priority}
 * //             WHERE ROWID = '${rowId}'
 * //           `);
 * //         }
 * //       }
 * //       processedTranTypes.add(typeKey);
 * //     }
 * //   }
 * // }
 *
 * =============================================================================
 */
