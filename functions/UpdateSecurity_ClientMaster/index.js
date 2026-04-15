"use strict";

const { Readable } = require("stream");
const csv = require("csv-parser");
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

/** Case-insensitive header lookup on csv-parser row objects. */
function getCell(row, ...candidates) {
  const keys = Object.keys(row);
  for (const name of candidates) {
    if (!name) continue;
    if (
      row[name] !== undefined &&
      String(row[name]).trim() !== ""
    ) {
      const s = String(row[name]).trim();
      if (s.toLowerCase() !== "null") return s;
    }
    const lower = name.toLowerCase();
    for (const k of keys) {
      if (
        k.toLowerCase() === lower &&
        String(row[k]).trim() !== ""
      ) {
        const s = String(row[k]).trim();
        if (s.toLowerCase() !== "null") return s;
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

function createCsvParser() {
  return csv({
    skipEmptyLines: true,
    mapHeaders: ({ header }) =>
      String(header ?? "").replace(/^\uFEFF/, "").trim(),
  });
}

/**
 * Stream CSV via csv-parser (handles quoted commas/newlines). Accumulate unique
 * clientIds + Security_List keys in Maps, then flush with ensure*.
 */
async function processCsvObjectForMasters(bucket, objectKey, zcql) {
  const clients = new Map();
  const securities = new Map();
  let dataRowCount = 0;

  const mergeRow = (row) => {
    /*
     * Upload columns (Stratus transaction CSV) → masters:
     * - clientIds: WS_client_id ← Broker Code | WS_client_id ; WS_Account_code ← BROKERACID
     * - Security_List: ISIN ← SYMBOLCODE ; Security_Code ← SYMBOLCODE (else CASHSYMBOLCODE) ;
     *   Security_Name ← Description | Security_Name
     */
    const wsClientId = getCell(
      row,
      "Broker Code",
      "broker code",
      "brokercode",
      "WS_client_id",
      "ws_client_id",
    );
    const wsAccountCode = getCell(
      row,
      "BROKERACID",
      "brokeracid",
      "WS_Account_code",
      "ws_account_code",
    );
    const isin = getCell(row, "SYMBOLCODE", "symbolcode", "ISIN", "isin");
    const securityCode = getCell(
      row,
      "SYMBOLCODE",
      "symbolcode",
      "CASHSYMBOLCODE",
      "cashsymbolcode",
      "Security_Code",
      "security_code",
    );
    const securityName = getCell(
      row,
      "Description",
      "description",
      "Security_Name",
      "security_name",
    );

    if (isValid(wsAccountCode)) {
      const lookupId = isValid(wsClientId) ? wsClientId : wsAccountCode;
      const cur = clients.get(wsAccountCode);
      if (!cur) {
        clients.set(wsAccountCode, {
          wsClientId: lookupId,
          wsAccountCode,
        });
      } else if (
        cur.wsClientId === wsAccountCode &&
        isValid(wsClientId) &&
        wsClientId !== wsAccountCode
      ) {
        cur.wsClientId = wsClientId;
      }
    }

    if (!isValid(securityCode) && !isValid(isin)) return;

    const secKey = isValid(isin) ? `i:${isin}` : `c:${securityCode}`;
    const prev = securities.get(secKey);
    if (!prev) {
      securities.set(secKey, {
        securityCode: isValid(securityCode) ? securityCode : "",
        isin: isValid(isin) ? isin : "",
        securityName: isValid(securityName) ? securityName : "",
      });
    } else {
      if (
        securityName &&
        (!prev.securityName ||
          securityName.length > (prev.securityName || "").length)
      ) {
        prev.securityName = securityName;
      }
      if (isValid(securityCode) && !isValid(prev.securityCode)) {
        prev.securityCode = securityCode;
      }
      if (isValid(isin) && !isValid(prev.isin)) prev.isin = isin;
    }
  };

  const raw = await bucket.getObject(objectKey);
  let inputStream;
  if (raw != null && typeof raw === "object" && typeof raw.pipe === "function") {
    inputStream = raw;
  } else if (Buffer.isBuffer(raw)) {
    inputStream = Readable.from(raw);
  } else {
    inputStream = Readable.from([rawToUtf8(raw)], { encoding: "utf8" });
  }

  await new Promise((resolve, reject) => {
    const parser = createCsvParser();
    inputStream
      .pipe(parser)
      .on("data", (row) => {
        dataRowCount++;
        mergeRow(row);
      })
      .on("end", resolve)
      .on("error", reject);
  });

  if (dataRowCount === 0) {
    console.warn(
      "[UpdateSecurity_ClientMaster] No data rows in CSV (or empty file):",
      objectKey,
    );
  }

  console.log(
    "[UpdateSecurity_ClientMaster] Parsed",
    objectKey,
    `dataRows=${dataRowCount} uniqueClients=${clients.size} uniqueSecurities=${securities.size}`,
  );

  for (const c of clients.values()) {
    await ensureClientIds(zcql, c.wsClientId, c.wsAccountCode);
  }
  for (const s of securities.values()) {
    await ensureSecurityList(zcql, s.securityCode, s.isin, s.securityName);
  }
}

async function ensureClientIds(zcql, wsClientId, wsAccountCode) {
  if (!isValid(wsAccountCode)) return;

  const lookupId = isValid(wsClientId) ? wsClientId : wsAccountCode;

  const existingClient = await zcql.executeZCQLQuery(`
    SELECT ROWID FROM clientIds
    WHERE WS_Account_code = '${esc(wsAccountCode)}'
  `);

  if (!existingClient.length) {
    try {
      await zcql.executeZCQLQuery(`
        INSERT INTO clientIds (WS_client_id, WS_Account_code)
        VALUES ('${esc(lookupId)}', '${esc(wsAccountCode)}')
      `);
    } catch (err) {
      if (String(err?.message || "").includes("Duplicate")) return;
      throw err;
    }
  }
}

async function ensureSecurityList(zcql, securityCode, isin, securityName) {
  if (!isValid(securityCode) && !isValid(isin)) return;

  let exists = false;

  if (isValid(isin)) {
    const byIsin = await zcql.executeZCQLQuery(`
      SELECT ROWID FROM Security_List
      WHERE ISIN = '${esc(isin)}'
    `);
    if (byIsin.length) exists = true;
  }

  if (!exists && isValid(securityCode)) {
    const byCode = await zcql.executeZCQLQuery(`
      SELECT ROWID FROM Security_List
      WHERE Security_Code = '${esc(securityCode)}'
    `);
    if (byCode.length) exists = true;
  }

  if (!exists) {
    const codeToInsert = isValid(securityCode) ? securityCode : (isin || "");
    try {
      await zcql.executeZCQLQuery(`
        INSERT INTO Security_List (Security_Code, ISIN, Security_Name)
        VALUES (
          '${esc(codeToInsert)}',
          '${esc(isin || "")}',
          '${esc(securityName || "")}'
        )
      `);
    } catch (err) {
      if (String(err?.message || "").includes("Duplicate")) return;
      throw err;
    }
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

        await processCsvObjectForMasters(stratusBucket, objectKey, zcql);
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
