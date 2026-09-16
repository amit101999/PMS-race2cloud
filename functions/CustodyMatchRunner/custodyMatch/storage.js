/**
 * Custody Matching — Stratus storage.
 *
 * Bucket: temporary-files (pre-existing). Two folders, business date as the
 * first segment inside each:
 *
 *   FA-Transaction/<date>/raw-<ms>-<name>.csv          as uploaded, untouched
 *   FA-Transaction/<date>/final-<date>-FA.csv          consolidated onto actual codes
 *   FA-Transaction/<date>/difference-<date>-<runId>.csv
 *   Custodian-Transaction/<date>/<SCHEME>-<TYPE>-<ms>.csv
 *   Custodian-Transaction/<date>/final-<date>-Custodian.csv
 *
 * The date segment is what makes "delete every custodian file for this date" a
 * prefix-scoped list instead of a scan of every object ever uploaded — the cost
 * of the latter grows every day the system runs.
 *
 * Keys encode everything needed to describe a file (date, scheme, type, upload
 * time), so listing a day needs no tracking table. Same idea as the
 * HoldingCmp-<ms>-<name> keys in controller/comparison/HoldingComparisonUpload.js.
 *
 * Takes catalystApp as a parameter rather than initialising it, so the same file
 * works in AppSail and inside the job function.
 */

import { CUSTODY_FILE_TYPE } from "./constants.js";
import { PROFILE_TYPES, defaultProfile, fromStoredProfile } from "./fieldMap.js";

export const BUCKET_NAME = "temporary-files";

export const FOLDER = {
  FA: "FA-Transaction",
  CUSTODY: "Custodian-Transaction",
  /*
   * Field mappings are CONFIG, not a day's data, so they sit outside the
   * date-prefixed folders the cleanup cron sweeps. Deleting a mapping because it
   * is older than the retention window would break every future upload.
   */
  FIELD_MAPS: "field-maps",
  /*
   * Copies of the most recent custodian uploads, kept so they can be downloaded
   * again. Outside the date folders for the same reason as the mappings:
   * replacing or removing a file, clearing a day and the cleanup cron all work
   * on date folders, and none of them should erase the record of an upload.
   */
  CUSTODY_HISTORY: "Custodian-History",
};

/** How many custodian uploads Previous Uploads keeps. Older copies are deleted. */
export const CUSTODY_HISTORY_LIMIT = 5;

export const SIDE_KEY = {
  FA: "fa",
  CUSTODY: "custody",
};

/** Account types a custodian file may be uploaded as. One file per scheme. */
export const CUSTODY_FILE_TYPES = [
  CUSTODY_FILE_TYPE.RESIDENTIAL,
  CUSTODY_FILE_TYPE.NDPMS,
];

const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

/** Object keys are parsed by splitting on "-", so tokens must not contain one. */
const sanitiseToken = (v) =>
  String(v || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9_]+/g, "_")
    .replace(/^_+|_+$/g, "");

/** Keep a user's filename recognisable in the key without letting it break the path. */
const sanitiseFileName = (v) => {
  const base = String(v || "file.csv")
    .trim()
    .replace(/\\/g, "/")
    .split("/")
    .pop();
  return base.replace(/[^A-Za-z0-9._ ()-]+/g, "_").slice(0, 120) || "file.csv";
};

export const assertBusinessDate = (businessDate) => {
  const d = String(businessDate || "").trim();
  if (!DATE_PATTERN.test(d)) {
    throw new Error(
      `businessDate must be yyyy-mm-dd; received "${businessDate}".`,
    );
  }
  return d;
};

// ---------------------------------------------------------------- key builders

export const faDayPrefix = (businessDate) =>
  `${FOLDER.FA}/${assertBusinessDate(businessDate)}/`;

export const custodyDayPrefix = (businessDate) =>
  `${FOLDER.CUSTODY}/${assertBusinessDate(businessDate)}/`;

export const buildFaRawKey = ({
  businessDate,
  originalFileName,
  uploadedAtMs = Date.now(),
}) =>
  `${faDayPrefix(businessDate)}raw-${uploadedAtMs}-${sanitiseFileName(originalFileName)}`;

export const buildFaFinalKey = (businessDate) =>
  `${faDayPrefix(businessDate)}final-${assertBusinessDate(businessDate)}-FA.csv`;

export const buildCustodyFinalKey = (businessDate) =>
  `${custodyDayPrefix(businessDate)}final-${assertBusinessDate(businessDate)}-Custodian.csv`;

/**
 * What stage S1 worked out, persisted beside the FA final.
 *
 * The consolidated CSV keeps the 26-column contract, so it has nowhere to record
 * two things S1 knows and later stages need:
 *
 *   unmapped     — codes with no clientIds entry. A row with no actual code has
 *                  no place in the consolidated file at all, so without this a
 *                  run resuming at S2+ would silently drop every UNMAPPED_CODE.
 *   actualToVirtual — which virtual codes rolled into each actual code. S4 reads
 *                  the consolidated file back, where every row is already on its
 *                  actual code, so the virtual codes are gone by then. They are
 *                  what the user needs to find the offending row in their OWN
 *                  file, which is written in virtual codes.
 */
export const buildFaMetaKey = (businessDate) =>
  `${faDayPrefix(businessDate)}fa-meta-${assertBusinessDate(businessDate)}.json`;

/**
 * The dates a run was told to compare, written when the run is created.
 *
 * Kept beside the run rather than on it: Custody_Match_Run has no column for a
 * list, and a sidecar keyed by runId survives a retry — which job params alone
 * would not, since a retry builds a fresh submission.
 *
 * Absent means "every date in the files", which is what a run with no selection
 * has always done.
 */
export const buildRunDatesKey = (businessDate, runId) =>
  `${faDayPrefix(businessDate)}run-${sanitiseToken(runId)}-dates.json`;

/** Keyed by run id, not just date: a re-run after a fix must not overwrite the evidence of what was wrong. */
export const buildDifferenceKey = (businessDate, runId) =>
  `${faDayPrefix(businessDate)}difference-${assertBusinessDate(businessDate)}-${sanitiseToken(runId)}.csv`;

/**
 * <SCHEME>-<TYPE>-<FIRST>-<LAST>-<ms>-<original name>
 *
 * A custodian file can itself span days, so it is identified by the RANGE its
 * rows cover, not a single date. Collapsing a range to one day (the most common
 * date, say) makes two genuinely different files look identical and lets the
 * second silently replace the first.
 *
 * The folder stays the RUN's date, so everything for a run is listed, replaced
 * and cleaned up together. Dates are compact to keep the fixed part of the name
 * a fixed width, which is what lets the user's own file name — hyphens, spaces
 * and all — sit on the end and still be read back, as the FA key already does.
 */
export const buildCustodyKey = ({
  businessDate,
  scheme,
  fileType,
  coveredFrom,
  coveredTo,
  originalFileName = "file.csv",
  uploadedAtMs = Date.now(),
}) => {
  const type = sanitiseToken(fileType);
  if (!CUSTODY_FILE_TYPES.includes(type)) {
    throw new Error(
      `Unknown custodian file type "${fileType}". Expected ${CUSTODY_FILE_TYPES.join(" or ")}.`,
    );
  }
  const s = sanitiseToken(scheme);
  if (!s) throw new Error("A custodian file must name its scheme.");

  const from = assertBusinessDate(coveredFrom || businessDate).replace(/-/g, "");
  const to = assertBusinessDate(coveredTo || coveredFrom || businessDate).replace(/-/g, "");

  return `${custodyDayPrefix(businessDate)}${s}-${type}-${from}-${to}-${uploadedAtMs}-${sanitiseFileName(originalFileName)}`;
};

// ---------------------------------------------------------------- key parsers

const lastSegment = (key) => String(key || "").split("/").pop() || "";

/**
 * @returns {{businessDate,coveredFrom,coveredTo,scheme,fileType,originalFileName,
 *   uploadedAtMs,key}|null}
 */
export const parseCustodyKey = (key) => {
  const parts = String(key || "").split("/");
  if (parts.length < 3 || parts[0] !== FOLDER.CUSTODY) return null;
  const businessDate = parts[1];
  if (!DATE_PATTERN.test(businessDate)) return null;

  /*
   * The scheme is sanitised to A-Z, 0-9 and underscore, so it cannot contain a
   * hyphen — which is what makes the leading group unambiguous and lets the
   * trailing file name keep its own hyphens.
   */
  const pattern = new RegExp(
    String.raw`^([A-Z0-9_]+)-(${CUSTODY_FILE_TYPES.join("|")})-(\d{8})-(\d{8})-(\d+)-(.+)$`,
  );
  const m = lastSegment(key).match(pattern);
  if (!m) return null;

  const spread = (c) => `${c.slice(0, 4)}-${c.slice(4, 6)}-${c.slice(6, 8)}`;
  const coveredFrom = spread(m[3]);
  const coveredTo = spread(m[4]);
  if (!DATE_PATTERN.test(coveredFrom) || !DATE_PATTERN.test(coveredTo)) return null;

  return {
    key,
    businessDate,
    scheme: m[1],
    fileType: m[2],
    coveredFrom,
    coveredTo,
    uploadedAtMs: Number(m[5]),
    originalFileName: m[6],
  };
};

/** @returns {{businessDate,originalFileName,uploadedAtMs,key}|null} */
export const parseFaRawKey = (key) => {
  const parts = String(key || "").split("/");
  if (parts.length < 3 || parts[0] !== FOLDER.FA) return null;
  const businessDate = parts[1];
  if (!DATE_PATTERN.test(businessDate)) return null;

  const name = lastSegment(key);
  const m = name.match(/^raw-(\d+)-(.+)$/);
  if (!m) return null;

  return {
    key,
    businessDate,
    uploadedAtMs: Number(m[1]),
    originalFileName: m[2],
  };
};

/**
 * Custodian-History/<uploadedAtMs>-<SCHEME>-<TYPE>-<yyyymmdd>-<name>
 *
 * Everything the Previous Uploads table shows is in the key, so listing needs
 * no tracking table — the same idea as the day-folder keys above.
 */
export const buildCustodyHistoryKey = ({
  uploadedAtMs,
  scheme,
  fileType,
  businessDate,
  originalFileName = "file.csv",
}) => {
  const type = sanitiseToken(fileType);
  if (!CUSTODY_FILE_TYPES.includes(type)) {
    throw new Error(`Unknown custodian file type "${fileType}".`);
  }
  const s = sanitiseToken(scheme);
  if (!s) throw new Error("A custodian file must name its scheme.");
  const date = assertBusinessDate(businessDate).replace(/-/g, "");

  return `${FOLDER.CUSTODY_HISTORY}/${Number(uploadedAtMs)}-${s}-${type}-${date}-${sanitiseFileName(originalFileName)}`;
};

/** @returns {{key,uploadedAtMs,scheme,fileType,businessDate,originalFileName}|null} */
export const parseCustodyHistoryKey = (key) => {
  const parts = String(key || "").split("/");
  if (parts.length !== 2 || parts[0] !== FOLDER.CUSTODY_HISTORY) return null;

  const pattern = new RegExp(
    String.raw`^(\d+)-([A-Z0-9_]+)-(${CUSTODY_FILE_TYPES.join("|")})-(\d{8})-(.+)$`,
  );
  const m = parts[1].match(pattern);
  if (!m) return null;

  const businessDate = `${m[4].slice(0, 4)}-${m[4].slice(4, 6)}-${m[4].slice(6, 8)}`;
  if (!DATE_PATTERN.test(businessDate)) return null;

  return {
    key,
    uploadedAtMs: Number(m[1]),
    scheme: m[2],
    fileType: m[3],
    businessDate,
    originalFileName: m[5],
  };
};

// ---------------------------------------------------------------- bucket access

export const getBucket = (catalystApp, bucketName = BUCKET_NAME) => {
  if (!catalystApp) {
    throw new Error(
      "Catalyst app is not initialised. In a job function this must be catalyst.initialize(context, { scope: 'admin' }).",
    );
  }
  return catalystApp.stratus().bucket(bucketName);
};

export const bucketExists = async (catalystApp, bucketName = BUCKET_NAME) => {
  try {
    return Boolean(await catalystApp.stratus().headBucket(bucketName));
  } catch {
    return false;
  }
};

/**
 * The SDK returns either StratusObject instances (fields under `keyDetails`) or
 * plain objects with the fields at the top level. Normalise both, exactly as
 * listUploadHistory in TempTransactionUpload.js does.
 */
const normaliseListEntry = (entry) => {
  const o =
    entry && typeof entry === "object" && "keyDetails" in entry && entry.keyDetails
      ? entry.keyDetails
      : entry;
  if (!o || typeof o.key !== "string" || !o.key) return null;
  if (o.key_type && o.key_type !== "file") return null;
  return {
    key: o.key,
    sizeBytes: Number(o.size) || 0,
    contentType: o.content_type || "text/csv",
    lastModified: o.last_modified ?? null,
  };
};

const isTruncated = (result) =>
  result?.truncated === true ||
  String(result?.truncated || "").toLowerCase() === "true";

const nextToken = (result) =>
  result?.next_continuation_token || result?.nextContinuationToken || null;

/**
 * Every object under a prefix, following continuation tokens.
 *
 * listPagedObjects returns one page; a day folder is small, but the loop is what
 * makes "delete all files for this date" actually delete all of them rather than
 * the first page of them.
 */
export const listAllObjects = async (bucket, prefix, { maxPages = 50 } = {}) => {
  const items = [];
  let continuationToken = null;

  for (let page = 0; page < maxPages; page++) {
    const opts = { prefix, maxKeys: 100 };
    if (continuationToken) opts.continuationToken = continuationToken;

    const result = await bucket.listPagedObjects(opts);
    for (const entry of result?.contents || []) {
      const item = normaliseListEntry(entry);
      if (item) items.push(item);
    }

    if (!isTruncated(result)) break;
    continuationToken = nextToken(result);
    if (!continuationToken) break;
  }

  return items;
};

export const objectExists = async (bucket, key) => {
  const items = await listAllObjects(bucket, key, { maxPages: 1 });
  return items.some((i) => i.key === key);
};

// ---------------------------------------------------------------- read / write

/*
 * `overwrite` defaults to FALSE in the SDK, and several keys here are stable by
 * design — the field mappings, the consolidated finals, the S1 sidecar. Without
 * this, saving a mapping a second time or re-running a match for the same date
 * fails with "key is already associated with another object in the bucket".
 */
export const putText = (bucket, key, body, contentType = "text/csv") =>
  bucket.putObject(key, body, { contentType, overwrite: true });

export const putBuffer = (bucket, key, buffer, contentType = "text/csv") =>
  bucket.putObject(key, buffer, { contentType, overwrite: true });

export const putJson = (bucket, key, value) =>
  bucket.putObject(key, JSON.stringify(value), {
    contentType: "application/json",
    overwrite: true,
  });

/** getObject returns a Readable; it has to be consumed before there is any data. */
export const getBuffer = async (bucket, key) => {
  const stream = await bucket.getObject(key);
  if (Buffer.isBuffer(stream)) return stream;
  if (typeof stream === "string") return Buffer.from(stream, "utf8");
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
};

export const getText = async (bucket, key) =>
  (await getBuffer(bucket, key)).toString("utf8");

export const getJson = async (bucket, key) => JSON.parse(await getText(bucket, key));

/** Deletes are issued a few at a time — a day folder holds single digits of files, not thousands. */
export const deleteKeys = async (bucket, keys, { concurrency = 4 } = {}) => {
  const deleted = [];
  const failed = [];
  const queue = [...keys];

  const worker = async () => {
    while (queue.length > 0) {
      const key = queue.shift();
      if (!key) continue;
      try {
        await bucket.deleteObject(key);
        deleted.push(key);
      } catch (err) {
        failed.push({ key, message: err?.message || String(err) });
      }
    }
  };

  await Promise.all(
    Array.from({ length: Math.min(concurrency, Math.max(queue.length, 1)) }, worker),
  );

  return { deleted, failed };
};

// ---------------------------------------------------------------- day view

/**
 * Everything stored for one business date, described from the object keys alone.
 * Backs the "a file already exists for this date — replace it?" prompt and the
 * page reload.
 */
export const listDay = async (catalystApp, businessDate, { bucket = null } = {}) => {
  const date = assertBusinessDate(businessDate);
  const b = bucket || getBucket(catalystApp);

  const [faObjects, custodyObjects] = await Promise.all([
    listAllObjects(b, faDayPrefix(date)),
    listAllObjects(b, custodyDayPrefix(date)),
  ]);

  const faFinalKey = buildFaFinalKey(date);
  const custodyFinalKey = buildCustodyFinalKey(date);

  const faRaw = [];
  const differences = [];
  let faFinal = null;

  for (const obj of faObjects) {
    if (obj.key === faFinalKey) {
      faFinal = obj;
      continue;
    }
    const parsed = parseFaRawKey(obj.key);
    if (parsed) {
      faRaw.push({ ...obj, ...parsed });
      continue;
    }
    if (lastSegment(obj.key).startsWith("difference-")) differences.push(obj);
  }

  const custodyFiles = [];
  let custodyFinal = null;

  for (const obj of custodyObjects) {
    if (obj.key === custodyFinalKey) {
      custodyFinal = obj;
      continue;
    }
    const parsed = parseCustodyKey(obj.key);
    if (parsed) custodyFiles.push({ ...obj, ...parsed });
  }

  faRaw.sort((a, b2) => b2.uploadedAtMs - a.uploadedAtMs);
  /*
   * The screen shows one row per file, so this is a flat list — a scheme holds
   * as many files as the run needs, one per range of dates the custodian sent.
   * Which ranges are actually expected is not something the bucket can know, so
   * a date with no custodian file surfaces later as MISSING_IN_CUSTODY rather
   * than being guessed at here.
   */
  custodyFiles.sort(
    (a, b2) =>
      a.scheme.localeCompare(b2.scheme) ||
      a.coveredFrom.localeCompare(b2.coveredFrom) ||
      b2.uploadedAtMs - a.uploadedAtMs,
  );

  return {
    businessDate: date,
    fa: {
      raw: faRaw,
      latestRaw: faRaw[0] || null,
      final: faFinal,
      differences,
      hasFile: faRaw.length > 0,
    },
    custody: {
      files: custodyFiles,
      final: custodyFinal,
      hasFiles: custodyFiles.length > 0,
    },
  };
};

/**
 * Clear one side of a business date.
 *
 * Custody is deliberately all-or-nothing: replacing a single file by name is how
 * you end up comparing Tuesday's deal file against Wednesday's allocation file.
 *
 * @param {"fa"|"custody"} side
 */
export const deleteDaySide = async (
  catalystApp,
  businessDate,
  side,
  { bucket = null } = {},
) => {
  const date = assertBusinessDate(businessDate);
  const b = bucket || getBucket(catalystApp);

  let prefix;
  if (side === SIDE_KEY.FA) prefix = faDayPrefix(date);
  else if (side === SIDE_KEY.CUSTODY) prefix = custodyDayPrefix(date);
  else throw new Error(`side must be "${SIDE_KEY.FA}" or "${SIDE_KEY.CUSTODY}".`);

  const objects = await listAllObjects(b, prefix);
  const result = await deleteKeys(
    b,
    objects.map((o) => o.key),
  );

  return { side, businessDate: date, prefix, ...result, count: result.deleted.length };
};

/** Both sides. Used by the cleanup cron and by a full restart of a day. */
export const deleteDay = async (catalystApp, businessDate, { bucket = null } = {}) => {
  const b = bucket || getBucket(catalystApp);
  const fa = await deleteDaySide(catalystApp, businessDate, SIDE_KEY.FA, { bucket: b });
  const custody = await deleteDaySide(catalystApp, businessDate, SIDE_KEY.CUSTODY, {
    bucket: b,
  });
  return { businessDate: assertBusinessDate(businessDate), fa, custody };
};

/**
 * Distinct schemes seen in the bucket, newest use first.
 *
 * The custodian key already encodes everything the picker needs
 * (`<SCHEME>-<TYPE>-<FIRST>-<LAST>-<ms>.csv` under a dated folder), so the list of
 * schemes is a
 * by-product of the files themselves — no scheme master table, and each entry
 * knows its own account type, which is what lets picking a scheme fill the
 * layout dropdown too.
 *
 * A scheme used under both layouts appears once per layout; that is a real
 * distinction, not a duplicate.
 */
export const listKnownSchemes = async (catalystApp, { bucket = null } = {}) => {
  const b = bucket || getBucket(catalystApp);
  const objects = await listAllObjects(b, `${FOLDER.CUSTODY}/`, { maxPages: 200 });

  const seen = new Map();
  for (const o of objects) {
    const parsed = parseCustodyKey(o.key);
    if (!parsed) continue;

    const id = `${parsed.scheme}|${parsed.fileType}`;
    const prev = seen.get(id);
    if (!prev || parsed.businessDate > prev.lastUsedDate) {
      seen.set(id, {
        scheme: parsed.scheme,
        fileType: parsed.fileType,
        lastUsedDate: parsed.businessDate,
      });
    }
  }

  return [...seen.values()].sort(
    (a, b2) => b2.lastUsedDate.localeCompare(a.lastUsedDate) || a.scheme.localeCompare(b2.scheme),
  );
};

/** Business dates currently held in the bucket, newest first. Used by the cleanup cron. */
export const listStoredDates = async (catalystApp, { bucket = null } = {}) => {
  const b = bucket || getBucket(catalystApp);
  const dates = new Set();

  for (const folder of [FOLDER.FA, FOLDER.CUSTODY]) {
    const objects = await listAllObjects(b, `${folder}/`, { maxPages: 200 });
    for (const o of objects) {
      const part = o.key.split("/")[1];
      if (DATE_PATTERN.test(part)) dates.add(part);
    }
  }

  return [...dates].sort().reverse();
};

/* --------------------------------------------------------------------------
 * Custodian upload history
 * ------------------------------------------------------------------------ */

const newestFirst = (a, b) => b.uploadedAtMs - a.uploadedAtMs;

/** Every stored history copy, newest first. */
export const listCustodyHistory = async (catalystApp, { bucket = null } = {}) => {
  const b = bucket || getBucket(catalystApp);
  const objects = await listAllObjects(b, `${FOLDER.CUSTODY_HISTORY}/`);

  const items = [];
  for (const o of objects) {
    const parsed = parseCustodyHistoryKey(o.key);
    if (parsed) items.push({ ...parsed, sizeBytes: o.sizeBytes });
  }
  return items.sort(newestFirst);
};

/**
 * Store a copy of an uploaded custodian file and delete copies beyond the limit.
 *
 * Returns the copies that remain, newest first. Stratus's listing lags a fresh
 * write by around twenty seconds, so the new copy is added to the list by hand
 * rather than trusting the listing to contain it yet.
 */
export const saveToCustodyHistory = async (
  catalystApp,
  { buffer, scheme, fileType, businessDate, originalFileName, uploadedAtMs },
  { bucket = null } = {},
) => {
  const b = bucket || getBucket(catalystApp);
  const key = buildCustodyHistoryKey({
    uploadedAtMs,
    scheme,
    fileType,
    businessDate,
    originalFileName,
  });
  await putBuffer(b, key, buffer, "text/csv");

  const listed = (await listCustodyHistory(catalystApp, { bucket: b })).filter((f) => f.key !== key);
  const all = [{ ...parseCustodyHistoryKey(key), sizeBytes: buffer.length }, ...listed].sort(
    newestFirst,
  );

  const kept = all.slice(0, CUSTODY_HISTORY_LIMIT);
  const expired = all.slice(CUSTODY_HISTORY_LIMIT);
  if (expired.length > 0) {
    const result = await deleteKeys(b, expired.map((f) => f.key));
    if (result.failed.length > 0) {
      // An old copy left behind is harmless: the list only ever shows the newest
      // five, and the next upload tries to delete it again.
      console.warn("saveToCustodyHistory: could not delete old copies:", result.failed);
    }
  }

  return kept;
};

// ---------------------------------------------------------------- download URL

/**
 * generatePreSignedUrl's result shape varies by SDK version.
 *
 * Inlined rather than imported from util/stratusSignedUrl.js so this file stays
 * a straight copy when it is vendored into the job function — the same reason
 * functions/ExportCorpActionReport/virtualToActual.js is a copy rather than an
 * import. Keep the two in step if either changes.
 */
export const signedUrlToString = (res) => {
  if (res == null) return "";
  if (typeof res === "string") return res;
  if (typeof res.accessUrl === "string") return res.accessUrl;
  if (typeof res.signature === "string") return res.signature;
  if (res.signature && typeof res.signature === "object") {
    return signedUrlToString(res.signature);
  }
  return "";
};

/** A presigned GET, normalised to a plain string. Note the SDK key is expiryIn, not expiresIn. */
export const signedGetUrl = async (bucket, key, expiryIn = 3600) => {
  const raw = await bucket.generatePreSignedUrl(key, "GET", {
    expiryIn: String(expiryIn),
  });
  return signedUrlToString(raw);
};


/* --------------------------------------------------------------------------
 * Field mappings
 *
 * One small JSON object per file type, outside the date folders so the cleanup
 * cron never touches them. Stratus has no atomic update, which is fine here:
 * three objects edited by hand, rarely, by one team.
 * ------------------------------------------------------------------------ */

/** field-maps/<TYPE>.json */
export const buildFieldMapKey = (type) => `${FOLDER.FIELD_MAPS}/${sanitiseToken(type)}.json`;

/**
 * Read one stored mapping.
 *
 * A missing object is the normal case before anybody has edited anything, so it
 * resolves to null rather than throwing. Callers pass that to fromStoredProfile,
 * which falls back to the built-in default.
 */
export const readFieldMapBlob = async (catalystApp, type, { bucket = null } = {}) => {
  const b = bucket || getBucket(catalystApp);
  const key = buildFieldMapKey(type);
  try {
    if (!(await objectExists(b, key))) return null;
    return await getJson(b, key);
  } catch {
    // A corrupt or unreadable config must not take the upload screen down.
    return null;
  }
};

/** Write one stored mapping. */
export const writeFieldMapBlob = async (catalystApp, type, blob, { bucket = null } = {}) => {
  const b = bucket || getBucket(catalystApp);
  const key = buildFieldMapKey(type);
  await putJson(b, key, blob);
  return key;
};

/**
 * Delete one stored mapping.
 *
 * Nothing stored is already the goal, so a missing object is not an error.
 * Returns whether there was a file to delete.
 */
export const deleteFieldMapBlob = async (catalystApp, type, { bucket = null } = {}) => {
  const b = bucket || getBucket(catalystApp);
  const key = buildFieldMapKey(type);
  if (!(await objectExists(b, key))) return false;
  await b.deleteObject(key);
  return true;
};

/** Read every stored mapping at once, keyed by type. Missing ones come back null. */
export const readAllFieldMapBlobs = async (catalystApp, types, { bucket = null } = {}) => {
  const b = bucket || getBucket(catalystApp);
  const out = {};
  for (const type of types) out[type] = await readFieldMapBlob(catalystApp, type, { bucket: b });
  return out;
};

/**
 * The mappings every parser call needs, ready to pass straight through.
 *
 * Always returns a usable profile for all three types: an unsaved or unreadable
 * one falls back to the built-in default, so a bucket problem degrades to
 * today's behaviour rather than failing the upload.
 */
export const loadFieldMapProfiles = async (catalystApp, { bucket = null } = {}) => {
  const out = {};
  try {
    const blobs = await readAllFieldMapBlobs(catalystApp, PROFILE_TYPES, { bucket });
    for (const type of PROFILE_TYPES) out[type] = fromStoredProfile(type, blobs[type]).profile;
  } catch {
    for (const type of PROFILE_TYPES) out[type] = defaultProfile(type);
  }
  return out;
};
