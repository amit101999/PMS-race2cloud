/**
 * Holding Comparison — Stratus keys and bucket access (bucket "temporary-files").
 *
 *   Custodian-Holdings/<date>/consolidated-<ms>-<name>.csv   custodian uploads
 *   FA-Holdings/<date>/fa-holdings-<ms>.csv                   FA holdings snapshots
 *   Holding-Differences/<date>/diff-HC-<ms>.csv               difference reports
 *
 * The newest object in a date folder is the current one. Older objects are kept
 * so the "previous files" lists can offer them, and so every run's inputs stay
 * auditable. Metadata (date, time, file name) is read back from the key, so no
 * tracking table is needed.
 */

import { DATE_PATTERN } from "./constants.js";

export const BUCKET_NAME = "temporary-files";

export const FOLDER = {
  CUSTODY: "Custodian-Holdings",
  FA: "FA-Holdings",
  DIFFERENCE: "Holding-Differences",
};

export const HISTORY_LIMIT = 10;

export const getBucket = (catalystApp, bucketName = BUCKET_NAME) =>
  catalystApp.stratus().bucket(bucketName);

/** Keep a file name safe to use inside an object key. */
const safeFileName = (name) =>
  String(name || "file.csv")
    .replace(/[\\/]+/g, "_")
    .replace(/[^\w.\- ()]+/g, "_")
    .slice(0, 150);

export const buildCustodyKey = (date, uploadedAtMs, originalFileName) =>
  `${FOLDER.CUSTODY}/${date}/consolidated-${uploadedAtMs}-${safeFileName(originalFileName)}`;

export const buildFaKey = (date, generatedAtMs) =>
  `${FOLDER.FA}/${date}/fa-holdings-${generatedAtMs}.csv`;

/**
 * Progress of an FA holdings generation that did not fit in one job invocation.
 * Kept outside FA-Holdings/ so it never shows up as a snapshot.
 */
export const buildFaWorkKey = (date) => `FA-Holdings-Work/${date}.json`;

export const buildDifferenceKey = (date, runId) => `${FOLDER.DIFFERENCE}/${date}/diff-${runId}.csv`;

const KEY_PATTERNS = [
  /^Custodian-Holdings\/(\d{4}-\d{2}-\d{2})\/consolidated-(\d+)-([^/]+)$/,
  /^FA-Holdings\/(\d{4}-\d{2}-\d{2})\/(fa-holdings-(\d+)\.csv)$/,
  /^Holding-Differences\/(\d{4}-\d{2}-\d{2})\/(diff-HC-(\d+)\.csv)$/,
];

/**
 * Read date, time and display name back from one of this module's keys.
 * @returns {{ key, businessDate, timeMs, fileName } | null}
 */
export const parseKey = (key) => {
  const custody = KEY_PATTERNS[0].exec(key);
  if (custody) {
    return { key, businessDate: custody[1], timeMs: Number(custody[2]), fileName: custody[3] };
  }
  for (const pattern of KEY_PATTERNS.slice(1)) {
    const m = pattern.exec(key);
    if (m) return { key, businessDate: m[1], timeMs: Number(m[3]), fileName: m[2] };
  }
  return null;
};

/** Only this module's own objects may be downloaded through its endpoints. */
export const isModuleKey = (key) => parseKey(String(key || "")) !== null;

/** The SDK returns either StratusObject instances (fields under keyDetails) or plain objects. */
const listEntry = (entry) => {
  const o = entry?.keyDetails || entry;
  if (!o || typeof o.key !== "string") return null;
  if (o.key_type && o.key_type !== "file") return null;
  return { key: o.key, sizeBytes: Number(o.size) || 0 };
};

/** Every object under a prefix, following continuation tokens. */
export const listAll = async (bucket, prefix, maxPages = 50) => {
  const items = [];
  let continuationToken = null;
  for (let page = 0; page < maxPages; page++) {
    const opts = { prefix, maxKeys: 100 };
    if (continuationToken) opts.continuationToken = continuationToken;
    const result = await bucket.listPagedObjects(opts);
    for (const entry of result?.contents || []) {
      const item = listEntry(entry);
      if (item) items.push(item);
    }
    const truncated = result?.truncated === true || String(result?.truncated) === "true";
    continuationToken = result?.next_continuation_token || result?.nextContinuationToken || null;
    if (!truncated || !continuationToken) break;
  }
  return items;
};

/** This module's objects under a prefix, newest first. */
const listParsed = async (bucket, prefix) => {
  const objects = await listAll(bucket, prefix);
  return objects
    .map((o) => {
      const parsed = parseKey(o.key);
      return parsed ? { ...parsed, sizeBytes: o.sizeBytes } : null;
    })
    .filter(Boolean)
    .sort((a, b) => b.timeMs - a.timeMs);
};

/**
 * The newest file in one date folder ("custody" | "fa"), or null.
 *
 * Stratus listings keep showing a deleted object for about twenty seconds, so
 * the newest listed file is confirmed to still exist before it is returned.
 * Otherwise a file removed a moment ago would come straight back as staged.
 */
export const latestForDate = async (bucket, folder, date) => {
  if (!DATE_PATTERN.test(date)) return null;
  const items = await listParsed(bucket, `${folder}/${date}/`);
  for (const item of items) {
    if (await bucket.headObject(item.key, { throwErr: false })) return item;
  }
  return null;
};

/*
 * Removing the staged custodian file must not bring back an earlier upload for
 * the same date: those stay in the history list, but the operator is about to
 * upload the correct file. So removal records when the date was cleared, and
 * only a file uploaded after that counts as staged. Kept outside
 * Custodian-Holdings/ so it never shows up as an upload.
 */
const custodyClearedKey = (date) => `Custodian-Holdings-Cleared/${date}.json`;

export const markCustodyCleared = (bucket, date, clearedAtMs) =>
  putText(bucket, custodyClearedKey(date), JSON.stringify({ clearedAtMs }), "application/json");

const readCustodyClearedAt = async (bucket, date) => {
  const key = custodyClearedKey(date);
  if (!(await bucket.headObject(key, { throwErr: false }))) return 0;
  return Number(JSON.parse((await getBuffer(bucket, key)).toString("utf8")).clearedAtMs) || 0;
};

/** The staged custodian file for a date: the newest upload since the date was last cleared, or null. */
export const stagedCustodyFile = async (bucket, date) => {
  if (!DATE_PATTERN.test(date)) return null;
  const clearedAtMs = await readCustodyClearedAt(bucket, date);
  const items = await listParsed(bucket, `${FOLDER.CUSTODY}/${date}/`);
  for (const item of items) {
    if (item.timeMs <= clearedAtMs) return null;
    if (await bucket.headObject(item.key, { throwErr: false })) return item;
  }
  return null;
};

/** Whether an upload for a date was made after the date was last cleared. */
export const isUploadedSinceCleared = async (bucket, date, timeMs) =>
  timeMs > (await readCustodyClearedAt(bucket, date));

/** The newest HISTORY_LIMIT files across all dates in one folder. */
export const listRecent = async (bucket, folder, limit = HISTORY_LIMIT) =>
  (await listParsed(bucket, `${folder}/`)).slice(0, limit);

/*
 * overwrite defaults to false in the SDK. Difference reports use a fixed key per
 * run, so a retried job has to be able to replace its own earlier write.
 */
export const putText = (bucket, key, body, contentType = "text/csv") =>
  bucket.putObject(key, body, { contentType, overwrite: true });

/** getObject returns a stream; it has to be consumed before there is any data. */
export const getBuffer = async (bucket, key) => {
  const data = await bucket.getObject(key);
  if (Buffer.isBuffer(data)) return data;
  if (typeof data === "string") return Buffer.from(data, "utf8");
  const chunks = [];
  for await (const chunk of data) chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  return Buffer.concat(chunks);
};

/** A saved field mapping blob, or null when none is saved or it cannot be read. */
export const readFieldMapProfile = async (bucket, profileType) => {
  const key = `field-maps/${profileType}.json`;
  try {
    const items = await listAll(bucket, key, 1);
    if (!items.some((i) => i.key === key)) return null;
    const blob = JSON.parse((await getBuffer(bucket, key)).toString("utf8"));
    return blob?.version === 1 && blob.profile && typeof blob.profile === "object"
      ? blob.profile
      : null;
  } catch (err) {
    console.warn(`readFieldMapProfile(${profileType}) failed; using the standard template:`, err.message);
    return null;
  }
};
