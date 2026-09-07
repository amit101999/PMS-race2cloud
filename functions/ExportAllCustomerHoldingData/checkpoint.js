"use strict";

/**
 * Stratus-backed checkpoint helpers for the baton-passed "Export All Clients"
 * job — mirrors the dispatch-state pattern in
 * functions/CalculateHoldingMaster/index.js (loadDispatchState/saveDispatchState).
 *
 * Per jobName (e.g. "EA_2026-07-21" / "EAC_2026-07-21") this keeps three objects
 * on the `upload-data-bucket` under exports-meta/:
 *   - <jobName>-manifest.json    snapshot of the client/group work list, taken
 *                                once on the first invocation so a moving
 *                                clientIds table doesn't shift indexes mid-run
 *   - <jobName>-checkpoint.json  cursor into the manifest + multipart upload
 *                                state (uploadId, nextPartNumber) + price cache
 *   - <jobName>-pending.csv      CSV bytes accumulated but not yet big enough
 *                                to flush as a multipart part (<5MB)
 */

const META_PREFIX = "exports-meta";

const manifestKeyFor = (jobName) => `${META_PREFIX}/${jobName}-manifest.json`;
const checkpointKeyFor = (jobName) => `${META_PREFIX}/${jobName}-checkpoint.json`;
const pendingBufferKeyFor = (jobName) => `${META_PREFIX}/${jobName}-pending.csv`;

function rawToUtf8(raw) {
  if (raw == null) return "";
  if (Buffer.isBuffer(raw)) return raw.toString("utf8");
  if (raw instanceof ArrayBuffer) return Buffer.from(raw).toString("utf8");
  if (typeof raw === "string") return raw;
  return null;
}

async function streamToString(stream) {
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks).toString("utf8");
}

function isStratusNotFound(err) {
  const code = String(err?.code || "").toUpperCase();
  const status = Number(err?.statusCode);
  const msg = String(err?.message || "").toLowerCase();
  return (
    status === 404 ||
    code === "NOT_FOUND" ||
    msg.includes("no such object") ||
    msg.includes("not found")
  );
}

async function readStratusObject(bucket, key) {
  try {
    const raw = await bucket.getObject(key);
    if (raw != null && typeof raw === "object" && typeof raw.pipe === "function") {
      return await streamToString(raw);
    }
    const s = rawToUtf8(raw);
    return s == null ? "" : s;
  } catch (err) {
    if (isStratusNotFound(err)) return null;
    throw err;
  }
}

async function writeStratusObject(bucket, key, text, contentType) {
  await bucket.putObject(key, Buffer.from(text, "utf8"), {
    overwrite: true,
    contentType: contentType || "application/json",
  });
}

async function deleteStratusObjectSafe(bucket, key) {
  try {
    await bucket.deleteObject(key);
  } catch (err) {
    if (!isStratusNotFound(err)) {
      console.warn(`Failed to delete Stratus object ${key}:`, err.message);
    }
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Deletes an object and CONFIRMS it's actually gone before returning, instead
 * of trusting a delete call that may have silently failed (wrong permission
 * scope, transient error, etc.) — a scenario that previously went unnoticed
 * because the delete's own errors were only ever logged, never checked
 * against what Stratus subsequently reports for the key. If the key is still
 * readable after retries, throws instead of letting the caller move on to
 * initiateMultipartUpload, which would otherwise fail later with an opaque
 * 409 key_already_exists.
 *
 * maxAttempts=12 (~20s total backoff) rather than a couple of quick retries —
 * this runs inside a background Job, not a request/response path, so it's
 * fine to wait out Stratus's eventual-consistency window on a delete rather
 * than fail a re-export just because the read-after-delete hadn't caught up
 * yet (observed in practice on same-date re-exports issued shortly after a
 * prior run).
 */
async function ensureObjectDeleted(bucket, key, maxAttempts = 12) {
  try {
    await bucket.deleteObject(key);
  } catch (err) {
    if (!isStratusNotFound(err)) {
      console.error(`ensureObjectDeleted: deleteObject failed for ${key}:`, err.statusCode, err.message);
    }
  }

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      await bucket.getObject(key);
      // Object still readable — delete hasn't taken effect yet (or failed).
    } catch (err) {
      if (isStratusNotFound(err)) return; // confirmed gone
      console.warn(
        `ensureObjectDeleted: verify getObject failed for ${key} (attempt ${attempt}):`,
        err.statusCode,
        err.message
      );
    }
    if (attempt < maxAttempts) await sleep(300 * attempt);
  }

  throw new Error(
    `Could not confirm deletion of Stratus object "${key}" after ${maxAttempts} attempts — ` +
      `it may still exist and would block a fresh multipart upload on this key.`
  );
}

async function loadManifest(bucket, jobName) {
  const raw = await readStratusObject(bucket, manifestKeyFor(jobName));
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : null;
  } catch (err) {
    console.warn(`Invalid manifest JSON for ${jobName}:`, err.message);
    return null;
  }
}

async function saveManifest(bucket, jobName, entries) {
  await writeStratusObject(bucket, manifestKeyFor(jobName), JSON.stringify(entries));
}

async function loadCheckpoint(bucket, jobName) {
  const raw = await readStratusObject(bucket, checkpointKeyFor(jobName));
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch (err) {
    console.warn(`Invalid checkpoint JSON for ${jobName}:`, err.message);
    return null;
  }
}

async function saveCheckpoint(bucket, jobName, state) {
  const payload = { ...state, updatedAtMs: Date.now() };
  await writeStratusObject(bucket, checkpointKeyFor(jobName), JSON.stringify(payload));
}

async function loadPendingBuffer(bucket, jobName) {
  const raw = await readStratusObject(bucket, pendingBufferKeyFor(jobName));
  return raw || "";
}

async function savePendingBuffer(bucket, jobName, text) {
  if (!text) {
    await deleteStratusObjectSafe(bucket, pendingBufferKeyFor(jobName));
    return;
  }
  await writeStratusObject(bucket, pendingBufferKeyFor(jobName), text, "text/csv");
}

/**
 * Force-closes a dangling multipart upload so its key can be reused.
 *
 * Stratus has no "abort upload" API — an upload started with
 * initiateMultipartUpload keeps the key reserved until completeMultipartUpload
 * runs, and deleteObject on the key does nothing until then. The only way to
 * release the key with the calls Stratus does expose is to push it to a
 * finished (if garbage) state and then delete that result: upload one empty
 * "final" part (multipart uploads accept an undersized last part — see the
 * final flush in index.js), complete the upload, then delete the object it
 * produced.
 */
async function releaseStaleUpload(bucket, fileName, uploadId, nextPartNumber) {
  try {
    await bucket.uploadPart(fileName, uploadId, Buffer.alloc(0), nextPartNumber || 1, "true");
  } catch (err) {
    console.warn(`releaseStaleUpload: uploadPart failed for ${fileName}:`, err.message);
  }
  try {
    await bucket.completeMultipartUpload(fileName, uploadId);
  } catch (err) {
    console.warn(`releaseStaleUpload: completeMultipartUpload failed for ${fileName}:`, err.message);
  }
  await deleteStratusObjectSafe(bucket, fileName);
}

async function deleteExportArtifacts(bucket, jobName) {
  await deleteStratusObjectSafe(bucket, manifestKeyFor(jobName));
  await deleteStratusObjectSafe(bucket, checkpointKeyFor(jobName));
  await deleteStratusObjectSafe(bucket, pendingBufferKeyFor(jobName));
}

module.exports = {
  manifestKeyFor,
  checkpointKeyFor,
  pendingBufferKeyFor,
  loadManifest,
  saveManifest,
  loadCheckpoint,
  saveCheckpoint,
  loadPendingBuffer,
  savePendingBuffer,
  deleteExportArtifacts,
  deleteStratusObjectSafe,
  releaseStaleUpload,
  ensureObjectDeleted,
};
