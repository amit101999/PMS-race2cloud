"use strict";

const catalyst = require("zcatalyst-sdk-node");
const { runDemergerApply, DEMERGER_JOB_TYPE_DEFAULT } = require("./demergerApplyCore.js");

const REBUILD_BATCH = 400;

async function queueRebuildHoldingsJobs(
  catalystApp,
  accountCodes,
  source,
  isins,
  caTrackKey = "",
) {
  const uniq = [...new Set(accountCodes)]
    .map((a) => String(a || "").trim())
    .filter(Boolean);
  if (!uniq.length) return;

  /* Scope the rebuild to the ISIN(s) this demerger touched (old + new).
     Without this the downstream RebuildHoldingtable re-derives EVERY ISIN for
     EVERY account, which blows past the 15-min Catalyst timeout on a large book. */
  /* Do NOT change case here — the rebuild scope must match the EXACT ISIN case
     stored by the apply (demerger preserves the typed case, e.g. "testNewSec101").
     Uppercasing made the scoped rebuild miss the new ISIN entirely. */
  const scopedIsins = [...new Set(isins || [])]
    .map((i) => String(i || "").trim())
    .filter(Boolean);

  const scheduling = catalystApp.jobScheduling();
  const zcql = catalystApp.zcql();
  for (let i = 0; i < uniq.length; i += REBUILD_BATCH) {
    const chunk = uniq.slice(i, i + REBUILD_BATCH);
    const catalystJobName = `H${Date.now()}${i}`.slice(0, 20);
    const jobParams = {
      accountCodesJson: JSON.stringify(chunk),
      source,
      ...(scopedIsins.length ? { isinsJson: JSON.stringify(scopedIsins) } : {}),
    };

    // Notification: register a piece for this chunk under the demerger's track
    // key BEFORE the "_root" placeholder is flipped, so the parent can't read
    // "all done" while this rebuild is still pending.
    if (caTrackKey) {
      const childSubId = `${caTrackKey}_dh${Date.now()}_${i}`;
      try {
        await zcql.executeZCQLQuery(
          `INSERT INTO JobStatusPerAccount (jobType, WS_Account_code, status, lastError, jobName) ` +
            `VALUES ('DEMERGER_APPLY', '${esc(childSubId)}', 'RUNNING', '', '${esc(caTrackKey)}')`,
        );
        jobParams.caTrackKey = caTrackKey;
        jobParams.caSubId = childSubId;
      } catch (e) {
        console.warn(`[DemergerFn] child piece insert failed (${childSubId}):`, e.message);
      }
    }

    await scheduling.JOB.submitJob({
      job_name: catalystJobName,
      jobpool_name: "CorporateActions",
      target_name: "RebuildHoldingtable",
      target_type: "Function",
      /* Catalyst Job Pool: retries only on execution failure. Min interval 1m. */
      job_config: {
        number_of_retries: 5,
        retry_interval: 60 * 1000,
      },
      params: jobParams,
    });
  }
}

const esc = (v) => String(v ?? "").replace(/'/g, "''");

/**
 * "Apply Demerger" notification progress (params.caTrackKey / caSubId, set by
 * DemergerController — no-op otherwise). Flips this run's placeholder piece in
 * `JobStatusPerAccount` (jobName = trackKey, WS_Account_code = subId) to
 * SUCCESS / FAILED, then settles the parent `Jobs` row named with the trackKey
 * once no piece for it is still RUNNING. The rebuild jobs this demerger spawns
 * register their own pieces (see queueRebuildHoldingsJobs) BEFORE this
 * placeholder is flipped, so the parent can't read "all done" early.
 */
async function settleCaTrackPiece(zcql, trackKey, subId, outcome) {
  if (!trackKey || !subId) return;
  const pieceStatus = outcome === "SUCCESS" ? "SUCCESS" : "FAILED";
  try {
    await zcql.executeZCQLQuery(
      `UPDATE JobStatusPerAccount SET status = '${esc(pieceStatus)}', ` +
        `lastError = '${pieceStatus === "FAILED" ? "demerger apply failed" : ""}' ` +
        `WHERE jobName = '${esc(trackKey)}' AND WS_Account_code = '${esc(subId)}'`,
    );
  } catch (e) {
    console.warn(`[DemergerFn] ca-track mark '${subId}' ${pieceStatus} failed:`, e.message);
    return;
  }
  try {
    const running = await zcql.executeZCQLQuery(
      `SELECT status FROM JobStatusPerAccount WHERE jobName = '${esc(trackKey)}' AND status = 'RUNNING' LIMIT 1`,
    );
    if (running && running.length) return;
    const failed = await zcql.executeZCQLQuery(
      `SELECT status FROM JobStatusPerAccount WHERE jobName = '${esc(trackKey)}' AND status = 'FAILED' LIMIT 1`,
    );
    const parentStatus = failed && failed.length ? "COMPLETED_WITH_ERRORS" : "COMPLETED";
    await zcql.executeZCQLQuery(
      `UPDATE Jobs SET status = '${parentStatus}' WHERE jobName = '${esc(trackKey)}'`,
    );
    console.log(`[DemergerFn] ca track '${trackKey}' finalized as ${parentStatus}`);
  } catch (e) {
    console.warn(`[DemergerFn] ca-track finalize '${trackKey}' failed:`, e.message);
  }
}

async function setJobStatus(zcql, jobName, status) {
  if (!jobName) return;
  try {
    await zcql.executeZCQLQuery(
      `UPDATE Jobs SET status = '${esc(status)}' WHERE jobName = '${esc(jobName)}'`,
    );
  } catch (err) {
    console.error(`[DemergerFn] Failed to set status ${status}:`, err.message);
  }
}

module.exports = async (jobRequest, context) => {
  const catalystApp = catalyst.initialize(context);
  const zcql = catalystApp.zcql();

  let jobName = "";
  // "Apply Demerger" notification tracking (see settleCaTrackPiece). caSubId is
  // this run's "_root" placeholder piece.
  let caTrackKey = "";
  let caSubId = "";
  const startedAt = Date.now();

  try {
    const params = jobRequest.getAllJobParams();
    jobName = String(params.jobName || "").trim();
    caTrackKey = String(params.caTrackKey || "").trim();
    caSubId = String(params.caSubId || "").trim();
    const demergerJobType =
      String(params.jobType || "").trim() || DEMERGER_JOB_TYPE_DEFAULT;

    const oldIsin = String(params.oldIsin || "").trim();
    const newIsin = String(params.newIsin || "").trim();
    const oldSecurityCode = String(params.oldSecurityCode || "").trim();
    const oldSecurityName = String(params.oldSecurityName || "").trim();
    const newSecurityCode = String(params.newSecurityCode || "").trim();
    const newSecurityName = String(params.newSecurityName || "").trim();
    const r1 = Number(params.ratio1 ?? params.r1);
    const r2 = Number(params.ratio2 ?? params.r2);
    const pct = Number(params.allocationPct ?? params.allocationToNewPercent);
    const effectiveDateISO = String(params.effectiveDateIso || "").trim();
    const recordDateISO = String(params.recordDateIso || "").trim();

    console.log("[DemergerFn] Started", {
      jobName,
      demergerJobType,
      jobStatusTracking: Boolean(jobName),
      oldIsin,
      newIsin,
      r1,
      r2,
      pct,
      effectiveDateISO,
      recordDateISO,
    });

    try {
      await zcql.executeZCQLQuery(
        `INSERT INTO Jobs (jobName, status) VALUES ('${esc(jobName)}', 'RUNNING')`,
      );
    } catch (insertErr) {
      console.warn(
        "[DemergerFn] Jobs insert failed (may exist):",
        insertErr.message,
      );
      await setJobStatus(zcql, jobName, "RUNNING");
    }

    if (
      !jobName ||
      !oldIsin ||
      !newIsin ||
      oldIsin === newIsin ||
      !oldSecurityCode ||
      !oldSecurityName ||
      !newSecurityCode ||
      !newSecurityName ||
      !Number.isFinite(r1) ||
      r1 <= 0 ||
      !Number.isFinite(r2) ||
      r2 <= 0 ||
      !Number.isFinite(pct) ||
      pct < 0 ||
      pct > 100 ||
      !/^\d{4}-\d{2}-\d{2}$/.test(effectiveDateISO) ||
      !/^\d{4}-\d{2}-\d{2}$/.test(recordDateISO)
    ) {
      console.error("[DemergerFn] Invalid params");
      await setJobStatus(zcql, jobName, "FAILED");
      await settleCaTrackPiece(zcql, caTrackKey, caSubId, "FAILURE");
      context.closeWithFailure();
      return;
    }

    const applyResult = await runDemergerApply(zcql, {
      oldIsin,
      oldSecurityCode,
      oldSecurityName,
      newIsin,
      newSecurityCode,
      newSecurityName,
      r1,
      r2,
      pct,
      effectiveDateISO,
      recordDateISO,
      jobTracking: {
        enabled: Boolean(jobName),
        jobName,
        jobType: demergerJobType,
      },
    });

    console.log("[DemergerFn] Records inserted:", applyResult.recordsInserted);
    if (applyResult.trackingFailures > 0) {
      console.warn(
        `[DemergerFn] ${applyResult.trackingFailures} account(s) FAILED — check JobStatusPerAccount`,
      );
    }

    if (applyResult.accountsAffected.length > 0) {
      try {
        await queueRebuildHoldingsJobs(
          catalystApp,
          applyResult.accountsAffected,
          "DemergerFn",
          [oldIsin, newIsin],
          caTrackKey,
        );
        console.log(
          "[DemergerFn] Queued RebuildHoldingtable for",
          applyResult.accountsAffected.length,
          "account(s)",
        );
      } catch (rbErr) {
        console.error("[DemergerFn] Queue holdings rebuild failed:", rbErr);
        await setJobStatus(zcql, jobName, "FAILED");
        await settleCaTrackPiece(zcql, caTrackKey, caSubId, "FAILURE");
        context.closeWithFailure();
        return;
      }
    }

    console.log(`[DemergerFn] Done in ${Date.now() - startedAt}ms`);
    const finalMerged =
      applyResult.trackingFailures > 0 ? "FAILED" : "COMPLETED";
    await setJobStatus(zcql, jobName, finalMerged);
    // The rebuild jobs above carry their own pieces now — release the
    // placeholder so the parent settles once those pieces finish (or right
    // away if there were no accounts to rebuild).
    await settleCaTrackPiece(
      zcql,
      caTrackKey,
      caSubId,
      finalMerged === "FAILED" ? "FAILURE" : "SUCCESS",
    );
    context.closeWithSuccess();
  } catch (error) {
    console.error("[DemergerFn] FATAL:", error);
    await setJobStatus(zcql, jobName, "FAILED");
    try {
      await settleCaTrackPiece(zcql, caTrackKey, caSubId, "FAILURE");
    } catch (trackErr) {
      console.error("[DemergerFn] settle ca-track piece failed:", trackErr);
    }
    context.closeWithFailure();
  }
};
