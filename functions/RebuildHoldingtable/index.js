"use strict";

const catalyst = require("zcatalyst-sdk-node");
const {
  rebuildHoldingsForAccountList,
} = require("./holdingsRebuildFromSources.js");

const esc = (v) => String(v ?? "").replace(/'/g, "''");

/**
 * Only relevant when this job was dispatched by UpdateISINWorker's rebuild
 * phase (params.parentStatusKey / params.subTaskKey present — other callers,
 * e.g. Merger/Demerger flows, never set these and this is a no-op for them).
 *
 * Marks this chunk's own tracking row in `Jobs` with the outcome, then — if
 * no sibling chunk row is still RUNNING — finalizes the PARENT `Jobs` row
 * (statusKey) as SUCCESS, or FAILURE if any sibling chunk failed. This is
 * what makes the ISIN rename's status only report done once the Holdings
 * rebuild it triggered has actually finished, not just been queued.
 *
 * Deliberately avoids `LIKE '<prefix>%'` — ZCQL does not support `%`
 * wildcards (it silently matches zero rows, not an error), which would make
 * this always think "no siblings" and finalize on the very first chunk to
 * finish. Instead it reads the exact chunk count UpdateISINWorker recorded
 * ("<parentStatusKey>_stotal") and checks all sibling rows by exact name via
 * `IN (...)`.
 */
async function finalizeSubtaskIfNeeded(zcql, parentStatusKey, subTaskKey, outcome) {
  if (!subTaskKey) return;

  try {
    await zcql.executeZCQLQuery(
      `UPDATE Jobs SET status = '${esc(outcome)}' WHERE jobName = '${esc(subTaskKey)}'`,
    );
  } catch (e) {
    console.warn(`[RebuildHoldingtable] mark subtask '${subTaskKey}' ${outcome} failed:`, e.message);
  }

  if (!parentStatusKey) return;

  try {
    const totalRows = await zcql.executeZCQLQuery(
      `SELECT status FROM Jobs WHERE jobName = '${esc(parentStatusKey)}_stotal' LIMIT 1`,
    );
    const total = Number(totalRows?.[0]?.Jobs?.status ?? totalRows?.[0]?.status ?? 0);
    if (!(total > 0)) {
      // Total row missing/unreadable — can't safely tell if siblings remain.
      // Leave the parent as-is rather than guessing; it'll be re-checked the
      // next time any sibling chunk finishes.
      console.warn(`[RebuildHoldingtable] no valid total for '${parentStatusKey}', skipping finalize`);
      return;
    }

    const siblingNames = [];
    for (let i = 0; i < total; i++) {
      siblingNames.push(`'${esc(parentStatusKey)}_s${i}'`);
    }

    const siblingRows = await zcql.executeZCQLQuery(
      `SELECT status FROM Jobs WHERE jobName IN (${siblingNames.join(",")})`,
    );
    const statuses = (siblingRows || []).map((r) =>
      String((r?.Jobs ?? r)?.status ?? "").toUpperCase(),
    );

    if (statuses.length < total || statuses.some((s) => s === "RUNNING")) {
      return; // a sibling row is missing (not yet inserted) or still running
    }

    const parentStatus = statuses.some((s) => s === "FAILURE") ? "FAILURE" : "SUCCESS";

    await zcql.executeZCQLQuery(
      `UPDATE Jobs SET status = '${esc(parentStatus)}' WHERE jobName = '${esc(parentStatusKey)}'`,
    );
    console.log(`[RebuildHoldingtable] parent '${parentStatusKey}' finalized as ${parentStatus}`);
  } catch (e) {
    console.warn(`[RebuildHoldingtable] finalize parent '${parentStatusKey}' failed:`, e.message);
  }
}

/**
 * Split "Confirm & Apply" progress tracking (params.caTrackKey / caSubId,
 * set by SplitController and by UpdateBonusTable's rebuild fan-out — no-op for
 * every other caller).
 *
 * Flips this chunk's piece row in `JobStatusPerAccount` (jobName = trackKey,
 * WS_Account_code = subId) to SUCCESS / FAILED, then — if no piece row for this
 * trackKey is still RUNNING — settles the parent `Jobs` row named with the
 * trackKey to COMPLETED (or COMPLETED_WITH_ERRORS if any piece FAILED). This is
 * what lets the UI keep the split notification "in progress" until the holdings
 * rebuild AND any bonus-recompute fan-out have all actually finished.
 *
 * Uses an exact-match `jobName = '<key>'` filter (many rows per key) — no `LIKE`
 * prefix scan, which ZCQL cannot do.
 */
async function finalizeCaTrackIfNeeded(zcql, trackKey, subId, outcome) {
  if (!trackKey || !subId) return;
  const pieceStatus = outcome === "SUCCESS" ? "SUCCESS" : "FAILED";

  try {
    await zcql.executeZCQLQuery(
      `UPDATE JobStatusPerAccount SET status = '${esc(pieceStatus)}', ` +
        `lastError = '${pieceStatus === "FAILED" ? "rebuild failed" : ""}' ` +
        `WHERE jobName = '${esc(trackKey)}' AND WS_Account_code = '${esc(subId)}'`,
    );
  } catch (e) {
    console.warn(`[RebuildHoldingtable] split-track mark '${subId}' ${pieceStatus} failed:`, e.message);
    return;
  }

  try {
    const running = await zcql.executeZCQLQuery(
      `SELECT status FROM JobStatusPerAccount WHERE jobName = '${esc(trackKey)}' AND status = 'RUNNING' LIMIT 1`,
    );
    if (running && running.length) return; // pieces still in flight

    const failed = await zcql.executeZCQLQuery(
      `SELECT status FROM JobStatusPerAccount WHERE jobName = '${esc(trackKey)}' AND status = 'FAILED' LIMIT 1`,
    );
    const parentStatus = failed && failed.length ? "COMPLETED_WITH_ERRORS" : "COMPLETED";

    await zcql.executeZCQLQuery(
      `UPDATE Jobs SET status = '${parentStatus}' WHERE jobName = '${esc(trackKey)}'`,
    );
    console.log(`[RebuildHoldingtable] split track '${trackKey}' finalized as ${parentStatus}`);
  } catch (e) {
    console.warn(`[RebuildHoldingtable] split-track finalize '${trackKey}' failed:`, e.message);
  }
}

/**
 * Catalyst job — rebuild `Holdings` from Transaction / Bonus / Split / Demerger / Merger.
 *
 * Params:
 * - accountCodesJson: JSON array string of WS_Account_code (preferred)
 * - accountCodes: comma-separated fallback
 * - asOnDate: optional YYYY-MM-DD cutoff
 * - source: optional tag for logs (e.g. DemergerFn, MegerFn)
 * - keepCaRecords: "true" rebuilds Holdings only and leaves Split_Account
 *   untouched. Sent by Transaction Deletion, which must not change any
 *   corporate action record.
 * - parentStatusKey / subTaskKey: set only when dispatched by UpdateISINWorker;
 *   used to report this chunk's outcome back so the ISIN rename's status
 *   reflects the rebuild actually finishing (see finalizeSubtaskIfNeeded above).
 *
 * @param {import("./types/job").JobRequest} jobRequest
 * @param {import("./types/job").Context} context
 */
module.exports = async (jobRequest, context) => {
  const catalystApp = catalyst.initialize(context);
  const zcql = catalystApp.zcql();
  let parentStatusKey = "";
  let subTaskKey = "";
  // Split "Confirm & Apply" notification tracking (see finalizeCaTrackIfNeeded).
  let caTrackKey = "";
  let caSubId = "";

  try {
    const params = jobRequest.getAllJobParams();
    const source = String(params.source || "").trim() || "RebuildHoldingtable";
    const asOnDateRaw = String(params.asOnDate || "").trim();
    const asOnDate =
      /^\d{4}-\d{2}-\d{2}$/.test(asOnDateRaw) ? asOnDateRaw : null;
    parentStatusKey = String(params.parentStatusKey || "").trim();
    subTaskKey = String(params.subTaskKey || "").trim();
    caTrackKey = String(params.caTrackKey || "").trim();
    caSubId = String(params.caSubId || "").trim();

    let accounts = [];

    const jsonParam = params.accountCodesJson;
    if (jsonParam) {
      try {
        const parsed = JSON.parse(jsonParam);
        if (Array.isArray(parsed)) {
          accounts = parsed.map((a) => String(a || "").trim()).filter(Boolean);
        }
      } catch (e) {
        console.error("[RebuildHoldingtable] Invalid accountCodesJson:", e.message);
        await finalizeSubtaskIfNeeded(zcql, parentStatusKey, subTaskKey, "FAILURE");
        await finalizeCaTrackIfNeeded(zcql, caTrackKey, caSubId, "FAILURE");
        context.closeWithFailure();
        return;
      }
    }

    if (!accounts.length && params.accountCodes) {
      accounts = String(params.accountCodes)
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
    }

    // Optional ISIN scope: when present, rebuild ONLY these ISIN(s) for each
    // account instead of the account's full ISIN set. Absent → full rebuild.
    let scopedIsins = [];
    if (params.isinsJson) {
      try {
        const parsed = JSON.parse(params.isinsJson);
        if (Array.isArray(parsed)) {
          scopedIsins = parsed.map((i) => String(i || "").trim()).filter(Boolean);
        }
      } catch (e) {
        console.error("[RebuildHoldingtable] Invalid isinsJson:", e.message);
      }
    }
    if (!scopedIsins.length && params.isin) {
      const single = String(params.isin).trim();
      if (single) scopedIsins = [single];
    }
    const scopedIsinsSet = scopedIsins.length ? new Set(scopedIsins) : null;
    const keepCaRecords = String(params.keepCaRecords || "").trim() === "true";

    const uniq = [...new Set(accounts)];
    console.log("[RebuildHoldingtable] Start", {
      source,
      accountCount: uniq.length,
      asOnDate,
      scopedIsins: scopedIsinsSet ? scopedIsins : "off",
      keepCaRecords,
    });

    if (!uniq.length) {
      console.error("[RebuildHoldingtable] No account codes in params");
      await finalizeSubtaskIfNeeded(zcql, parentStatusKey, subTaskKey, "FAILURE");
      await finalizeCaTrackIfNeeded(zcql, caTrackKey, caSubId, "FAILURE");
      context.closeWithFailure();
      return;
    }

    const counters = await rebuildHoldingsForAccountList(
      zcql,
      uniq,
      asOnDate,
      scopedIsinsSet,
      keepCaRecords,
    );

    if (counters.errors > 0) {
      console.error("[RebuildHoldingtable] Completed with errors", counters);
      await finalizeSubtaskIfNeeded(zcql, parentStatusKey, subTaskKey, "FAILURE");
      await finalizeCaTrackIfNeeded(zcql, caTrackKey, caSubId, "FAILURE");
      context.closeWithFailure();
      return;
    }

    console.log("[RebuildHoldingtable] Success", counters);
    await finalizeSubtaskIfNeeded(zcql, parentStatusKey, subTaskKey, "SUCCESS");
    await finalizeCaTrackIfNeeded(zcql, caTrackKey, caSubId, "SUCCESS");
    context.closeWithSuccess();
  } catch (err) {
    console.error("[RebuildHoldingtable] FATAL:", err);
    await finalizeSubtaskIfNeeded(zcql, parentStatusKey, subTaskKey, "FAILURE");
    await finalizeCaTrackIfNeeded(zcql, caTrackKey, caSubId, "FAILURE");
    context.closeWithFailure();
  }
};
