"use strict";

/**
 * FAHoldingsGenerator — builds the FA holdings snapshot for Holding Comparison.
 *
 * Params: asOnDate (yyyy-mm-dd), generationId (ms of the generate request),
 *         resume ("true" on a hand-off invocation)
 *
 * Writes temporary-files/FA-Holdings/<date>/fa-holdings-<ms>.csv with one row per
 * actual account code + ISIN, holdings summed across the account's virtual codes.
 *
 * The holdings are worked out exactly as the Reports consolidated "Export All"
 * job does (ExportAllCustomerHoldingData): read each virtual code's Holdings rows
 * up to the date and re-derive the as-on-date quantity. It skips what the
 * comparison does not use — prices (Bhav_Copy) and per-account security lookups —
 * and fetches security names once, in bulk, at the end. That removes most of the
 * queries the export runs, so generation takes a fraction of the time.
 *
 * holdingsFromTable.js, allAccountCodes.js and zcqlTimeout.js are straight copies
 * from functions/ExportAllCustomerHoldingData/. Re-copy them when those change so
 * both jobs keep computing the same holdings.
 *
 * A Catalyst job stops after 15 minutes. When time runs low the progress is saved
 * to FA-Holdings-Work/<date>.json and the job re-submits itself with resume=true.
 *
 * Progress is tracked on the Jobs row HCFA_<date>: RUNNING -> COMPLETED | FAILED.
 *
 * ./holdingComparison/ is a straight copy of appsail-nodejs/util/holdingComparison/.
 * Re-copy after changing it:
 *
 *   cp appsail-nodejs/util/holdingComparison/*.js functions/FAHoldingsGenerator/holdingComparison/
 */

const catalyst = require("zcatalyst-sdk-node");
const {
  fetchHoldingsRowsPaged,
  fetchSecurityListByIsins,
  rollupLastSnapshotByIsin,
} = require("./holdingsFromTable.js");
const { getAccountActualMapFromDatabase } = require("./allAccountCodes.js");

/** Hand off to a new invocation this long before the 15-minute job limit. */
const TIME_BUFFER_MS = 90_000;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** Retry a query-heavy step a few times; ZCQL calls fail or time out now and then. */
const withRetry = async (fn, label, attempts = 3) => {
  for (let attempt = 1; ; attempt++) {
    try {
      return await fn();
    } catch (err) {
      if (attempt >= attempts) throw new Error(`${label}: ${err.message}`);
      console.warn(`Retry ${attempt} for ${label}: ${err.message}`);
      await sleep(500 * attempt);
    }
  }
};

const csvCell = (v) => `"${String(v ?? "").replace(/"/g, '""')}"`;

// Decimals are stored in binary, so adding holdings leaves leftovers
// (10.1 + 20.2 = 30.299999999999997). Rounding to 6 decimals keeps the total
// equal to a hand calculation (same rule as the FIFO engine). `=== 0` turns -0 into 0.
const roundQty = (n) => {
  const v = Math.round((Number(n) || 0) * 1e6) / 1e6;
  return v === 0 ? 0 : v;
};

/** One actual account code: holdings per ISIN, summed across its virtual codes. */
const holdingsForGroup = async (zcql, group, asOnDate) => {
  const byIsin = new Map();
  for (const virtualCode of group.virtualCodes) {
    const rows = await withRetry(
      () => fetchHoldingsRowsPaged(zcql, virtualCode, asOnDate, ""),
      `virtual code ${virtualCode} (group ${group.actualCode})`,
    );
    // Saved HOLDING as it is, the same figure the holdings screen shows.
    for (const { isin, lastRow } of rollupLastSnapshotByIsin(rows)) {
      byIsin.set(isin, roundQty((byIsin.get(isin) || 0) + (Number(lastRow.HOLDING) || 0)));
    }
  }
  return [...byIsin].map(([isin, holding]) => [group.actualCode, isin, holding]);
};

module.exports = async (jobRequest, context) => {
  const app = catalyst.initialize(context, { scope: "admin" });
  const HC = await import("./holdingComparison/index.js");
  const zcql = app.zcql();
  const bucket = HC.getBucket(app);

  const params = jobRequest.getAllJobParams() || {};
  const asOnDate = String(params.asOnDate || "").trim();
  const generationId = String(params.generationId || "").trim();
  if (!HC.DATE_PATTERN.test(asOnDate) || !generationId) {
    console.error(`FAHoldingsGenerator: bad params asOnDate="${asOnDate}" generationId="${generationId}"`);
    context.closeWithFailure();
    return;
  }

  const jobName = HC.faJobName(asOnDate);
  const workKey = HC.buildFaWorkKey(asOnDate);

  const setStatus = async (status) => {
    const table = app.datastore().table("Jobs");
    const rows = await zcql.executeZCQLQuery(`SELECT ROWID FROM Jobs WHERE jobName = '${jobName}' LIMIT 1`);
    const rowId = rows?.[0]?.Jobs?.ROWID;
    if (rowId) await table.updateRow({ ROWID: rowId, status });
    else await table.insertRow({ jobName, status });
  };

  try {
    let progress;
    if (params.resume === "true") {
      // No progress file means this chain already finished (a retry after the
      // file was deleted), so there is nothing left to do.
      if (!(await bucket.headObject(workKey, { throwErr: false }))) {
        console.log(`FAHoldingsGenerator ${asOnDate}: no saved progress for generation ${generationId}; nothing to resume.`);
        context.closeWithSuccess();
        return;
      }
      progress = JSON.parse((await HC.getBuffer(bucket, workKey)).toString("utf8"));
      // A newer Generate request has started over; this older chain stops here.
      if (progress.generationId !== generationId) {
        console.log(`FAHoldingsGenerator: generation ${generationId} replaced by ${progress.generationId}; stopping.`);
        context.closeWithSuccess();
        return;
      }
    } else {
      progress = {
        generationId,
        groups: await withRetry(() => getAccountActualMapFromDatabase(zcql, "clientIds"), "client list"),
        cursor: 0,
        holdings: [],
      };
    }

    // Also refreshes MODIFIEDTIME on every invocation, which the status check
    // uses to tell a slow generation from an abandoned one.
    await setStatus(HC.FA_STATUS.RUNNING);
    console.log(
      `FAHoldingsGenerator ${asOnDate}: ${params.resume === "true" ? "resumed" : "started"} at group ` +
        `${progress.cursor}/${progress.groups.length}`,
    );

    while (progress.cursor < progress.groups.length) {
      if (context.getRemainingExecutionTimeMs() < TIME_BUFFER_MS) {
        await HC.putText(bucket, workKey, JSON.stringify(progress), "application/json");
        await app.jobScheduling().JOB.submitJob({
          job_name: `HCFA_${Date.now()}`.slice(0, 20),
          jobpool_name: HC.JOB_POOL,
          target_name: HC.FA_GENERATOR_TARGET,
          target_type: "Function",
          // Same retries as the first run: a retried resume re-reads the saved progress.
          job_config: { number_of_retries: 2, retry_interval: 60 * 1000 },
          params: { asOnDate, generationId, resume: "true" },
        });
        console.log(`FAHoldingsGenerator ${asOnDate}: handed off at group ${progress.cursor}/${progress.groups.length}`);
        context.closeWithSuccess();
        return;
      }

      // A group that still fails after retries fails the whole generation: a
      // snapshot silently missing an account would report false differences.
      const group = progress.groups[progress.cursor];
      progress.holdings.push(...(await holdingsForGroup(zcql, group, asOnDate)));
      progress.cursor += 1;
    }

    const isins = [...new Set(progress.holdings.map(([, isin]) => isin))];
    const securities = await withRetry(() => fetchSecurityListByIsins(zcql, isins), "security names");

    const generatedAt =
      new Date(Date.now() + 5.5 * 60 * 60 * 1000).toISOString().replace("T", " ").slice(0, 19) + " IST";
    const lines = [HC.FA_HEADERS.join(",")];
    for (const [accountCode, isin, holding] of progress.holdings) {
      const security = securities[isin] || {};
      lines.push(
        [generatedAt, asOnDate, accountCode, security.securityName || isin, security.securityCode || "", isin, holding]
          .map(csvCell)
          .join(","),
      );
    }

    const key = HC.buildFaKey(asOnDate, Date.now());
    await HC.putText(bucket, key, `${lines.join("\n")}\n`);

    // New FA holdings mean earlier comparisons for this date no longer describe them.
    const { superseded } = await HC.supersedeRuns(app, asOnDate);
    await setStatus(HC.FA_STATUS.COMPLETED);

    if (params.resume === "true") {
      await bucket.deleteObject(workKey).catch((err) => console.warn(`Could not delete ${workKey}:`, err.message));
    }

    console.log(
      `FAHoldingsGenerator ${asOnDate}: ${progress.groups.length} group(s), ${progress.holdings.length} holding(s) -> ` +
        `${key}, ${superseded} run(s) superseded`,
    );
    context.closeWithSuccess();
  } catch (error) {
    console.error("FAHoldingsGenerator failed:", error?.message || error);
    try {
      await setStatus(HC.FA_STATUS.FAILED);
    } catch (statusErr) {
      console.error("FAHoldingsGenerator: could not record the failure:", statusErr?.message || statusErr);
    }
    context.closeWithFailure();
  }
};
