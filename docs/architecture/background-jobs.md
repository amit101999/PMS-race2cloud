# Background Jobs, Long-Running Work and Progress Tracking

Status: **[BUILT]** at commit `25dfb9d` unless labelled otherwise.

This file explains how PMS runs work that is too long or heavy for an HTTP request, and how the UI learns when that work has finished.

---

## 1. Why background jobs exist here

- The API is an Express app on Catalyst AppSail (`appsail-nodejs`). Holdings rebuilds, corporate-action applies, bulk exports, ISIN renames and custody matching all touch many rows. They don't fit inside a request.
- The work is pushed to **Catalyst job functions** in `functions/` with `catalystApp.jobScheduling().JOB.submitJob({ jobpool_name, target_name, params, job_config })`.
- Job functions have a **15-minute execution wall**. This is referenced in code comments in `CustodyMatchRunner`, `CustodyMatchCleanup` and `ExportAllCustomerHoldingData`. Most of the patterns below exist to survive that limit.
- `catalyst.json` declares 35 function targets. Of the `catalyst-config.json` files present: 26 are `job`, 3 `event`, 1 `cron`.

## 2. Patterns used to survive the 15-minute wall

### 2.1 Baton-pass with dispatch state: `CalculateHoldingMaster`

- It dispatches `CalculateHoldingWorkers` jobs, with at most `MAX_IN_FLIGHT = 10` running at once.
- When its own time budget is nearly used up (`context.getRemainingExecutionTimeMs()`), it saves dispatch state to Stratus (`saveDispatchState`) and hands off to a fresh invocation. The code calls this a "baton-pass".
- If `submitJob` hits a concurrency limit, it retries up to 12 times, 8 seconds apart.

### 2.2 Manifest + checkpoint + multipart upload: `ExportAllCustomerHoldingData`

From `functions/ExportAllCustomerHoldingData/checkpoint.js`. Per job name (e.g. `EA_2026-07-21`) it stores three objects under `exports-meta/`:

| Object | Purpose |
|---|---|
| `<jobName>-manifest.json` | Client work list, snapshotted once, so a changing `clientIds` table can't shift indexes mid-run |
| `<jobName>-checkpoint.json` | Cursor into the manifest, multipart upload state (`uploadId`, `nextPartNumber`), price cache |
| `<jobName>-pending.csv` | CSV bytes not yet large enough (under 5 MB) to flush as a multipart part |

### 2.3 Stage checkpoints: `CustodyMatchRunner`

Each stage writes its checkpoint **before** it starts, and every stage is safe to repeat. See [custody-matching.md](custody-matching.md#6-the-job-pipeline-custodymatchrunner).

### 2.4 Stop early, continue next run: `CustodyMatchCleanup`

It stops 60 seconds before the wall, and the next scheduled run picks up the remaining work.

## 3. Guarding against a ZCQL call that never returns

**[FIXED `76b88f6`]** "optimized reports", kajal kharbanda, 2026-09-02.

- **Problem** (documented in `zcqlTimeout.js`): the Catalyst ZCQL client "has been observed to hang indefinitely on some calls instead of rejecting". A hung `await` never throws, so the retry logic never ran. The job just hit its 15-minute timeout with no further logs.
- **Fix:** `queryWithTimeout(zcql, sql, 20_000)` races each query against a 20-second timer. A silent hang becomes a normal rejection that the existing retry and baton-pass logic can handle.
- **Tradeoff:** a slow but valid query over 20 seconds is treated as a failure. The rejected query also isn't cancelled on the server; the code only stops waiting for it.
- **Scope:** `functions/ExportAllCustomerHoldingData/` only. Other jobs don't use this helper (verified by the file location; no shared copy exists elsewhere).

## 4. Re-running the same export on the same day

- **[FIXED `1e6baf0`]** Same-date re-export failed on the second attempt. The fix confirms the Stratus delete of the old file before regenerating.
- **[BUILT `424e23b`]** If a `Jobs` row for the same job name is `COMPLETED` *today* (IST, compared by date only) and the file still exists (`headObject`), the export reuses it and skips the rebuild. If the row says completed but the file is missing, it logs a warning and regenerates.
- **Tradeoff:** transactions uploaded after the first export on the same day won't appear in a same-day repeat request. Whether that is acceptable is **Needs developer confirmation**.

## 5. Tracking progress of corporate-action applies

**[BUILT `d1f92de`, extended in `034cf73`]** by kajal kharbanda, in `appsail-nodejs/util/caJobProgress.js`.

Split, bonus, merger and demerger applies fan out into background `RebuildHoldingtable` work. Progress is tracked **without a new table**:

```text
Jobs row (jobName = track key)            RUNNING → COMPLETED | COMPLETED_WITH_ERRORS | ERROR
JobStatusPerAccount rows (jobName = key)  one per background piece: RUNNING → SUCCESS | FAILED
```

- Each background piece inserts a `RUNNING` row, then flips it to `SUCCESS` or `FAILED`.
- **Whichever piece finishes last** settles the parent row.
- **Self-settling read.** `readCaJobProgress` also finalises the parent if no piece is still `RUNNING`. The code calls this "belt and suspenders for a lost finalize race".
- **Stale guard.** A parent still `RUNNING` after 1 hour (`STALE_MS`) is force-marked `ERROR`.
- **Exact-match lookups only.** The comment says look-ups use `jobName = '...'`, "never a LIKE prefix scan, which ZCQL cannot do". Related: `3105e0e` fixed "a ZCQL LIKE '%' bug that broke status tracking" in Update ISIN. `exportDividendFile.js` still uses `LIKE 'DIV_<isin>_*'`; whether that works as intended is **Needs developer confirmation**.
- **Tradeoff.** Reusing existing tables avoids a schema change. The cost is that job state is spread across two generic tables and is only correct if every piece reliably writes its final status. The 1-hour stale rule then turns a slow-but-healthy job into `ERROR`.

See [ADR-002](../decisions/ADR-002-job-progress-tracking-without-new-table.md).

## 6. The notification bell (frontend)

**[BUILT `3105e0e`, extended `d736512`, `d1f92de`, `9799b03`, `034cf73`]**, in `react-app/src/notifications/`.

- `JobNotificationProvider` wraps `HashRouter` in `App.js`, so tracking **survives route changes**.
- `trackJob({ id, label, statusUrl, parseStatus })` polls `statusUrl` every **5 seconds**. After **20 minutes** it shows `timeout` ("Still processing … Check back later.").
- **One failed poll doesn't end tracking.** It logs and keeps polling until the timeout.
- `reportJob(...)` pushes a one-shot entry for work that finishes synchronously.
- `parseStatus` adapts each backend's status vocabulary. The default handles ISIN `jobStatus` (`SUCCESSFUL`/`FAILURE`/`PARTIAL_FAILURE`). `parseReportStatus` handles Reports export `status` values (`COMPLETED`/`NO_DATA`/`FAILED`/`ERROR`).
- **Used by:** Update ISIN, Reports tabs (Holdings, Cash, Corporate Action, Transactions), Split, Bonus, Dividend, Merger, Demerger.

Limitations (**[FOUND]**, from reading the code):

- Tracked jobs live only in React state. **A page reload or new tab loses the tray**, although the background job itself keeps running.
- Polling is per job, every 5 seconds, per open browser tab. At current usage this is simple and adequate; the number of concurrent users is **Needs developer confirmation**.

## 7. Failure summary

| Dependency fails | Effect |
|---|---|
| `submitJob` rejected | Custody match returns 502. `CalculateHoldingMaster` retries on concurrency errors. Other callers vary. |
| Job crashes | Catalyst retries according to `job_config.number_of_retries`. Correctness depends on each job being idempotent (see open findings for jobs not verified as idempotent). |
| ZCQL hangs | Guarded only in `ExportAllCustomerHoldingData` |
| Status endpoint errors | Bell keeps polling until 20 minutes, then shows `timeout` |
| Piece never writes final status | Parent forced to `ERROR` after 1 hour |
