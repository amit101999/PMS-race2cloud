# Production Scenarios

> **Every scenario below is a HYPOTHETICAL production scenario.** None is a recorded incident. Where a scenario is based on a real past fix, that commit is cited as "related", but the scenario itself didn't happen. Don't present these as experiences.

Each scenario follows: **What would I check → Possible causes → Immediate action → Long-term fix.**

---

## 1. A corporate-action preview starts timing out again

**Related real fix:** `f5e409c`, `3ae14d1`, `780a500`.

- **Check:**
  - AppSail logs for the endpoint and ISIN.
  - How many holders the ISIN has.
  - Whether a recent commit reintroduced a per-account loop (for example in `mapVirtualToActualCodes.js` or `holdingsFromTable.js`).
- **Possible causes:** a new N+1 loop, a very widely held ISIN, slow ZCQL.
- **Immediate action:** revert the offending change, or use the export path instead of the preview.
- **Long term:** a code-review rule of "no query inside a per-account loop", and a timing log per preview.

## 2. The notification bell shows an apply stuck "in progress"

- **Check:**
  - The `Jobs` row for the track key.
  - `JobStatusPerAccount` rows still `RUNNING`.
  - Catalyst job logs for `RebuildHoldingtable`.
- **Possible causes:** a piece crashed before writing its final status, or the job pool is saturated.
- **Immediate action:** the status read force-settles to `ERROR` after 1 hour. Re-run the failed accounts.
- **Long term:** write the final status from a `finally` path, and alert on stale `RUNNING` rows.

## 3. A custody match run sits at stage S4 for a long time

- **Check:**
  - `Custody_Match_Run.stage`, `attemptCount` and `comparedKeyCount`.
  - Whether the job is retrying.
- **Possible causes:** a very large file; job functions without admin scope stalling (documented in code); repeated 15-minute timeouts.
- **Immediate action:** use `POST /run/:runId/retry` (system failures only). Split the dates using the date filter.
- **Long term:** record per-stage timings, and make S4 resume from its offset instead of restarting.

## 4. Transactions appear twice after an import

- **Check:**
  - Upload history for the date.
  - Custody runs with the "Imported into production" marker for that date.
  - Whether anyone called `POST /api/transaction-uploader/upload-temp-file` directly.
- **Possible causes:** the old direct route is still mounted; the double-import guard is check-then-set.
- **Immediate action:** stop further imports for the date, identify the duplicate batch, and remove it.
- **Long term:**
  - Remove or protect the direct route.
  - Add a uniqueness guarantee per business date or per transaction reference.
  - Add authentication.

## 5. Cash balances stop including new transactions after year end

**Related code fact:** `functions/Cal_CB_Per_TNX/index.js` rebuilds passbooks over date chunks from `GLOBAL_START = "2020-01-01"` to `GLOBAL_END = "2026-12-31"`.

- **Check:** whether a full passbook rebuild excludes rows dated after 2026-12-31.
- **Possible cause:** a hard-coded end date.
- **Immediate action:** raise `GLOBAL_END` and re-run the affected accounts.
- **Long term:** derive the end date from the data or from today.

## 6. Someone reads another client's holdings without logging in

- **Check:** AppSail access logs. Note that the API has no authentication middleware, so there's nothing to check on the application side.
- **Possible cause:** every `/api/*` route is unauthenticated. How the deployed URL is protected at the platform level is **Needs developer confirmation**.
- **Immediate action:** restrict network access or put the platform's authentication in front of the API.
- **Long term:** authentication on every route, plus per-user authorization.

## 7. A same-day export shows yesterday's data

- **Check:** the `Jobs` row `CREATEDTIME` for the export name, and when the transactions were uploaded.
- **Possible cause:** same-day reuse (`424e23b`) returned the file generated earlier today.
- **Immediate action:** delete the file or the job row, then re-export.
- **Long term:** include a data-version or last-upload timestamp in the reuse check.

## 8. "No such key" when matching right after deleting a file

- **Check:** the time between the delete and the match.
- **Cause:** documented in code: Stratus keeps listing a deleted object for about 20 seconds.
- **Action:** re-upload and match again. The code already turns this into a readable `FA_FILE_MISSING` message.

---

## General answer: "Production is broken. How do you investigate?"

1. **Scope:** which page or endpoint, which account or ISIN, since when.
2. **Recent change:** `git log` for commits touching that path. Many past bugs came from one copy of the FIFO or holdings logic drifting from another.
3. **Logs:** AppSail console logs and Catalyst job logs. Job functions log with prefixes like `[CustodyMatch <runId>]` and `[BulkCallback]`.
4. **State tables:** `Jobs`, `JobStatusPerAccount`, `Custody_Match_Run` show where background work stopped.
5. **Mitigate first:** revert, re-run or disable, then find the root cause.

**Honest gap:** there are no request IDs, metrics, dashboards or alerts in the repository. Logging is `console.log` / `console.error` only.
