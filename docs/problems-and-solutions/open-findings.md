# Open Findings

Every item is **[FOUND]**: identified by reading code at commit `25dfb9d`. **None is a recorded incident.** Line numbers refer to that commit.

Severity is a judgement call to help prioritise, not a measured risk.

---

## HIGH: security and data integrity

### F1. No authentication or authorization on any API route

- **Where:** `appsail-nodejs/index.js`. No auth middleware exists. The only "Authorization" match in the code is the CORS `allowedHeaders` list. No route checks a user, role or account ownership.
- **Threat:** anyone who can reach the AppSail URL can read any client's holdings and cash, start corporate-action applies, delete custody files, or import transactions (broken access control / IDOR).
- **Mitigating factors:** CORS allows only `localhost` origins, but CORS doesn't stop non-browser callers. Whether Catalyst platform settings protect the deployed URL is **Needs developer confirmation**.
- **Recommendation [PROPOSED]:** platform authentication or a token check on every `/api` route, plus role checks for mutating routes.

### F2. SQL (ZCQL) injection from request values

| File:line | Value | Source | Escaped? |
|---|---|---|---|
| `controller/analytics/tabs/holding/CashController.js:34` | `accountCode` | `req.query`, via `GET /api/analytics/getCashBalanceSummary` | No |
| `controller/uploader/DividendUploader.js:297` | `isin` | `req.body` (line 221) | No |
| `controller/uploader/DividendUploader.js:762` | `isin` | `req.body` (line 703 destructure) | No |
| `controller/uploader/DividendUploader.js:777, 884` | `jobName` built from `isin` | derived from `req.body` | No |
| `controller/uploader/DividendUploader.js:868` | `jobName` | `req.query` (line 862) | No |

- **Context:** the same file uses an `escSql` helper for other queries (around lines 43–98), and `runStore.js` escapes quotes. The problem is inconsistency.
- **Threat:** a crafted value can change the query's WHERE clause.
- **Recommendation [PROPOSED]:** route every interpolated value through one escape helper, and validate format (ISIN `^[A-Z]{2}[A-Z0-9]{9}[0-9]$`, account code pattern) at the boundary.

### F3. The custody-match gate can be bypassed through the old upload route

- **Where:** `router/uploaderRouter/TempTransactionUploaderRouter.js` still mounts `POST /api/transaction-uploader/upload-temp-file`. The UI removed the button (`TempTransaction.js` comment), but the route calls the same importer without any custody run check.
- **Threat:** unreconciled imports. Combined with F1, this needs no credentials.
- **Recommendation [PROPOSED]:** remove the route, or require a valid custody run id.

### F4. Gate 2 rule contradicts its own documentation

- `CustodyMatchRun.js` header says: Gate 2 requires `COMPLETED_CLEAN AND exceptionCount === 0`.
- `TempTransaction.js` comment says: "nothing reaches production until custody matching comes back clean".
- `proceedToImport` actually **allows import with open exceptions**, and records the count.
- **Needs developer confirmation** of the intended business rule.

### F5. Double-import guard is not atomic

- **Where:** `proceedToImport` calls `findImportedRunForDate`, then `setStageDetail(IMPORTED_MARKER)`. The code comment calls it a "soft guard".
- **Risk:** two proceed requests for different runs on the same business date, arriving together, could both pass the check and import the file twice.
- **Recommendation [PROPOSED]:** a uniqueness guarantee per business date (a dedicated row with a unique column), checked and written in one step.

### F6. Bulk-callback endpoint trusts its caller

- **Where:** `TempTransactionUpload.js:986` `handleBulkCallback`, route `POST /api/transaction-uploader/bulk-callback`.
- **Behaviour:** it responds 200 immediately, then acts on `status`, `pipelineJobName`, `bucketName`, `objectKey` and similar fields taken from the body and query. There's no signature or shared-secret check.
- **Risk:** a forged callback could fail or advance an upload pipeline.
- **Recommendation [PROPOSED]:** verify a secret parameter set when the bulk job is created.
- Whether Catalyst supports signed callbacks is **Needs developer confirmation**.

### F7. Job-name collisions from truncated ISINs

- **Where:** `BonusController.js:314` `BON_${isin.slice(-6)}_${exDate}`, `DividendUploader.js:774` `DIV_${isin.slice(-6)}_${recordDate}`, `SplitController.js:441` `BONRC_${isin.slice(-6)}_…`.
- **Evidence of real risk:** in the committed data file, 55 groups of distinct ISINs share their last 6 characters (424 distinct ISINs).
- **Risk:** two different securities with the same record/ex-date map to the same `Jobs` row, so status or duplicate checks can mix them up. The BONRC key also has a timestamp, which lowers its risk.
- **Recommendation [PROPOSED]:** use the full ISIN, which the dividend export already does (`DIV_${isin}_${ts}`).

## MEDIUM: reliability and correctness

### F8. Cash passbook rebuild has a hard-coded end date

- **Where:** `functions/Cal_CB_Per_TNX/index.js:17` `GLOBAL_END = "2026-12-31"`. It's used by `rebuildCashPassbookForAccount` (around line 355) to generate date chunks.
- **Risk:** transactions dated after 2026-12-31 are not included in a full passbook rebuild. Whether other cash paths cover them is **Needs developer confirmation**.

### F9. Cash rebuild is delete-then-insert without a transaction

- **Where:** `rebuildCashPassbookForAccount` deletes all passbook rows, verifies they're gone, then inserts chunk by chunk.
- **Risk:** a crash mid-rebuild leaves an account with a partial passbook until it is re-run.

### F10. `TDI` and `TDO` are both in `CASH_SUBTRACT`

- **Where:** `Cal_CB_Per_TNX/index.js:46–47`.
- If TDI means an inward transfer, subtracting it would be wrong. The business meaning of these codes is **Needs developer confirmation**.

### F11. No upload size limits on two routers

- `TempTransactionUploaderRouter.js` and `BhavUploaderRouter.js` call `fileUpload()` with no `limits`.
- AppSail has 256 MB memory, so a very large file is read fully into memory.
- The custody-match router shows the intended pattern: 25 MB with `abortOnLimit`.

### F12. Duplicated FIFO/holdings logic

- At least 7 files named `*fifo*`, plus `holdingsFromTable.js` and `UpdateBonusTable/holdingsSnapshot.js`.
- Several recent fixes (`ed934d4`, `49b8f7f`, `beeed58`, `38b1a2b`) were caused by copies disagreeing.

### F13. Vendored custody-match module is copied manually

- Three copies (AppSail plus two functions) are identical at `25dfb9d`. Nothing enforces that they stay that way.

### F14. Temporary holdings bridge still in place

- `recomputeHoldingsSnapshot` is marked TEMPORARY in `ce22219`, pending a `RebuildHoldingtable` re-run.
- `P_L` is not re-derived.
- Whether the re-run has happened is **Needs developer confirmation**.

## LOW: operability and hygiene

- **F15.** Almost no automated tests: one CRA placeholder test, no backend tests.
- **F16.** No request IDs, metrics or alerts. Logging is `console.*`. `handleBulkCallback` logs the full raw body.
- **F17.** Committed `BASE_URL` points at `http://localhost:3001/api` (`react-app/src/constant.js`). Needs developer confirmation of how deployed builds get the real URL.
- **F18.** Custody `startedBy` comes from the request body or `x-user` header and is spoofable. This only matters once F1 is fixed.
- **F19.** The notification tray is in-memory, so a reload loses tracked jobs.
- **F20.** `catalyst.json` lists two targets with no directory (`HoldingdataUpdate`, `DailyClientHoldingTransaction`).
- **F21.** The root `README.md` is stale (see [docs index](../README.md)).
- **F22.** `CustodyMatchCleanup` does nothing until `CLEANUP_DRY_RUN=false` is set. That's by design, but working data grows until then.

## Previously reported, not re-verified in this pass

These appear in earlier private notes but weren't re-checked against code in this run. They're **not** asserted here:

- `MegerFn` retry re-inserting rows
- `CalculateHoldingWorkers` lacking a backdated-transaction guard
- dividend apply ignoring merger/demerger lots
