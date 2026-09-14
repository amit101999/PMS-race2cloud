# Custody Matching (Transaction Upload gate)

Status: **[BUILT]**. It exists in code at commit `25dfb9d`. It was introduced in `f59a3cc` ("Transaction upload codebase", 2026-09-10, author identity `amit helios tech`).
Production rollout state: **Needs developer confirmation.** The repo can't show whether the `CustodyMatch` job pool exists, whether `CustodyMatchRunner` and `CustodyMatchCleanup` are deployed, or what cron schedule the cleanup uses.

---

## 1. What it does

Before a day's transaction file is imported into the `Transaction` table, the system compares it with the **custodian's deal-allocation files**. It then reports every disagreement and only imports through a server-side "proceed" call.

The code calls the broker/front-office file the **FA** file. What "FA" stands for is **Needs developer confirmation**.

```text
User uploads FA file + one custodian file per scheme
        │  (validated synchronously in AppSail, stored in Stratus)
        ▼
Gate 1  POST /api/custody-match/run           → queues CustodyMatchRunner job (202)
        ▼
CustodyMatchRunner  S1 → S2 → S3 → S4 → S5 → S6   (checkpointed in Custody_Match_Run)
        ▼
UI polls GET /api/custody-match/run/:runId, pages exceptions, can export CSV
        ▼
Gate 2  POST /api/custody-match/run/:runId/proceed → existing uploadTempTransaction pipeline
```

## 2. Where the code lives

| Piece | Path |
|---|---|
| Routes (`/api/custody-match/*`) | `appsail-nodejs/router/custodyMatchRouter/CustodyMatchRouter.js` |
| Upload / day / download handlers | `appsail-nodejs/controller/custodyMatch/CustodyMatchUpload.js` |
| Gates, run status, exceptions, retry | `appsail-nodejs/controller/custodyMatch/CustodyMatchRun.js` |
| Field-mapping profiles | `appsail-nodejs/controller/custodyMatch/CustodyMatchFieldMap.js` |
| Pure parse/compare module (no SDK) | `appsail-nodejs/util/custodyMatch/*` |
| Pipeline job | `functions/CustodyMatchRunner/index.js` (type `job`, node20) |
| Nightly sweep | `functions/CustodyMatchCleanup/index.js` (type `cron`, node20) |
| UI | `react-app/src/pages/TempTransactionUpload/TempTransaction.js` + `custody/*` |

## 3. Inputs

- **FA file.** Exactly 26 headers, checked for order, spelling and case (`FA_HEADERS` in `constants.js`). It mirrors `EXPECTED_HEADERS_IN_ORDER` in `TempTransactionUpload.js`.
- **Custodian format 1 (Resident).** 15 headers, in any order (`CUSTODY_F1_HEADERS`).
- **Custodian format 2 (NRE/NRO, "NDPMS").** 18 headers (`CUSTODY_F2_HEADERS`).
- The format is detected from the headers and cross-checked against the file-type tag. Each scheme gets one file.
- **Field-mapping profiles.** A user can map their own column names onto internal fields. Saving a profile supersedes stored runs.
- **Upload limit.** 25 MB with `abortOnLimit: true`. The code comment explains why: a truncated CSV "parses cleanly and reconciles wrongly", so rejecting the file is safer than silently truncating it.

## 4. The match rule

From `util/custodyMatch/compare.js`, labelled in the code as "THE RULE (user, explicit)":

```text
key            = Actual Code + ISIN + Transaction Date + Transaction Type
compared value = Quantity   (summed per key, unsigned)
```

- **Virtual account codes** are resolved to **actual codes** through `clientIds` before rows are keyed.
- **Quantity is unsigned** and the direction (BUY/SELL) is part of the key. The code explains that signing and netting would let "buy 1000 / sell 400" collide with "buy 400 / sell 1000".
- Rate, brokerage, STT, net amount, settlement date, contract number and similar fields are **carried onto the exception row for investigation only**. They never change the verdict.
- **Exception types:** `QUANTITY_MISMATCH`, `MISSING_IN_CUSTODY`, `MISSING_IN_FA`, `DATE_MISMATCH`, `TYPE_MISMATCH`, `UNMAPPED_CODE`.
- **Behaviour switches** (`DEFAULT_OPTIONS`):

  | Option | Default | What it does |
  |---|---|---|
  | `repairOrphans` | `true` | Collapses the two orphan rows a wrong date produces into one DATE/TYPE mismatch |
  | `unmappedAsException` | `true` | Reports a code missing from `clientIds` as an exception instead of failing the run |
  | `isinChecksum` | `true` | ISIN check digit, warning only |
  | `clientCodeFallback` | `false` | A wrong fallback code would create unexplainable mismatches |
  | `quantityEpsilon` | `1e-6` | Float tolerance |

- The key walk is **sorted**, so offset *n* means the same key on every attempt. That is what makes the S4 resume checkpoint valid.

## 5. Gate 1: start a run (`startRun`)

1. Validate `businessDate` as `yyyy-mm-dd` (400 otherwise).
2. `checkGateOne(day)` requires every custody scheme to be complete and a staged FA file. If not, it returns **409 `GATE_1_BLOCKED`** with a list of blockers.
3. **Reuse.** If a non-superseded run already covers exactly the same dates, return it (`reused: true`) instead of re-running.
4. Create a `Custody_Match_Run` row.
5. If specific dates were selected, write a `dates` JSON sidecar to Stratus **before** queuing. A retry arrives as a fresh job submission, so it has to read the filter from storage.
6. `submitJob` to pool `CustodyMatch`, target `CustodyMatchRunner`, with `number_of_retries: 3` and `retry_interval: 60s`. If the submit fails, the run is marked and the API returns **502 `JOB_SUBMIT_FAILED`**, so the screen doesn't poll a run that will never move.
7. Return **202 Accepted**.

## 6. The job pipeline (`CustodyMatchRunner`)

| Stage | Work | Why it's safe to repeat |
|---|---|---|
| S1 BUILD_FA | Re-read the raw FA file and write `final-<date>-FA.csv` | Fixed object key, so it overwrites |
| S2 BUILD_CUSTODY | Re-read the custodian files and write `final-<date>-Custodian.csv` | Fixed object key |
| S3 PERSIST | Verify both finals read back | Read-only |
| S4 COMPARE | Walk sorted keys in chunks of 2,000 (`COMPARE_CHUNK`) | Always restarts from zero, and progress is only a counter |
| S5 REPORT | Delete this run's exceptions, then rewrite them | Delete-then-insert |
| S6 DONE | Write terminal status and counts | |

Design points visible in code:

- **Checkpoint before work.** The stage is written to the run row *before* the stage starts, so after a crash `stage` points at the step that needs re-running.
- **Terminal runs are a no-op.** A redelivered job for a run that is `COMPLETED_*` or `SUPERSEDED` exits successfully without doing anything.
- **Time budget.** The job stops 45 seconds (`TIME_BUFFER_MS`) before the 15-minute job wall so the checkpoint always gets written.
- **Admin scope is required.** The comment says that without `catalyst.initialize(context, { scope: "admin" })`, Data Store calls stall for about 60 seconds instead of failing.
- **Persisted finals.** The two final CSVs are stored so S4/S5 can resume without redoing S1/S2, and so a disputed result can be reproduced from exactly what was compared.

## 7. Gate 2: import (`proceedToImport`)

What the handler actually refuses:

| Condition | Response |
|---|---|
| Run not found | 404 `RUN_NOT_FOUND` |
| Run `QUEUED` / `RUNNING` | 409 `RUN_NOT_FINISHED` |
| Run `SUPERSEDED` (files or mapping changed after it ran) | 409 `RUN_SUPERSEDED` |
| This run already imported | 409 `ALREADY_IMPORTED` |
| Another run already imported this business date | 409 `DATE_ALREADY_IMPORTED` |
| Raw FA file missing from bucket | 409 `FA_FILE_MISSING` |
| Custom FA mapping no longer parses | 409 `FA_RELAYOUT_FAILED` |

What it does **not** refuse: **open exceptions**. The in-code comment says: "Exceptions do NOT block this … an unreconciled import is allowed and recorded rather than refused." The run is stamped `Imported into production with N open exception(s)`.

How the import happens:

1. Re-read the **original** raw FA file from Stratus. If a custom mapping is in use, re-lay it out into the 26-column template.
2. Set the "imported" marker on the run **before** importing, as a soft guard against double clicks.
3. Build a **synthetic request** (`{ catalystApp, files: { file } }`) and call the existing `uploadTempTransaction(req, res)` unchanged. The existing pipeline's response goes back to the client as-is.
4. Wrap `res.status` to capture the status code. If the import returns 400 or higher, or throws, restore the previous marker so the run isn't locked out.

> **Inconsistency, Needs developer confirmation.** The header comment of `CustodyMatchRun.js` says Gate 2 requires `COMPLETED_CLEAN AND exceptionCount === 0`. The page comment in `TempTransaction.js` says "nothing reaches production until custody matching comes back clean". The implemented `proceedToImport` allows import with open exceptions. `isGateOpen(run)` (clean and zero exceptions) only drives the `canProceed` flag in the status response. Which rule is intended is not established from the repository.

## 8. Supersede: stale answers can't be reused

`supersedeRunsForDate` is called when a file is uploaded, a file is removed, or a day's files are deleted (`CustodyMatchUpload.js`, 4 call sites), and when a field mapping is saved (`CustodyMatchFieldMap.js`). A superseded run can be neither reused by Gate 1 nor imported by Gate 2.

## 9. Cleanup cron (`CustodyMatchCleanup`)

This function deletes data unattended, so the code is written to **fail safe**:

- **Dry run by default.** It only deletes when `CLEANUP_DRY_RUN` is exactly `"false"`.
- **Retention.** `CUSTODY_RETENTION_DAYS` defaults to 30, with a **hard floor of 7** so a typo can't wipe the current week.
- **Dates compared as `yyyy-mm-dd` strings**, not as `Date` objects, to avoid timezone slips. The comment notes that Catalyst stores timestamps in project time with no offset.
- **Skip active dates.** Any date with a `QUEUED` or `RUNNING` run is skipped regardless of age.
- **Order.** Objects are deleted first, then run and exception rows. A crash in between leaves rows pointing at missing files, which is visible, instead of files nobody can explain.
- **Time budget.** It stops 60 seconds before the wall, and the backlog continues on the next run.

Needs developer confirmation: the actual schedule, and whether dry run has been switched off in any environment.

## 10. Sharing code between AppSail and the job

- `util/custodyMatch/` is **pure logic**: no SDK, Data Store or Stratus inside the parse/compare core. The same code validates uploads synchronously and runs the pipeline in the job, so there's no second implementation to drift.
- It is **vendored by straight file copy** into `functions/CustodyMatchRunner/custodyMatch/` and `functions/CustodyMatchCleanup/custodyMatch/`. At `25dfb9d` the three copies are identical except for an extra `package.json` (`{"type":"module"}`) in each function copy.
- The functions are CommonJS and load the ESM copy with `await import("./custodyMatch/index.js")`.
- **Tradeoff.** No build step or private package is needed, but every change has to be manually re-copied (`cp appsail-nodejs/util/custodyMatch/*.js functions/CustodyMatchRunner/custodyMatch/`). Nothing enforces that the copies stay identical.

## 11. Catalyst platform constraints the code works around

Each of these is documented in code comments in `runStore.js`, `storage.js` or `CustodyMatchRunner`.

| Constraint | Handling |
|---|---|
| ZCQL returns at most 300 rows per query, silently | Every read loops on `OFFSET` |
| ZCQL allows at most 20 columns per SELECT, and `Custody_Match_Run` has 23 | Read as two queries merged on `ROWID` |
| `insertRows` accepts at most 200 rows | Exceptions inserted in chunks |
| `text` columns hold 10,000 characters | Values truncated to `TEXT_MAX` |
| Job functions have a 15-minute wall | Stage checkpoints plus time buffer |
| Stratus keeps listing a deleted object for about 20 seconds | Clear `FA_FILE_MISSING` message instead of a raw `NoSuchKey` |

## 12. Failure behaviour

| Failure | What happens |
|---|---|
| Job submit fails | 502, and the run is marked "Could not queue the job" |
| Job crashes mid-stage | Catalyst retries (3×). The runner resumes from the checkpointed `stage` |
| Job redelivered after finishing | No-op success |
| File removed after queueing | S1 fails with `FA_FILE_MISSING` and a readable message |
| Import rejects the file | Imported marker is rolled back |
| Retry | `POST /run/:runId/retry` is intended for "system failures only" (route comment) |

## 13. Known gaps

All labelled **[FOUND]**: identified by reading the code, not observed incidents. Details are in [open-findings](../problems-and-solutions/open-findings.md).

- No authentication on any `/api/custody-match/*` route. `startedBy` comes from the request body or an `x-user` header and is not verified.
- The old direct import route `POST /api/transaction-uploader/upload-temp-file` is **still mounted**. The UI button was removed, but the gate can be bypassed by calling the API directly.
- The double-import guard is check-then-set (`findImportedRunForDate`, then `setStageDetail`), and the code itself calls it a "soft guard". Two concurrent proceeds for different runs on the same date aren't atomically prevented.
- There are no automated tests for the compare rule, the gates or the cleanup.
- The whole file is parsed in memory in AppSail, which is configured with `memory: 256` in `app-config.json`. The 25 MB upload cap limits this.
- `README.md` doesn't describe custody matching yet.
