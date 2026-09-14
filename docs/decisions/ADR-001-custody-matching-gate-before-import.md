# ADR-001: Gate transaction import behind custody matching

- **Status:** Accepted, implemented in `f59a3cc` (2026-09-10). The exact Gate 2 rule is **Needs developer confirmation** (see Consequences).
- **Date of implementation:** 2026-09-10 (commit date)
- **Evidence:** code and in-code comments in `appsail-nodejs/controller/custodyMatch/CustodyMatchRun.js`, `util/custodyMatch/*`, `functions/CustodyMatchRunner`, `functions/CustodyMatchCleanup`, and `react-app/src/pages/TempTransactionUpload/TempTransaction.js`.

## Context

Before this change, staff imported the daily 26-column transaction file straight into the `Transaction` table from the Transaction Upload page (`POST /api/transaction-uploader/upload-temp-file`). Nothing compared it against the custodian's own deal-allocation records before it reached holdings and cash calculations.

The business reason (for example, a specific incident or a compliance requirement) is **not established from repository history. Needs developer confirmation.**

## Problem

A wrong or incomplete transaction file could reach production data with no reconciliation step.

## Options considered

The repository doesn't record which alternatives were discussed. The options below are the realistic ones, listed for interview discussion, and are **not** a record of an actual debate.

1. Keep direct import; reconcile afterwards with a report.
2. Reconcile synchronously inside the upload request.
3. Stage files, reconcile in a background job, then import through a separate server-side step. **(Chosen.)**

## Decision

- Stage both sides in Stratus (`temporary-files`, organised by business date).
- **Gate 1** (`POST /run`) checks the preconditions and queues `CustodyMatchRunner`.
- The job compares on **Actual Code + ISIN + Transaction Date + Transaction Type**, with **Quantity** as the compared value, and writes exceptions.
- **Gate 2** (`POST /run/:runId/proceed`) re-reads the staged file and passes it to the **existing** `uploadTempTransaction` handler through a synthetic request.
- Remove the direct import button from the UI. The code comment says "a button that skips the match would make the whole gate advisory".
- Enforce both gates on the server, not with `disabled` buttons.

## Why (from code comments)

- The reconciliation for a full day can exceed request time, and job functions can checkpoint and retry.
- Reusing `uploadTempTransaction` unchanged avoids a second import implementation.
- Persisting the two consolidated "final" CSVs lets stages resume, and a disputed result can be reproduced from exactly what was compared.
- Direction lives in the match key and quantity is unsigned, so opposite trades can't net into a false match.

## Tradeoffs

| Gained | Cost |
|---|---|
| Disagreements surface before import | Extra steps for staff every day |
| Crash-safe, resumable pipeline | Two more tables (`Custody_Match_Run`, `Custody_Match_Exception`), a bucket folder layout, two more functions |
| One shared parse/compare module | The module is vendored by manual copy into two functions |
| Fail-safe cleanup of working data | Cleanup is dry run until someone explicitly sets `CLEANUP_DRY_RUN=false` |

## Consequences / open issues

- **Rule inconsistency.** The file header says Gate 2 needs a clean run with zero exceptions. The implementation allows import with open exceptions and records the count. Needs developer confirmation of the intended rule.
- **Bypass.** The old route `POST /api/transaction-uploader/upload-temp-file` is still mounted. With no authentication in the API, the gate is enforced for the UI but not for a direct API caller.
- The double-import guard is a check-then-set "soft guard", not an atomic constraint.

## Interview explanation

"Before we import the day's transaction file, we reconcile it against the custodian's deal files. Both files get staged in object storage. A background job builds a key per account, ISIN, date and buy/sell, sums the quantities on each side, and writes every difference as an exception. The job checkpoints each stage, so if it crashes Catalyst retries it and it resumes. The import itself is a separate server-side call that re-reads the staged file and feeds it into our existing upload pipeline, so we didn't write a second importer. One thing I'd still fix is that the old direct upload endpoint is still reachable, so today the gate is enforced through the UI and the new API, but not against someone calling the old endpoint."
