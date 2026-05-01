# MegerFn — Lot-level merger apply (Catalyst Cloud Function)

Runs the heavy lot-level merger work in the background and writes one row per
**surviving original buy lot** into the `Merger` table (instead of the previous
behaviour of one consolidated row per account). Original buy dates are preserved
so LTCG / STCG / FIFO downstream stays correct.

> Function name in `catalyst-config.json` is `MegerFn`. The AppSail controller
> submits the job with `target_name: "MegerFn"`.

---

## Architecture (one paragraph)

`POST /api/merger/apply` no longer does the work itself. It validates input,
checks for an in-flight job in the `Jobs` table, generates a deterministic
`jobName` (`MRG_<old6>_<new6>_<yyyymmdd>`), submits this Catalyst function via
`jobScheduling().JOB.submitJob`, and returns `{ jobName, status: "PENDING" }`
immediately. The React `MergerPage` polls `GET /api/merger/apply-status` every
3 seconds until the job reaches `COMPLETED` / `FAILED` / `ERROR`.

Inside the function:

1. Insert / mark `Jobs` row as `RUNNING`.
2. Read all `Transaction`, `Bonus`, `Split`, prior `Demerger`, prior `Merger`
   rows for the **old ISIN** in one paginated pass each (no per-account
   queries) — far fewer ZCQL round trips at 500-customer scale.
3. Group by account in memory and run a lot-aware FIFO walk
   (`fifo.js → runFifoForLots`) per account. The walk preserves each lot's
   `Source_Tran_ROWID` and `originalTrandate`.
4. For every surviving open lot: `newQty = floor(lotQty × ratio1 ÷ ratio2)`.
   - If `newQty == 0` the lot is skipped (logged via `skippedZeroLots` /
     `skippedCostBasis`); cost basis on those shares is lost — this matches
     the existing consolidated apply for fractional rounding.
   - Otherwise we keep the **full lot cost** on the surviving shares
     (`WAP = lotCost / newQty`), so cost basis is preserved.
5. Build a CSV of all `(account, lot)` rows.
6. Upload to Stratus bucket `upload-data-bucket` under `merger-apply/...`.
7. Bulk-insert into `Merger` via `datastore.table("Merger").bulkJob("write")`
   (matches the pattern used by `CalcullateAllTransactionHoldingOnce`).
8. Insert one row into `Merger_Record`, update / insert `Security_List` for
   the new ISIN, mark `Jobs` row as `COMPLETED`.

---

## Required schema changes (do these in the Catalyst console BEFORE running)

### 1. `Merger` — add one new column

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `Source_Tran_ROWID` | Text (e.g. VARCHAR 64 / String) | Yes | Stores the originating `Transaction.ROWID` for transaction lots. Empty for bonus/demerger/merger-derived lots. Used for audit / future undo. |

The function's bulk-insert CSV includes this column. Without it the bulk
import will fail.

### 2. `Jobs` — no change required

The existing `Jobs` table (used by `UpdateBonusTable`, `ExportCashBalance`,
etc.) already has `jobName`, `status`, `CREATEDTIME`, `ROWID`. We reuse it.

### 3. `Merger_Record` — no change required

The existing schema (`ISIN`, `Security_Code`, `Security_Name`, `OldISIN`,
`Ratio1`, `Ratio2`, `TRANDATE`, `SETDATE`) is unchanged.

### 4. `Merger` — semantic change (read carefully)

After this change, the `Merger` table will contain **multiple rows per
(account, new ISIN, merger event)** — one per surviving lot. Existing
readers handle this correctly because:

- `appsail-nodejs/util/analytics/transactionHistory/mergers.js` already
  iterates **all** matching rows for an account (no `LIMIT 1` assumption).
- `runFifoEngine` in `appsail-nodejs/util/analytics/transactionHistory/fifo.js`
  already treats every `Merger` row as its own MERGER event in the timeline,
  so each surviving lot becomes its own opening lot on the new ISIN.

Old consolidated `Merger` rows from previous mergers are **left untouched**.
Only future mergers go through the lot-level path.

---

## Deploying the function

From the project root:

```bash
catalyst deploy --only functions:MegerFn
```

Or push the whole `functions/` folder via the Catalyst CLI as you normally do.

---

## Triggering / status

| Endpoint | Method | Body / Query | Returns |
|---|---|---|---|
| `/api/merger/preview` | POST | `{ recordDateIso, ratio1, ratio2, oldCompanies, mergeIntoNewCompany }` | Synchronous consolidated preview rows (unchanged). |
| `/api/merger/apply` | POST | Same body as preview | `{ jobName, status: "PENDING" }` — function runs in background. |
| `/api/merger/apply-status` | GET | `?jobName=MRG_...` | `{ status: "NOT_STARTED" \| "PENDING" \| "RUNNING" \| "COMPLETED" \| "FAILED" \| "ERROR" }` |

The frontend `MergerPage.js` polls `apply-status` every 3 s; status pill in
the UI updates from `PENDING` → `RUNNING` → `COMPLETED`.

Idempotency: if the same `jobName` (= same old ISIN, new ISIN, record date)
already has a `PENDING` / `RUNNING` row younger than 1 hour, a fresh apply
call returns the existing job instead of queueing a duplicate.

---

## Performance expectations (500-customer book)

| Step | Approx cost |
|---|---|
| One-pass reads for all tables (old ISIN history) | 5–20 sec |
| In-memory FIFO across all accounts | 2–5 sec |
| CSV build + Stratus upload | 2–5 sec |
| Bulk insert via `bulkJob` (1k–5k rows) | 5–15 sec |
| **Total** | **~15–45 sec end-to-end** |

The Catalyst 15-minute function timeout is well out of reach at this scale.

---

## Verifying after a run

1. `SELECT COUNT(*) FROM Merger WHERE OldISIN = '<oldIsin>' AND TRANDATE != '<recordDate>'` — should return the lot row count (each carrying its original buy date).
2. `SELECT * FROM Merger_Record WHERE ISIN = '<newIsin>' ORDER BY ROWID DESC LIMIT 1` — should show the new corp-action header.
3. `SELECT * FROM Security_List WHERE ISIN = '<newIsin>'` — should be present.
4. Holding tab for a sample affected client — totals should match the preview's `totalNewShares` and `totalCarriedCost` (within fractional-floor rounding).

If a lot was skipped because `floor(lotQty × r1 / r2) == 0`, the function
logs it as `skippedZeroLots` / `skippedCostBasis`; see Catalyst function logs.
