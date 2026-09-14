# Fixed Problems: Aug to Sep 2026

Every entry is **[FIXED]**, and the evidence is a commit. The problem description comes from that commit's message and diff. Nothing here is inferred beyond what the commit says.

- **Measured impact:** none of these commits contain before/after timings or user counts. Don't quote numbers in interviews.
- **Authorship:** each entry names the committer. Several commits list an AI co-author trailer (for example `ce22219`, `424e23b`). The user's own involvement in a teammate's fix (review, pairing, merge) is **Needs developer confirmation**.

---

## 1. Split/Bonus dates shifted back one day in IST

- **Commit:** `ce22219`, SambhavGrover, 2026-08-31.
- **Symptom:** an ex-date of 2025-06-28 was evaluated as 2025-06-27, so the preview showed the previous day's holdings. It reproduced locally but never on Catalyst.
- **Root cause:** dates were normalised with `new Date(v).setHours(0,0,0,0)` followed by `toISOString()`. That snaps to *local* midnight and then reads the value back in UTC, which moves the date back a day in any zone ahead of UTC. The deployed runtime is UTC, where `setHours(0…)` does nothing, which is why production looked fine.
- **Fix:** local `toISODateOnly` / `nextISODate` helpers that never cross a timezone boundary. The commit notes that `MergerController` already had this fix and Split/Bonus had been missed.
- **Lesson:** treat business dates as `yyyy-mm-dd` strings, not `Date` instants. The later `CustodyMatchCleanup` follows the same rule explicitly.
- **Interview line:** "It was a classic timezone bug. We turned a date string into local midnight and serialised it as UTC, so in India the date slipped back a day. Production runs in UTC, so it only showed up on developer machines. The fix was to stop converting business dates through Date objects at all."

## 2. Analytics moved from settlement-date to transaction-date basis

- **Commit:** `ce22219` (same commit as #1).
- **Change:** holdings "as on date" now uses the transaction date. The exception is `OPI`, which stays settlement-dated because "an inward transfer is not owned until it settles". This matches `RebuildHoldingtable`.
- **Complication:** the `Holdings` table stores `HOLDING` as a running balance built up in settlement order. A temporary bridge, `recomputeHoldingsSnapshot`, re-derives HOLDING, WAP and HOLDING_VALUE in memory in effective-date order.
- **Known gaps listed in the commit:**
  - `P_L` isn't re-derived.
  - Exports still read stored values. `beeed58` later moved the Report Holdings export onto the shared calculation.
  - As-on-date reads are slower.
- **Open:** the commit says the bridge should be removed once `RebuildHoldingtable` has been re-run. Whether that re-run has happened is **Needs developer confirmation**.

## 3. Merger/demerger roughly doubled a holding

- **Commit:** `ed934d4`, SambhavGrover, 2026-09-07.
- **Root cause:** `recomputeHoldingsSnapshot` treated MERGER/DEMERGER rows as additive. A demerger's old-ISIN row carries the unchanged quantity (only cost is reduced), so it stacked on the lots it was meant to replace.
- **Fix:** the first row of an event now **clears prior lots**, and the rest of that event accumulate onto the empty queue. This matches `RebuildHoldingtable` and `DemergerController`. MERGER/DEMERGER rows are dated by `CA_DATE`, not by each lot's original trade date.
- **Remaining limitation (from the diff comment):** legacy rows without `CA_DATE` fall back to the lot date, so a multi-lot event can undercount.
- **Lesson:** the same FIFO rule exists in several places, and they drifted apart. See [open-findings](open-findings.md) on duplicated FIFO logic.

## 4. Bonus apply granted too few shares

- **Commit:** `49b8f7f`, SambhavGrover, 2026-09-08.
- **Root cause:** the apply job rebuilt each client's position from raw transactions and was never given merger/demerger data. Shares that came from a corporate action were invisible, so grants came out short, or zero for a client who had never traded the security directly.
- **Fix:** the apply job reads the same Holdings ledger the preview reads (`functions/UpdateBonusTable/holdingsSnapshot.js`). A 375-line private `fifo.js` copy was deleted.
- **Claim in the commit:** "No past bonus was affected." Only the commit message supports this; there's no data-level evidence in the repo.
- **Lesson:** a preview and its apply must use the same calculation, or the user approves one number and gets another.

## 5. Corporate-action previews timed out on the deployed environment

- **Commits:**
  - `f006231` (Split export preview), `c29751f` (Bonus export preview), `f5e409c` (Dividend preview): kajal kharbanda, 2026-09-03.
  - `945cdac`, `3ae14d1`, `780a500`: SambhavGrover, 2026-09-04.
- **Symptom:** previews timed out on the deployed environment. The comment added in `mapVirtualToActualCodes.js` says endpoints returned **408** "once an ISIN had enough holders".
- **Root cause:** per-account query fan-out, an **N+1 query pattern**. Holdings were queried once per account, and `clientIds` was queried once per virtual code.
- **Fix:**
  - One keyset/paged read of all Holdings rows for the ISIN, then bucket rows by account **in memory** (`3ae14d1`, `945cdac`).
  - Look up `clientIds` with `WHERE WS_Account_code IN (...)` batches instead of one query per code (`f5e409c`, `780a500`). `780a500` keeps a per-code fallback for error recovery.
- **Tradeoff:** more rows held in AppSail memory per request, and more complex batching and paging code.
- **Interview line:** "Each preview looped over every client holding an ISIN and ran a query per client. That's fine locally and times out in production once a popular stock has enough holders. We changed it to one paged read for the whole ISIN plus batched IN-list lookups, and grouped the results in memory."

## 6. Holdings summary priced one ISIN at a time

- **Commits:** `778a8ad` and `2a60d63`, SambhavGrover, 2026-09-04.
- **Root cause:** the latest price was fetched with a query per ISIN, and the fallback for unpriced ISINs added another query per ISIN.
- **Fix:** bulk price query (`util/analytics/bhavPrices.js`) and one batched fallback query.

## 7. Smaller efficiency fixes

All SambhavGrover, 2026-09-04.

| Commit | Problem | Fix |
|---|---|---|
| `5b3100f` | Pagination always ran one extra empty query | Stop when a page returns fewer rows than the limit |
| `52f158c` | Separate `/getAllBuys` and `/getAllSells` calls | Compute buy and sell totals in memory from rows already fetched |
| `a122e6c` | Transaction tab re-queried on every keystroke | Fetch options once per account/date with a cancellation token, and filter client-side with `useMemo` |

## 8. Same-date re-export failed on the second attempt

- **Commit:** `1e6baf0`, kajal kharbanda, 2026-09-03.
- **Fix:** confirm the Stratus delete of the previous file before regenerating. Checkpoint helpers were added in `functions/ExportAllCustomerHoldingData/checkpoint.js`.
- **Follow-up:** `424e23b` reuses a file already generated today instead of regenerating it (see [background-jobs](../architecture/background-jobs.md#4-re-running-the-same-export-on-the-same-day)).
- **Root cause detail:** the commit message only says "confirming Stratus delete". The exact failure mode is **Needs developer confirmation**.

## 9. Export job hung silently until its timeout

- **Commit:** `76b88f6`, kajal kharbanda, 2026-09-02.
- **Fix:** `queryWithTimeout` (20 seconds). See [background-jobs](../architecture/background-jobs.md#3-guarding-against-a-zcql-call-that-never-returns).

## 10. Update ISIN status tracking broken; Apply New ISIN always "succeeded"

- **Commit `3105e0e`** (kajal kharbanda, 2026-09-08): "Fixed a ZCQL LIKE '%' bug that broke status tracking". It was replaced with exact `IN (...)` matching. The same commit made the rename retry-then-continue and removed the post-rename Holdings rebuild phase.
- **Commit `f6dc6b5`** (kajal kharbanda, 2026-09-08): `runApplyNewMode` now opens a `Jobs` row, retries each Security_Code/Name sync up to 3 times, and reports `SUCCESS` or `PARTIAL_FAILURE` honestly.
- **Open question:** why the Holdings rebuild step after rename was no longer needed is **Needs developer confirmation**.

## 11. Other fixes

| Commit | Author | Fix |
|---|---|---|
| `4b01afd` | SambhavGrover | Broken merge-conflict resolution in Split, Dividend and `clientIds` mapping (95 lines removed) |
| `beeed58` | SambhavGrover | Report Holdings export used a stale copy of the Analytics calculation; now uses the shared one |
| `38b1a2b` | SambhavGrover | Dividend preview re-derives holding instead of reading the stored running balance |
| `a2d42e8` | SambhavGrover | Split/Bonus export preview moved to transaction-date basis |

## Patterns across these fixes (useful in interviews)

1. **Duplicated calculations drift.** Fixes #3, #4 and the #11 entries for `beeed58` and `38b1a2b` all came from one copy of holdings/FIFO logic disagreeing with another.
2. **N+1 queries only hurt at production data sizes** (#5, #6).
3. **Timezones:** business dates should stay as strings (#1).
4. **A background job must report its real outcome**, not assume success (#10, and ADR-002).
