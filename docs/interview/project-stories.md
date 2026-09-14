# Project Stories (STAR format)

Rules for this file:

- Only real events with a commit as evidence.
- The committer is named. **Your role in each story is Needs developer confirmation** unless the commit is under your identity (`amit helios tech`, `amit@helios`, `Amit Thakur`).
- Git shows that `Amit Thakur` performed 130 of the 138 "Merge pull request" merge commits. Merging a PR doesn't prove you reviewed or wrote the code.
- **No metrics exist in the repository.** Don't add numbers such as "reduced latency by X%".

---

## Story 1: Reconciling transactions against the custodian before import

- **Evidence:** `f59a3cc` (2026-09-10, `amit helios tech`), about 19,600 lines added across AppSail, two job functions and the React page.
- **Your role:** committed under your identity. How much you designed, wrote, or generated with AI assistance is **Needs developer confirmation**.
- **Situation:** the daily transaction file was imported straight into production with no comparison against the custodian's records.
- **Task:** block the import until the file has been reconciled, without rewriting the existing import pipeline.
- **Action:**
  - Staged both files in object storage.
  - Built a pure parse/compare module shared by the API and a background job.
  - Matched on account + ISIN + date + buy/sell with unsigned quantity.
  - Checkpointed each job stage so retries resume.
  - Superseded runs when files change.
  - Imported through a server-side endpoint that reuses the existing upload handler.
  - Added a fail-safe cleanup cron that is dry run by default.
- **Result:** the feature exists in code and the UI's direct import button was removed. Production usage and outcomes are **Needs developer confirmation**.
- **What I'd improve:** remove the old upload route, add authentication, replace the soft double-import guard with a real constraint, add unit tests for the compare rule.

## Story 2: Corporate-action previews timing out in production

- **Evidence:** `f006231`, `c29751f`, `f5e409c` (kajal kharbanda); `945cdac`, `3ae14d1`, `780a500` (SambhavGrover). 2026-09-03 to 2026-09-04.
- **Your role:** **Needs developer confirmation.** Tell it as a team story unless confirmed.
- **Situation:** split, bonus and dividend previews worked locally but timed out on the deployed environment (408) for widely held ISINs.
- **Task:** make the previews finish in time.
- **Action:** replaced per-account queries with one paged read per ISIN plus batched `IN (...)` lookups, grouping in memory.
- **Result:** the commit messages describe the timeouts as fixed. No timing numbers were recorded.
- **Lesson:** N+1 patterns hide in local data and show up at production size.

## Story 3: A demerger doubled the holding

- **Evidence:** `ed934d4` (SambhavGrover, 2026-09-07).
- **Your role:** **Needs developer confirmation.**
- **Situation:** the holdings view roughly doubled positions for demerged securities.
- **Task:** find why the view disagreed with the rebuild job.
- **Action:** traced it to merger/demerger rows being treated as additive. Changed the logic so the first row of an event clears prior lots, matching the rebuild job, and dated those rows by `CA_DATE`.
- **Result:** the view now matches `RebuildHoldingtable`. A legacy-row limitation (rows without `CA_DATE`) is documented in code.
- **Lesson:** the same FIFO rule was implemented in several places, and they drifted apart.

## Story 4: Dates off by one day only on developer machines

- **Evidence:** `ce22219` (SambhavGrover, 2026-08-31; lists an AI co-author).
- **Your role:** **Needs developer confirmation.**
- **Situation:** split/bonus previews showed the previous day's holdings locally, but production looked fine.
- **Action:** found `setHours(0,0,0,0)` followed by `toISOString()` shifting IST dates back a day. Production runs in UTC, which hid the bug. Replaced it with string-based date helpers.
- **Lesson:** keep business dates as `yyyy-mm-dd` strings.

## Story 5: Background jobs that always said "success"

- **Evidence:** `f6dc6b5`, `3105e0e`, `d1f92de`, `034cf73` (kajal kharbanda, 2026-09-08 to 2026-09-10).
- **Your role:** **Needs developer confirmation.**
- **Situation:** Apply New ISIN reported success regardless of the outcome, and ISIN status tracking was broken by a `LIKE '%'` query.
- **Action:** real status rows with retries, exact-match status queries, a shared progress model for fan-out jobs, and a global notification bell.
- **Lesson:** a background job must report its real outcome, and the UI must track it beyond one page.
