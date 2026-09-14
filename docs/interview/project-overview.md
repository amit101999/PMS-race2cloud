# Project Overview: How to Explain PMS

Verified at commit `25dfb9d`. Architecture detail: [architecture/current-architecture.md](../architecture/current-architecture.md).

**Before using any "I built…" wording:**

- Git shows about 19 author identities.
- Your identities (`Amit Thakur`, `amit helios tech`, `amit@helios`) have the most commits.
- `Amit Thakur` performed 130 of 138 PR merge commits.
- Many recent fixes were committed by SambhavGrover and kajal kharbanda.

Which parts you personally designed or wrote is **Needs developer confirmation**. Use "we" for team work.

Also: **the project is Node.js on Zoho Catalyst, not Java.** Don't describe it as a Spring Boot or AWS system. For Java roles, use [java-spring-aws-translation.md](java-spring-aws-translation.md) to show how you *would* build it.

---

## 30-second explanation

"PMS is a portfolio management system for Indian equity portfolios. Staff upload daily broker transaction files. We keep FIFO holdings for every client account, apply corporate actions like splits, bonuses, dividends and mergers, keep a cash passbook, and produce analytics and exports. It's a React front end with a Node/Express API on Zoho Catalyst. Heavy work like holdings rebuilds and exports runs as background job functions. Recently we added a reconciliation step that matches the day's file against the custodian's records before anything is imported."

## 2-minute explanation

1. **Problem:** a portfolio manager needs accurate positions and cash per client, including the effect of corporate actions, from broker files.
2. **Architecture:**
   - React 19 SPA.
   - Express API on Catalyst AppSail.
   - Catalyst Data Store queried with ZCQL.
   - Stratus object storage for files and exports.
   - About 33 Catalyst functions for background work.
3. **Core logic:** holdings are FIFO lots. As-on-date views use the transaction date, except inward transfers (settlement date) and mergers/demergers (corporate-action date).
4. **Why background jobs:** rebuilds and exports touch many rows, and functions stop at 15 minutes. So jobs checkpoint progress in storage and hand over to a fresh invocation.
5. **Recent engineering:**
   - Removed N+1 queries that made corporate-action previews time out in production.
   - Fixed a timezone off-by-one.
   - Fixed a demerger double count caused by duplicated FIFO logic.
   - Added a notification bell that tracks background jobs across pages.
   - Built a custody reconciliation gate before import.
6. **Honest gaps:** no authentication yet, some unescaped queries, almost no automated tests, several copies of the FIFO logic.

## 5-minute deep dive outline

Pick one or two and go deep:

| Topic | Talking points | Doc |
|---|---|---|
| Custody matching | Match key, unsigned quantity, stage checkpoints, idempotent retries, supersede, server-side gates, fail-safe cleanup | [custody-matching.md](custody-matching.md) |
| Performance | N+1 in previews, batched IN lists, bulk price lookup, ZCQL hang guarded by a timeout | [background-jobs-and-performance.md](background-jobs-and-performance.md) |
| Correctness | Timezone bug, transaction-date basis, merger/demerger restate, preview and apply using the same ledger | [fixed problems](../problems-and-solutions/fixed-problems-2026-08-09.md) |
| Security | No auth yet, and what you'd fix first | [security.md](security.md) |

## Numbers you can safely quote

From the committed sample file `Data_inception_transactionFile.csv`: about **75k transactions, 440 accounts, 424 ISINs**, TRANDATE from 2010 to Sep 2025.

Don't quote latency, uptime, user counts or business impact. None are recorded. **Needs developer confirmation.**

## Likely interviewer questions

- **Why Catalyst and not AWS?** The reason isn't recorded in the repository. **Needs developer confirmation.**
- **Why a single API plus functions, not microservices?** The observable reason is that one small team and one API is simpler, and functions handle the heavy async work. Any recorded rationale is **Needs developer confirmation**.
- **Hardest bug?** Candidates: the demerger double count (`ed934d4`) or preview timeouts (`f5e409c`). Say who fixed it.
- **What would you change at 10× data?** Incremental holdings instead of full rebuilds, previews moved to jobs, one shared FIFO module, authentication, tests.

## Quick revision

- React 19, Express on Catalyst AppSail, Data Store (ZCQL), Stratus, about 33 job functions.
- FIFO holdings, corporate actions, cash passbook, exports.
- 15-minute job limit leads to checkpoints and baton-pass.
- Recent work: N+1 removal, date-basis and timezone fixes, notification bell, custody gate.
- Gaps: auth, injection, tests, duplicated FIFO.
