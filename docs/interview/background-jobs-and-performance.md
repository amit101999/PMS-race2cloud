# Interview Topic: Background Jobs and Performance Fixes

Labels: **[BUILT]** / **[FIXED commit]**. Details: [architecture/background-jobs.md](../architecture/background-jobs.md), [problems-and-solutions/fixed-problems-2026-08-09.md](../problems-and-solutions/fixed-problems-2026-08-09.md).
Most fixes here were committed by teammates (SambhavGrover, kajal kharbanda). Present them as team work unless you confirm your part. **Needs developer confirmation.**

---

## What is it?

Heavy work (holdings rebuilds, bulk exports, corporate-action applies, custody matching) runs as background job functions. The API only starts the job and reports status.

## Why do we use it?

That work touches many rows and can take minutes. An HTTP request would time out, and job functions give us retries.

## Key constraint in our project

Job functions stop at **15 minutes**, and ZCQL returns at most **300 rows** per query. Most of the design is about working within those two limits.

## Interview questions

### Basic

**Q: How does the UI know a background job finished?**
A: The job writes its status to a table. The UI polls a status endpoint every 5 seconds through one notification provider mounted above the router, so it keeps tracking even if you change page. It gives up and shows "still processing" after 20 minutes.

**Q: What's an N+1 query, and did you have one?**
A: It means one query for a list, then one more query per item. Yes, our corporate-action previews queried holdings once per client. Locally it was fine; on the deployed environment it timed out once a stock had enough holders. We replaced it with one paged read for the ISIN plus batched `IN (...)` lookups, grouped in memory. (Commits `f006231`, `c29751f`, `f5e409c`, `3ae14d1`, `780a500`.)

### Intermediate

**Q: How does a job longer than 15 minutes finish?**
A: We use a few patterns:
- The holdings master job dispatches at most 10 workers at a time. When its time is nearly up, it saves its progress to object storage and hands over to a new invocation.
- The all-clients export keeps a manifest, a cursor and multipart-upload state, so each invocation continues where the last one stopped.
- Custody matching checkpoints each stage.

**Q: What if a database call just hangs?**
A: We actually saw that. The comment in `zcqlTimeout.js` says the ZCQL client sometimes hung instead of failing, so the job died at 15 minutes with no logs. We race every export query against a 20-second timer, which turns the hang into a normal error our retry logic can handle.

**Q: A corporate action fans out into several jobs. How do you know when it's all done?**
A: Each piece writes a row under a shared key. When the last piece finishes it settles the parent row. The status read also settles it if nothing is still running, and anything stuck over an hour becomes an error.

### Scenario / follow-up

**Q: A user repeats the same export twice in one day. What happens?**
A: If today's run completed and the file still exists, we reuse it instead of rebuilding. The tradeoff is that transactions uploaded between the two requests won't be in the file.

**Q: What's the risk of retries?**
A: A retry re-runs the job, so the job must be idempotent. Custody matching is designed for that. Not every older job has been verified, and that's on my list.

**Q: Why polling and not WebSockets?**
A: Polling needs nothing extra from the platform, and the job count is small. The downsides are extra requests, and the tray lives in memory, so a page reload loses it.

## Tradeoffs

- Background jobs mean retries and no request timeouts, but also eventual consistency and more moving parts.
- Batching removes N+1 queries but holds more rows in memory per request.
- The 20-second query timeout prevents silent hangs, but a valid slow query is treated as a failure.

## Quick revision

- 15-minute job limit leads to baton-pass, checkpoints and stage resume.
- ZCQL 300-row cap means every read pages with OFFSET.
- N+1 in previews was fixed with one paged read plus IN-list batches.
- The ZCQL hang was fixed by racing queries against a 20-second timeout.
- Bell: 5-second polling, 20-minute timeout, survives navigation, not reloads.
