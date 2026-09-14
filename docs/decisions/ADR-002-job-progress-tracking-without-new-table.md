# ADR-002: Track background corporate-action progress with existing tables and a global notification bell

- **Status:** Accepted, implemented.
- **Commits:** `3105e0e` (notification tray, Update ISIN), `d736512` (Reports exports), `d1f92de` (Split and Bonus progress plus `caJobProgress.js`), `9799b03` (Dividend), `034cf73` (Merger and Demerger). All by kajal kharbanda, 2026-09-08 to 2026-09-10.
- **Evidence:** `appsail-nodejs/util/caJobProgress.js`, `react-app/src/notifications/*`.

## Context

Corporate-action applies (split, bonus, merger, demerger) and exports start background jobs. The commit message of `f6dc6b5` shows that "Apply New ISIN" used to report success "always" instead of tracking the real outcome. There was no shared way for the UI to know when background work had actually finished.

## Problem

1. Users couldn't see whether a background job had completed or failed.
2. Page-level polling stops when the user navigates away.

## Decision

**Backend.** Represent progress with the existing `Jobs` table (one parent row per track key) and `JobStatusPerAccount` (one row per background piece). The piece that finishes last settles the parent. Reads also self-settle, and a parent `RUNNING` for over 1 hour is forced to `ERROR`.

**Frontend.** A single `JobNotificationProvider` mounted above the router owns all poll loops (5-second interval, 20-minute timeout). Each page calls `trackJob({ id, label, statusUrl, parseStatus })`.

## Why

- The code comment in `caJobProgress.js` says "Model — no dedicated table". Why a new table was avoided is **not established from repository history**. Likely motives such as avoiding a Catalyst schema change are unverified. **Needs developer confirmation.**
- Mounting the provider above `HashRouter` keeps tracking alive across navigation (stated in the provider's doc comment).
- A pluggable `parseStatus` lets one tray understand different backend status vocabularies without changing those endpoints.

## Options (for discussion, not a recorded debate)

| Option | Why it may not have been chosen |
|---|---|
| Dedicated job-progress table | More schema to manage. Actual reason not recorded. |
| Push (WebSocket/SSE) | AppSail and job functions would need a push channel; polling needs none |
| Per-page polling only | Stops when the user leaves the page |

## Tradeoffs

- **Gained:** consistent status UX across many features, and no schema change.
- **Cost:**
  - Progress is only as reliable as each piece writing its final row.
  - Generic tables are overloaded by `jobName` convention.
  - Tray state is in memory only, so a reload loses it.
  - The 1-hour stale rule can mislabel a slow job as `ERROR`.

## Interview explanation

"Our corporate-action applies fan out into several background rebuild jobs, so there's no single job to wait on. Each piece writes its own status row under a shared key, and the parent row is settled when the last piece finishes. If a finalize gets lost, the status read settles it anyway, and anything stuck for an hour becomes an error. On the frontend there's one provider above the router that owns all the polling, so if you start a rebuild and go to another page, the bell still tells you when it's done."
