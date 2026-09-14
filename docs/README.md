# PMS Engineering and Interview Knowledge Base

- **Verified against:** commit `25dfb9d` (last commit dated 2026-09-10), on branch `worktree-java-interview-docs`.
- **Source of truth:** the code. Where a document and the code disagree, the code wins and the document should be fixed.

## Baseline warning

When this set was written, this worktree held **no committed engineering documentation** in `docs/` other than `MODULES.md` and data samples. Engineering documentation may exist **uncommitted** in the main checkout (for example numbered `docs/00-…` folders or an existing `docs/interview/`). That copy wasn't read.

**Needs developer confirmation:** reconcile this set with any such uncommitted docs before merging, to avoid two overlapping knowledge bases.

## Labels used everywhere

| Label | Meaning |
|---|---|
| **[BUILT]** | Exists in code at the verified commit |
| **[FIXED `commit`]** | A real problem, fixed by that commit. Evidence is the commit message and diff |
| **[FOUND]** | A potential problem identified by reading code. **Not** an observed incident |
| **[PROPOSED]** | A recommendation, not implemented |
| **[HYPOTHETICAL]** | A practice scenario that did not happen |
| **Needs developer confirmation** | Can't be established from the repository |

## Map

| Area | Document |
|---|---|
| Business modules (existing, functional) | [MODULES.md](MODULES.md) |
| Engineering detail per route/function (existing, partly stale) | [../README.md](../README.md) |
| System architecture as built | [architecture/current-architecture.md](architecture/current-architecture.md) |
| Custody matching feature | [architecture/custody-matching.md](architecture/custody-matching.md) |
| Background jobs, progress tracking, notification bell | [architecture/background-jobs.md](architecture/background-jobs.md) |
| ADR-001: custody match gate before import | [decisions/ADR-001-custody-matching-gate-before-import.md](decisions/ADR-001-custody-matching-gate-before-import.md) |
| ADR-002: job progress without a new table | [decisions/ADR-002-job-progress-tracking-without-new-table.md](decisions/ADR-002-job-progress-tracking-without-new-table.md) |
| Fixed problems (Aug to Sep 2026) | [problems-and-solutions/fixed-problems-2026-08-09.md](problems-and-solutions/fixed-problems-2026-08-09.md) |
| Open findings (security, integrity, reliability) | [problems-and-solutions/open-findings.md](problems-and-solutions/open-findings.md) |
| **Interview:** overview and 30s/2min/5min explanations | [interview/project-overview.md](interview/project-overview.md) |
| Interview: custody matching | [interview/custody-matching.md](interview/custody-matching.md) |
| Interview: background jobs and performance | [interview/background-jobs-and-performance.md](interview/background-jobs-and-performance.md) |
| Interview: security | [interview/security.md](interview/security.md) |
| Interview: production scenarios (hypothetical) | [interview/production-scenarios.md](interview/production-scenarios.md) |
| Interview: STAR stories | [interview/project-stories.md](interview/project-stories.md) |
| Interview: Java/Spring/AWS translation (not built) | [interview/java-spring-aws-translation.md](interview/java-spring-aws-translation.md) |

## Known stale spots in the root `README.md`

These are listed for the maintainers. The root README wasn't edited in this pass.

- It says "26 declared, 24 implemented" functions. `catalyst.json` now has 35 targets and `functions/` has 33 directories.
- It doesn't describe custody matching (`/api/custody-match`, `CustodyMatchRunner`, `CustodyMatchCleanup`) or the notification bell.
- "Update ISIN: No job completion polling" is outdated; it now uses the notification tray (`3105e0e`).
- "`App.css` File missing" is outdated; `react-app/src/App.css` exists.
