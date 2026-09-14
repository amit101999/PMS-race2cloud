# Current Architecture (as built)

Status: **[BUILT]** at commit `25dfb9d`. Every count below was taken from the repository at that commit.

---

## 1. What the application does

PMS is a portfolio management system for Indian equity portfolios. It:

- ingests broker transaction files and custodian files
- keeps FIFO-based holdings per client account
- applies corporate actions (split, bonus, dividend, merger, demerger)
- keeps a cash passbook
- serves analytics screens and CSV exports

Business detail: [MODULES.md](../MODULES.md).

## 2. Technology (verified from manifests)

| Layer | Technology | Evidence |
|---|---|---|
| Frontend | React 19, React Router 6 (`HashRouter`), Create React App (`react-scripts` 5) | `react-app/package.json`, `App.js` |
| API | Node.js, Express 4, ESM, `zcatalyst-sdk-node`, `express-fileupload`, `csv-parser` | `appsail-nodejs/package.json` |
| API hosting | Zoho Catalyst **AppSail**, `stack: node16`, `memory: 256` | `appsail-nodejs/app-config.json` |
| Background work | Catalyst **functions**: 35 targets in `catalyst.json`, 33 directories in `functions/` | `catalyst.json` |
| Database | Catalyst **Data Store**, queried with **ZCQL** | SDK usage throughout |
| Object storage | Catalyst **Stratus** (buckets seen in code include `temporary-files` and `upload-data-bucket`) | `util/custodyMatch/storage.js`, `ExportAllCustomerHoldingData/checkpoint.js` |

**Not present in the repository:** Java, Spring Boot, AWS, Terraform, Docker, CI pipelines, Redis or queues other than Catalyst job scheduling. See [interview/java-spring-aws-translation.md](../interview/java-spring-aws-translation.md) for study material, clearly marked as not built.

## 3. Component diagram

```text
Browser (React SPA, HashRouter, 14 active routes)
   │  fetch → BASE_URL (react-app/src/constant.js)
   ▼
AppSail: Express API (appsail-nodejs/index.js)
   cors (localhost:5000, localhost:3000 only)
   → express.json / urlencoded
   → catalyst.initialize(req) → req.catalystApp
   → 16 routers under /api/*  +  GET /api/holding-update  +  GET /
   │
   ├── ZCQL ─────────────► Data Store (Transaction, Holdings, clientIds, Jobs,
   │                        JobStatusPerAccount, Cash tables, Custody_Match_* …)
   ├── Stratus ──────────► buckets (uploads, exports, custody working files)
   └── jobScheduling() ──► Catalyst functions (job / event / cron)
                              │ write results back to Data Store + Stratus
                              ▼
                     UI polls status endpoints (notification bell)
```

## 4. Inventory

**Mounted routers** (`index.js`): analytics, transaction, dashboard, split, export, bhav, transaction-uploader, cash-balance, bonus, dividend, isin, demerger, merger, client, security, custody-match.

**Functions:**

- `catalyst-config.json` types: 26 `job`, 3 `event`, 1 `cron`. Stacks: 28 `node20`, 2 `node24`.
- `catalyst.json` lists `HoldingdataUpdate` and `DailyClientHoldingTransaction`, but neither has a directory. Whether those two targets are obsolete is **Needs developer confirmation**.

**Frontend routes** (`App.js`):

- Active: `/`, `/analytics`, `/split`, `/bonus`, `/dividend`, `/demerger`, `/merger`, `/bhav-copy`, `/temp-transaction`, `/updateISIN`, `/cash-balance`, `/reports`, `/master-client`, `/master-security`.
- `/transaction-upload` is commented out.

**Tests:** one file, `react-app/src/App.test.js`. There are no backend or function tests.

## 5. Data volume (from the committed sample file)

`Data_inception_transactionFile.csv`:

| Measure | Value |
|---|---|
| Transaction rows | 74,928 |
| Distinct account codes (`WS_Account_code`) | 440 |
| Distinct ISINs | 424 |
| Rows with no ISIN (cash-only movements) | 20,619 (about 27.5%) |
| TRANDATE range | 2010-04-01 to 2025-09-29 |

Whether this file matches current production volume is **Needs developer confirmation**. Request rates and concurrent users aren't recorded anywhere in the repository.

## 6. Request lifecycle

1. The CORS check allows only `http://localhost:5000` and `http://localhost:3000`.
2. JSON and urlencoded bodies are parsed.
3. `catalyst.initialize(req)` is attached as `req.catalystApp`. On failure it's set to `null` and the request continues; handlers check for this and return 500.
4. The router hands off to the controller, which runs ZCQL, Stratus calls and/or `submitJob`.
5. The controller writes the JSON response.

There is **no authentication or authorization step**. See [open-findings](../problems-and-solutions/open-findings.md).

Committed configuration to check: `react-app/src/constant.js` currently sets `BASE_URL = "http://localhost:3001/api"`, with the deployed AppSail URL commented out. How the deployed build points at the real API is **Needs developer confirmation**.

## 7. Design pattern

AppSail is a **thin orchestration API**:

- it reads pre-computed holdings and cash tables for screens
- it runs FIFO calculations in memory for previews
- it pushes heavy writes (uploads, corporate-action applies, rebuilds, exports, custody matching) to background functions

Details: [background-jobs.md](background-jobs.md).

### Platform constraints that shape the code

These are stated in code comments.

- ZCQL returns at most 300 rows per query, so reads page with `OFFSET`.
- ZCQL allows at most 20 columns per SELECT.
- `insertRows` accepts at most 200 rows.
- Job functions have a 15-minute limit.
- Background functions need `scope: "admin"` or Data Store calls stall.

## 8. Holdings and dates (recent change)

- Since `ce22219` (2026-08-31), analytics use the **transaction date**. The exceptions are `OPI` (settlement date) and `MERGER`/`DEMERGER` (`CA_DATE`, since `ed934d4`).
- The canonical holdings writer is `functions/RebuildHoldingtable`.
- Several other FIFO/holdings implementations exist: at least 7 files named `*fifo*` across `appsail-nodejs/util` and `functions/`, plus `holdingsFromTable.js` and `UpdateBonusTable/holdingsSnapshot.js`. Keeping them in agreement has needed repeated fixes ([fixed problems](../problems-and-solutions/fixed-problems-2026-08-09.md)).

## 9. Architecture evolution (from git history only)

| When | What changed | Evidence |
|---|---|---|
| 2025-12-30 | First commit: Catalyst app (React + AppSail + functions) | `git log --reverse` |
| through 2026-08 | Features added module by module; 19 author identities in `git shortlog` (some are the same person), 138 PR merge commits | git |
| 2026-08-31 | Analytics switched to transaction-date basis; timezone bug fixed | `ce22219` |
| 2026-09-02 to 09-04 | Performance pass: N+1 removal, bulk price lookups, ZCQL timeout, export reuse | see fixed problems |
| 2026-09-07 to 09-08 | Holdings correctness: merger/demerger restate, bonus uses holdings ledger | `ed934d4`, `49b8f7f` |
| 2026-09-08 to 09-10 | Notification bell and job-progress tracking | ADR-002 |
| 2026-09-10 | Custody matching gate before import | ADR-001 |

**Future direction:** no committed file mentions migrating off Catalyst. Any plan such as a Java/Spring/AWS rebuild is **Needs developer confirmation** and must be labelled [PROPOSED].

## 10. Scalability view

This is an analysis, not measured.

| Growth | Likely bottleneck | Realistic next step |
|---|---|---|
| 10× transactions | ZCQL paging at 300 rows and per-account rebuilds hitting the 15-minute limit | More baton-pass or checkpointing; incremental holdings instead of full rebuilds |
| 10× holders per ISIN | Corporate-action previews held in AppSail memory (256 MB) | Move previews to jobs; stream results |
| More concurrent users | 5-second polling per tracked job per tab | Longer or backoff intervals; a single status endpoint for many jobs |
| Larger custody files | In-memory parse in AppSail (25 MB cap) | Validate inside the job; streaming parse |

## 11. Reliability view

- Retries come from Catalyst `job_config` (for example custody match: 3 retries, 60 seconds apart).
- Only some jobs are written to be idempotent (custody match is, explicitly).
- A ZCQL hang is guarded only in the all-clients export.
- There are no health checks beyond `GET /` and `GET /api/custody-match/health`.
- There are no metrics or alerts, and logging is `console.*` only.
