# Translating PMS Patterns to Java / Spring Boot / AWS

> **[PROPOSED / STUDY MATERIAL].** **No Java, Spring Boot or AWS code exists in this repository** at commit `25dfb9d`. The system is Node.js (Express on Zoho Catalyst AppSail), Catalyst job functions, Catalyst Data Store (ZCQL) and Stratus, with a React 19 frontend.
>
> Whether a Java/Spring/AWS rebuild is planned is **Needs developer confirmation**. Nothing committed mentions it. Use this page to answer "how would you build this in Java?", never "how did you build it in Java?".

---

## Mapping table

| What exists today (verified) | Java / Spring equivalent | AWS equivalent | Why it would help |
|---|---|---|---|
| Express routers in `appsail-nodejs/router/*` | `@RestController` + `@RequestMapping` | ECS Fargate service behind an ALB | Standard layering, typed request/response DTOs |
| `catalyst.initialize(req)` middleware sets `req.catalystApp` | Spring DI: inject `DataSource`, `S3Client` beans | IAM task role instead of per-request SDK init | No per-request client construction |
| ZCQL built with string concatenation (some unescaped) | JPA / `JdbcTemplate` with **bound parameters** | RDS PostgreSQL | Bound parameters remove SQL injection |
| No authentication middleware | Spring Security + JWT resource server | Cognito (or other IdP) | Authentication and per-role authorization |
| `submitJob` to Catalyst job pools | Message producer (`SqsTemplate`) | SQS queue + worker service on ECS | Retries, visibility timeout, DLQ |
| 15-minute job wall with baton-pass/checkpoints | Spring Batch step with `JobRepository` restart, or a chunked SQS worker | ECS task (no 15-minute wall); Lambda also caps at 15 minutes | Restartable chunks tracked in a database |
| `Jobs` / `JobStatusPerAccount` progress rows | A `job_run` table with a JPA entity and optimistic locking (`@Version`) | RDS | Atomic status transitions |
| Custody run check-then-set double-import guard | `UNIQUE (business_date)` on an import record, inside `@Transactional` | RDS | The database enforces "import once" |
| Stratus buckets and presigned GET | `S3Client` + `S3Presigner` | S3 | Same model |
| `CustodyMatchCleanup` cron (dry-run default) | `@Scheduled` job, or a separate scheduled task | EventBridge Scheduler; S3 lifecycle rules for object expiry | Lifecycle rules delete old objects without code |
| `queryWithTimeout` (20-second race) | JDBC `setQueryTimeout` / Hikari + statement timeout | RDS parameter `statement_timeout` | The database cancels the query (JS only stops waiting) |
| Retry loops in job code | Spring Retry / Resilience4j | SQS redrive policy | Declarative retries and backoff |
| `console.log` logging | SLF4J + Logback JSON + MDC request ID | CloudWatch Logs + metrics + alarms | Correlated, searchable logs |
| Vendored `custodyMatch/` copied into 3 places | A shared Maven/Gradle module | — | One source of truth, no manual copy |
| ~7 files named `*fifo*` implementing lot logic | One `HoldingsCalculator` service with unit tests | — | Removes the drift that caused several fixes |

## Practice questions (answer as "I would…")

**Q: How would you build the custody match pipeline in Spring Boot?**
A: I'd have `POST /runs` validate, insert a `match_run` row and publish an SQS message, then return 202. A worker service consumes it and runs stages, saving the stage to the row before each one, the same checkpoint idea we use today. Exceptions go into a table with batch inserts. The import endpoint runs in a transaction with a unique constraint on the business date, so a double import fails in the database rather than depending on a soft check.

**Q: How would `@Transactional` change the holdings rebuild?**
A: Today the rebuild deletes and re-inserts across separate calls, so a crash in the middle can leave partial data. That's labelled FOUND in the open findings. In Postgres with `@Transactional`, delete and insert commit together or not at all. For very large accounts I'd rebuild into a staging table and swap, to keep transactions short.

**Q: How would you stop SQL injection?**
A: Use bound parameters everywhere (`JdbcTemplate` `?` placeholders or JPA). Never concatenate request values. In our Node code some queries escape quotes by hand and some don't, and that inconsistency is exactly the risk.

**Q: What does SQS give you that Catalyst job scheduling doesn't?**
A: A dead-letter queue, visibility timeouts and redrive policies, all configured on the queue. Whether Catalyst offers equivalents wasn't established from this repo.

**Q: ECS or Lambda for the workers?**
A: Lambda has the same 15-minute cap we already fight, so long rebuilds would need the same checkpointing. ECS tasks have no wall, which makes long rebuilds simpler, at the cost of managing a service.

## Quick revision

- None of this exists yet; it's "how I would".
- The biggest wins over today: bound SQL parameters, authentication, database transactions and unique constraints, one shared FIFO module, structured logs.
