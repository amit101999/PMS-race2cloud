# Interview Topic: Custody Matching (Reconciliation Before Import)

Label: **[BUILT]** at `25dfb9d`. Full technical detail: [architecture/custody-matching.md](../architecture/custody-matching.md).
Your personal ownership of this feature is **Needs developer confirmation**. The commit (`f59a3cc`) is under the `amit helios tech` identity. Before claiming it, confirm how much you designed versus reviewed or generated.

---

## What is it?

A reconciliation step. Before the day's transaction file is imported, we compare it with the custodian's deal-allocation files and list every difference.

## Why do we use it?

So a wrong or incomplete transaction file is caught before it changes holdings and cash. The specific business trigger is **Needs developer confirmation**.

## How it works in our project (the 30-second version)

1. Staff upload the transaction file and one custodian file per scheme. Both are validated immediately and stored in object storage by date.
2. "Match" queues a background job.
3. The job builds a key of **account + ISIN + date + buy/sell**, sums **quantity** on each side, and writes an exception for every difference.
4. The screen polls for status and shows the exceptions.
5. "Proceed" imports the staged file through the existing upload pipeline.

## Simple example

```text
FA side:        AYAN119 | INE213A01029 | 2025-03-12 | SELL | 2172
Custody side:   AYAN119 | INE213A01029 | 2025-03-12 | SELL | 2000
Result:         QUANTITY_MISMATCH  (FA 2172 vs custody 2000)
```

This is an illustrative row built from the committed data file's column shape. It is not a real exception from production.

## Interview questions

### Basic

**Q: Why do the matching in a background job instead of in the request?**
A: A full day can take longer than a request should, and Catalyst job functions can retry and checkpoint. The API validates the upload synchronously, then returns 202 and queues the job. The UI polls for the result.

**Q: What's in the match key, and why isn't quantity signed?**
A: Account, ISIN, trade date and buy/sell. Direction is in the key and quantity stays positive. If you signed and netted quantities, a buy of 1000 and sell of 400 would look the same as a buy of 400 and sell of 1000.

**Q: Why ignore rate, brokerage and STT?**
A: They're shown on the exception to help investigation, but they don't decide a break. The rule in the code says a brokerage difference isn't a custody break.

### Intermediate

**Q: What happens if the job crashes halfway?**
A: Each stage writes its checkpoint to the run row *before* it starts. Catalyst retries up to 3 times, and the job resumes from the saved stage. Every stage can safely run twice: file outputs go to fixed keys, compare restarts from zero, and report deletes its old exceptions before writing new ones.

**Q: What if Catalyst delivers the same job twice after it already finished?**
A: The job checks the run status first. If it's already completed or superseded, it exits successfully without doing anything.

**Q: What if someone replaces a file after a match?**
A: Uploading, removing or remapping a file marks every run for that date as SUPERSEDED. A superseded run can't be reused or imported, so you have to match again.

**Q: How do you stop a double import?**
A: The run is marked "imported" before the import starts, and we also refuse if any run for that date was already imported. Honestly, it's a check-then-set, so it's a soft guard. A database uniqueness constraint would make it airtight.

### Practical / scenario

**Q: Is the gate really enforced?**
A: The new endpoints enforce it on the server, not just with disabled buttons. The weak spot is that the old direct upload endpoint is still mounted, and the API has no authentication, so a direct API call can skip it. I'd remove or protect that route.

**Q: Does an import with open exceptions get blocked?**
A: In the current code, no. It's allowed and recorded as "imported with N open exceptions". A comment elsewhere says import needs a clean run, so the intended rule needs confirming. **Needs developer confirmation.**

**Q: How is the same logic shared between the API and the job?**
A: The parse and compare code is pure JavaScript with no platform SDK inside. It's copied file-for-file into the two functions. That's simple, but it depends on remembering to re-copy after every change. A shared package or a build step would be safer.

**Q: How does the cleanup avoid deleting the wrong data?**
A: It's dry run by default. Retention can't go below 7 days. It compares dates as strings to avoid timezone slips, skips any date with an active run, and deletes files before rows.

## Follow-up questions to prepare

- How would you test the compare rule? (There are no tests today. Table-driven unit tests on `compareSides` with pure inputs would be the natural start.)
- What if the files grow 10×? Parsing happens in memory and AppSail is configured with 256 MB. Streaming parse or doing validation in the job would help.
- How would you build this on AWS? See [java-spring-aws-translation.md](java-spring-aws-translation.md).

## Tradeoffs

| Pro | Con |
|---|---|
| Errors caught before production | Extra daily step for staff |
| Resumable, retry-safe pipeline | More tables, functions and storage layout to operate |
| Reuses the existing importer | Synthetic-request trick couples it to that handler's inputs |

## Quick revision

- Key = account + ISIN + date + buy/sell, value = unsigned quantity.
- Gate 1 queues the job (202); Gate 2 imports through the existing pipeline.
- Checkpoint before each stage; every stage is idempotent; finished runs are a no-op.
- File changes supersede runs.
- Weak spots: old upload route still open, no auth, soft double-import guard, no tests.
