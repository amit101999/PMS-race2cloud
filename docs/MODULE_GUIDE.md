# PMS Audit-Bridge — Module Guide

**What this document is:** a plain-language guide to every module of the system. For each module it explains:

- what the module is for (the business reason),
- which files it keeps and where,
- which tables it adds rows to, changes, or deletes from,
- which background jobs it starts, and in which job pool,
- which other modules feel the effect of a change.

Technical names (tables, folders, jobs) are written `like this`, so they can be found in the code.

---

## Contents

1. [Words used in this guide](#1-words-used-in-this-guide)
2. [The big picture](#2-the-big-picture)
3. [Shared building blocks](#3-shared-building-blocks)
4. [Modules](#4-modules)
   - 4.1 [Client and Security Masters](#41-client-and-security-masters)
   - 4.2 [Bhav Copy (closing prices)](#42-bhav-copy-closing-prices)
   - 4.3 [Field Mapping](#43-field-mapping)
   - 4.4 [Transaction Upload and Custody Matching](#44-transaction-upload-and-custody-matching)
   - 4.5 [Holdings engine](#45-holdings-engine)
   - 4.6 [Cash passbook](#46-cash-passbook)
   - 4.7 [Analytics](#47-analytics)
   - 4.8 [Custodian Upload (corporate action files)](#48-custodian-upload-corporate-action-files)
   - 4.9 [Corporate actions: the two-stage custodian check](#49-corporate-actions-the-two-stage-custodian-check)
   - 4.10 [Split](#410-split)
   - 4.11 [Bonus](#411-bonus)
   - 4.12 [Dividend](#412-dividend)
   - 4.13 [Merger](#413-merger)
   - 4.14 [Demerger](#414-demerger)
   - 4.15 [Holding Comparison](#415-holding-comparison)
   - 4.16 [Update ISIN](#416-update-isin)
   - 4.17 [Reports and exports](#417-reports-and-exports)
   - 4.18 [Notifications](#418-notifications)
5. [What a change affects (impact table)](#5-what-a-change-affects-impact-table)
6. [Scheduled cleanup (cron)](#6-scheduled-cleanup-cron)
7. [Before going to Production](#7-before-going-to-production)

---

## 1. Words used in this guide

| Word | Meaning |
| --- | --- |
| **Actual Code** | One real client. The custodian knows clients only by this code (also called UCC). |
| **Virtual Code** | A client's sub-account for one scheme. One actual code can have many virtual codes. Trades, holdings and cash are kept at this level. Table column: `WS_Account_code`. |
| **ISIN** | The 12-character code of a share or fund. |
| **FA** | Fund Accounting: our own books, built by this system. |
| **Custodian** | The bank that holds the shares and keeps a second, independent record. |
| **Record date** | The date that decides who gets a corporate action and on what holding. All corporate action maths uses it. |
| **Effective date** | The ex-date or issue date of a corporate action. Shown on screens and reports only. |
| **Table** | A place in the database (Catalyst Data Store) where rows are kept. |
| **Stratus** | Catalyst's file storage. Files live in **buckets**, and inside a bucket in **folders**. |
| **Job** | Work that runs in the background (a Catalyst job function), so the screen does not have to wait. |
| **Job pool** | A named queue that jobs run in. Jobs in the same pool share its capacity. |
| **Cron** | A job that runs by itself on a schedule. |
| **Baton pass** | A job may run for at most 15 minutes. Long jobs save their place, start a fresh copy of themselves, and stop. The new copy carries on. |

---

## 2. The big picture

Every day the operator loads the trades, checks them against the custodian, and commits them. Holdings and cash then update by themselves. Company events (corporate actions) are checked against the custodian and applied when they happen. Reports and checks read the result.

**Three rules hold the whole system together:**

1. **No trade reaches the books before matching.** The Commit button only exists after a match run.
2. **Committing trades adds holdings; corporate actions rebuild them.** A commit adds new holding rows on top. A corporate action deletes and re-calculates the holding rows of the clients and shares it touches.
3. **Viewing never writes.** Analytics, reports and Holding Comparison only read data.

---

## 3. Shared building blocks

### 3.1 Job pools

| Job pool | What runs in it | Started by |
| --- | --- | --- |
| `UpdateMasters` | `UpdatesSecurity_ClientMasters`, `EnrichTransactionSecurity`, `CalculateHoldingMaster`, `CalculateHoldingWorkers`, `Cal_CB_Append_TxnUpload`, `UpdateISIN`, `UpdateISINWorker`, `HoldingUpdateManually` | Trade commit, Update ISIN |
| `CustodyMatch` | `CustodyMatchRunner` | Match / Retry on Transaction Upload |
| `CorporateActions` | `UpdateBonusTable`, `UpdateDividendData`, `MegerFn`, `DemergerFn`, `RebuildHoldingtable` | Split, Bonus, Dividend, Merger, Demerger |
| `HoldingComparison` | `FAHoldingsGenerator`, `HoldingComparisonRunner` | Holding Comparison page |
| `Export` | `ExportAllCustomerHoldingData`, `ExportCashBalance`, `ExportCashBalSingleClient`, `ExportDividendAccounts`, `ExportCorpActionReport` | Reports page, Cash page, Dividend page |
| `Finance` | `CashCalculation` (old daily cash ledger) | Manual trigger only |

### 3.2 Stratus buckets and folders

| Bucket | Folder | What is kept there |
| --- | --- | --- |
| `temporary-files` | `FA-Transaction/<date>/` | Our trade files as uploaded, the combined "final" file per run, and the difference file |
| `temporary-files` | `Custodian-Transaction/<date>/` | Custodian deal files per scheme, and the combined "final" file per run |
| `temporary-files` | `Custodian-History/` | Copies of the last 5 custodian deal files, for download |
| `temporary-files` | `field-maps/` | Saved Field Mappings, one per file layout |
| `temporary-files` | `Custodian-Holdings/<date>/` | Custodian holding statements (Holding Comparison) |
| `temporary-files` | `FA-Holdings/<date>/` | Our holdings snapshots (Holding Comparison) |
| `temporary-files` | `Holding-Differences/<date>/` | Holding Comparison difference reports |
| `temporary-files` | `CA-Differences/<type>-<stage>/<record date>/` | Split, bonus and dividend custodian check results |
| `custodian-ca-files` | `CA-Ex-Entitlement/<date>/` | Custodian Ex-Entitlement files |
| `custodian-ca-files` | `CA-Benefit-Collection/<date>/` | Custodian Corporate Benefit Collection files |
| `custodian-ca-files` | `CA-Import/` | Short-lived copies used to load those files into tables; deleted after loading |
| `client-transaction-files` | `transactions/` | Committed trade files (the permanent copy) |
| `client-transaction-files` | `transactions-meta/` | Lists of the client codes and ISINs found in each committed file |
| `upload-data-bucket` | – | Bhav Copy files, holdings exports, cash exports |
| `export-app-data` | – | Dividend export, single-account transaction export |

### 3.3 Tracking tables (used by every module)

| Table | What it holds | Who writes it |
| --- | --- | --- |
| `Jobs` | One row per background task, with its status (running, completed, failed). Screens check it to see when work is finished. | Almost every module |
| `JobStatusPerAccount` | Progress per client for long corporate action and holdings jobs | Corporate action jobs, holdings jobs |
| `Notifications` | One row per task shown on the notification bell | All modules that start background work |

---

## 4. Modules

### 4.1 Client and Security Masters

**Business purpose.** Keep the list of all client accounts (`clientIds`) and all shares and funds (`Security_List`). Every other module looks up names and codes here.

**How it works.**

1. After a trade commit, the job `UpdatesSecurity_ClientMasters` reads the committed file and collects every client code and ISIN in it. It saves both lists to `client-transaction-files/transactions-meta/`.
2. It adds the new ones as short "stub" rows by a bulk write. A stub client row holds only the virtual code, and a stub security row holds only the ISIN. Rows that already exist are skipped.
3. It then starts `EnrichTransactionSecurity`, which fills the security code and name on the new trade rows from `Security_List`, about 100 ISINs at a time. Only blank fields are filled. It baton-passes when time runs low.
4. Outside the daily flow, merger and demerger add the new company's ISIN to `Security_List`, and Update ISIN can add or change a security code and name.

| Table | Added | Changed | Deleted |
| --- | --- | --- | --- |
| `clientIds` | New virtual codes from a committed file | – | – |
| `Security_List` | New ISINs from a committed file, merger, demerger, Update ISIN | Security code and name (Update ISIN, merger) | – |
| `Transaction` | – | Security code and name filled in after commit | – |

**What depends on it.**

- **Custody Matching** uses `clientIds` to turn virtual codes into actual codes. A virtual code with no actual code becomes an "Unmapped code" exception.
- **Consolidated reports and Holding Comparison** use the same link to add virtual codes together.
- **Every corporate action screen** picks its share from `Security_List`.

**Watch out.** A stub client row has no actual code. The actual-code link has to be filled in separately. Until then, that client's trades show as Unmapped in matching.

---

### 4.2 Bhav Copy (closing prices)

**Business purpose.** Load the exchange's daily closing prices, used only to value holdings (market value = quantity × closing price).

**How it works.**

1. The operator uploads the exchange's Bhav Copy file. Only `.csv` files are accepted.
2. The file is saved to `upload-data-bucket` as `bhavcopy-<time>-<file name>`.
3. A Catalyst bulk write loads every row into `Bhav_Copy`. It only adds rows, so uploading the same file twice adds the prices twice.
4. Screens and reports that show market value read the latest closing price for each share from `Bhav_Copy`.

| Table | Added | Changed | Deleted |
| --- | --- | --- | --- |
| `Bhav_Copy` | One row per security per day | Only by Update ISIN (ISIN rename) | – |

**What depends on it.** Only valuation: Analytics, holdings reports and exports. Quantities and cash never change because of a price file. A missing price shows as a market value of zero.

---

### 4.3 Field Mapping

**Business purpose.** Every custodian sends files in its own layout. Field Mapping lets the operator say once which column holds which field. After that, every file of that layout is read the same way.

**Where it is used.** Transaction Upload (custodian and FA files), Custodian Upload (Ex-Entitlement and Benefit Collection files), and Holding Comparison (custodian holding statement).

**How it works.**

1. The operator picks a file. It is **not uploaded yet**.
2. The system reads the header and the first few rows and opens the mapping window.
3. Each required field is matched to one of the file's own columns, picked from a drop-down. The first time, nothing is filled in. Every field must be mapped, and one column can feed only one field.
4. **Save mapping** stores it, and the button turns into **Upload**.
5. Next time the saved mapping is used. If a mapped column is missing or has changed shape, that field is cleared and shown in red. It must be fixed before uploading.

**Where it is kept.** Stratus `temporary-files/field-maps/<layout>.json`. No table.

**Effect of a change.** Saving a new mapping marks earlier match runs as **superseded**, because they were read with the old mapping. They must be matched again before commit.

---

### 4.4 Transaction Upload and Custody Matching

**Business purpose.** Bring the day's trades into the books, but only after comparing them with the custodian's record of the same trades.

**Files it holds.**

- **Custodian side:** one deal file per scheme and trade date. Account types: Residential, NRE/NRO/NDPMS, Mutual Funds.
- **FA side:** our trade file per trade date. Kinds: Equity, Mutual Funds.
- Everything is filed under the **upload date** the operator picks (never a future date). Each upload date is its own set of files.

#### How it works

1. **Upload.** Each file is checked as soon as it is picked. A file whose layout does not fit the chosen account type is rejected immediately. After mapping, each row must carry the file's trade date and correct values (a real ISIN, a number for quantity, and so on). All dates are turned into year-month-day.
2. **Same file twice.** If a file is uploaded again for the same upload date, trade date and slot, the operator is asked whether to **replace** it. Replacing deletes the old file and its row.
3. **Match (job `CustodyMatchRunner`).** It runs in stages and saves its place before each one, so a crash can resume:
   - S1: build one FA file with every virtual code turned into its actual code.
   - S2: build one custodian file from all scheme files.
   - S3: check that both files were saved correctly.
   - S4: compare. The key is **actual code + ISIN + trade date + buy/sell**, and only **quantity** is compared. Mutual fund trades are compared only with Mutual Funds custodian files, and all other trades only with Residential and NRE/NRO/NDPMS files.
   - S5: write the exceptions and the difference file.
   - S6: final status and counts.
4. **Exceptions.** Quantity mismatch, Missing in custody, Missing in FA, Date mismatch, Buy/sell mismatch, Unmapped code. A date or buy/sell mismatch is reported when exactly one unmatched trade on each side agrees on client, ISIN and quantity.
5. **Commit (Gate 2).** Allowed once the run has finished. Open exceptions do not block it, but the operator gets a strong warning. **Each trade date can be committed only once**, whichever upload date it came from. That stops the same trades being booked twice. Equity and Mutual Funds files of the day are combined into one import file.
   - **Several dates.** When the operator ticks several dates for matching, the screen shows every date's result, one Exception details list and one Export for all of them. **Commit Transaction Files** commits every shown date that is not committed yet, together, as **one** import file. Every date is checked first; if one fails a check, nothing is committed and the message names the date.
   - **One import at a time.** The import is refused while an earlier import's load, holdings update or cash update is still running. Those jobs treat every trade loaded since their own import started as new, so an overlapping import would be counted twice in holdings or lose cash entries. A job still marked running 3 hours after its import started is taken as dead: the import goes ahead with a warning to check it.
   - **Committing a date again.** A date opens again when its bulk write fails (`releaseImportMarker`, stage detail `Import failed …`) or when Transaction Deletion deletes trades of its file (`reopenRunsForDeletedTrades`, stage detail `Re-opened …`; the newest 300 committed runs whose file date, or FA file date range, covers a deleted trade's `TRANDATE`). Committing such a date leaves out the rows of its file still in `Transaction`, counted per trade on account + ISIN + type + trade date + settlement date + quantity + rate (`dropTradesStillBooked` in `CustodyMatchRun.js`). The reply carries `skippedAlreadyBooked`; if nothing is left, the commit is refused with `NOTHING_NEW_TO_COMMIT`. Example: 200 trades committed, 3 deleted, the corrected 200-trade file books 3.
6. **Import.** The file is saved to `client-transaction-files/transactions/`, loaded into `Transaction` by a bulk write, and then three jobs are queued 10 seconds apart (see the job table below). Before the holdings and cash jobs are queued, `Jobs` rows `HOLD_<import start ms>` and `CASH_<import start ms>` are set to `RUNNING`; `CalculateHoldingMaster` and `Cal_CB_Append_TxnUpload` set them to `COMPLETED` at their real end (holdings also `FAILED` when chunks failed). A crash Catalyst will retry leaves the row `RUNNING`. If the bulk write is still going after 45 minutes, its `TxnCsv_*` row becomes `TIMED_OUT` (not `FAILED`) and is polled every 5 minutes for up to 3 hours; a late finish, by poll or callback, still queues the holdings and cash jobs, and a new import is refused while the row is `TIMED_OUT`. After 3 hours it is left `TIMED_OUT` and the runs stay locked. A client rebuilt because of a late trade (see 4.5 and 4.6) gets its own row, `HOLD_<ms>_R<chunk start>_<account>` or `CASH_<ms>_R_<account>`, which `RebuildHoldingtable` / `Cal_CB_Per_TNX` set to `COMPLETED`; a new import waits for those too.

#### Run statuses

`QUEUED` → `RUNNING` → `COMPLETED_CLEAN` / `COMPLETED_WITH_EXCEPTIONS` / `FAILED`. A run becomes `SUPERSEDED` when its files or the mapping change after it ran.

#### Tables, files and jobs

| Table | Added | Changed | Deleted |
| --- | --- | --- | --- |
| `Custody_Match_File` | One row per uploaded file (upload date, trade date, side, type, scheme) | – | When a file is replaced or removed, or a day is cleared |
| `Custody_Match_Run` | One row per match attempt | Stage, progress, status, counts; "imported" marker after commit | By the nightly cleanup |
| `Custody_Match_Exception` | One row per difference | Rewritten if the run is retried | By the nightly cleanup |
| `Transaction` | Every trade row of the committed file | Security code and name (by `EnrichTransactionSecurity`) | – |
| `Jobs` / `Notifications` | Import and match task rows; `HOLD_<ms>` and `CASH_<ms>` rows per import | Status | – |

| Stratus | What happens |
| --- | --- |
| `temporary-files/FA-Transaction/<date>/` | Raw FA files, final FA file, difference file |
| `temporary-files/Custodian-Transaction/<date>/` | Raw custodian files, final custodian file |
| `temporary-files/Custodian-History/` | Copy of each custodian upload; only the newest 5 are kept |
| `client-transaction-files/transactions/` | The committed file |

| Job | Pool | Starts |
| --- | --- | --- |
| `CustodyMatchRunner` | `CustodyMatch` | – |
| `UpdatesSecurity_ClientMasters` | `UpdateMasters` | `EnrichTransactionSecurity` |
| `CalculateHoldingMaster` | `UpdateMasters` | Up to 10 `CalculateHoldingWorkers` at a time, about 200 client-share pairs each; baton-passes itself |
| `Cal_CB_Append_TxnUpload` | `UpdateMasters` | Itself again (baton pass after about 13 minutes) |

**What a commit changes elsewhere.** New clients and securities appear in the Masters. Holdings and the cash passbook of every client in the file update. Analytics and reports show the new position. If a committed trade is dated **before** rows already saved for its client and share (a late trade, or dates committed out of order), that client's shares from the file are rebuilt instead of added on top (see 4.5), and the passbook is rebuilt (see 4.6). The rebuild replays corporate action records **as saved** (`keepCaRecords`), the same as after a deletion: a split's ratio applies to the new holding, but a bonus, merger or demerger quantity worked out on the old holding stays, and a client with no record does not get the action. The operator re-opens the action on its page (Split, Bonus or Dividend). The late client shows with no status, and **Send as Receivable** then **Confirm & Apply** apply the action to that client only (see 4.9, "Late clients").

---
 
### 4.5 Holdings engine

**Business purpose.** Work out what every client owns, share by share, at any date. Nobody types holdings in. They are calculated from trades and corporate actions.

**Rules.**

- **FIFO:** each purchase is a separate lot, and sales use up the oldest lot first.
- A **purchase** counts from its **settlement date**, a **sale** from its **trade date**.
- Same-day order: trades, then split, then bonus, then demerger, then merger.
- **Average cost (WAP)** = cost of shares still held ÷ shares still held.
- Tiny leftovers near zero from arithmetic are treated as zero. Real fractions (for example mutual fund units) are kept.

**Table `Holdings`.** One row per event (trade or corporate action) per client and share. The row carries the running holding and cost after that event. To know the holding on a date, the system reads the rows up to that date in the order they were written.

**How it works.** Holdings are written in two ways.

*Add, after a trade commit:*

1. The import lists every client and share pair in the committed file and saves the list to `client-transaction-files/transactions-meta/holdings-pairs-<time>.json`.
2. `CalculateHoldingMaster` splits the list into chunks of about 200 pairs and starts one `CalculateHoldingWorkers` job per chunk, at most 10 at a time. It saves its progress to Stratus and baton-passes when time runs low.
3. Each worker looks only at the trades of this import. If a pair has only new purchases, it carries on the quantity, cost and average cost from the pair's last `Holdings` row and adds one row per purchase.
4. If a pair has a new sale, the worker first replays the pair's existing `Holdings` rows to find the open lots, then applies the new trades by FIFO and adds one row per trade.
5. Earlier `Holdings` rows are never changed or deleted. Corporate actions are not replayed here.
6. **Late trades.** Before adding anything for an account, the worker checks each pair: is a new trade dated before a `Holdings` row saved before this import (same date rule as the read side: OPI by settlement, merger/demerger by `CA_DATE`, the rest by trade date)? If any pair is, nothing is added for the account. Its ISINs from this chunk go to `RebuildHoldingtable` (`keepCaRecords: "true"`, `importJobRowName`), with a `RUNNING` row `HOLD_<ms>_R<chunk start>_<account>` in `Jobs`. A retry of the chunk skips an account whose row exists (unless `FAILED`), so its trades are never added on top of the rebuild. Example: holding 100; 9 Sept (buy 50) committed, then 7 Sept (buy 20). Added on top, the 7 Sept row would carry 170 and show as the holding on 8 Sept; rebuilt, 8 Sept shows 120 and today 170.

*Rebuild, after a corporate action, a transaction deletion or a late trade:*

1. The corporate action sends `RebuildHoldingtable` (pool `CorporateActions`) the affected clients, in chunks, and the shares involved.
2. The job reads every trade and corporate action of those clients and shares, and replays the whole history using FIFO.
3. It deletes their old holding rows and writes the new rows to `Holdings` (and to `Split_Account` for a split).
4. Each chunk marks its own progress row when it finishes. When the last chunk is done, the corporate action shows as completed on the bell.

**FIFO example:**

- January: buy 100 at Rs 10 (Lot A). March: buy 100 at Rs 12 (Lot B).
- June: sell 150 at Rs 15. The sale uses all 100 of Lot A (cost Rs 1,000) and 50 of Lot B (cost Rs 600).
- Profit = 2,250 − 1,600 = Rs 650. Left: 50 shares at Rs 12, average cost Rs 12.

| Table | Added | Changed | Deleted |
| --- | --- | --- | --- |
| `Holdings` | New rows after commit; all rows of a client+share after a rebuild (corporate action, deletion, late trade) | Renamed by Update ISIN | Before every rebuild, for the client+shares in scope |
| `Split_Account` | Re-written by a rebuild (old and new holding per split) | Keeps Receivable status and received date | Re-written by a rebuild |

**Other holdings jobs** (manual or older paths, not part of the daily flow): `HoldingUpdateManually`, `CalculateHoldingPerAccount`, `CalculateHoldingsPerAccount`.

**What depends on it.** Analytics, all holdings reports, Holding Comparison (FA snapshot), and every corporate action preview, which reads holdings on the record date.

---

### 4.6 Cash passbook

**Business purpose.** Keep a running cash balance per client account, like a bank passbook.

**Rules.**

- Money in (sales, deposits, dividends, interest and other income codes) is placed by its **settlement date**. Money out (purchases, withdrawals, fees and charges) is placed by its **trade date**.
- STT is always taken from the balance: a sale adds proceeds minus STT, and a purchase takes cost plus STT.
- The client's first cash deposit (`CS+`) sets the opening balance.
- A dividend adds the net amount (after tax) on the record date. Later lines' balances are adjusted to include it.

**How it works.**

1. After a trade commit, `Cal_CB_Append_TxnUpload` finds every client with trades from this import (the `Transaction` rows created after the import started), up to 300 clients at a time.
2. For each client it first removes any passbook lines this import already wrote, so a re-run after a crash never counts a trade twice.
3. It carries on from the client's last passbook line. Each new trade adds or takes money by the rules above, and its line gets the running balance and the next sequence number.
   - **Late trades.** If the earliest new cash event is dated before a line already in the passbook (same date rule: money in by settlement date, the rest by trade date), nothing is added. The account goes to `Cal_CB_Per_TNX` for a full rebuild (`importJobRowName`), with a `RUNNING` row `CASH_<ms>_R_<account>` in `Jobs`. A re-run of this job skips an account whose row exists (unless `FAILED`) before its clean-up delete, which would otherwise wipe the rebuilt lines (they were all written after the import started). Example: balance Rs 1,00,000; 9 Sept (buy Rs 40,000) committed, then 7 Sept (sale Rs 10,000 paid 8 Sept). Rebuilt: 8 Sept Rs 1,10,000, after 9 Sept Rs 70,000.
4. After about 13 minutes it saves the last client it finished and starts a fresh copy of itself, which carries on from the next client.
5. A confirmed dividend adds its line through the Dividend apply job (see 4.12). The line's Price is the rate and its Quantity the holding, but its Amount is the **net** cash (gross − tax), so Price × Quantity differs from Amount when tax was deducted. The tax itself is kept on `Dividend_Record`, not on the passbook line.
6. A full rebuild of the passbook is done by `Cal_CB_Per_TNX`: by hand, per client after Transaction Deletion, and per client for a late trade in a commit (step 3).

| Table | Added | Changed | Deleted |
| --- | --- | --- | --- |
| `Cash_Balance_Per_Transaction` | One line per cash movement | Later balances when a dividend is inserted earlier in time | Only by the full rebuild job `Cal_CB_Per_TNX` (manual, Transaction Deletion, late trade) |
| `Cash_Ledger` | Older daily ledger, written by `CashCalculation` (manual, pool `Finance`): each run deletes an account's rows and rebuilds one closing balance per day from all its trades (settlement date, or trade date when there is none) | Same | Same |

**Watch out.**

- `Cash_Ledger` and `Cash_Balance_Per_Transaction` are two separate cash systems and are not kept in step. The passbook (`Cash_Balance_Per_Transaction`) is the one used by the screens and uploads.
- `Cal_CB_Per_TNX` deletes **every** passbook line of an account, dividend lines included, and rebuilds from `Transaction` plus the `RECEIVED` `Dividend_Record` rows (`fetchReceivedDividendEvents`). A change to how the Dividend apply job writes a credit must be mirrored there, or the rebuild loses that cash.

---

### 4.7 Analytics

**Business purpose.** Look at one client or one share. It is **read only**.

- **By account:** holdings with quantity, average cost, holding value, last price and market value, plus an equity and cash summary. An "as on" date shows a past position.
- **By ISIN:** every account holding that share.
- **Drill down:** the full history of one share in one account.
- **Transaction tab:** the account's trades, page by page.

**How it works.**

1. The operator picks an account (or a share, for "By ISIN") and, if needed, an "as on" date.
2. The system reads that account's `Holdings` rows up to the date, in the order they were written, and works out the quantity and average cost of each share on that date.
3. Market value = quantity × the latest closing price from `Bhav_Copy`.
4. The cash summary is read from the passbook, and the Transaction tab reads `Transaction` page by page.
5. Nothing is written.

| Reads | Writes |
| --- | --- |
| `Holdings`, `Bhav_Copy`, `clientIds`, `Transaction`, cash tables | Nothing |

---

### 4.8 Custodian Upload (corporate action files)

**Business purpose.** One place to upload the custodian's two corporate action files, so every corporate action screen reads the same files. Nobody has to upload them again for each action.

| File | Tells us | Read by |
| --- | --- | --- |
| **Ex-Entitlement** | What each client is **entitled to**: holding on the record date, shares or amount due, ratio, dividend rate, payment date | Stage 1 of Split, Bonus, Dividend |
| **Corporate Benefit Collection** | What each client **actually received**: quantity or amount, tax deducted, date | Stage 2 of Split, Bonus, Dividend |

*Merger and Demerger do not read these files yet.*

**How it works.**

1. The operator picks the file kind (Ex-Entitlement or Benefit Collection), the report date and the file. Only CSV files are accepted.
2. The file is checked against its saved Field Mapping (see 4.3). A missing mapped column opens the mapping window. The fields the corporate action modules need must be mapped, and the file must have data rows.
3. The file's fingerprint is compared with earlier uploads. Exactly the same file is refused (`DUPLICATE_FILE`) unless the operator chooses Replace, which removes the earlier upload and its rows first.
4. The file is saved to `custodian-ca-files/CA-Ex-Entitlement/<date>/` or `CA-Benefit-Collection/<date>/`, and a `CA_Custodian_Upload` row is added with status Importing.
5. Using the mapping, the rows are written into a short-lived file in `CA-Import/` and loaded into `CA_Ex_Entitlement` or `CA_Benefit_Collection` by a bulk write. When the load ends, the status becomes Done or Failed.
6. Split, Bonus and Dividend then read their rows from these tables, as described below.

If files were stored before the tables existed (or after deploying to a new environment), **Import missing files** loads every stored file that has no table rows yet.

**How a module finds its rows.** By **ISIN + corporate action type + record date**, from the uploaded file with the **latest report date** that contains that event. Older uploads are still searched, so an event in an older file is not lost.

| Table | Added | Changed | Deleted |
| --- | --- | --- | --- |
| `CA_Custodian_Upload` | One row per uploaded file | Import status (importing, done, failed) | When the file is removed or replaced |
| `CA_Ex_Entitlement` | Every row of an Ex-Entitlement file | – | With its upload (in the background) |
| `CA_Benefit_Collection` | Every row of a Benefit Collection file | – | With its upload (in the background) |

**Rules.** Exactly the same file uploaded again is refused (`DUPLICATE_FILE`) unless the operator chooses Replace. Removing a file deletes every upload of that kind for its date. The last 5 uploads of each kind are listed for download.

---

### 4.9 Corporate actions: the two-stage custodian check

Split, Bonus and Dividend all follow the same path. Holdings or cash change **only after the custodian confirms receipt**.

**How it works.**

1. **Preview.** The operator enters the share, the ratio (or the dividend rate) and the record date. The system works out each client's holding on the record date and what the client should get.
2. **Stage 1: compare with Ex-Entitlement.** The preview is compared with the custodian's Ex-Entitlement rows for the same share, action and record date, by actual code. For a dividend this check is part of the preview. The result is saved as a CSV.
3. **Send as Receivable.** One row per client in the preview is saved with status `RECEIVABLE`. Nothing changes in holdings or cash yet. If saving stops half-way, the rows this click saved (the `RECEIVABLE` ones) are removed so it can be sent again; rows already `RECEIVED` are never touched.
4. **Stage 2: compare with Benefit Collection.** The Receivable clients (virtual codes turned into actual codes) are compared with what the custodian says was received. The result is saved as a CSV.
5. **Confirm & Apply.** The Benefit Collection file is checked again first, so a bad file changes nothing. The received dates are taken from it, the event is saved, and a background job applies it.
6. Clients are marked `RECEIVED` only once their part is applied. If anything fails, they stay Receivable and Confirm can be pressed again.
7. The bell shows the action as running until every background piece, including the holdings rebuild, has finished.
8. The page keeps the form after Confirm & Apply and reloads the preview, so the Status column shows `RECEIVED` (for Bonus and Dividend, once the background job has finished).

**Stage 1 rules.**

- Records are fetched by **record date**.
- A different record date in the file **blocks** the check. A different ratio is only a **warning**.
- **Send as Receivable** marks every client in the preview as `RECEIVABLE`. Clients found only in the custodian file are reported but not booked.

**Stage 2 rules.**

- The Receivable clients are compared with what the Benefit Collection file says was received.
- **Confirm & Apply** marks clients `RECEIVED`, saves the received date, and starts the apply job.

**Result labels.** Matched, Qty (or Amount) Mismatch, Missing in Custodian, Missing in FA, Not in Benefit File, **Already Received**. In stage 2, a client the custodian lists again but who was already `RECEIVED` for the same event shows as Already Received: our figure, no difference, not counted as a break, listed last. (The Dividend page's own stage 1 grid uses its own labels: Matched, Mismatch, Missing in file, Missing in system, Already paid.)

**Late clients (event already applied).** A client can appear after an event is applied, for example a trade dated before the record date that is committed later (see 4.4).

1. The preview leaves out the event's own result row on the record date (the `SPLIT` or `BONUS` row in `Holdings`), so the old and new figures stay correct after the event is applied.
2. If the event is applied, its per-client table has rows, and a client in the preview has no row, the page reopens at **stage 1**.
3. **Send as Receivable** saves rows only for clients with no row yet. Clients already `RECEIVED` are left alone. If every client already has a row, it answers "already applied".
4. **Confirm & Apply** applies the event to the `RECEIVABLE` clients only. Split saves no second `Split` row and rebuilds only those clients. Bonus and Dividend jobs skip clients already done.
5. Events applied before the per-client tables existed have no rows, so they stay `RECEIVED` and cannot be re-sent.

**Where results are kept.** Each comparison is saved as a CSV in `temporary-files/CA-Differences/<split|bonus|dividend>-<entitlement|benefit>/<record date>/`. The page lists the newest 10 of each stage for download (Split, Bonus and Dividend pages).

**Preview export.** The preview can be exported per page. It carries a `STATUS` column (`RECEIVABLE`, `RECEIVED` or blank) and a `DIFFERENCE` column: the shares added for Split and Bonus, and our dividend amount − the custodian's Ex-Entitlement amount for Dividend. `DIFFERENCE` is shown on every row while any client is not yet `RECEIVED`, and left out once every client is `RECEIVED`.

**Which stage an event is in** is read from the database (by ISIN, record date and ratio), so the page always reopens at the right stage. Old rows with a blank status count as already applied.

---

### 4.10 Split

**Business meaning.** Each share is divided into more shares. Total cost stays the same.

**Maths.** New holding = old holding × "investor gets" ÷ "for every". Fractions are kept (3 shares on 2-for-3 = 4.5). Old lots are closed and replaced by new lots carrying the same total cost.

**How it works.**

1. **Preview:** for every client holding the share on the record date, the old and new holding.
2. **Stage 1 and Send as Receivable** (see 4.9): one `Split_Account` row per client, with old and new holding, status `RECEIVABLE`.
3. **Stage 2 and Confirm & Apply:** the Benefit Collection file is checked. A split already fully applied (no client left `RECEIVABLE`) is skipped.
4. The `Split` row is saved. If the split was already saved (an earlier Confirm, or late clients being added), it is not saved again. The duplicate check is ISIN + record date + ratio.
5. `RebuildHoldingtable` jobs (pool `CorporateActions`) are queued, 100 clients per job: for a first apply, every client holding the share on the record date; for an already-applied split, only the `RECEIVABLE` (late) clients.
6. Every bonus on the same share with a record date on or after the split is re-sized: an `UpdateBonusTable` job runs in recompute mode for each, and then queues its own rebuild.
7. The clients' `Split_Account` rows are marked `RECEIVED` with the received date.

| Table | Added | Changed | Deleted |
| --- | --- | --- | --- |
| `Split_Account` | One row per client at Send as Receivable (late clients: only the missing ones) | Status → RECEIVED, Received_Date; old/new holding re-written by rebuild | Only the `RECEIVABLE` rows of a failed Send are removed so it can be sent again |
| `Split` | The split event at Confirm & Apply | – | – |
| `Holdings` | Re-calculated rows | – | Old rows of the affected clients and share |
| `Bonus` | – | Bonus shares re-sized if a bonus on the same share has a record date on or after the split | – |

**Effect on other modules.** Holdings, Analytics and reports show the new quantity and lower average cost. Any later bonus on the same share is recalculated. The split report reads `Split_Account`.

---

### 4.11 Bonus

**Business meaning.** Free extra shares for existing holders.

**Maths.** Bonus shares = holding on the record date × "bonus shares" ÷ "for every held", **rounded down**. Added as a new lot at **zero cost**, dated on the record date. Clients who would get 0 bonus shares are left out. *Paying the fraction as cash is not built yet.*

**How it works.**

1. **Preview:** each client's holding on the record date and bonus shares.
2. **Stage 1 and Send as Receivable** (see 4.9): one `Bonus_Record` row per client, status `RECEIVABLE`.
3. **Stage 2 and Confirm & Apply:** the received dates are taken from the Benefit Collection file, and job `UpdateBonusTable` (pool `CorporateActions`) is queued.
4. The job works out each client's holding on the record date from `Holdings`, leaving out this bonus's own `BONUS` row (so a re-run sizes on the holding before the bonus), and adds one `Bonus` row per eligible client. A client who already has this bonus is skipped: its per-client job status is `SUCCESS` from the earlier run (same job name `BON_<ISIN>_<record date>`), or its `Bonus` row already exists.
5. It saves the bonus event row in `Bonus_Record` (once) and marks the clients' rows `RECEIVED`.
6. It queues `RebuildHoldingtable` for the clients it added, so the bonus lot appears in `Holdings`. For late clients (see 4.9) only they are added and rebuilt; no change to the deployed job was needed for this.

| Table | Added | Changed | Deleted |
| --- | --- | --- | --- |
| `Bonus_Record` | Per-client rows at Send as Receivable (late clients: only the missing ones); one event row at apply | Status, Received_Date | Only the `RECEIVABLE` rows of a failed Send are removed |
| `Bonus` | One row per eligible client | Re-sized by a later split (recompute) | – |
| `Holdings` | Re-calculated rows | – | Old rows of the affected clients and share |

**Duplicate guard.** A bonus already applied for the same ISIN, client and date is skipped.

---

### 4.12 Dividend

**Business meaning.** Cash paid per share held. **Shares never change.**

**Maths.**

- Gross = holding on the record date × rate.
- Stage 1 also checks the rate and payment date against the Ex-Entitlement file.
- Stage 2 rule: **our gross − tax deducted (from the custodian file) = amount received**.
- Cash credited = **gross − tax**, on the record date. When an actual code has several virtual codes, the tax is shared between them in proportion to their amounts.

**How it works.**

1. **Preview:** each client's holding on the record date and gross amount, with Tax, Net Amount, Status and Received Date for clients already paid. **Compare with Custodian File** checks holding, amount, rate and payment date against the Ex-Entitlement file and saves the comparison CSV (stage 1 history).
2. **Send as Receivable:** one `Dividend_Record` row per client with the gross amount, status `RECEIVABLE`. For an already-paid dividend only late clients with no row are sent (see 4.9).
3. **Stage 2:** the Receivable clients are compared with the amounts in the Benefit Collection file. Clients already paid show as Already Received.
4. **Confirm & Apply:** only the clients the Benefit Collection file shows as paid are taken. Their received dates are saved, and job `UpdateDividendData` (pool `CorporateActions`) is queued with those clients and their tax.
5. The job saves the `Dividend` event (once) and works out each client's holding on the record date.
6. For each paid client it stores the gross `Dividend_Amount`, the `Tax_Amount` and the `Net_Amount` (gross − tax) on `Dividend_Record`, adds one net cash line to `Cash_Balance_Per_Transaction` on the record date, then marks the client `RECEIVED`.
7. Clients not paid yet stay `RECEIVABLE`. They can be confirmed later with a newer Benefit Collection file.

**Example.** 200 shares, rate ₹1, custodian tax ₹20: `Dividend_Amount` 200, `Tax_Amount` 20, `Net_Amount` 180; the passbook gets **+180** on the record date.

| Table | Added | Changed | Deleted |
| --- | --- | --- | --- |
| `Dividend_Record` | One row per client at Send as Receivable (gross amount; late clients: only the missing ones) | Status → RECEIVED, Received_Date, Tax_Amount, Net_Amount | Only the `RECEIVABLE` rows of a failed Send are removed |
| `Dividend` | The dividend event, once | – | – |
| `Cash_Balance_Per_Transaction` | Net dividend line on the record date | Balances and sequence of later lines | – |

**Rules.** Only clients listed in the Benefit Collection file are paid. The rest stay `RECEIVABLE` and can be confirmed later with a newer file. Clients already paid, or present only on the custodian side, are skipped. **No holdings rebuild** happens for a dividend. The corporate action report's `CASH_RECEIVED` is `Net_Amount` (gross for rows paid before `Net_Amount` existed).

**Deployment note.** The deployed `UpdateDividendData` must be the current one: an older copy ignores the tax and credits the gross amount.

---

### 4.13 Merger

**Business meaning.** Old company (or companies) absorbed into a new one. Old shares are cancelled and new shares are given.

**Maths (lot by lot).** New shares = old lot quantity × Ratio 1 ÷ Ratio 2, **rounded down**. The **whole cost** of the old lot moves to the new shares, and the original buy dates are kept. Lots that would give 0 new shares are skipped.

**No custodian stage yet:** Preview → Apply.

**How it works.**

1. The operator enters the old company (or companies), the new ISIN with its code and name, the ratio and the record date. The preview shows each client's new shares.
2. **Apply** checks that the same merger is not already running, then queues job `MegerFn` (pool `CorporateActions`). The page checks its status until it finishes.
3. The job finds every client who traded the old ISIN. It reads their trades and earlier corporate actions on it up to the record date, and works out the open lots by FIFO.
4. For each lot it works out the new shares by the maths above. Lots that give 0 new shares are skipped.
5. It writes one row per lot to a file in `upload-data-bucket` and loads it into `Merger` by a bulk write. It adds one `Merger_Record` row and adds or updates the new ISIN in `Security_List`.
6. It queues `RebuildHoldingtable` for the affected clients, which rebuilds the old and the new ISIN.

| Table | Added | Changed | Deleted |
| --- | --- | --- | --- |
| `Merger`, `Merger_Record` | Merger rows per client and lot | – | – |
| `Security_List` | New ISIN if missing | New ISIN's code and name | – |
| `Holdings` | Re-calculated rows for old and new ISIN | – | Old rows of those ISINs |

**Effect on other modules.** The old ISIN shows zero and is hidden from holdings views and rebuilds. The new ISIN appears with the carried-over cost.

---

### 4.14 Demerger

**Business meaning.** Part of a company is split into a new company. Holders **keep** the old shares and **also** get new shares.

**Maths (lot by lot).** New shares = old quantity × Ratio 1 ÷ Ratio 2, **rounded down**. The chosen **% of cost** moves to the new shares, and the rest stays with the old. Old quantity is unchanged; only its average cost falls.

**No custodian stage yet:** Preview → Apply.

**How it works.**

1. The operator enters the old ISIN, the new ISIN, the ratio, the % of cost that moves and the record date. The preview shows each client's new shares and cost split.
2. **Apply** queues job `DemergerFn` (pool `CorporateActions`). The page checks its status until it finishes.
3. The job saves the `Demerger` event (once) and finds every client holding the old ISIN in `Holdings`. It reads their lots up to the record date.
4. For each lot it works out the new shares and moves the chosen % of cost to them. The original trade date is kept on both sides.
5. It writes `Demerger_Record` rows for the old side and the new side, and adds the new ISIN to `Security_List` if it is missing.
6. It queues `RebuildHoldingtable` for the affected clients.

| Table | Added | Changed | Deleted |
| --- | --- | --- | --- |
| `Demerger`, `Demerger_Record` | Event and per-lot rows | – | – |
| `Security_List` | New ISIN if missing | – | – |
| `Holdings` | Re-calculated rows for old and new ISIN | – | Old rows of those ISINs |

---

### 4.15 Holding Comparison

**Business purpose.** End-of-period check. Do our holdings on a date agree with the custodian's holding statement? It is **report only** and never changes the books.

**How it works.**

1. **Custodian file.** The operator picks a date and uploads the custodian's holding statement. The whole file is checked against its saved Field Mapping (a mismatch opens the mapping window), then saved in `temporary-files/Custodian-Holdings/<date>/`.
2. **Our holdings.** **Generate FA holdings** starts job `FAHoldingsGenerator` (pool `HoldingComparison`). It works out every client's holding on the date from `Holdings`, adds virtual codes together under their actual code, and saves `FA-Holdings/<date>/fa-holdings-<time>.csv`. Its progress is on the `Jobs` row `HCFA_<date>`.
3. **Run (Gate 1).** Both files must exist. If the same two files were already compared (or are being compared), that result is shown instead of running again.
4. Otherwise a `Holding_Comparison_Run` row is added and job `HoldingComparisonRunner` starts. It reads the custodian file with the mapping, reads our snapshot, compares them, writes the difference CSV to `Holding-Differences/<date>/`, and saves the counts and final status.
5. The operator views the differences on screen or downloads them.

**Rules.**

- The key is **actual code + ISIN**, and only **quantity** is compared.
- Results: Quantity mismatch, Missing in custody, Missing in FA.
- A zero FA holding with no custodian row is not a difference.
- The same two input files give the same answer, so a finished run is reused instead of recalculated.
- A new custodian file supersedes earlier runs for that date.

| Table | Added | Changed | Deleted |
| --- | --- | --- | --- |
| `Holding_Comparison_Run` | One row per run | Stage, status, counts | – |
| `Jobs` | `HCFA_<date>` for FA holdings generation | Status | – |

The FA snapshot job baton-passes using `temporary-files/FA-Holdings-Work/<date>.json`.

---

### 4.16 Update ISIN

**Business purpose.** When a company's ISIN changes, replace the old ISIN with the new one everywhere, without changing any quantity or cost.

**How it works.** There are two tools.

*Rename ISIN:*

1. The operator enters the old and new ISIN. Both are required and must differ. The old ISIN must exist in `Security_List`, and the new one must not already belong to a security.
2. Job `UpdateISIN` (pool `UpdateMasters`) opens a `Jobs` row and starts `UpdateISINWorker`.
3. The worker renames the ISIN in 300-row batches, table by table (list below). It baton-passes on large tables (`Transaction` can hold 10–12 lakh rows). Holdings are **renamed in place, not recalculated**.
4. The page checks the job status until it finishes.

*Apply new security details:*

1. The operator enters an ISIN and a new security code, name, or both.
2. Job `UpdateISIN` runs in "apply new" mode and updates the code and name for that ISIN in `Security_List` and `Transaction`.

**Tables renamed:** `Security_List`, `Transaction`, `Holdings`, `Bhav_Copy`, `Cash_Balance_Per_Transaction`, `Bonus`, `Bonus_Record`, `Split`, `Dividend`, `Dividend_Record`, `Merger`, `Merger_Record`, `Demerger`, `Demerger_Record`, `Temp_Transaction`, `Temp_Custodian`. In merger and demerger tables both the old-side and new-side ISIN columns are renamed.

**Effect on other modules.** Every screen and report shows the new ISIN. History is kept under it.

---

### 4.17 Reports and exports

**Business purpose.** CSV files for clients, management and auditors. Large exports run in the background, and the bell says when the file is ready.

**How it works.**

- **Immediate reports:** the server reads the data, builds the CSV, and either streams it to the browser or saves it and returns a download link.
- **Background reports:**
  1. The button adds a `Jobs` row and queues the export job in pool `Export`.
  2. The job reads the data, writes the CSV to its bucket, and marks the `Jobs` row `COMPLETED` (or `FAILED`).
  3. The page and the bell check the status. The download is allowed only once the status is `COMPLETED`. Earlier files are listed in the history.

| Report | Runs as | Job / pool | File saved in |
| --- | --- | --- | --- |
| Holdings, one account (scheme-wise or consolidated) | Immediately | – | `upload-data-bucket` |
| Holdings, one ISIN | Immediately | – | Streamed download |
| Holdings, all clients (scheme-wise or consolidated) | Background | `ExportAllCustomerHoldingData` / `Export` | `upload-data-bucket` |
| Transactions, one account | Immediately | – | `export-app-data` |
| Cash, one account | Background | `ExportCashBalSingleClient` / `Export` | `upload-data-bucket` |
| Cash, all clients as on a date | Background | `ExportCashBalance` / `Export` | `upload-data-bucket` |
| Corporate action events in a date range | Immediately | – | Streamed download |
| Corporate action client-impact report (one type, date range) | Background | `ExportCorpActionReport` / `Export` | `upload-data-bucket` |
| Split / Bonus preview export (with `DIFFERENCE` and `STATUS`, see 4.9) | Immediately | – | `export-app-data` |
| Dividend preview export on the Dividend page (with custodian amount, `Difference`, tax, net, status) | Immediately, built in the browser | – | Direct download |
| Dividend preview export, older background path (not called by the page) | Background | `ExportDividendAccounts` / `Export` | `export-app-data` |

Reports only **read** data. They write a `Jobs` row and the output file. Top Clients is not built yet.

---

### 4.18 Notifications

**Business purpose.** A bell that shows the status of every background task (uploads, matches, imports, corporate actions, exports) to every user. It works even after the person who started the task has closed the browser.

**How it works.**

1. When a page starts background work, it adds a `Notifications` row (in progress), named after the job.
2. When anyone opens the bell, each row still in progress is checked against the place its job records its end: the `Jobs` row, the progress rows of a corporate action (including its rebuild jobs), `Custody_Match_Run` for a match, or `Holding_Comparison_Run` for a holding comparison.
3. The row is updated to completed, failed or partial failure.
4. The bell shows the newest 5 rows. Rows older than 24 hours are removed (checked at most once an hour). **Clear** removes them all.

| Table | Added | Changed | Deleted |
| --- | --- | --- | --- |
| `Notifications` | One row per task | Status settled from `Jobs` (in progress → completed / failed / partial failure) | Old rows pruned; Clear removes them |

The bell shows the newest 5 tasks.

---

## 5. What a change affects (impact table)

| When this happens... | ...these tables change | ...and these modules see it |
| --- | --- | --- |
| A custodian or FA file is uploaded | `Custody_Match_File`, Stratus day folders | Transaction Upload cards; Gate 1 may open |
| A file is replaced or removed, or a mapping is saved | `Custody_Match_File` row deleted; earlier runs marked superseded | Match must be run again before commit |
| A match runs | `Custody_Match_Run`, `Custody_Match_Exception` | Exception grid, difference report, Commit button |
| A trade date is committed | `Transaction`, `clientIds`, `Security_List`, `Holdings` (added), `Cash_Balance_Per_Transaction` (added) | Masters, Analytics, Cash page, all reports, corporate action previews, Holding Comparison |
| A Bhav Copy is uploaded | `Bhav_Copy` | Market values in Analytics and reports only |
| A custodian CA file is uploaded | `CA_Custodian_Upload`, `CA_Ex_Entitlement` or `CA_Benefit_Collection` | Stage 1 / Stage 2 of Split, Bonus, Dividend |
| Send as Receivable (split, bonus, dividend) | `Split_Account` / `Bonus_Record` / `Dividend_Record` rows with status RECEIVABLE | Status column on the page; nothing in holdings or cash yet |
| A split is confirmed | `Split`, `Split_Account`, `Holdings` (rebuilt); `Bonus` if a later bonus exists | Analytics, holdings reports, split report, later bonuses |
| A bonus is confirmed | `Bonus_Record`, `Bonus`, `Holdings` (rebuilt) | Analytics, holdings reports, bonus report |
| A dividend is confirmed | `Dividend`, `Dividend_Record` (status, received date, tax, net), `Cash_Balance_Per_Transaction` | Cash page, cash reports, dividend report (holdings unchanged) |
| A merger is applied | `Merger`, `Merger_Record`, `Security_List`, `Holdings` (rebuilt) | Old ISIN disappears from holdings; new ISIN appears |
| A demerger is applied | `Demerger`, `Demerger_Record`, `Security_List`, `Holdings` (rebuilt) | New ISIN appears; old ISIN's average cost falls |
| An ISIN is renamed | 16 tables renamed | Every screen and report shows the new ISIN |
| A Holding Comparison runs | `Holding_Comparison_Run`, Stratus reports | Holding Comparison page only |
| A late trade is committed (dated before saved rows) | `Holdings` of that client's shares and `Cash_Balance_Per_Transaction` of the client (rebuilt, corporate action records replayed as saved) | Analytics, holdings and cash reports. A client without a record of an applied corporate action still misses it until the operator re-sends it on the Split / Bonus / Dividend page (late clients, see 4.9) |
| Trades are deleted (Transaction Deletion) | `Transaction`, `Transaction_Deletion_Log`, `Holdings` and passbook of the clients (rebuilt), `Custody_Match_Run` stage detail (dates re-opened) | The dates can be committed again on Transaction Upload; trades still booked are skipped |

---

## 6. Scheduled cleanup (cron)

**`CustodyMatchCleanup`** is the only cron. It runs nightly (the schedule is set in the Catalyst console) and removes old **working data** of custody matching:

- Stratus day folders under `FA-Transaction/` and `Custodian-Transaction/`,
- matching rows in `Custody_Match_File`, `Custody_Match_Run` and `Custody_Match_Exception`.

**How it works.**

1. It works out the cut-off date: today minus the number of days to keep.
2. For each upload date older than that, it skips the date if a match is queued or running.
3. Otherwise it lists the Stratus files and table rows of that date.
4. In a dry run it only reports them. When dry run is switched off, it deletes them.

**Safety rules.**

- Keeps the last **30 days** by default (`CUSTODY_RETENTION_DAYS`). There is a hard minimum that configuration cannot go below.
- **Dry run by default:** it only reports what it would delete. It deletes only when `CLEANUP_DRY_RUN=false`.
- Skips any date with a match that is queued or running.
- Never touches `Transaction`, `client-transaction-files`, `field-maps/` or `Custodian-History/`.

---

## 7. Before going to Production

Some recent work exists only in the **Development** environment. Before deploying, create these in Production, then deploy AppSail and the listed functions together:

| Item | Needed by |
| --- | --- |
| `Split_Account` table, including `Status` and `Received_Date` | Split two-stage check, `RebuildHoldingtable` |
| `Record_Date` column on `Split`, `Bonus_Record`, `Bonus` | Split and Bonus (record date drives the maths) |
| `Dividend_Record.Received_Date` | Dividend two-stage check |
| `Dividend_Record.Tax_Amount` and `Dividend_Record.Net_Amount` (decimal, 2 places) | Dividend tax and net amount; read by the Dividend page and `ExportCorpActionReport` |
| New columns on `CA_Ex_Entitlement` (20) and `CA_Benefit_Collection` (36, including `CA_Tax_Amt`) | Custodian Upload, dividend tax rule |
| `Custody_Match_File` table and `Custody_Match_Run.uploadDate` | Date-based uploads (`CustodyMatchRunner`, `CustodyMatchCleanup`) |
| `Custody_Match_Exception` quantity columns changed to decimal | Mutual fund units |

After deploying the split changes, run `RebuildHoldingtable` for clients holding split ISINs, so `Split_Account` is filled for past splits.

Deploy the current `UpdateDividendData` and `ExportCorpActionReport` together with AppSail. An older `UpdateDividendData` credits the gross dividend (no tax deducted) and does not fill `Tax_Amount` / `Net_Amount`.
