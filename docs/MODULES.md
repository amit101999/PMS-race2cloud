# PMS Functional Documentation — Module by Module

This document describes the Portfolio Management System (PMS) in detail: what each module lets a user do, every field/filter/column shown on screen, the exact input and output file formats, and the business rules governing each calculation. It is organized by **business module**, not by technical layer.

> For engineering-level detail (source file locations, internal function names, database schema), see `README.md`. This document names on-screen fields, table columns, and file/report formats as they actually appear to a user, but avoids source-code file paths and internal function names.

> This document is the long-form companion to `PMS_Modules_Business_Logic.xlsx`. The two are kept in sync: every task row in the spreadsheet appears here in the module's **Features & status** table, and the spreadsheet's Summary sheet is reproduced under [Module Coverage Summary](#module-coverage-summary) below.

### How to read the status labels

Each module below opens with a **Features & status** table breaking the module into discrete tasks. The status vocabulary is:

| Status | Meaning |
|---|---|
| **Live** | Built, working, and usable by staff today. |
| **Live — known gap** | Working and in daily use, but with a specific documented limitation (spelled out in the module's Known gaps section). Counted under *Known Gaps*, not *Live*, in the coverage summary. |
| **Placeholder** | The UI element exists but has no functionality behind it. |
| **Internal tool — not staff-facing** | Implemented, but only reachable by an engineer/ops via a direct backend call — there is no page or button in the app. |
| **Not reachable from UI** | The logic is implemented and correct, but nothing in the running application can trigger it. |

## Introduction

The system manages equity portfolios for a broker/advisory business operating in India. It ingests broker-issued transaction files, maintains a running record of what every client holds, applies corporate actions (splits, bonuses, dividends, mergers, demergers) to those holdings, tracks each client's cash movements, and produces reports and exports for internal and client use.

The modules connect roughly in this order: **Master Data** underpins everything → **Transaction Upload** and **Bhav Copy Upload** bring in raw data → the **Holdings Engine** turns that raw data into current positions → **Analytics & Dashboard** let staff view those positions → **Corporate Actions** modify holdings and cash when companies issue splits, bonuses, dividends, mergers or demergers → **Cash Balance** tracks money movements → **ISIN Management** keeps security identifiers correct across the system → **Reports & Exports** package all of the above for download.

---

## Master Data Management

### What it does
Maintains the two reference lists every other module depends on: the client/account master and the security master.

### Features & status

| # | Feature / Task | What it does | Status |
|---|---|---|---|
| 1 | Client Master Search & Listing | Browse/search the client (account) reference list. | Live |
| 2 | Security Master Search & Listing | Browse/search the tradable security reference list. | Live |

### On-screen fields

**Client (Master → Client) page:**
- Search field: **"Filter by Virtual Code"** — free-text box, placeholder "Search Virtual or Actual Code...", with a live autocomplete dropdown (header "Virtual Code", footer "`{n}` of `{total}` matches", capped at 50 suggestions).
- Section heading "Clients", with a live count "Showing `{filtered}` of `{total}`".
- Table columns: the first column is always **Virtual Code** (the account's `WS_Account_code`), the second is always **Actual Code**; every other column is generated automatically from whatever fields exist on the client record (excluding internal record-tracking fields), title-cased for display. Empty state: "No clients match your filter."
- Paginated, 10 rows per page.

**Security (Master → Security) page:**
- Search field: **"Filter by Security Name or ISIN"** — free-text box, placeholder "Search Security Name or ISIN...", autocomplete dropdown (header "Security Name / ISIN", each suggestion showing name + ISIN, footer "`{n}` of `{total}` matches").
- Section heading "Securities", with count "Showing `{filtered}` of `{total}`".
- Table columns (fixed): **Security Name, ISIN, Security Code**. Empty state: "No securities match your filter."
- Paginated, 10 rows per page.

### Business rules
- Every account is identified by a broker-issued **Actual Code**. A single Actual Code can be split across several **Virtual Codes**, each representing a distinct client-facing portfolio carved out of the same underlying broker account. The Client search matches either code type in one box — entering an Actual Code surfaces every Virtual Code that belongs to it.
- One Actual Code may map to many Virtual Codes (portfolio splitting), but each Virtual Code belongs to exactly one Actual Code.
- Security identity is always by ISIN; code/name are descriptive fields that can be corrected later without changing the ISIN (see ISIN Management).
- Both master lists are plain, direct reads of what's stored — nothing is computed or joined in from elsewhere, so what staff see here is exactly the raw reference data, loaded in full and filtered in the browser as you type (not a server-side search).

### Known gaps
None specific to this module.

---

## Transaction Upload

### What it does
Lets staff upload a broker-format transaction CSV file (buys, sells, and other cash-affecting entries) to bring new trading activity into the system.

### Features & status

| # | Feature / Task | What it does | Status |
|---|---|---|---|
| 1 | Broker CSV Header Validation | Validates the uploaded transaction file before import. | Live |
| 2 | Auto Net Rate / Net Amount Derivation | Fills in Net Rate and Net Amount when the broker leaves them blank. | Live |
| 3 | Date Format Validation | Checks that trade/settlement dates are recognizable before import. | **Partial — sample-based only (known gap)** |
| 4 | Post-Upload Job Chain | Automatically brings a successful upload into holdings/cash after import. | Live |
| 5 | Upload History & Re-download | Lets staff see and re-download recent uploads. | Live |

### Input file format

The uploaded file must have **exactly 26 columns, in this exact order, with these exact header names** (spelling and case must match precisely — this is checked before anything else happens):

| # | Column header | Where it ends up |
|---|---|---|
| 1 | `Broker Code` | Broker/client identifier (stored in two places internally) |
| 2 | `BROKERACID` | Account code (Virtual Code) |
| 3 | `SYMBOLCODE` | ISIN |
| 4 | `EXCHG` | Exchange |
| 5 | `TRANSTYPE` | Transaction type code (e.g. buy/sell code) |
| 6 | `DATEPUR_ACQUI` | Trade date |
| 7 | `SETDATE` | Settlement date |
| 8 | `QUANTITY` | Quantity |
| 9 | `RATE` | Rate/price |
| 10 | `BROKERAGEPERSHARE` | Brokerage per share |
| 11 | `SERVICETAX` | Service tax |
| 12 | `SETDATEFLAG` | *(read but not stored)* |
| 13 | `MKTRATE` | *(read but not stored)* |
| 14 | `CASHSYMBOLCODE` | Security code |
| 15 | `TRANEXPENSE` | STT (securities transaction tax) |
| 16 | `ACCRUEDINTEREST` | Accrued interest |
| 17 | `BLOCKID` | Portfolio ID |
| 18 | `TRANSREF` | Transaction reference number |
| 19 | `Description` | Security name (and transaction description — same value stored in both places) |
| 20 | `Cheque number` | Cheque number |
| 21 | `CHQDETAIL` | Cheque detail |
| 22 | `BankRef` | Bank account reference |
| 23 | `filler` (1st) | Net rate — auto-calculated if left blank (see below) |
| 24 | `filler` (2nd) | Net amount — auto-calculated if left blank (see below) |
| 25 | `filler` (3rd) | *(ignored)* |
| 26 | `CashSetdate` | Payment date |

**Header validation:** the file's first row must match this list cell-for-cell in count, order, and spelling. Any mismatch is rejected immediately with a breakdown of exactly which column(s) didn't match (expected vs. actual), before a single row is imported.

**Auto-derived Net Rate / Net Amount:** if the broker leaves columns 23–24 blank, the system computes them:
- For buy-type transactions: **Net Rate = Rate + Brokerage per share**
- For sell-type (and other non-buy) transactions: **Net Rate = Rate − Brokerage per share**
- **Net Amount = Quantity (absolute value) × Net Rate**
- Both values are rounded to 4 decimal places. Service tax and STT are kept in their own columns and are not folded into these figures.

**Date format:** dates must be in `yyyy-mm-dd` format (e.g. `2026-07-24`); the trade date and settlement date columns are required, the payment date column is optional (blank, `0`, `-`, `na`, or `n/a` are all treated as "not provided" rather than an error). **Only the first 3 data rows of the file are checked for date-format problems** — a badly-formatted date further down in a large file will not be caught at this validation step.

The column separator is auto-detected: tab-separated if the header row contains a tab character, otherwise comma-separated.

### What happens after upload
- On a successful upload, the new transactions are recorded, and — automatically, without further user action, but with short deliberate delays between each step so the system isn't overwhelmed — the client/security masters are refreshed (including filling in any missing security code/name on the new rows), account holdings are recalculated to reflect the new trades, and each affected account's cash balance is updated.
- A history of recent uploads is kept, and previously uploaded files can be re-downloaded.

### Business rules
- Every transaction is tied to a specific settlement or trade date, which determines when it takes effect in holdings (see Holdings Engine for the buy/sell date rule).
- Upload is all-or-nothing at the header-validation stage — a file with header/format problems is rejected outright rather than partially imported.
- Very large uploads are processed in the background; the user is not required to wait for downstream holdings/cash recalculation to finish before the upload itself is considered accepted.
- Holdings recalculation for a fresh upload only ever adds new lots/quantities on top of what's already stored — it never deletes or rewrites existing holding rows for that account.

### Known gaps
- **Date validation is sample-based, not full-file.** Only the first few rows are checked for a recognizable date format; a malformed date deeper in a large file will not be caught at upload time.
- Holdings recalculation triggered by a new upload only appends the new activity on top of current holdings — it does not replay historical corporate actions. In practice this means uploads should generally happen before, not interleaved with, corporate action processing for the same security.
- A second, similarly-named transaction upload page exists in the codebase but is not reachable from the running application — the upload experience described above is the only one currently in use.

---

## Bhav Copy Upload

### What it does
Lets staff upload the daily official closing-price file (the "Bhav Copy") for one or more securities. This is the source of the market price used to value client holdings.

### Features & status

| # | Feature / Task | What it does | Status |
|---|---|---|---|
| 1 | Daily Closing Price File Upload | Uploads the daily official closing-price file feeding holdings valuation. | **Live but unvalidated (known gap)** |

### Input file format
**There is no enforced column, header, or date format for this file.** The only check performed is that the selected file has a `.csv` extension — its contents are passed straight through to storage and to the price table with no validation of columns, dates, or values. The file's columns must line up with the destination price table's own columns for the import to make sense, but nothing in the upload flow itself checks or reports this.

### Business rules
- Prices are dated — the system uses the closing price as of the relevant valuation date when showing holding value, not always "today's" price.
- Without a Bhav Copy upload for a given date, holdings valuation for that date will be missing or stale.

### Known gaps
- This upload does not show a progress/completion status the way transaction upload does — the user gets a simple success message (file name, storage location, and a job reference).
- No validation exists beyond the `.csv` extension check — a malformed or mismatched price file can be accepted without warning.
- Re-uploading a file for a date that was already uploaded **adds duplicate price rows rather than replacing them** — there is no dedup/overwrite behavior. This normally doesn't change which price is shown (the latest-dated row wins), but if a corrected re-upload has a different price for a date that already has a row, which one "wins" is not guaranteed.

---

## Holdings Engine

### What it does
This is the core calculation layer: it works out, at any point in time, exactly what quantity of each security every account holds, at what average cost.

### Features & status

| # | Feature / Task | What it does | Status |
|---|---|---|---|
| 1 | FIFO Lot Tracking | Core share-quantity/cost calculation for every account/security. | Live |
| 2 | Effective Date Rule | Determines which date a transaction starts/stops counting in holdings. | Live |
| 3 | Same-Day Event Ordering | Resolves the order of same-day events on one security. | Live |
| 4 | Fractional Share / Epsilon-Snap Handling | Keeps genuine fractional holdings while removing floating-point "dust". | Live |
| 5 | Average Cost (WAP) Calculation | Computes the running average cost per share shown against every holding. | Live |
| 6 | Consolidated (Actual Code) View | Rolls up holdings across all Virtual Codes under one Actual Code. | Live |
| 7 | Live Holdings Read Order & As-Of-Date Filtering | Governs how the running balance is displayed and how historical snapshots are filtered. | Live |
| 8 | Two Recalculation Modes (Append vs Full Rebuild) | Defines when holdings are incrementally appended vs fully recomputed. | Live |
| 9 | Dual-Engine Consistency (drill-down vs stored table) | Two independently-maintained calculations exist behind the scenes. | **Known gap** |
| 10 | Manual Holdings-Recalculation Trigger | Ops-only tool to recalculate holdings for a specific account list. | **Internal tool — not staff-facing** |

### Business rules
- **First-in-first-out (FIFO):** shares are tracked lot by lot in the order they were acquired. When shares are sold (or consumed by a corporate action), the oldest lots are used up first.
- **Effective date rule:** a buy takes effect on its settlement date; a sell takes effect on its trade date. This determines exactly which date a transaction starts or stops counting toward a holding.
- **Same-day ordering rule:** if a transaction, a split, a bonus, a demerger, and a merger all land on the same date for the same security, they are always processed in this fixed order: transaction, then split, then bonus, then demerger, then merger. This specifically prevents a split from inflating a quantity and then a bonus being calculated on the already-inflated (post-split) quantity, which would double-count the bonus.
- **Fractional shares:** genuine fractional share quantities (e.g. 5.533 shares) are fully supported and displayed as-is. Separately, tiny near-zero leftover amounts caused by computer arithmetic (e.g. a position that should be exactly zero showing as a sliver like 0.0000001) are automatically cleaned up to a clean zero, so fully-sold positions don't linger as "phantom" holdings. This clean-up never changes a genuine fractional holding — it only removes arithmetic dust.
- **Average cost (WAP) formula:** `WAP = Total Cost of Remaining Holdings ÷ Quantity Held`. This is recalculated after every FIFO event (buy, sell, or corporate-action lot change) — it is a running weighted-average cost per share, not a fixed value set once at purchase.
- **Consolidated view:** holdings can be viewed either per Virtual Code, or rolled up to the Actual Code level. The consolidated quantity is simply the sum of quantity across every Virtual Code under that Actual Code. The consolidated view intentionally shows quantity only — average cost, price, and market value are left out, since those don't cleanly combine across client-facing portfolios the way share counts do.
- **Read order for the live holdings view:** the running balance (quantity, average cost, holding value) is read back in the exact order the rows were originally created, not re-sorted by transaction date. This is deliberate — sorting by date alone would reshuffle the running-balance snapshots and make the "balance as it accumulated" story stop making sense, since more than one row can share the same date.
- **As-of-date filtering at the row level:** when staff view holdings as of a past date, the same buy-uses-settlement-date / sell-uses-trade-date rule is re-applied per row, so a buy that technically hasn't settled yet doesn't incorrectly count toward a historical snapshot.
- **Two distinct recalculation modes:**
  - **Append mode** (used after a routine transaction upload): only adds new lots/quantities on top of what's already stored; existing holding rows are never deleted or rewritten.
  - **Full rebuild mode** (used after any corporate action — split, bonus, merger, demerger — and after an ISIN rename): deletes and fully recomputes holdings from scratch for every affected account, rather than adjusting incrementally. This is why a corporate action or ISIN rename can take noticeably longer to reflect in holdings than a routine upload.

### Known gaps
- Holdings recalculation triggered by a new upload only appends (see Transaction Upload).
- There are two independently-maintained calculations behind the scenes — one used for the live transaction-history drill-down view, and one used to (re)build the stored holdings table. Both apply the same FIFO, same-day-ordering, and fractional-share rules, but for a demerger or merger event that produces more than one resulting lot, the two can in rare cases disagree on the exact lot breakdown for an account. This is a latent inconsistency to be aware of rather than a routine occurrence.
- A manual "recalculate holdings now" trigger exists as an internal maintenance tool (a direct backend link, not a button on any staff-facing page), intended for ops use when a specific set of accounts needs a holdings refresh outside the normal upload/corporate-action flow. Its account list is currently empty in the code, so using it today would recalculate nothing until someone fills that list in — it is not something staff can use through the app.

---

## Analytics & Dashboard

### What it does
Gives staff a way to search and review holdings and transaction history, plus a landing dashboard.

### Features & status

| # | Feature / Task | What it does | Status |
|---|---|---|---|
| 1 | Search by Security (ISIN mode) | Shows every account holding a given security. | Live |
| 2 | Search by Account (Account mode) | Shows one account's full holdings + summary breakdown. | Live |
| 3 | Transaction Ledger Drill-down | Full merged trade + corporate-action ledger for an account/security. | Live |
| 4 | Dashboard Recent Trades | Live feed of recent transaction activity. | Live |
| 5 | Dashboard Summary Cards / Charts / Filters | Landing-page KPIs, charts, and filter controls. | **Placeholder** |
| 6 | Allocation & Performance Tabs | Planned analytics views. | **Placeholder** |

### On-screen fields — Analytics page

**Filter bar (shared across all tabs):**
- **ISIN** — free-text search with autocomplete ("Search ISIN / Security..."); selecting one switches the view into ISIN mode and clears Account Code.
- **Account Code** — free-text search with autocomplete ("Search Account Code..."); selecting one switches the view into account mode and clears ISIN.
- **Filter by Date** — a date picker; re-applies whichever mode (ISIN or Account) is currently active as of that date.

**Tabs:** **Holding**, **Allocation**, **Performance**, **Transaction**.

**Holding Summary card** (account mode only) — always exactly 3 rows:
| Description | Cost | Mkt Val | Income | G/L | % G/L | % Assets |
|---|---|---|---|---|---|---|
| Total | total assets | total assets | – | – | – | 100.00% |
| Equity | equity market value | equity market value | – | – | – | computed |
| Cash and Equivalent | cash balance | cash balance | – | – | 0.00% | computed |

The three rows are fixed, not data-driven: whatever the account holds, all three always render, in this order. An account with no holdings and no cash still shows all three (as ₹0). The card appears in **account mode only** — it is hidden entirely in ISIN mode.

**Where the numbers come from.** Only two figures are actually calculated; everything else on the card is derived from them or is a fixed label.

| Input | How it is obtained |
|---|---|
| **Equity Market Value** | Sum of `Market Value` across every holding row currently loaded for the account, where each row's Market Value = `Quantity Held × Last Price`. Last Price is the most recent Bhav Copy closing price on or before the selected as-on date. If no price exists for a security, its Last Price is treated as 0 — that security then contributes nothing to Equity Market Value. |
| **Cash Balance** | The account's latest cash-ledger balance, taken from the **older daily-summary cash system**, not the Cash Balance page's passbook (see the note under Business rules below). Defaults to 0 if no ledger entry exists. |

**Formulas:**
- `Total Assets = Equity Market Value + Cash Balance`
- `Equity % of Assets = (Equity Market Value ÷ Total Assets) × 100`
- `Cash % of Assets = (Cash Balance ÷ Total Assets) × 100`
- Both percentages fall back to `0.00%` when Total Assets is zero (avoiding a divide-by-zero).

**What is *not* calculated on this card.** Four of the seven columns are display placeholders, and this is the single most important thing to know before quoting any figure from it:

- **Cost and Mkt Val show the same number on every row.** There is no cost-basis calculation in this card at all — each row simply prints its one figure (total assets / equity market value / cash balance) into both columns. This is notable because the data to do it properly is already loaded: every holding row carries **Holding Value** (`Quantity × WAP`, i.e. what was actually paid) alongside **Market Value** (`Quantity × Last Price`, current worth). The card ignores Holding Value.
- **Income, G/L and % G/L are literal dashes ("–") for Total and Equity.** Since Gain/Loss is by definition `Mkt Val − Cost`, and the card sets Cost = Mkt Val, any computed G/L would always be exactly zero and therefore meaningless. Rather than print a misleading 0, the columns render as dashes.
- **Cash's % G/L is a hardcoded `0.00%`,** not a dash and not a calculation. Economically the value is defensible — a cash balance does not appreciate — but it is a constant, so it will read 0.00% regardless of what is in the ledger.
- **Total's % Assets is the literal text `100.00%`,** never computed. It therefore reads 100% even for an entirely empty account.

**Practical guidance:** treat **Mkt Val** and **% Assets** as reliable — those trace back to real holdings and ledger data. **Cost, Income, G/L and % G/L are placeholders and must not be quoted to anyone as portfolio performance.** Making them real would mean summing Holding Value for the Equity row's Cost and deriving `G/L = Mkt Val − Cost` and `% G/L = G/L ÷ Cost × 100`; cash's cost legitimately equals its balance, so its 0.00% would then fall out naturally instead of being pinned.

**Holding tab — ISIN mode** (heading "ISIN Report (`{ISIN}`) - Accounts (`{count}`)"): table columns **Virtual Code, Actual Code, Quantity, WAP, Holding Value, Last Price, Market Value**, plus a "View" action per row (opens that account's transaction history).

**Holding tab — Account mode**: a 3-way sub-filter (**All / Equity / Cash**) above a holdings grid with columns **Security Name, Security Code, Security ISIN, Current Holding, Average Holding Value, Holding Value, Last Price, Market value**, plus "View" per row; fully-sold rows carry a "FULLY SOLD" badge.

**Transaction tab**: its own **Security** search field ("Search Security Name or ISIN..."), inheriting the Account Code/date from the shared filter bar above. Table (heading "Transaction History") columns: **TRANDATE, SETDATE, Type, Security Name, ISIN, Quantity, Price, Total Amount**. This view merges ordinary trades with corporate-action-generated rows (bonus/split/merger/demerger shares) into one combined, paginated ledger, newest first.

**Allocation tab** and **Performance tab**: not yet implemented — each currently shows only its own placeholder label.

### On-screen fields — Dashboard page

- Header button: **"Import File (Excel/CSV)"** — present but not functional.
- **Summary cards** (all fixed/hardcoded, not live): Total Trades = 2,65,814; Total Value = ₹0; Buy Trades = 1,250; Sell Trades = 880.
- **Charts** (all static sample data, not live): a line chart ("Daily Trade Volume (Last 30 Days)"), a bar chart ("Top 10 Stocks by Trade Value"), and a donut chart ("Exchange Distribution": NSE 53% / BSE 37% / MCX 10%).
- **Filters bar** (present but not functional — none of these affect anything below them): Search Security (All Securities/Equity/F&O), Exchange (All Exchanges/NSE/BSE), Transaction Type (All Types/Buy/Sell), Client ID (text), Start Date, End Date.
- **Recent Trades** — the only genuinely live section, reading real data. Its table has a very large column set (mirroring the full transaction record): Row ID, Creator ID, Created Time, Modified Time, WS Client ID, WS Account Code, Transaction Date, Set Date, Transaction Type, Transaction Description, Security Type, Security Type Description, Detail Type Name, ISIN, Security Code, Security Name, Exchange, Broker Code, Depository Registrar, DP ID AMC, DP Client ID Folio, Bank Code, Bank Account ID, Quantity, Rate, Brokerage, Service Tax, Net Rate, Net Amount, STT, Transfer Date, Transfer Rate, Transfer Amount, Total Transaction Fee, Total Transaction Fee STAX, Transaction Reference No, Description Memo, Cheque No, Cheque Details, Portfolio ID, Delivery Date, Payment Date, Accrued Interest, Issuer, Issuer Name, TDS Amount, Stamp Duty, TPMS Gain, RM ID, RM Name, Advisor ID, Advisor Name, Branch ID, Branch Name, Group ID, Group Name, Owner ID, Owner Name, Wealth Advisor Name, Scheme ID, Scheme Name.

### Business rules
- Quantities display real decimals for ordinary trades but whole numbers for corporate-action-generated holdings (since corporate actions round to whole shares — see each action's rules below). This is purely a display rule; it doesn't change what's actually stored.
- The cash figure shown on the Analytics summary card is drawn from the older, less current of the two cash-tracking systems described in Cash Balance below — it can disagree with the passbook figure shown on the Cash Balance page for the same account. Treat the Cash Balance page as authoritative if the two ever differ.

### Known gaps
- Dashboard summary cards, charts, and the entire filters bar are placeholders — none of them reflect live data or affect what's shown.
- The Dashboard's "Import File" button has no action behind it yet.
- Analytics' Allocation and Performance tabs are not yet implemented (each shows only bare placeholder text today).
- On the Holding Summary card, the **Cost, Income, G/L and % G/L columns are not calculated** — Cost duplicates Mkt Val, Income and G/L are dashes, and Cash's % G/L is a hardcoded 0.00%. The columns exist in the header as placeholders for a cost-basis/performance feature that has not been built, even though the underlying Holding Value (cost) data is already available on every holding row.

---

## Corporate Action — Split

### What it does
Lets staff apply a stock split (e.g. 1 share becomes 5) to a security, converting existing holdings into the new higher (or lower) share count.

### Features & status

| # | Feature / Task | What it does | Status |
|---|---|---|---|
| 1 | Split Entry & Preview | Set up a stock split and preview its impact before applying. | Live |
| 2 | Split Apply | Commits the split and recalculates affected holdings. | **Live — no progress visibility (known gap)** |
| 3 | Split Export | Downloadable spreadsheet of the preview. | Live |
| 4 | Split Holdings-Impact Report (dedicated export) | Standalone "export holdings affected by split" report. | **Placeholder** (general preview export covers this need) |

### On-screen fields
**Entry form:** ISIN (searchable, autocomplete), Security Code and Security Name (auto-filled, read-only), **Ratio (For every)**, **Ratio (Investor gets)**, **Effective Date**. Submit button: "Fetch Affected Accounts".

**Preview table ("Split Impact Preview")** — grouped by Actual Code:
- Parent/group row: **Actual Code** (with an expand toggle and child count), **Current Holding, New Holding, Δ Change**.
- Expanded child row (one per Virtual Code): the same 4 columns at the individual account level.

**Actions:** "Export" (generates a downloadable spreadsheet, enabling a "Download" button), "Confirm & Apply Split".

### Business rules
- A split is applied only to accounts that actually hold the security as of the effective date.
- **Calculation formula:** with Ratio (For every) = X and Ratio (Investor gets) = Y, `New Holding = floor(Current Holding × Y / X)` and `Δ Change = New Holding − Current Holding`. This is the same formula used in the preview, the export, and (for the rounded reference figure) the applied result.
- In the preview, any account whose Virtual Code can't be resolved to a known Actual Code is shown as its **own separate group** (keyed by its own account code) rather than being bucketed together with other unmapped accounts — contrast with Bonus below, which handles this differently.
- Group totals in the preview are computed by summing the already-rounded child-row figures, so the parent group's total always matches the sum of what's shown underneath it, even though (per the rounding caveat below) that may not be the account's final real-world holding.
- Saving a split records it right away and returns success immediately — but the actual recalculation of every affected account's holdings happens afterward in the background, without a visible progress indicator the way Bonus and Dividend have. If that background step is delayed or fails for some accounts, there is currently no on-screen signal of that.
- The preview and the exported spreadsheet always show new share quantities rounded down to a whole number. The actual holding that ends up recorded for the account after the background recalculation can, in some cases, come out as a genuine fraction rather than matching that rounded preview number — so the preview/export are a close guide to the outcome but are not guaranteed to match the final stored holding exactly.
- Re-submitting the same split (same security, effective date, and ratio) that was already applied is recognized and skipped rather than applied twice.
- If a bonus for the same security is later found to be affected by a split (same-day or overlapping), the system can recompute that bonus's numbers in place to stay consistent with the same-day ordering rule described in Holdings Engine.

### Export format
The "Export" spreadsheet contains one row per Virtual Code with columns: **ISIN, Virtual Code, Actual Code, Current Holding, New Holding, Delta** (New Holding and Delta calculated the same way as the preview).

### Known gaps
- No visible confirmation exists that the background holdings recalculation for a split has actually finished for every affected account — unlike Bonus/Dividend, there is no progress screen to check.
- The rounding shown in the preview/export is not guaranteed to exactly match the final recorded holding (see Business rules above).
- Split and Bonus handle "unmapped account" rows differently in their previews (Split gives each its own group; Bonus lumps them into one shared group — see Bonus below) — a genuine inconsistency, not a deliberate design choice.
- A separate "export holdings affected by this split" report is a placeholder and not yet functional (the general preview export above remains available).

---

## Corporate Action — Bonus

### What it does
Lets staff apply a bonus share issue (e.g. 1 free share for every 2 held) to a security.

### Features & status

| # | Feature / Task | What it does | Status |
|---|---|---|---|
| 1 | Bonus Entry & Preview | Set up a bonus issue and preview its impact. | Live |
| 2 | Bonus Apply | Commits the bonus with visible background progress. | Live |
| 3 | Bonus Export | Downloadable spreadsheet of the preview. | Live |
| 4 | Bonus Holdings-Impact Report (dedicated export) | Standalone "export bonus holdings" report. | **Placeholder** (working preview export covers this need) |

### On-screen fields
**Entry form:** ISIN (searchable, autocomplete), Security Code and Security Name (auto-filled, read-only), **Ratio (For every held)**, **Ratio (Bonus shares)**, **Effective Date**. Submit button: "Fetch Affected Accounts".

**Preview table ("Bonus Impact Preview")** — grouped by Actual Code:
- Parent/group row: **Actual Code** (with virtual-account count badge), **ISIN, Current Holding, Bonus Shares, New Holding, Δ Change**.
- Expanded child row (labeled "Virtual" + account code): the same columns at the individual account level.

**Actions:** "Export" (generates a downloadable spreadsheet), "Confirm & Apply Bonus" (label changes to "Applying Bonus..." then "Bonus Applied!" while the background job runs and completes).

### Business rules
- Eligibility is determined in two steps: first, only accounts that have ever traded the security at all are considered; then, among those, only the ones with an actual positive FIFO holding as of the ex-date qualify for a bonus.
- **Calculation formula:** with Ratio (For every held) = X and Ratio (Bonus shares) = Y, `Bonus Shares = floor(Current Holding × Y / X)` and `New Holding = Current Holding + Bonus Shares`. Bonus shares are always rounded down to a whole number, consistently in the preview, the export, and the actual applied result. This follows the same same-day ordering rule as Split — bonus is calculated after any same-day split, never before.
- In the preview, every account whose Virtual Code can't be resolved to a known Actual Code is grouped together into **one shared "unmapped" group** — unlike Split, which gives each such account its own separate group (see Split above, and the Known Gaps note there).
- Re-running a bonus apply for a security/date/account combination that has already been processed will not create a duplicate bonus entry. This check is done per account, not just once for the whole batch — so if a bonus apply partially fails (succeeding for some accounts, failing for others), it can safely be re-run and will only reprocess the accounts that didn't succeed the first time.
- If a same-day split later requires the bonus to be recalculated, the existing bonus record is updated in place rather than replaced with a new one.
- Applying runs as a background job with visible progress tracking (unlike Split), polled automatically until it reaches a finished state.

### Export format
The "Export" spreadsheet contains one row per Virtual Code with columns: **ISIN, Virtual Code, Actual Code, Current Holding, Bonus Shares, New Holding, Delta**.

### Known gaps
A separate "export bonus holdings" report is a placeholder and not yet functional (the working preview export above remains available).

---

## Corporate Action — Dividend

### What it does
Lets staff record a dividend declaration for a security and determine which accounts are eligible, and for how much — reconciled directly against the custodian's own records.

### Features & status

| # | Feature / Task | What it does | Status |
|---|---|---|---|
| 1 | Dividend Entry & Custodian File Upload | Declare a dividend and reconcile against the custodian's own record. | Live |
| 2 | Reconciliation Engine | Compares system holdings vs. custodian file per account (7 status outcomes). | Live |
| 3 | Dividend Apply Modes | Selective application of the dividend to reconciled accounts. | Live |
| 4 | Dividend Cash Credit | Credits eligible accounts' cash passbook. | Live |
| 5 | Dividend Reconciliation Export | Downloadable reconciliation grid. | Live |

### On-screen fields
**Entry form ("Add Dividend"):** Security Code (searchable, autocomplete), ISIN and Security Name (auto-filled), **Dividend Type** (Interim / Special / Final / Interest), **Dividend Rate**, **Unit** (Per Share / Per Unit / Per Unit-Interest), **Ex-Date**, **Record Date** (required — used for the eligibility calculation), **Payment Date** (required).

**Custodian file upload** — required before a preview can be generated: an "Upload Custodian File" control accepting the custodian's Benefit Collection Report as CSV/Excel. Submit button: "Fetch Affected Accounts" (disabled until a file is attached).

**Reconciliation view ("Custodian Reconciliation"):**
- If the uploaded file contains more than one dividend event for the security, tabs appear, one per event (labeled by reference number, rate, and account count).
- Status filter chips, each showing a count: **Matched, Mismatch, Partial, Over-paid, Already paid, Missing in system, Missing in file**, plus an "All" chip.
- Reconciliation table, grouped by account (with a virtual-code drill-down), columns: **Account, Holding (system/file/difference), Rate (system/file), Gross (system/file/difference), CA Tax Amt, Net, Already Received, To Receive (Net), Status**.

**Apply controls:** two buttons — **"Apply Matched Only"** (only cleanly-matched accounts) and **"Apply All Reconciled"** (matched, mismatch, partial, over-paid, and missing-in-file accounts — deliberately excluding already-paid and missing-in-system accounts as unsafe to auto-apply). Both open a confirmation dialog showing the account count and estimated total gross amount before submitting.

**Export:** an "Export" button producing a reconciliation spreadsheet directly in the browser (no server round-trip).

### Business rules
- Eligibility is based on the FIFO holding as of the **record date** specifically (not the ex-date or payment date).
- **Dividend amount formula:** `Dividend Amount = Eligible Holding (as of Record Date) × Rate`.
- The custodian reconciliation file is **mandatory** — a preview cannot be generated without uploading it first; there is no calculation-only preview path.
- Reconciliation status is computed per account by comparing the system's own FIFO-based holding/amount against the custodian file's figures, using small tolerance margins so tiny rounding differences aren't flagged as real mismatches:
  - **Missing in file** — the system expects this account to be eligible, but the custodian file has no matching row.
  - **Missing in system** — the file has a row for this account, but the system's own holding calculation doesn't show it as eligible.
  - **Over-paid** — the amount already received historically is **more than ₹0.005 above** the custodian file's gross figure.
  - **Partial** — some amount has already been received, but it's **more than ₹0.005 below** the custodian file's gross figure (i.e. not fully paid yet).
  - **Already paid** — the amount already received is within ₹0.005 of the custodian file's gross figure (neither over-paid nor partial).
  - **Mismatch** — both sides have a record for the account, nothing has been paid yet, but the holding differs by **more than 0.0001 shares** or the gross amount differs by **more than ₹0.005** between the system and the file.
  - **Matched** — both sides agree (within the tolerances above) and nothing has been paid yet.
- If the custodian file has **no rows at all for the security**, but the system's own FIFO calculation still shows accounts with an eligible holding, the system doesn't silently drop those accounts — it creates a placeholder reconciliation entry for the security so those accounts still surface (correctly flagged as "Missing in file") instead of vanishing from the reconciliation view.
- A dividend apply **credits cash only** — it does not change any security holding quantities.
- The cash credit is inserted at its correct chronological position in the account's cash passbook (not just tacked onto the end), which means applying a dividend for a past record date can adjust the stored running balance on every later passbook entry for that account, not only add one new line.
- Re-applying a dividend for a security/record-date combination that was already processed is blocked outright at the whole-batch level — unlike Bonus, this check is not per account, so if even one account in a dividend run needs to be redone, the entire (security, record date) combination needs attention rather than just the affected accounts.

### Export format
The on-screen "Export" button produces a spreadsheet with columns: **Account_Code, Client_Name, Holding_PMS, Holding_File, Rate_PMS, Rate_File, Gross_PMS, Gross_File, CA_Tax_Amt, Net, Already_Received, Cash_Credited (Yes/No), To_Receive_Net, Status**.

The custodian's own Benefit Collection Report file is read flexibly — it recognizes many different real-world header spellings for the same field (for example, an account code column may be labeled "Client UCC Code", "UCC Code", "UCC", "Account Code", or a couple of other variants, and the system treats them all the same way). The fields it looks for, regardless of exact header wording, are: account code, client name, ISIN, security name, a reference number for the dividend event, ex-date, record date, payment date, rate, holding quantity, gross amount, tax amount, net amount, status, and action type. ISIN values with a trailing currency suffix (e.g. "-INR") are automatically cleaned up before matching.

### Known gaps
None specific beyond the general system caveats noted at the end of this document.

---

## Corporate Action — Merger

### What it does
Lets staff apply a merger, converting holdings in an old (acquired) security into a new (surviving) security, at a specified conversion ratio.

### Features & status

| # | Feature / Task | What it does | Status |
|---|---|---|---|
| 1 | Merger Entry & Preview | Set up conversion of an old security into a new one. | Live |
| 2 | FIFO Lot Conversion & Cost Reallocation | Converts each lot's quantity and cost basis. | Live |
| 3 | Merger Apply | Commits the merger with visible background progress. | Live |
| 4 | Merger Export | Downloadable spreadsheet of the preview. | Live |

### On-screen fields
**Entry form:** **Effective date**, **Record date**, **Ratio 1**, **Ratio 2**; an "Old Company (Old ISIN)" section with a searchable Old ISIN field (autocomplete shows security name once matched); a "Merge Into New Company (New ISIN)" section with **New ISIN**, **Security code for new ISIN**, **Security name for new ISIN**. Submit button: "Fetch Affected Account".

**Preview summary strip:** Accounts affected, Surviving lots, New shares total, Carried cost, Skipped lots (round-to-0), Skipped cost basis.

**Preview table** — flat, lot-level (no Actual Code grouping), columns: **Account code, Old ISIN, Effective Date, WAP, QTY, Ratio 1, Ratio 2, New WAP, New ISIN, New QTY**. Any lot that would round down to zero new shares is visually flagged (highlighted row with a tooltip) so staff can see it will be skipped before applying.

**Actions:** "Export CSV" (client-side spreadsheet), "Confirm & Apply Merger" (shows a live "Status: ..." indicator while the background job runs).

### Business rules
- Only one old security can be merged into one new security per merger operation today, even though the entry form's shape looks like it could accept more than one.
- The preview is built by replaying the security's entire prior history — ordinary trades plus any earlier bonus, split, demerger, or merger events on it — not just its raw buy/sell transactions, so the lot-level picture going into the merger already reflects everything that happened to it before.
- **Calculation formula:** for each lot, `New Qty = floor(Lot Qty × Ratio 1 / Ratio 2)` and `New WAP = Original Lot Cost ÷ New Qty` (the lot's entire original cost is preserved and spread across the new, usually smaller, share count, so no cost basis is lost on a lot that survives the conversion). A lot that rounds down to zero shares loses its cost basis entirely and is flagged as skipped.
- Because the merger changes which ISIN an account holds, it also updates the security master so the new ISIN is properly recognized going forward — if that security already exists in the master, its code/name are refreshed to match; if not, a new entry is created. Holdings recalculation afterward is limited to the accounts and the two securities actually involved, not the whole system.
- The ISIN entered for a merger is automatically standardized to uppercase, so case differences in data entry don't cause a mismatch.
- If a security already had an earlier merger or demerger applied to it, a new event on it is treated as a full replacement of the lot picture the first time that event is encountered — but multiple lot rows that belong to the very same event are correctly combined together rather than overwriting each other.
- Applying is safe to retry: progress is tracked per account, so if an apply is interrupted or partially fails, re-running it will skip every account that already succeeded and only reprocess the ones that didn't.
- If an apply appears to hang for an extended period (roughly an hour or more) without finishing, the system automatically marks it as stalled/errored rather than leaving it stuck, so staff can retry instead of waiting indefinitely.

### Export format
**Export CSV** columns: **Account Code, Old ISIN, Effective Date, WAP, QTY, Ratio 1, Ratio 2, New WAP, New ISIN, New QTY**.

### Known gaps
Unlike Split/Bonus/Dividend, the Merger preview does not group by Actual Code — it shows lot-level detail directly. There is also no confirmation dialog before applying (unlike Dividend) — clicking Apply starts the job immediately.

---

## Corporate Action — Demerger

### What it does
Lets staff apply a demerger, where a new security is spun off from an existing one and a portion of the original holding's cost is allocated to the new security.

### Features & status

| # | Feature / Task | What it does | Status |
|---|---|---|---|
| 1 | Demerger Entry & Preview | Set up a spin-off of a new security from an existing holding. | Live |
| 2 | Cost Allocation Percentage Logic | Splits original cost basis between retained and spun-off holdings. | Live |
| 3 | Demerger Apply | Commits the demerger with visible background progress. | **Live — security-master upsert inconsistency vs Merger (known gap)** |
| 4 | Demerger Export | Downloadable spreadsheet of the preview. | Live |

### On-screen fields
**Entry form:** an "Old Company (existing)" section with a searchable field (autocomplete) plus auto-filled Security Code/Security Name; a "New Company (demerged)" section with **New ISIN**, **New Security Code**, **New Security Name**; a "Demerger Details" section with **Ratio 1**, **Ratio 2**, **Effective Date**, **Record Date** (holdings are calculated up to one day before this date), and **Allocation to new company (%)** (0–100, with a live "Remainder to old company: X%" hint). Submit button: "Fetch Affected Accounts".

**Preview table** — flat, lot-level (no Actual Code grouping, and no skip-flagging the way Merger has), columns: **Account Code, Old ISIN, Source Buy Date, Old Qty, Old WAP (Before), Old WAP (After), New ISIN, New Qty, New WAP**.

**Actions:** "Export CSV" (client-side spreadsheet), "Confirm & Apply Demerger" (shows a "Background job status: ..." banner while the job runs).

### Business rules
- New shares are computed from the ratio and rounded down to a whole number, consistent with the other ratio-based actions.
- The allocation percentage determines how the original position's **cost basis** is split between the old (retained) and new (spun-off) holding — it does not affect the share ratio itself, only how cost is divided. This percentage is a deliberate business choice entered per demerger, not a fixed rule.
- Exact formulas per lot: the old side keeps its full share quantity but its cost is reduced to `(1 − allocation%)` of the original; the new side receives `floor(quantity × ratio1 / ratio2)` shares, carrying `allocation%` of the original cost.
- A demerger builds its "before" picture from each account's current recorded holding, rather than replaying the security's full transaction history the way Merger does — so holdings should generally be up to date before running a demerger on that security.
- Security codes and ISINs are matched exactly as entered, including letter case, for this action — unlike Merger, which standardizes case automatically. A mistyped case on the security identifiers can cause a demerger not to find the intended holding.
- Each surviving lot produces two linked results — one for the retained (old-security) side and one for the new spun-off side — so the historical connection between the original lot and its spun-off portion is preserved, not just a single blended row.
- Re-submitting the exact same demerger event (same old ISIN, new ISIN, and record date) is recognized and will not create a duplicate master record.
- Applying is safe to retry: progress is tracked per account, so if an apply is interrupted or partially fails, re-running it will skip every account that already succeeded and only reprocess the ones that didn't.

### Export format
**Export CSV** columns: **Account Code, Old ISIN, Source Buy Date, Old Qty, Old WAP (Before), Old WAP (After), New ISIN, New Qty, New WAP**.

### Known gaps
- Like Merger, the Demerger preview does not group by Actual Code — it shows detail directly rather than a drill-down summary. There is also no confirmation dialog before applying, and no visual "will be skipped" flag on individual rows the way Merger has.
- If the new security already exists in the security master before a demerger is applied, Demerger does **not** update its code/name to match what was entered on the form (it only creates a new entry if one is missing) — unlike Merger, which refreshes the code/name either way. This is an inconsistency between the two actions worth being aware of, not a deliberate rule.

---

## Cash Balance

### What it does
Tracks the cash impact of every account's trading activity and corporate action proceeds (like dividends), so staff can see a running cash balance ("passbook") per account.

### Features & status

| # | Feature / Task | What it does | Status |
|---|---|---|---|
| 1 | Cash Passbook Search & Statement | Per-account running cash balance statement. | Live |
| 2 | Cash Effect Rules | Determines whether a transaction adds to or subtracts from cash. | Live |
| 3 | Cash Export | Downloadable passbook/snapshot exports. | Live |
| 4 | Legacy Daily Cash System (parallel, unsynced) | Older daily-summary cash system still running in parallel. | **Known gap** |
| 5 | Full Passbook Rebuild Tool | Maintenance tool that wipes and fully recomputes an account's cash history. | **Internal tool — not staff-facing** |

### On-screen fields
**Filters:** **Account Code** (searchable, required before fetching), **From Date**, **To Date**, **ISIN** (searchable, enabled only once an account is chosen), a "Fetch" button.

**Summary strip** (after fetching): **Account**, **Closing Balance (Page)**, **Total Records**.

**Cash Passbook table** columns: **Tran Date, Set Date, Account, ISIN, Security Name, Tran Type, QTY, Rate, STT, Amount, Balance** — the transaction-type column is shown as a color-coded badge (dividend / buy / sell / cash-in / cash-out).

**Export:** a single "Export CSV" button that becomes "Download CSV" once the background job completes (no format options besides CSV).

### Business rules
- Buys and certain fees/charges reduce cash; sells, cash deposits, and dividend credits increase cash. Transaction taxes are always deducted from the cash effect regardless of direction.
- **Running balance formulas:** for a cash-increasing (inflow) transaction, `New Balance = Previous Balance + Amount − Tax`; for a cash-decreasing (outflow) transaction, `New Balance = Previous Balance − Amount − Tax`. Tax (STT) reduces the balance either way.
- An account's very first cash-deposit entry **sets** its starting balance directly (`Balance = Amount − Tax`), rather than adding on top of an assumed-zero balance.
- Every transaction upload and every dividend apply feeds into this passbook automatically — staff do not need to manually record cash effects.

### Export format
**Single-client export** columns: **Tran Date, Set Date, Account Code, ISIN, Security Name, Tran Type, QTY, Rate, STT, Amount, Cash Balance**.
**All-clients export** columns: **Date, Account Code, Cash Balance** (one row per account, showing its closing balance as of the chosen date).

### Known gaps
- A second, older daily-cash-summary system exists in parallel and is **not kept in sync** with the passbook described here. This isn't just a dormant legacy artifact — it's the figure actually shown on the Analytics page's cash summary card (see Analytics & Dashboard above), so staff can genuinely see two different cash numbers for the same account on two different screens. The passbook on this page should be treated as the authoritative, current view of cash.
- The internal tool that refreshes that older daily-cash-summary system runs against a **hardcoded, fixed list of roughly 100 accounts** rather than the full client base (the code to pull the complete account list is present but switched off). In practice this means that older system only ever reflects a limited subset of accounts, never all of them — one more reason the passbook on this page, not that summary figure, should be trusted.
- A full passbook rebuild tool also exists for maintenance purposes (wiping and fully recomputing an account's cash history from scratch), but like the holdings-recalculation tool above, it has no button or page in the app — it can only be run manually/externally, not by staff through the UI.

---

## ISIN Management

### What it does
Lets staff correct a security's ISIN system-wide, or update a security's descriptive code/name without changing its ISIN.

### Features & status

| # | Feature / Task | What it does | Status |
|---|---|---|---|
| 1 | ISIN Rename (system-wide) | Replaces an ISIN everywhere it appears in the system. | **Live — status label is queue-time, not finish-time (known gap)** |
| 2 | Security Detail Update (code/name only) | Corrects a security's descriptive code/name without changing its ISIN. | **Live — status label is queue-time, not finish-time (known gap)** |

### On-screen fields
**Card 1 — "Update Script" (rename):** **Old ISIN** (searchable, shows matched security name/code, warns if unmatched), **New ISIN** (plain text entry), "Update" button, a **Status** indicator (In progress / Completed / Failed).

**Card 2 — "New ISIN" (apply new details):** **NEW ISIN** (searchable, same match/warning behavior), **NEW Security Code**, **NEW Security Name**, "Apply" button, its own **Status** indicator.

### Business rules
- **Rename ISIN**: given an old and new ISIN (the old one must already exist in the security master), the system replaces every reference to the old ISIN with the new one everywhere it appears — the security master, every transaction, every corporate action schedule and applied record (bonus, split, dividend, demerger, merger, on both the old and new side where relevant), price history, cash passbook entries, uploaded-but-not-yet-committed staging data, and the holdings table itself — and then automatically recalculates holdings for every account that ends up holding the renamed security, so figures stay correct after the rename.
- **Update security details**: given an existing ISIN, staff can correct its security code and/or name. This updates the security master and also refreshes the descriptive code/name shown on every existing transaction row for that ISIN — it does not touch holdings or any corporate-action history.
- A rename is comprehensive — it is designed to leave no stale references to the old ISIN anywhere in the system.
- Because a rename can affect a large number of accounts, it can take some time to complete in the background; large renames are processed in batches automatically until finished.
- The on-screen status for both a rename and a security-detail update shows "Completed" as soon as the request has been accepted and queued — not necessarily once the underlying work has actually finished running. For a large rename in particular, the real processing can still be in progress after the screen already says "Completed".

### Known gaps
The Update ISIN page does not show a live, accurate progress indicator while a rename (or detail update) is actually running in the background — the "Completed" status reflects that the request was accepted, not that the work is done.

---

## Reports & Exports

### What it does
Provides a central place to generate and download reports across the system, selected from one dropdown: **Holdings, Transactions, Cash, Top Clients, Corporate Action**.

### Features & status

| # | Feature / Task | What it does | Status |
|---|---|---|---|
| 1 | Holdings Reports | Single / consolidated / by-ISIN / all-clients holdings exports. | Live |
| 2 | Cash Reports | Single-client and all-clients cash exports. | Live |
| 3 | Transaction Report | Single-account transaction history export. | Live |
| 4 | Dividend Eligibility Report | Preview-only report of who would be eligible for a dividend. | Live |
| 5 | Corporate Action Event Log Report | Log of every corporate action in a date range, all 5 types combined. | Live |
| 6 | Corporate Action Per-Client Impact Reports | Per-account (or per-lot) breakdown of a corporate action's effect. | Live |
| 7 | Reconciliation / Differential Report | System-vs-custodian holdings comparison report. | **Not reachable from UI (known gap)** |
| 8 | Top Clients Report | Planned ranking report of top clients. | **Placeholder** |

### On-screen fields per report type

**Holdings tab:** a Scheme Wise / Consolidated mode toggle, an export-scope toggle (Export All Clients / Export Single Client / ISIN), a conditional Account Code / Actual Code / ISIN field depending on scope, an "As On Date" field, and an "Export" action. All-clients exports are tracked in a history table (**File Name, Type, Timestamp, Status, Action**).

**Transactions tab:** Account Code, As On Date, "Export" action (no history table — single-account exports only).

**Cash tab:** Export All Clients / Export Single Client toggle, conditional Account Code field, As On Date, "Export" action, with a history table (**File Name, Timestamp, Status, Action**) for all-clients exports.

**Top Clients tab:** not yet implemented ("will be available soon").

**Corporate Action tab:** a report-type selector with six options — an overall event-log export, plus a dedicated per-client impact report for each of Split, Bonus, Dividend, Merger, and Demerger — shared From Date/To Date fields, an "Export CSV" / "Generate Report" action depending on the option chosen, and a history table (**From Date, To Date, Timestamp, Status, Action**).

### Report / export formats

**Holdings — single-client export:** GENERATED_AT, AS_ON_DATE, CODE, ACTUAL_CODE, SECURITY_NAME, SECURITY_CODE, ISIN, HOLDING, WAP, HOLDING_VALUE, LAST_PRICE, MARKET_VALUE.

**Holdings — consolidated (by Actual Code) export:** GENERATED_AT, AS_ON_DATE, ACCOUNT_CODE, SECURITY_NAME, SECURITY_CODE, ISIN, HOLDING (quantity only — no value/price columns, since these don't combine cleanly across a client's split portfolios).

**Holdings — by ISIN (cross-account) export:** GENERATED_AT, AS_ON_DATE, VIRTUAL_CODE, ACTUAL_CODE, SECURITY_NAME, SECURITY_CODE, ISIN, QUANTITY, WAP, HOLDING_VALUE, LAST_PRICE, MARKET_VALUE.

**Holdings — all-clients export:** same two column layouts as above (scheme-wise or consolidated), produced as one combined file across every client.

**Cash — single-client export:** Tran Date, Set Date, Account Code, ISIN, Security Name, Tran Type, QTY, Rate, STT, Amount, Cash Balance.

**Cash — all-clients export:** Date, Account Code, Cash Balance.

**Transaction export:** Date, Type, Security Name, Security Code, ISIN, Quantity, Price, Total Amount, STT, Cash Balance — merges ordinary trades with dividend/bonus/split entries into one dated, running-balance ledger.

**Dividend eligibility report** (preview-only, no holdings/cash change): ISIN, Account Code, Holding as on Record Date, Rate, Ex-Date, Payment Date, Dividend Amount — only accounts with a real positive holding as of the record date are listed.

**Corporate action event log** (all five action types combined into one file): Date, Old ISIN, Ratio 1, Ratio 2, Type, Security Code, Security Name, Rate, Payment Date, Dividend Type, New ISIN, Effective Date, Record Date, Allocation to New (%) — one row per declared/applied event across all corporate actions in the chosen date range; columns that don't apply to a given action type (e.g. Rate for a split) are left blank on that row.

**Corporate action per-client impact reports** (one report per action type, one row per affected account — or per lot for Merger/Demerger):
- **Split:** Generated At, From Date, To Date, Issue Date, Virtual Code, Actual Code, ISIN, Type, Split Ratio, Shares Allocated.
- **Bonus:** Generated At, From Date, To Date, Ex-Date, Virtual Code, Actual Code, ISIN, Type, Bonus Ratio, Shares Allocated.
- **Dividend:** Generated At, From Date, To Date, Record Date, Virtual Code, Actual Code, ISIN, Type, Rate, Shares Held, Cash Received.
- **Merger:** Generated At, From Date, To Date, Effective Date, Record Date, Virtual Code, Actual Code, Old ISIN, New ISIN, Type, Merger Ratio, Quantity, New WAP (one row per surviving lot).
- **Demerger:** Generated At, From Date, To Date, Record Date, Effective Date, Virtual Code, Actual Code, Old ISIN, New ISIN, Type, Demerger Ratio, Allocation to New (%), Old Quantity, Old WAP, Old Holding Value, New Quantity, New WAP, New Holding Value (one row per lot, showing both the retained and spun-off side together).

**Reconciliation / differential report** (system vs. custodian comparison): Account Code, ISIN, Quantity (system), Quantity (custodian), Difference, Matching (Yes/No), Mismatch Reason ("Qty Mismatch" / "Not in Custodian" / "Not in FA" / blank), Security, Client Name, Last Traded Price Date, Last Traded Price. **Formula:** `Difference = System Quantity − Custodian Quantity`; `Matching = "Yes"` only when Difference is exactly 0, otherwise "No" with the Mismatch Reason set to "Qty Mismatch" (both sides have the position but the quantity disagrees), "Not in Custodian" (system has it, custodian file doesn't), or "Not in FA" (custodian file has it, system doesn't).

### Business rules
- Every export reflects the same FIFO-based holdings logic used elsewhere in the system — there is no separate calculation path just for reports.

### Known gaps
- A "Top Clients" report tab exists in the Reports page but is not yet implemented.
- Dedicated, standalone "export holdings affected by this action" reports for Split and Bonus are placeholders and produce nothing — this doesn't reduce functionality in practice, since the general preview export (Split/Bonus) and the per-client corporate-action impact report (above) already cover this need.
- The reconciliation/differential report has no working trigger point in the current application — the page it was meant to be launched from is not part of the running app, so it cannot be run today even though the report logic itself is implemented and correct.

---

## Module Coverage Summary

A roll-up of every task listed in this document — 60 tasks across 13 modules. This reproduces the **Summary** sheet of `PMS_Modules_Business_Logic.xlsx`.

| Module | # Tasks Listed | Live | Placeholder | Known Gaps | Internal Tools |
|---|---:|---:|---:|---:|---:|
| Master Data Management | 2 | 2 | 0 | 0 | 0 |
| Transaction Upload | 5 | 4 | 0 | 1 | 0 |
| Bhav Copy Upload | 1 | 0 | 0 | 1 | 0 |
| Holdings Engine | 10 | 8 | 0 | 1 | 1 |
| Analytics & Dashboard | 6 | 4 | 2 | 0 | 0 |
| Corporate Action — Split | 4 | 2 | 1 | 1 | 0 |
| Corporate Action — Bonus | 4 | 3 | 1 | 0 | 0 |
| Corporate Action — Dividend | 5 | 5 | 0 | 0 | 0 |
| Corporate Action — Merger | 4 | 4 | 0 | 0 | 0 |
| Corporate Action — Demerger | 4 | 3 | 0 | 1 | 0 |
| Cash Balance | 5 | 3 | 0 | 1 | 1 |
| ISIN Management | 2 | 0 | 0 | 2 | 0 |
| Reports & Exports | 8 | 6 | 1 | 1 | 0 |
| **Total** | **60** | **44** | **5** | **9** | **2** |

**How to read these counts:** each task is counted in exactly one column. A feature that works but carries a documented limitation is counted under *Known Gaps* rather than *Live* — this is why, for example, Bhav Copy Upload and both ISIN Management tasks show 0 under Live despite being in daily use. *Internal Tools* are implemented but have no page or button in the app, so they are counted separately from both Live and Placeholder.

**Where the risk is concentrated:** ISIN Management (2 of 2 tasks carry a gap) and Bhav Copy Upload (1 of 1) are the two modules where every listed task has a caveat. Analytics & Dashboard holds the largest share of pure placeholders (2 of 6 — the Dashboard widgets and the Allocation/Performance tabs).

---

## Cross-Module Notes

A few system-wide things worth knowing regardless of which module you're using:

- **No automated testing** currently backs this system — changes should be manually verified before relying on new behavior.
- **Some UI elements are placeholders**, not yet live: certain Dashboard summary widgets/charts/filters, the Analytics Allocation and Performance tabs, the Reports "Top Clients" tab, and the Split/Bonus dedicated holdings-export reports.
- **Two parallel cash-tracking systems exist** (the per-transaction passbook and an older daily summary, the latter still visibly surfaced on the Analytics page); they are not synchronized, and the Cash Balance page's passbook should be treated as authoritative.
- **New transaction uploads append to current holdings** rather than replaying every past corporate action from scratch — this is efficient for routine updates but means corporate actions and uploads for the same security are best sequenced carefully rather than interleaved arbitrarily.
- **Merger and Demerger previews are flat/detailed**, while Split, Bonus, and Dividend previews are grouped by Actual Code with drill-down — this is a genuine difference in today's UI, not an oversight to route around.
- **The reconciliation/differential report currently cannot be run from the UI at all** — treat any request for it as something to escalate rather than a self-service report, until it's re-wired to a working page.
- **Split's background recalculation has no visible completion signal** — unlike Bonus and Dividend, staff should not assume a split has fully finished processing for every account just because the initial save succeeded.
