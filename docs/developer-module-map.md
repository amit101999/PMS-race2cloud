# PMS Audit-Bridge — Developer Module Map

This map shows which files, background functions and database tables make up each module, and which of them are shared between modules. It is the developer companion to the business guide (`docs/PMS documentation (business flow).docx`), which explains what each module does for the operations team.

## How to use this map

1. Find the module you are working on in **Section 4** and open its section.
2. Its files are split into **Own files** (only this module uses them) and **Shared files** (other modules use them too).
3. Before you change anything shared, check **Sections 1–3** to see which other modules use it, and re-test those modules as well.

**How a module is wired.** Every module follows the same path:

```text
Screen (react-app/src/pages/...)
  → API (appsail-nodejs/index.js mounts the router → router → controller → util)
  → Background function (functions/<Name>, started as a Catalyst job)
  → Data Store tables
```

Background functions are deployed on their own and **cannot import** files from `appsail-nodejs`. So when a function needs the same logic as the API, the code is **copied** into the function folder. Section 2 lists these copies.

---

## 1. Shared code files

A change to one of these files affects every module listed against it. Pure display pieces (badges, icons, buttons, layout and CSS) are left out on purpose.

### 1.1 Backend (`appsail-nodejs`)

| File | Used by modules | Re-test after a change |
|---|---|---|
| `util/analytics/holdingsFromTable.js` | Analytics, Reports, Split, Bonus, Dividend, Custodian check, Transaction Deletion | Holdings "as on" a date in Analytics, Split/Bonus/Dividend preview, holdings reports. Also update its copies (Section 2). |
| `util/mapVirtualToActualCodes.js` | Analytics, Reports, Split, Bonus, Dividend, Custodian check | Consolidated (actual code) views, corporate action previews grouped by actual code. Copy in `functions/ExportCorpActionReport/virtualToActual.js`. |
| `util/allAccountCodes.js` | Analytics, Reports, Cash Balance | Account lists, all-client exports, cash calculation. Copies in two functions (Section 2). |
| `controller/analytics/tabs/holding/AnalyticsControllers.js` (with `util/analytics/bhavPrices.js`) | Analytics, Reports (single, consolidated and by-ISIN holdings exports) | Analytics holding tab and every holdings report. |
| `controller/analytics/tabs/transaction/transaction.js` (with `util/analytics/transactionHistory/ledger.js`) | Analytics, Reports (transaction export) | Analytics transaction tab and transaction report. |
| `controller/TransactionController.js` | Analytics (drill-down history), Transaction Deletion | Clicking a holding row in Analytics, and the Transaction Deletion page. |
| `controller/corporateAction/CustodianUploadController.js` (with `util/corporateAction/*`, `util/custodian/parseCustodianCsv.js`) | Custodian Upload, Split, Bonus, Dividend | Custodian file upload, and the custodian check on all three corporate action pages. |
| `util/analytics/splitPreviewCalc.js` | Split, Custodian check | Split preview, split export, split custodian check. |
| `util/analytics/bonusPreviewCalc.js`, `util/analytics/bonusAccountRows.js` | Bonus, Custodian check | Bonus preview, bonus export, bonus custodian check. |
| `util/analytics/dividendAccountRows.js` | Dividend, Custodian check | Dividend preview and dividend custodian check. |
| `controller/custodyMatch/CustodyMatchFieldMap.js`, `util/custodyMatch/fieldMap.js` | Transaction Upload, Custodian Upload, Holding Comparison | The Field Mapping window on all three pages. Also update the copies (Section 2). |
| `util/custodyMatch/normalize.js` | Transaction Upload, Custodian Upload | Date and value cleaning in trade files and custodian corporate action files. |
| `util/custodyMatch/constants.js`, `util/custodyMatch/runStore.js` | Transaction Upload, Notifications | Matching runs and the notification bell. |
| `util/holdingComparison/parse.js`, `storage.js`, `constants.js` | Holding Comparison, Custodian Upload, Transaction Upload (field mapping) | Holding statement upload, custodian file upload, field mapping preview. |
| `util/holdingComparison/runStore.js` | Holding Comparison, Transaction Upload (field mapping), Notifications | Saving the custodian holding-statement mapping marks old comparison runs as out of date. Check the bell too. |
| `util/caJobProgress.js` | Split, Bonus, Merger, Demerger, Notifications | Apply progress on all four corporate action pages and the bell. |
| `util/notifications.js` | Notifications, Transaction Upload | The notification bell for every background job. |
| `util/stratusSignedUrl.js` | Transaction Upload, Custodian Upload, Holding Comparison, Cash Balance, Reports | Every file download link. |
| `controller/cashBalance/exportCashBalance.js` | Cash Balance, Reports (Cash tab) | Single-client cash export from both pages. |

### 1.2 Frontend (`react-app/src`)

| File | Used by modules | Re-test after a change |
|---|---|---|
| `constant.js` (`BASE_URL`) | All modules | Every page that calls the API. |
| `pages/TempTransactionUpload/custody/api.js` | Transaction Upload, Custodian Upload, Holding Comparison, Split, Bonus | It lives in the Transaction Upload folder but is shared: its error and date helpers are used by four other pages. |
| `pages/TempTransactionUpload/custody/FieldMappingPanel.js` | Transaction Upload, Custodian Upload, Holding Comparison | The Field Mapping window on all three pages. |
| `pages/SplitPage/SplitReconcilePanel.js` | Split, Bonus, Dividend | The custodian check panel. It calls `/api/<split|bonus|dividend>/...` based on the page. |
| `pages/HoldingComparison/holdingComparison/PreviousFiles.js`, `api.js` | Holding Comparison, Split, Bonus | The "previous files" lists on those pages. |
| `hooks/GetAllCodes.js` | Analytics, Cash Balance, Reports | Account code drop-downs. |
| `notifications/JobNotificationContext.js` | Every module that starts a background job | The notification bell. |

### 1.3 API routes used by another module's page

| Route | Owned by | Also called by |
|---|---|---|
| `/api/split/getAllSecuritiesList` | Split | Demerger page |
| `/api/analytics/getAllAccountCodes`, `/getAllActualCodes` | Analytics | Cash Balance, Reports (through `hooks/GetAllCodes.js`) |
| `/api/security/list` | Security Master | Analytics, Reports |
| `/api/custodian-upload/latest` | Custodian Upload | Split, Bonus, Dividend (through `SplitReconcilePanel.js`) |
| `/api/cash-balance/export*` | Cash Balance | Reports (Cash tab) |
| `/api/transaction/getStockTransactionHistory` | Transaction router (`router/TransactionRouter.js`, shared with Transaction Deletion) | Analytics drill-down (`pages/TransactionPage/TransactionPage.js`) |

---

## 2. Copied code (must change together)

There is no import link between these copies. If you fix one and not the others, they silently drift apart.

**Straight copies** should stay identical to the original.

| Original | Copies | Same today? |
|---|---|---|
| `appsail-nodejs/util/custodyMatch/*` | `functions/CustodyMatchRunner/custodyMatch/*`, `functions/CustodyMatchCleanup/custodyMatch/*` | Runner: yes. Cleanup: `fieldMap.js` is an older copy (about 140 lines differ). |
| `appsail-nodejs/util/holdingComparison/*` | `functions/FAHoldingsGenerator/holdingComparison/*`, `functions/HoldingComparisonRunner/holdingComparison/*` | Yes, except `storage.js`: one `export` keyword differs. |
| `functions/ExportAllCustomerHoldingData/holdingsFromTable.js`, `allAccountCodes.js`, `zcqlTimeout.js` | `functions/FAHoldingsGenerator/` (same three files) | Yes. |

**Ports** are rewritten for the function and are not identical by design. They must follow the same rules as the original.

| Original (rule source) | Ports / twins |
|---|---|
| `appsail-nodejs/util/analytics/holdingsFromTable.js` | `functions/ExportAllCustomerHoldingData/holdingsFromTable.js`, `functions/FAHoldingsGenerator/holdingsFromTable.js`, `functions/UpdateBonusTable/holdingsSnapshot.js`, `functions/UpdateDividendData/holdingsSnapshot.js`, `functions/ExportDividendAccounts/holdingsSnapshot.js` |
| `holdingsEffectiveDate` in `holdingsFromTable.js` | Same rule in `controller/BonusController.js`, `controller/uploader/SplitController.js`, `controller/uploader/DemergerController.js`, `functions/DemergerFn/demergerApplyCore.js` |
| FIFO engine `appsail-nodejs/util/analytics/transactionHistory/fifo.js` | `functions/RebuildHoldingtable/holdingsRebuildFromSources.js`, `functions/CalculateHoldingPerAccount/index.js`, `functions/CalculateHoldingsPerAccount/index.js`, `functions/HoldingUpdateManually/index.js` |
| `appsail-nodejs/util/merger/lotFifo.js`, `mergerEngine.js` (preview) | `functions/MegerFn/fifo.js`, `functions/MegerFn/index.js` (apply) |
| `appsail-nodejs/controller/uploader/DemergerController.js` (preview) | `functions/DemergerFn/demergerApplyCore.js` (apply) |
| `appsail-nodejs/util/mapVirtualToActualCodes.js` | `functions/ExportCorpActionReport/virtualToActual.js` |
| `calculateHoldingsSummary` in `AnalyticsControllers.js` | `functions/ExportAllCustomerHoldingData/analyticsController.js` |
| `functions/Cal_CB_Append_TxnUpload/index.js` (cash after a commit) | `functions/Cal_CB_Per_TNX/index.js` (full cash rebuild) |
| `functions/UpdatesSecurity_ClientMasters/index.js` (job) | `functions/UpdateSecurity_ClientMaster/index.js` (event) |

**Values that must match across files:**

- `EXPECTED_HEADERS_IN_ORDER` and `OPTIONAL_TRAILING_HEADERS` in `controller/uploader/TempTransactionUpload.js` must match `util/custodyMatch/constants.js`.
- The ISIN target count in `controller/isin/updateIsin.js` must match `ISIN_TARGETS.length` in `functions/UpdateISINWorker/index.js`.
- `SUPPORTED_TYPES` in `controller/export/exportCorporateAction/exportCaImpactReport.js` must match the handlers in `functions/ExportCorpActionReport/handlers/`.

---

## 3. Shared database tables

A table links modules even when they share no code file. If you change how a column is written, check every reader.

| Table | Written by | Read by |
|---|---|---|
| `Transaction` | Transaction Upload commit (`TempTransactionUpload.js`), `EnrichTransactionSecurity` (fills security code and name), `UpdateSecurity_ClientMaster`, Update ISIN, Transaction Deletion (deletes rows) | Holdings functions, cash functions, Analytics, Reports, Dashboard, Transaction Deletion, Merger preview, custody matching |
| `Holdings` | `CalculateHoldingWorkers` (after a commit), `RebuildHoldingtable` (after a corporate action or a transaction deletion), `HoldingUpdateManually`, `CalculateHoldingPerAccount`, `CalculateHoldingsPerAccount`, Update ISIN | Everything that uses `holdingsFromTable.js`, Demerger, `UpdateBonusTable`, `UpdateDividendData`, `ExportDividendAccounts`, `ExportAllCustomerHoldingData`, `FAHoldingsGenerator` |
| `Cash_Balance_Per_Transaction` (passbook) | `Cal_CB_Append_TxnUpload`, `Cal_CB_Per_TNX` (manual, or after a transaction deletion), `UpdateDividendData` (dividend credit), Update ISIN | Cash Balance passbook, cash exports, Dividend (already-paid check) |
| `Cash_Ledger` | `CashCalculation` | Analytics cash summary (`CashController.js`). This is a different table from the passbook. |
| `Security_List` | `UpdatesSecurity_ClientMasters` / `UpdateSecurity_ClientMaster` (new ISINs), `MegerFn`, `DemergerFn` (new company ISIN), Update ISIN | Security Master, every corporate action page, Analytics, Reports, `EnrichTransactionSecurity` |
| `clientIds` (Client Master) | `UpdatesSecurity_ClientMasters` / `UpdateSecurity_ClientMaster` | Client Master, everything that uses `mapVirtualToActualCodes.js`, custody matching, `ExportCorpActionReport`, `FAHoldingsGenerator`, `ExportAllCustomerHoldingData` |
| `Bhav_Copy` | Bhav Copy upload, Update ISIN | Analytics, holdings reports, `FAHoldingsGenerator`, `ExportAllCustomerHoldingData` |
| `Split`, `Split_Account` | Split (`SplitController.js`), `RebuildHoldingtable` (`Split_Account`), Update ISIN (`Split`) | Holdings functions, custodian check, transaction export, corporate action reports, Merger preview |
| `Bonus`, `Bonus_Record` | `UpdateBonusTable`, Bonus (`Bonus_Record` status), Update ISIN | Holdings functions, custodian check, transaction export, corporate action reports, Merger preview |
| `Dividend`, `Dividend_Record` | `UpdateDividendData`, Dividend (`Dividend_Record` status), Update ISIN | Custodian check, transaction export, corporate action reports, `Cal_CB_Per_TNX` (re-adds RECEIVED dividends to the passbook on a full rebuild) |
| `Merger`, `Merger_Record` | `MegerFn`, Update ISIN | Holdings functions (`Merger`), Merger preview, corporate action reports |
| `Demerger`, `Demerger_Record` | `DemergerFn`, Update ISIN | Holdings functions (`Demerger_Record`), Merger preview, corporate action reports |
| `CA_Custodian_Upload`, `CA_Ex_Entitlement`, `CA_Benefit_Collection` | Custodian Upload (`util/corporateAction/custodianTables.js`) | Split, Bonus, Dividend custodian checks |
| `Custody_Match_Run`, `Custody_Match_Exception`, `Custody_Match_File` | Transaction Upload controllers, `CustodyMatchRunner`, `CustodyMatchCleanup` (nightly clean-up) | Transaction Upload, Notifications (`Custody_Match_Run`) |
| `Holding_Comparison_Run` | Holding Comparison controllers, `FAHoldingsGenerator`, `HoldingComparisonRunner`, Transaction Upload field mapping (marks runs out of date when the holding-statement mapping is saved) | Holding Comparison, Notifications |
| `Jobs`, `JobStatusPerAccount` | Almost every module that starts a background job | Apply-status and export-status endpoints, Notifications |
| `Notifications` | `NotificationController.js`, `util/notifications.js` | Notification bell |
| `Transaction_Deletion_Log` | Transaction Deletion (`TransactionController.js`) | Transaction Deletion (Deletion History), `Cal_CB_Per_TNX` (leaves out dividend credits earned through a deleted buy) |
| `Temp_Transaction`, `Temp_Custodian` | Update ISIN only (rename) | Tables from the older upload flow |

---

## 4. Modules

Each module uses the same template. The order matches the business guide.

### 4.1 Client Master
List of client accounts: virtual code → actual code. Business guide 3.1.

- **Screen:** `react-app/src/pages/Master/ClientPage.js`
- **API:** `/api/client` → `router/clientRouter/ClientRouter.js` → `controller/client/clientController.js`
- **Own files:** the three files above.
- **Shared files:** none.
- **Background functions:** `UpdatesSecurity_ClientMasters` (job, pool `UpdateMasters`, started by the Transaction Upload commit) adds new codes. `UpdateSecurity_ClientMaster` is its event-driven twin.
- **Tables:** reads `clientIds`. New rows are written by the functions above.
- **Before you change:** `clientIds` drives every virtual → actual code conversion, so a column change affects custody matching, all corporate action checks, Analytics and Reports.

### 4.2 Security Master
List of every share and fund. Business guide 3.2.

- **Screen:** `react-app/src/pages/Master/SecurityPage.js`
- **API:** `/api/security` → `router/securityRouter/SecurityRouter.js` → `controller/security/securityController.js`
- **Own files:** the three files above.
- **Shared files:** `/api/security/list` is also called by Analytics and Reports.
- **Background functions:** `UpdatesSecurity_ClientMasters` (new ISINs), then `EnrichTransactionSecurity` (fills security code and name on trades). `MegerFn` and `DemergerFn` add new-company ISINs.
- **Tables:** reads `Security_List`.
- **Before you change:** every corporate action page picks its share from `Security_List`.

### 4.3 Bhav Copy
Daily closing prices. Business guide 3.3.

- **Screen:** `react-app/src/pages/BhavCopyUpload/BhavCopyUploadPage.js`
- **API:** `/api/bhav` → `router/uploaderRouter/BhavUploaderRouter.js` → `controller/uploader/BhavUploader.js`
- **Own files:** the three files above.
- **Shared files:** none.
- **Background functions:** none. The file goes in with a Catalyst bulk write job.
- **Tables:** writes `Bhav_Copy`.
- **Before you change:** `Bhav_Copy` is read for market value by Analytics, holdings reports, `FAHoldingsGenerator` and `ExportAllCustomerHoldingData`.

### 4.4 Transaction Upload and Custody Matching
Upload → Validate → Match → Review Exceptions → Commit. Business guide 3.4.

- **Screen:** `react-app/src/pages/TempTransactionUpload/TempTransaction.js`, `WorkflowTracker.js`, and the `custody/` folder (`CustodyMatchPanel.js`, `ExceptionGrid.js`, `DifferenceReports.js`, `GateTwo.js`, `PreviousCustodianUploads.js`, `FieldMappingPanel.js`, `api.js`)
- **API:**
  - `/api/custody-match` → `router/custodyMatchRouter/CustodyMatchRouter.js` → `controller/custodyMatch/CustodyMatchFieldMap.js`, `CustodyMatchUpload.js`, `CustodyMatchRun.js`
  - `/api/transaction-uploader` → `router/uploaderRouter/TempTransactionUploaderRouter.js` → `controller/uploader/TempTransactionUpload.js` (upload history, and the commit that `CustodyMatchRun.js` calls)
- **Own files:** `util/custodyMatch/compare.js`, `parseCustody.js`, `parseFA.js`, `storage.js`, `resolveActual.js`, `lookupCodes.js`, `index.js`, and the screen files above apart from the shared ones.
- **Shared files:** `util/custodyMatch/fieldMap.js` and `CustodyMatchFieldMap.js` (Custodian Upload, Holding Comparison), `util/custodyMatch/normalize.js` (Custodian Upload), `util/custodyMatch/constants.js` and `runStore.js` (Notifications), `util/holdingComparison/parse.js` and `runStore.js` (Holding Comparison), `util/notifications.js`, `util/stratusSignedUrl.js`, and the frontend `custody/api.js` and `FieldMappingPanel.js`.
- **Background functions:**
  - `CustodyMatchRunner` (job) runs the match.
  - `CustodyMatchCleanup` (cron) clears old working files and runs every night.
  - After a commit (pool `UpdateMasters`): `UpdatesSecurity_ClientMasters` → `EnrichTransactionSecurity`, `CalculateHoldingMaster` → `CalculateHoldingWorkers`, and `Cal_CB_Append_TxnUpload`.
- **Tables:** writes `Transaction`, `Custody_Match_Run`, `Custody_Match_Exception`, `Custody_Match_File`, `Jobs`, `JobStatusPerAccount` and `Notifications`. Reads `clientIds`. Saving the custodian holding-statement mapping updates `Holding_Comparison_Run`.
- **Before you change:**
  - `util/custodyMatch/*` is copied into `CustodyMatchRunner` and `CustodyMatchCleanup`, so copy your change across.
  - The trade file headers in `TempTransactionUpload.js` must match `util/custodyMatch/constants.js`.
  - The field mapping code also serves Custodian Upload and Holding Comparison.
  - A change to how `Transaction` rows are written affects holdings, cash, Analytics and Reports.
  - The three FA `filler` columns are the optional Sub Folio, Transaction Fee and Transaction Fee Service Tax fields. The import writes them to `SUB_FOLIO`, `TOTAL_TRXNFEE` and `TOTAL_TRXNFEE_STAX`, and always calculates `NETRATE` / `Net_Amount` itself (`TempTransactionUpload.js`). `MKTRATE` (Market Rate) is saved to `MKTRATE`; only SETDATEFLAG is dropped. `SUB_FOLIO` must exist in every environment, or the commit fails.
  - Equity vs Mutual Funds transaction file is decided by `detectFaFileType` in `parseFA.js` (a TDS or Stamp Duty amount on any row means Mutual Funds). It runs in the field-map preview when the file is picked and again in `uploadFaFile`.
  - Commit sends every shown run (`POST /runs/proceed`) and they are imported as one file. `uploadTempTransaction` refuses while a `Jobs` row `TxnCsv_*`, `HOLD_*` or `CASH_*` is `RUNNING` (younger than 3 hours): the holdings and cash jobs read every trade loaded since their import started, so imports must not overlap. The `HOLD_<ms>` / `CASH_<ms>` names are built in `TempTransactionUpload.js` and again in `functions/CalculateHoldingMaster` and `functions/Cal_CB_Append_TxnUpload`; change all three together.

### 4.5 Transaction Deletion
Find committed trades by commit date, delete the wrong ones, and see what was deleted on each day. Business guide 3.5.

- **Screen:** `react-app/src/pages/TransactionDeletion/TransactionDeletionPage.js`, `TransactionDeletionPage.css`
- **API:** `/api/transaction` → `router/TransactionRouter.js` (`/commit-dates`, `/committed`, `POST /delete`, `/deletion-dates`, `/deleted`) → `controller/TransactionController.js`
- **Own files:** the screen files above.
- **Shared files:** `controller/TransactionController.js` and `router/TransactionRouter.js` also serve the Analytics drill-down. The controller imports `util/analytics/holdingsFromTable.js` for the drill-down only.
- **Background functions:** after a delete, for each affected client: `RebuildHoldingtable` (pool `CorporateActions`, limited to the ISINs that lost trades, with `keepCaRecords: "true"`) and `Cal_CB_Per_TNX` (pool `UpdateMasters`, whole passbook).
- **Tables:** deletes from `Transaction`. Writes `Transaction_Deletion_Log` (a copy of every deleted row; the history reads it by its `CREATEDTIME`). The rebuild jobs rewrite `Holdings` and `Cash_Balance_Per_Transaction`.
- **Before you change:**
  - Corporate action tables (`Bonus`, `Split`, `Split_Account`, `Merger`, `Demerger_Record`, `Dividend_Record`) are deliberately not changed by a delete. `keepCaRecords` stops `RebuildHoldingtable` from rewriting `Split_Account`; the rebuild replays the rest as saved.
  - Dividend cash is the one exception: `Cal_CB_Per_TNX` reads `Transaction_Deletion_Log` and leaves out a dividend credit when a deleted buy of that ISIN is dated on or before the record date and no remaining buy is (`dropDividendsEarnedByDeletedBuys`). This applies to every later full rebuild too.
  - `Cash_Ledger` (Analytics cash summary) is not rebuilt, the same as after an upload commit.
  - One delete is capped at 5,000 rows (`DELETE_MAX_ROWS`).

### 4.6 Holdings calculation
Works out holdings from trades and corporate actions using FIFO. Business guide 3.6. There is no screen of its own. Other modules read the result.

- **Screen:** none (read through Analytics, Reports and the corporate action previews).
- **API:** `/api/holding-update` in `appsail-nodejs/index.js` starts a manual rebuild.
- **Own files:** the function folders below.
- **Shared files:** `util/analytics/holdingsFromTable.js` is the read side. See Section 1.
- **Background functions:**
  - `CalculateHoldingMaster` → `CalculateHoldingWorkers`: adds new trades after a commit.
  - `RebuildHoldingtable`: full rebuild after Split, Bonus, Merger and Demerger, and an ISIN-limited rebuild per client after a transaction deletion. The `keepCaRecords` param (sent only by Transaction Deletion) skips the `Split_Account` writer.
  - `HoldingUpdateManually`: manual full rebuild through `/api/holding-update`.
  - `CalculateHoldingPerAccount` (event) and `CalculateHoldingsPerAccount` (job): other entry points to the same FIFO engine.
- **Tables:** reads `Transaction`, `Bonus`, `Split`, `Merger` and `Demerger_Record`. Writes `Holdings`, `Split_Account`, `Jobs` and `JobStatusPerAccount`.
- **Before you change:**
  - The FIFO engine exists in several functions (Section 2), so change every copy.
  - The same-day order (trades, split, bonus, demerger, merger) and the effective-date rule must stay the same in all of them and in `holdingsFromTable.js`.
  - Rounding rule: every quantity is rounded to 6 decimals and every amount (cost, WAP, P/L) to 4 decimals as it is calculated (`roundQty` / `roundAmt`). This stops binary leftovers such as 190.88 − 190 = 0.8799999999999955. The rule is in the FIFO engine and its copies, `FifoQty.js`, `CalculateHoldingWorkers` and `holdingsFromTable.js` and its ports. Keep it in any new copy. Screens round to 2 decimals and never cut digits off.
  - Holdings screens and exports (Analytics account and ISIN views, Export All, `FAHoldingsGenerator`) show the saved `HOLDING`, `WAP` and `HOLDING_VALUE` of the last saved row up to the as-on date. They do not re-derive them, because the `PRICE` column keeps only 4 decimals and a re-derived value can be a few paise off. Corporate action previews (Split, Bonus, Dividend) still use `recomputeHoldingsSnapshot` for the quantity. Old rows saved before the trade-date change need a holdings rebuild to be right.
  - The rule also covers the differential report FIFO (`functions/ExportDifferentialReport/fifo.js`), the unused `util/export/fifo.js`, and the consolidated totals that add a client's schemes together (`util/analytics/consolidatedHoldings.js`, `ExportAllCustomerHoldingData/index.js`, `FAHoldingsGenerator/index.js`). Not yet covered: the merger and demerger lot code.

### 4.7 Cash Balance
Client cash passbook. Business guide 3.7.

- **Screen:** `react-app/src/pages/CashBalancePage/CashBalancePage.js`
- **API:** `/api/cash-balance` → `router/cashBalanceRouter/CashbalanceRouter.js` → `controller/cashBalance/cashPassbookController.js`, `exportCashBalance.js`, `calculateBalanceOnce.js`
- **Own files:** `CashBalancePage.js`, `cashPassbookController.js`, `calculateBalanceOnce.js`
- **Shared files:** `exportCashBalance.js` (Reports), `util/allAccountCodes.js`, `util/stratusSignedUrl.js`, `hooks/GetAllCodes.js`
- **Background functions:**
  - `Cal_CB_Append_TxnUpload`: adds passbook lines after a commit.
  - `Cal_CB_Per_TNX`: full rebuild from `Transaction` plus RECEIVED `Dividend_Record` rows (from 2020 onwards, no end date), leaving out dividends earned only through a deleted buy (`Transaction_Deletion_Log`). Run manually, and per client by Transaction Deletion.
  - `ExportCashBalSingleClient` (pool `Export`).
  - `CashCalculation` (pool `Finance`) fills `Cash_Ledger`: it rebuilds each account from all its trades on every run (no "ROWID greater than last done" increment, because ROWIDs are not in time order).
  - `UpdateDividendData` (Dividend module) adds dividend credits.
- **Tables:** reads `Cash_Balance_Per_Transaction`. The functions above write it, and `CashCalculation` writes `Cash_Ledger`. `Cal_CB_Append_TxnUpload` also marks its import's `CASH_<ms>` row in `Jobs` completed.
- **Before you change:**
  - `Cal_CB_Append_TxnUpload` and `Cal_CB_Per_TNX` are twins and must follow the same money-in/money-out rules.
  - Dividend credits land in the same table. `Cal_CB_Per_TNX` deletes them with everything else and re-adds them from `Dividend_Record`, so a change to how `UpdateDividendData` writes a credit must be mirrored in `fetchReceivedDividendEvents`.
  - The Analytics cash summary reads `Cash_Ledger`, not the passbook.

### 4.8 Analytics
Read-only view of one client or one share. Business guide 3.8.

- **Screen:** `react-app/src/pages/Analytics/AnalyticsPage.js`, `tabs/holding/HoldingTab.js`, `tabs/transaction/TransactionTab.js`, `tabs/allocations/AllocationTab.js` and `tabs/performance/PerformanceTab.js` (placeholders), `pages/Holding/HoldingCards.js`, `pages/TransactionPage/TransactionPage.js` (drill-down), `hooks/GetHolding.js`
- **API:** `/api/analytics` → `router/AnalyticsRouter.js` → `controller/analytics/tabs/holding/AnalyticsControllers.js`, `CashController.js`, `controller/analytics/tabs/transaction/transaction.js`. The drill-down uses `/api/transaction/getStockTransactionHistory`.
- **Own files:** the screen files, `CashController.js`, `util/analytics/bhavPrices.js`
- **Shared files:** `AnalyticsControllers.js` and `transaction.js` (Reports), `controller/TransactionController.js` (Transaction Deletion), `util/analytics/holdingsFromTable.js`, `util/mapVirtualToActualCodes.js`, `util/allAccountCodes.js`, `hooks/GetAllCodes.js`
- **Background functions:** none.
- **Tables:** reads `Holdings`, `Transaction`, `Bhav_Copy`, `Security_List`, `clientIds` and `Cash_Ledger`.
- **Before you change:** the holdings and transaction controllers also produce the Reports exports, and `functions/ExportAllCustomerHoldingData/analyticsController.js` mirrors the holdings summary.

### 4.9 Custodian Upload and the two-stage custodian check
Ex-Entitlement and Benefit Collection files, and the Receivable → Received check. Business guide 3.9–3.11.

- **Screen:** `react-app/src/pages/CustodianUploadPage/CustodianUploadPage.js`, plus `pages/SplitPage/SplitReconcilePanel.js` (the check panel on Split, Bonus and Dividend)
- **API:** `/api/custodian-upload` → `router/CustodianUploadRouter.js` → `controller/corporateAction/CustodianUploadController.js`. The Split and Bonus routers also use this controller for `compare-custodian`, `compare-benefit` and `history`. The Dividend router uses its `compareDividendWithBenefit`.
- **Own files:** `CustodianUploadPage.js`, `util/corporateAction/compareCustodian.js`, `util/corporateAction/custodianTables.js`, `util/custodian/parseCustodianCsv.js`
- **Shared files:** `CustodianUploadController.js` and `SplitReconcilePanel.js` (Split, Bonus, Dividend), `util/custodyMatch/fieldMap.js` and `normalize.js` (Transaction Upload), `util/holdingComparison/parse.js` and `storage.js` (Holding Comparison), the preview calculations in `util/analytics/` (`splitPreviewCalc.js`, `bonusPreviewCalc.js`, `bonusAccountRows.js`, `dividendAccountRows.js`)
- **Background functions:** none. Files are imported with a Catalyst bulk write job.
- **Tables:** writes `CA_Custodian_Upload`, `CA_Ex_Entitlement` and `CA_Benefit_Collection`. Reads `Holdings`, `Split_Account`, `Bonus_Record`, `Dividend`, `Dividend_Record`, `Security_List` and `clientIds`.
- **Before you change:** any change to the comparison labels or columns shows up on three pages (Split, Bonus, Dividend) at once.

### 4.10 Split
Business guide 3.12.

- **Screen:** `react-app/src/pages/SplitPage/SplitPage.js` (+ `SplitReconcilePanel.js`)
- **API:** `/api/split` → `router/SplitRouter.js` → `controller/uploader/SplitController.js`, `controller/export/exportSplit/exportSplitFile.js`, `CustodianUploadController.js`
- **Own files:** `SplitPage.js`, `SplitController.js`, `exportSplitFile.js`
- **Shared files:** `util/analytics/splitPreviewCalc.js`, `holdingsFromTable.js`, `mapVirtualToActualCodes.js`, `caJobProgress.js`, `CustodianUploadController.js`, `SplitReconcilePanel.js`, `PreviousFiles.js`. The Demerger page also calls `/api/split/getAllSecuritiesList`.
- **Background functions:** `RebuildHoldingtable` (pool `CorporateActions`). `UpdateBonusTable` recalculates any bonus whose record date is after the split.
- **Tables:** writes `Split` and `Split_Account`. Reads `Holdings`, `Security_List` and `clientIds`.
- **Before you change:** a split can trigger a bonus recalculation, so re-test Bonus too.

### 4.11 Bonus
Business guide 3.13.

- **Screen:** `react-app/src/pages/BonusPage/BonusPage.js` (+ `SplitReconcilePanel.js`)
- **API:** `/api/bonus` → `router/BonusRouter.js` → `controller/BonusController.js`, `controller/export/exportBonus/exportBonusFile.js`, `CustodianUploadController.js`
- **Own files:** `BonusPage.js`, `BonusController.js`, `exportBonusFile.js`
- **Shared files:** `util/analytics/bonusPreviewCalc.js`, `bonusAccountRows.js`, `holdingsFromTable.js`, `mapVirtualToActualCodes.js`, `caJobProgress.js`, `CustodianUploadController.js`, `SplitReconcilePanel.js`, `PreviousFiles.js`
- **Background functions:** `UpdateBonusTable` (pool `CorporateActions`), then `RebuildHoldingtable`.
- **Tables:** writes `Bonus` and `Bonus_Record`. Reads `Holdings`, `Security_List` and `clientIds`.
- **Before you change:** `functions/UpdateBonusTable/holdingsSnapshot.js` must give the same holding as `holdingsFromTable.js`. If they differ, the applied bonus will not match the approved preview.

### 4.12 Dividend
Business guide 3.14.

- **Screen:** `react-app/src/pages/Dividend/DividendPage.js` (+ `SplitReconcilePanel.js`)
- **API:** `/api/dividend` → `router/uploaderRouter/DividendUploaderRouter.js` → `controller/uploader/DividendUploader.js`, `controller/export/exportDividend/exportDividendFile.js`, `CustodianUploadController.js`
- **Own files:** `DividendPage.js`, `DividendUploader.js`, `exportDividendFile.js`
- **Shared files:** `util/analytics/dividendAccountRows.js`, `holdingsFromTable.js`, `mapVirtualToActualCodes.js`, `CustodianUploadController.js`, `SplitReconcilePanel.js`
- **Background functions:** `UpdateDividendData` (pool `CorporateActions`) and `ExportDividendAccounts` (pool `Export`).
- **Tables:** writes `Dividend`, `Dividend_Record` and `Cash_Balance_Per_Transaction`. Reads `Holdings`.
- **Before you change:** a dividend writes into the cash passbook, so re-test Cash Balance. The `holdingsSnapshot.js` ports must match `holdingsFromTable.js`.

### 4.13 Merger
Business guide 3.15.

- **Screen:** `react-app/src/pages/MergerPage/MergerPage.js`
- **API:** `/api/merger` → `router/MergerRouter.js` → `controller/MergerController.js`
- **Own files:** `MergerPage.js`, `MergerController.js`, `util/merger/lotFifo.js`, `util/merger/mergerEngine.js`
- **Shared files:** `util/caJobProgress.js`
- **Background functions:** `MegerFn` (pool `CorporateActions`), then `RebuildHoldingtable`.
- **Tables:** writes `Merger`, `Merger_Record` and `Security_List` (new ISIN). Reads `Transaction`, `Bonus`, `Split` and `Demerger_Record`.
- **Before you change:** the preview (`util/merger/*`) and the apply job (`functions/MegerFn/*`) are twins. Per-lot rounding must be the same in both, or the applied result will not match the preview.

### 4.14 Demerger
Business guide 3.16.

- **Screen:** `react-app/src/pages/DemergerPage/DemergerPage.js`
- **API:** `/api/demerger` → `router/DemergerRouter.js` → `controller/uploader/DemergerController.js`. The ISIN list comes from `/api/split/getAllSecuritiesList`.
- **Own files:** `DemergerPage.js`, `DemergerController.js`
- **Shared files:** `util/caJobProgress.js`, and the Split router's securities list.
- **Background functions:** `DemergerFn` (pool `CorporateActions`), then `RebuildHoldingtable`.
- **Tables:** writes `Demerger`, `Demerger_Record` and `Security_List` (new ISIN). Reads `Holdings`.
- **Before you change:** `DemergerController.js` (preview) and `functions/DemergerFn/demergerApplyCore.js` (apply) are twins.

### 4.15 Holding Comparison
Our holdings vs the custodian's holding statement. Business guide 3.17.

- **Screen:** `react-app/src/pages/HoldingComparison/HoldingComparison.js` and the `holdingComparison/` folder (`HoldingComparisonPanel.js`, `ExceptionGrid.js`, `PreviousFiles.js`, `api.js`)
- **API:** `/api/holding-comparison` → `router/holdingComparisonRouter/HoldingComparisonRouter.js` → `controller/holdingComparison/HoldingComparisonUpload.js`, `HoldingComparisonFaHoldings.js`, `HoldingComparisonRun.js`
- **Own files:** `HoldingComparison.js`, `HoldingComparisonPanel.js`, `ExceptionGrid.js`, the three controllers, `util/holdingComparison/compare.js`, `util/holdingComparison/index.js`
- **Shared files:** `util/holdingComparison/parse.js`, `storage.js`, `constants.js`, `runStore.js` (Custodian Upload, Transaction Upload field mapping, Notifications), `PreviousFiles.js` and `api.js` (Split, Bonus), the frontend `custody/FieldMappingPanel.js` and `custody/api.js`
- **Background functions:** `FAHoldingsGenerator` builds the FA snapshot. `HoldingComparisonRunner` runs the comparison.
- **Tables:** writes `Holding_Comparison_Run` and `Jobs`. The functions read `Holdings`, `Bhav_Copy`, `Security_List` and `clientIds`.
- **Before you change:** `util/holdingComparison/*` is copied into both functions. `FAHoldingsGenerator` also carries copies of `holdingsFromTable.js` and `allAccountCodes.js`.

### 4.16 Update ISIN
Rename an ISIN everywhere, or update security details. Business guide 3.18.

- **Screen:** `react-app/src/pages/update-isin/UpdateISINPage.js`
- **API:** `/api/isin` → `router/IsinRouter.js` → `controller/isin/updateIsin.js`
- **Own files:** the three files above.
- **Shared files:** none.
- **Background functions:** `UpdateISIN` (pool `UpdateMasters`), then `UpdateISINWorker`, one piece per table.
- **Tables:** renames the ISIN in `Transaction`, `Holdings`, `Cash_Balance_Per_Transaction`, `Bhav_Copy`, `Security_List`, every corporate action table and their `_Record` tables, `Temp_Transaction` and `Temp_Custodian`.
- **Before you change:**
  - A **new table with an ISIN column** must be added to `ISIN_TARGETS` in `functions/UpdateISINWorker/index.js`, or renamed ISINs will be left behind in it.
  - The target count in `updateIsin.js` must match `ISIN_TARGETS.length`.

### 4.17 Reports
CSV exports. Business guide 3.19.

- **Screen:** `react-app/src/pages/Reports/ReportsPage.js` and `tabs/` (`Holdings.js`, `Transactionstab.js`, `CashTab.js`, `CorporateActionTab.js`, `TopClientsTab.js`), `notifications/reportJobStatus.js`
- **API:** `/api/export` → `router/export/ExportRouter.js` → `controller/export/exportHolding/*`, `exportTransaction/exportSingleClient.js`, `exportCashBalance/exportAllClientsCash.js`, `exportCorporateAction/*`. The Cash tab also uses `/api/cash-balance/export*`.
- **Own files:** the screen files, the export controllers above, `util/analytics/consolidatedHoldings.js`, `util/reportTimestamp.js`
- **Shared files:** `AnalyticsControllers.js` and `transaction.js` (Analytics), `exportCashBalance.js` (Cash Balance), `holdingsFromTable.js`, `mapVirtualToActualCodes.js`, `allAccountCodes.js`, `stratusSignedUrl.js`, `hooks/GetAllCodes.js`
- **Background functions:** `ExportAllCustomerHoldingData`, `ExportCashBalance`, `ExportCashBalSingleClient` and `ExportCorpActionReport` (all pool `Export`).
- **Tables:** reads `Holdings`, `Transaction`, `Cash_Balance_Per_Transaction`, every corporate action table, `Bhav_Copy`, `Security_List` and `clientIds`.
- **Before you change:**
  - Holdings and transaction reports reuse the Analytics controllers.
  - A new corporate action type needs a handler in `functions/ExportCorpActionReport/handlers/` and an entry in `SUPPORTED_TYPES`.

### 4.18 Dashboard and Notifications
Business guide 3.20.

- **Screen:** `react-app/src/pages/Dashboard/DashboardPage.js`, `components/dashboard/DashboardComponents.js`, `notifications/NotificationTray.js`, `notifications/JobNotificationContext.js`
- **API:**
  - `/api/dashboard` → `router/DashboardRouter.js` → `controller/DashboardController.js`
  - `/api/notifications` → `router/NotificationRouter.js` → `controller/NotificationController.js`
- **Own files:** the screen files, both controllers, `util/notifications.js`
- **Shared files:** `util/caJobProgress.js`, `util/custodyMatch/runStore.js`, `util/holdingComparison/runStore.js`
- **Background functions:** none. The bell follows the jobs that other modules start.
- **Tables:** Dashboard reads `Transaction`. Notifications reads and writes `Notifications`, and reads `Jobs`, `JobStatusPerAccount`, `Custody_Match_Run` and `Holding_Comparison_Run`.
- **Before you change:** the bell reads the job and run tables of other modules. A change to their status values must be reflected in `util/notifications.js`.
