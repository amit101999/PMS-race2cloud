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
| `controller/custodyMatch/CustodyMatchFieldMap.js`, `util/custodyMatch/fieldMap.js` | Transaction Upload, Mutual Funds Upload, OPI/OPO Upload (its own `FA_OPI_OPO` mapping: the `FA` fields without `transactionId`), Custodian Upload, Holding Comparison | The Field Mapping window on all three pages. Also update the copies (Section 2). |
| `util/custodyMatch/normalize.js` | Transaction Upload, Custodian Upload | Date and value cleaning in trade files and custodian corporate action files. |
| `util/custodyMatch/constants.js`, `util/custodyMatch/runStore.js` | Transaction Upload, Mutual Funds Upload, OPI/OPO Upload (`FA_FILE_TYPE`), Notifications, Transaction Deletion (`runStore.js`: `reopenRunsForDeletedTrades`) | Matching runs, the notification bell, and a delete re-opening committed dates. |
| `util/holdingComparison/parse.js`, `storage.js`, `constants.js` | Holding Comparison, Custodian Upload, Transaction Upload (field mapping) | Holding statement upload, custodian file upload, field mapping preview. |
| `util/holdingComparison/runStore.js` | Holding Comparison, Transaction Upload (field mapping), Notifications | Saving the custodian holding-statement mapping marks old comparison runs as out of date. Check the bell too. |
| `util/caJobProgress.js` | Split, Bonus, Merger, Demerger, Notifications | Apply progress on all four corporate action pages and the bell. |
| `util/notifications.js` | Notifications, Transaction Upload | The notification bell for every background job. |
| `util/stratusSignedUrl.js` | Transaction Upload, Custodian Upload, Holding Comparison, Cash Balance, Reports | Every file download link. |
| `controller/cashBalance/exportCashBalance.js` | Cash Balance, Reports (Cash tab) | Single-client cash export from both pages. |
| `controller/uploader/TempTransactionUpload.js` (`normalizeCsvForBulkUpload`, `HEADER_MAP`) | Transaction Upload, Mutual Funds Upload, OPI/OPO Upload | All three tabs' Commit turn their final list into the bulk-write file here; `{ mutualFund: true }` works out the mutual fund amount and marks the row `Security_Type = 'm'`. |
| `controller/custodyMatch/EquityReload.js`, `util/custodyMatch/equityReload.js`, `util/custodyMatch/changeWindow.js` | Transaction Upload, Mutual Funds Upload, OPI/OPO Upload | One upload / window / Commit engine for the two date-based tabs, chosen by `kind` (`EQUITY` or `MUTUAL_FUND`); `bookedTradeTab` decides which tab owns a booked trade. The OPI/OPO tab (`OpiOpoUpload.js`) uses its job helpers and `equityReload.js` rules. See 4.6, 4.7 and 4.8. |
| `util/custodyMatch/parseFA.js`, `storage.js` | Transaction Upload, Mutual Funds Upload, OPI/OPO Upload | Reading the trade file and the staged files on all three tabs (`FA_FILE_TYPES` in `storage.js`). |
| `pages/TempTransactionUpload/custody/CustodyMatchPanel.js`, `PreCommitPanel.js`, `CommitHistory.js`, `api.js` | Transaction Upload, Mutual Funds Upload, OPI/OPO Upload (all but `PreCommitPanel.js`) | The Fund Accounting card, Commit section and Commit history (`kind` prop); the Mutual Funds and OPI/OPO tabs hide the custodian card and Reconciliation, and OPI/OPO also hides the card date and the T+N dropdown. |

### 1.2 Frontend (`react-app/src`)

| File | Used by modules | Re-test after a change |
|---|---|---|
| `constant.js` (`BASE_URL`) | All modules | Every page that calls the API. |
| `pages/TempTransactionUpload/custody/api.js` | Transaction Upload, Custodian Upload, Holding Comparison, Split, Bonus | It lives in the Transaction Upload folder but is shared: its error and date helpers are used by four other pages. |
| `pages/TempTransactionUpload/custody/FieldMappingPanel.js` | Transaction Upload, Mutual Funds Upload, OPI/OPO Upload, Custodian Upload, Holding Comparison | The Field Mapping window on all three pages. |
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
| `appsail-nodejs/util/custodyMatch/*` | `functions/CustodyMatchRunner/custodyMatch/*`, `functions/CustodyMatchCleanup/custodyMatch/*` | Runner: yes. Cleanup: yes (`fieldMap.js` brought in line 2026-11-17). |
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
- `BOOKED_COLUMNS` in `util/custodyMatch/equityReload.js` (Transaction column → template position) must be the reverse of `HEADER_MAP` in `controller/uploader/TempTransactionUpload.js`.
- The mutual fund rule (ISIN `INF…` and exchange `DIR`) in `isMutualFundTrade` (`util/custodyMatch/equityReload.js`) must match the one in `util/custodyMatch/normalize.js`.
- The 34 mutual fund columns written by `tradesToMutualFundCsv` (`util/custodyMatch/equityReload.js`, `MUTUAL_FUND_BOOKED_COLUMNS`: `TDSAMOUNT`, `STAMPDUTY`, `Column1`–`Column5`) must match `HEADER_MAP` in `TempTransactionUpload.js`, `EXTRA_COLUMN_HEADERS` in `util/custodyMatch/parseFA.js` and `EXTRA_COLUMN_FIELDS` in `fieldMap.js`. The Equity consolidated file's `Column1`–`Column5` (`EXTRA_BOOKED_COLUMNS` in `equityReload.js`, `FA_CONSOLIDATED_PROFILE` in `fieldMap.js`) follow the same names.
- The mutual fund marker (`Security_Type = 'm'`) is written in `TempTransactionUpload.js` and read by the cash and holdings functions listed in 4.7, and by `isBookedMutualFund` (`util/custodyMatch/equityReload.js`). `bookedTradeTab` uses it, then `OPI_OPO_TYPES`, to split booked trades between the three tabs (4.8).
- `FA_FILE_TYPE` in `util/custodyMatch/constants.js` and `FA_FILE_TYPES` in `storage.js` must match their copies in `CustodyMatchRunner` and `CustodyMatchCleanup`, and `FA_FILE_TYPE` in the frontend `custody/api.js`.
- The ISIN target count in `controller/isin/updateIsin.js` must match `ISIN_TARGETS.length` in `functions/UpdateISINWorker/index.js`.
- `SUPPORTED_TYPES` in `controller/export/exportCorporateAction/exportCaImpactReport.js` must match the handlers in `functions/ExportCorpActionReport/handlers/`.

---

## 3. Shared database tables

A table links modules even when they share no code file. If you change how a column is written, check every reader.

| Table | Written by | Read by |
|---|---|---|
| `Transaction` | Transaction Upload commit (`TempTransactionUpload.js`, old screen), Mutual Funds Pre-commit (`EquityReload.js` with `kind = MUTUAL_FUND`: deletes the date's mutual fund trades, then bulk-writes them again, 4.7), Equity Pre-commit (`EquityReload.js`: deletes the date's equity trades, then bulk-writes them again), OPI/OPO Commit (`OpiOpoUpload.js`: adds a file's OPI and OPO rows, never deletes, 4.8), `EnrichTransactionSecurity` (fills security code and name), `UpdateSecurity_ClientMaster`, Update ISIN, Transaction Deletion (deletes rows) | Holdings functions, cash functions, Analytics, Reports, Dashboard, Transaction Deletion, Merger preview, custody matching |
| `Holdings` | `CalculateHoldingWorkers` (after a commit), `RebuildHoldingtable` (after a corporate action, a transaction deletion, a commit with a late trade, an Equity or Mutual Funds Pre-commit, or an OPI/OPO Commit), the Equity and Mutual Funds Pre-commits (`EquityReload.js` deletes the affected pairs first), `HoldingUpdateManually`, `CalculateHoldingPerAccount`, `CalculateHoldingsPerAccount`, Update ISIN | Everything that uses `holdingsFromTable.js`, Demerger, `UpdateBonusTable`, `UpdateDividendData`, `ExportDividendAccounts`, `ExportAllCustomerHoldingData`, `FAHoldingsGenerator` |
| `Cash_Balance_Per_Transaction` (passbook) | `Cal_CB_Append_TxnUpload`, `Cal_CB_Per_TNX` (manual, after a transaction deletion, for a commit with a late trade, or after an Equity Pre-commit), Equity Pre-commit (`EquityReload.js` deletes the affected passbooks first), `UpdateDividendData` (dividend credit), Update ISIN | Cash Balance passbook, cash exports, Dividend (already-paid check). The OPI/OPO Commit never touches it. |
| `Cash_Ledger` | `CashCalculation` | Analytics cash summary (`CashController.js`). This is a different table from the passbook. |
| `Security_List` | `UpdatesSecurity_ClientMasters` / `UpdateSecurity_ClientMaster` (new ISINs), Equity Pre-commit (deletes unused stub rows), `MegerFn`, `DemergerFn` (new company ISIN), Update ISIN | Security Master, every corporate action page, Analytics, Reports, `EnrichTransactionSecurity` |
| `clientIds` (Client Master) | `UpdatesSecurity_ClientMasters` / `UpdateSecurity_ClientMaster`, Equity Pre-commit (deletes unused stub rows) | Client Master, everything that uses `mapVirtualToActualCodes.js`, custody matching, `ExportCorpActionReport`, `FAHoldingsGenerator`, `ExportAllCustomerHoldingData` |
| `Bhav_Copy` | Bhav Copy upload, Update ISIN | Analytics, holdings reports, `FAHoldingsGenerator`, `ExportAllCustomerHoldingData` |
| `Split`, `Split_Account` | Split (`SplitController.js`), `RebuildHoldingtable` (`Split_Account`), Update ISIN (`Split`) | Holdings functions, custodian check, transaction export, corporate action reports, Merger preview |
| `Bonus`, `Bonus_Record` | `UpdateBonusTable`, Bonus (`Bonus_Record` status), Update ISIN | Holdings functions, custodian check, transaction export, corporate action reports, Merger preview |
| `Dividend`, `Dividend_Record` | `UpdateDividendData`, Dividend (`Dividend_Record` status), Update ISIN | Custodian check, transaction export, corporate action reports, `Cal_CB_Per_TNX` (re-adds RECEIVED dividends to the passbook on a full rebuild) |
| `Merger`, `Merger_Record` | `MegerFn`, Update ISIN | Holdings functions (`Merger`), Merger preview, corporate action reports |
| `Demerger`, `Demerger_Record` | `DemergerFn`, Update ISIN | Holdings functions (`Demerger_Record`), Merger preview, corporate action reports |
| `CA_Custodian_Upload`, `CA_Ex_Entitlement`, `CA_Benefit_Collection` | Custodian Upload (`util/corporateAction/custodianTables.js`) | Split, Bonus, Dividend custodian checks |
| `Custody_Match_Run`, `Custody_Match_Exception`, `Custody_Match_File` | Transaction Upload controllers, `CustodyMatchRunner`, `CustodyMatchCleanup` (nightly clean-up), Transaction Deletion (re-opens committed runs) | Transaction Upload, Notifications (`Custody_Match_Run`) |
| `Holding_Comparison_Run` | Holding Comparison controllers, `FAHoldingsGenerator`, `HoldingComparisonRunner`, Transaction Upload field mapping (marks runs out of date when the holding-statement mapping is saved) | Holding Comparison, Notifications |
| `Jobs`, `JobStatusPerAccount` | Almost every module that starts a background job | Apply-status and export-status endpoints, Notifications |
| `Notifications` | `NotificationController.js`, `util/notifications.js` | Notification bell |
| `Transaction_Commit_History` | Equity and Mutual Funds Commit (`EquityReload.js`), OPI/OPO Commit (`OpiOpoUpload.js`); Mutual Funds rows set `Asset_Kind = 'MUTUAL_FUND'`, OPI/OPO rows `'OPI_OPO'`, Equity rows leave it empty | Commit history on each tab (`listCommitHistory` splits by `Asset_Kind`) |
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
- **Before you change:**
  - `clientIds` drives every virtual → actual code conversion, so a column change affects custody matching, all corporate action checks, Analytics and Reports.
  - `/api/client/list` and `/details` return only `CLIENT_COLUMNS` in `clientController.js` (11 columns plus `ROWID`). PAN, Aadhaar, bank and nominee columns must never be sent. To show another column, add it to `CLIENT_COLUMNS` and to `COLUMNS` in `ClientPage.js`.

### 4.2 Security Master
List of every share and fund. Business guide 3.2.

- **Screen:** `react-app/src/pages/Master/SecurityPage.js`
- **API:** `/api/security` → `router/securityRouter/SecurityRouter.js` → `controller/security/securityController.js`
- **Own files:** the three files above.
- **Shared files:** `/api/security/list` is also called by Analytics and Reports.
- **Background functions:** `UpdatesSecurity_ClientMasters` (new ISINs), then `EnrichTransactionSecurity` (fills security code and name on trades). `MegerFn` and `DemergerFn` add new-company ISINs.
- **Tables:** reads `Security_List`.
- **Before you change:** every corporate action page picks its share from `Security_List`.

### 4.3 Transaction Type Master
List of transaction type codes and what each one means (BY- = Buy, SL+ = Sell, ...). Business guide 3.3.

- **Screen:** `react-app/src/pages/Master/TransactionTypePage.js`
- **API:** `/api/transaction-type` (`GET /list`, `POST /add`) → `router/transactionTypeRouter/TransactionTypeRouter.js` → `controller/transactionType/transactionTypeController.js`
- **Own files:** the three files above.
- **Shared files:** `styles/ops-pages.css` (the Master section also holds the Add button and modal styles).
- **Background functions:** none. The first 28 rows were loaded from the transaction types master file; the **+ Add** button adds more.
- **Tables:** reads and writes `Transaction_Type` (`Tran_Type` unique code, stored in upper case; `Tran_Desc`).
- **Before you change:** nothing else reads this table yet.

### 4.4 Scheme Master
List of PMS schemes (investment strategies) with their UCC, DP ID, acronym and pool and dividend bank accounts. Business guide 3.4.

- **Screen:** `react-app/src/pages/Master/SchemePage.js`
- **API:** `/api/scheme` (`GET /list`, `POST /add`) → `router/schemeRouter/SchemeRouter.js` → `controller/scheme/schemeController.js`
- **Own files:** the three files above.
- **Shared files:** `styles/ops-pages.css` (Master section: Add button and modal styles, shared with Transaction Type).
- **Background functions:** none. The first 13 rows were loaded from the schemes master file (only the highlighted columns); the **+ Add** button adds more.
- **Tables:** reads and writes `Scheme` (`Scheme_Name` unique). Add checks Name, UCC / Strategy and Acronym are unique, and sets `Scheme_Status` from `Scheme_Active` (Active / Inactive).
- **Before you change:** nothing else reads this table yet. Client rows name their scheme in `clientIds.Scheme_Code`, written differently (for example "PriceBridge Upswing" vs "PriceBridge - Upswing"), so the two are not linked.

### 4.5 Bhav Copy
Daily closing prices. Business guide 3.5.

- **Screen:** `react-app/src/pages/BhavCopyUpload/BhavCopyUploadPage.js`
- **API:** `/api/bhav` → `router/uploaderRouter/BhavUploaderRouter.js` → `controller/uploader/BhavUploader.js`
- **Own files:** the three files above.
- **Shared files:** none.
- **Background functions:** none. The file goes in with a Catalyst bulk write job.
- **Tables:** writes `Bhav_Copy`.
- **Before you change:** `Bhav_Copy` is read for market value by Analytics, holdings reports, `FAHoldingsGenerator` and `ExportAllCustomerHoldingData`.

### 4.6 Transaction Upload and Custody Matching
Upload → Validate → Match → Review Exceptions → Commit. Business guide 3.6.

- **Screen:** `react-app/src/pages/TempTransactionUpload/TempTransaction.js` (tabs Equity / Mutual Funds / Cash / OPI/OPO; the old single page is kept there, commented out), `WorkflowTracker.js`, and the `custody/` folder (`CustodyMatchPanel.js` with `reloadMode` on the Equity tab, `PreCommitPanel.js` (shown as **Commit**), `CommitHistory.js`, `ReloadBadges.js`, `ExceptionGrid.js`, `DifferenceReports.js`, `GateTwo.js` (old Commit, no longer used), `PreviousCustodianUploads.js` (no longer shown; the History section lists the Consolidated files instead), `FieldMappingPanel.js`, `api.js`)
- **API:**
  - `/api/custody-match` → `router/custodyMatchRouter/CustodyMatchRouter.js` → `controller/custodyMatch/CustodyMatchFieldMap.js`, `CustodyMatchUpload.js`, `CustodyMatchRun.js`
  - `/api/custody-match/reload/*` (Equity tab: `fa-file`, `custody-file`, `run`, `consolidated-files`, `commit-history`, `windows`, `:date`, `:date/window` (PUT), `:date/pre-commit`, `:date/retry`) → `controller/custodyMatch/EquityReload.js`, rules in `util/custodyMatch/equityReload.js`. The old `POST /runs/proceed` (Commit) route is commented out.
  - `/api/transaction-uploader` → `router/uploaderRouter/TempTransactionUploaderRouter.js` → `controller/uploader/TempTransactionUpload.js` (upload history, and the commit that `CustodyMatchRun.js` calls)
- **Own files:** `util/custodyMatch/equityReload.js` (not copied into any function; shared with 4.7), `util/custodyMatch/changeWindow.js` (reads and saves each trade date's change window, one per tab; AppSail only, kept out of the copied `storage.js`; shared with 4.7), `controller/custodyMatch/EquityReload.js` (shared with 4.7), `util/custodyMatch/compare.js`, `parseCustody.js`, `parseFA.js`, `storage.js`, `resolveActual.js`, `lookupCodes.js`, `index.js`, and the screen files above apart from the shared ones.
- **Shared files:** `util/custodyMatch/fieldMap.js` and `CustodyMatchFieldMap.js` (Custodian Upload, Holding Comparison), `util/custodyMatch/normalize.js` (Custodian Upload), `util/custodyMatch/constants.js` and `runStore.js` (Notifications), `util/holdingComparison/parse.js` and `runStore.js` (Holding Comparison), `util/notifications.js`, `util/stratusSignedUrl.js`, and the frontend `custody/api.js` and `FieldMappingPanel.js`.
- **Background functions:**
  - `CustodyMatchRunner` (job) runs the match. With the `consolidatedKey` param (Equity tab) it reads that one file as the FA side and every custodian file of the date, whatever its upload date.
  - After an Equity Pre-commit (started from AppSail, no job of its own): `UpdatesSecurity_ClientMasters` → `EnrichTransactionSecurity`, then per affected client `RebuildHoldingtable` (pool `CorporateActions`, `keepCaRecords`) and `Cal_CB_Per_TNX` (pool `UpdateMasters`), each with `importJobRowName` `RELOAD_<date>_H_<n>` / `RELOAD_<date>_C_<n>`.
  - `CustodyMatchCleanup` (cron) clears old working files and runs every night.
  - After a commit (pool `UpdateMasters`): `UpdatesSecurity_ClientMasters` → `EnrichTransactionSecurity`, `CalculateHoldingMaster` → `CalculateHoldingWorkers`, and `Cal_CB_Append_TxnUpload`.
  - For a client with a late trade (dated before rows already saved): `CalculateHoldingWorkers` queues `RebuildHoldingtable` (pool `CorporateActions`) and `Cal_CB_Append_TxnUpload` queues `Cal_CB_Per_TNX` (pool `UpdateMasters`), instead of appending.
- **Tables:** writes `Transaction`, `Custody_Match_Run`, `Custody_Match_Exception`, `Custody_Match_File`, `Transaction_Commit_History`, `Jobs`, `JobStatusPerAccount` and `Notifications`. Reads `clientIds`. Saving the custodian holding-statement mapping updates `Holding_Comparison_Run`.
- **Before you change:**
  - `util/custodyMatch/*` is copied into `CustodyMatchRunner` and `CustodyMatchCleanup`, so copy your change across.
  - The trade file headers in `TempTransactionUpload.js` must match `util/custodyMatch/constants.js`.
  - The field mapping code also serves Custodian Upload and Holding Comparison.
  - A change to how `Transaction` rows are written affects holdings, cash, Analytics and Reports.
  - The three FA `filler` columns are the optional Sub Folio, Transaction Fee and Transaction Fee Service Tax fields. The import writes them to `SUB_FOLIO`, `TOTAL_TRXNFEE` and `TOTAL_TRXNFEE_STAX`, and always calculates `NETRATE` / `Net_Amount` itself (`TempTransactionUpload.js`). `MKTRATE` (Market Rate) is saved to `MKTRATE`; only SETDATEFLAG is dropped. `SUB_FOLIO` must exist in every environment, or the commit fails.
  - Equity vs Mutual Funds transaction file is decided by `detectFaFileType` in `parseFA.js` (a TDS or Stamp Duty amount on any row means Mutual Funds). It runs in the field-map preview when the file is picked and again in `uploadFaFile`, which refuses a Mutual Funds file and points to the Mutual Funds tab (4.7).
  - Mutual funds are no longer matched. The Mutual Funds options in both file-type selects, the `CUSTODY_MUTUAL_FUND` mapping choice and the INF + DIR split in `compare.js` (`splitMutualFunds`) are commented out; both upload routes refuse `MUTUAL_FUND` with `USE_MUTUAL_FUNDS_TAB`.
  - Equity tab — Transaction ID: each trade's own ID is the FA field `transactionId` ("Transaction ID" on the Field Mapping screen, optional in `FA_FIELDS` in `fieldMap.js`, not in the 26-column template). It is **not** TRANSREF (`ref`, stored in `Txn_Ref_No`). The consolidated file adds it as column 27 (`FA_TRANSACTION_ID_HEADER`, read with `FA_CONSOLIDATED_PROFILE`), and the import stores it in `Transaction.Transaction_ID` (`HEADER_MAP` in `TempTransactionUpload.js`). The column exists in Development only (created 2026-10-30); create `Transaction_ID` (varchar 255, **unique**, not mandatory) in Production before deploying. Because it is unique, `uploadEquityFaFile` refuses a file whose ID is already booked on another date (the bulk write would drop that row silently). `fieldMap.js` and `index.js` are copied into `functions/CustodyMatchRunner/custodyMatch/`.
  - Extra columns (**Add column** on Field Mapping): `EXTRA_COLUMN_ENTRIES` (`column1`–`column5`, `extra: true`) are in the `FA` and `FA_MUTUAL_FUND` field lists in `fieldMap.js` only (not `FA_OPI_OPO` or any custodian file); `checkExtraLabels` saves their names for any type whose list has them. Equity FA file: `tradesOfParsedFile` adds the values with `withExtraColumnValues`, the consolidated file has 32 columns (`tradesToTemplateCsv`: template, `Transaction ID`, `Column1`–`Column5`; `FA_CONSOLIDATED_PROFILE` finds them by heading, so an older 27-column file still reads), the import stores them in `Transaction.Column1`–`Column5`, and `readBookedTrades` reads them back with a second SELECT (the main one is at the 30-column limit). They are compared as `extras`, so changing one on re-upload makes the trade Changed. A file without a mapped extra column is not refused under the strict header check (`resolveFieldMap`); its values read as blank. The Resident and NDPMS custodian files offered extra columns for one day (2026-11-18) but never stored them, so they were removed; `RETIRED_FIELDS` drops `column1`–`column5` from a custodian mapping saved then, so it does not fall back to the default. `Transaction.Column1`–`Column5` exist in Development only; create them in Production before deploying (4.7).
  - Equity tab (`EquityReload.js`): a date's equity trades are `TRANDATE = <date>` minus mutual fund trades (ISIN `INF…` **and** exchange `DIR`; an `INF…` ETF on NSE is equity), minus every row committed from the Mutual Funds tab (`Security_Type = 'm'`, any exchange; `isBookedMutualFund`), and minus every `OPI` / `OPO` row (4.8); `bookedTradeTab` decides. An Equity file with an `OPI` or `OPO` row is refused (`OPI_OPO_ROW`). Its state is in Stratus `temporary-files/reload/<date>/` (`state.json` pending FA files and their mode, `consolidated.csv`, `affected.json`, `summary.json`, `progress.json`) and its lock is the `Jobs` row `RELOAD_<date>`. Pre-commit runs in AppSail step by step (Transaction → Holdings → Cash → stub masters → insert + count check → rebuild jobs) and saves the step before each one, so **Try again** continues from it. Pre-commit refuses while an old import (`TxnCsv_`/`HOLD_`/`CASH_`), another date's `RELOAD_<date>`, any Mutual Funds lock `RELOAD_MF_<date>` (4.7) or any OPI/OPO lock `RELOAD_OPI_<fileId>` (4.8) is `RUNNING`. Booked trades are written back to the 26 columns through `BOOKED_COLUMNS` in `equityReload.js`, the reverse of `HEADER_MAP` in `TempTransactionUpload.js`; change both together. With no pending FA file the match checks the books against the custodian files, and Pre-commit is refused because nothing would be loaded. A pre-committed FA file keeps its `Custody_Match_File` row so the FA card still lists it (pending means listed in `state.json`, not having a row); an FA upload decided as Replace retires the date's older equity FA files straight away (`retireFaFiles`, copies in `reload/<date>/replaced/`), while their trades stay in the books until the Pre-commit. Each trade date has its own change window (T+N): N working days after the trade date, that day included (Saturday and Sunday skipped, no holidays yet). The operator picks N on the Fund Accounting card next to the file date (`WINDOW_PRESETS` 1, 2, 3, 150, or Custom 1–150; default 150; `PUT /reload/:date/window`). It is kept in Stratus `temporary-files/reload/<date>/window.json` as `{ days, fixed }`; Pre-commit sets `fixed` at the date's first commit, and a fixed window refuses changes (409 `WINDOW_FIXED`). Only a committed date locks: a date is locked when its window is fixed and today (India) is past `lastChangeDate(date, days)`. A date never committed stays open, so a late file can still be booked; a date first committed after its window locks straight after that commit. A date with no `window.json` but trades in `Transaction` (booked before windows existed) counts as T+150, fixed. A failed read fails the request rather than guessing, which could open a locked date. The rules are `dateWindowView` / `closedDateMessage` in `util/custodyMatch/equityReload.js`; the screen holds no copy and asks `GET /reload/windows?dates=…` (and `GET /reload/:date` carries `window`); after that the server answers 409 `DATE_CLOSED` to uploads, matching, file removal (`deleteUploadedFile`) and Commit, and the screen shows a closed note, disables the drop zones, hides the remove buttons and blocks the date in the Match dialog. Only Try again of a commit that started in time stays open. No table holds the lock: it is worked out from the trade date, today in India and the date's window record (`closedDateMessage`). The Commit section shows **Locked** for a locked date instead of the Commit button. The screen calls Pre-commit **Commit**; code names and the route keep the old word. A date's first commit (no equity trades of the date booked, whatever the history says) starts at the insert step and skips the clean-up steps; the rebuild jobs still run and clear what they rebuild. `Transaction_Commit_History` exists in Development only (created 2026-11-02); create it in Production before deploying, or every commit is refused: `businessDate` (date, mandatory), `commitType` and `status` (varchar 20, mandatory), `runId` (varchar 100), `tradesBefore`, `tradesAfter`, `clients` (int), `fileNames` (text), `fileKey` (varchar 255), `startedAtMs` (bigint, mandatory), `finishedAtMs` (bigint).
  - Equity tab, custodian upload (`uploadEquityCustodyFile` → `uploadCustodyFile` with `req.equityReload`): a scheme and type can hold several files for one trade date, under any upload date. `compareCustodyRows` in `equityReload.js` compares the file's lines (trade number + client + ISIN + buy/sell) with the stored ones: all new is added, nothing new or changed is skipped, and anything else (including new numbers on rows that mostly look like stored ones) needs **Replace file**. Replace deletes that date's rows in `Custody_Match_Exception` and `Custody_Match_Run`, its `difference-*`, `final-*`, `fa-meta-*` and `run-*` files in Stratus and `reload/<date>/summary.json` (`clearMatchResults` in `EquityReload.js`), and is refused while a match of the date is running. Other dates and our books are not touched.
  - Old screen only: Commit sends every shown run (`POST /runs/proceed`) and they are imported as one file. `uploadTempTransaction` refuses while a `Jobs` row `TxnCsv_*`, `HOLD_*` or `CASH_*` is `RUNNING` (younger than 3 hours): the holdings and cash jobs read every trade loaded since their import started, so imports must not overlap. The `HOLD_<ms>` / `CASH_<ms>` names are built in `TempTransactionUpload.js` and again in `functions/CalculateHoldingMaster` and `functions/Cal_CB_Append_TxnUpload`; change all three together. A late-trade rebuild adds its own row, `HOLD_<ms>_R<chunk start>_<account>` (`CalculateHoldingWorkers`) or `CASH_<ms>_R_<account>` (`Cal_CB_Append_TxnUpload`), set `COMPLETED` by `RebuildHoldingtable` / `Cal_CB_Per_TNX` through their `importJobRowName` param; the guard reads the digits after the prefix, so keep the start time right after it.
  - A date committed before and opened again (stage detail `Import failed …` or `Re-opened …`, see `runStore.js`) can be committed again; `proceedToImport` then leaves out the file's rows still in `Transaction` (`dropTradesStillBooked`, key built from `faRowStoredValues` in `parseFA.js`). The key must follow how the import stores a row (`normalizeCsvForBulkUpload` in `TempTransactionUpload.js`).

### 4.7 Mutual Funds Transaction Upload
Upload → Field Mapping → Commit, re-uploaded and re-committed within each date's own change window, like the Equity tab but with no custodian file and no matching. The mutual fund TDS files have their own **TDS** tab (below). Business guide 3.7.

- **Screen:** the **Mutual Funds** tab of `react-app/src/pages/TempTransactionUpload/TempTransaction.js`. It renders the Equity tab's components with `kind = MUTUAL_FUND`: `custody/CustodyMatchPanel.js` (`reloadMode`, only the Fund Accounting card; the custodian card and Reconciliation are hidden; FA file type fixed to Mutual Funds, `FA_MUTUAL_FUND` mapping only), `custody/PreCommitPanel.js` (one Commit block per date with a mutual fund file on the card, `runId` `mf-<date>`), and `custody/CommitHistory.js`. The old `mutualFund/MfUploadPanel.js` is gone. The **TDS** tab (after Mutual Funds) is `custody/TdsUploadPanel.js`: a trade date, a drop zone that takes several files, and the date's file list with download and ×.
- **API:** the Equity routes `/api/custody-match/reload/*` (`fa-file`, `windows`, `:date`, `:date/window`, `:date/pre-commit`, `:date/retry`, `commit-history`, `consolidated-files`) with `kind=MUTUAL_FUND` in the body or query → `controller/custodyMatch/EquityReload.js`. `custody-file` and `run` are Equity only. TDS tab: `/api/custody-match/tds/*` → `controller/custodyMatch/TdsUpload.js`: `GET :date` (the date's files), `POST :date` (form-data `file` and `uploadDate`, one file per call; the upload date is checked like the other tabs' (`readUploadedOn`) and goes into the key), `DELETE :date?key=…`, `GET download?key=…` (registered before `:date`). The old `POST /reload/:date/tds-file` (`uploadMfTdsFile`) and the `tdsFiles` field of `GET /reload/:date` are removed. The old `/api/mf-upload` router, `MfTransactionUpload.js` and `util/mutualFund/mfStorage.js` are removed.
- **Own files:** `controller/custodyMatch/TdsUpload.js` and `react-app/src/pages/TempTransactionUpload/custody/TdsUploadPanel.js` (TDS tab only). The upload engine is shared with 4.6 and 4.8 (`KIND` in `EquityReload.js` holds the per-tab settings).
- **Shared files:** `controller/custodyMatch/EquityReload.js`, `util/custodyMatch/equityReload.js`, `util/custodyMatch/changeWindow.js`, `CustodyMatchUpload.js` (`deleteUploadedFile`), `util/custodyMatch/fieldMap.js` (the `FA_MUTUAL_FUND` mapping, with the optional `totalAmount`, `tds`, `stampDuty`, the extra fields `column1`–`column5` and their `labels`; and the `FA_MUTUAL_FUND_TDS` mapping of the TDS file; copied into `CustodyMatchRunner` and `CustodyMatchCleanup`), `parseFA.js`, `storage.js`, `runStore.js` (`Custody_Match_File`), `CustodyMatchFieldMap.js`, `controller/uploader/TempTransactionUpload.js` (`normalizeCsvForBulkUpload`), and the frontend `custody/api.js`, `CustodyMatchPanel.js`, `PreCommitPanel.js`, `CommitHistory.js`, `FieldMappingPanel.js` (**Add column**), `UploadErrorDetails.js`, `StatusBadge.js`.
- **Background functions:** the same as an Equity Commit: `UpdatesSecurity_ClientMasters`, and per affected client `RebuildHoldingtable` and `Cal_CB_Per_TNX`.
- **Tables:** writes `Transaction` (mutual fund rows only), including `TDSAMOUNT`, `STAMPDUTY`, `Column1`–`Column5`, `Security_Type = 'm'` and `Security_Type_Description = 'Mutual Funds'`; `Custody_Match_File` (FA files with `fileType = MUTUAL_FUND`); `Transaction_Commit_History` (`Asset_Kind = 'MUTUAL_FUND'`); `Jobs` (lock `RELOAD_MF_<date>`, rebuild rows `RELOAD_MF_<date>_H_n` / `_C_n`); `Holdings` and `Cash_Balance_Per_Transaction` (cleaned and rebuilt for affected clients). Stratus `temporary-files/reload-mf/<date>/`: `state.json`, `consolidated.csv` (35 columns), `affected.json` (with `keep`: the final list's ISINs and clients, for the stub-master step), `summary.json`, `progress.json`, `window.json`, `replaced/`. Raw files stay under `FA-Transaction/<date>/raw-MUTUAL_FUND-…`. The TDS files are in their own Stratus bucket `tds-files`, one folder per trade date: `<trade date>/<uploadedAtMs>_<uploadedOn>_<file name>` (created in Development 2026-11-18; create it in Production before deploying, or every TDS upload fails).
- **Before you change:**
  - A booked trade belongs to this tab when it is not `OPI` / `OPO` (`bookedTradeTab` checks that first; those rows are the OPI/OPO tab's, 4.8) and `isBookedMutualFund` is true (INF + DIR, or `Security_Type = 'm'` on any exchange); everything else is Equity's. Each tab reads, deletes and re-books only its own trades of the date (`rowIdsOfDate`, `readBookedTrades`).
  - There is no match. `GET /reload/:date?kind=MUTUAL_FUND` builds the summary live from books + pending files (`buildMutualFundList`), and Pre-commit builds and saves the final list itself (`mutualFundListNow`), instead of reading a match's `summary.json`.
  - Upload rules are Equity's: First / Add / Replace / Ask by Transaction ID (`decideMode`, `compareWithList`), an ID booked on another date is refused (`idsBookedOnOtherDates`). The compared values also include `tds`, `stampDuty`, `extras` (Column1–5) and `totalAmount`, carried by `withMutualFundValues`. `totalAmount` is the file's TOTAL AMOUNT, or `mutualFundAmount` when that cell is blank, or a booked row's `Net_Amount`, so an unchanged row stays Same. MF-only checks: `mutualFundTradeProblems` (types BY-, SQB, SL+ only; rate > 0; TOTAL AMOUNT > 0; TDS / Stamp Duty / TRANEXPENSE numbers ≥ 0) and `notMutualFundFileError` (the mapping must find at least one of TOTAL AMOUNT, TDS and Stamp Duty, `NOT_A_MUTUAL_FUND_FILE`). Equity's `MUTUAL_FUND_ROW` refusal is switched off for this tab (`fileProblems(…, { mutualFund: true })`).
  - The bulk-write file has 35 columns (`tradesToMutualFundCsv`): the 26 template columns, `TDS`, `Stamp Duty`, `Transaction ID`, `Column1`–`Column5`, `TOTAL AMOUNT`. `normalizeCsvForBulkUpload(…, { mutualFund: true })` sets `Net_Amount` = TOTAL AMOUNT when filled, otherwise works it out (`deriveMutualFundAmount` → `mutualFundAmount` in `equityReload.js`: buy = QTY × RATE + Stamp Duty, sell = QTY × RATE − STT); `NETRATE` = `Net_Amount` ÷ QTY; TDS does not change it. TOTAL AMOUNT has no Transaction column. It also adds `Security_Type = 'm'`.
  - TDS files (TDS tab): the operator picks the upload date in the tab bar (as on the Mutual Funds and OPI/OPO tabs), the trade date on the card, and one or more files. The batch's first file opens the Field Mapping window with `FA_MUTUAL_FUND_TDS` only (required: Broker acc id, Transaction type, Transaction date, AMT); once it is saved the rest are sent one by one with no window, and `uploadTdsFile` checks every file against the saved mapping (`TDS_COLUMNS_MISSING`). Closing the window without uploading drops the whole batch. Only the headings are read; no row is read and nothing in our books changes yet. Every upload is kept (the upload time in the key keeps same-named files apart); the operator can remove one with ×. The trade date may not be in the future. Listing confirms each object with `headObject`, because Stratus keeps listing a deleted object for a short while.
  - Rows with `Security_Type = 'm'` follow two rules elsewhere. The cash functions (`Cal_CB_Append_TxnUpload`, `Cal_CB_Per_TNX`, `CashCalculation`) use `Net_Amount` as it is, without ± STT. The holdings functions (`CalculateHoldingWorkers`, `RebuildHoldingtable`, `HoldingUpdateManually`, `CalculateHoldingPerAccount`, `CalculateHoldingsPerAccount`) give a buy (BY-, SQB) its SETDATE as TRANDATE when the row is read (`settleMutualFundBuy`), so the Holdings row is settlement-dated. Inception rows already marked `m` follow these rules too on a rebuild.
  - The change window is per tab: `readDateWindow` / `saveDateWindow` take `{ mutualFund }` and use `reload-mf/<date>/window.json`. With no file, a date counts as T+150 fixed only when it has booked trades of that tab. Removing a mutual fund file checks the Mutual Funds window and leaves the date's Equity match runs alone.
  - `busyElsewhere` refuses a Commit while any `RELOAD_<date>`, `RELOAD_MF_<date>` or `RELOAD_OPI_<fileId>` lock (another date or another tab) or an old import is running, because the tabs rebuild the same holdings and cash.
  - `Transaction` needs `Column1`–`Column5` (varchar 255) and `Transaction_Commit_History` needs `Asset_Kind` (varchar 20) in every environment where the Mutual Funds tab is used; both exist in Development only (Asset_Kind added 2026-11-16). Since 2026-11-18 the Equity tab reads and writes `Column1`–`Column5` too (4.6), so they must exist before either tab is deployed; Equity does not read `Asset_Kind`.

### 4.8 OPI/OPO Upload
Upload → Field Mapping → Commit for shares moved in (OPI) or out (OPO) from another system. No custodian file, no matching, no change window, no Transaction ID and no file date: one file may hold many trade and settlement dates, and one Commit adds the whole file. Cash is never rebuilt. Business guide 3.8.

- **Screen:** the **OPI/OPO** tab of `react-app/src/pages/TempTransactionUpload/TempTransaction.js` (after **Cash**). `custody/CustodyMatchPanel.js` with `kind = OPI_OPO` (only the Fund Accounting card, with no controls row: no file type dropdown (the file type is always `FA_FILE_TYPE.OPI_OPO`), no card date and no T+N dropdown; `FA_OPI_OPO` mapping only; a committed file has no ×), `custody/OpiOpoCommitPanel.js` (one Commit block per file on the card) and `custody/CommitHistory.js`.
- **API:** `/api/custody-match/opi-opo/*` → `controller/custodyMatch/OpiOpoUpload.js`: `POST file` (form-data `file`, `uploadDate`; reads and saves the file only, no database read), `GET :fileId`, `POST :fileId/commit`, `POST :fileId/retry`. History is `GET /reload/commit-history?kind=OPI_OPO` (`EquityReload.js`). The old `uploadFaFile` route refuses `fileType = OPI_OPO` (`USE_OPI_OPO_TAB`).
- **Own files:** `controller/custodyMatch/OpiOpoUpload.js`, `react-app/src/pages/TempTransactionUpload/custody/OpiOpoCommitPanel.js`.
- **Shared files:** `controller/custodyMatch/EquityReload.js` (exports `busyElsewhere`, `submitRebuildJobs`, `waitForBulkWrite`, `readJobStatuses`), `util/custodyMatch/equityReload.js` (`OPI_OPO_TYPES`, `isOpiOpoType`, `bookedTradeTab`, `fingerprint`, `fileProblems(…, { opiOpo: true })`, `tradesToOpiOpoCsv`, `affectedOf`), `util/custodyMatch/fieldMap.js` (its own `FA_OPI_OPO` profile: the `FA` fields without `transactionId`, so saving it never changes the Equity mapping; copied into `CustodyMatchRunner`), `CustodyMatchFieldMap.js` (the preview reports an Equity-looking file as `FA_OPI_OPO`; saving this mapping does not supersede Equity match runs), `parseFA.js` (a blank CashSetdate is allowed on OPI / OPO rows; copied into both functions), `normalize.js` (`normTranType`: OPO is an FA sell; copied into both functions), `constants.js` (`FA_FILE_TYPE.OPI_OPO`) and `storage.js` (`FA_FILE_TYPES`), both copied into `CustodyMatchRunner` and `CustodyMatchCleanup`, `CustodyMatchUpload.js` (`deleteUploadedFile`, `listMatchDates`), `runStore.js` (`Custody_Match_File`), `TempTransactionUpload.js` (`normalizeCsvForBulkUpload`), and the frontend `custody/api.js`, `CustodyMatchPanel.js`, `CommitHistory.js`, `FieldMappingPanel.js`.
- **Background functions:** `UpdatesSecurity_ClientMasters` (adds new account codes and ISINs as stub rows) → `EnrichTransactionSecurity`, then per affected client `RebuildHoldingtable` (pool `CorporateActions`, `importJobRowName` `RELOAD_OPI_<fileId>_H_<n>`). No `Cal_CB_Per_TNX`.
- **Tables:** writes `Transaction` (OPI and OPO rows, normal equity rows, `Transaction_ID` left empty); `Custody_Match_File` (the raw file, `fileType = OPI_OPO`, `forDate` = the upload date); `Transaction_Commit_History` (`Asset_Kind = 'OPI_OPO'`, `businessDate` = the file's first trade date, `runId` `opi-<fileId>`); `Jobs` (lock `RELOAD_OPI_<fileId>`, holdings rows `RELOAD_OPI_<fileId>_H_n`); `Holdings` (rebuilt for affected pairs); `Security_List` and `clientIds` (stub rows added). `Cash_Balance_Per_Transaction` is not touched. Stratus `temporary-files/FA/<upload date>/raw-OPI_OPO-<first>-<last>-<fileId>-<name>` (the raw file) and `temporary-files/opi-opo/<fileId>/`: `file.json` (rows, dates, affected pairs, fingerprints, `committed`), `rows.csv` (26 template columns: `tradesToOpiOpoCsv`), `progress.json`. The committed copy is `transactions-original/TxnUpload-<ms>-OpiOpo-<first date>.csv`.
- **Before you change:**
  - `fileId` is the upload time in ms, the same number as in the raw file key, so the screen takes it from the file listing (`uploadedAtMs`).
  - A booked trade belongs to this tab when `bookedTradeTab` returns `OPI_OPO`: `Tran_Type` is exactly `OPI` or `OPO` (any case), checked before the mutual fund rule, so mutual fund units moved in or out (INF on DIR) stay this tab's and a Mutual Funds re-commit never removes them. The Equity and Mutual Funds tabs never read, delete or re-book these rows, and an Equity file with an `OPI` / `OPO` row is refused (`OPI_OPO_ROW`).
  - Upload checks: every row `OPI` or `OPO` (`NOT_OPI_OPO_ROW`), readable trade and settlement dates (any number of dates), not a Mutual Funds file (TDS / Stamp Duty amounts), at least one row. Mutual fund rows (INF on DIR) are accepted (`fileProblems` skips `MUTUAL_FUND_ROW` with `opiOpo`); they are stored as ordinary rows, without `Security_Type = 'm'`. There is no Transaction ID (`fileProblems` skips `MISSING_TRANSACTION_ID`).
  - Duplicates are checked at Commit, not at upload, so the upload stays quick: a row's `fingerprint` is account + ISIN + OPI/OPO + trade date + settlement date + quantity + rate, and Commit answers 409 `ALREADY_BOOKED` when any of the file's rows matches a booked OPI/OPO row. Two waiting files with the same rows can both be uploaded; the second to be committed is refused. `bookedOpiOpoRows` reads only OPI/OPO rows (`Tran_Type IN ('OPI','OPO','opi','opo')`), 50 trade dates per query. Two identical rows inside one file are both kept.
  - A Commit only adds rows; it never deletes rows that were there before. Because uploads and Commit refuse already-booked rows, the rows of the file's dates matching its fingerprints are exactly what its own load wrote (`loadedRowIds`): an attempt deletes those first and checks their count afterwards, so a failed or half-finished bulk write is loaded again cleanly.
  - Once `progress.json` exists the file cannot be removed (`deleteUploadedFile` answers 409 `ALREADY_COMMITTED`), and Commit is refused for it again.
  - The files are filed under their upload date (`forDate` = `uploadedOn`), so `listMatchDates` leaves them out; the Equity card hides them by file type.
  - Holdings rules already in the holdings functions apply unchanged: OPI is a buy dated by its SETDATE, OPO is a sell dated by its TRANDATE and consumes FIFO lots, and a holding never goes below 0. The cash functions have no OPI / OPO type in their inflow or outflow lists.
  - `busyElsewhere` refuses a Commit on any tab while another Commit lock (`RELOAD_<date>`, `RELOAD_MF_<date>`, `RELOAD_OPI_<fileId>`) or an old import is running.
  - `Transaction_Commit_History.Asset_Kind` (varchar 20) must exist in every environment where this tab is used; it exists in Development only.

### 4.9 Transaction Deletion
Find committed trades by commit date, delete the wrong ones, and see what was deleted on each day. Business guide 3.9.

- **Screen:** `react-app/src/pages/TransactionDeletion/TransactionDeletionPage.js`, `TransactionDeletionPage.css`
- **API:** `/api/transaction` → `router/TransactionRouter.js` (`/commit-dates`, `/committed`, `POST /delete`, `/deletion-dates`, `/deleted`) → `controller/TransactionController.js`
- **Own files:** the screen files above.
- **Shared files:** `controller/TransactionController.js` and `router/TransactionRouter.js` also serve the Analytics drill-down. The controller imports `util/analytics/holdingsFromTable.js` for the drill-down only.
- **Background functions:** after a delete, for each affected client: `RebuildHoldingtable` (pool `CorporateActions`, limited to the ISINs that lost trades, with `keepCaRecords: "true"`) and `Cal_CB_Per_TNX` (pool `UpdateMasters`, whole passbook).
- **Shared files (added):** `util/custodyMatch/runStore.js` (`reopenRunsForDeletedTrades`): a delete re-opens the committed Transaction Upload runs whose file holds a deleted trade's date, so the corrected file can be committed again.
- **Tables:** deletes from `Transaction`. Writes `Transaction_Deletion_Log` (a copy of every deleted row; the history reads it by its `CREATEDTIME`) and the stage detail of re-opened `Custody_Match_Run` rows. The rebuild jobs rewrite `Holdings` and `Cash_Balance_Per_Transaction`.
- **Before you change:**
  - Corporate action tables (`Bonus`, `Split`, `Split_Account`, `Merger`, `Demerger_Record`, `Dividend_Record`) are deliberately not changed by a delete. `keepCaRecords` stops `RebuildHoldingtable` from rewriting `Split_Account`; the rebuild replays the rest as saved.
  - Dividend cash is the one exception: `Cal_CB_Per_TNX` reads `Transaction_Deletion_Log` and leaves out a dividend credit when a deleted buy of that ISIN is dated on or before the record date and no remaining buy is (`dropDividendsEarnedByDeletedBuys`). This applies to every later full rebuild too.
  - `Cash_Ledger` (Analytics cash summary) is not rebuilt, the same as after an upload commit.
  - One delete is capped at 5,000 rows (`DELETE_MAX_ROWS`).

### 4.10 Holdings calculation
Works out holdings from trades and corporate actions using FIFO. Business guide 3.10. There is no screen of its own. Other modules read the result.

- **Screen:** none (read through Analytics, Reports and the corporate action previews).
- **API:** `/api/holding-update` in `appsail-nodejs/index.js` starts a manual rebuild.
- **Own files:** the function folders below.
- **Shared files:** `util/analytics/holdingsFromTable.js` is the read side. See Section 1.
- **Background functions:**
  - `CalculateHoldingMaster` → `CalculateHoldingWorkers`: adds new trades after a commit.
  - `RebuildHoldingtable`: full rebuild after Split, Bonus, Merger and Demerger, and an ISIN-limited rebuild per client after a transaction deletion or for a commit with a late trade. The `keepCaRecords` param (sent by Transaction Deletion and by `CalculateHoldingWorkers` for a late trade) skips the `Split_Account` writer. `importJobRowName` (late trade only) is set `COMPLETED` in `Jobs` when it succeeds.
  - `HoldingUpdateManually`: manual full rebuild through `/api/holding-update`.
  - `CalculateHoldingPerAccount` (event) and `CalculateHoldingsPerAccount` (job): other entry points to the same FIFO engine.
- **Tables:** reads `Transaction`, `Bonus`, `Split`, `Merger` and `Demerger_Record`. Writes `Holdings`, `Split_Account`, `Jobs` and `JobStatusPerAccount`.
- **Before you change:**
  - The FIFO engine exists in several functions (Section 2), so change every copy.
  - Mutual fund buys (`Security_Type = 'm'`, BY- / SQB) are settlement-dated: each function that reads `Transaction` replaces TRANDATE with SETDATE as the row is read (`settleMutualFundBuy`). Keep it in every copy (4.7).
  - The same-day order (trades, split, bonus, demerger, merger) and the effective-date rule must stay the same in all of them and in `holdingsFromTable.js`.
  - Rounding rule: every quantity is rounded to 6 decimals and every amount (cost, WAP, P/L) to 4 decimals as it is calculated (`roundQty` / `roundAmt`). This stops binary leftovers such as 190.88 − 190 = 0.8799999999999955. The rule is in the FIFO engine and its copies, `FifoQty.js`, `CalculateHoldingWorkers` and `holdingsFromTable.js` and its ports. Keep it in any new copy. Screens round to 2 decimals and never cut digits off.
  - Holdings screens and exports (Analytics account and ISIN views, Export All, `FAHoldingsGenerator`) show the saved `HOLDING`, `WAP` and `HOLDING_VALUE` of the last saved row up to the as-on date. They do not re-derive them, because the `PRICE` column keeps only 4 decimals and a re-derived value can be a few paise off. Corporate action previews (Split, Bonus, Dividend) still use `recomputeHoldingsSnapshot` for the quantity. Old rows saved before the trade-date change need a holdings rebuild to be right.
  - The rule also covers the differential report FIFO (`functions/ExportDifferentialReport/fifo.js`), the unused `util/export/fifo.js`, and the consolidated totals that add a client's schemes together (`util/analytics/consolidatedHoldings.js`, `ExportAllCustomerHoldingData/index.js`, `FAHoldingsGenerator/index.js`). Not yet covered: the merger and demerger lot code.

### 4.11 Cash Balance
Client cash passbook. Business guide 3.11.

- **Screen:** `react-app/src/pages/CashBalancePage/CashBalancePage.js`
- **API:** `/api/cash-balance` → `router/cashBalanceRouter/CashbalanceRouter.js` → `controller/cashBalance/cashPassbookController.js`, `exportCashBalance.js`, `calculateBalanceOnce.js`
- **Own files:** `CashBalancePage.js`, `cashPassbookController.js`, `calculateBalanceOnce.js`
- **Shared files:** `exportCashBalance.js` (Reports), `util/allAccountCodes.js`, `util/stratusSignedUrl.js`, `hooks/GetAllCodes.js`
- **Background functions:**
  - `Cal_CB_Append_TxnUpload`: adds passbook lines after a commit.
  - `Cal_CB_Per_TNX`: full rebuild from `Transaction` plus RECEIVED `Dividend_Record` rows (from 2020 onwards, no end date), leaving out dividends earned only through a deleted buy (`Transaction_Deletion_Log`). Run manually, per client by Transaction Deletion, and per client by `Cal_CB_Append_TxnUpload` for a late trade (`importJobRowName` is set `COMPLETED` in `Jobs` when it succeeds).
  - `ExportCashBalSingleClient` (pool `Export`).
  - `CashCalculation` (pool `Finance`) fills `Cash_Ledger`: it rebuilds each account from all its trades on every run (no "ROWID greater than last done" increment, because ROWIDs are not in time order).
  - `UpdateDividendData` (Dividend module) adds dividend credits.
- **Tables:** reads `Cash_Balance_Per_Transaction`. The functions above write it, and `CashCalculation` writes `Cash_Ledger`. `Cal_CB_Append_TxnUpload` also marks its import's `CASH_<ms>` row in `Jobs` completed.
- **Before you change:**
  - `Cal_CB_Append_TxnUpload` and `Cal_CB_Per_TNX` are twins and must follow the same money-in/money-out rules.
  - Mutual fund rows (`Security_Type = 'm'`) move cash by `Net_Amount` exactly; STT is not added or taken off again (`isMutualFundRow`, also in `CashCalculation`). See 4.7.
  - Dividend credits land in the same table. `Cal_CB_Per_TNX` deletes them with everything else and re-adds them from `Dividend_Record`, so a change to how `UpdateDividendData` writes a credit must be mirrored in `fetchReceivedDividendEvents`.
  - The Analytics cash summary reads `Cash_Ledger`, not the passbook.

### 4.12 Analytics
Read-only view of one client or one share. Business guide 3.12.

- **Screen:** `react-app/src/pages/Analytics/AnalyticsPage.js`, `tabs/holding/HoldingTab.js`, `tabs/transaction/TransactionTab.js`, `tabs/allocations/AllocationTab.js` and `tabs/performance/PerformanceTab.js` (placeholders), `pages/Holding/HoldingCards.js`, `pages/TransactionPage/TransactionPage.js` (drill-down), `hooks/GetHolding.js`
- **API:** `/api/analytics` → `router/AnalyticsRouter.js` → `controller/analytics/tabs/holding/AnalyticsControllers.js`, `CashController.js`, `controller/analytics/tabs/transaction/transaction.js`. The drill-down uses `/api/transaction/getStockTransactionHistory`.
- **Own files:** the screen files, `CashController.js`, `util/analytics/bhavPrices.js`
- **Shared files:** `AnalyticsControllers.js` and `transaction.js` (Reports), `controller/TransactionController.js` (Transaction Deletion), `util/analytics/holdingsFromTable.js`, `util/mapVirtualToActualCodes.js`, `util/allAccountCodes.js`, `hooks/GetAllCodes.js`
- **Background functions:** none.
- **Tables:** reads `Holdings`, `Transaction`, `Bhav_Copy`, `Security_List`, `clientIds` and `Cash_Ledger`.
- **Before you change:** the holdings and transaction controllers also produce the Reports exports, and `functions/ExportAllCustomerHoldingData/analyticsController.js` mirrors the holdings summary.

### 4.13 Custodian Upload and the two-stage custodian check
Ex-Entitlement and Benefit Collection files, and the Receivable → Received check. Business guide 3.13–3.14.

- **Screen:** `react-app/src/pages/CustodianUploadPage/CustodianUploadPage.js`, plus `pages/SplitPage/SplitReconcilePanel.js` (the check panel on Split, Bonus and Dividend)
- **API:** `/api/custodian-upload` → `router/CustodianUploadRouter.js` → `controller/corporateAction/CustodianUploadController.js`. The Split and Bonus routers also use this controller for `compare-custodian`, `compare-benefit` and `history`. The Dividend router uses its `compareDividendWithBenefit`.
- **Own files:** `CustodianUploadPage.js`, `util/corporateAction/compareCustodian.js`, `util/corporateAction/custodianTables.js`, `util/custodian/parseCustodianCsv.js`
- **Shared files:** `CustodianUploadController.js` and `SplitReconcilePanel.js` (Split, Bonus, Dividend), `util/custodyMatch/fieldMap.js` and `normalize.js` (Transaction Upload), `util/holdingComparison/parse.js` and `storage.js` (Holding Comparison), the preview calculations in `util/analytics/` (`splitPreviewCalc.js`, `bonusPreviewCalc.js`, `bonusAccountRows.js`, `dividendAccountRows.js`)
- **Background functions:** none. Files are imported with a Catalyst bulk write job.
- **Tables:** writes `CA_Custodian_Upload`, `CA_Ex_Entitlement` and `CA_Benefit_Collection`. Reads `Holdings`, `Split_Account`, `Bonus_Record`, `Dividend`, `Dividend_Record`, `Security_List` and `clientIds`.
- **Before you change:** any change to the comparison labels or columns shows up on three pages (Split, Bonus, Dividend) at once.

### 4.14 Split
Business guide 3.16.

- **Screen:** `react-app/src/pages/SplitPage/SplitPage.js` (+ `SplitReconcilePanel.js`)
- **API:** `/api/split` → `router/SplitRouter.js` → `controller/uploader/SplitController.js`, `controller/export/exportSplit/exportSplitFile.js`, `CustodianUploadController.js`
- **Own files:** `SplitPage.js`, `SplitController.js`, `exportSplitFile.js`
- **Shared files:** `util/analytics/splitPreviewCalc.js`, `holdingsFromTable.js`, `mapVirtualToActualCodes.js`, `caJobProgress.js`, `CustodianUploadController.js`, `SplitReconcilePanel.js`, `PreviousFiles.js`. The Demerger page also calls `/api/split/getAllSecuritiesList`.
- **Background functions:** `RebuildHoldingtable` (pool `CorporateActions`). `UpdateBonusTable` recalculates any bonus whose record date is after the split.
- **Tables:** writes `Split` and `Split_Account`. Reads `Holdings`, `Security_List` and `clientIds`.
- **Before you change:** a split can trigger a bonus recalculation, so re-test Bonus too.

### 4.15 Bonus
Business guide 3.17.

- **Screen:** `react-app/src/pages/BonusPage/BonusPage.js` (+ `SplitReconcilePanel.js`)
- **API:** `/api/bonus` → `router/BonusRouter.js` → `controller/BonusController.js`, `controller/export/exportBonus/exportBonusFile.js`, `CustodianUploadController.js`
- **Own files:** `BonusPage.js`, `BonusController.js`, `exportBonusFile.js`
- **Shared files:** `util/analytics/bonusPreviewCalc.js`, `bonusAccountRows.js`, `holdingsFromTable.js`, `mapVirtualToActualCodes.js`, `caJobProgress.js`, `CustodianUploadController.js`, `SplitReconcilePanel.js`, `PreviousFiles.js`
- **Background functions:** `UpdateBonusTable` (pool `CorporateActions`), then `RebuildHoldingtable`.
- **Tables:** writes `Bonus` and `Bonus_Record`. Reads `Holdings`, `Security_List` and `clientIds`.
- **Before you change:** `functions/UpdateBonusTable/holdingsSnapshot.js` must give the same holding as `holdingsFromTable.js`. If they differ, the applied bonus will not match the approved preview.

### 4.16 Dividend
Business guide 3.18.

- **Screen:** `react-app/src/pages/Dividend/DividendPage.js` (+ `SplitReconcilePanel.js`)
- **API:** `/api/dividend` → `router/uploaderRouter/DividendUploaderRouter.js` → `controller/uploader/DividendUploader.js`, `controller/export/exportDividend/exportDividendFile.js`, `CustodianUploadController.js`
- **Own files:** `DividendPage.js`, `DividendUploader.js`, `exportDividendFile.js`
- **Shared files:** `util/analytics/dividendAccountRows.js`, `holdingsFromTable.js`, `mapVirtualToActualCodes.js`, `CustodianUploadController.js`, `SplitReconcilePanel.js`
- **Background functions:** `UpdateDividendData` (pool `CorporateActions`) and `ExportDividendAccounts` (pool `Export`).
- **Tables:** writes `Dividend`, `Dividend_Record` and `Cash_Balance_Per_Transaction`. Reads `Holdings`.
- **Before you change:** a dividend writes into the cash passbook, so re-test Cash Balance. The `holdingsSnapshot.js` ports must match `holdingsFromTable.js`.

### 4.17 Merger
Business guide 3.19.

- **Screen:** `react-app/src/pages/MergerPage/MergerPage.js`
- **API:** `/api/merger` → `router/MergerRouter.js` → `controller/MergerController.js`
- **Own files:** `MergerPage.js`, `MergerController.js`, `util/merger/lotFifo.js`, `util/merger/mergerEngine.js`
- **Shared files:** `util/caJobProgress.js`
- **Background functions:** `MegerFn` (pool `CorporateActions`), then `RebuildHoldingtable`.
- **Tables:** writes `Merger`, `Merger_Record` and `Security_List` (new ISIN). Reads `Transaction`, `Bonus`, `Split` and `Demerger_Record`.
- **Before you change:** the preview (`util/merger/*`) and the apply job (`functions/MegerFn/*`) are twins. Per-lot rounding must be the same in both, or the applied result will not match the preview.

### 4.18 Demerger
Business guide 3.20.

- **Screen:** `react-app/src/pages/DemergerPage/DemergerPage.js`
- **API:** `/api/demerger` → `router/DemergerRouter.js` → `controller/uploader/DemergerController.js`. The ISIN list comes from `/api/split/getAllSecuritiesList`.
- **Own files:** `DemergerPage.js`, `DemergerController.js`
- **Shared files:** `util/caJobProgress.js`, and the Split router's securities list.
- **Background functions:** `DemergerFn` (pool `CorporateActions`), then `RebuildHoldingtable`.
- **Tables:** writes `Demerger`, `Demerger_Record` and `Security_List` (new ISIN). Reads `Holdings`.
- **Before you change:** `DemergerController.js` (preview) and `functions/DemergerFn/demergerApplyCore.js` (apply) are twins.

### 4.19 Holding Comparison
Our holdings vs the custodian's holding statement. Business guide 3.21.

- **Screen:** `react-app/src/pages/HoldingComparison/HoldingComparison.js` and the `holdingComparison/` folder (`HoldingComparisonPanel.js`, `ExceptionGrid.js`, `PreviousFiles.js`, `api.js`)
- **API:** `/api/holding-comparison` → `router/holdingComparisonRouter/HoldingComparisonRouter.js` → `controller/holdingComparison/HoldingComparisonUpload.js`, `HoldingComparisonFaHoldings.js`, `HoldingComparisonRun.js`
- **Own files:** `HoldingComparison.js`, `HoldingComparisonPanel.js`, `ExceptionGrid.js`, the three controllers, `util/holdingComparison/compare.js`, `util/holdingComparison/index.js`
- **Shared files:** `util/holdingComparison/parse.js`, `storage.js`, `constants.js`, `runStore.js` (Custodian Upload, Transaction Upload field mapping, Notifications), `PreviousFiles.js` and `api.js` (Split, Bonus), the frontend `custody/FieldMappingPanel.js` and `custody/api.js`
- **Background functions:** `FAHoldingsGenerator` builds the FA snapshot. `HoldingComparisonRunner` runs the comparison.
- **Tables:** writes `Holding_Comparison_Run` and `Jobs`. The functions read `Holdings`, `Bhav_Copy`, `Security_List` and `clientIds`.
- **Before you change:** `util/holdingComparison/*` is copied into both functions. `FAHoldingsGenerator` also carries copies of `holdingsFromTable.js` and `allAccountCodes.js`.

### 4.20 Update ISIN
Rename an ISIN everywhere, or update security details. Business guide 3.22.

- **Screen:** `react-app/src/pages/update-isin/UpdateISINPage.js`
- **API:** `/api/isin` → `router/IsinRouter.js` → `controller/isin/updateIsin.js`
- **Own files:** the three files above.
- **Shared files:** none.
- **Background functions:** `UpdateISIN` (pool `UpdateMasters`), then `UpdateISINWorker`, one piece per table.
- **Tables:** renames the ISIN in `Transaction`, `Holdings`, `Cash_Balance_Per_Transaction`, `Bhav_Copy`, `Security_List`, every corporate action table and their `_Record` tables, `Temp_Transaction` and `Temp_Custodian`.
- **Before you change:**
  - A **new table with an ISIN column** must be added to `ISIN_TARGETS` in `functions/UpdateISINWorker/index.js`, or renamed ISINs will be left behind in it.
  - The target count in `updateIsin.js` must match `ISIN_TARGETS.length`.

### 4.21 Reports
CSV exports. Business guide 3.23.

- **Screen:** `react-app/src/pages/Reports/ReportsPage.js` and `tabs/` (`Holdings.js`, `Transactionstab.js`, `CashTab.js`, `CorporateActionTab.js`, `TopClientsTab.js`), `notifications/reportJobStatus.js`
- **API:** `/api/export` → `router/export/ExportRouter.js` → `controller/export/exportHolding/*`, `exportTransaction/exportSingleClient.js`, `exportCashBalance/exportAllClientsCash.js`, `exportCorporateAction/*`. The Cash tab also uses `/api/cash-balance/export*`.
- **Own files:** the screen files, the export controllers above, `util/analytics/consolidatedHoldings.js`, `util/reportTimestamp.js`
- **Shared files:** `AnalyticsControllers.js` and `transaction.js` (Analytics), `exportCashBalance.js` (Cash Balance), `holdingsFromTable.js`, `mapVirtualToActualCodes.js`, `allAccountCodes.js`, `stratusSignedUrl.js`, `hooks/GetAllCodes.js`
- **Background functions:** `ExportAllCustomerHoldingData`, `ExportCashBalance`, `ExportCashBalSingleClient` and `ExportCorpActionReport` (all pool `Export`).
- **Tables:** reads `Holdings`, `Transaction`, `Cash_Balance_Per_Transaction`, every corporate action table, `Bhav_Copy`, `Security_List` and `clientIds`.
- **Before you change:**
  - Holdings and transaction reports reuse the Analytics controllers.
  - A new corporate action type needs a handler in `functions/ExportCorpActionReport/handlers/` and an entry in `SUPPORTED_TYPES`.

### 4.22 Dashboard and Notifications
Business guide 3.24.

- **Screen:** `react-app/src/pages/Dashboard/DashboardPage.js`, `components/dashboard/DashboardComponents.js`, `notifications/NotificationTray.js`, `notifications/JobNotificationContext.js`
- **API:**
  - `/api/dashboard` → `router/DashboardRouter.js` → `controller/DashboardController.js`
  - `/api/notifications` → `router/NotificationRouter.js` → `controller/NotificationController.js`
- **Own files:** the screen files, both controllers, `util/notifications.js`
- **Shared files:** `util/caJobProgress.js`, `util/custodyMatch/runStore.js`, `util/holdingComparison/runStore.js`
- **Background functions:** none. The bell follows the jobs that other modules start.
- **Tables:** Dashboard reads `Transaction`. Notifications reads and writes `Notifications`, and reads `Jobs`, `JobStatusPerAccount`, `Custody_Match_Run` and `Holding_Comparison_Run`.
- **Before you change:** the bell reads the job and run tables of other modules. A change to their status values must be reflected in `util/notifications.js`.
