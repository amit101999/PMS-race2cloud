/**
 * Holding Comparison — shared constants.
 *
 * This folder is self-contained (only csv-parser and the Catalyst app passed in)
 * so it can be copied as-is into functions/HoldingComparisonRunner and
 * functions/FAHoldingsGenerator. Do not import from ../custodyMatch here.
 */

export const RUN_STATUS = {
  QUEUED: "QUEUED",
  RUNNING: "RUNNING",
  COMPLETED_CLEAN: "COMPLETED_CLEAN",
  COMPLETED_WITH_EXCEPTIONS: "COMPLETED_WITH_EXCEPTIONS",
  FAILED: "FAILED",
  SUPERSEDED: "SUPERSEDED",
};

export const STAGE = {
  READ_CUSTODY: "S1",
  READ_FA: "S2",
  COMPARE: "S3",
  REPORT: "S4",
  DONE: "S5",
};

export const EXCEPTION_TYPE = {
  QUANTITY_MISMATCH: "QUANTITY_MISMATCH",
  MISSING_IN_CUSTODY: "MISSING_IN_CUSTODY",
  MISSING_IN_FA: "MISSING_IN_FA",
};

/** Status values of the HCFA_<date> row in the Jobs table (FA holdings generation). */
export const FA_STATUS = {
  NOT_STARTED: "NOT_STARTED",
  PENDING: "PENDING",
  RUNNING: "RUNNING",
  COMPLETED: "COMPLETED",
  FAILED: "FAILED",
};

export const JOB_POOL = "HoldingComparison";
export const RUNNER_TARGET = "HoldingComparisonRunner";
export const FA_GENERATOR_TARGET = "FAHoldingsGenerator";

/** Jobs-table name that tracks FA holdings generation for a date. */
export const faJobName = (date) => `HCFA_${date}`;

/**
 * The standard 29-column Custodian Consolidated holdings template, in order.
 *
 * `field` is the name a saved Field Mapping uses; `header` is the column name
 * in the standard file. Only accountCode, isin and quantity are compared;
 * asOfDate and securityName are carried into the difference report.
 *
 * Keep in step with CUSTODY_HOLDINGS_COLUMNS in util/custodyMatch/fieldMap.js,
 * which drives the mapping popup (that folder is vendored separately, so it
 * cannot import this file).
 */
export const CUSTODY_COLUMNS = [
  { field: "srNo", header: "SrNo" },
  { field: "accountCode", header: "UCC" },
  { field: "clientName", header: "ClientName" },
  { field: "clientId", header: "ClientId" },
  { field: "parentName", header: "Parent Name" },
  { field: "fundStrategyCode", header: "FundStrategyCode" },
  { field: "securityName", header: "SecurityName" },
  { field: "isin", header: "ISIN" },
  { field: "isinSubType", header: "ISINSubType" },
  { field: "investorType", header: "InvestorType" },
  { field: "purchaseOutstanding", header: "PurchaseOutstanding" },
  { field: "saleOutstanding", header: "SaleOutstanding" },
  { field: "purAct", header: "PurAct" },
  { field: "saleAct", header: "SaleAct" },
  { field: "purchaseUnderProcess", header: "PurchaseUnderProcess" },
  { field: "saleUnderProcess", header: "SaleUnderProcess" },
  { field: "pendingDemat", header: "PendingDemat" },
  { field: "pendingReg", header: "PendingReg" },
  { field: "physicalFree", header: "PhysicalFree" },
  { field: "physicalLocked", header: "PhysicalLocked" },
  { field: "dematFree", header: "DematFree" },
  { field: "dematLockedQty", header: "DematLockedQty" },
  { field: "dematPledge", header: "DematPledge" },
  { field: "dematFreezQty", header: "DematFreezQty" },
  { field: "saleStockQty", header: "SaleStockQty" },
  { field: "quantity", header: "NetBalance" },
  { field: "ltpExchange", header: "LTPExchange" },
  { field: "asOfDate", header: "LTPDate" },
  { field: "ltpPrice", header: "LTPPrice" },
];

export const CUSTODY_TEMPLATE_HEADERS = CUSTODY_COLUMNS.map((c) => c.header);

/**
 * The only columns a custodian file must have: the comparison key (account code
 * + ISIN), the compared quantity, and the as-of date shown in the report. The
 * other template columns are read when present but never required.
 */
export const CUSTODY_REQUIRED_FIELDS = ["accountCode", "isin", "quantity", "asOfDate"];

/**
 * The mapping profile type, stored by the existing Field Mapping endpoints at
 * field-maps/CUSTODY_HOLDINGS.json.
 */
export const CUSTODY_PROFILE_TYPE = "CUSTODY_HOLDINGS";

export const FA_HEADERS = [
  "GENERATED_AT",
  "AS_ON_DATE",
  "ACCOUNT_CODE",
  "SECURITY_NAME",
  "SECURITY_CODE",
  "ISIN",
  "HOLDING",
];

export const DIFFERENCE_HEADERS = [
  "ACCOUNT_CODE",
  "ISIN",
  "SECURITY_NAME",
  "AS_OF_DATE",
  "FA_HOLDING_QTY",
  "CUSTODY_QTY",
  "DIFFERENCE",
  "EXCEPTION_TYPE",
];

export const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;
