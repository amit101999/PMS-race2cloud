/**
 * Custody Matching — user-defined field mapping.
 *
 * Until now every column name lived in code: FA_HEADERS (26 exact strings, exact
 * order), CUSTODY_F1_HEADERS, CUSTODY_F2_HEADERS. A custodian renaming QTY to
 * Quantity meant a code change. This module moves that decision to the user.
 *
 * The CANONICAL FIELDS are still fixed — they are what the match key is built
 * from, so they cannot be user-defined without the comparison losing meaning.
 * What becomes dynamic is only which column in *their* file feeds each one.
 *
 * Two details drive the shape of a mapping entry, and neither is incidental:
 *
 *   1. Sources address a column by NAME or by INDEX. The FA template has three
 *      columns literally named "filler" and the second one is Net_Amount, so a
 *      name-only model cannot express it.
 *   2. A source may carry a TRANSFORM. NDPMS files have no date column at all —
 *      the trade date is the first 8 characters of ContractNo.
 *
 * DEFAULT_PROFILES reproduce today's hardcoded behaviour exactly, so nothing
 * changes until somebody edits a profile.
 */

import { CUSTODY_FORMAT, FA_HEADERS, SIDE } from "./constants.js";
import {
  detectSeparator,
  firstLineOf,
  normIsin,
  normMutualFundTranType,
  normTranType,
  num,
  parseContractNoDate,
  parseDate,
  parseIsoDate,
  splitHeaderCells,
  text,
} from "./normalize.js";

/** Which file a profile describes. One profile per type, shared across schemes. */
export const PROFILE_TYPE = {
  FA: "FA",
  FA_MUTUAL_FUND: "FA_MUTUAL_FUND",
  /*
   * The OPI/OPO tab's file: the Equity layout without a Transaction ID, so it
   * has its own mapping. Saving it never changes the Equity tab's mapping.
   */
  FA_OPI_OPO: "FA_OPI_OPO",
  /*
   * The OPI/OPO tab's Mutual Funds sub-tab file: the OPI/OPO layout plus TDS and
   * Stamp Duty on the end. Its own mapping, like every other file.
   */
  FA_OPI_OPO_MF: "FA_OPI_OPO_MF",
  /*
   * The TDS tab's mutual fund TDS file. For now it is only mapped and stored; no
   * row of it is read into our books (TdsUpload.js).
   */
  FA_MUTUAL_FUND_TDS: "FA_MUTUAL_FUND_TDS",
  CUSTODY_RESIDENTIAL: "CUSTODY_RESIDENTIAL",
  CUSTODY_NDPMS: "CUSTODY_NDPMS",
  CUSTODY_MUTUAL_FUND: "CUSTODY_MUTUAL_FUND",
  /*
   * The custodian consolidated holdings file used by Holding Comparison. It lives
   * here only so that module can reuse this mapping screen; custody matching
   * never reads it. See util/holdingComparison/parse.js.
   */
  CUSTODY_HOLDINGS: "CUSTODY_HOLDINGS",
  /*
   * The custodian corporate actions file uploaded on Corporate Actions ->
   * Custodian Upload. Like CUSTODY_HOLDINGS it is here only to reuse the
   * mapping screen; custody matching never reads it.
   */
  CUSTODY_CORP_ACTION: "CUSTODY_CORP_ACTION",
  /*
   * The custodian "Corporate Benefit Collection" file, uploaded next to the
   * corporate actions file on Custodian Upload. Here only for the mapping
   * screen; custody matching never reads it.
   */
  CUSTODY_CORP_ALLOCATION: "CUSTODY_CORP_ALLOCATION",
};

export const PROFILE_TYPES = Object.values(PROFILE_TYPE);

/** The transaction-file (fund accounting) profiles, as opposed to the custodian ones. */
export const isFaProfileType = (type) =>
  type === PROFILE_TYPE.FA ||
  type === PROFILE_TYPE.FA_MUTUAL_FUND ||
  type === PROFILE_TYPE.FA_OPI_OPO ||
  type === PROFILE_TYPE.FA_OPI_OPO_MF;

/** Stored blobs carry this so a later shape change can migrate rather than misread. */
export const PROFILE_VERSION = 1;

/** How a mapping entry addresses a column in the user's file. */
export const SOURCE_BY = {
  NAME: "name",
  INDEX: "index",
  /**
   * "This field has no column in my file."
   *
   * Stored rather than simply omitted. Every field has to be answered before a
   * mapping can be saved, and an omitted field is indistinguishable from one
   * the user has not reached yet — persisting the absence is what stops the
   * next upload asking again about a column the file has never carried.
   */
  ABSENT: "absent",
};

/** Value-level rewrites applied after a column is located. */
export const TRANSFORM = {
  NONE: "NONE",
  /** First 8 characters, read as a compact date. NDPMS ContractNo -> trade date. */
  DATE_FROM_FIRST_8: "DATE_FROM_FIRST_8",
};

export const TRANSFORMS = Object.values(TRANSFORM);

/** Errors this module reports. Kept distinct from FILE_ERROR — these are config faults, not file faults. */
export const MAP_ERROR = {
  UNKNOWN_TYPE: "UNKNOWN_TYPE",
  UNKNOWN_FIELD: "UNKNOWN_FIELD",
  MISSING_REQUIRED: "MISSING_REQUIRED",
  BAD_SOURCE: "BAD_SOURCE",
  BAD_TRANSFORM: "BAD_TRANSFORM",
  TRANSFORM_NOT_ALLOWED: "TRANSFORM_NOT_ALLOWED",
  COLUMN_NOT_IN_FILE: "COLUMN_NOT_IN_FILE",
  INDEX_OUT_OF_RANGE: "INDEX_OUT_OF_RANGE",
};

/**
 * The Custodian Consolidated holdings template: [field, standard header, kind].
 *
 * All 29 columns are shown in the popup, but only UCC, ISIN, NetBalance and
 * LTPDate must be mapped — those are what the comparison uses. UCC, ISIN and
 * NetBalance carry a strict kind; the rest accept any value. Keep in step with CUSTODY_COLUMNS in
 * util/holdingComparison/constants.js (this folder is vendored into
 * CustodyMatchRunner, so it cannot import that file).
 */
const CUSTODY_HOLDINGS_COLUMNS = [
  ["srNo", "SrNo", "text"],
  ["accountCode", "UCC", "code"],
  ["clientName", "ClientName", "text"],
  ["clientId", "ClientId", "text"],
  ["parentName", "Parent Name", "text"],
  ["fundStrategyCode", "FundStrategyCode", "text"],
  ["securityName", "SecurityName", "text"],
  ["isin", "ISIN", "isin"],
  ["isinSubType", "ISINSubType", "text"],
  ["investorType", "InvestorType", "text"],
  ["purchaseOutstanding", "PurchaseOutstanding", "text"],
  ["saleOutstanding", "SaleOutstanding", "text"],
  ["purAct", "PurAct", "text"],
  ["saleAct", "SaleAct", "text"],
  ["purchaseUnderProcess", "PurchaseUnderProcess", "text"],
  ["saleUnderProcess", "SaleUnderProcess", "text"],
  ["pendingDemat", "PendingDemat", "text"],
  ["pendingReg", "PendingReg", "text"],
  ["physicalFree", "PhysicalFree", "text"],
  ["physicalLocked", "PhysicalLocked", "text"],
  ["dematFree", "DematFree", "text"],
  ["dematLockedQty", "DematLockedQty", "text"],
  ["dematPledge", "DematPledge", "text"],
  ["dematFreezQty", "DematFreezQty", "text"],
  ["saleStockQty", "SaleStockQty", "text"],
  ["quantity", "NetBalance", "number"],
  ["ltpExchange", "LTPExchange", "text"],
  ["asOfDate", "LTPDate", "text"],
  ["ltpPrice", "LTPPrice", "text"],
];

const CUSTODY_HOLDINGS_KEY_FIELDS = ["accountCode", "isin", "quantity"];

/** The columns the comparison needs; the other template columns may be left unmapped. */
const CUSTODY_HOLDINGS_REQUIRED_FIELDS = [...CUSTODY_HOLDINGS_KEY_FIELDS, "asOfDate"];

/**
 * The custodian corporate actions template: [field, standard header].
 *
 * Every field is read as text. The Split / Bonus pages compare the file against
 * their preview using CORP_ACTION_REQUIRED_FIELDS, so those must be mapped (and
 * cannot be marked absent); every other column stays optional.
 */
const CORP_ACTION_COLUMNS = [
  ["parentClientName", "Parent Client Name"],
  ["clientUccCode", "Client UCC Code"],
  ["clientName", "Client Name"],
  ["dpName", "DP Name"],
  ["dpId", "DP ID"],
  ["depositoryName", "DepositoryName"],
  ["uccStatus", "UCC Status"],
  ["nsdlClientId", "NSDL Client ID"],
  ["isin", "ISIN"],
  ["isinName", "ISIN Name"],
  ["caRefNo", "CA REF NO"],
  ["exDate", "EX Date"],
  ["bookClosureFromDate", "Book Closure FM DT"],
  ["bookClosureToDate", "Book Closure TO DT"],
  ["recordDate", "Record Date"],
  ["tentativePaymentDate", "Tentative Payment Date"],
  ["caType", "CA Type"],
  ["caTypeDescr", "CA Type Descr"],
  ["faceValue", "Face Value"],
  ["ratio", "Ratio (Propose : Held)"],
  ["dividendRate", "Dividend Rate"],
  ["holdingQty", "Holding Qty"],
  ["caQty", "CA Qty"],
  ["fractionQty", "Fraction Qty"],
  ["caAmt", "CA Amt"],
  ["newIsin", "New ISIN"],
  ["newIsinName", "New ISIN Name"],
  ["newIsinFaceValue", "New ISIN FaceValue"],
  ["remarks", "Remarks"],
  ["bankAccountType", "Bank Account Type"],
  ["bankAcNo", "Bank AC NO"],
  ["bankName", "Bank Name"],
  ["brName", "BR Name"],
  ["custodyBankAccountFlag", "Custody Bank Account Flag"],
];

/**
 * The custodian "Corporate Benefit Collection" template: [field, standard header].
 * Every field is optional text: the file is stored and can be read by any
 * module, but none requires a column yet.
 */
const CORP_ALLOCATION_COLUMNS = [
  ["parentClientName", "Parent Client Name"],
  ["clientUccCode", "Client UCC Code"],
  ["clientName", "Client Name"],
  ["uccStatus", "UCC Status"],
  ["bankAccountType", "Bank Account Type"],
  ["bankAcNo", "Bank AC NO"],
  ["bankName", "Bank Name"],
  ["brName", "BR Name"],
  ["dematClientId", "Demat Client ID"],
  ["dpName", "DP Name"],
  ["dpId", "DP ID"],
  ["depositoryName", "DepositoryName"],
  ["isin", "ISIN"],
  ["isinName", "ISIN Name"],
  ["caRefNo", "CA Ref. No."],
  ["participatedCurrency", "Participated Currency"],
  ["exDate", "Ex-Date"],
  ["bookClosureFromDate", "Book Closure From Date"],
  ["bookClosureToDate", "Book Closure To Date"],
  ["recordDate", "Record Date"],
  ["caType", "CA Type"],
  ["eventType", "Event Type"],
  ["caTypeDescription", "CA Type Description"],
  ["faceValue", "Face Value"],
  ["ratio", "Ratio (Propose : Held)"],
  ["dividendRate", "Dividend Rate"],
  ["holdingQty", "Holding Qty"],
  ["poolHoldingQty", "Pool Holding Qty"],
  ["newIsin", "New ISIN"],
  ["newIsinName", "New ISIN Name"],
  ["newIsinFaceValue", "New ISIN FaceValue"],
  ["caAmount", "CA Amount"],
  ["poolCaAmount", "Pool CA Amount"],
  ["caQty", "CA Qty"],
  ["poolCaQty", "Pool CA Qty"],
  ["fractionQty", "Fraction Qty"],
  ["poolFractionQty", "Pool Fraction Qty"],
  ["remarks", "Remarks"],
  ["receivedDate", "Received Date"],
  ["receivedAmount", "Received Amount"],
  ["balanceAmount", "Balance Amount"],
  ["receivedQty", "Received Quantity"],
  ["balanceQty", "Balance Qty"],
  ["caTaxAmt", "CA TaxAmt"],
  ["eventStatus", "Event Status"],
  ["fundStrategy", "Fund Strategy"],
  ["tentativePaymentDate", "Tentative Payment Date"],
  ["custodyBankAccountFlag", "Custody Bank Account Flag"],
];

/** Columns the Split / Bonus custodian comparison reads. */
const CORP_ACTION_REQUIRED_FIELDS = [
  "clientUccCode",
  "isin",
  "caTypeDescr",
  "recordDate",
  "ratio",
  "holdingQty",
];

/**
 * The canonical fields, per profile type.
 *
 * `matchKey` marks the five that build the match key (Actual Code + ISIN +
 * Transaction Date + Transaction Type, compared on Quantity). They must point
 * at a real column: they cannot be left unanswered and cannot be declared
 * absent, because without them the comparison has nothing to compare.
 * NDPMS has four: its Transaction Date is the date picked for the file.
 *
 * `required` marks a field the mapping screen will not let the user leave
 * unanswered. Every field carries it — but "answered" includes answering
 * SOURCE_BY.ABSENT, so a file that genuinely lacks a column is still mappable.
 * The two flags are deliberately separate: `matchKey` decides what makes a
 * FILE unreadable, `required` decides what makes a MAPPING incomplete.
 *
 * `kind` is what a column's contents are checked against before the mapping is
 * accepted; it does not change how the column is located.
 */
/**
 * The transaction file's canonical fields.
 *
 * The base shared by both transaction profiles. The mutual fund file carries
 * these same 26 under one different heading, plus two of its own — so it
 * extends this list rather than restating it.
 */
const FA_FIELDS = [
  { field: "accountCode", label: "Account Code", kind: "code", required: true, matchKey: true },
  { field: "isin", label: "ISIN", kind: "isin", required: true, matchKey: true },
  { field: "tranDate", label: "Transaction Date", kind: "date", required: true, matchKey: true },
  { field: "tranType", label: "Transaction Type", kind: "type", required: true, matchKey: true },
  { field: "quantity", label: "Quantity", kind: "number", required: true, matchKey: true },

  { field: "brokerCode", label: "Broker Code", kind: "code", required: true },
  { field: "exchange", label: "Exchange", kind: "text", required: true },
  { field: "settlementDate", label: "Settlement Date", kind: "date", required: true },
  { field: "rate", label: "Rate", kind: "number", required: true },
  { field: "brokerage", label: "Brokerage", kind: "number", required: true },
  { field: "serviceTax", label: "Service Tax", kind: "number", required: true },
  { field: "stt", label: "STT", kind: "number", required: true },
  { field: "ref", label: "Transaction Ref", kind: "text", required: true },
  // Each trade's own unique ID, a different column from Transaction Ref. The
  // 26-column template has no such column, so it is optional here and the
  // operator maps it on the Field Mapping screen. The Equity tab refuses a row
  // without it (EquityReload.js); it is stored in Transaction.Transaction_ID.
  { field: "transactionId", label: "Transaction ID", kind: "text", required: false },
  { field: "description", label: "Description", kind: "text", required: true },

  // Not used by matching, but carried through to the production import. A
  // remapped file that cannot fill these would silently lose Security_code
  // and PORTFOLIOID on every row it writes to the Transaction table.
  { field: "setDateFlag", label: "Settlement Date Flag", kind: "text", required: true },
  { field: "mktRate", label: "Market Rate", kind: "number", required: true },
  { field: "cashSymbolCode", label: "Cash Symbol Code", kind: "code", required: true },
  { field: "accruedInterest", label: "Accrued Interest", kind: "number", required: true },
  { field: "blockId", label: "Block / Portfolio ID", kind: "code", required: true },
  { field: "chequeNumber", label: "Cheque Number", kind: "text", required: true },
  { field: "chequeDetail", label: "Cheque Detail", kind: "text", required: true },
  { field: "bankRef", label: "Bank Ref", kind: "text", required: true },
  // The three template columns headed "filler". Often blank, so they may be
  // left unanswered on the mapping screen.
  { field: "subFolio", label: "filler/Sub Folio", kind: "text", required: false },
  { field: "transactionFee", label: "filler/Transaction_Fee", kind: "number", required: false },
  {
    field: "transactionFeeServiceTax",
    label: "filler/Transaction_Fee_Service_Tax",
    kind: "number",
    required: false,
  },
  { field: "cashSetdate", label: "Cash Settlement Date", kind: "date", required: true },
];

/**
 * The mutual fund file's two extra trailing columns.
 *
 * Deductions the fund house reports per transaction. Neither is compared — they
 * are carried for the reconciliation to display — so neither can reject a row.
 */
/*
 * Extra columns the user adds on the mapping screen with "Add column". The
 * name the user typed for each is stored with the mapping as `labels` and shown
 * in place of "Extra column N".
 *
 * Transaction files only (Equity and Mutual Funds): each goes to the
 * Transaction column of the same name (Column1 ... Column5).
 */
export const EXTRA_COLUMN_FIELDS = ["column1", "column2", "column3", "column4", "column5"];

/** Longest name a user may give an extra column. */
export const EXTRA_COLUMN_LABEL_MAX = 40;

const EXTRA_COLUMN_ENTRIES = EXTRA_COLUMN_FIELDS.map((field, i) => ({
  field,
  label: `Extra column ${i + 1}`,
  kind: "text",
  required: false,
  extra: true,
}));

const FA_MUTUAL_FUND_FIELDS = [
  // Every mutual fund row carries its own Transaction ID, so it must be mapped here.
  ...FA_FIELDS.map((entry) =>
    entry.field === "transactionId" ? { ...entry, required: true } : entry,
  ),
  // The trade's final amount. When filled it is booked as it is; when blank the
  // import works it out (TempTransactionUpload.js deriveMutualFundAmount).
  { field: "totalAmount", label: "TOTAL AMOUNT", kind: "number", required: false },
  { field: "tds", label: "TDS", kind: "number", required: false },
  { field: "stampDuty", label: "Stamp Duty", kind: "number", required: false },
  ...EXTRA_COLUMN_ENTRIES,
];

// OPI / OPO entries come from another system and carry no Transaction ID.
const FA_OPI_OPO_FIELDS = FA_FIELDS.filter((entry) => entry.field !== "transactionId");

// Mutual Funds OPI / OPO: the same, then TDS and Stamp Duty. Stamp Duty is part of an OPI's cost.
const FA_OPI_OPO_MF_FIELDS = [
  ...FA_OPI_OPO_FIELDS,
  { field: "tds", label: "TDS", kind: "number", required: false },
  { field: "stampDuty", label: "Stamp Duty", kind: "number", required: false },
];

/*
 * The Mutual Funds TDS file, one row per TDI / TDO entry. Only the columns that
 * say whose tax it is, which entry, which day and how much are required. Types
 * are "text" because TDI / TDO are not buy/sell codes.
 */
const MF_TDS_COLUMNS = [
  ["brokerCode", "Broker code", "code", false],
  ["accountCode", "Broker acc id", "code", true],
  ["securityCode", "Security code", "code", false],
  ["exchange", "Exchange", "text", false],
  ["tranType", "Transaction type", "text", true],
  ["tranDate", "Transaction date", "date", true],
  ["paymentDate", "Payment date", "date", false],
  ["amount", "AMT", "number", true],
  ["price", "Price", "number", false],
  ["brokerage", "Brokerage", "number", false],
  ["serviceTax", "Service tax", "number", false],
  ["setDateFlag", "Settlement date flag", "text", false],
  ["mktRate", "Market rate as on security in date", "number", false],
  ["cashSymbol", "Cash symbol", "code", false],
  ["stt", "STT amount", "number", false],
  ["accruedInterest", "Accrued interest", "number", false],
  ["blockRef", "Block ref.", "text", false],
  ["remarks", "Remarks", "text", false],
  ["chequeNumber", "Cheque number", "text", false],
  ["chequeDetail", "Cheque details", "text", false],
  ["bankRef", "bankref", "text", false],
];

const FA_MUTUAL_FUND_TDS_FIELDS = MF_TDS_COLUMNS.map(([field, label, kind, required]) => ({
  field,
  label,
  kind,
  required,
}));

export const FIELD_CATALOG = {
  // The OPI/OPO file is built from FA_FIELDS, so it gets no extra columns.
  [PROFILE_TYPE.FA]: [...FA_FIELDS, ...EXTRA_COLUMN_ENTRIES],
  [PROFILE_TYPE.FA_MUTUAL_FUND]: FA_MUTUAL_FUND_FIELDS,
  [PROFILE_TYPE.FA_OPI_OPO]: FA_OPI_OPO_FIELDS,
  [PROFILE_TYPE.FA_OPI_OPO_MF]: FA_OPI_OPO_MF_FIELDS,
  [PROFILE_TYPE.FA_MUTUAL_FUND_TDS]: FA_MUTUAL_FUND_TDS_FIELDS,

  [PROFILE_TYPE.CUSTODY_RESIDENTIAL]: [
    { field: "actualCode", label: "Actual Code", kind: "code", required: true, matchKey: true },
    { field: "isin", label: "ISIN", kind: "isin", required: true, matchKey: true },
    { field: "tranDate", label: "Transaction Date", kind: "date", required: true, matchKey: true },
    { field: "tranType", label: "Transaction Type", kind: "type", required: true, matchKey: true },
    { field: "quantity", label: "Quantity", kind: "number", required: true, matchKey: true },

    { field: "fallbackCode", label: "Fallback Code", kind: "code", required: true },
    { field: "rate", label: "Rate", kind: "number", required: true },
    { field: "brokerage", label: "Brokerage", kind: "number", required: true },
    { field: "stt", label: "STT", kind: "number", required: true },
    { field: "netAmount", label: "Net Amount", kind: "number", required: true },
    { field: "settlementDate", label: "Settlement Date", kind: "date", required: true },
    { field: "ref", label: "Trade / Contract Ref", kind: "text", required: true },
    { field: "exchange", label: "Exchange", kind: "text", required: true },
    { field: "brokerCode", label: "Broker Code", kind: "code", required: true },
  ],

  /*
   * No Transaction Date here: NDPMS files carry no date column, so every row
   * takes the date the user picked for the file (see parseCustodyFile).
   */
  [PROFILE_TYPE.CUSTODY_NDPMS]: [
    { field: "actualCode", label: "Actual Code", kind: "code", required: true, matchKey: true },
    { field: "isin", label: "ISIN", kind: "isin", required: true, matchKey: true },
    { field: "tranType", label: "Transaction Type", kind: "type", required: true, matchKey: true },
    { field: "quantity", label: "Quantity", kind: "number", required: true, matchKey: true },

    { field: "rate", label: "Rate", kind: "number", required: true },
    { field: "brokerage", label: "Brokerage", kind: "number", required: true },
    { field: "stt", label: "STT", kind: "number", required: true },
    { field: "netAmount", label: "Net Amount", kind: "number", required: true },
    { field: "settlementDate", label: "Settlement Date", kind: "date", required: true },
    { field: "ref", label: "Trade / Contract Ref", kind: "text", required: true },
    { field: "exchange", label: "Exchange", kind: "text", required: true },
    { field: "brokerCode", label: "Broker Code", kind: "code", required: true },
  ],

  /*
   * Mutual Funds transaction file — every one of the 22 standard columns is
   * mapped, so nothing in the custodian's file is read past without an answer.
   *
   * `required` means the mapping screen needs an ANSWER for every field, which
   * "not present in this file" satisfies. When a file is read, only the five
   * match-key columns must actually be present (parseCustody resolves this
   * layout non-strictly); everything else is display-only and may be blank.
   *
   * The twelve beyond the match key and the original display set are read onto
   * the row and reach the run, but the exception table has no column for them
   * yet, so they do not appear in the difference report until it does.
   */
  [PROFILE_TYPE.CUSTODY_MUTUAL_FUND]: [
    { field: "actualCode", label: "Actual Code (UCC)", kind: "code", required: true, matchKey: true },
    { field: "isin", label: "ISIN", kind: "isin", required: true, matchKey: true },
    // DD-MM-YYYY in the files this custodian actually sends ("03-09-2026"),
    // so the wider reading is used rather than the strict YYYY-MM-DD one.
    { field: "tranDate", label: "Trade Date", kind: "date", required: true, matchKey: true },
    { field: "tranType", label: "Transaction Type", kind: "type", required: true, matchKey: true },
    { field: "quantity", label: "Units", kind: "number", required: true, matchKey: true },

    { field: "netAmount", label: "Amount", kind: "number", required: true },
    { field: "settlementDate", label: "Settlement Date", kind: "date", required: true },
    { field: "ref", label: "Reference No", kind: "text", required: true },
    { field: "exchange", label: "Market Type", kind: "text", required: true },
    // ARN codes carry a hyphen ("ARN-12345"), so they are read as text.
    { field: "brokerCode", label: "Broker ARN Code", kind: "text", required: true },

    /*
     * The four columns worth naming beyond the shared set. All display-only and
     * all read as text — none is compared, and text cannot reject a file.
     *
     * The other eight columns in the layout (Mode, BankAccount, the two
     * sub-broker codes, EUINNumber, both Target* switch columns and RiaCode)
     * are deliberately left unmapped: nothing in the reconciliation reads them,
     * and the ignored-column warning already reports that the file carries
     * them. Mapping a column that feeds nothing is work with no decision in it.
     */
    { field: "folioNo", label: "Folio No", kind: "text", required: true },
    { field: "schemeName", label: "Scheme Name", kind: "text", required: true },
    { field: "amcName", label: "Mutual Fund (AMC)", kind: "text", required: true },
    { field: "firstHolderName", label: "First Holder Name", kind: "text", required: true },
  ],
  [PROFILE_TYPE.CUSTODY_HOLDINGS]: CUSTODY_HOLDINGS_COLUMNS.map(([field, header, kind]) => ({
    field,
    label: header,
    kind,
    required: CUSTODY_HOLDINGS_REQUIRED_FIELDS.includes(field),
    matchKey: CUSTODY_HOLDINGS_KEY_FIELDS.includes(field),
  })),
  [PROFILE_TYPE.CUSTODY_CORP_ACTION]: CORP_ACTION_COLUMNS.map(([field, header]) => ({
    field,
    label: header,
    kind: "text",
    required: CORP_ACTION_REQUIRED_FIELDS.includes(field),
    matchKey: CORP_ACTION_REQUIRED_FIELDS.includes(field),
  })),
  [PROFILE_TYPE.CUSTODY_CORP_ALLOCATION]: CORP_ALLOCATION_COLUMNS.map(([field, header]) => ({
    field,
    label: header,
    kind: "text",
    required: false,
    matchKey: false,
  })),
};

/** Custodian layout -> profile type, so callers holding a CUSTODY_FORMAT can find the profile. */
export const FORMAT_TO_PROFILE_TYPE = {
  [CUSTODY_FORMAT.RESIDENT]: PROFILE_TYPE.CUSTODY_RESIDENTIAL,
  [CUSTODY_FORMAT.NDPMS]: PROFILE_TYPE.CUSTODY_NDPMS,
  [CUSTODY_FORMAT.MUTUAL_FUND]: PROFILE_TYPE.CUSTODY_MUTUAL_FUND,
};

const byName = (name) => ({ by: SOURCE_BY.NAME, name, transform: TRANSFORM.NONE });
const byIndex = (index) => ({ by: SOURCE_BY.INDEX, index, transform: TRANSFORM.NONE });

/**
 * The built-in profiles — byte-for-byte today's behaviour.
 *
 * FA is addressed by INDEX because of the three "filler" columns; the header row
 * is validated for exact order before rows are read, so index is safe there.
 * Custodian files are addressed by NAME because custodians reorder their exports.
 */
/**
 * The 26-column template, addressed by position.
 *
 * Also the starting point for the mutual fund transaction file, which carries
 * the same columns in the same order and differs only in one heading
 * (SYMBOLCODE/ISIN CODE rather than SYMBOLCODE). Because the positions are
 * unchanged, every field lands on the right column before the user touches
 * anything, and mapping that file is a confirmation rather than 26 decisions.
 */
const FA_DEFAULT_PROFILE = {
  accountCode: byIndex(1), // BROKERACID
  isin: byIndex(2), // SYMBOLCODE
  tranDate: byIndex(5), // DATEPUR_ACQUI
  tranType: byIndex(4), // TRANSTYPE
  quantity: byIndex(7), // QUANTITY

  brokerCode: byIndex(0),
  exchange: byIndex(3),
  settlementDate: byIndex(6),
  rate: byIndex(8),
  brokerage: byIndex(9),
  serviceTax: byIndex(10),
  stt: byIndex(14), // TRANEXPENSE
  ref: byIndex(17), // TRANSREF
  description: byIndex(18),

  setDateFlag: byIndex(11),
  mktRate: byIndex(12),
  cashSymbolCode: byIndex(13),
  accruedInterest: byIndex(15),
  blockId: byIndex(16),
  chequeNumber: byIndex(19),
  chequeDetail: byIndex(20),
  bankRef: byIndex(21),
  // The three "filler" columns, in order.
  subFolio: byIndex(22),
  transactionFee: byIndex(23),
  transactionFeeServiceTax: byIndex(24),
  cashSetdate: byIndex(25),
};

/** Fields added after mappings were first saved; see withNewFillerFields. */
const FA_FILLER_FIELDS = ["subFolio", "transactionFee", "transactionFeeServiceTax"];

export const DEFAULT_PROFILES = {
  [PROFILE_TYPE.FA]: FA_DEFAULT_PROFILE,
  // The same 26 template positions as the Equity file.
  [PROFILE_TYPE.FA_OPI_OPO]: FA_DEFAULT_PROFILE,

  // The 26 template positions, then this file's own two on the end.
  [PROFILE_TYPE.FA_MUTUAL_FUND]: {
    ...FA_DEFAULT_PROFILE,
    tds: byIndex(26),
    stampDuty: byIndex(27),
  },
  // The same layout as the Mutual Funds file.
  [PROFILE_TYPE.FA_OPI_OPO_MF]: {
    ...FA_DEFAULT_PROFILE,
    tds: byIndex(26),
    stampDuty: byIndex(27),
  },

  // The headings of the sample TDS file.
  [PROFILE_TYPE.FA_MUTUAL_FUND_TDS]: Object.fromEntries(
    MF_TDS_COLUMNS.map(([field, header]) => [field, byName(header)]),
  ),

  [PROFILE_TYPE.CUSTODY_RESIDENTIAL]: {
    actualCode: byName("CLIENT_CODE"),
    isin: byName("ISIN"),
    tranDate: byName("TRADE_DATE"),
    tranType: byName("B_S"),
    quantity: byName("QTY"),

    fallbackCode: byName("UCC_CODE"),
    rate: byName("RATE"),
    brokerage: byName("BROKERAGE"),
    stt: byName("STT"),
    netAmount: byName("NET"),
    settlementDate: byName("SETTLEMENT_DATE"),
    ref: byName("TRADENO"),
    exchange: byName("EXCHANGE"),
    brokerCode: byName("BROKERCODE"),
  },

  [PROFILE_TYPE.CUSTODY_NDPMS]: {
    actualCode: byName("CPCode_UCC"),
    isin: byName("ISIN"),
    tranType: byName("TranType"),
    quantity: byName("Quantity"),

    rate: byName("MarketRate"),
    brokerage: byName("Brokerage"),
    stt: byName("STT"),
    netAmount: byName("NetAmount"),
    settlementDate: byName("SettlementDate"),
    ref: byName("ContractNo"),
    exchange: byName("Exchange"),
    brokerCode: byName("BrokerRegNo"),
  },

  [PROFILE_TYPE.CUSTODY_MUTUAL_FUND]: {
    actualCode: byName("UCC"),
    isin: byName("SourceMFISIN"),
    tranDate: byName("TradeDate"),
    tranType: byName("TransactionType"),
    quantity: byName("Units"),

    netAmount: byName("Amount"),
    settlementDate: byName("SettlementDate"),
    ref: byName("REFNo"),
    exchange: byName("MarketType"),
    brokerCode: byName("BrokerARNCode"),

    folioNo: byName("FolioNo"),
    schemeName: byName("SourceMFScheme"),
    amcName: byName("MutualFund"),
    firstHolderName: byName("FirstHolderName"),
  },
  [PROFILE_TYPE.CUSTODY_HOLDINGS]: Object.fromEntries(
    CUSTODY_HOLDINGS_COLUMNS.map(([field, header]) => [field, byName(header)]),
  ),
  [PROFILE_TYPE.CUSTODY_CORP_ACTION]: Object.fromEntries(
    CORP_ACTION_COLUMNS.map(([field, header]) => [field, byName(header)]),
  ),
  [PROFILE_TYPE.CUSTODY_CORP_ALLOCATION]: Object.fromEntries(
    CORP_ALLOCATION_COLUMNS.map(([field, header]) => [field, byName(header)]),
  ),
};

/** Case / space / punctuation-insensitive header form — same rule parseCustody.js uses. */
export const normHeader = (h) =>
  String(h || "")
    .trim()
    .toLowerCase()
    .replace(/[\s._\-/()]+/g, "");

const catalogFor = (type) => FIELD_CATALOG[type] || null;

/** @returns {object|null} the catalog entry for one field, or null when the field is not in the catalog. */
export const catalogEntry = (type, field) =>
  (catalogFor(type) || []).find((e) => e.field === field) || null;

/**
 * @returns {string[]} every field the mapping screen must see an answer for.
 * Answering SOURCE_BY.ABSENT counts — see checkProfileComplete.
 */
export const requiredFields = (type) =>
  (catalogFor(type) || []).filter((e) => e.required).map((e) => e.field);

/**
 * @returns {string[]} the fields that build the match key.
 *
 * These are the ones that decide whether a FILE can be read at all, which is a
 * different question from whether a MAPPING is finished. Everything that used
 * to branch on `required` and meant "the run cannot proceed without this"
 * branches on this instead.
 */
export const matchKeyFields = (type) =>
  (catalogFor(type) || []).filter((e) => e.matchKey).map((e) => e.field);

/** A deep-enough copy that callers can edit a default without mutating it. */
export const defaultProfile = (type) => {
  const src = DEFAULT_PROFILES[type];
  if (!src) return null;
  const out = {};
  for (const [field, source] of Object.entries(src)) out[field] = { ...source };
  return out;
};

const allowedTransforms = (entry) =>
  entry && Array.isArray(entry.allowTransforms) ? entry.allowTransforms : [TRANSFORM.NONE];

/**
 * Check a profile in isolation — shape, unknown fields, required coverage.
 *
 * Deliberately does NOT need the file: this is what the save endpoint calls, and
 * a profile must be storable before the next day's file exists. Checks that need
 * the actual header row happen in resolveFieldMap.
 *
 * @param {string} type   one of PROFILE_TYPE
 * @param {object} profile  { field: {by, name|index, transform} }
 * @returns {{ok:boolean, errors:Array<{code:string,field?:string,message:string}>}}
 */
export const validateProfile = (type, profile) => {
  const errors = [];
  const catalog = catalogFor(type);

  if (!catalog) {
    return {
      ok: false,
      errors: [
        {
          code: MAP_ERROR.UNKNOWN_TYPE,
          message: `"${type}" is not a known file type. Expected one of: ${PROFILE_TYPES.join(", ")}.`,
        },
      ],
    };
  }

  const entries = profile && typeof profile === "object" ? profile : {};

  for (const [field, source] of Object.entries(entries)) {
    const entry = catalogEntry(type, field);

    if (!entry) {
      errors.push({
        code: MAP_ERROR.UNKNOWN_FIELD,
        field,
        message: `"${field}" is not a field on the ${type} layout.`,
      });
      continue;
    }

    if (!source || typeof source !== "object") {
      errors.push({
        code: MAP_ERROR.BAD_SOURCE,
        field,
        message: `${entry.label} has no column selected.`,
      });
      continue;
    }

    const transform = source.transform || TRANSFORM.NONE;
    if (!TRANSFORMS.includes(transform)) {
      errors.push({
        code: MAP_ERROR.BAD_TRANSFORM,
        field,
        message: `${entry.label} uses an unknown transform "${transform}".`,
      });
    } else if (!allowedTransforms(entry).includes(transform)) {
      errors.push({
        code: MAP_ERROR.TRANSFORM_NOT_ALLOWED,
        field,
        message: `${entry.label} cannot use the "${transform}" transform.`,
      });
    }

    if (source.by === SOURCE_BY.ABSENT) {
      /*
       * "Not present in this file" is a real answer, but never for a match-key
       * field: the comparison is built from those five, so a run without one of
       * them would compare nothing and report every row as missing.
       */
      if (entry.matchKey) {
        errors.push({
          code: MAP_ERROR.BAD_SOURCE,
          field,
          message: `${entry.label} cannot be marked as not present — it is part of the match key.`,
        });
      }
      continue;
    }

    if (source.by === SOURCE_BY.NAME) {
      const name = String(source.name ?? "").trim();
      if (!name) {
        errors.push({
          code: MAP_ERROR.BAD_SOURCE,
          field,
          message: `${entry.label} has an empty column name.`,
        });
        continue;
      }
    } else if (source.by === SOURCE_BY.INDEX) {
      const index = source.index;
      if (!Number.isInteger(index) || index < 0) {
        errors.push({
          code: MAP_ERROR.BAD_SOURCE,
          field,
          message: `${entry.label} has an invalid column position.`,
        });
        continue;
      }
    } else {
      errors.push({
        code: MAP_ERROR.BAD_SOURCE,
        field,
        message: `${entry.label} must be mapped by column name, by position, or marked as not present.`,
      });
    }
  }

  /*
   * Only the match key is enforced here, NOT every required field.
   *
   * validateProfile is what fromStoredProfile uses to decide whether a stored
   * blob is readable, and a mapping saved before every field became required
   * is still perfectly readable — it simply has fields nobody has answered yet.
   * Demanding completeness here would silently discard those profiles and fall
   * back to the built-in default. Completeness is a save-time rule, and it
   * lives in checkProfileComplete.
   */
  for (const field of matchKeyFields(type)) {
    if (!entries[field]) {
      const entry = catalogEntry(type, field);
      errors.push({
        code: MAP_ERROR.MISSING_REQUIRED,
        field,
        // No heading was ever chosen, so the field's own name is what the
        // message can name.
        label: entry.label,
        required: true,
        message: `${entry.label} must be mapped — it is part of the match key.`,
      });
    }
  }

  return { ok: errors.length === 0, errors };
};

/**
 * Is every field answered?
 *
 * Separate from validateProfile on purpose. validateProfile answers "can this
 * mapping be used", which an older, partial profile still can. This answers
 * "has the user finished", which is what the save endpoint and the mapping
 * screen enforce. Answering SOURCE_BY.ABSENT counts as answered.
 *
 * @returns {{ok:boolean, missing:Array<{field:string,label:string}>}}
 */
export const checkProfileComplete = (type, profile) => {
  const catalog = catalogFor(type);
  if (!catalog) return { ok: false, missing: [] };

  const entries = profile && typeof profile === "object" ? profile : {};
  const missing = catalog
    .filter((entry) => entry.required && !entries[entry.field])
    .map((entry) => ({ field: entry.field, label: entry.label }));

  return { ok: missing.length === 0, missing };
};

/**
 * Read a file's header row so the mapping screen can offer real columns.
 *
 * Columns are returned WITH their position and a disambiguated label, because
 * the FA template repeats the name "filler" three times and picking the right
 * one is the whole point.
 *
 * @param {Buffer} buffer
 * @returns {{separator:string, columns:Array<{index:number,name:string,label:string,duplicate:boolean}>}}
 */
export const readHeaderColumns = (buffer) => {
  if (!buffer || buffer.length === 0) return { separator: ",", columns: [] };

  const headerLine = firstLineOf(buffer);
  const separator = detectSeparator(headerLine);
  const cells = splitHeaderCells(headerLine, separator);

  // A header line that is present but entirely blank identifies no columns at
  // all. Reporting one nameless column would let the mapping screen offer it.
  if (cells.every((c) => String(c ?? "").trim() === "")) return { separator, columns: [] };

  const counts = new Map();
  for (const cell of cells) {
    const key = normHeader(cell);
    counts.set(key, (counts.get(key) || 0) + 1);
  }

  const columns = cells.map((name, index) => {
    const duplicate = (counts.get(normHeader(name)) || 0) > 1;
    return {
      index,
      name,
      // A repeated name is useless on its own, so show where it sits.
      label: duplicate ? `${name} (column ${index + 1})` : name,
      duplicate,
    };
  });

  return { separator, columns };
};

/** Sample values echoed back per column, so a message can quote the data. */
const SAMPLE_VALUES_SHOWN = 5;

/**
 * What a column's contents may be used as.
 *
 * Mirrors the `kind` values in FIELD_CATALOG. Ordered most specific first: a
 * column of ISINs is also valid free text, but calling it text tells the user
 * nothing about why it cannot feed Quantity.
 */
export const COLUMN_KIND = {
  ISIN: "isin",
  /** A date in the standard YYYY-MM-DD format only. */
  ISO_DATE: "isoDate",
  /** A date in any accepted format (DD-MM-YYYY, YYYY-MM-DD, ...). */
  DATE: "date",
  TYPE: "type",
  NUMBER: "number",
  CODE: "code",
  TEXT: "text",
};

const KIND_ORDER = [
  COLUMN_KIND.ISIN,
  COLUMN_KIND.ISO_DATE,
  COLUMN_KIND.DATE,
  COLUMN_KIND.TYPE,
  COLUMN_KIND.NUMBER,
  COLUMN_KIND.CODE,
  COLUMN_KIND.TEXT,
];

/**
 * One test per kind, run against a single non-blank cell.
 *
 * These are the SAME functions the parsers use, deliberately. A column that
 * passes here and fails during the real parse would be the worst outcome: the
 * mapping screen would have promised something the run then refuses.
 */
const KIND_TESTS = {
  [COLUMN_KIND.NUMBER]: (v) => num(v) !== null,
  [COLUMN_KIND.DATE]: (v) => parseDate(v) !== null,
  [COLUMN_KIND.ISO_DATE]: (v) => parseIsoDate(v) !== null,
  // ISIN format is not validated — any non-blank cell is accepted.
  [COLUMN_KIND.ISIN]: (v) => normIsin(v) !== "",
  // Side matters: SQB and OPI are BUY codes on the FA side only, and
  // Subscription / Redemption appear only in custodian (Mutual Funds) files.
  [COLUMN_KIND.TYPE]: (v, side) =>
    normTranType(v, side) !== null ||
    (side === SIDE.CUSTODY && normMutualFundTranType(v) !== null),
  // Any non-blank cell is a usable code or free text.
  [COLUMN_KIND.CODE]: () => true,
  [COLUMN_KIND.TEXT]: () => true,
};

/** Does this cell yield a real date once DATE_FROM_FIRST_8 is applied? */
const readsAsCompactDate = (v) => parseContractNoDate(v) !== null;

/**
 * Describe each column by what is actually under it.
 *
 * Takes sample rows as POSITIONAL arrays, not objects keyed by heading: the FA
 * template repeats "filler" three times, and any object keyed by name collapses
 * those three into one.
 *
 * Detection is strict — every non-blank sample must pass. One value that is not
 * a date means the column is not a date. A tolerant rule would pass a column
 * that then fails during the real parse, where the failure is far more
 * expensive to diagnose than a red row on the mapping screen.
 *
 * Blank cells are ignored, and a column that is blank throughout the sample is
 * reported as compatible with everything: it is common for an optional column
 * to be empty in the first few rows, and refusing it would block a mapping the
 * file can perfectly well support.
 *
 * @param {Array<{index:number,name:string,label:string,duplicate:boolean}>} columns from readHeaderColumns
 * @param {Array<string[]>} sampleRows positional cells, header row excluded
 * @param {object} [opts]
 * @param {string} [opts.type] PROFILE_TYPE, which decides the transaction-type side
 * @returns {Array<object>} the same columns, each with samples and compatibility
 */
export const describeColumns = (columns, sampleRows = [], { type = null } = {}) => {
  // Both transaction files (Equity and Mutual Funds) use the fund accounting type codes.
  const side = isFaProfileType(type) ? SIDE.FA : SIDE.CUSTODY;
  const rows = Array.isArray(sampleRows) ? sampleRows : [];

  return (columns || []).map((col) => {
    const values = rows.map((row) => (Array.isArray(row) ? row[col.index] : undefined));
    const filled = values.map(text).filter((v) => v !== "");
    const unjudgeable = filled.length === 0;

    const compatibleKinds = unjudgeable
      ? [...KIND_ORDER]
      : KIND_ORDER.filter((kind) => filled.every((v) => KIND_TESTS[kind](v, side)));

    /*
     * Compatibility under a transform is tracked separately because the check
     * has to run on the TRANSFORMED value. NDPMS Transaction Date maps to
     * ContractNo, whose raw value ("20260703000123") is a number, not a date —
     * judged as-is, the only correct answer would be rejected.
     */
    const transformCompatible = {
      [TRANSFORM.DATE_FROM_FIRST_8]:
        unjudgeable || filled.every(readsAsCompactDate) ? [COLUMN_KIND.DATE] : [],
    };

    /*
     * The first row that rules each kind out, so the screen can say "row 600
     * has abc" instead of only "this column is not numbers". Row numbers count
     * the header as row 1, the same as upload errors.
     */
    const firstMismatch = (test) => {
      const i = values.findIndex((v) => text(v) !== "" && !test(text(v)));
      return i === -1 ? null : { row: i + 2, value: text(values[i]) };
    };
    const mismatch = {};
    for (const kind of KIND_ORDER) {
      if (!compatibleKinds.includes(kind)) {
        mismatch[kind] = firstMismatch((v) => KIND_TESTS[kind](v, side));
      }
    }
    const transformMismatch = {
      [TRANSFORM.DATE_FROM_FIRST_8]: transformCompatible[TRANSFORM.DATE_FROM_FIRST_8].length
        ? null
        : firstMismatch(readsAsCompactDate),
    };

    return {
      ...col,
      samples: filled.slice(0, SAMPLE_VALUES_SHOWN),
      sampleCount: values.length,
      blankCount: values.length - filled.length,
      // null when the column was blank throughout — "unknown", not "text".
      detectedKind: unjudgeable ? null : compatibleKinds[0] || COLUMN_KIND.TEXT,
      compatibleKinds,
      transformCompatible,
      mismatch,
      transformMismatch,
    };
  });
};

/**
 * Can this column feed this field, under this transform?
 *
 * The single rule behind both the disabled options and the red row error, kept
 * here so the screen and the server cannot disagree about it.
 */
export const isColumnCompatible = (column, entry, transform = TRANSFORM.NONE) => {
  if (!column || !entry) return false;
  const kinds =
    transform && transform !== TRANSFORM.NONE
      ? column.transformCompatible?.[transform] || []
      : column.compatibleKinds || [];
  return kinds.includes(entry.kind);
};

/**
 * Bind a profile to one file's actual header row.
 *
 * @param {string} type
 * @param {object} profile     validated profile; pass null to use the default
 * @param {string[]} rawHeaders  the file's row-1 cells, in order
 * @param {object} [opts]
 * @param {boolean} [opts.strict] treat EVERY mapped field as required. The
 *        mapping is a contract: if it names a heading, the file must carry it.
 *        Off, only the match-key fields have to resolve.
 * @returns {{ok:boolean, index:object, transforms:object, errors:Array, unmappedOptional:string[]}}
 *          `index` is field -> 0-based column position, ready for positional reads.
 */
export const resolveFieldMap = (type, profile, rawHeaders, { strict = false } = {}) => {
  const effective = profile || defaultProfile(type);
  const base = validateProfile(type, effective);
  if (!base.ok) {
    return { ok: false, index: {}, transforms: {}, errors: base.errors, unmappedOptional: [] };
  }

  const headers = Array.isArray(rawHeaders) ? rawHeaders : [];
  const nameToIndex = new Map();
  headers.forEach((h, i) => {
    const key = normHeader(h);
    // First occurrence wins; duplicates are only addressable by index, by design.
    if (!nameToIndex.has(key)) nameToIndex.set(key, i);
  });

  const index = {};
  const transforms = {};
  const errors = [];
  const unmappedOptional = [];

  for (const [field, source] of Object.entries(effective)) {
    const entry = catalogEntry(type, field);
    let resolved = -1;

    if (source.by === SOURCE_BY.ABSENT) {
      // The user has said this column is not in the file. Not an error at any
      // strictness — saying so is exactly what this answer means. Match-key
      // fields cannot reach here; validateProfile refuses them above.
      unmappedOptional.push(field);
      continue;
    }

    if (source.by === SOURCE_BY.NAME) {
      const key = normHeader(source.name);
      resolved = nameToIndex.has(key) ? nameToIndex.get(key) : -1;
      // An extra column (Column1–5) is optional data: a file without it still
      // reads, with blank values. This also keeps an Equity consolidated file
      // saved before Column1–5 existed readable.
      if (resolved === -1 && (entry.matchKey || (strict && !entry.extra))) {
        errors.push({
          code: MAP_ERROR.COLUMN_NOT_IN_FILE,
          field,
          label: entry.label,
          expected: source.name,
          required: Boolean(entry.matchKey),
          message: `${entry.label} is mapped to a column named "${source.name}", which this file does not have.`,
          hint: "Rename the column in the file to match, or open Field Mapping and change the mapping to the name the file uses.",
        });
      }
    } else {
      resolved = source.index < headers.length ? source.index : -1;
      /*
       * The contract is what the user NAMED. An index is a position in the
       * standard template, so for an optional field it says nothing about a
       * file of a different shape — enforcing it would refuse an upload over
       * fields nobody chose, naming column numbers instead of headings.
       * Required fields are still enforced either way.
       */
      if (resolved === -1 && entry.matchKey) {
        errors.push({
          code: MAP_ERROR.INDEX_OUT_OF_RANGE,
          field,
          label: entry.label,
          expected: `column ${source.index + 1}`,
          required: Boolean(entry.matchKey),
          message: `${entry.label} is mapped to column ${source.index + 1}, but this file has only ${headers.length}.`,
          hint: "Open Field Mapping and re-pick the column.",
        });
      }
    }

    if (resolved === -1) {
      if (!entry.matchKey) unmappedOptional.push(field);
      continue;
    }

    index[field] = resolved;
    transforms[field] = source.transform || TRANSFORM.NONE;
  }

  return { ok: errors.length === 0, index, transforms, errors, unmappedOptional };
};

/**
 * Check the names the user typed for the extra columns.
 *
 * Every extra column that points at a file column needs a name, because the
 * name is the only thing that tells the user what Column1 holds. A name for a
 * slot that points at nothing is dropped: that slot was removed on screen.
 *
 * @returns {{ok:boolean, labels:object, errors:Array}}
 */
export const checkExtraLabels = (type, profile, labels) => {
  const given = labels && typeof labels === "object" ? labels : {};
  const kept = {};
  const errors = [];
  const hasExtraColumns = (catalogFor(type) || []).some((entry) => entry.extra);
  if (!hasExtraColumns) return { ok: true, labels: kept, errors };

  for (const field of EXTRA_COLUMN_FIELDS) {
    const source = profile?.[field];
    const inUse = source && source.by !== SOURCE_BY.ABSENT;
    if (!inUse) continue;
    const name = String(given[field] ?? "").trim();
    if (!name) {
      errors.push({ field, message: `Extra column ${field.slice(-1)} needs a name.` });
    } else if (name.length > EXTRA_COLUMN_LABEL_MAX) {
      errors.push({
        field,
        message: `The name "${name}" is longer than ${EXTRA_COLUMN_LABEL_MAX} characters.`,
      });
    } else {
      kept[field] = name;
    }
  }
  return { ok: errors.length === 0, labels: kept, errors };
};

/** A stored profile blob, ready for Stratus. `labels` = names of the extra columns. */
export const toStoredProfile = (
  type,
  profile,
  { updatedBy = "", updatedAtMs = Date.now(), labels = {} } = {},
) => ({
  version: PROFILE_VERSION,
  type,
  profile,
  labels,
  updatedBy: String(updatedBy || "").slice(0, 255),
  updatedAtMs,
});

/**
 * Read a stored blob back, falling back to the default when it is absent or
 * unreadable. A corrupt config must not take the upload screen down with it.
 *
 * @returns {{profile:object, isDefault:boolean, updatedAtMs:number|null, updatedBy:string}}
 */
/*
 * Fields that are no longer mapped. Mappings saved before still carry them;
 * drop them on read so such a mapping stays valid instead of silently falling
 * back to the default.
 *
 * Net Rate and Net Amount on the transaction files: the import always derives
 * them (RATE ± brokerage, × quantity).
 * Transaction Date on NDPMS: every row takes the date picked for the file.
 * Extra columns on the Resident and NDPMS custodian files: offered for a short
 * while (November 2026) but never stored, so they were removed.
 */
const RETIRED_FIELDS = {
  [PROFILE_TYPE.FA]: ["netRate", "netAmount"],
  [PROFILE_TYPE.FA_MUTUAL_FUND]: ["netRate", "netAmount"],
  [PROFILE_TYPE.FA_OPI_OPO]: ["netRate", "netAmount", "transactionId"],
  [PROFILE_TYPE.CUSTODY_RESIDENTIAL]: [...EXTRA_COLUMN_FIELDS],
  [PROFILE_TYPE.CUSTODY_NDPMS]: ["tranDate", ...EXTRA_COLUMN_FIELDS],
};

const withoutRetiredFields = (type, profile) => {
  const kept = { ...profile };
  for (const field of RETIRED_FIELDS[type] || []) delete kept[field];
  return kept;
};

/*
 * The three filler fields are optional, so a transaction mapping saved before
 * they existed is never asked about them, and their values would silently stop
 * reaching the import. Give such a mapping the standard filler positions; the
 * mapping screen shows them and the user can still change them.
 */
const withNewFillerFields = (type, profile) => {
  if (!isFaProfileType(type)) return profile;
  const filled = { ...profile };
  for (const field of FA_FILLER_FIELDS) {
    if (!filled[field]) filled[field] = { ...FA_DEFAULT_PROFILE[field] };
  }
  return filled;
};

export const fromStoredProfile = (type, blob) => {
  const fallback = {
    profile: defaultProfile(type),
    labels: {},
    isDefault: true,
    updatedAtMs: null,
    updatedBy: "",
  };

  if (!blob || typeof blob !== "object" || !blob.profile) return fallback;
  if (blob.version !== PROFILE_VERSION) return fallback;

  const profile = withNewFillerFields(type, withoutRetiredFields(type, blob.profile));
  if (!validateProfile(type, profile).ok) return fallback;

  return {
    profile,
    labels: checkExtraLabels(type, profile, blob.labels).labels,
    isDefault: false,
    updatedAtMs: Number(blob.updatedAtMs) || null,
    updatedBy: String(blob.updatedBy || ""),
  };
};

/** The full FA header list, exposed so the mapping UI can seed a profile with no sample file. */
export const FA_DEFAULT_COLUMNS = FA_HEADERS.map((name, index) => ({
  index,
  name,
  label: FA_HEADERS.filter((h) => h === name).length > 1 ? `${name} (column ${index + 1})` : name,
  duplicate: FA_HEADERS.filter((h) => h === name).length > 1,
}));

/**
 * Canonical field -> its position in the 26-column FA template.
 *
 * The inverse of the FA default profile, and the thing that lets a file in ANY
 * layout be rewritten into the template the production import still expects.
 * The three "filler" columns (23-25) are Sub Folio, Transaction Fee and
 * Transaction Fee Service Tax. NETRATE and Net_Amount have no column: the
 * import always calculates them.
 */
export const FA_TEMPLATE_SLOT = Object.fromEntries(
  Object.entries(DEFAULT_PROFILES[PROFILE_TYPE.FA]).map(([field, source]) => [field, source.index]),
);

/**
 * The Equity tab's consolidated file: the 26 template columns, the Transaction
 * ID as column 27 (heading FA_TRANSACTION_ID_HEADER), then Column1 ... Column5.
 * Read with this profile, because the plain template has neither. The extra
 * columns are found by heading, so a file saved before they existed still
 * reads (they come back blank).
 */
export const FA_TRANSACTION_ID_HEADER = "Transaction ID";
export const FA_CONSOLIDATED_PROFILE = {
  ...DEFAULT_PROFILES[PROFILE_TYPE.FA],
  transactionId: byIndex(FA_HEADERS.length),
  ...Object.fromEntries(EXTRA_COLUMN_FIELDS.map((field, i) => [field, byName(`Column${i + 1}`)])),
};

/**
 * Is this profile the shipped default?
 *
 * Callers use it to keep the strict path strict: an unedited FA profile still
 * gets the exact 26-column header check, so nothing about today's behaviour
 * changes until somebody actually edits the mapping.
 */
export const isDefaultProfile = (type, profile) => {
  const base = DEFAULT_PROFILES[type];
  if (!base) return false;
  if (!profile) return true;

  const fields = new Set([...Object.keys(base), ...Object.keys(profile)]);
  for (const field of fields) {
    const a = base[field];
    const b = profile[field];
    if (!a || !b) return false;
    if (a.by !== b.by) return false;
    if (a.by === SOURCE_BY.NAME && normHeader(a.name) !== normHeader(b.name)) return false;
    if (a.by === SOURCE_BY.INDEX && a.index !== b.index) return false;
    if ((a.transform || TRANSFORM.NONE) !== (b.transform || TRANSFORM.NONE)) return false;
  }
  return true;
};

/**
 * field -> the raw header it resolved to, for error messages.
 *
 * Errors have to name the column as it appears in the USER's file. Saying
 * "BROKERACID is blank" about a file whose column is called "Account" is worse
 * than useless.
 */
export const columnNamesFor = (resolved, rawHeaders) => {
  const out = {};
  const headers = Array.isArray(rawHeaders) ? rawHeaders : [];
  for (const [field, index] of Object.entries(resolved.index || {})) {
    out[field] = headers[index] ?? `column ${index + 1}`;
  }
  return out;
};
