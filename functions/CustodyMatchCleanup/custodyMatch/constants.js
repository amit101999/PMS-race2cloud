/**
 * Custody Matching — shared constants.
 *
 * Authored here and vendored into functions/CustodyMatchRunner/ at deploy time
 * (same pattern as functions/ExportCorpActionReport/virtualToActual.js).
 * Keep this file dependency-free so the copy stays a straight file copy.
 */

/**
 * FA daily transaction file: row-1 headers — exact count (26), order, spelling, case.
 * Mirrors EXPECTED_HEADERS_IN_ORDER in controller/uploader/TempTransactionUpload.js.
 * The format is unchanged by custody matching; we only read from it.
 */
export const FA_HEADERS = [
  "Broker Code",
  "BROKERACID",
  "SYMBOLCODE",
  "EXCHG",
  "TRANSTYPE",
  "DATEPUR_ACQUI",
  "SETDATE",
  "QUANTITY",
  "RATE",
  "BROKERAGEPERSHARE",
  "SERVICETAX",
  "SETDATEFLAG",
  "MKTRATE",
  "CASHSYMBOLCODE",
  "TRANEXPENSE",
  "ACCRUEDINTEREST",
  "BLOCKID",
  "TRANSREF",
  "Description",
  "Cheque number",
  "CHQDETAIL",
  "BankRef",
  "filler",
  "filler",
  "filler",
  "CashSetdate",
];

/**
 * Columns the mutual fund transaction file carries after the 26, in this order.
 * Must match OPTIONAL_TRAILING_HEADERS in controller/uploader/TempTransactionUpload.js,
 * which writes them to TDSAMOUNT and STAMPDUTY.
 */
export const FA_DEDUCTION_HEADERS = ["TDS", "Stamp Duty"];

/** The five FA columns the match key and compared value are built from. */
export const FA_KEY_COLUMNS = {
  accountCode: "BROKERACID",
  isin: "SYMBOLCODE",
  tranType: "TRANSTYPE",
  tranDate: "DATEPUR_ACQUI",
  quantity: "QUANTITY",
};

/** Custodian format 1 — Resident deal-allocation file. Header set, order not enforced. */
export const CUSTODY_F1_HEADERS = [
  "UCC_CODE",
  "CLIENT_CODE",
  "TRADE_DATE",
  "ISIN",
  "QTY",
  "B_S",
  "EXECABCON_DATE",
  "RATE",
  "BROKERAGE",
  "STT",
  "NET",
  "SETTLEMENT_DATE",
  "TRADENO",
  "BROKERCODE",
  "EXCHANGE",
];

/** Custodian format 2 — NRE / NRO (NDPMS) deal-allocation file. */
export const CUSTODY_F2_HEADERS = [
  "Column1",
  "CPCode_UCC",
  "ContractNo",
  "Exchange",
  "BrokerRegNo",
  "SettlementNo",
  "Segment",
  "ISIN",
  "TranType",
  "Quantity",
  "MarketRate",
  "Amount",
  "Brokerage",
  "STT",
  "NetAmount",
  "Column2",
  "Column3",
  "SettlementDate",
];

/**
 * Custodian format 3 — Mutual Funds transaction file. Standard header set; only
 * the five mapped match columns must be present, the rest are informational.
 */
export const CUSTODY_F3_HEADERS = [
  "MarketType",
  "REFNo",
  "TransactionType",
  "Mode",
  "UCC",
  "FirstHolderName",
  "FolioNo",
  "BankAccount",
  "BrokerARNCode",
  "SubBrokerARNCode",
  "SubBrokerCode",
  "EUINNumber",
  "SourceMFISIN",
  "MutualFund",
  "SourceMFScheme",
  "TargetMFISIN",
  "TargetMFScheme",
  "Amount",
  "Units",
  "TradeDate",
  "SettlementDate",
  "RiaCode",
];

/** Which custodian layout a file uses. Detected from headers, cross-checked against the filename tag. */
export const CUSTODY_FORMAT = {
  RESIDENT: "F1_RESIDENT",
  NDPMS: "F2_NDPMS",
  MUTUAL_FUND: "F3_MUTUAL_FUND",
};

/**
 * Account type -> custodian layout. One file per scheme, either way; the types
 * differ only in the columns inside the file.
 */
export const CUSTODY_FILE_TYPE = {
  RESIDENTIAL: "RESIDENTIAL",
  NDPMS: "NDPMS",
  MUTUAL_FUND: "MUTUAL_FUND",
};

/**
 * Schemes the picker always offers, whether or not a file has been filed yet.
 *
 * A scheme is normally DISCOVERED — it exists because a file for it exists,
 * which is the one definition that cannot drift. That works from the second
 * upload onwards, but on a fresh system the list is empty and every scheme has
 * to be typed from memory. These are the ones known up front; anything filed
 * beyond them still appears automatically.
 *
 * `scheme` must be in the stored form — uppercase, with anything outside
 * A-Z 0-9 _ replaced by an underscore — or a filed file would list separately
 * from its own entry here. `fileType` is the layout pre-selected when the
 * scheme is picked; the user can still change it beside the picker.
 *
 * Add to this list as new schemes are taken on.
 */
export const PREDEFINED_SCHEMES = [
  { scheme: "ETERNAL", fileType: CUSTODY_FILE_TYPE.RESIDENTIAL },
];

export const CUSTODY_TYPE_TO_FORMAT = {
  [CUSTODY_FILE_TYPE.RESIDENTIAL]: CUSTODY_FORMAT.RESIDENT,
  [CUSTODY_FILE_TYPE.NDPMS]: CUSTODY_FORMAT.NDPMS,
  [CUSTODY_FILE_TYPE.MUTUAL_FUND]: CUSTODY_FORMAT.MUTUAL_FUND,
};

/**
 * Which transaction file was uploaded.
 *
 * Mutual fund transactions arrive in their own fund-accounting file, read
 * through its own field mapping: it carries the same 26 fields as the regular
 * file under one different heading, so one mapping cannot describe both. A day
 * holds at most one file of each type.
 *
 * OPI_OPO is the OPI/OPO tab's file: shares moved in (OPI) or out (OPO) from
 * another system. It uses the regular file's layout and mapping; it has its own
 * type so each tab lists, replaces and commits only its own files. It is never
 * matched against the custodian files.
 */
export const FA_FILE_TYPE = {
  REGULAR: "REGULAR",
  MUTUAL_FUND: "MUTUAL_FUND",
  OPI_OPO: "OPI_OPO",
};

/** How each transaction file is named in a message to the user. */
export const FA_FILE_TYPE_LABEL = {
  [FA_FILE_TYPE.REGULAR]: "transaction",
  [FA_FILE_TYPE.MUTUAL_FUND]: "mutual fund transaction",
  [FA_FILE_TYPE.OPI_OPO]: "OPI/OPO transaction",
};

/**
 * How an FA row is recognised as a mutual fund transaction: an INF ISIN (the
 * prefix given to mutual fund units) traded "DIR" (direct with the fund house).
 * The exchange check keeps ETFs — also INF, but traded on NSE/BSE — with the
 * equity custodian files.
 */
export const MF_ISIN_PREFIX = "INF";
export const MF_FA_EXCHANGE = "DIR";

/** Normalised transaction direction. Direction lives in the match key, never in the sign of quantity. */
export const TRAN_TYPE = {
  BUY: "BUY",
  SELL: "SELL",
};

/** Which side of the reconciliation a row came from. Gates the FA-only SQB/OPI buy codes. */
export const SIDE = {
  FA: "FA",
  CUSTODY: "CUSTODY",
};

export const EXCEPTION_TYPE = {
  QUANTITY_MISMATCH: "QUANTITY_MISMATCH",
  MISSING_IN_CUSTODY: "MISSING_IN_CUSTODY",
  MISSING_IN_FA: "MISSING_IN_FA",
  DATE_MISMATCH: "DATE_MISMATCH",
  TYPE_MISMATCH: "TYPE_MISMATCH",
  UNMAPPED_CODE: "UNMAPPED_CODE",
};

export const EXCEPTION_STATUS = {
  OPEN: "OPEN",
  RESOLVED: "RESOLVED",
};

/** Pipeline stages. Stored on Custody_Match_Run.stage and used as the resume point. */
export const STAGE = {
  VALIDATE: "S0",
  BUILD_FA: "S1",
  BUILD_CUSTODY: "S2",
  PERSIST: "S3",
  COMPARE: "S4",
  REPORT: "S5",
  DONE: "S6",
};

export const RUN_STATUS = {
  QUEUED: "QUEUED",
  RUNNING: "RUNNING",
  COMPLETED_CLEAN: "COMPLETED_CLEAN",
  COMPLETED_WITH_EXCEPTIONS: "COMPLETED_WITH_EXCEPTIONS",
  FAILED: "FAILED",
  SUPERSEDED: "SUPERSEDED",
};

/** Custody_Match_File.side — which half of the comparison a stored file belongs to. */
export const FILE_SIDE = {
  FA: "FA",
  CUSTODY: "CUSTODY",
};

/** File-level failures. The file is rejected whole; no rows are read. */
export const FILE_ERROR = {
  EMPTY_FILE: "EMPTY_FILE",
  NO_HEADER: "NO_HEADER",
  WRONG_COLUMN_ORDER: "WRONG_COLUMN_ORDER",
  MISSING_COLUMNS: "MISSING_COLUMNS",
  DUPLICATE_HEADERS: "DUPLICATE_HEADERS",
  UNKNOWN_CUSTODY_FORMAT: "UNKNOWN_CUSTODY_FORMAT",
  FORMAT_TYPE_MISMATCH: "FORMAT_TYPE_MISMATCH",
  NO_DATA_ROWS: "NO_DATA_ROWS",
  PARSE_FAILED: "PARSE_FAILED",
  /** A user-defined field mapping points at columns this file does not have. */
  UNMAPPED_COLUMNS: "UNMAPPED_COLUMNS",
  /** The file's headings do not match the saved mapping for this file type. */
  HEADER_MAPPING_MISMATCH: "HEADER_MAPPING_MISMATCH",
  /** An NRE/NRO/NDPMS file has no readable file date in its first row (B2). */
  FILE_DATE_MISSING: "FILE_DATE_MISSING",
  /** An NRE/NRO/NDPMS file's own date (B2) is not the date picked for it. */
  FILE_DATE_MISMATCH: "FILE_DATE_MISMATCH",
};

/** Row-level failures. The row is rejected; the rest of the file still parses. */
export const ROW_ERROR = {
  COLUMN_COUNT_MISMATCH: "COLUMN_COUNT_MISMATCH",
  MISSING_ACCOUNT_CODE: "MISSING_ACCOUNT_CODE",
  MISSING_ISIN: "MISSING_ISIN",
  BAD_DATE: "BAD_DATE",
  BAD_DATE_FORMAT: "BAD_DATE_FORMAT",
  MISSING_DATE: "MISSING_DATE",
  /** The row's date is not the date the user said the file is for. */
  WRONG_DATE: "WRONG_DATE",
  BAD_CONTRACT_NO: "BAD_CONTRACT_NO",
  UNKNOWN_TRAN_TYPE: "UNKNOWN_TRAN_TYPE",
  MISSING_TRAN_TYPE: "MISSING_TRAN_TYPE",
  BAD_QUANTITY: "BAD_QUANTITY",
  ZERO_QUANTITY: "ZERO_QUANTITY",
  MISSING_VALUE: "MISSING_VALUE",
  BAD_NUMBER: "BAD_NUMBER",
  NEGATIVE_NUMBER: "NEGATIVE_NUMBER",
};

/** Non-fatal observations. Surfaced to the user; never block. */
export const WARNING = {
  DATE_NOT_BUSINESS_DATE: "DATE_NOT_BUSINESS_DATE",
  NON_NUMERIC_DISPLAY_FIELD: "NON_NUMERIC_DISPLAY_FIELD",
  CONTRACT_DATE_AMBIGUOUS: "CONTRACT_DATE_AMBIGUOUS",
  FORMAT_TYPE_MISMATCH: "FORMAT_TYPE_MISMATCH",
  /** A field we can store has no column in this file, so it will be blank. */
  MISSING_OPTIONAL_COLUMN: "MISSING_OPTIONAL_COLUMN",
  /** A column in the file feeds nothing, so its values are ignored. */
  UNUSED_FILE_COLUMN: "UNUSED_FILE_COLUMN",
};

/**
 * Behaviour switches, so the two design proposals awaiting sign-off can be
 * turned off without touching the comparison logic.
 *
 * repairOrphans      — collapse the two rows a wrong date produces into one
 *                      DATE_MISMATCH / TYPE_MISMATCH. Off => two orphan rows.
 * unmappedAsException— report a code with no clientIds entry as UNMAPPED_CODE.
 *                      Off => resolveActualCodes throws and the run fails.
 * clientCodeFallback — on a blank CLIENT_CODE, fall back to UCC_CODE. Default OFF:
 *                      the code is part of the match key, so a wrong fallback
 *                      produces a mismatch nobody can explain. The user states
 *                      CLIENT_CODE is always present, so this should stay off.
 * quantityEpsilon    — float tolerance. Quantities are share counts, so this only
 *                      guards against representation error, not real differences.
 */
export const DEFAULT_OPTIONS = {
  repairOrphans: true,
  unmappedAsException: true,
  clientCodeFallback: false,
  quantityEpsilon: 1e-6,
};
