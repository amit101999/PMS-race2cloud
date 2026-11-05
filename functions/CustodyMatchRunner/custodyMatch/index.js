/**
 * Custody Matching — parse & compare core.
 *
 * Pure logic: no Catalyst SDK, no Data Store, no Stratus. Everything here runs
 * offline against a Buffer, which is what lets the same code back both the
 * synchronous validation in AppSail (S0) and the pipeline inside the job
 * function (S1-S5) without a second implementation drifting away from the first.
 *
 * Vendored into functions/CustodyMatchRunner/ as a straight file copy — the only
 * external dependency is csv-parser, which the function's package.json declares.
 */

export * from "./constants.js";
export * from "./normalize.js";
export { validateFaHeader, parseFaFile, faRowsToConsolidatedCsv, detectFaFileType } from "./parseFA.js";
export {
  detectCustodyFormat,
  parseCustodyFile,
  mergeCustodyFiles,
  custodyRowsToCsv,
  parseCustodyFinalCsv,
  CUSTODY_FINAL_COLUMNS,
} from "./parseCustody.js";
export { resolveActualCodes, actualCodesOf } from "./resolveActual.js";
export { buildCodeMaps } from "./lookupCodes.js";
export {
  aggregateByKey,
  buildKeyUnion,
  splitMutualFunds,
  compareKeyRange,
  compareSides,
  repairOrphans,
  unmappedToExceptions,
  exceptionsToCsv,
  DIFFERENCE_CSV_COLUMNS,
  DIFFERENCE_CSV_HEADERS,
} from "./compare.js";
export {
  PROFILE_TYPE,
  PROFILE_TYPES,
  PROFILE_VERSION,
  SOURCE_BY,
  TRANSFORM,
  TRANSFORMS,
  MAP_ERROR,
  COLUMN_KIND,
  FIELD_CATALOG,
  FA_TEMPLATE_SLOT,
  FA_CONSOLIDATED_PROFILE,
  FA_DEFAULT_COLUMNS,
  FORMAT_TO_PROFILE_TYPE,
  DEFAULT_PROFILES,
  catalogEntry,
  requiredFields,
  matchKeyFields,
  defaultProfile,
  validateProfile,
  checkProfileComplete,
  readHeaderColumns,
  describeColumns,
  isColumnCompatible,
  resolveFieldMap,
  isDefaultProfile,
  isFaProfileType,
  columnNamesFor,
  toStoredProfile,
  fromStoredProfile,
} from "./fieldMap.js";
export * from "./storage.js";
export * from "./runStore.js";
