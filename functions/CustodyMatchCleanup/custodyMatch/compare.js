/**
 * Custody Matching — the comparison itself.
 *
 * THE RULE (user, explicit):
 *   key            = Actual Code + ISIN + Transaction Date + Transaction Type
 *   compared value = Quantity
 *
 * Everything else — rate, brokerage, STT, net/amount, settlement date, contract
 * or trade number, exchange, broker code, segment, Column1/2/3 — is carried onto
 * the exception row for investigation and NEVER influences a verdict. A brokerage
 * discrepancy is not a custody break.
 *
 * Quantity is unsigned; direction lives in the key. Signing and netting instead
 * would let buy 1000 / sell 400 collide with buy 400 / sell 1000.
 *
 * The key walk is sorted so it is deterministic: offset n means the same thing on
 * every attempt, which is what makes the S4 resume checkpoint meaningful.
 */

import {
  DEFAULT_OPTIONS,
  EXCEPTION_STATUS,
  EXCEPTION_TYPE,
} from "./constants.js";
import {
  buildMatchKey,
  buildOrphanKey,
  isMutualFundCustodyRow,
  isMutualFundFaRow,
  parseMatchKey,
} from "./normalize.js";

const round = (n) => Math.round((n + Number.EPSILON) * 1e6) / 1e6;

const joinSet = (set, limit = 10) => {
  const values = [...set].filter(Boolean);
  if (values.length === 0) return "";
  if (values.length <= limit) return values.join(",");
  return `${values.slice(0, limit).join(",")},+${values.length - limit} more`;
};

/**
 * Collapse rows onto match keys, summing quantity.
 *
 * @param {object[]} rows rows carrying actualCode / isin / tranDate / tranType / quantity
 * @returns {Map<string, object>}
 */
export const aggregateByKey = (rows) => {
  const map = new Map();

  for (const row of rows) {
    const actualCode = row.actualCode || row.accountCode || "";
    const key = buildMatchKey({
      actualCode,
      isin: row.isin,
      tranDate: row.tranDate,
      tranType: row.tranType,
    });

    let agg = map.get(key);
    if (!agg) {
      agg = {
        key,
        actualCode,
        isin: row.isin,
        tranDate: row.tranDate,
        tranType: row.tranType,
        quantity: 0,
        rowCount: 0,
        virtualCodes: new Set(),
        schemes: new Set(),
        refs: new Set(),
        sourceRows: [],
        // First row wins for display fields; they are reference only.
        rate: row.rate ?? 0,
        brokerage: row.brokerage ?? 0,
        stt: row.stt ?? 0,
        netAmount: row.netAmount ?? 0,
        settlementDate: row.settlementDate ?? null,
        exchange: row.exchange ?? "",
        brokerCode: row.brokerCode ?? "",
      };
      map.set(key, agg);
    }

    agg.quantity = round(agg.quantity + (row.quantity || 0));
    agg.rowCount += 1;
    // A single row carries the one virtual code it came from (in-memory, S1);
    // a row rebuilt from the consolidated file carries the whole set that rolled
    // into its actual code (S4, from the fa-meta sidecar). Both are accepted.
    if (row.virtualCode) agg.virtualCodes.add(row.virtualCode);
    if (Array.isArray(row.virtualCodes)) {
      for (const v of row.virtualCodes) if (v) agg.virtualCodes.add(v);
    }
    if (row.scheme) agg.schemes.add(row.scheme);
    if (row.ref) agg.refs.add(row.ref);
    if (agg.sourceRows.length < 50) agg.sourceRows.push(row.rowNumber);
  }

  return map;
};

/**
 * Split both sides into the two comparisons a run makes.
 *
 * Mutual fund FA rows (INF ISIN, exchange DIR) are compared only with the
 * Mutual Funds custodian files, and every other FA row only with the Residential
 * and NRE/NRO files. Without the split, an equity trade would read as missing
 * against a mutual fund file and the other way round.
 *
 * The two sides are recognised differently, and deliberately: a custodian row
 * knows which file it came from, while FA rows reach here from the consolidated
 * rewrite, which does not record that. Mutual fund rows only ever enter it from
 * the mutual fund transaction file, so both readings pick out the same rows.
 *
 * @returns {{name:string, faRows:object[], custodyRows:object[]}[]}
 */
export const splitMutualFunds = (faRows, custodyRows) => {
  const mutualFunds = { name: "MUTUAL_FUND", faRows: [], custodyRows: [] };
  const others = { name: "OTHER", faRows: [], custodyRows: [] };

  for (const row of faRows) {
    if (isMutualFundFaRow(row)) {
      mutualFunds.faRows.push(row);
    } else {
      others.faRows.push(row);
    }
  }
  for (const row of custodyRows) {
    if (isMutualFundCustodyRow(row)) {
      mutualFunds.custodyRows.push(row);
    } else {
      others.custodyRows.push(row);
    }
  }

  return [mutualFunds, others];
};

/**
 * Sorted union of both sides' keys. Sorting is what makes a resume offset stable.
 */
export const buildKeyUnion = (faMap, custodyMap) => {
  const keys = new Set([...faMap.keys(), ...custodyMap.keys()]);
  return [...keys].sort();
};

const schemeFor = (faAgg, custodyAgg, schemeByActualCode) => {
  // Custody rows know their scheme from the filename they arrived in.
  if (custodyAgg && custodyAgg.schemes.size > 0) return joinSet(custodyAgg.schemes);
  if (faAgg && faAgg.schemes.size > 0) return joinSet(faAgg.schemes);
  // FA-only rows have no scheme in the file. Until the source is settled
  // (clientIds column vs Transaction history), the caller may supply a map.
  const code = (faAgg || custodyAgg)?.actualCode;
  if (schemeByActualCode && code && schemeByActualCode.has(code)) {
    return schemeByActualCode.get(code);
  }
  return "";
};

const makeException = ({
  faAgg,
  custodyAgg,
  exceptionType,
  schemeByActualCode,
  overrideKeyParts = null,
}) => {
  const parts = overrideKeyParts || parseMatchKey((faAgg || custodyAgg).key);
  const faQuantity = faAgg ? faAgg.quantity : 0;
  const custodyQuantity = custodyAgg ? custodyAgg.quantity : 0;

  return {
    actualCode: parts.actualCode,
    isin: parts.isin,
    tranDate: parts.tranDate,
    tranType: parts.tranType,
    virtualCodes: faAgg ? joinSet(faAgg.virtualCodes) : "",
    scheme: schemeFor(faAgg, custodyAgg, schemeByActualCode),
    faQuantity,
    custodyQuantity,
    difference: round(faQuantity - custodyQuantity),
    exceptionType,
    status: EXCEPTION_STATUS.OPEN,
    faRef: faAgg ? joinSet(faAgg.refs, 3) : "",
    custodyRef: custodyAgg ? joinSet(custodyAgg.refs, 3) : "",
    sortKey: `${parts.actualCode}|${parts.isin}|${parts.tranDate}`,
    // Reference only — for the difference CSV and the row detail view.
    faRowCount: faAgg ? faAgg.rowCount : 0,
    custodyRowCount: custodyAgg ? custodyAgg.rowCount : 0,
    faTranDate: faAgg ? faAgg.tranDate : null,
    faTranType: faAgg ? faAgg.tranType : null,
    custodyTranDate: custodyAgg ? custodyAgg.tranDate : null,
    custodyTranType: custodyAgg ? custodyAgg.tranType : null,
    faSourceRows: faAgg ? faAgg.sourceRows : [],
    custodySourceRows: custodyAgg ? custodyAgg.sourceRows : [],
    rate: (custodyAgg || faAgg).rate,
    brokerage: (custodyAgg || faAgg).brokerage,
    stt: (custodyAgg || faAgg).stt,
    netAmount: (custodyAgg || faAgg).netAmount,
    settlementDate: (custodyAgg || faAgg).settlementDate,
    exchange: (custodyAgg || faAgg).exchange,
    brokerCode: (custodyAgg || faAgg).brokerCode,
  };
};

/**
 * Walk a slice of the key union and return the exceptions found in it.
 *
 * Sliced rather than all-at-once so S4 can checkpoint `comparedKeyCount` between
 * chunks and resume from it after a crash instead of restarting the comparison.
 *
 * @returns {{exceptions:object[], matchedCount:number}}
 */
export const compareKeyRange = (
  faMap,
  custodyMap,
  keys,
  start,
  end,
  { options = {}, schemeByActualCode = null } = {},
) => {
  const cfg = { ...DEFAULT_OPTIONS, ...options };
  const exceptions = [];
  let matchedCount = 0;

  for (let i = start; i < end && i < keys.length; i++) {
    const key = keys[i];
    const faAgg = faMap.get(key);
    const custodyAgg = custodyMap.get(key);

    if (faAgg && custodyAgg) {
      if (Math.abs(faAgg.quantity - custodyAgg.quantity) <= cfg.quantityEpsilon) {
        matchedCount += 1;
        continue;
      }
      exceptions.push(
        makeException({
          faAgg,
          custodyAgg,
          exceptionType: EXCEPTION_TYPE.QUANTITY_MISMATCH,
          schemeByActualCode,
        }),
      );
      continue;
    }

    if (faAgg) {
      exceptions.push(
        makeException({
          faAgg,
          custodyAgg: null,
          exceptionType: EXCEPTION_TYPE.MISSING_IN_CUSTODY,
          schemeByActualCode,
        }),
      );
      continue;
    }

    exceptions.push(
      makeException({
        faAgg: null,
        custodyAgg,
        exceptionType: EXCEPTION_TYPE.MISSING_IN_FA,
        schemeByActualCode,
      }),
    );
  }

  return { exceptions, matchedCount };
};

/**
 * Re-pair leftover orphans so one wrong date does not read as two unrelated breaks.
 *
 * A MISSING_IN_CUSTODY and a MISSING_IN_FA that agree on account code, ISIN and
 * quantity are the same trade with a disagreeing date or direction. Reported as
 * one DATE_MISMATCH / TYPE_MISMATCH row, which still hard-blocks the upload — it
 * just tells the truth about what is wrong.
 *
 * Only pairs when exactly one candidate exists on each side. With several, which
 * pairs with which is a guess, and a wrong guess is worse than two honest orphans.
 */
export const repairOrphans = (exceptions, { schemeByActualCode = null } = {}) => {
  const faOrphans = new Map();
  const custodyOrphans = new Map();

  for (const ex of exceptions) {
    if (ex.exceptionType === EXCEPTION_TYPE.MISSING_IN_CUSTODY) {
      const k = buildOrphanKey({
        actualCode: ex.actualCode,
        isin: ex.isin,
        quantity: ex.faQuantity,
      });
      if (!faOrphans.has(k)) faOrphans.set(k, []);
      faOrphans.get(k).push(ex);
    } else if (ex.exceptionType === EXCEPTION_TYPE.MISSING_IN_FA) {
      const k = buildOrphanKey({
        actualCode: ex.actualCode,
        isin: ex.isin,
        quantity: ex.custodyQuantity,
      });
      if (!custodyOrphans.has(k)) custodyOrphans.set(k, []);
      custodyOrphans.get(k).push(ex);
    }
  }

  const paired = new Set();
  const repaired = [];

  for (const [k, faList] of faOrphans) {
    const custodyList = custodyOrphans.get(k);
    if (!custodyList) continue;
    if (faList.length !== 1 || custodyList.length !== 1) continue;

    const fa = faList[0];
    const cu = custodyList[0];
    paired.add(fa);
    paired.add(cu);

    const dateDiffers = fa.tranDate !== cu.tranDate;
    const typeDiffers = fa.tranType !== cu.tranType;

    repaired.push({
      ...fa,
      exceptionType: dateDiffers
        ? EXCEPTION_TYPE.DATE_MISMATCH
        : EXCEPTION_TYPE.TYPE_MISMATCH,
      custodyQuantity: cu.custodyQuantity,
      difference: round(fa.faQuantity - cu.custodyQuantity),
      custodyRef: cu.custodyRef,
      scheme: cu.scheme || fa.scheme,
      custodyTranDate: cu.custodyTranDate,
      custodyTranType: cu.custodyTranType,
      custodyRowCount: cu.custodyRowCount,
      custodySourceRows: cu.custodySourceRows,
      // The key is now genuinely two keys; show the FA side and carry the custody
      // side in the custodyTran* fields the grid renders beside it.
      sortKey: fa.sortKey,
      repairedFrom: {
        fa: { tranDate: fa.tranDate, tranType: fa.tranType },
        custody: { tranDate: cu.tranDate, tranType: cu.tranType },
        dateDiffers,
        typeDiffers,
      },
    });
  }

  if (repaired.length === 0) return exceptions;

  const kept = exceptions.filter((ex) => !paired.has(ex));
  return [...kept, ...repaired];
};

/** Unresolvable FA codes become their own exception type rather than failing the run. */
export const unmappedToExceptions = (unmapped, { schemeByActualCode = null } = {}) =>
  unmapped.map((u) => ({
    actualCode: u.accountCode,
    isin: u.isin || "",
    tranDate: u.tranDate || "",
    tranType: u.tranType || "",
    virtualCodes: u.accountCode,
    scheme:
      schemeByActualCode && schemeByActualCode.has(u.accountCode)
        ? schemeByActualCode.get(u.accountCode)
        : "",
    faQuantity: round(u.quantity),
    custodyQuantity: 0,
    difference: round(u.quantity),
    exceptionType: EXCEPTION_TYPE.UNMAPPED_CODE,
    status: EXCEPTION_STATUS.OPEN,
    faRef: "",
    custodyRef: "",
    sortKey: `${u.accountCode}|${u.isin || ""}|${u.tranDate || ""}`,
    faRowCount: u.rowCount,
    custodyRowCount: 0,
    faTranDate: u.tranDate || null,
    faTranType: u.tranType || null,
    custodyTranDate: null,
    custodyTranType: null,
    faSourceRows: u.rows,
    custodySourceRows: [],
    rate: 0,
    brokerage: 0,
    stt: 0,
    netAmount: 0,
    settlementDate: null,
    exchange: "",
    brokerCode: "",
  }));

/**
 * Full comparison. The job function uses buildKeyUnion + compareKeyRange directly
 * so it can checkpoint; this is the one-shot form used by tests and small runs.
 *
 * @param {object[]} faRows      rows carrying actualCode (post resolveActualCodes)
 * @param {object[]} custodyRows canonical custodian rows
 * @param {object} [opts]
 * @param {object[]} [opts.unmapped] unresolved codes from resolveActualCodes
 * @param {Map<string,string>} [opts.schemeByActualCode]
 * @param {object} [opts.options]
 * @param {number} [opts.chunkSize] keys per checkpoint slice
 * @param {(done:number,total:number)=>void} [opts.onProgress]
 */
export const compareSides = (
  faRows,
  custodyRows,
  {
    unmapped = [],
    schemeByActualCode = null,
    options = {},
    chunkSize = 2000,
    onProgress = null,
  } = {},
) => {
  const cfg = { ...DEFAULT_OPTIONS, ...options };

  const faMap = aggregateByKey(faRows);
  const custodyMap = aggregateByKey(custodyRows);
  const keys = buildKeyUnion(faMap, custodyMap);

  let exceptions = [];
  let matchedKeyCount = 0;

  for (let start = 0; start < keys.length; start += chunkSize) {
    const end = Math.min(start + chunkSize, keys.length);
    const slice = compareKeyRange(faMap, custodyMap, keys, start, end, {
      options: cfg,
      schemeByActualCode,
    });
    exceptions.push(...slice.exceptions);
    matchedKeyCount += slice.matchedCount;
    if (onProgress) onProgress(end, keys.length);
  }

  if (cfg.repairOrphans) {
    exceptions = repairOrphans(exceptions, { schemeByActualCode });
  }

  if (cfg.unmappedAsException && unmapped.length > 0) {
    exceptions.push(...unmappedToExceptions(unmapped, { schemeByActualCode }));
  }

  exceptions.sort(
    (a, b) =>
      a.sortKey.localeCompare(b.sortKey) ||
      a.exceptionType.localeCompare(b.exceptionType),
  );

  const byType = {};
  for (const ex of exceptions) {
    byType[ex.exceptionType] = (byType[ex.exceptionType] || 0) + 1;
  }

  return {
    exceptions,
    summary: {
      faRowCount: faRows.length,
      custodyRowCount: custodyRows.length,
      faKeyCount: faMap.size,
      custodyKeyCount: custodyMap.size,
      totalKeyCount: keys.length,
      matchedKeyCount,
      exceptionCount: exceptions.length,
      unmappedCount: unmapped.length,
      byType,
      clean: exceptions.length === 0,
    },
  };
};

/*
 * The difference file mirrors the exception grid exactly — same columns, same
 * order, same headings. Somebody reading the export is looking at the same
 * report they were just looking at on screen, so the two must not drift.
 *
 * The reference detail an exception also carries (virtual codes, both refs,
 * rate/brokerage/STT, row counts) stays on the row and in the table; it is
 * simply not what this report is for.
 */
export const DIFFERENCE_CSV_COLUMNS = [
  "actualCode",
  "scheme",
  "isin",
  "tranDate",
  "tranType",
  "faQuantity",
  "custodyQuantity",
  "difference",
  "exceptionType",
  "status",
];

/** Headings, in the same order, worded as the grid words them. */
export const DIFFERENCE_CSV_HEADERS = [
  "Actual code",
  "Scheme",
  "ISIN",
  "Date",
  "Type",
  "FA qty",
  "Custody qty",
  "Difference",
  "Exception",
  "Status",
];

/** Render exceptions as CSV text for the stored difference file. */
export const exceptionsToCsv = (exceptions) => {
  const escape = (v) => {
    if (v === null || v === undefined) return "";
    const s = String(v);
    return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const lines = [DIFFERENCE_CSV_HEADERS.join(",")];
  for (const ex of exceptions) {
    lines.push(DIFFERENCE_CSV_COLUMNS.map((c) => escape(ex[c])).join(","));
  }
  return lines.join("\n");
};

