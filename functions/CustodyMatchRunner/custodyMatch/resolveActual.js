/**
 * Custody Matching — virtual -> actual code resolution (pure).
 *
 * The daily FA file carries a mix of virtual and actual codes. The custodian
 * only ever sees broker-level accounts, so every FA row has to be rewritten onto
 * its Actual Code before the two sides describe the same thing.
 *
 * Why summing across virtual codes is correct, not a false-mismatch risk:
 * docs/MODULES.md:56-57 — one Actual Code splits into many Virtual Codes, each a
 * client-facing portfolio carved out of the same broker account, and each Virtual
 * Code belongs to exactly one Actual Code. The custodian never sees that split.
 * The Actual Code is the only level at which FA and custody are comparable.
 *
 * This module takes an already-built Map and does no I/O, so it stays testable
 * offline. Building the map is the caller's job — and should be done ONCE for the
 * whole file (see buildVirtualToActualMap in util/mapVirtualToActualCodes.js),
 * never per row.
 */

import { DEFAULT_OPTIONS, EXCEPTION_TYPE } from "./constants.js";
import { normCode } from "./normalize.js";

/**
 * Rewrite FA rows onto their actual codes.
 *
 * A code already present as an actual code, or absent from the map entirely, is
 * treated as already-actual only when `treatUnknownAsActual` says so; otherwise
 * it is reported. Codes with no resolution become UNMAPPED_CODE exceptions
 * rather than aborting the run — docs/MODULES.md:318,362 record that unresolvable
 * codes occur routinely today, so a hard stop on the first one is the wrong
 * response to a normal condition.
 *
 * @param {object[]} rows      FA rows from parseFaFile
 * @param {Map<string,string>} virtualToActual  virtual code -> actual code
 * @param {object} [opts]
 * @param {Set<string>} [opts.knownActualCodes] codes known to be actual already
 * @param {object} [opts.options] overrides for DEFAULT_OPTIONS
 * @returns {{rows:object[], unmapped:object[], resolvedCount:number, passthroughCount:number}}
 * @throws when unmappedAsException is off and a code cannot be resolved
 */
export const resolveActualCodes = (
  rows,
  virtualToActual,
  { knownActualCodes = null, options = {} } = {},
) => {
  const cfg = { ...DEFAULT_OPTIONS, ...options };
  const map = virtualToActual instanceof Map ? virtualToActual : new Map();

  const resolved = [];
  const unmappedByCode = new Map();
  // actual code -> the virtual codes that rolled into it. Carried forward so an
  // exception can name the code as it appears in the user's own file.
  const actualToVirtual = new Map();
  let resolvedCount = 0;
  let passthroughCount = 0;

  for (const row of rows) {
    const code = normCode(row.accountCode);
    const mapped = map.get(code);

    if (mapped) {
      const actual = normCode(mapped);
      resolvedCount += 1;
      if (!actualToVirtual.has(actual)) actualToVirtual.set(actual, new Set());
      actualToVirtual.get(actual).add(code);
      resolved.push({ ...row, actualCode: actual, virtualCode: code });
      continue;
    }

    // Already an actual code: it appears as a value in the map, or the caller
    // supplied the set of known actual codes.
    const isAlreadyActual = knownActualCodes
      ? knownActualCodes.has(code)
      : false;

    if (isAlreadyActual) {
      passthroughCount += 1;
      resolved.push({ ...row, actualCode: code, virtualCode: null });
      continue;
    }

    if (!cfg.unmappedAsException) {
      throw Object.assign(
        new Error(
          `Account code "${code}" (row ${row.rowNumber}) has no entry in clientIds and could not be resolved to an actual code.`,
        ),
        { code: EXCEPTION_TYPE.UNMAPPED_CODE, accountCode: code, row: row.rowNumber },
      );
    }

    const existing = unmappedByCode.get(code);
    if (existing) {
      existing.rowCount += 1;
      existing.quantity += row.quantity;
      if (existing.rows.length < 20) existing.rows.push(row.rowNumber);
    } else {
      unmappedByCode.set(code, {
        accountCode: code,
        rowCount: 1,
        quantity: row.quantity,
        isin: row.isin,
        tranDate: row.tranDate,
        tranType: row.tranType,
        rows: [row.rowNumber],
      });
    }
  }

  return {
    rows: resolved,
    unmapped: [...unmappedByCode.values()],
    // Plain object of arrays, so it survives JSON.stringify into the S1 sidecar.
    actualToVirtual: Object.fromEntries(
      [...actualToVirtual.entries()].map(([actual, set]) => [actual, [...set]]),
    ),
    resolvedCount,
    passthroughCount,
  };
};

/**
 * The set of actual codes a virtual->actual map knows about.
 * Lets resolveActualCodes recognise a row that is already keyed on an actual code
 * without a second lookup.
 */
export const actualCodesOf = (virtualToActual) => {
  const set = new Set();
  if (!(virtualToActual instanceof Map)) return set;
  for (const actual of virtualToActual.values()) {
    const code = normCode(actual);
    if (code) set.add(code);
  }
  return set;
};
