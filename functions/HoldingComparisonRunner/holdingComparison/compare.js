import { DIFFERENCE_HEADERS, EXCEPTION_TYPE } from "./constants.js";

/** Quantities are compared at 4 decimal places so floating-point noise is not reported as a break. */
const round4 = (n) => Math.round(n * 10000) / 10000;

/**
 * Compare FA holdings with custodian holdings, key = account code + ISIN.
 *
 * Only quantity is compared. A key missing on one side counts as 0 there, and a
 * zero against a missing row is not a difference: an FA position that has been
 * fully sold still appears in the export with HOLDING 0, and the custodian simply
 * does not list it.
 *
 * @param {Map<string, object>} faHoldings      from parseFaHoldings
 * @param {Map<string, object>} custodyHoldings from parseCustodyHoldings
 * @returns {{ exceptions: object[], totalKeyCount: number, counts: object }}
 */
export const compareHoldings = (faHoldings, custodyHoldings) => {
  const keys = [...new Set([...faHoldings.keys(), ...custodyHoldings.keys()])].sort();
  const exceptions = [];

  for (const key of keys) {
    const fa = faHoldings.get(key);
    const custody = custodyHoldings.get(key);
    const faQty = round4(fa?.quantity ?? 0);
    const custodyQty = round4(custody?.quantity ?? 0);

    if (faQty === custodyQty) continue;

    let exceptionType = EXCEPTION_TYPE.QUANTITY_MISMATCH;
    if (!custody) exceptionType = EXCEPTION_TYPE.MISSING_IN_CUSTODY;
    else if (!fa) exceptionType = EXCEPTION_TYPE.MISSING_IN_FA;

    const either = custody || fa;
    exceptions.push({
      accountCode: either.accountCode,
      isin: either.isin,
      securityName: custody?.securityName || fa?.securityName || "",
      // The custodian's date when it has one, otherwise the FA as-on date.
      asOfDate: custody?.asOfDate || fa?.asOfDate || "",
      faQuantity: fa ? faQty : null,
      custodyQuantity: custody ? custodyQty : null,
      // The size of the break only; EXCEPTION_TYPE and the two quantities show its direction.
      difference: round4(Math.abs(faQty - custodyQty)),
      exceptionType,
    });
  }

  const counts = {
    quantityMismatchCount: 0,
    missingInCustodyCount: 0,
    missingInFaCount: 0,
  };
  for (const e of exceptions) {
    if (e.exceptionType === EXCEPTION_TYPE.QUANTITY_MISMATCH) counts.quantityMismatchCount += 1;
    else if (e.exceptionType === EXCEPTION_TYPE.MISSING_IN_CUSTODY) counts.missingInCustodyCount += 1;
    else counts.missingInFaCount += 1;
  }

  return { exceptions, totalKeyCount: keys.length, counts };
};

const csvCell = (v) => {
  const s = v === null || v === undefined ? "" : String(v);
  return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
};

export const exceptionsToCsv = (exceptions) => {
  const lines = [DIFFERENCE_HEADERS.join(",")];
  for (const e of exceptions) {
    lines.push(
      [
        e.accountCode,
        e.isin,
        e.securityName,
        e.asOfDate,
        e.faQuantity,
        e.custodyQuantity,
        e.difference,
        e.exceptionType,
      ]
        .map(csvCell)
        .join(","),
    );
  }
  return `${lines.join("\n")}\n`;
};
