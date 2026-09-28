/**
 * Split / Bonus preview vs the custodian corporate actions file.
 *
 * Both sides are the client's holding ON THE RECORD DATE, before the action:
 * ours is the Split / Bonus preview's CURRENT HOLDING, the custodian's is its
 * "Holding Qty". Clients are matched on the actual account code, which is the
 * custodian's "Client UCC Code".
 */

export const STATUS = {
  MATCHED: "MATCHED",
  QTY_MISMATCH: "QTY_MISMATCH",
  MISSING_IN_CUSTODY: "MISSING_IN_CUSTODY",
  MISSING_IN_FA: "MISSING_IN_FA",
};

const CSV_HEADERS = [
  "ISIN",
  "ISIN_NAME",
  "ACCOUNT_CODE",
  "RECORD_DATE",
  "FA_HOLDING",
  "CUSTODIAN_HOLDING",
  "DIFFERENCE",
  "STATUS",
];

/** Quantities are compared at 4 decimal places so floating-point noise is not reported as a break. */
const round4 = (n) => Math.round(n * 10000) / 10000;

/** "1:2", "1 : 2" or "1/2" → 0.5; null when it is not two positive numbers. */
const ratioValue = (text) => {
  const m = /^\s*(\d+(?:\.\d+)?)\s*[:/]\s*(\d+(?:\.\d+)?)\s*$/.exec(String(text || ""));
  if (!m) return null;
  const propose = Number(m[1]);
  const held = Number(m[2]);
  return propose > 0 && held > 0 ? propose / held : null;
};

/**
 * Checks that apply to the whole event rather than to one client.
 *
 * The record date must match: otherwise the file is describing a different
 * event and every client would show as a break. The ratio does not affect the
 * holdings being compared, so a different ratio is only a warning — but it
 * would give clients the wrong shares on Apply, so it is shown.
 *
 * @param {object[]} custodyRows       from readLatestCorpActionRows
 * @param {string}   recordDateISO     entered on the page
 * @param {number}   pageRatio         the page's ratio as "propose / held"
 * @param {string}   pageRatioLabel    the page's ratio as shown to the user
 * @returns {{ error: string|null, warnings: string[] }}
 */
export const checkEvent = ({ custodyRows, recordDateISO, pageRatio, pageRatioLabel }) => {
  const custodyDates = [...new Set(custodyRows.map((r) => r.recordDate || "(blank or unreadable)"))];
  const otherDates = custodyDates.filter((d) => d !== recordDateISO);
  if (otherDates.length > 0) {
    return {
      error:
        `Record Date does not match. You entered ${recordDateISO}; ` +
        `the custodian file has ${custodyDates.join(", ")}.`,
      warnings: [],
    };
  }

  const warnings = [];
  for (const rawRatio of new Set(custodyRows.map((r) => r.ratio))) {
    const value = ratioValue(rawRatio);
    if (value === null) {
      warnings.push(`The custodian ratio "${rawRatio}" could not be read, so it was not checked.`);
    } else if (Math.abs(value - pageRatio) > 1e-9) {
      warnings.push(
        `Ratio differs: you entered ${pageRatioLabel}; the custodian file has ${rawRatio} (Propose : Held). ` +
          "Check it before applying.",
      );
    }
  }
  return { error: null, warnings };
};

/**
 * Per-client comparison.
 *
 * Our preview has one row per code, and one real account can hold shares under
 * several virtual codes, so rows are summed per actual code (the same grouping
 * the Split / Bonus preview table shows). The custodian can list one client more
 * than once (e.g. two DP accounts), so its rows are summed per UCC too.
 *
 * @param {object[]} previewRows  computeSplitPreviewRows / computeBonusPreviewRows
 * @param {object[]} custodyRows  readLatestCorpActionRows
 * @returns {{ rows: object[], counts: object }} every client, differences first
 */
export const compareHoldings = ({ previewRows, custodyRows, isin, isinName, recordDateISO }) => {
  const faByAccount = new Map();
  for (const row of previewRows) {
    const account = String(row.actualCode || row.accountCode || "").trim().toUpperCase();
    if (!account) continue;
    faByAccount.set(account, (faByAccount.get(account) || 0) + (Number(row.currentHolding) || 0));
  }

  const custodyByAccount = new Map();
  let custodyIsinName = "";
  for (const row of custodyRows) {
    if (!row.accountCode) continue;
    custodyByAccount.set(row.accountCode, (custodyByAccount.get(row.accountCode) || 0) + row.holdingQty);
    if (!custodyIsinName && row.isinName) custodyIsinName = row.isinName;
  }

  const counts = {
    total: 0,
    matched: 0,
    qtyMismatch: 0,
    missingInCustody: 0,
    missingInFa: 0,
  };
  const rows = [];

  for (const account of new Set([...faByAccount.keys(), ...custodyByAccount.keys()])) {
    const inFa = faByAccount.has(account);
    const inCustody = custodyByAccount.has(account);
    const faHolding = round4(faByAccount.get(account) || 0);
    const custodianHolding = round4(custodyByAccount.get(account) || 0);

    let status = STATUS.MATCHED;
    if (!inCustody) status = STATUS.MISSING_IN_CUSTODY;
    else if (!inFa) status = STATUS.MISSING_IN_FA;
    else if (faHolding !== custodianHolding) status = STATUS.QTY_MISMATCH;

    counts.total += 1;
    if (status === STATUS.MATCHED) counts.matched += 1;
    if (status === STATUS.QTY_MISMATCH) counts.qtyMismatch += 1;
    if (status === STATUS.MISSING_IN_CUSTODY) counts.missingInCustody += 1;
    if (status === STATUS.MISSING_IN_FA) counts.missingInFa += 1;

    rows.push({
      isin,
      isinName: custodyIsinName || isinName || "",
      accountCode: account,
      recordDate: recordDateISO,
      faHolding: inFa ? faHolding : null,
      custodianHolding: inCustody ? custodianHolding : null,
      difference: round4(Math.abs(faHolding - custodianHolding)),
      status,
    });
  }

  // Differences first (largest gap first), then matched clients by account code.
  rows.sort((a, b) => {
    const aMatched = a.status === STATUS.MATCHED;
    const bMatched = b.status === STATUS.MATCHED;
    if (aMatched !== bMatched) return aMatched ? 1 : -1;
    if (a.difference !== b.difference) return b.difference - a.difference;
    return a.accountCode.localeCompare(b.accountCode);
  });

  return { rows, counts };
};

const csvCell = (v) => {
  const s = v === null || v === undefined ? "" : String(v);
  return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
};

export const comparisonToCsv = (rows) => {
  const lines = [CSV_HEADERS.join(",")];
  for (const r of rows) {
    lines.push(
      [
        r.isin,
        r.isinName,
        r.accountCode,
        r.recordDate,
        r.faHolding,
        r.custodianHolding,
        r.difference,
        r.status,
      ]
        .map(csvCell)
        .join(","),
    );
  }
  return `${lines.join("\n")}\n`;
};
