/**
 * Split / Bonus preview vs the custodian corporate actions files.
 *
 * Entitlement comparison (Split stage 1, Bonus): both sides are the client's
 * holding ON THE RECORD DATE, before the action — ours is the preview's CURRENT
 * HOLDING, the custodian's is its "Holding Qty". Split also compares the CA
 * quantity: the total shares after the split (96 held on 1:2 → CA Qty 192).
 *
 * Receipt comparison (Split stage 2): the CA quantity of every account marked
 * Receivable vs the "Received Quantity" in the Corporate Benefit Collection file.
 *
 * Clients are matched on the actual account code, which is the custodian's
 * "Client UCC Code".
 */

export const STATUS = {
  MATCHED: "MATCHED",
  QTY_MISMATCH: "QTY_MISMATCH",
  MISSING_IN_CUSTODY: "MISSING_IN_CUSTODY",
  MISSING_IN_FA: "MISSING_IN_FA",
};

export const RECEIPT_STATUS = {
  RECEIVED: "RECEIVED",
  QTY_MISMATCH: "QTY_MISMATCH",
  NOT_IN_BENEFIT_FILE: "NOT_IN_BENEFIT_FILE",
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

const CA_QTY_CSV_HEADERS = [
  "ISIN",
  "ISIN_NAME",
  "ACCOUNT_CODE",
  "RECORD_DATE",
  "FA_HOLDING",
  "CUSTODIAN_HOLDING",
  "DIFFERENCE",
  "FA_CA_QTY",
  "CUSTODIAN_CA_QTY",
  "CA_QTY_DIFF",
  "STATUS",
];

const RECEIPT_CSV_HEADERS = [
  "ISIN",
  "ISIN_NAME",
  "ACCOUNT_CODE",
  "CA_REF_NO",
  "RECORD_DATE",
  "FA_CA_QTY",
  "CUSTODIAN_CA_QTY",
  "RECEIVED_QTY",
  "BALANCE_QTY",
  "RECEIVED_DATE",
  "DIFFERENCE",
  "CUSTODIAN_EVENT_STATUS",
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
 * The rows are already for the entered record date (custodianTables.js reads
 * one event). The ratio does not affect the holdings being compared, so a
 * different ratio is only a warning — but it would give clients the wrong
 * shares on Apply, so it is shown.
 *
 * @param {object[]} custodyRows       from readLatestCorpActionRows
 * @param {number}   pageRatio         the page's ratio as "propose / held"
 * @param {string}   pageRatioLabel    the page's ratio as shown to the user
 * @returns {{ warnings: string[] }}
 */
export const checkEvent = ({ custodyRows, pageRatio, pageRatioLabel }) => {
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
  return { warnings };
};

/** Differences first (largest gap first), then matched clients by account code. */
const sortDifferencesFirst = (rows, matchedStatus) =>
  rows.sort((a, b) => {
    const aMatched = a.status === matchedStatus;
    const bMatched = b.status === matchedStatus;
    if (aMatched !== bMatched) return aMatched ? 1 : -1;
    const aGap = Math.max(a.difference, a.caQtyDifference || 0);
    const bGap = Math.max(b.difference, b.caQtyDifference || 0);
    if (aGap !== bGap) return bGap - aGap;
    return a.accountCode.localeCompare(b.accountCode);
  });

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
 * @param {boolean}  withCaQty    Split only: also compare our newHolding with the custodian's CA Qty
 * @returns {{ rows: object[], counts: object }} every client, differences first
 */
export const compareHoldings = ({ previewRows, custodyRows, isin, isinName, recordDateISO, withCaQty = false }) => {
  const faByAccount = new Map(); // account → { holding, caQty }
  for (const row of previewRows) {
    const account = String(row.actualCode || row.accountCode || "").trim().toUpperCase();
    if (!account) continue;
    const totals = faByAccount.get(account) || { holding: 0, caQty: 0 };
    totals.holding += Number(row.currentHolding) || 0;
    totals.caQty += Number(row.newHolding) || 0;
    faByAccount.set(account, totals);
  }

  const custodyByAccount = new Map(); // account → { holding, caQty }
  let custodyIsinName = "";
  for (const row of custodyRows) {
    if (!row.accountCode) continue;
    const totals = custodyByAccount.get(row.accountCode) || { holding: 0, caQty: 0 };
    totals.holding += row.holdingQty;
    totals.caQty += row.caQty || 0;
    custodyByAccount.set(row.accountCode, totals);
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
    const fa = faByAccount.get(account) || { holding: 0, caQty: 0 };
    const custody = custodyByAccount.get(account) || { holding: 0, caQty: 0 };
    const faHolding = round4(fa.holding);
    const custodianHolding = round4(custody.holding);
    const faCaQty = round4(fa.caQty);
    const custodianCaQty = round4(custody.caQty);

    let status = STATUS.MATCHED;
    if (!inCustody) status = STATUS.MISSING_IN_CUSTODY;
    else if (!inFa) status = STATUS.MISSING_IN_FA;
    else if (faHolding !== custodianHolding) status = STATUS.QTY_MISMATCH;
    else if (withCaQty && faCaQty !== custodianCaQty) status = STATUS.QTY_MISMATCH;

    counts.total += 1;
    if (status === STATUS.MATCHED) counts.matched += 1;
    if (status === STATUS.QTY_MISMATCH) counts.qtyMismatch += 1;
    if (status === STATUS.MISSING_IN_CUSTODY) counts.missingInCustody += 1;
    if (status === STATUS.MISSING_IN_FA) counts.missingInFa += 1;

    const row = {
      isin,
      isinName: custodyIsinName || isinName || "",
      accountCode: account,
      recordDate: recordDateISO,
      faHolding: inFa ? faHolding : null,
      custodianHolding: inCustody ? custodianHolding : null,
      difference: round4(Math.abs(faHolding - custodianHolding)),
      status,
    };
    if (withCaQty) {
      row.faCaQty = inFa ? faCaQty : null;
      row.custodianCaQty = inCustody ? custodianCaQty : null;
      row.caQtyDifference = round4(Math.abs(faCaQty - custodianCaQty));
    }
    rows.push(row);
  }

  sortDifferencesFirst(rows, STATUS.MATCHED);
  return { rows, counts };
};

/**
 * Split stage 2: every account marked Receivable vs the Corporate Benefit
 * Collection file.
 *
 * Our CA qty (Split_Account.New_Holding, summed per actual code) is compared
 * with the custodian's "Received Quantity". CA Qty, Balance Qty and Received
 * Date are carried along for the reader.
 *
 * @param {Array<{ accountCode: string, newHolding: number }>} receivableRows
 *        one per virtual code; accountCode is already the ACTUAL code
 * @param {Array<{ accountCode, isinName, caRefNo, caQty, receivedQty, balanceQty, receivedDate, eventStatus }>} allocationRows
 * @returns {{ rows: object[], counts: object }} every client, differences first
 */
export const compareReceipts = ({ receivableRows, allocationRows, isin, isinName, recordDateISO }) => {
  const faByAccount = new Map(); // actual code → CA qty
  for (const row of receivableRows) {
    const account = String(row.accountCode || "").trim().toUpperCase();
    if (!account) continue;
    faByAccount.set(account, (faByAccount.get(account) || 0) + (Number(row.newHolding) || 0));
  }

  // The custodian can list one client more than once (e.g. two DP accounts).
  const custodyByAccount = new Map();
  let custodyIsinName = "";
  for (const row of allocationRows) {
    if (!row.accountCode) continue;
    const totals = custodyByAccount.get(row.accountCode) || {
      caQty: 0,
      receivedQty: 0,
      balanceQty: 0,
      receivedDate: "",
      caRefNos: new Set(),
      eventStatuses: new Set(),
    };
    totals.caQty += row.caQty;
    totals.receivedQty += row.receivedQty;
    totals.balanceQty += row.balanceQty;
    // The latest date is when the last of the shares arrived.
    if (row.receivedDate && row.receivedDate > totals.receivedDate) totals.receivedDate = row.receivedDate;
    if (row.caRefNo) totals.caRefNos.add(row.caRefNo);
    if (row.eventStatus) totals.eventStatuses.add(row.eventStatus);
    custodyByAccount.set(row.accountCode, totals);
    if (!custodyIsinName && row.isinName) custodyIsinName = row.isinName;
  }

  const counts = {
    total: 0,
    received: 0,
    qtyMismatch: 0,
    notInBenefitFile: 0,
    missingInFa: 0,
  };
  const rows = [];

  for (const account of new Set([...faByAccount.keys(), ...custodyByAccount.keys()])) {
    const inFa = faByAccount.has(account);
    const custody = custodyByAccount.get(account);
    const faCaQty = round4(faByAccount.get(account) || 0);
    const receivedQty = round4(custody?.receivedQty || 0);

    let status = RECEIPT_STATUS.RECEIVED;
    if (!custody) status = RECEIPT_STATUS.NOT_IN_BENEFIT_FILE;
    else if (!inFa) status = RECEIPT_STATUS.MISSING_IN_FA;
    else if (faCaQty !== receivedQty) status = RECEIPT_STATUS.QTY_MISMATCH;

    counts.total += 1;
    if (status === RECEIPT_STATUS.RECEIVED) counts.received += 1;
    if (status === RECEIPT_STATUS.QTY_MISMATCH) counts.qtyMismatch += 1;
    if (status === RECEIPT_STATUS.NOT_IN_BENEFIT_FILE) counts.notInBenefitFile += 1;
    if (status === RECEIPT_STATUS.MISSING_IN_FA) counts.missingInFa += 1;

    rows.push({
      isin,
      isinName: custodyIsinName || isinName || "",
      accountCode: account,
      caRefNo: custody ? [...custody.caRefNos].join(" ") : "",
      recordDate: recordDateISO,
      faCaQty: inFa ? faCaQty : null,
      custodianCaQty: custody ? round4(custody.caQty) : null,
      receivedQty: custody ? receivedQty : null,
      balanceQty: custody ? round4(custody.balanceQty) : null,
      receivedDate: custody?.receivedDate || "",
      difference: round4(Math.abs(faCaQty - receivedQty)),
      eventStatus: custody ? [...custody.eventStatuses].join(" ") : "",
      status,
    });
  }

  sortDifferencesFirst(rows, RECEIPT_STATUS.RECEIVED);
  return { rows, counts };
};

const csvCell = (v) => {
  const s = v === null || v === undefined ? "" : String(v);
  return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
};

const toCsv = (headers, rows, cellsOf) => {
  const lines = [headers.join(",")];
  for (const r of rows) lines.push(cellsOf(r).map(csvCell).join(","));
  return `${lines.join("\n")}\n`;
};

export const comparisonToCsv = (rows, { withCaQty = false } = {}) =>
  withCaQty
    ? toCsv(CA_QTY_CSV_HEADERS, rows, (r) => [
        r.isin,
        r.isinName,
        r.accountCode,
        r.recordDate,
        r.faHolding,
        r.custodianHolding,
        r.difference,
        r.faCaQty,
        r.custodianCaQty,
        r.caQtyDifference,
        r.status,
      ])
    : toCsv(CSV_HEADERS, rows, (r) => [
        r.isin,
        r.isinName,
        r.accountCode,
        r.recordDate,
        r.faHolding,
        r.custodianHolding,
        r.difference,
        r.status,
      ]);

export const receiptsToCsv = (rows) =>
  toCsv(RECEIPT_CSV_HEADERS, rows, (r) => [
    r.isin,
    r.isinName,
    r.accountCode,
    r.caRefNo,
    r.recordDate,
    r.faCaQty,
    r.custodianCaQty,
    r.receivedQty,
    r.balanceQty,
    r.receivedDate,
    r.difference,
    r.eventStatus,
    r.status,
  ]);
