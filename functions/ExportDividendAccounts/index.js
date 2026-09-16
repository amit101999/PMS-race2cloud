const { Readable } = require("stream");
const catalyst = require("zcatalyst-sdk-node");
const {
  QTY_EPS,
  holdingsEffectiveDate,
  recomputeHoldingsSnapshot,
  nextISODate,
} = require("./holdingsSnapshot.js");

const BATCH_SIZE = 270;

const esc = (s) => String(s ?? "").replace(/'/g, "''");

/**
 * Every Holdings row for ONE ISIN that is effective on or before the record
 * date, grouped by account in FIFO insert order.
 *
 * PORT of `fetchHoldingsRowsByAccountForIsin` in
 * appsail-nodejs/util/analytics/holdingsFromTable.js, which the dividend
 * PREVIEW reads, and identical to the copy in functions/UpdateDividendData —
 * keep all three in sync so preview, export and apply see the same rows.
 * Keyset (`ROWID > cursor`) rather than OFFSET so each page doesn't re-scan the
 * whole ISIN.
 */
async function fetchHoldingsRowsByAccountForIsin(zcql, isin, recordDateISO) {
  const cutoff = nextISODate(recordDateISO);
  const byAccount = new Map();
  let lastRowId = "0";

  while (true) {
    const batch = await zcql.executeZCQLQuery(`
      SELECT ROWID, WS_Account_code, TRANSACTION_DATE, SETTLEMENT_DATE, TYPE,
             QUANTITY, PRICE, TOTAL_AMOUNT, HOLDING, WAP, HOLDING_VALUE,
             CA_DATE, CREATEDTIME
      FROM Holdings
      WHERE ISIN = '${esc(isin)}'
        AND ROWID > ${lastRowId}
        AND (SETTLEMENT_DATE < '${cutoff}' OR TRANSACTION_DATE < '${cutoff}')
      ORDER BY ROWID ASC
      LIMIT ${BATCH_SIZE}
    `);
    if (!batch || batch.length === 0) break;

    let maxRowId = lastRowId;
    for (const r of batch) {
      const h = r.Holdings || r;
      const rowId = String(h.ROWID ?? "");
      // Advance the cursor over every row, including ones dropped below —
      // filtering first could stall it and loop forever.
      if (rowId && (rowId.length > maxRowId.length || (rowId.length === maxRowId.length && rowId > maxRowId))) {
        maxRowId = rowId;
      }
      const code = String(h.WS_Account_code ?? "").trim();
      if (!code) continue;

      // The SQL ORs both dates — it can't express the OPI exception or the
      // merger/demerger CA_DATE rule. Apply the real per-type rule here.
      const eff = holdingsEffectiveDate(h);
      if (eff && eff >= cutoff) continue;

      if (!byAccount.has(code)) byAccount.set(code, []);
      byAccount.get(code).push(h);
    }

    // Guard against a non-advancing cursor (non-numeric / duplicate ROWIDs).
    if (maxRowId === lastRowId) break;
    lastRowId = maxRowId;
    if (batch.length < BATCH_SIZE) break;
  }

  // Rows arrive in global ROWID order; restore CREATEDTIME, ROWID order per account.
  for (const rows of byAccount.values()) {
    rows.sort((a, b) => {
      const ta = String(a.CREATEDTIME ?? "");
      const tb = String(b.CREATEDTIME ?? "");
      if (ta !== tb) return ta < tb ? -1 : 1;
      const ra = String(a.ROWID ?? "0");
      const rb = String(b.ROWID ?? "0");
      if (ra.length !== rb.length) return ra.length - rb.length;
      return ra < rb ? -1 : ra > rb ? 1 : 0;
    });
  }

  return byAccount;
}

/**
 * @param {import("./types/job").JobRequest} jobRequest
 * @param {import("./types/job").Context} context
 */
module.exports = async (jobRequest, context) => {
  const catalystApp = catalyst.initialize(context);
  const zcql = catalystApp.zcql();
  let jobName = "";

  try {
    console.log("[ExportDividend] Job started");

    const jobDetails = jobRequest.getAllJobParams();
    const { isin, exDate, recordDate, rate, paymentDate, fileName } = jobDetails;
    jobName = jobDetails.jobName;
    const rateNum = Number(rate);

    await zcql.executeZCQLQuery(
      `INSERT INTO Jobs (jobName, status) VALUES ('${jobName}', 'PENDING')`
    );

    if (!isin || !recordDate || !paymentDate || !Number.isFinite(rateNum) || rateNum <= 0) {
      console.error("[ExportDividend] Invalid params:", { isin, recordDate, rate, paymentDate });
      await zcql.executeZCQLQuery(
        `UPDATE Jobs SET status = 'FAILED' WHERE jobName = '${jobName}'`
      );
      context.closeWithFailure();
      return;
    }

    const recordDateObj = new Date(recordDate);
    recordDateObj.setUTCHours(0, 0, 0, 0);
    const recordDateISO = recordDateObj.toISOString().split("T")[0];

    const exDateForDisplay =
      exDate && String(exDate).trim()
        ? (() => {
            const d = new Date(exDate);
            d.setUTCHours(0, 0, 0, 0);
            return d.toISOString().split("T")[0];
          })()
        : recordDateISO;

    const paymentDateObj = new Date(paymentDate);
    paymentDateObj.setUTCHours(0, 0, 0, 0);
    const paymentDateISO = paymentDateObj.toISOString().split("T")[0];

    const stratus = catalystApp.stratus();
    const bucket = stratus.bucket("export-app-data");

    /* ================= STEP 1: READ THE HOLDINGS LEDGER AS AT THE RECORD DATE =================
       Same rows, same date rule and same position maths as the dividend
       PREVIEW, so this CSV matches what the Dividend page shows. One read gives
       both the eligible accounts and the rows to size each one from.

       ===== OLD LOGIC — raw-table replay. Kept for rollback (fifo.js is in git
       history, deleted with this change). =====
       Accounts came from `SELECT WS_Account_code FROM Transaction`, then the
       Transaction / Bonus / Split rows ≤ RecordDate were replayed through
       `runFifoEngine(tx, bonuses, splits, true)`. That engine was never given
       merger or demerger inputs, so shares that arrived through a corporate
       action counted as zero here while the preview counted them.
       ===== END OLD LOGIC ===== */
    const rowsByAccount = await fetchHoldingsRowsByAccountForIsin(zcql, isin, recordDateISO);

    if (!rowsByAccount.size) {
      console.log("[ExportDividend] No accounts with Holdings rows for ISIN:", isin);
      await zcql.executeZCQLQuery(
        `UPDATE Jobs SET status = 'COMPLETED' WHERE jobName = '${jobName}'`
      );
      context.closeWithSuccess();
      return;
    }

    console.log(`[ExportDividend] Found ${rowsByAccount.size} accounts for ISIN: ${isin}`);

    /* ================= STEP 2: HOLDING + BUILD CSV ================= */
    const UTF8_BOM = "\uFEFF";
    const csvLines = [];
    csvLines.push(
      UTF8_BOM +
        "ISIN,ACCOUNT_CODE,HOLDING_AS_ON_RECORD_DATE,RATE,EX_DATE,PAYMENT_DATE,DIVIDEND_AMOUNT\n"
    );

    let eligibleCount = 0;

    for (const [accountCode, holdingRows] of rowsByAccount) {
      // Re-derive rather than read the last row's stored HOLDING — that running
      // balance can be stale for the trade-date rule. Identical call to the preview's.
      const holdingAsOnRecordDate = recomputeHoldingsSnapshot(holdingRows).HOLDING;
      if (holdingAsOnRecordDate <= QTY_EPS) continue;

      const dividendAmount = holdingAsOnRecordDate * rateNum;
      const exDateValue = exDateForDisplay || "";

      const line = [
        isin,
        accountCode,
        holdingAsOnRecordDate,
        rateNum,
        exDateValue,
        paymentDateISO,
        dividendAmount,
      ]
        .map((v) => `"${String(v).replace(/"/g, '""')}"`)
        .join(",");

      csvLines.push(line + "\n");
      eligibleCount++;
    }

    console.log(
      `[ExportDividend] ${rowsByAccount.size} accounts checked, ${eligibleCount} eligible for dividend`
    );

    /* ================= UPLOAD COMPLETE CSV ================= */
    const csvContent = csvLines.join("");
    const readableStream = Readable.from([csvContent]);

    console.log(
      `[ExportDividend] Uploading CSV (${csvLines.length} lines, ~${Math.round(
        csvContent.length / 1024
      )} KB)`
    );

    await bucket.putObject(fileName, readableStream, {
      overwrite: true,
      contentType: "text/csv",
    });

    console.log("[ExportDividend] Job completed successfully");

    try {
      await zcql.executeZCQLQuery(
        `UPDATE Jobs SET status = 'COMPLETED' WHERE jobName = '${jobName}'`
      );
    } catch (statusErr) {
      console.error(
        "[ExportDividend] Failed to mark job as COMPLETED (file was uploaded):",
        statusErr
      );
    }

    context.closeWithSuccess();
  } catch (error) {
    console.error("[ExportDividend] Job failed:", error);

    try {
      await zcql.executeZCQLQuery(
        `UPDATE Jobs SET status = 'FAILED' WHERE jobName = '${jobName}'`
      );
    } catch (updateErr) {
      console.error("[ExportDividend] Failed to update job status to FAILED:", updateErr);
    }

    context.closeWithFailure();
  }
};
