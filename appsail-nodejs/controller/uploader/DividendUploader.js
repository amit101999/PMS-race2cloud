import {
  fetchHoldingsRowsByAccountForIsin,
  recomputeHoldingsSnapshot,
} from "../../util/analytics/holdingsFromTable.js";
import { readLatestCorpActionRows, readLatestReceiptRows } from "../corporateAction/CustodianUploadController.js";
import { buildVirtualToActualMap } from "../../util/mapVirtualToActualCodes.js";
import {
  DIVIDEND_STATUS,
  dividendAccountWhere,
  dividendStage,
  isDividendApplied,
  readDividendAccountRows,
} from "../../util/analytics/dividendAccountRows.js";

const ACCOUNT_LOOKUP_BATCH = 50;
const ACCOUNT_ROW_CHUNK = 200;

/**
 * Normalise an incoming date to `YYYY-MM-DD` without crossing a timezone
 * boundary (same helper as the Split / Bonus controllers).
 */
const toISODateOnly = (v) => {
  const s = String(v ?? "").trim();
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (m) return `${m[1]}-${m[2]}-${m[3]}`;
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return "";
  const p = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`;
};

const parseCatalystTime = (ct) => {
  if (!ct) return 0;
  const fixed = ct.replace(/:(\d{3})$/, ".$1");
  const ms = new Date(fixed).getTime();
  return isNaN(ms) ? 0 : ms;
};

const escSql = (s) => String(s ?? "").replace(/'/g, "''");

/**
 * Round to 2 decimals.
 */
const round2 = (n) => Math.round((Number(n) || 0) * 100) / 100;

/**
 * Read all rows from Dividend_Record for (ISIN, RecordDate) restricted to a
 * given list of account codes. Batches the IN-list to keep the ZCQL query
 * size sane.
 *
 * Returns Map<accountCode, { gross, tds, lastPaidOn, count }>.
 */
const lookupAlreadyReceived = async (
  zcql,
  isin,
  recordDateISO,
  accountCodes,
) => {
  const out = new Map();
  if (!accountCodes.length) return out;

  for (let i = 0; i < accountCodes.length; i += ACCOUNT_LOOKUP_BATCH) {
    const slice = accountCodes.slice(i, i + ACCOUNT_LOOKUP_BATCH);
    const inList = slice.map((a) => `'${escSql(a)}'`).join(",");
    let rows = [];
    try {
      // A RECEIVABLE row has not been paid yet, so it is not "already received".
      rows = await zcql.executeZCQLQuery(`
        SELECT WS_Account_code, Dividend_Amount, PaymentDate
        FROM Dividend_Record
        WHERE ISIN='${escSql(isin)}'
          AND RecordDate='${recordDateISO}'
          AND WS_Account_code IN (${inList})
          AND (Status IS NULL OR Status <> '${DIVIDEND_STATUS.RECEIVABLE}')
      `);
    } catch (err) {
      console.error("lookupAlreadyReceived batch failed:", err.message);
      continue;
    }
    for (const r of rows || []) {
      const d = r.Dividend_Record || r;
      const acc = d.WS_Account_code;
      if (!acc) continue;
      const cur = out.get(acc) || { gross: 0, lastPaidOn: null, count: 0 };
      cur.gross = round2(cur.gross + (Number(d.Dividend_Amount) || 0));
      cur.count += 1;
      const pd = d.PaymentDate;
      if (pd && (!cur.lastPaidOn || String(pd) > String(cur.lastPaidOn))) {
        cur.lastPaidOn = pd;
      }
      out.set(acc, cur);
    }
  }
  return out;
};

/**
 * Check which accounts already have a DIVIDEND row in
 * Cash_Balance_Per_Transaction for (ISIN, RecordDate). Returns Set<accountCode>.
 */
const lookupCashCredited = async (
  zcql,
  isin,
  recordDateISO,
  accountCodes,
) => {
  const out = new Set();
  if (!accountCodes.length) return out;

  for (let i = 0; i < accountCodes.length; i += ACCOUNT_LOOKUP_BATCH) {
    const slice = accountCodes.slice(i, i + ACCOUNT_LOOKUP_BATCH);
    const inList = slice.map((a) => `'${escSql(a)}'`).join(",");
    let rows = [];
    try {
      rows = await zcql.executeZCQLQuery(`
        SELECT Account_Code
        FROM Cash_Balance_Per_Transaction
        WHERE ISIN='${escSql(isin)}'
          AND Transaction_Date='${recordDateISO}'
          AND Transaction_Type='DIVIDEND'
          AND Account_Code IN (${inList})
      `);
    } catch (err) {
      console.error("lookupCashCredited batch failed:", err.message);
      continue;
    }
    for (const r of rows || []) {
      const c = r.Cash_Balance_Per_Transaction || r;
      if (c.Account_Code) out.add(c.Account_Code);
    }
  }
  return out;
};

/**
 * Decide a per-row status badge for the reconciliation grid.
 *
 * Buckets:
 *   already_paid       — already received gross >= file gross AND cash row exists
 *   overpaid           — already received gross > file gross
 *   partial            — 0 < already received gross < file gross
 *   missing_in_system  — in file but FIFO never saw the account
 *   missing_in_file    — FIFO has holding but file row absent
 *   mismatch           — both sides present but holding or gross differs
 *   ready              — both sides match, nothing already paid
 */
const computeStatus = ({
  inFile,
  inSystem,
  holdingDelta,
  grossDelta,
  alreadyReceivedGross,
  fileGross,
  alreadyCreditedCash,
}) => {
  if (!inFile && inSystem) return "missing_in_file";
  if (inFile && !inSystem) return "missing_in_system";

  if (inFile && alreadyReceivedGross > 0) {
    if (alreadyReceivedGross > fileGross + 0.005) return "overpaid";
    if (alreadyReceivedGross + 0.005 < fileGross) return "partial";
    if (alreadyCreditedCash) return "already_paid";
    return "already_paid";
  }

  if (Math.abs(holdingDelta) > 0.0001 || Math.abs(grossDelta) > 0.005) {
    return "mismatch";
  }
  return "ready";
};

/**
 * Each account's dividend on the record date: holding × rate, read from the
 * materialised Holdings ledger (the same rows and maths UpdateDividendData
 * uses, so the amount sent as Receivable is the amount later credited).
 *
 * One bulk keyset read for the whole ISIN instead of a query per account:
 * the per-account loop overran the deployed AppSail request budget for a
 * widely-held ISIN. Accounts come from Holdings, not Transaction, so clients
 * whose shares all arrived through a merger / demerger are included.
 *
 * @returns {Promise<Array<{ accountCode, holding, amount }>>}
 */
const computeDividendSystemRows = async (zcql, { isin, recordDateISO, rate }) => {
  const rowsByAccount = await fetchHoldingsRowsByAccountForIsin(zcql, isin, recordDateISO);
  const rows = [];
  for (const [accountCode, holdingRows] of rowsByAccount) {
    /* Re-derive rather than reading the last row's stored HOLDING. That
       running balance was frozen in settlement order at rebuild time, so it
       can already include a buy the trade-date filter just dropped. Same
       re-derivation Split and Bonus previews apply. */
    const holding = recomputeHoldingsSnapshot(holdingRows).HOLDING;
    if (holding <= 1e-6) continue;
    rows.push({ accountCode, holding, amount: round2(holding * rate) });
  }
  return rows;
};

export const getAllSecuritiesISINs = async (req, res) => {
    try {
      if (!req.catalystApp) {
        return res.status(500).json({
          success: false,
          message: "Catalyst app not initialized",
        });
      }
  
      const zcql = req.catalystApp.zcql();
      const LIMIT = 270;
      let offset = 0;
      const securities = [];
  
      while (true) {
        const rows = await zcql.executeZCQLQuery(`
          SELECT ISIN, Security_Code, Security_Name
          FROM Security_List
          WHERE ISIN IS NOT NULL
          LIMIT ${LIMIT} OFFSET ${offset}
        `);
  
        if (!rows || rows.length === 0) break;
  
        rows.forEach((r) => {
          const s = r.Security_List;
          securities.push({
            isin: s.ISIN,
            securityCode: s.Security_Code,
            securityName: s.Security_Name,
          });
        });
  
        offset += LIMIT;
      }
  
      return res.json({ success: true, data: securities });
    } catch (error) {
      console.error(error);
      return res.status(500).json({
        success: false,
        message: error.message,
      });
    }
  };

  /**
   * POST /api/dividend/preview
   *
   * JSON body { isin, recordDate, rate, paymentDate, compare? }.
   *
   * Always returns the system report as `data` (holding on the record date
   * × rate, per account). The page shows it first.
   *
   * With `compare: true` it also reconciles against the custodian side: the
   * Ex-Entitlement rows uploaded on Corporate Actions -> Custodian Upload,
   * limited to "Dividend In Cash" rows for this ISIN and record date (from the
   * latest uploaded report that has them). It then ALSO returns:
   *   - custodianFile — which uploaded file was used.
   *   - events[]  — file rows grouped by (CARefNo, Rate) with reconciled
   *                 per-account rows merging FIFO + file + already-paid
   *                 history.
   *   - summary   — totals + counts per status bucket.
   *   - warnings  — soft issues like "file has rows for other ISINs".
   */
  export const previewStockDividend = async (req, res) => {
    try {
      if (!req.catalystApp) {
        return res.status(500).json({
          success: false,
          message: "Catalyst app not initialized",
        });
      }

      const { isin, recordDate, rate, paymentDate } = req.body || {};
      const compare = req.body?.compare === true;
      const rateNum = Number(rate);

      if (!Number.isFinite(rateNum) || rateNum <= 0) {
        return res.status(400).json({
          success: false,
          message: "Invalid dividend rate value",
        });
      }
      if(!isin || !recordDate || !paymentDate){
        return res.status(400).json({
          success: false,
          message: "Invalid ISIN, record date or payment date",
        });
      }

      const recordDateISO = toISODateOnly(recordDate);
      const paymentDateISO = toISODateOnly(paymentDate);
      if (!recordDateISO || !paymentDateISO) {
        return res.status(400).json({
          success: false,
          message: "Invalid record date or payment date",
        });
      }

      /* ======================================================
         STEP 0: READ THE CUSTODIAN FILE (compare only)
         Needed for the comparison, which is the only place dividends can be
         applied from — we never apply from FIFO alone. Only the "Dividend In
         Cash" rows for this ISIN and record date are used, from the latest
         report uploaded on the Custodian Upload page that has them. The system report alone does not need it.
         ====================================================== */
      const warnings = [];
      let custodian = null;
      let fileParsed = null;
      if (compare) {
        try {
          custodian = await readLatestCorpActionRows(req.catalystApp, {
            caType: "dividend",
            isin,
            recordDateISO,
          });
        } catch (fileErr) {
          if (!fileErr.code) throw fileErr;
          return res.status(409).json({
            success: false,
            code: fileErr.code,
            message: fileErr.message,
          });
        }

        // Same row shape the reconciliation below was written for. The common
        // custodian file has no TDS or Net amount columns.
        fileParsed = {
          rows: custodian.rows.map((r) => ({
            accountCode: r.accountCode,
            clientName: r.clientName,
            isin: r.isin,
            isinName: r.isinName,
            caRef: r.caRefNo,
            exDate: r.exDate,
            recordDate: r.recordDate,
            paymentDate: r.paymentDate,
            rate: r.dividendRate,
            holding: r.holdingQty,
            gross: r.caAmt,
            tds: null,
            net: null,
            caType: "Dividend In Cash",
          })),
        };
      }

      const zcql = req.catalystApp.zcql();

      // Where this dividend is in the custodian reconciliation, and each account's status.
      const dividendEvent = { isin, recordDateISO };
      const accountRows = await readDividendAccountRows(zcql, dividendEvent);
      const stage = dividendStage(accountRows, await isDividendApplied(zcql, dividendEvent));
      const statusByAccount = new Map(accountRows.map((r) => [r.accountCode, r]));

      /* ======================================================
         STEP 1+2: HOLDING AS OF RECORD DATE (read materialised Holdings)
         See computeDividendSystemRows. We don't early-return on an empty
         result: the file may carry rows we need to surface as
         "missing_in_system" for ops to investigate.
         ====================================================== */
      const preview = [];
      const fifoByAccount = new Map();

      for (const row of await computeDividendSystemRows(zcql, { isin, recordDateISO, rate: rateNum })) {
        fifoByAccount.set(row.accountCode, { holding: row.holding, gross: row.amount });
        preview.push({
          isin,
          accountCode: row.accountCode,
          holdingAsOnRecordDate: row.holding,
          rate: rateNum,
          paymentDate: paymentDateISO,
          dividendAmount: row.amount,
          status: statusByAccount.get(row.accountCode)?.status || "",
          receivedDate: statusByAccount.get(row.accountCode)?.receivedDate || "",
        });
      }

      /* ======================================================
         STEP 7: RECONCILIATION (only when a custodian file is present)
         ------------------------------------------------------
         - Filter file rows to current ISIN.
         - Group by (CARefNo, Rate) -> separate "events".
         - Look up "already received" history for the union of accounts.
         - Merge file + FIFO + history into per-account reconciled rows.
         - Compute summary totals per status bucket.
         ====================================================== */
      let events = null;
      let summary = null;

      if (fileParsed) {
        const allFileRows = fileParsed.rows;

        const targetIsinUC = String(isin).toUpperCase();
        const matchingRows = allFileRows.filter(
          (r) => r.isin && r.isin.toUpperCase() === targetIsinUC,
        );
        const otherIsinCount = allFileRows.length - matchingRows.length;
        if (otherIsinCount > 0) {
          warnings.push(
            `Custodian file has ${otherIsinCount} row(s) for other ISINs ` +
              `that were ignored.`,
          );
        }

        // Sanity check: file's record date should match the form's record date.
        const fileRecordDates = new Set(
          matchingRows.map((r) => r.recordDate).filter(Boolean),
        );
        if (
          fileRecordDates.size === 1 &&
          !fileRecordDates.has(recordDateISO)
        ) {
          warnings.push(
            `Custodian file has Record Date ${[...fileRecordDates][0]}, ` +
              `but the form has ${recordDateISO}. Reconciliation uses the ` +
              `form's date.`,
          );
        } else if (fileRecordDates.size > 1) {
          warnings.push(
            `Custodian file has multiple Record Dates: ${[
              ...fileRecordDates,
            ].join(", ")}. Reconciliation uses the form's date.`,
          );
        }

        // Group file rows by (CARefNo + Rate) so multi-event dividends on
        // the same Record Date stay separated.
        const eventsMap = new Map();
        for (const row of matchingRows) {
          const key = `${row.caRef || "-"}|${row.rate}`;
          if (!eventsMap.has(key)) {
            eventsMap.set(key, {
              caRef: row.caRef || null,
              rate: row.rate,
              caType: row.caType || null,
              filePaymentDate: row.paymentDate || null,
              fileExDate: row.exDate || null,
              fileRecordDate: row.recordDate || null,
              rowsByAccount: new Map(),
            });
          }
          const evt = eventsMap.get(key);
          // If a UCC appears twice within the same event (rare), sum amounts.
          const prev = evt.rowsByAccount.get(row.accountCode);
          if (prev) {
            prev.holding += row.holding;
            prev.gross = round2(prev.gross + row.gross);
            prev.tds = round2(prev.tds + row.tds);
            prev.net = round2(prev.net + row.net);
          } else {
            evt.rowsByAccount.set(row.accountCode, { ...row });
          }
        }

        /*
         * Edge case: file had no rows for this ISIN but the system has FIFO
         * holdings. Without this fallback, the renderer would loop over zero
         * events and display nothing — orphaning the FIFO accounts. We emit
         * one synthetic event keyed by the form rate so those FIFO accounts
         * surface as missing_in_file rows the user can investigate.
         */
        if (eventsMap.size === 0 && fifoByAccount.size > 0) {
          eventsMap.set(`-|${rateNum}`, {
            caRef: null,
            rate: rateNum,
            caType: null,
            filePaymentDate: null,
            fileExDate: null,
            fileRecordDate: null,
            rowsByAccount: new Map(),
            synthetic: true,
          });
          warnings.push(
            "Custodian file has no rows for this ISIN. Showing system " +
              "(FIFO) accounts as 'Missing in file' for review.",
          );
        }

        // Union of accounts referenced anywhere (file OR FIFO).
        const accountUnion = new Set();
        for (const evt of eventsMap.values()) {
          for (const acc of evt.rowsByAccount.keys()) accountUnion.add(acc);
        }
        for (const acc of fifoByAccount.keys()) accountUnion.add(acc);
        const accountList = [...accountUnion];

        // Already-received and cash-credited lookups (one query each, batched).
        // virtualToActual resolves each account code to its underlying real
        // account (Actual_Code). Codes booked directly under an actual code map
        // to "" and are grouped by their own accountCode on the frontend.
        const [alreadyReceivedMap, cashCreditedSet, virtualToActual] =
          await Promise.all([
            lookupAlreadyReceived(zcql, isin, recordDateISO, accountList),
            lookupCashCredited(zcql, isin, recordDateISO, accountList),
            buildVirtualToActualMap(zcql, accountList),
          ]);

        const eventsArr = [];
        const summaryAgg = {
          events: 0,
          accounts: 0,
          ready: 0,
          mismatch: 0,
          partial: 0,
          overpaid: 0,
          alreadyPaid: 0,
          missingInSystem: 0,
          missingInFile: 0,
          totalGrossSys: 0,
          totalGrossFile: 0,
          totalNetFile: 0,
          totalTdsFile: 0,
          totalAlreadyReceived: 0,
          totalToReceiveNet: 0,
          totalToReceiveGross: 0,
        };

        for (const [, evt] of eventsMap) {
          const reconciledRows = [];

          // Per-event union: file accounts ∪ FIFO accounts whose rate matches.
          const evtAccountSet = new Set();
          for (const acc of evt.rowsByAccount.keys()) evtAccountSet.add(acc);
          // Only attribute FIFO to this event if rates match the form rate
          // (single-event case) or if the event rate equals the form rate.
          // Otherwise FIFO holdings are shown only against the event(s)
          // whose Rate equals the form's Rate.
          const fifoAttributesToEvent =
            Math.abs(evt.rate - rateNum) < 0.0001 || eventsMap.size === 1;
          if (fifoAttributesToEvent) {
            for (const acc of fifoByAccount.keys()) evtAccountSet.add(acc);
          }

          const sortedAccounts = [...evtAccountSet].sort();

          for (const accountCode of sortedAccounts) {
            const fileRow = evt.rowsByAccount.get(accountCode);
            const fifoRow = fifoAttributesToEvent
              ? fifoByAccount.get(accountCode)
              : undefined;

            const inFile = !!fileRow;
            const inSystem = !!fifoRow;

            const holdingFile = inFile ? fileRow.holding : null;
            const holdingSys = inSystem ? fifoRow.holding : null;
            const holdingDelta =
              inFile && inSystem ? holdingFile - holdingSys : 0;

            const grossFile = inFile ? fileRow.gross : null;
            // System gross uses file rate if attributed to this event;
            // otherwise it's the form rate.
            const grossSys =
              inSystem && fifoRow
                ? round2(holdingSys * evt.rate)
                : null;
            const grossDelta =
              inFile && inSystem ? round2(grossFile - grossSys) : 0;

            const tdsFile = inFile ? fileRow.tds : null;
            const netFile = inFile ? fileRow.net : null;

            const alreadyHist = alreadyReceivedMap.get(accountCode);
            const alreadyReceivedGross = alreadyHist ? alreadyHist.gross : 0;
            const lastPaidOn = alreadyHist ? alreadyHist.lastPaidOn : null;
            const alreadyCreditedCash = cashCreditedSet.has(accountCode);

            const baseForToReceive = inFile ? grossFile : grossSys || 0;
            const toReceiveGross = round2(
              Math.max(0, baseForToReceive - alreadyReceivedGross),
            );
            const toReceiveNet =
              inFile && netFile != null
                ? round2(
                    Math.max(
                      0,
                      netFile - alreadyReceivedGross,
                    ),
                  )
                : toReceiveGross;

            const status = computeStatus({
              inFile,
              inSystem,
              holdingDelta,
              grossDelta,
              alreadyReceivedGross,
              fileGross: grossFile || 0,
              alreadyCreditedCash,
            });

            reconciledRows.push({
              accountCode,
              actualCode: virtualToActual.get(accountCode) || "",
              clientName: fileRow ? fileRow.clientName : null,
              inFile,
              inSystem,
              holdingSys,
              holdingFile,
              holdingDelta,
              rateSys: rateNum,
              rateFile: inFile ? fileRow.rate : null,
              grossSys,
              grossFile,
              grossDelta,
              tdsFile,
              netFile,
              alreadyReceivedGross,
              alreadyCreditedCash,
              lastPaidOn,
              toReceiveGross,
              toReceiveNet,
              status,
            });

            // Summary aggregation
            summaryAgg.totalGrossSys += grossSys || 0;
            summaryAgg.totalGrossFile += grossFile || 0;
            summaryAgg.totalNetFile += netFile || 0;
            summaryAgg.totalTdsFile += tdsFile || 0;
            summaryAgg.totalAlreadyReceived += alreadyReceivedGross || 0;
            summaryAgg.totalToReceiveNet += toReceiveNet || 0;
            summaryAgg.totalToReceiveGross += toReceiveGross || 0;
            switch (status) {
              case "ready": summaryAgg.ready++; break;
              case "mismatch": summaryAgg.mismatch++; break;
              case "partial": summaryAgg.partial++; break;
              case "overpaid": summaryAgg.overpaid++; break;
              case "already_paid": summaryAgg.alreadyPaid++; break;
              case "missing_in_system": summaryAgg.missingInSystem++; break;
              case "missing_in_file": summaryAgg.missingInFile++; break;
              default: break;
            }
          }

          eventsArr.push({
            caRef: evt.caRef,
            rate: evt.rate,
            caType: evt.caType,
            filePaymentDate: evt.filePaymentDate,
            fileExDate: evt.fileExDate,
            fileRecordDate: evt.fileRecordDate,
            isin,
            recordDate: recordDateISO,
            paymentDate: paymentDateISO,
            rowCount: reconciledRows.length,
            rows: reconciledRows,
          });
        }

        // Round summary totals to 2 decimals.
        summaryAgg.totalGrossSys = round2(summaryAgg.totalGrossSys);
        summaryAgg.totalGrossFile = round2(summaryAgg.totalGrossFile);
        summaryAgg.totalNetFile = round2(summaryAgg.totalNetFile);
        summaryAgg.totalTdsFile = round2(summaryAgg.totalTdsFile);
        summaryAgg.totalAlreadyReceived = round2(
          summaryAgg.totalAlreadyReceived,
        );
        summaryAgg.totalToReceiveNet = round2(summaryAgg.totalToReceiveNet);
        summaryAgg.totalToReceiveGross = round2(summaryAgg.totalToReceiveGross);
        summaryAgg.events = eventsArr.length;
        summaryAgg.accounts = accountList.length;

        events = eventsArr;
        summary = summaryAgg;
      }

      const response = { success: true, data: preview, stage };
      if (custodian) response.custodianFile = custodian.file;
      if (events) response.events = events;
      if (summary) response.summary = summary;
      if (warnings.length) response.warnings = warnings;
      return res.json(response);
    } catch (error) {
      console.error("Preview dividend error:", error);
      return res.status(500).json({
        success: false,
        message: error.message,
      });
    }
  };

  /* ======================================================
     CUSTODIAN RECONCILIATION (Receivable → Received)
     Stage 1: compare with the Ex-Entitlement file (preview with
              compare: true), then "Send as Receivable" inserts one
              Dividend_Record row per chosen account (Status RECEIVABLE,
              no cash yet).
     Stage 2: compare with the Corporate Benefit Collection file, then
              "Confirm & Apply" stores each paid account's Received Date and
              starts UpdateDividendData, which credits the cash and marks the
              row RECEIVED. Accounts the file does not list stay Receivable
              (no cash) until a later Confirm with a newer file.
     See util/analytics/dividendAccountRows.js.
     ====================================================== */

  /**
   * POST /api/dividend/send-receivable
   * body: { isin, securityCode, rate, recordDate, paymentDate, accountCodes }
   *
   * accountCodes is the page's choice (matched only / all reconciled). The
   * amounts are recomputed here rather than taken from the browser, and every
   * code must be an account holding the ISIN on the record date.
   */
  export const sendDividendReceivable = async (req, res) => {
    try {
      const app = req.catalystApp;
      const zcql = app.zcql();
      const { isin, securityCode, rate: rateParam, recordDate, paymentDate, accountCodes } = req.body || {};
      const rate = Number(rateParam);
      const recordDateISO = toISODateOnly(recordDate);
      const paymentDateISO = toISODateOnly(paymentDate);
      const chosen = new Set(
        (Array.isArray(accountCodes) ? accountCodes : []).map((s) => String(s || "").trim()).filter(Boolean),
      );
      if (!isin || !recordDateISO || !paymentDateISO || !(rate > 0)) {
        return res
          .status(400)
          .json({ success: false, message: "ISIN, rate, record date and payment date are required." });
      }
      if (chosen.size === 0) {
        return res.status(400).json({ success: false, message: "Choose at least one account to send as Receivable." });
      }

      const dividendEvent = { isin, recordDateISO };
      if (await isDividendApplied(zcql, dividendEvent)) {
        return res
          .status(409)
          .json({ success: false, code: "ALREADY_SENT", message: "This dividend has already been applied." });
      }
      if ((await readDividendAccountRows(zcql, dividendEvent)).length > 0) {
        return res.status(409).json({
          success: false,
          code: "ALREADY_SENT",
          message: "These accounts are already Receivable. Compare with the Corporate Benefit Collection file next.",
        });
      }

      // Throws NO_CUSTODIAN_ROWS (caught below) when no uploaded file has this dividend.
      await readLatestCorpActionRows(app, { caType: "dividend", isin, recordDateISO });

      const systemRows = await computeDividendSystemRows(zcql, { isin, recordDateISO, rate });
      const eligible = new Set(systemRows.map((r) => r.accountCode));
      const notEligible = [...chosen].filter((code) => !eligible.has(code));
      if (notEligible.length > 0) {
        return res.status(409).json({
          success: false,
          code: "NOT_ELIGIBLE",
          message:
            `${notEligible.length} account(s) hold no ${isin} on ${recordDateISO}: ` +
            `${notEligible.slice(0, 5).join(", ")}. Refresh the preview and try again.`,
        });
      }

      const newRows = systemRows
        .filter((r) => chosen.has(r.accountCode))
        .map((r) => ({
          ISIN: isin,
          RecordDate: recordDateISO,
          WS_Account_code: r.accountCode,
          Holding: r.holding,
          Rate: rate,
          Dividend_Amount: r.amount,
          PaymentDate: paymentDateISO,
          Security_Code: securityCode || "",
          Status: DIVIDEND_STATUS.RECEIVABLE,
        }));
      const table = app.datastore().table("Dividend_Record");
      try {
        for (let i = 0; i < newRows.length; i += ACCOUNT_ROW_CHUNK) {
          await table.insertRows(newRows.slice(i, i + ACCOUNT_ROW_CHUNK));
        }
      } catch (insertErr) {
        // A half-sent dividend would show as Receivable with accounts missing.
        // No row of this dividend existed before (checked above), so every row
        // was inserted by this call — remove them and let the user send again.
        try {
          await zcql.executeZCQLQuery(`DELETE FROM Dividend_Record WHERE ${dividendAccountWhere(dividendEvent)}`);
        } catch (cleanupErr) {
          console.error("[DividendUploader] Could not remove a half-sent receivable:", cleanupErr.message);
        }
        throw insertErr;
      }

      return res.json({
        success: true,
        count: newRows.length,
        message: `${newRows.length} account(s) updated with status Receivable.`,
      });
    } catch (error) {
      if (error.code) return res.status(409).json({ success: false, code: error.code, message: error.message });
      console.error("[DividendUploader] sendDividendReceivable:", error);
      return res
        .status(500)
        .json({ success: false, message: error.message || "Could not send the dividend as receivable." });
    }
  };

  /**
   * The Receivable accounts the Corporate Benefit Collection file says were
   * paid, each with the date it was paid (matched on the actual / UCC code).
   * Nothing is written here, so a missing Benefit file stops Confirm before
   * any change.
   *
   * @returns {Promise<{ payable: Array<{ rowId, accountCode, receivedDate }> } | { code: string, error: string }>}
   */
  const planDividendReceipt = async (app, zcql, { isin, recordDateISO, receivable }) => {
    let allocationRows;
    try {
      ({ rows: allocationRows } = await readLatestReceiptRows(app, { caType: "dividend", isin, recordDateISO }));
    } catch (err) {
      if (err.code) return { code: err.code, error: err.message };
      throw err;
    }

    // The latest date is when the last of a client's payments arrived.
    const receivedDateByActual = new Map();
    for (const row of allocationRows) {
      if (!row.receivedDate) continue;
      const previous = receivedDateByActual.get(row.accountCode);
      if (!previous || row.receivedDate > previous) receivedDateByActual.set(row.accountCode, row.receivedDate);
    }

    const virtualToActual = await buildVirtualToActualMap(
      zcql,
      receivable.map((r) => r.accountCode),
    );
    const payable = [];
    for (const r of receivable) {
      const actual = String(virtualToActual.get(r.accountCode) || r.accountCode).toUpperCase();
      const receivedDate = receivedDateByActual.get(actual);
      if (receivedDate) payable.push({ rowId: r.rowId, accountCode: r.accountCode, receivedDate });
    }
    return { payable };
  };

  /* ======================================================
     CONFIRM & APPLY DIVIDEND (BACKGROUND JOB) — stage 2
       - Validate inputs.
       - Only Receivable accounts that the Benefit Collection file lists are
         paid; their Received Dates are stored before the job starts.
       - Reuse / clean up any existing Jobs row for jobName.
       - Submit UpdateDividendData with those accounts as its allow-list. It
         inserts the Dividend master row (once), credits
         Cash_Balance_Per_Transaction and marks each account RECEIVED.
       - Return immediately with { jobName, status: "PENDING" } so the React
         UI can poll /dividend/apply-status.
     ====================================================== */
  export const applyStockDividendMaster = async (req, res) => {
    try {
      if (!req.catalystApp) {
        return res.status(500).json({
          success: false,
          message: "Catalyst app not initialized",
        });
      }

      const {
        isin,
        securityCode,
        securityName,
        rate: rateParam,
        exDate,
        recordDate,
        paymentDate,
        dividendType,
      } = req.body;

      const rate = Number(rateParam);

      if (!Number.isFinite(rate) || rate <= 0) {
        return res.status(400).json({
          success: false,
          message: "Invalid dividend rate value",
        });
      }
      if (
        !isin ||
        !securityCode ||
        !securityName ||
        !recordDate ||
        !paymentDate
      ) {
        return res.status(400).json({
          success: false,
          message: "Invalid or missing required fields",
        });
      }

      const recordDateISO = toISODateOnly(recordDate);
      const exDateISO = (exDate && String(exDate).trim())
        ? toISODateOnly(exDate)
        : recordDateISO;
      const paymentDateISO = toISODateOnly(paymentDate);
      if (!recordDateISO || !exDateISO || !paymentDateISO) {
        return res.status(400).json({
          success: false,
          message: "Invalid record date, ex date or payment date",
        });
      }

      const catalystApp = req.catalystApp;
      const zcql = catalystApp.zcql();
      const jobScheduling = catalystApp.jobScheduling();

      // Stage 2 of the custodian reconciliation: only accounts sent as
      // Receivable can be confirmed, and only those the Benefit Collection
      // file says were paid. The rest stay Receivable for a later Confirm.
      const dividendEvent = { isin, recordDateISO };
      const receivable = (await readDividendAccountRows(zcql, dividendEvent)).filter(
        (r) => r.status === DIVIDEND_STATUS.RECEIVABLE,
      );
      if (receivable.length === 0) {
        const applied = await isDividendApplied(zcql, dividendEvent);
        return res.status(409).json({
          success: false,
          code: applied ? "ALREADY_APPLIED" : "NOT_RECEIVABLE",
          message: applied
            ? "This dividend has already been applied."
            : `This dividend (${isin}, Record Date ${recordDateISO}) has not been sent as Receivable yet. ` +
              "Compare with the Ex-Entitlement file and send it as receivable first.",
        });
      }
      const receipt = await planDividendReceipt(catalystApp, zcql, { isin, recordDateISO, receivable });
      if (receipt.error) {
        return res.status(409).json({ success: false, code: receipt.code, message: receipt.error });
      }
      if (receipt.payable.length === 0) {
        return res.status(409).json({
          success: false,
          code: "NOTHING_RECEIVED",
          message:
            "No Receivable account is in the Corporate Benefit Collection file yet. " +
            "Upload a newer Benefit Collection file and try again.",
        });
      }

      const jobName = `DIV_${isin.slice(-6)}_${recordDateISO}`;

      const existing = await zcql.executeZCQLQuery(
        `SELECT ROWID, status, CREATEDTIME FROM Jobs WHERE jobName = '${jobName}' LIMIT 1`
      );

      if (existing.length) {
        const oldStatus = existing[0].Jobs.status;
        const oldRowId = existing[0].Jobs.ROWID;
        const createdTime = existing[0].Jobs.CREATEDTIME;

        const STALE_TIMEOUT_MS = 60 * 60 * 1000;
        const jobAge = Date.now() - parseCatalystTime(createdTime);
        const isStale = jobAge > STALE_TIMEOUT_MS;

        if ((oldStatus === "PENDING" || oldStatus === "RUNNING") && !isStale) {
          return res.json({
            success: true,
            jobName,
            status: oldStatus,
            message: "Dividend application is already in progress",
          });
        }

        try {
          await zcql.executeZCQLQuery(`DELETE FROM Jobs WHERE ROWID = ${oldRowId}`);
        } catch (delErr) {
          console.error("Error deleting old dividend job:", delErr);
        }
      }

      // Received Dates first; the job sets Status RECEIVED once each account's
      // cash is credited, so a failure leaves the account Receivable.
      const rowIdsByDate = new Map();
      for (const p of receipt.payable) {
        if (!rowIdsByDate.has(p.receivedDate)) rowIdsByDate.set(p.receivedDate, []);
        rowIdsByDate.get(p.receivedDate).push(p.rowId);
      }
      for (const [date, rowIds] of rowIdsByDate) {
        for (let i = 0; i < rowIds.length; i += ACCOUNT_ROW_CHUNK) {
          await zcql.executeZCQLQuery(`
            UPDATE Dividend_Record SET Received_Date = '${escSql(date)}'
            WHERE ROWID IN (${rowIds.slice(i, i + ACCOUNT_ROW_CHUNK).join(", ")})
          `);
        }
      }

      await jobScheduling.JOB.submitJob({
        job_name: "ApplyDividend",
        jobpool_name: "CorporateActions",
        target_name: "UpdateDividendData",
        target_type: "Function",
        params: {
          isin,
          securityCode,
          securityName,
          rate: String(rate),
          exDate: exDateISO,
          recordDate: recordDateISO,
          paymentDate: paymentDateISO,
          dividendType: dividendType || "Final",
          jobName,
          // Only these accounts are paid by this Confirm.
          accountCodesJson: JSON.stringify(receipt.payable.map((p) => p.accountCode)),
        },
      });

      return res.json({
        success: true,
        jobName,
        status: "PENDING",
        count: receipt.payable.length,
        remaining: receivable.length - receipt.payable.length,
        message: "Dividend application job started",
      });

    } catch (error) {
      console.error("Apply dividend master error:", error);
      return res.status(500).json({
        success: false,
        message: error.message,
      });
    }
  };

  /* ======================================================
     GET DIVIDEND APPLY JOB STATUS
     Mirrors getBonusApplyStatus exactly (same Jobs table
     contract used by UpdateDividendData function).
     ====================================================== */
  export const getDividendApplyStatus = async (req, res) => {
    try {
      const catalystApp = req.catalystApp;
      const zcql = catalystApp.zcql();

      const { jobName } = req.query;
      if (!jobName) {
        return res.status(400).json({ success: false, message: "jobName is required" });
      }

      const result = await zcql.executeZCQLQuery(
        `SELECT ROWID, status, CREATEDTIME FROM Jobs WHERE jobName = '${jobName}' LIMIT 1`
      );

      if (!result.length) {
        return res.json({ success: true, status: "NOT_STARTED" });
      }

      let status = result[0].Jobs.status;
      const createdTime = result[0].Jobs.CREATEDTIME;

      const STALE_TIMEOUT_MS = 60 * 60 * 1000;
      const jobAge = Date.now() - parseCatalystTime(createdTime);

      if ((status === "PENDING" || status === "RUNNING") && jobAge > STALE_TIMEOUT_MS) {
        try {
          await zcql.executeZCQLQuery(
            `UPDATE Jobs SET status = 'ERROR' WHERE jobName = '${jobName}'`
          );
        } catch (updateErr) {
          console.error("Failed to mark stale dividend job as ERROR:", updateErr);
        }
        status = "ERROR";
      }

      return res.json({ success: true, jobName, status });
    } catch (error) {
      console.error("Error fetching dividend job status:", error);
      return res.status(500).json({
        success: false,
        message: "Failed to fetch dividend job status",
      });
    }
  };
