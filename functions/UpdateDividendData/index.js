/**
 * UpdateDividendData (Catalyst job function)
 *
 * Triggered as a background job by the AppSail controller
 * `applyStockDividendMaster` (DividendUploader.js) via
 * `jobScheduling.JOB.submitJob({ target_name: "UpdateDividendData", ... })`.
 *
 * Job params (all required, supplied by the controller):
 *   isin, securityCode, securityName, rate, exDate, recordDate,
 *   paymentDate, dividendType, jobName
 *
 * What it does:
 *   1. INSERT a row into `Jobs` (status='PENDING') keyed by jobName so the
 *      React UI can poll `/dividend/apply-status?jobName=...`.
 *   2. Idempotency check on (ISIN, RecordDate) in `Dividend`.
 *   3. INSERT one master row into `Dividend`.
 *   4. Read the Holdings ledger for the ISIN as at the RecordDate — the same
 *      rows, date rule and position maths as the dividend preview.
 *   5. Re-derive each account's holding on the RecordDate from those rows.
 *   6. INSERT one row into `Dividend_Record` per eligible account.
 *   7. APPEND one row into `Cash_Balance_Per_Transaction` per eligible
 *      account (running balance + monotonic Sequence per account).
 *   8. UPDATE the `Jobs` row to status='COMPLETED' (or 'FAILED' on error).
 */

const catalyst = require("zcatalyst-sdk-node");
const {
  QTY_EPS,
  holdingsEffectiveDate,
  recomputeHoldingsSnapshot,
  nextISODate,
} = require("./holdingsSnapshot.js");

const ZCQL_ROW_LIMIT = 270;

const esc = (s) => String(s ?? "").replace(/'/g, "''");

/**
 * Every Holdings row for ONE ISIN that is effective on or before the record
 * date, grouped by account in FIFO insert order.
 *
 * PORT of `fetchHoldingsRowsByAccountForIsin` in
 * appsail-nodejs/util/analytics/holdingsFromTable.js, which the dividend
 * PREVIEW reads — keep the two in sync so preview and apply see the same rows.
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
      LIMIT ${ZCQL_ROW_LIMIT}
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
    if (batch.length < ZCQL_ROW_LIMIT) break;
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

const normalizeDate = (d) => {
  const dt = new Date(d);
  dt.setUTCHours(0, 0, 0, 0);
  return dt.toISOString().split("T")[0];
};

/* =========================================================================
   JOB ENTRY POINT
   ========================================================================= */
module.exports = async (jobRequest, context) => {
  const catalystApp = catalyst.initialize(context);
  const zcql = catalystApp.zcql();
  let jobName = "";

  try {
    console.log("UpdateDividendData job started");

    /* ---- 1. Resolve inputs from job params (controller always supplies these) ---- */
    const params = jobRequest.getAllJobParams();
    const {
      isin,
      securityCode,
      securityName,
      exDate,
      recordDate,
      paymentDate,
      dividendType,
      accountCodesJson,
      applyMode,
    } = params;
    const rate = Number(params.rate);
    jobName = params.jobName;

    /*
     * Optional account filter passed by the controller. When present the job
     * still discovers all FIFO-eligible accounts but only writes rows for
     * those in this allow-list. Used by the Apply Matched Only / Apply All
     * Reconciled buttons on the Dividend page.
     */
    let accountFilter = null;
    if (accountCodesJson) {
      try {
        const arr = JSON.parse(accountCodesJson);
        if (Array.isArray(arr) && arr.length) {
          accountFilter = new Set(arr.map((s) => String(s).trim()));
        }
      } catch (parseErr) {
        console.warn(
          "UpdateDividendData: could not parse accountCodesJson; treating as no filter:",
          parseErr.message,
        );
      }
    }
    if (accountFilter) {
      console.log(
        `UpdateDividendData: applyMode=${applyMode || "matched"}, ` +
          `account filter contains ${accountFilter.size} accounts`,
      );
    } else {
      console.log(
        `UpdateDividendData: applyMode=${applyMode || "system"} (no account filter)`,
      );
    }

    if (!Number.isFinite(rate) || rate <= 0) {
      console.error("UpdateDividendData: invalid dividend rate ->", params.rate);
      if (jobName) {
        try {
          await zcql.executeZCQLQuery(
            `UPDATE Jobs SET status = 'FAILED' WHERE jobName = '${jobName}'`
          );
        } catch (e) {
          console.error("Failed to mark job FAILED on validation error:", e);
        }
      }
      context.closeWithFailure();
      return;
    }
    if (!isin || !securityCode || !securityName || !recordDate || !paymentDate) {
      console.error("UpdateDividendData: missing required fields", {
        isin,
        securityCode,
        securityName,
        recordDate,
        paymentDate,
      });
      if (jobName) {
        try {
          await zcql.executeZCQLQuery(
            `UPDATE Jobs SET status = 'FAILED' WHERE jobName = '${jobName}'`
          );
        } catch (e) {
          console.error("Failed to mark job FAILED on validation error:", e);
        }
      }
      context.closeWithFailure();
      return;
    }

    /* ---- 1b. Insert PENDING row in Jobs so /apply-status can be polled ---- */
    await zcql.executeZCQLQuery(
      `INSERT INTO Jobs (jobName, status) VALUES ('${jobName}', 'PENDING')`
    );

    const recordDateISO = normalizeDate(recordDate);
    const exDateISO = exDate && String(exDate).trim()
      ? normalizeDate(exDate)
      : recordDateISO;
    const paymentDateISO = normalizeDate(paymentDate);

    console.log(
      `UpdateDividendData: ISIN=${isin} RecordDate=${recordDateISO} Rate=${rate} PaymentDate=${paymentDateISO}`,
    );

    /* ---- 2. Idempotency check ---- */
    const existing = await zcql.executeZCQLQuery(`
      SELECT ROWID
      FROM Dividend
      WHERE ISIN='${esc(isin)}'
      AND RecordDate='${recordDateISO}'
      LIMIT 1
    `);

    if (existing && existing.length > 0) {
      console.warn(
        `Dividend already exists for ISIN=${isin} RecordDate=${recordDateISO}. Skipping master insert and per-account allocation.`,
      );
      await zcql.executeZCQLQuery(
        `UPDATE Jobs SET status = 'COMPLETED' WHERE jobName = '${jobName}'`
      );
      context.closeWithSuccess();
      return;
    }

    /* ---- 3. INSERT INTO Dividend (master row) ---- */
    await zcql.executeZCQLQuery(`
      INSERT INTO Dividend
      (
        SecurityCode,
        Security_Name,
        ISIN,
        Rate,
        ExDate,
        RecordDate,
        PaymentDate,
        Dividend_Type,
        Status
      )
      VALUES
      (
        '${esc(securityCode)}',
        '${esc(securityName)}',
        '${esc(isin)}',
        ${rate},
        '${exDateISO}',
        '${recordDateISO}',
        '${paymentDateISO}',
        '${esc(dividendType || "Final")}',
        'Draft'
      )
    `);
    console.log(`Dividend master row inserted for ${isin}/${recordDateISO}`);

    /* ---- 4. Read the Holdings ledger as at the RecordDate ----
     * Same rows, same date rule and same position maths as the dividend
     * PREVIEW (DividendUploader.previewStockDividend), so the amount applied is
     * the amount that was previewed. One read gives both the eligible accounts
     * and the rows to size each one from.
     *
     * ===== OLD LOGIC — raw-table replay. Kept for rollback (the inline
     * runFifoEngine is in git history, deleted with this change). =====
     * Accounts came from `SELECT WS_Account_code FROM Transaction`, then the
     * Transaction / Bonus / Split rows ≤ RecordDate were replayed through
     * `runFifoEngine(tx, bonuses, splits, true)`. That engine was never given
     * merger or demerger inputs, so shares that arrived through a corporate
     * action counted as zero here while the preview counted them.
     * ===== END OLD LOGIC ===== */
    const rowsByAccount = await fetchHoldingsRowsByAccountForIsin(
      zcql,
      isin,
      recordDateISO,
    );
    console.log(`Accounts with Holdings rows as at ${recordDateISO}: ${rowsByAccount.size}`);

    if (rowsByAccount.size === 0) {
      console.log("No eligible accounts. Job complete (master row inserted only).");
      await zcql.executeZCQLQuery(
        `UPDATE Jobs SET status = 'COMPLETED' WHERE jobName = '${jobName}'`
      );
      context.closeWithSuccess();
      return;
    }

    /* ---- 5. Holding + INSERT INTO Dividend_Record + Cash_Balance_Per_Transaction
     *        per eligible account.
     *
     *        Cash_Balance_Per_Transaction insert rules (append-only):
     *          - Skip if a row already exists for
     *            (Account_Code, ISIN, Transaction_Date=RecordDate, Type='DIVIDEND').
     *          - Otherwise read the last row for the account (ORDER BY Sequence DESC),
     *            insert a new row with Sequence=lastSeq+1 and
     *            Cash_Balance=lastBalance+dividendAmount.
     *          - Tran Date = RecordDate, Set Date = RecordDate (both store/display
     *            the record date so the Cash Passbook UI shows the dividend's
     *            entitlement date in both date columns).
     * ---- */
    let insertedCount = 0;
    let skippedFlat = 0;
    let skippedFiltered = 0;
    let insertErrors = 0;
    let cashInsertedCount = 0;
    let cashSkippedDup = 0;
    let cashErrors = 0;

    for (const [accountCode, holdingRows] of rowsByAccount) {
      // Apply Matched Only / Apply All Reconciled: skip accounts that are
      // not in the allow-list passed by the controller.
      if (accountFilter && !accountFilter.has(accountCode)) {
        skippedFiltered++;
        continue;
      }

      // Re-derive rather than read the last row's stored HOLDING — that running
      // balance can be stale for the trade-date rule. Identical call to the preview's.
      const holding = recomputeHoldingsSnapshot(holdingRows).HOLDING;
      if (holding <= QTY_EPS) {
        skippedFlat++;
        continue;
      }

      const dividendAmount = Math.round(holding * rate * 100) / 100;

      try {
        await zcql.executeZCQLQuery(`
          INSERT INTO Dividend_Record
          (ISIN, RecordDate, WS_Account_code, Holding, Rate, Dividend_Amount, PaymentDate, Security_Code, Status)
          VALUES (
            '${esc(isin)}',
            '${recordDateISO}',
            '${esc(accountCode)}',
            ${holding},
            ${rate},
            ${dividendAmount},
            '${paymentDateISO}',
            '${esc(securityCode)}',
            'Draft'
          )
        `);
        insertedCount++;

        /* ---- Cash_Balance_Per_Transaction: append dividend credit ---- */
        try {
          const existingCash = await zcql.executeZCQLQuery(`
            SELECT ROWID FROM Cash_Balance_Per_Transaction
            WHERE Account_Code = '${esc(accountCode)}'
              AND ISIN = '${esc(isin)}'
              AND Transaction_Date = '${recordDateISO}'
              AND Transaction_Type = 'DIVIDEND'
            LIMIT 1
          `);

          if (existingCash && existingCash.length > 0) {
            cashSkippedDup++;
          } else {
            /*
             * Insert the dividend row at its CHRONOLOGICAL position, not at
             * the end of the table. We:
             *   1. Find the latest existing row whose Transaction_Date is on
             *      or before this dividend's RecordDate; that row's Sequence
             *      is `priorSequence` and its Cash_Balance is `priorBalance`.
             *   2. Bump every later row's Sequence by 1 and add the dividend
             *      amount to its stored Cash_Balance, processed in DESCENDING
             *      Sequence order so we never collide on a uniqueness
             *      assumption.
             *   3. INSERT the dividend at Sequence = priorSequence + 1 with
             *      Cash_Balance = priorBalance + dividendAmount.
             *
             * Updating the stored Cash_Balance for shifted rows keeps the
             * chip-filtered Cash Passbook view (which displays the per-row
             * stored balance, not a recomputed running total) consistent.
             *
             * The "All" view of the passbook recomputes the running balance
             * via a Sequence-ordered carry walk in cashPassbookController.js,
             * so as long as Sequence is genuinely chronological the dividend
             * will appear inline between the trades of its RecordDate and
             * every later balance will be correct.
             */
            const priorRow = await zcql.executeZCQLQuery(`
              SELECT Cash_Balance, Sequence FROM Cash_Balance_Per_Transaction
              WHERE Account_Code = '${esc(accountCode)}'
                AND Transaction_Date <= '${recordDateISO}'
              ORDER BY Sequence DESC
              LIMIT 1
            `);

            let priorBalance = 0;
            let priorSequence = 0;
            if (priorRow && priorRow.length > 0) {
              const r = priorRow[0].Cash_Balance_Per_Transaction || priorRow[0];
              priorBalance = Number(r.Cash_Balance) || 0;
              priorSequence = Number(r.Sequence) || 0;
            }

            const newSequence = priorSequence + 1;
            const newBalance =
              Math.round((priorBalance + dividendAmount) * 100) / 100;

            const SHIFT_BATCH = ZCQL_ROW_LIMIT;
            const rowsToShift = [];
            let collectOffset = 0;
            while (true) {
              const batch = await zcql.executeZCQLQuery(`
                SELECT ROWID, Sequence, Cash_Balance
                FROM Cash_Balance_Per_Transaction
                WHERE Account_Code = '${esc(accountCode)}'
                  AND Sequence > ${priorSequence}
                ORDER BY Sequence ASC
                LIMIT ${SHIFT_BATCH} OFFSET ${collectOffset}
              `);
              if (!batch || batch.length === 0) break;
              for (const r of batch) {
                const row = r.Cash_Balance_Per_Transaction || r;
                rowsToShift.push({
                  ROWID: row.ROWID,
                  Sequence: Number(row.Sequence),
                  Cash_Balance: Number(row.Cash_Balance) || 0,
                });
              }
              if (batch.length < SHIFT_BATCH) break;
              collectOffset += SHIFT_BATCH;
            }

            // Descending order so Sequence N becomes N+1 only after Sequence N+1
            // has already become N+2 — avoids any transient duplicate.
            rowsToShift.sort((a, b) => b.Sequence - a.Sequence);

            // Track successfully shifted rows so we can roll back on failure.
            const shiftedSoFar = [];
            try {
              for (const row of rowsToShift) {
                const newSeq = row.Sequence + 1;
                const newBal =
                  Math.round((row.Cash_Balance + dividendAmount) * 100) / 100;
                await zcql.executeZCQLQuery(`
                  UPDATE Cash_Balance_Per_Transaction
                  SET Sequence = ${newSeq}, Cash_Balance = ${newBal}
                  WHERE ROWID = '${row.ROWID}'
                `);
                shiftedSoFar.push(row);
              }

              await zcql.executeZCQLQuery(`
                INSERT INTO Cash_Balance_Per_Transaction
                (Account_Code, Transaction_Type, Transaction_Date, Settlement_Date, Price, Cash_Balance, Security_Name, ISIN, Quantity, Total_Amount, STT, Sequence)
                VALUES (
                  '${esc(accountCode)}',
                  'DIVIDEND',
                  '${recordDateISO}',
                  '${recordDateISO}',
                  ${rate},
                  ${newBalance},
                  '${esc(securityCode)}',
                  '${esc(isin)}',
                  ${Math.round(Number(holding) || 0)},
                  ${dividendAmount},
                  0,
                  ${newSequence}
                )
              `);
              cashInsertedCount++;
            } catch (insertErr) {
              // Compensate: undo the shifts in ASCENDING new-sequence order so
              // restoring N+1 → N happens before restoring N+2 → N+1.
              shiftedSoFar.sort((a, b) => a.Sequence - b.Sequence);
              for (const row of shiftedSoFar) {
                try {
                  await zcql.executeZCQLQuery(`
                    UPDATE Cash_Balance_Per_Transaction
                    SET Sequence = ${row.Sequence}, Cash_Balance = ${row.Cash_Balance}
                    WHERE ROWID = '${row.ROWID}'
                  `);
                } catch (rollbackErr) {
                  console.error(
                    `Rollback failed for ROWID=${row.ROWID} (account ${accountCode}): ${rollbackErr.message}`,
                  );
                }
              }
              throw insertErr;
            }
          }
        } catch (cashErr) {
          cashErrors++;
          console.error(
            `Cash_Balance_Per_Transaction insert failed for ${accountCode}: ${cashErr.message}`,
          );
        }
      } catch (insertErr) {
        insertErrors++;
        console.error(
          `Dividend_Record insert failed for ${accountCode}: ${insertErr.message}`,
        );
      }
    }

    console.log(
      `UpdateDividendData done. Master=1, Records inserted=${insertedCount}, ` +
        `skipped(flat)=${skippedFlat}, ` +
        `skipped(filtered)=${skippedFiltered}, errors=${insertErrors}, ` +
        `CashRows inserted=${cashInsertedCount}, CashRows skipped(dup)=${cashSkippedDup}, CashRows errors=${cashErrors}`,
    );

    await zcql.executeZCQLQuery(
      `UPDATE Jobs SET status = 'COMPLETED' WHERE jobName = '${jobName}'`
    );
    context.closeWithSuccess();
  } catch (error) {
    console.error("UpdateDividendData failed:", error);
    if (jobName) {
      try {
        await zcql.executeZCQLQuery(
          `UPDATE Jobs SET status = 'FAILED' WHERE jobName = '${jobName}'`
        );
      } catch (updateErr) {
        console.error("Failed to update job status to FAILED:", updateErr);
      }
    }
    context.closeWithFailure();
  }
};
