const catalyst = require("zcatalyst-sdk-node");
const {
  QTY_EPS,
  holdingsEffectiveDate,
  recomputeHoldingsSnapshot,
  nextISODate,
} = require("./holdingsSnapshot.js");

const ZCQL_ROW_LIMIT = 270;
const REBUILD_BATCH = 400;

/** Written to JobStatusPerAccount.jobType (override via params.jobType). */
const BONUS_JOB_TYPE_DEFAULT = "BONUS_APPLY";

const esc = (v) => String(v ?? "").replace(/'/g, "''");

function rowFromJobStatusRow(r) {
  if (!r) return null;
  return r.JobStatusPerAccount || r;
}

async function getPerAccountJobStatus(zcql, jobName, accountCode) {
  try {
    const rows = await zcql.executeZCQLQuery(`
      SELECT status FROM JobStatusPerAccount
      WHERE jobName = '${esc(jobName)}' AND WS_Account_code = '${esc(accountCode)}'
      LIMIT 1
    `);
    if (!rows?.length) return "";
    const st = rowFromJobStatusRow(rows[0]);
    return String(st?.status ?? "").trim();
  } catch (e) {
    console.warn("[UpdateBonusTable][JobStatusPerAccount] read failed:", e.message);
    return "";
  }
}

async function upsertAccountRowRunning(zcql, jobName, jobType, accountCode) {
  try {
    await zcql.executeZCQLQuery(`
      INSERT INTO JobStatusPerAccount (jobType, WS_Account_code, status, lastError, jobName)
      VALUES (
        '${esc(jobType)}',
        '${esc(accountCode)}',
        'RUNNING',
        '',
        '${esc(jobName)}'
      )
    `);
  } catch (insertErr) {
    try {
      await zcql.executeZCQLQuery(`
        UPDATE JobStatusPerAccount
        SET status = 'RUNNING', lastError = '', jobType = '${esc(jobType)}'
        WHERE jobName = '${esc(jobName)}' AND WS_Account_code = '${esc(accountCode)}'
      `);
    } catch (upErr) {
      console.warn(
        `[UpdateBonusTable][JobStatusPerAccount] upsert RUNNING failed ${accountCode}:`,
        upErr.message,
      );
    }
  }
}

async function markAccountSuccess(zcql, jobName, accountCode) {
  try {
    await zcql.executeZCQLQuery(`
      UPDATE JobStatusPerAccount
      SET status = 'SUCCESS', lastError = ''
      WHERE jobName = '${esc(jobName)}' AND WS_Account_code = '${esc(accountCode)}'
    `);
  } catch (e) {
    console.warn(
      `[UpdateBonusTable][JobStatusPerAccount] SUCCESS failed ${accountCode}:`,
      e.message,
    );
  }
}

async function markAccountFailed(zcql, jobName, accountCode, errMsg) {
  const msg = esc(String(errMsg || "UNKNOWN").slice(0, 500));
  try {
    await zcql.executeZCQLQuery(`
      UPDATE JobStatusPerAccount
      SET status = 'FAILED', lastError = '${msg}'
      WHERE jobName = '${esc(jobName)}' AND WS_Account_code = '${esc(accountCode)}'
    `);
  } catch (e) {
    console.warn(
      `[UpdateBonusTable][JobStatusPerAccount] FAILED failed ${accountCode}:`,
      e.message,
    );
  }
}

/**
 * Split "Confirm & Apply" progress tracking (params.caTrackKey / caSubId,
 * set only when this bonus recompute was queued by SplitController). Flips this
 * bonus's placeholder piece in `JobStatusPerAccount` (jobName = trackKey,
 * WS_Account_code = subId) to SUCCESS / FAILED, then settles the parent `Jobs`
 * row named with the trackKey once no piece for it is still RUNNING. The rebuild
 * jobs this bonus spawns register their own pieces (see queueRebuildHoldingsJobs)
 * BEFORE this placeholder is flipped, so the parent can't read "all done" early.
 */
async function settleCaTrackPiece(zcql, trackKey, subId, outcome) {
  if (!trackKey || !subId) return;
  const pieceStatus = outcome === "SUCCESS" ? "SUCCESS" : "FAILED";
  try {
    await zcql.executeZCQLQuery(
      `UPDATE JobStatusPerAccount SET status = '${esc(pieceStatus)}', ` +
        `lastError = '${pieceStatus === "FAILED" ? "bonus recompute failed" : ""}' ` +
        `WHERE jobName = '${esc(trackKey)}' AND WS_Account_code = '${esc(subId)}'`,
    );
  } catch (e) {
    console.warn(`[UpdateBonusTable] split-track mark '${subId}' ${pieceStatus} failed:`, e.message);
    return;
  }
  try {
    const running = await zcql.executeZCQLQuery(
      `SELECT status FROM JobStatusPerAccount WHERE jobName = '${esc(trackKey)}' AND status = 'RUNNING' LIMIT 1`,
    );
    if (running && running.length) return;
    const failed = await zcql.executeZCQLQuery(
      `SELECT status FROM JobStatusPerAccount WHERE jobName = '${esc(trackKey)}' AND status = 'FAILED' LIMIT 1`,
    );
    const parentStatus = failed && failed.length ? "COMPLETED_WITH_ERRORS" : "COMPLETED";
    await zcql.executeZCQLQuery(
      `UPDATE Jobs SET status = '${parentStatus}' WHERE jobName = '${esc(trackKey)}'`,
    );
    console.log(`[UpdateBonusTable] split track '${trackKey}' finalized as ${parentStatus}`);
  } catch (e) {
    console.warn(`[UpdateBonusTable] split-track finalize '${trackKey}' failed:`, e.message);
  }
}

/**
 * A bonus event is identified by its record date. Bonuses saved before
 * Record_Date existed only have ExDate, which served as their record date.
 */
const bonusRecordDateOf = (row) => String(row.Record_Date || row.ExDate || "").slice(0, 10);

/**
 * ROWID of this account's Bonus row for this bonus event, or null.
 * Used to skip the INSERT on retry, and to find the row a split recompute overwrites.
 */
async function findBonusRowId(zcql, isin, accountCode, recordDateISO) {
  try {
    const rows = await zcql.executeZCQLQuery(`
      SELECT ROWID, Record_Date, ExDate FROM Bonus
      WHERE ISIN = '${esc(isin)}'
        AND WS_Account_code = '${esc(accountCode)}'
    `);
    const match = (rows || [])
      .map((r) => r.Bonus || r)
      .find((b) => bonusRecordDateOf(b) === recordDateISO);
    return match ? match.ROWID : null;
  } catch (e) {
    console.warn(`[UpdateBonusTable] findBonusRowId check failed:`, e.message);
    return null;
  }
}

async function ensureBonusRecord(
  zcql,
  { secCode, secName, isin, r1, r2, recordDateISO, effectiveDateISO },
) {
  // Bonus_Record also holds one row per account (WS_Account_code set) for the
  // Bonus page's custodian reconciliation; only an event row counts here.
  const existing = await zcql.executeZCQLQuery(`
    SELECT ROWID, Record_Date, ExDate FROM Bonus_Record
    WHERE ISIN = '${esc(isin)}' AND WS_Account_code IS NULL
  `);
  const alreadyThere = (existing || [])
    .map((r) => r.Bonus_Record || r)
    .some((b) => bonusRecordDateOf(b) === recordDateISO);
  if (alreadyThere) {
    console.log("[UpdateBonusTable] Bonus_Record already exists — skip INSERT (retry)");
    return;
  }
  await zcql.executeZCQLQuery(`
      INSERT INTO Bonus_Record
      (
        SecurityCode,
        SecurityName,
        ISIN,
        Ratio1,
        Ratio2,
        Record_Date,
        ExDate
      )
      VALUES
      (
        '${(secCode || "").replace(/'/g, "''")}',
        '${(secName || "").replace(/'/g, "''")}',
        '${esc(isin)}',
        ${r1},
        ${r2},
        '${esc(recordDateISO)}',
        '${esc(effectiveDateISO)}'
      )
    `);
}

/**
 * The Bonus page's custodian reconciliation: once this bonus is applied, its
 * account rows in Bonus_Record (WS_Account_code set, sent as Receivable by the
 * page) become RECEIVED. The page stored their Received Dates already.
 */
async function markBonusAccountsReceived(zcql, { isin, recordDateISO, r1, r2 }) {
  const where =
    `ISIN = '${esc(isin)}' AND Record_Date = '${esc(recordDateISO)}' ` +
    `AND Ratio1 = ${Number(r1)} AND Ratio2 = ${Number(r2)} ` +
    `AND WS_Account_code IS NOT NULL AND Status = 'RECEIVABLE'`;
  let marked = 0;
  // Bounded: if an UPDATE ever changed nothing, the same rows would come back forever.
  for (let pass = 0; pass < 1000; pass += 1) {
    const page = ((await zcql.executeZCQLQuery(
      `SELECT ROWID FROM Bonus_Record WHERE ${where} ORDER BY ROWID ASC LIMIT 200`,
    )) || []).map((r) => (r.Bonus_Record || r).ROWID);
    if (page.length === 0) return marked;
    await zcql.executeZCQLQuery(
      `UPDATE Bonus_Record SET Status = 'RECEIVED' WHERE ROWID IN (${page.join(", ")})`,
    );
    marked += page.length;
  }
  return marked;
}

async function queueRebuildHoldingsJobs(
  catalystApp,
  accountCodes,
  source,
  isins,
  caTrackKey = "",
) {
  const uniq = [...new Set(accountCodes)]
    .map((a) => String(a || "").trim())
    .filter(Boolean);
  if (!uniq.length) return;

  // Scope the rebuild to the affected ISIN(s) so each account only rebuilds
  // those ISIN(s) instead of its full holding set.
  const scopedIsins = [...new Set(isins || [])]
    .map((i) => String(i || "").trim())
    .filter(Boolean);

  const scheduling = catalystApp.jobScheduling();
  const zcql = catalystApp.zcql();
  for (let i = 0; i < uniq.length; i += REBUILD_BATCH) {
    const chunk = uniq.slice(i, i + REBUILD_BATCH);
    const catalystJobName = `H${Date.now()}${i}`.slice(0, 20);
    const jobParams = {
      accountCodesJson: JSON.stringify(chunk),
      isinsJson: JSON.stringify(scopedIsins),
      source,
    };

    // Split notification: register a piece for this chunk under the split's
    // track key BEFORE the bonus placeholder is flipped, so the parent can't
    // read "all done" while this rebuild is still pending.
    if (caTrackKey) {
      const childSubId = `${caTrackKey}_bh${Date.now()}_${i}`;
      try {
        await zcql.executeZCQLQuery(
          `INSERT INTO JobStatusPerAccount (jobType, WS_Account_code, status, lastError, jobName) ` +
            `VALUES ('SPLIT_APPLY', '${esc(childSubId)}', 'RUNNING', '', '${esc(caTrackKey)}')`,
        );
        jobParams.caTrackKey = caTrackKey;
        jobParams.caSubId = childSubId;
      } catch (e) {
        console.warn(`[UpdateBonusTable] child piece insert failed (${childSubId}):`, e.message);
      }
    }

    await scheduling.JOB.submitJob({
      job_name: catalystJobName,
      jobpool_name: "CorporateActions",
      target_name: "RebuildHoldingtable",
      target_type: "Function",
      /* Catalyst Job Pool: retries only on execution failure. Min interval 1m. */
      job_config: {
        number_of_retries: 5,
        retry_interval: 60 * 1000,
      },
      params: jobParams,
    });
  }
}
/**
 * @param {import("./types/job").JobRequest} jobRequest
 * @param {import("./types/job").Context} context
 */
module.exports = async (jobRequest, context) => {
  const catalystApp = catalyst.initialize(context);
  const zcql = catalystApp.zcql();
  let jobName = "";
  // Split "Confirm & Apply" notification: set only when SplitController queued
  // this recompute. `caSubId` is this bonus's placeholder piece.
  let caTrackKey = "";
  let caSubId = "";

  try {
    console.log("Bonus apply job started");

    const params = jobRequest.getAllJobParams();
    const { isin, secCode, secName } = params;
    const r1 = Number(params.ratio1);
    const r2 = Number(params.ratio2);
    jobName = params.jobName;
    caTrackKey = String(params.caTrackKey || "").trim();
    caSubId = String(params.caSubId || "").trim();
    const bonusJobType =
      String(params.jobType || "").trim() || BONUS_JOB_TYPE_DEFAULT;
    const trackingOn = Boolean(String(jobName || "").trim());
    // Recompute mode: a split was uploaded on/before this bonus's record date, so the
    // previously stored BonusShare was sized on the pre-split holding and is now
    // stale. In this mode an existing Bonus row is OVERWRITTEN (not skipped) with
    // a freshly sized count. The split row already exists in the DB by the time
    // this runs, so the FIFO holding below is post-split.
    const recompute =
      String(params.recompute || "").trim().toLowerCase() === "true";

    try {
      await zcql.executeZCQLQuery(
        `INSERT INTO Jobs (jobName, status) VALUES ('${esc(jobName)}', 'PENDING')`,
      );
    } catch (insertJobErr) {
      await zcql.executeZCQLQuery(
        `UPDATE Jobs SET status = 'PENDING' WHERE jobName = '${esc(jobName)}'`,
      );
    }

    // Record date decides who is eligible and when the bonus shares land in
    // holdings. `exDate` carries the effective date, stored for reference only.
    // A job queued before Record_Date existed sends only exDate, which then
    // served as the record date.
    const recordDateISO = String(params.recordDate || params.exDate || "").slice(0, 10);
    const effectiveDateISO = String(params.exDate || recordDateISO).slice(0, 10);

    console.log("[UpdateBonusTable]", {
      jobName,
      bonusJobType,
      jobStatusTracking: trackingOn,
      isin,
      recordDateISO,
      effectiveDateISO,
    });

    /* ======================================================
       STEP 1+2: READ THE HOLDINGS LEDGER AS AT THE RECORD DATE

       Same source, same date rule and same position maths as the Bonus PREVIEW
       (appsail-nodejs/util/analytics/bonusPreviewCalc.js) — one read gives both
       the eligible accounts and the rows to size each grant from.

       ===== OLD LOGIC — raw-table replay. Kept for rollback (see fifo.js in git
       history, deleted with this change). =====
       Accounts came from `SELECT WS_Account_code FROM Transaction`, then the
       Transaction / Bonus / Split tables were replayed through a local FIFO
       engine: `runFifoEngine(tx, bonuses, splits, true)`.

       That engine was never given merger or demerger inputs, so shares that
       arrived through a corporate action did not exist as far as it was
       concerned. The preview sized the grant on the client's real position while
       this job sized it on the traded position only — and when the client had
       never traded the ISIN directly (every share came out of a demerger), the
       account was not even in `eligibleAccounts` and silently got nothing.
       ===== END OLD LOGIC =====
       ====================================================== */
    const cutoff = nextISODate(recordDateISO);

    const rowsByAccount = new Map(); // WS_Account_code → effective Holdings rows
    const seenRowIds = new Set();
    let holdOffset = 0;

    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT ROWID, WS_Account_code, HOLDING, WAP, HOLDING_VALUE,
               QUANTITY, PRICE, TOTAL_AMOUNT,
               TYPE, SETTLEMENT_DATE, TRANSACTION_DATE, CA_DATE
        FROM Holdings
        WHERE ISIN = '${esc(isin)}'
          AND (SETTLEMENT_DATE < '${esc(cutoff)}' OR TRANSACTION_DATE < '${esc(cutoff)}')
        ORDER BY CREATEDTIME ASC, ROWID ASC
        LIMIT ${ZCQL_ROW_LIMIT} OFFSET ${holdOffset}
      `);

      if (!batch || batch.length === 0) break;

      for (const row of batch) {
        const h = row.Holdings || row;
        if (h.ROWID != null) {
          if (seenRowIds.has(h.ROWID)) continue;
          seenRowIds.add(h.ROWID);
        }
        const acc = String(h.WS_Account_code || "").trim();
        if (!acc) continue;

        // The SQL above deliberately ORs both dates — it cannot express the OPI
        // exception or the merger/demerger CA_DATE rule on its own. Apply the
        // per-type rule here so a row that is not yet effective on the record
        // date cannot count towards the entitlement.
        const eff = holdingsEffectiveDate(h);
        if (eff && eff >= cutoff) continue;

        // Size this bonus on the holding BEFORE this bonus is added. On a
        // recompute the stale BONUS row for this same bonus is already in the
        // ledger (the rebuild dates it by the record date); dropping it keeps
        // the basis post-split, not post-split + stale-bonus.
        if (
          String(h.TYPE || "").trim().toUpperCase() === "BONUS" &&
          eff === recordDateISO
        ) {
          continue;
        }

        if (!rowsByAccount.has(acc)) rowsByAccount.set(acc, []);
        rowsByAccount.get(acc).push(h);
      }

      if (batch.length < ZCQL_ROW_LIMIT) break;
      holdOffset += ZCQL_ROW_LIMIT;
    }

    const eligibleAccounts = [...rowsByAccount.keys()];

    if (!eligibleAccounts.length) {
      console.log("No eligible accounts found");
      await zcql.executeZCQLQuery(
        `UPDATE Jobs SET status = 'COMPLETED' WHERE jobName = '${esc(jobName)}'`,
      );
      // Nothing to rebuild for the split notification — release the placeholder.
      await settleCaTrackPiece(zcql, caTrackKey, caSubId, "SUCCESS");
      context.closeWithSuccess();
      return;
    }

    /* ======================================================
       STEP 3: SIZE + INSERT BONUS (one per eligible account)
       ====================================================== */
    let inserted = 0;
    let errorCount = 0;
    const accountsRebuild = [];

    for (const accountCode of eligibleAccounts) {
      if (trackingOn) {
        const prev = await getPerAccountJobStatus(zcql, jobName, accountCode);
        if (prev === "SUCCESS") {
          console.log(
            `[UpdateBonusTable] skip ${accountCode} — JobStatusPerAccount already SUCCESS`,
          );
          continue;
        }
        await upsertAccountRowRunning(
          zcql,
          jobName,
          bonusJobType,
          accountCode,
        );
      }

      try {
        const accRows = rowsByAccount.get(accountCode) || [];
        if (!accRows.length) {
          if (trackingOn) await markAccountSuccess(zcql, jobName, accountCode);
          continue;
        }

        // The stored HOLDING column is a running balance frozen in settlement
        // order at rebuild time, so it can contradict the rows the trade-date
        // rule selected above. Re-derive the position from the rows themselves —
        // identical call and identical helper to the preview's.
        const snap = recomputeHoldingsSnapshot(accRows);
        if (snap.HOLDING <= QTY_EPS) {
          // Fully sold (or restated away) as at the record date.
          if (trackingOn) await markAccountSuccess(zcql, jobName, accountCode);
          continue;
        }

        const bonusShares = Math.floor((snap.HOLDING * r1) / r2);
        if (bonusShares <= 0) {
          if (trackingOn) await markAccountSuccess(zcql, jobName, accountCode);
          continue;
        }

        const existingBonusRowId = await findBonusRowId(zcql, isin, accountCode, recordDateISO);
        if (existingBonusRowId) {
          if (recompute) {
            // Split-driven re-size: overwrite the stale (pre-split) BonusShare.
            await zcql.executeZCQLQuery(`
              UPDATE Bonus
              SET BonusShare = ${bonusShares}
              WHERE ROWID = ${existingBonusRowId}
            `);
            console.log(
              `[UpdateBonusTable] Bonus recomputed for ${accountCode}: ${bonusShares} shares (post-split)`,
            );
          } else {
            console.log(
              `[UpdateBonusTable] Bonus row already present for ${accountCode} — idempotent`,
            );
          }
          if (trackingOn) await markAccountSuccess(zcql, jobName, accountCode);
          accountsRebuild.push(accountCode);
          inserted++;
          continue;
        }

        await zcql.executeZCQLQuery(`
          INSERT INTO Bonus
          (
            ISIN,
            SecurityCode,
            SecurityName,
            WS_Account_code,
            BonusShare,
            Record_Date,
            ExDate
          )
          VALUES
          (
            '${esc(isin)}',
            '${(secCode || "").replace(/'/g, "''")}',
            '${(secName || "").replace(/'/g, "''")}',
            '${esc(accountCode)}',
            ${bonusShares},
            '${esc(recordDateISO)}',
            '${esc(effectiveDateISO)}'
          )
        `);

        inserted++;
        accountsRebuild.push(accountCode);
        console.log(`Bonus applied for ${accountCode}: ${bonusShares} shares`);

        if (trackingOn) await markAccountSuccess(zcql, jobName, accountCode);
      } catch (accountErr) {
        errorCount++;
        console.error(`Error applying bonus for ${accountCode}:`, accountErr);
        if (trackingOn) {
          await markAccountFailed(
            zcql,
            jobName,
            accountCode,
            accountErr.message,
          );
        }
      }
    }

    /* ======================================================
       STEP 4: BONUS_RECORD (master record, idempotent)
       ====================================================== */
    let bonusRecorded = false;
    if (inserted > 0) {
      try {
        await ensureBonusRecord(zcql, {
          secCode,
          secName,
          isin,
          r1,
          r2,
          recordDateISO,
          effectiveDateISO,
        });
        bonusRecorded = true;
      } catch (recordErr) {
        console.error("Error inserting Bonus_Record:", recordErr);
      }
    }

    // Only a complete first-time apply confirms the custodian reconciliation;
    // otherwise the accounts stay Receivable and Confirm can be run again.
    if (bonusRecorded && errorCount === 0 && !recompute) {
      try {
        const marked = await markBonusAccountsReceived(zcql, { isin, recordDateISO, r1, r2 });
        console.log(`[UpdateBonusTable] ${marked} account row(s) marked RECEIVED`);
      } catch (markErr) {
        console.error("[UpdateBonusTable] Could not mark the accounts Received:", markErr);
      }
    }

    console.log(`Bonus job completed: ${inserted} accounts updated, ${errorCount} errors`);

    if (accountsRebuild.length > 0) {
      try {
        await queueRebuildHoldingsJobs(
          catalystApp,
          accountsRebuild,
          "UpdateBonusTable",
          [isin],
          caTrackKey,
        );
        console.log(
          `Queued RebuildHoldingtable for ${accountsRebuild.length} account(s)`,
        );
      } catch (rebuildErr) {
        console.error("Failed to queue RebuildHoldingtable:", rebuildErr);
        await zcql.executeZCQLQuery(
          `UPDATE Jobs SET status = 'FAILED' WHERE jobName = '${esc(jobName)}'`,
        );
        await settleCaTrackPiece(zcql, caTrackKey, caSubId, "FAILURE");
        context.closeWithFailure();
        return;
      }
    }

    // The rebuild jobs above carry their own split-notification pieces now, so
    // this bonus's placeholder can hand off. (If the bonus hit per-account
    // errors, mark the piece FAILED so the parent settles COMPLETED_WITH_ERRORS.)
    await settleCaTrackPiece(
      zcql,
      caTrackKey,
      caSubId,
      errorCount > 0 ? "FAILURE" : "SUCCESS",
    );

    if (trackingOn && errorCount > 0) {
      await zcql.executeZCQLQuery(
        `UPDATE Jobs SET status = 'FAILED' WHERE jobName = '${esc(jobName)}'`,
      );
      context.closeWithFailure();
      return;
    }

    await zcql.executeZCQLQuery(
      `UPDATE Jobs SET status = 'COMPLETED' WHERE jobName = '${esc(jobName)}'`,
    );
    context.closeWithSuccess();
  } catch (error) {
    console.error("Bonus apply job failed:", error);

    try {
      await zcql.executeZCQLQuery(
        `UPDATE Jobs SET status = 'FAILED' WHERE jobName = '${esc(jobName)}'`,
      );
    } catch (updateErr) {
      console.error("Failed to update job status to FAILED:", updateErr);
    }
    try {
      await settleCaTrackPiece(zcql, caTrackKey, caSubId, "FAILURE");
    } catch (trackErr) {
      console.error("Failed to settle split-track piece:", trackErr);
    }
    context.closeWithFailure();
  }
};
