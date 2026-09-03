import { computeSplitPreviewRows } from "../../util/analytics/splitPreviewCalc.js";

export const getAllSecuritiesISINs = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app not initialized" });
    }
    const zcql = app.zcql();

    const LIMIT = 300;
    let offset = 0;
    let hasMore = true;

    const securities = [];

    while (hasMore) {
      const query = `
        SELECT ISIN, Security_Code, Security_Name
        FROM Security_List
        WHERE ISIN IS NOT NULL
        LIMIT ${LIMIT} OFFSET ${offset}
      `;

      const response = await zcql.executeZCQLQuery(query);

      if (!response || response.length === 0) {
        hasMore = false;
        break;
      }

      response.forEach((row) => {
        const sec = row.Security_List;
        securities.push({
          isin: sec.ISIN,
          securityCode: sec.Security_Code,
          securityName: sec.Security_Name,
        });
      });

      offset += LIMIT;
    }

    return res.status(200).json({
      success: true,
      count: securities.length,
      data: securities,
    });
  } catch (error) {
    console.error("Error fetching securities:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to fetch security list",
    });
  }
};

export const previewStockSplit = async (req, res) => {
  try {
    if (!req.catalystApp) {
      return res.status(500).json({
        success: false,
        message: "Catalyst app not initialized",
      });
    }

    const { isin, ratio1, ratio2, issueDate } = req.body;

    const r1 = Number(ratio1);
    const r2 = Number(ratio2);

    if (!isin || !issueDate || !Number.isFinite(r1) || !Number.isFinite(r2) || r1 <= 0) {
      return res.status(400).json({
        success: false,
        message: "Invalid ISIN, ratio or date",
      });
    }

    const issueDateObj = new Date(issueDate);
    issueDateObj.setHours(0, 0, 0, 0);
    const issueDateISO = issueDateObj.toISOString().split("T")[0];

    const zcql = req.catalystApp.zcql();

    const preview = await computeSplitPreviewRows(zcql, {
      isin,
      r1,
      r2,
      issueDateISO,
    });

    return res.json({ success: true, data: preview });
  } catch (error) {
    console.error("Preview split error:", error);
    return res.status(500).json({
      success: false,
      message: error.message,
    });
  }
};

// Each RebuildHoldingtable job handles at most this many accounts. Smaller
// batches → more, shorter parallel jobs (run concurrently in the job pool),
// avoiding a single long-running job timing out as the account count grows.
const REBUILD_HOLDINGS_BATCH = 100;

export const addStockSplit = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({
        success: false,
        message: "Catalyst app not initialized",
      });
    }
    const zcql = app.zcql();

    const { securityCode, securityName, ratio1, ratio2, issueDate, isin } =
      req.body;

    if (!securityCode || !securityName || !ratio1 || !ratio2 || !issueDate || !isin) {
      return res.status(400).json({
        message: "Missing required fields (ISIN is required to scope the holdings rebuild)",
      });
    }

    const isinEsc = String(isin ?? "").replace(/'/g, "''");
    const issueDateEsc = String(issueDate ?? "").replace(/'/g, "''");

    // Idempotency guard (mirrors bonus's bonusRowExists): a split event is
    // uniquely identified by ISIN + Issue_Date + ratios. If it already exists,
    // skip the insert AND the rebuild — re-applying would otherwise insert a
    // duplicate Split row and apply the split ratio twice.
    const existingSplit = await zcql.executeZCQLQuery(`
      SELECT ROWID FROM Split
      WHERE ISIN = '${isinEsc}'
        AND Issue_Date = '${issueDateEsc}'
        AND Ratio1 = ${Number(ratio1)}
        AND Ratio2 = ${Number(ratio2)}
      LIMIT 1
    `);
    if (existingSplit && existingSplit.length > 0) {
      console.log(
        `[SplitController] Split already applied for ${isin} on ${issueDate} ` +
          `(${ratio1}:${ratio2}) — idempotent skip, no rebuild.`,
      );
      return res.status(200).json({
        success: true,
        alreadyApplied: true,
        message:
          "Split already applied for this ISIN, date and ratio — skipped (no duplicate inserted).",
      });
    }

    await zcql.executeZCQLQuery(`
      INSERT INTO Split
      (
        Security_Code,
        Security_Name,
        Ratio1,
        Ratio2,
        Issue_Date,
        ISIN
      )
      VALUES
      (
        '${securityCode}',
        '${securityName}',
        ${Number(ratio1)},
        ${Number(ratio2)},
        '${issueDate}',
        '${isin}'
      )
    `);

    const accountSet = new Set();
    let holdOffset = 0;

    while (true) {
      const batch = await zcql.executeZCQLQuery(`
        SELECT WS_Account_code
        FROM Transaction
        WHERE ISIN='${isinEsc}'
        LIMIT ${ZCQL_ROW_LIMIT} OFFSET ${holdOffset}
      `);

      if (!batch || batch.length === 0) break;
      batch.forEach((r) => accountSet.add(r.Transaction.WS_Account_code));
      if (batch.length < ZCQL_ROW_LIMIT) break;
      holdOffset += ZCQL_ROW_LIMIT;
    }

    const affectedAccounts = Array.from(accountSet);
    if (affectedAccounts.length > 0) {
      const scheduling = app.jobScheduling();
      for (let i = 0; i < affectedAccounts.length; i += REBUILD_HOLDINGS_BATCH) {
        const chunk = affectedAccounts.slice(i, i + REBUILD_HOLDINGS_BATCH);
        const catalystJobName = `H${Date.now()}${i}`.slice(0, 20);
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
          params: {
            accountCodesJson: JSON.stringify(chunk),
            isinsJson: JSON.stringify([isin]),
            source: "SplitController",
          },
        });
      }
    }

    // A bonus already applied on/after this split's date had its BonusShare
    // sized on the PRE-split holding — it is now stale. Re-run the bonus apply
    // in recompute mode (the Split row above is now in the DB, so the bonus
    // FIFO will re-size on the post-split holding and overwrite BonusShare).
    // The recompute job re-queues its own RebuildHoldingtable afterward.
    try {
      const bonusRecords = await zcql.executeZCQLQuery(`
        SELECT SecurityCode, SecurityName, ISIN, Ratio1, Ratio2, ExDate
        FROM Bonus_Record
        WHERE ISIN = '${isinEsc}' AND ExDate >= '${issueDateEsc}'
        ORDER BY ExDate ASC
      `);

      const scheduling = app.jobScheduling();
      for (const row of bonusRecords || []) {
        const b = row.Bonus_Record || row;
        const exDateISO = String(b.ExDate || "").slice(0, 10);
        if (!exDateISO) continue;

        // Unique tracking name so UpdateBonusTable's per-account SUCCESS guard
        // (scoped by jobName) does not skip a prior bonus run for this ex-date.
        const bonusJobName = `BONRC_${isin.slice(-6)}_${exDateISO}_${Date.now()}`;
        await scheduling.JOB.submitJob({
          job_name: `BRC${Date.now()}`.slice(0, 20),
          jobpool_name: "CorporateActions",
          target_name: "UpdateBonusTable",
          target_type: "Function",
          job_config: {
            number_of_retries: 5,
            retry_interval: 60 * 1000,
          },
          params: {
            isin,
            ratio1: String(Number(b.Ratio1) || 0),
            ratio2: String(Number(b.Ratio2) || 0),
            exDate: exDateISO,
            secCode: b.SecurityCode || securityCode || "",
            secName: b.SecurityName || securityName || "",
            recompute: "true",
            jobName: bonusJobName,
          },
        });
        console.log(
          `[SplitController] Queued bonus recompute for ${isin} ex-date ${exDateISO}`,
        );
      }
    } catch (recomputeErr) {
      console.error(
        "[SplitController] Failed to queue bonus recompute:",
        recomputeErr.message,
      );
    }

    return res.status(200).json({
      success: true,
      message: "Stock split applied successfully",
    });
  } catch (error) {
    console.error("Error in addStockSplit", error);
    return res.status(500).json({
      success: false,
      message: "Failed to apply stock split",
    });
  }
};
