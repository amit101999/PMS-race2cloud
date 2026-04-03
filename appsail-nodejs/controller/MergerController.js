import {
  escSql,
  fetchAccountsForIsin,
  fifoCardForIsinOnRecordDate,
  newMergedSharesFromRatio,
} from "../util/merger/mergerEngine.js";

/** Value for Merger.Tran_Type (transaction-style type code). */
const MERGER_TRAN_TYPE = "MERGER";

/**
 * Calendar date for DB (no timezone shift). HTML date inputs send yyyy-mm-dd — must not use
 * new Date(s).toISOString() or setHours+toISOString (1 Apr → 31 Mar in IST, etc.).
 */
function toIsoDate(d) {
  if (d == null || d === "") return null;
  const s = String(d).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  const dt = new Date(d);
  if (Number.isNaN(dt.getTime())) return null;
  const y = dt.getUTCFullYear();
  const m = String(dt.getUTCMonth() + 1).padStart(2, "0");
  const day = String(dt.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function normalizeBodyOldIsins(oldCompanies) {
  if (!Array.isArray(oldCompanies)) return [];
  const out = [];
  const seen = new Set();
  for (const c of oldCompanies) {
    const isin = String(c?.isin ?? "")
      .trim()
      .toUpperCase();
    if (!isin || seen.has(isin)) continue;
    seen.add(isin);
    out.push(isin);
  }
  return out;
}

/** Read merge-into fields from JSON (camelCase + common Catalyst-style keys). */
function mergeIntoNewCompanyFields(body) {
  const mic = body?.mergeIntoNewCompany;
  if (!mic || typeof mic !== "object") {
    return { isin: "", securityCode: "", securityName: "" };
  }
  const isin = String(mic.isin ?? mic.ISIN ?? "").trim().toUpperCase();
  const securityCode = String(
    mic.securityCode ?? mic.Security_Code ?? mic.security_code ?? "",
  ).trim();
  const securityName = String(
    mic.securityName ?? mic.Security_Name ?? mic.security_name ?? "",
  ).trim();
  return { isin, securityCode, securityName };
}

/**
 * Single old ISIN → new ISIN. mergedQty = floor(holding × ratio1 ÷ ratio2) (same as bonus).
 * @returns {{ ok: true, recordISO, newIsin, oldIsins, r1, r2, preview, meta } | { ok: false, status: number, body: object }}
 */
async function computeMergerPreview(zcql, body) {
  const { recordDateIso, ratio1, ratio2, oldCompanies } = body || {};
  const r1 = Number(ratio1);
  const r2 = Number(ratio2);
  const recordISO = toIsoDate(recordDateIso);
  const oldIsins = normalizeBodyOldIsins(oldCompanies);
  const { isin: newIsin } = mergeIntoNewCompanyFields(body);

  if (!recordISO) {
    return { ok: false, status: 400, body: { success: false, message: "recordDateIso is required (yyyy-mm-dd)." } };
  }
  if (oldIsins.length !== 1) {
    return {
      ok: false,
      status: 400,
      body: {
        success: false,
        message: "Exactly one old ISIN is required (select or type one security).",
      },
    };
  }
  if (!newIsin) {
    return { ok: false, status: 400, body: { success: false, message: "mergeIntoNewCompany.isin is required." } };
  }
  if (oldIsins[0] === newIsin) {
    return { ok: false, status: 400, body: { success: false, message: "New ISIN must differ from old ISIN." } };
  }
  if (!Number.isFinite(r1) || !Number.isFinite(r2) || r1 <= 0 || r2 <= 0) {
    return {
      ok: false,
      status: 400,
      body: { success: false, message: "ratio1 and ratio2 must be positive numbers." },
    };
  }

  const oldIsin = oldIsins[0];
  const accountSet = await fetchAccountsForIsin(zcql, oldIsin);
  const eligibleAccounts = Array.from(accountSet);
  if (!eligibleAccounts.length) {
    return {
      ok: true,
      recordISO,
      newIsin,
      oldIsins,
      r1,
      r2,
      preview: [],
      meta: { message: "No accounts with transactions on this old ISIN." },
    };
  }

  const fifoCard = await fifoCardForIsinOnRecordDate(zcql, oldIsin, recordISO);
  const preview = [];

  for (const accountCode of eligibleAccounts) {
    const fifo = fifoCard(accountCode);
    const holdings = Number(fifo?.holdings) || 0;
    const carriedCost = Number(fifo?.holdingValue) || 0;
    const newQty = newMergedSharesFromRatio(holdings, r1, r2);
    const mergedWAP = newQty > 0 ? carriedCost / newQty : 0;

    if (newQty <= 0 && carriedCost <= 0) continue;

    preview.push({
      accountCode,
      oldIsin,
      newIsin,
      ratio1: r1,
      ratio2: r2,
      holdingsOnRecordDate: holdings,
      totalNewShares: newQty,
      totalCarriedCost: carriedCost,
      mergedWAP,
      legs: [
        {
          oldIsin,
          holdingsOnRecordDate: holdings,
          carriedCost,
          ratio1: r1,
          ratio2: r2,
          newSharesFromLeg: newQty,
        },
      ],
      holdingsOldIsin1: holdings,
      holdingsOldIsin2: null,
    });
  }

  return {
    ok: true,
    recordISO,
    newIsin,
    oldIsins,
    r1,
    r2,
    preview,
    meta: {
      recordDateIso: recordISO,
      newIsin,
      oldIsins,
      note: "mergedQty = floor(holdings * ratio1 / ratio2), same as bonus. COA = FIFO on old ISIN. mergedWAP = COA / mergedQty. Apply persists TRANDATE/SETDATE = recordDateIso only.",
    },
  };
}

/**
 * POST /api/merger/preview
 * Body: { recordDateIso, ratio1, ratio2, oldCompanies: [{ isin }], mergeIntoNewCompany: { isin, securityName?, securityCode? } }
 * Exactly one old ISIN. mergedQty = floor(holdings * ratio1 / ratio2).
 */
export const previewMerger = async (req, res) => {
  try {
    if (!req.catalystApp) {
      return res.status(500).json({ success: false, message: "Catalyst app not initialized" });
    }
    const zcql = req.catalystApp.zcql();
    const result = await computeMergerPreview(zcql, req.body);
    if (!result.ok) {
      return res.status(result.status).json(result.body);
    }
    return res.json({
      success: true,
      data: result.preview,
      meta: result.meta,
    });
  } catch (error) {
    console.error("[previewMerger]", error);
    return res.status(500).json({ success: false, message: error.message || "Merger preview failed" });
  }
};

/**
 * POST /api/merger/apply
 * Body: same as preview (effectiveDateIso on the client is ignored here).
 * Merger + Merger_Record: TRANDATE and SETDATE both = user-selected record date only (recordDateIso → result.recordISO).
 */
export const applyMerger = async (req, res) => {
  try {
    if (!req.catalystApp) {
      return res.status(500).json({ success: false, message: "Catalyst app not initialized" });
    }

    const mic = mergeIntoNewCompanyFields(req.body);
    const secName = mic.securityName;
    const newIsin = mic.isin;
    const secCode = mic.securityCode || newIsin;

    const zcql = req.catalystApp.zcql();
    const result = await computeMergerPreview(zcql, req.body);
    if (!result.ok) {
      return res.status(result.status).json(result.body);
    }

    const recordISO = result.recordISO;
    const effectiveISO = toIsoDate(req.body.effectiveDateIso) || recordISO;
    const resolvedSecName = secName || `Merged ${newIsin}`;
    const rows = result.preview || [];
    const actionable = rows.filter((r) => r.totalNewShares > 0);
    if (!actionable.length) {
      return res.status(400).json({
        success: false,
        message: "No accounts with positive merged share quantity; nothing to apply.",
      });
    }

    let securityListInserted = false;
    let securityListUpdated = false;
    let securityListReason = null;
    let securityListTargetWasUpdate = false;
    try {
      const existingByIsin = await zcql.executeZCQLQuery(`
        SELECT ROWID FROM Security_List WHERE ISIN='${escSql(newIsin)}' LIMIT 1
      `);
      if (existingByIsin?.length) {
        securityListTargetWasUpdate = true;
        await zcql.executeZCQLQuery(`
          UPDATE Security_List
          SET Security_Code = '${escSql(secCode)}', Security_Name = '${escSql(resolvedSecName)}'
          WHERE ISIN = '${escSql(newIsin)}'
        `);
        securityListUpdated = true;
        securityListReason = "isin_updated_in_security_list";
      } else {
        await zcql.executeZCQLQuery(`
          INSERT INTO Security_List (Security_Code, ISIN, Security_Name)
          VALUES ('${escSql(secCode)}', '${escSql(newIsin)}', '${escSql(resolvedSecName)}')
        `);
        securityListInserted = true;
        securityListReason = "security_list_inserted";
      }
    } catch (e) {
      securityListReason = securityListTargetWasUpdate
        ? "security_list_update_failed"
        : "security_list_insert_failed";
      console.warn("[applyMerger] Security_List insert/update failed:", e.message);
    }

    const oldIsin = result.oldIsins[0] || "";
    const r1 = result.r1;
    const r2 = result.r2;

    try {
      await zcql.executeZCQLQuery(`
        INSERT INTO Merger_Record
        (
          ISIN,
          Security_Code,
          Security_Name,
          OldISIN,
          Ratio1,
          Ratio2,
          TRANDATE,
          SETDATE
        )
        VALUES
        (
          '${escSql(newIsin)}',
          '${escSql(secCode)}',
          '${escSql(resolvedSecName)}',
          '${escSql(oldIsin)}',
          ${r1},
          ${r2},
          '${escSql(effectiveISO)}',
          '${escSql(effectiveISO)}'
        )
      `);
    } catch (e) {
      console.error("[applyMerger] Merger_Record insert failed:", e);
      return res.status(500).json({
        success: false,
        message:
          "Merger_Record insert failed. Create table Merger_Record with columns: ISIN, Security_Code, Security_Name, OldISIN, Ratio1, Ratio2, TRANDATE, SETDATE. TRANDATE and SETDATE both store the merger record date (recordDateIso).",
        detail: e.message,
      });
    }

    let inserted = 0;
    const errors = [];
    for (const row of actionable) {
      try {
        const avg = Number(row.mergedWAP) || 0;
        const cost = Number(row.totalCarriedCost) || 0;
        const qty = Number(row.totalNewShares) || 0;
        const holdingValue = cost;
        await zcql.executeZCQLQuery(`
          INSERT INTO Merger
          (
            ISIN,
            SecurityCode,
            Security_Name,
            WS_Account_code,
            Holding,
            OldISIN,
            WAP,
            Total_Amount,
            TRANDATE,
            SETDATE,
            Tran_Type,
            Quantity,
            HoldingValue
          )
          VALUES
          (
            '${escSql(newIsin)}',
            '${escSql(secCode)}',
            '${escSql(resolvedSecName)}',
            '${escSql(row.accountCode)}',
            ${qty},
            '${escSql(oldIsin)}',
            ${avg},
            ${cost},
            '${escSql(effectiveISO)}',
            '${escSql(effectiveISO)}',
            '${escSql(MERGER_TRAN_TYPE)}',
            ${qty},
            ${holdingValue}
          )
        `);
        inserted++;
      } catch (e) {
        errors.push({ accountCode: row.accountCode, message: e.message });
      }
    }

    if (inserted === 0 && errors.length) {
      return res.status(500).json({
        success: false,
        message:
          "Merger row insert failed. Create table Merger with columns: ISIN, SecurityCode, Security_Name, WS_Account_code, Holding, OldISIN, WAP, Total_Amount, TRANDATE, SETDATE, Tran_Type, Quantity, HoldingValue. TRANDATE and SETDATE both store the merger record date (recordDateIso).",
        errors,
      });
    }

    return res.json({
      success: true,
      message: `Merger applied: ${inserted} account row(s), Merger_Record created.`,
      inserted,
      securityListInserted,
      securityListUpdated,
      securityListReason,
      applyErrors: errors.length ? errors : undefined,
    });
  } catch (error) {
    console.error("[applyMerger]", error);
    return res.status(500).json({ success: false, message: error.message || "Merger apply failed" });
  }
};
