/* ===========================
   GET ALL SECURITIES
   =========================== */
export const getAllSecuritiesISINs = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({
        success: false,
        message: "Catalyst app not initialized",
      });
    }

    const zcql = app.zcql();
    const LIMIT = 300;
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
        const sec = r.Security_List;
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
    console.error(error);
    return res.status(500).json({
      success: false,
      message: "Failed to fetch security list",
    });
  }
};

/* ===========================
   PREVIEW BONUS
   =========================== */
// export const previewStockBonus = async (req, res) => {
//   try {
//     const { isin, ratio1, ratio2 } = req.body || {};

//     const r1 = Number(ratio1);
//     const r2 = Number(ratio2);

//     // ✅ ratio1 must NOT be zero
//     if (!isin || !Number.isFinite(r1) || !Number.isFinite(r2) || r1 <= 0) {
//       return res.status(400).json({
//         success: false,
//         message: "Ratio 1 must be greater than zero",
//       });
//     }

//     const multiplier = r2 / r1;
//     const zcql = req.catalystApp.zcql();

//     // ✅ ONLY accounts with existing bonus
//     const rows = await zcql.executeZCQLQuery(`
//       SELECT
//         ROWID,
//         WS_Account_code,
//         OWNERNAME,
//         SCHEMENAME,
//         EXCHG,
//         BonusShare
//       FROM Bonus
//       WHERE ISIN = '${isin}'
//         AND BonusShare > 0
//     `);

//     const previewData = rows
//       .map((r) => {
//         const row = r.Bonus || r;

//         const oldBonus = Number(row.BonusShare);
//         const newBonus = Math.floor(oldBonus * multiplier);

//         // ❌ skip no-change rows
//         if (newBonus === oldBonus) return null;

//         return {
//           rowId: row.ROWID, // unique → no duplicates
//           accountCode: row.WS_Account_code,
//           accountName: row.OWNERNAME,
//           schemeName: row.SCHEMENAME,
//           exchange: row.EXCHG,
//           oldBonusShare: oldBonus,
//           newBonusShare: newBonus,
//           delta: newBonus - oldBonus,
//         };
//       })
//       .filter(Boolean);

//     // ✅ empty accounts message
//     if (previewData.length === 0) {
//       return res.status(200).json({
//         success: true,
//         data: [],
//         message: "No accounts available for this bonus ratio",
//       });
//     }

//     return res.status(200).json({
//       success: true,
//       count: previewData.length,
//       data: previewData,
//     });
//   } catch (error) {
//     console.error("Preview bonus error:", error);
//     return res.status(500).json({
//       success: false,
//       message: error.message,
//     });
//   }
// };
export const previewStockBonus = async (req, res) => {
  try {
    const { isin, ratio1, ratio2 } = req.body || {};

    const r1 = Number(ratio1);
    const r2 = Number(ratio2);

    if (!isin || !Number.isFinite(r1) || !Number.isFinite(r2) || r1 <= 0) {
      return res.status(400).json({
        success: false,
        message: "Invalid ISIN or ratios",
      });
    }

    const multiplier = r2 / r1;
    const zcql = req.catalystApp.zcql();

    /* ===========================
       CALCULATE NET HOLDINGS
       =========================== */
    const rows = await zcql.executeZCQLQuery(`
      SELECT
        WS_Account_code,
        OWNERNAME,
        SCHEMENAME,
        EXCHG,
        SUM(
          CASE
            WHEN LOWER(Tran_Type) = 'buy'  THEN QTY
            WHEN LOWER(Tran_Type) = 'sell' THEN -QTY
            ELSE 0
          END
        ) AS NET_QTY
      FROM Transaction
      WHERE ISIN = '${isin.replace(/'/g, "''")}'
      GROUP BY WS_Account_code, OWNERNAME, SCHEMENAME, EXCHG
      HAVING
        SUM(
          CASE
            WHEN LOWER(Tran_Type) = 'buy'  THEN QTY
            WHEN LOWER(Tran_Type) = 'sell' THEN -QTY
            ELSE 0
          END
        ) > 0
    `);

    const previewData = rows.map((r) => {
      const row = r.Transaction || r;

      const oldQty = Number(row.NET_QTY);
      const newQty = Math.floor(oldQty * multiplier);

      return {
        accountCode: row.WS_Account_code,
        accountName: row.OWNERNAME,
        schemeName: row.SCHEMENAME,
        exchange: row.EXCHG,
        oldQty,
        newQty,
        delta: newQty - oldQty,
      };
    });

    if (previewData.length === 0) {
      return res.status(200).json({
        success: true,
        data: [],
        message: "No affected accounts found for this ISIN",
      });
    }

    return res.status(200).json({
      success: true,
      count: previewData.length,
      data: previewData,
    });
  } catch (error) {
    console.error("Preview bonus error:", error);
    return res.status(500).json({
      success: false,
      message: error.message,
    });
  }
};

/* ===========================
   APPLY BONUS
   =========================== */
export const applyStockBonus = async (req, res) => {
  try {
    const { isin, ratio1, ratio2, exDate } = req.body || {};

    const r1 = Number(ratio1);
    const r2 = Number(ratio2);

    if (
      !isin ||
      !Number.isFinite(r1) ||
      !Number.isFinite(r2) ||
      r1 <= 0 ||
      !exDate
    ) {
      return res.status(400).json({
        success: false,
        message: "Invalid input values",
      });
    }

    const multiplier = r2 / r1;
    const zcql = req.catalystApp.zcql();

    const rows = await zcql.executeZCQLQuery(`
      SELECT ROWID, BonusShare
      FROM Bonus
      WHERE ISIN = '${isin}'
        AND BonusShare > 0
    `);

    let updatedCount = 0;

    for (const r of rows) {
      const row = r.Bonus || r;

      const oldBonus = Number(row.BonusShare);
      const newBonus = Math.floor(oldBonus * multiplier);

      if (newBonus === oldBonus) continue;

      await zcql.executeZCQLQuery(`
        UPDATE Bonus
        SET BonusShare = ${newBonus},
            ExDate = '${exDate}'
        WHERE ROWID = ${row.ROWID}
      `);

      updatedCount++;
    }

    return res.status(200).json({
      success: true,
      updatedAccounts: updatedCount,
      message:
        updatedCount === 0
          ? "No accounts required bonus update"
          : "Bonus applied successfully",
    });
  } catch (error) {
    console.error("Apply bonus error:", error);
    return res.status(500).json({
      success: false,
      message: error.message,
    });
  }
};
