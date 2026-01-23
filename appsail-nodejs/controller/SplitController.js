// export const getAllSecurityCodes = async (req, res) => {
//   try {
//     const app = req.catalystApp;
//     if (!app) {
//       return res.status(500).json({ message: "Catalyst app missing" });
//     }

//     const zcql = app.zcql();
//     const limit = 270;
//     let offset = 0;
//     let hasNext = true;

//     const securities = [];

//     while (hasNext) {
//       const rows = await zcql.executeZCQLQuery(`
//           SELECT Security_Code, Security_Name
//           FROM Security_List
//           LIMIT ${limit} OFFSET ${offset}
//         `);

//       if (!rows.length) break;

//       securities.push(
//         ...rows.map((r) => {
//           const row = r.Security_List || r; // Use Security_List, not Transaction
//           return {
//             securityCode: row.Security_Code || row.securityCode,
//             securityName: row.Security_Name || row.securityName || "",
//           };
//         })
//       );

//       if (rows.length < limit) break;
//       offset += limit;
//     }

//     return res.status(200).json({
//       data: securities,
//     });
//   } catch (error) {
//     console.error("getAllSecurities Error:", error);
//     return res.status(500).json({
//       message: "Failed to fetch securities",
//       error: error.message,
//     });
//   }
// };

// controllers/securityController.js

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

export const addStockSplit = async (req, res) => {
  try {
    const zohoCatalyst = req.catalystApp;
    let zcql = zohoCatalyst.zcql();

    const { securityCode, securityName, ratio1, ratio2, issueDate } = req.body;
    console.log(req.body);

    if (!securityCode || !securityName || !ratio1 || !ratio2 || !issueDate) {
      return res.status(400).json({
        message: "Missing required fields",
      });
    }

    /* ===========================
         INSERT INTO SPLIT TABLE
         =========================== */
    await zcql.executeZCQLQuery(`
        INSERT INTO Split
        (
          Security_Code,
          Security_Name,
          Ratio1,
          Ratio2,
          Issue_Date
        )
        VALUES
        (
          '${securityCode}',
          '${securityName}',
          ${Number(ratio1)},
          ${Number(ratio2)},
          '${issueDate}'
        )
      `);

    return res.status(200).json({
      message: "Stock split saved successfully",
    });
  } catch (error) {
    console.log("Error in saving split", error);
    res.status(400).json({ error: error.message });
  }
};
