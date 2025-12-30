export const getAllSecurityCodes = async (req, res) => {
    try {
      const app = req.catalystApp;
      if (!app) {
        return res.status(500).json({ message: "Catalyst app missing" });
      }
  
      const zcql = app.zcql();
      const limit = 270;
      let offset = 0;
      let hasNext = true;
  
      const securities = [];
  
      while (hasNext) {
        const rows = await zcql.executeZCQLQuery(`
          SELECT Security_Code, Security_Name
          FROM Security_List
          LIMIT ${limit} OFFSET ${offset}
        `);
  
        if (!rows.length) break;
  
        securities.push(
          ...rows.map(r => {
            const row = r.Security_List || r;  // Use Security_List, not Transaction
            return {
              securityCode: row.Security_Code || row.securityCode,
              securityName: row.Security_Name || row.securityName || "",
            };
          })
        );
  
        if (rows.length < limit) break;
        offset += limit;
      }
  
      return res.status(200).json({
        data: securities,
      });
  
    } catch (error) {
      console.error("getAllSecurities Error:", error);
      return res.status(500).json({
        message: "Failed to fetch securities",
        error: error.message,
      });
    }
  };
  

  export const addStockSplit = async (req, res) => {
    try {
      const zohoCatalyst = req.catalystApp;
      let zcql = zohoCatalyst.zcql();
  
      const {
        securityCode,
        securityName,
        ratio1,
        ratio2,
        issueDate,
      } = req.body;
  
      /* ===========================
         BASIC VALIDATION
         =========================== */
      if (
        !securityCode ||
        !securityName ||
        !ratio1 ||
        !ratio2 ||
        !issueDate
      ) {
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