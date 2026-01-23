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

export const addStockBonus = async (req, res) => {
  try {
    const zohoCatalyst = req.catalystApp;
    let zcql = zohoCatalyst.zcql();

    const { securityCode, securityName, /* there is no fields in the table */ } = req.body;
    console.log(req.body);

    if (!securityCode || !securityName /* there is no fields in the table */) {
      return res.status(400).json({
        message: "Missing required fields",
      });
    }

    await zcql.executeZCQLQuery(`
        INSERT INTO Bonus
        (
          Security_Code,
          Security_Name,
          /* there is no fields in the table */
        )
        VALUES
        (
          '${securityCode}',
          '${securityName}',
            /* there is no fields in the table */
        )
      `);

    return res.status(200).json({
      message: "Stock bonus saved successfully",
    });
  } catch (error) {
    console.log("Error in saving bonus", error);
    res.status(400).json({ error: error.message });
  }
};