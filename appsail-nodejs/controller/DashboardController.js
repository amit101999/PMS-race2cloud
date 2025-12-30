// DashboardController.js

export const getAllTransactions = async (req, res) => {
    try {
      const catalystApp = req.catalystApp;
  
      if (!catalystApp) {
        return res.status(500).json({
          message: "Catalyst app not initialized",
        });
      }
  
      const zcql = catalystApp.zcql();
  
      // Pagination
      const page = parseInt(req.query.page) || 1;
      const limit = parseInt(req.query.limit) || 25;
      const offset = (page - 1) * limit;
  
      /* -------------------------------
         1️⃣ Total count
      -------------------------------- */
      const countQuery = `
        SELECT COUNT(ROWID) AS total
        FROM Transaction
      `;
      const countResult = await zcql.executeZCQLQuery(countQuery);
      
      // Handle different ZCQL response formats for COUNT
      let total = 0;
      if (countResult && countResult.length > 0) {
        const firstRow = countResult[0];
        
        // Try all possible structures - ZCQL can return COUNT in various formats
        if (firstRow.Transaction) {
          // Format: { Transaction: { total: 123 } } or { Transaction: { "COUNT(ROWID)": "3" } }
          if (firstRow.Transaction.total !== undefined) {
            total = Number(firstRow.Transaction.total) || 0;
          } else if (firstRow.Transaction["COUNT(ROWID)"] !== undefined) {
            // ZCQL returns COUNT(ROWID) as a string with the literal column name as key
            total = Number(firstRow.Transaction["COUNT(ROWID)"]) || 0;
          } else {
            // Try to find any numeric value (including strings that can be converted)
            const values = Object.values(firstRow.Transaction);
            for (const val of values) {
              const num = Number(val);
              if (!isNaN(num) && num > 0) {
                total = num;
                break;
              }
            }
          }
        } else if (firstRow.total !== undefined) {
          // Format: { total: 123 }
          total = Number(firstRow.total) || 0;
        } else {
          // Format: { COUNT(ROWID): 123 } or any other structure
          // Get all values and find the first positive number (including string numbers)
          const allValues = Object.values(firstRow);
          for (const val of allValues) {
            const num = Number(val);
            if (!isNaN(num) && num > 0) {
              total = num;
              break;
            }
          }
        }
      }
  
      /* -------------------------------
         2️⃣ First 30 columns
      -------------------------------- */
      const dataQuery1 = `
        SELECT 
          ROWID, CREATORID, CREATEDTIME, MODIFIEDTIME,
          WS_client_id, WS_Account_code, TRANDATE, SETDATE,
          Tran_Type, Tran_Desc, Security_Type, Security_Type_Description,
          DETAILTYPENAME, ISIN, Security_code, Security_Name,
          EXCHG, BROKERCODE, Depositoy_Registrar, DPID_AMC,
          Dp_Client_id_Folio, BANKCODE, BANKACID, QTY,
          RATE, BROKERAGE, SERVICETAX, NETRATE,
          Net_Amount, STT
        FROM Transaction
        ORDER BY TRANDATE DESC, ROWID DESC
        LIMIT ${limit} OFFSET ${offset}
      `;
  
      const rows1 = await zcql.executeZCQLQuery(dataQuery1);
  
      // Extract ROWIDs
      const rowIds = rows1.map(row => (row.Transaction || row).ROWID);
  
      /* -------------------------------
         3️⃣ Remaining columns (split into two queries)
      -------------------------------- */
      let rows2 = [];
      let rows3 = [];

if (rowIds.length > 0) {
  const rowIdsStr = rowIds.join(",");

  // Second query: Next 30 columns (including ROWID for matching)
  const dataQuery2 = `
    SELECT 
      ROWID, TRFDATE, TRFRATE, TRFAMT,
      TOTAL_TRXNFEE, TOTAL_TRXNFEE_STAX, Txn_Ref_No, DESCMEMO,
      CHEQUENO, CHEQUEDTL, PORTFOLIOID, DELIVERYDATE,
      PAYMENTDATE, ACCRUEDINTEREST, ISSUER, ISSUERNAME,
      TDSAMOUNT, STAMPDUTY, TPMSGAIN, RMID,
      RMNAME, ADVISORID, ADVISORNAME, BRANCHID,
      BRANCHNAME, GROUPID, GROUPNAME, OWNERID,
      OWNERNAME, WEALTHADVISOR_NAME
    FROM Transaction
    WHERE ROWID IN (${rowIdsStr})
  `;
  rows2 = await zcql.executeZCQLQuery(dataQuery2);

  // Third query: Last 2 columns
  const dataQuery3 = `
    SELECT 
      ROWID, SCHEMEID, SCHEMENAME
    FROM Transaction
    WHERE ROWID IN (${rowIdsStr})
  `;
  rows3 = await zcql.executeZCQLQuery(dataQuery3);
}
  
      /* -------------------------------
         4️⃣ Merge results by ROWID
      -------------------------------- */
      const map1 = new Map();
rows1.forEach(row => {
  const r = row.Transaction || row;
  map1.set(r.ROWID, r);
});

      const map2 = new Map();
      rows2.forEach(row => {
        const r = row.Transaction || row;
        map2.set(r.ROWID, r);
      });

      const map3 = new Map();
      rows3.forEach(row => {
        const r = row.Transaction || row;
        map3.set(r.ROWID, r);
      });

      const transactions = rowIds.map(rowId => ({
        ...(map1.get(rowId) || {}),
        ...(map2.get(rowId) || {}),
        ...(map3.get(rowId) || {}),
      }));
      /* -------------------------------
         5️⃣ Final response
      -------------------------------- */
      return res.status(200).json({
        data: transactions,
        pagination: {
          page,
          limit,
          total,
          totalPages: Math.ceil(total / limit),
        },
      });
  
    } catch (error) {
      console.error("getAllTransactions error:", error);
      return res.status(500).json({
        message: "Failed to fetch transactions",
        error: error.message,
      });
    }
  };
  