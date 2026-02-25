"use strict";

const catalyst = require("zcatalyst-sdk-node");

const esc = (v) => String(v ?? "").replace(/'/g, "''");

const isValid = (v) =>
  v != null && v !== "" && String(v).toLowerCase() !== "null";

// Tran_Type -> executionPriority
const EXECUTION_PRIORITY_MAP = {
  "CS-": 0, "NF-": 0,
  "CS+": 1, "CSI": 1, "CUS": 1, "MGE": 1, "MGF": 1, "SQB": 1, "SQS": 1, "PRF": 1,
  "BY-": 2, "IN+": 2, "IN1": 2, "TDI": 2, "DI1": 2, "OI1": 2, "OPI": 2, "OPO": 2, "E01": 2, "E10": 2, "E22": 2, "E23": 2,
  "SL+": 3, "TDO": 3, "DIO": 3, "RD0": 3,
};

const getExecutionPriority = (tranType) => {
  if (!isValid(tranType)) return null;
  const key = String(tranType).trim().toUpperCase();
  return EXECUTION_PRIORITY_MAP[key] ?? null;
};

module.exports = async (event, context) => {
  try {
    const RAW_DATA = event.getRawData();
    const catalystApp = catalyst.initialize(context);
    const zcql = catalystApp.zcql();

    const events = RAW_DATA?.events || [];

    const processedTranTypes = new Set();

    for (const ev of events) {
      const d = ev?.data || {};

      const wsClientId = d.WS_client_id ?? d.ws_client_id;
      const wsAccountCode = d.WS_Account_code ?? d.ws_account_code;
      const securityCode = d.Security_code ?? d.Security_Code;
      const isin = d.ISIN ?? d.isin;
      const securityName = d.Security_Name ?? d.security_name;
      const tranType = d.Tran_Type ?? d.tran_type;

      /* ---------------- CLIENT INSERT (Avoid Duplicate) ---------------- */
      if (isValid(wsClientId) && isValid(wsAccountCode)) {

        const existingClient = await zcql.executeZCQLQuery(`
          SELECT ROWID FROM clientIds
          WHERE WS_client_id = '${esc(wsClientId)}'
        `);

        if (!existingClient.length) {
          await zcql.executeZCQLQuery(`
            INSERT INTO clientIds (WS_client_id, WS_Account_code)
            VALUES ('${esc(wsClientId)}', '${esc(wsAccountCode)}')
          `);
        }
      }

      /* ---------------- SECURITY INSERT (Avoid Duplicate) ---------------- */
      if (isValid(securityCode) || isValid(isin)) {

        const existingSecurity = await zcql.executeZCQLQuery(`
          SELECT ROWID FROM Security_List
          WHERE Security_Code = '${esc(securityCode || "")}'
        `);

        if (!existingSecurity.length) {
          await zcql.executeZCQLQuery(`
            INSERT INTO Security_List (Security_Code, ISIN, Security_Name)
            VALUES (
              '${esc(securityCode || "")}',
              '${esc(isin || "")}',
              '${esc(securityName || "")}'
            )
          `);
        }
      }

      /* ---------------- UPDATE TRANSACTION PRIORITY (NULL Only + 300 Limit Safe) ---------------- */
      if (isValid(tranType)) {

        const typeKey = String(tranType).trim().toUpperCase();
        const priority = getExecutionPriority(typeKey);

        if (priority !== null && !processedTranTypes.has(typeKey)) {

          let hasMore = true;

          while (hasMore) {

            // Fetch max 300 NULL priority rows
            const rows = await zcql.executeZCQLQuery(`
              SELECT ROWID FROM Transaction
              WHERE Tran_Type = '${esc(typeKey)}'
              AND executionPriority IS NULL
              LIMIT 300
            `);

            if (!rows || rows.length === 0) {
              hasMore = false;
              break;
            }

            for (const row of rows) {
              const rowId = row.Transaction.ROWID;

              await zcql.executeZCQLQuery(`
                UPDATE Transaction
                SET executionPriority = ${priority}
                WHERE ROWID = '${rowId}'
              `);
            }
          }

          processedTranTypes.add(typeKey);
        }
      }
    }

    context.closeWithSuccess();

  } catch (err) {
    console.error("UpdateSecurity_ClientMaster error:", err);
    context.closeWithFailure();
  }
};