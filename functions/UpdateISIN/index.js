"use strict";

const catalyst = require("zcatalyst-sdk-node");

const esc = (v) => String(v ?? "").replace(/'/g, "''");

const TABLES_WITH_ISIN_COLUMN = [
  "Security_List",
  "Transaction",
  "Bonus",
  "Bonus_Record",
  "Split",
  "Dividend",
  "Temp_Transaction",
  "Temp_Custodian",
  "Bhav_Copy",
  "Cash_Balance_Per_Transaction",
];

/**
 * @param {import("./types/job").JobRequest} jobRequest
 * @param {import("./types/job").Context} context
 */
module.exports = async (jobRequest, context) => {
  const catalystApp = catalyst.initialize(context);
  const zcql = catalystApp.zcql();

  try {
    const { old_isin, new_isin } = jobRequest.getAllJobParams();

    console.log(`[UpdateISIN] Started: "${old_isin}" → "${new_isin}"`);

    if (!old_isin || !new_isin) {
      throw new Error("Parameters old_isin and new_isin are required");
    }
    if (old_isin === new_isin) {
      throw new Error("Old and new ISIN must differ");
    }

    const oldSql = esc(old_isin);
    const newSql = esc(new_isin);

    for (const table of TABLES_WITH_ISIN_COLUMN) {
      try {
        await zcql.executeZCQLQuery(
          `UPDATE ${table} SET ISIN = '${newSql}' WHERE ISIN = '${oldSql}'`
        );
        console.log(`[UpdateISIN] Updated table: ${table}`);
      } catch (tableErr) {
        console.warn(`[UpdateISIN] Skipped table "${table}":`, tableErr?.message);
      }
    }

    console.log(`[UpdateISIN] Done: "${old_isin}" → "${new_isin}"`);
    context.closeWithSuccess();
  } catch (err) {
    console.error("[UpdateISIN] Fatal Error:", err.message);
    context.closeWithFailure();
  }
};
