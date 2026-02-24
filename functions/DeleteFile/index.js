/**
 * 
 * @param {import("./types/job").JobRequest} jobRequest 
 * @param {import("./types/job").Context} context 
 */

const catalyst = require('zcatalyst-sdk-node');

module.exports = async (jobRequest, context) => {

  const catalystApp = catalyst.initialize(context);
  const datastore = catalystApp.datastore();
  const zcql = catalystApp.zcql();
  const jobsTable = datastore.table('Jobs');

  const TEN_DAYS = 10;
  const IST_OFFSET = 5.5 * 60 * 60 * 1000; // IST = UTC + 5:30

  let deletedCount = 0;
  let jobRowId = null;

  try {

    console.log("Starting Jobs Table Cleanup...");

    // 🔹 Insert cleanup job row
    const insertedRow = await jobsTable.insertRow({
      jobName: "Jobs Table Cleanup",
      status: "RUNNING"
    });

    jobRowId = insertedRow.ROWID;

    // ===============================
    // 🔥 CALCULATE IST MIDNIGHT - 10 DAYS
    // ===============================

    const nowUTC = new Date();
    const nowIST = new Date(nowUTC.getTime() + IST_OFFSET);

    // Set to 12:00 AM IST
    nowIST.setHours(0, 0, 0, 0);

    // Subtract 10 days
    const thresholdIST = new Date(
      nowIST.getTime() - (TEN_DAYS * 24 * 60 * 60 * 1000)
    );

    // 🔥 Format to Catalyst datetime format: YYYY-MM-DD HH:mm:ss:SSS
    const formattedDate =
      thresholdIST.getFullYear() + '-' +
      String(thresholdIST.getMonth() + 1).padStart(2, '0') + '-' +
      String(thresholdIST.getDate()).padStart(2, '0') + ' ' +
      String(thresholdIST.getHours()).padStart(2, '0') + ':' +
      String(thresholdIST.getMinutes()).padStart(2, '0') + ':' +
      String(thresholdIST.getSeconds()).padStart(2, '0') + ':' +
      String(thresholdIST.getMilliseconds()).padStart(3, '0');

    console.log("Threshold Date (IST Midnight - 10 Days):", formattedDate);

    // ===============================
    // 🔥 DELETE OLD ROWS
    // ===============================

    const query = `
      SELECT ROWID FROM Jobs
      WHERE CREATEDTIME < '${formattedDate}'
    `;

    const response = await zcql.executeZCQLQuery(query);

    if (response && response.length > 0) {

      for (const row of response) {

        const rowId = row.Jobs.ROWID;

        // Don't delete the current cleanup row
        if (rowId === jobRowId) continue;

        console.log("Deleting row:", rowId);

        await jobsTable.deleteRow(rowId);
        deletedCount++;
      }
    }

    // ===============================
    // 🔹 UPDATE STATUS
    // ===============================

    await jobsTable.updateRow({
      ROWID: jobRowId,
      status: `SUCCESS - Deleted ${deletedCount} old rows`
    });

    console.log("Cleanup completed successfully.");
    context.closeWithSuccess();

  } catch (error) {

    console.error("Cleanup failed:", error);

    if (jobRowId) {
      await jobsTable.updateRow({
        ROWID: jobRowId,
        status: `FAILED - ${error.message}`
      });
    }

    context.closeWithFailure();
  }
};