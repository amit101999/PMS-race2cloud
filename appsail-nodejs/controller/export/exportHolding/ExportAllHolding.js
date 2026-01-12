// import { PassThrough } from "stream";
// import { getAllAccountCodesFromDatabase } from "../../../util/allAccountCodes.js";
// import { calculateHoldingsSummary } from "./analyticsController.js";

// export const exportAllData = async (req, res) => {
//   try {
//     /* ---------------- INIT ---------------- */
//     const catalystApp = req.catalystApp;
//     const stratus = catalystApp.stratus();
//     const bucket = stratus.bucket("upload-data-bucket");
//     const data = await bucket.getDetails();

//     const zcql = catalystApp.zcql();
//     const tableName = "clientIds";

//     /* ---------------- GET CLIENT IDS ---------------- */
//     const clientIds = await getAllAccountCodesFromDatabase(zcql, tableName);

//     if (!clientIds.length) {
//       return res.status(404).json({ message: "No clients found" });
//     }

//     /* ---------------- CREATE STREAM ---------------- */
//     const csvStream = new PassThrough();
//     const fileName = `all-clients-export-${Date.now()}.csv`;

//     const uploadPromise = bucket.putObject(fileName, csvStream, {
//       overwrite: true,
//       contentType: "text/csv",
//     });

//     /* ---------------- CSV HEADER ---------------- */
//     csvStream.write(
//       "ACCOUNT_CODE,SECURITY_NAME,SECURITY_CODE,ISIN,HOLDING,WAP,HOLDING_VALUE\n"
//     );

//     /* ---------------- FETCH & WRITE DATA ---------------- */
//     let count = 0;

//     for (const client of clientIds) {
//       const accountCode = client.clientIds.WS_Account_code;
//       console.log(
//         `Processing client ${count + 1}/${
//           clientIds.length
//         } account code : ${accountCode}`
//       );

//       // ✅ DIRECT FUNCTION CALL (NO HTTP)
//       const rows = await calculateHoldingsSummary({
//         catalystApp,
//         accountCode,
//       });

//       if (!Array.isArray(rows) || !rows.length) {
//         count++;
//         console.log(`No data for account code: ${accountCode}`);
//         continue;
//       }

//       for (const row of rows) {
//         const line = [
//           accountCode,
//           row.stockName ?? "",
//           row.securityCode ?? "",
//           row.isin ?? "",
//           row.currentHolding ?? "",
//           row.avgPrice ?? "",
//           row.holdingValue ?? "",
//         ]
//           .map((v) => `"${String(v).replace(/"/g, '""')}"`)
//           .join(",");

//         csvStream.write(line + "\n");
//       }

//       count++;
//     }

//     /* ---------------- CLOSE STREAM ---------------- */
//     csvStream.end();
//     await uploadPromise;

//     const downloadUrl = await bucket.generatePreSignedUrl(fileName, "GET", {
//       expiresIn: 3600, // 1 hour
//     });

//     return res.status(200).json({
//       message: "Single client export successful",
//       downloadUrl: downloadUrl,
//     });
//   } catch (error) {
//     console.error("Export error:", error);
//     return res.status(500).json({
//       message: "Internal Server Error",
//       error: error.message,
//     });
//   }
// };

export const exportAllData = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const jobScheduling = catalystApp.jobScheduling();
    await jobScheduling.JOB.submitJob({
      job_name: "ExportAll",
      jobpool_name: "Export",
      target_type: "Function",
      target_name: "ExportAllCustomerHoldingData",
    });
    res.send({ status: "queued" });
  } catch (error) {
    console.error("Error scheduling export job:", error);
    res.status(500).json({
      message: "Failed to schedule export job",
      error: error.message,
    });
  }
};
