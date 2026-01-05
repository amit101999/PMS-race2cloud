import { PassThrough } from "stream";
import { getAllAccountCodesFromDatabase } from "../util/allAccountCodes.js";

// export const exportAllData = async (req, res) => {
//   try {
//     const catalystApp = req.catalystApp;
//     const stratus = catalystApp.stratus();
//     const bucket = stratus.bucket("upload-data-bucket");
//     const data = await bucket.getDetails();
//     const zohoCatalyst = req.catalystApp;
//     let zcql = zohoCatalyst.zcql();
//     let tableName = "clientIds";

//     let clientData = [];
//     const cliendIds = await getAllAccountCodesFromDatabase(zcql, tableName);
//     let count = 0;
//     for (let clientid of cliendIds) {
//       if (count >= 3) break; // Limit to first 5 clients for testing
//       const id = clientid.clientIds.WS_Account_code;
//       const URL =
//         "https://backend-10114672040.development.catalystappsail.com/api/analytics";
//       const data = await fetch(
//         `${URL}/getHoldingsSummarySimple?accountCode=${id}`
//       );
//       const json = await data.json();
//       const holding = [...json, id];
//       clientData.push(holding);
//       count++;
//     }

//     res.status(200).send({ data: clientData });
//   } catch (error) {
//     return res
//       .status(500)
//       .send({ message: "Internal Server Error", error: error.message });
//   }
// };

export const exportAllData = async (req, res) => {
  try {
    /* ---------------- INIT ---------------- */
    const catalystApp = req.catalystApp;
    const stratus = catalystApp.stratus();
    const bucket = stratus.bucket("upload-data-bucket");
    const data = await bucket.getDetails();
    const zohoCatalyst = req.catalystApp;
    let zcql = zohoCatalyst.zcql();
    let tableName = "clientIds";

    /* ---------------- GET CLIENT IDS ---------------- */
    const clientIds = await getAllAccountCodesFromDatabase(zcql, tableName);

    if (!clientIds.length) {
      return res.status(404).json({ message: "No clients found" });
    }

    /* ---------------- CREATE STREAM ---------------- */
    const csvStream = new PassThrough();
    const fileName = `all-clients-export-${Date.now()}.csv`;

    // Upload stream to Stratus (DOCUMENTED API)
    const uploadPromise = bucket.putObject(fileName, csvStream, {
      overwrite: true,
      contentType: "text/csv",
    });

    /* ---------------- CSV HEADER ---------------- */
    csvStream.write(
      "ACCOUNT_CODE,SECURITY_NAME,SECURITY_CODE,HOLDING,WAP,HOLDING_VALUE,\n"
    );

    /* ---------------- FETCH & WRITE DATA ---------------- */
    const BASE_URL =
      "https://backend-10114672040.development.catalystappsail.com/api/analytics";

    let count = 0;

    for (const client of clientIds) {
      if (count >= 1) break; // testing limit (remove later)

      const accountCode = client.clientIds.WS_Account_code;

      const response = await fetch(
        `${BASE_URL}/getHoldingsSummarySimple?accountCode=${accountCode}`
      );

      if (!response.ok) continue;

      const rows = await response.json();

      for (const row of rows) {
        const line = [
          accountCode,
          row.stockName ?? "",
          row.securityCode ?? "",
          row.currentHolding ?? "",
          row.avgPrice ?? "",
          row.holdingValue ?? "",
        ]
          .map((v) => `"${v}"`)
          .join(",");
        csvStream.write(line + "\n");
      }

      count++;
    }

    /* ---------------- CLOSE STREAM ---------------- */
    csvStream.end();

    // wait until upload finishes
    await uploadPromise;
    res.status(200).json({
      message: "Export successful",
      fileName: fileName,
      bucketDetails: data,
    });
  } catch (error) {
    console.error("Export error:", error);
    res.status(500).json({
      message: "Internal Server Error",
      error: error.message,
    });
  }
};
