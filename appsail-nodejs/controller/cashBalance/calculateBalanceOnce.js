// this function calculates the cash balance for a user once

import { getAllAccountCodesFromDatabase } from "../../util/allAccountCodes.js";

const chunkArray = (array, size) => {
  const chunks = [];
  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size));
  }
  return chunks;
};

export const calculateCashBalanceJob = async (req, res) => {
  try {
    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();
    const jobScheduling = catalystApp.jobScheduling();

    // Fetch all account codes
    // const result = await getAllAccountCodesFromDatabase(zcql, "clientIds");

    // const allAccountCodes = result.map((r) => r.clientIds.WS_Account_code);

    const allAccountCodes = [
      "AYAN153",
      "ELAYAN035",
      "AYAN131",
      "AYAN152",
      "ELAYAN006",
      "NROAYAN34",
      "AYAN154",
      "NROAYAN33",
      "234NREAYAN",
      "239NREAYAN",
      "ELAYAN045",
      "ELAYAN044",
      "ELAYAN042",
      "TAYAN019",
      "ELNRO12",
      "ELAYAN027",
      "HCAYAN040",
      "TAYAN025",
      "HCAYAN037",
      "TMAYAN002",

      "AYAN206",
      "AYAN207",
      "IMFAYAN32",
      "TMAYAN005",
      "AYAN205",
      "ND01",
      "ND09",
      "TMAYAN007",
      "TMNRE01",
      "HCAYAN056",

      "TAYAN031",
      "HCAYAN049",
      "250NREAYAN",
      "ELNRE07",
      "TAYAN032",
      "HCAYAN050",
      "ELAYAN054",
      "HCAYAN048",
      "HCNRE01",
      "HCAYAN051",

      "AYAN197",
      "AYAN199",
      "TAYAN029",
      "ELAYAN051",
      "AYAN198",
      "AYAN195",
      "ELAYAN032",
      "HCAYAN047",
      "AYAN200",
      "IMFAYAN29",

      "ELAYAN041",
      "NROAYAN41",
      "ELNRO10",
      "AYAN180",
      "AYAN174",
      "AYAN181",
      "247NREAYAN",
      "ELNRE06",
      "AYAN182",
      "ELNRO11",

      "ELAYAN036",
      "AYAN155",
      "AYAN158",
      "HCAYAN032",
      "AYAN156",
      "AYAN160",
      "TMAYAN001",
      "HCAYAN033",
      "AYAN159",
      "240NREAYAN",

      "233NREAYAN",
      "ELNRO09",
      "TAYAN012",
      "IMFAYAN10",
      "IMFAYAN20",
      "ELNRE04",
      "AYAN140",
      "ELAYAN030",
      "IMFAYAN25",
      "AYAN139",
    ];

    if (!allAccountCodes.length) {
      return res.json({ message: "No accounts found" });
    }

    // Chunk account codes (SAFE size)
    const CHUNK_SIZE = 10; //
    const chunks = chunkArray(allAccountCodes, CHUNK_SIZE);

    const today = new Date().toISOString().split("T")[0];
    const scheduledJobs = [];

    // 3️⃣ Schedule one job per chunk
    for (let i = 0; i < chunks.length; i++) {
      const jobName = `CASH_LEDGER_${today}_BATCH_${i + 1}`;

      await jobScheduling.JOB.submitJob({
        job_name: `cashCal_retry_${i + 1}`,
        jobpool_name: "Finance",
        target_name: "CashCalculation",
        target_type: "Function",
        params: {
          accountCode: chunks[i],
          jobName: jobName,
        },
      });

      scheduledJobs.push({
        jobName,
        accounts: chunks[i].length,
      });
    }

    return res.json({
      message: "Cash ledger jobs scheduled successfully",
      totalAccounts: allAccountCodes.length,
      totalJobs: chunks.length,
      jobs: scheduledJobs,
    });
  } catch (error) {
    console.error("Failed to schedule cash ledger jobs:", error);
    return res.status(500).json({
      message: "Failed to schedule cash ledger jobs",
      error: error.message,
    });
  }
};
