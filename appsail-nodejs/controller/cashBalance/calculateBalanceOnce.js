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
    const result = await getAllAccountCodesFromDatabase(zcql, "clientIds");

    const allAccountCodes = result.map((r) => r.clientIds.WS_Account_code);

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
        job_name: `cashCal_${i + 1}`,
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
