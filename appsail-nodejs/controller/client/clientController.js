const esc = (s) => String(s ?? "").replace(/'/g, "''");
const CLIENT_LIST_BATCH = 270;

/*
 * Only these clientIds columns leave the API. The table also holds PAN, Aadhaar,
 * bank and nominee details, which the Client page must not receive at all.
 */
const CLIENT_COLUMNS = [
  "ROWID",
  "WS_Account_code",
  "Actual_Code",
  "Client_name",
  "Status",
  "Group_Name",
  "Scheme_Code",
  "Fund_Strategy_Code",
  "Account_Type",
  "Client_Status",
  "DP_Client_Id",
  "Inception_Date",
].join(", ");

/**
 * GET /api/client/list
 * Every client from the `clientIds` table (deduped by account code), with the
 * CLIENT_COLUMNS only.
 */
export const getClientList = async (req, res) => {
  try {
    const zcql = req.catalystApp.zcql();
    const rows = [];
    const seen = new Set();
    let offset = 0;

    while (true) {
      const batch = await zcql.executeZCQLQuery(
        `SELECT ${CLIENT_COLUMNS} FROM clientIds ORDER BY WS_Account_code ASC, ROWID ASC LIMIT ${CLIENT_LIST_BATCH} OFFSET ${offset}`,
      );
      if (!batch?.length) break;

      for (const r of batch) {
        const row = r.clientIds || r;
        const code = String(row.WS_Account_code ?? "").trim();
        if (code && seen.has(code)) continue;
        if (code) seen.add(code);
        rows.push(row);
      }

      if (batch.length < CLIENT_LIST_BATCH) break;
      offset += CLIENT_LIST_BATCH;
    }

    return res.json({ data: rows, count: rows.length });
  } catch (error) {
    console.error("Client list fetch error:", error);
    res.status(500).json({
      message: "Failed to fetch client list",
      error: error.message,
    });
  }
};

/**
 * Client master details for one account code from the `clientIds` table, with
 * the CLIENT_COLUMNS only.
 */
export const getClientDetails = async (req, res) => {
  try {
    const zcql = req.catalystApp.zcql();
    const accountCode = (req.query.accountCode || "").trim();

    if (!accountCode) {
      return res.status(400).json({ message: "accountCode is required" });
    }

    const rows = await zcql.executeZCQLQuery(
      `SELECT ${CLIENT_COLUMNS} FROM clientIds WHERE WS_Account_code = '${esc(accountCode)}'`,
    );

    const records = (rows || []).map((r) => r.clientIds || r);

    if (records.length === 0) {
      return res.status(404).json({
        message: "No client found for the given account code",
        data: null,
        records: [],
        count: 0,
      });
    }

    return res.json({
      data: records[0],
      records,
      count: records.length,
    });
  } catch (error) {
    console.error("Client details fetch error:", error);
    res.status(500).json({
      message: "Failed to fetch client details",
      error: error.message,
    });
  }
};
