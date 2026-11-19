const esc = (s) => String(s ?? "").replace(/'/g, "''");
const CLIENT_LIST_BATCH = 270;

/*
 * Catalyst's own bookkeeping columns. Every other clientIds column goes to the
 * Client page, including PAN, Aadhaar, bank and nominee details: the operators
 * need them, and the page lets each user choose which columns to show.
 * ROWID is still selected for row keys; it is just not offered as a column.
 */
const SYSTEM_COLUMNS = ["CREATORID", "CREATEDTIME", "MODIFIEDTIME"];

/** A clientIds row without the Catalyst system columns. */
const withoutSystemColumns = (row) => {
  const kept = { ...row };
  for (const column of SYSTEM_COLUMNS) delete kept[column];
  return kept;
};

/**
 * GET /api/client/list
 * Every client from the `clientIds` table (deduped by account code), with
 * every column except the system ones. `columns` lists those column names in
 * table order, so the page can offer them even when they are blank.
 */
export const getClientList = async (req, res) => {
  try {
    const zcql = req.catalystApp.zcql();
    const rows = [];
    const seen = new Set();
    let offset = 0;

    while (true) {
      const batch = await zcql.executeZCQLQuery(
        `SELECT * FROM clientIds ORDER BY WS_Account_code ASC, ROWID ASC LIMIT ${CLIENT_LIST_BATCH} OFFSET ${offset}`,
      );
      if (!batch?.length) break;

      for (const r of batch) {
        const row = r.clientIds || r;
        const code = String(row.WS_Account_code ?? "").trim();
        if (code && seen.has(code)) continue;
        if (code) seen.add(code);
        rows.push(withoutSystemColumns(row));
      }

      if (batch.length < CLIENT_LIST_BATCH) break;
      offset += CLIENT_LIST_BATCH;
    }

    // Collected across all rows, in first-seen order, so a column that is
    // blank on the first client is still offered.
    const columnSet = new Set();
    for (const row of rows) {
      for (const column of Object.keys(row)) {
        if (column !== "ROWID") columnSet.add(column);
      }
    }
    const columns = [...columnSet];

    return res.json({ data: rows, columns, count: rows.length });
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
 * every column except the system ones.
 */
export const getClientDetails = async (req, res) => {
  try {
    const zcql = req.catalystApp.zcql();
    const accountCode = (req.query.accountCode || "").trim();

    if (!accountCode) {
      return res.status(400).json({ message: "accountCode is required" });
    }

    const rows = await zcql.executeZCQLQuery(
      `SELECT * FROM clientIds WHERE WS_Account_code = '${esc(accountCode)}'`,
    );

    const records = (rows || []).map((r) => withoutSystemColumns(r.clientIds || r));

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
