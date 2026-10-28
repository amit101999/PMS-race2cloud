const esc = (s) => String(s ?? "").replace(/'/g, "''");

// Same limits as the Transaction_Type columns (varchar 20 / varchar 255).
const TRAN_TYPE_MAX = 20;
const TRAN_DESC_MAX = 255;

/**
 * GET /api/transaction-type/list
 * All transaction types from Transaction_Type (code + description), sorted by code.
 * The master is small (a few dozen rows), so one query is enough.
 */
export const getTransactionTypeList = async (req, res) => {
  try {
    const zcql = req.catalystApp.zcql();
    const rows = await zcql.executeZCQLQuery(`
      SELECT Tran_Type, Tran_Desc
      FROM Transaction_Type
      ORDER BY Tran_Type ASC
      LIMIT 300
    `);

    const data = (rows || []).map((r) => {
      const t = r.Transaction_Type || r;
      return {
        tranType: String(t.Tran_Type ?? "").trim(),
        tranDesc: String(t.Tran_Desc ?? "").trim(),
      };
    });

    return res.json({ data });
  } catch (error) {
    console.error("Transaction type list fetch error:", error);
    res.status(500).json({
      message: "Failed to fetch transaction types",
      error: error.message,
    });
  }
};

/**
 * POST /api/transaction-type/add   body: { tranType, tranDesc }
 * Adds one transaction type. Both fields are required. Codes are stored in
 * upper case (like the loaded master) so "by-" cannot sit beside "BY-".
 */
export const addTransactionType = async (req, res) => {
  const tranType = String(req.body?.tranType ?? "").trim().toUpperCase();
  const tranDesc = String(req.body?.tranDesc ?? "").trim();

  if (!tranType || !tranDesc) {
    return res
      .status(400)
      .json({ message: "Transaction type and description are both required." });
  }
  if (tranType.length > TRAN_TYPE_MAX) {
    return res.status(400).json({
      message: `Transaction type can be at most ${TRAN_TYPE_MAX} characters.`,
    });
  }
  if (tranDesc.length > TRAN_DESC_MAX) {
    return res.status(400).json({
      message: `Description can be at most ${TRAN_DESC_MAX} characters.`,
    });
  }

  try {
    const zcql = req.catalystApp.zcql();
    const existing = await zcql.executeZCQLQuery(
      `SELECT ROWID FROM Transaction_Type WHERE Tran_Type = '${esc(tranType)}' LIMIT 1`,
    );
    if (existing?.length) {
      return res
        .status(409)
        .json({ message: `Transaction type ${tranType} already exists.` });
    }

    await req.catalystApp
      .datastore()
      .table("Transaction_Type")
      .insertRow({ Tran_Type: tranType, Tran_Desc: tranDesc });

    return res.status(201).json({ data: { tranType, tranDesc } });
  } catch (error) {
    console.error("Transaction type add error:", error);
    res.status(500).json({
      message: "Failed to add transaction type",
      error: error.message,
    });
  }
};
