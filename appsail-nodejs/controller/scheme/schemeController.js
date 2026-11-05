const SCHEME_LIST_LIMIT = 300;

// Form field → Scheme column, with the column's varchar limit.
const FIELDS = {
  schemeName: { column: "Scheme_Name", max: 255, label: "Scheme Name" },
  uccStrategy: { column: "UCC_Strategy", max: 50, label: "UCC / Strategy" },
  schemeAcronym: { column: "Scheme_Acronym", max: 20, label: "Scheme Acronym" },
  schemeType: { column: "Scheme_Type", max: 50, label: "Scheme Type" },
  poolBankAccount: { column: "Pool_Bank_Account", max: 50, label: "Pool Bank Account" },
  ifsc: { column: "IFSC", max: 20, label: "IFSC" },
  dpid1: { column: "DPID_1", max: 20, label: "DPID 1" },
  dividendBankAccount: { column: "Dividend_Bank_Account", max: 50, label: "Dividend Bank Account" },
  dividendBankName: { column: "Dividend_Bank_Name", max: 100, label: "Dividend Bank Name" },
  dividendBankBranch: { column: "Dividend_Bank_Branch", max: 255, label: "Dividend Bank Branch" },
};

const text = (v) => String(v ?? "").trim();

const fetchSchemes = async (zcql) => {
  const rows = await zcql.executeZCQLQuery(`
    SELECT Scheme_Name, Scheme_Active, Pool_Bank_Account, IFSC, UCC_Strategy,
           DPID_1, Scheme_Acronym, Scheme_Type, Scheme_Status,
           Dividend_Bank_Account, Dividend_Bank_Name, Dividend_Bank_Branch
    FROM Scheme
    ORDER BY Scheme_Name ASC
    LIMIT ${SCHEME_LIST_LIMIT}
  `);
  return (rows || []).map((r) => {
    const s = r.Scheme || r;
    return {
      schemeName: text(s.Scheme_Name),
      schemeActive: text(s.Scheme_Active),
      poolBankAccount: text(s.Pool_Bank_Account),
      ifsc: text(s.IFSC),
      uccStrategy: text(s.UCC_Strategy),
      dpid1: text(s.DPID_1),
      schemeAcronym: text(s.Scheme_Acronym),
      schemeType: text(s.Scheme_Type),
      schemeStatus: text(s.Scheme_Status),
      dividendBankAccount: text(s.Dividend_Bank_Account),
      dividendBankName: text(s.Dividend_Bank_Name),
      dividendBankBranch: text(s.Dividend_Bank_Branch),
    };
  });
};

/**
 * GET /api/scheme/list
 * All schemes from the Scheme master, sorted by name.
 * The master is small (a few dozen rows), so one query is enough.
 */
export const getSchemeList = async (req, res) => {
  try {
    const data = await fetchSchemes(req.catalystApp.zcql());
    return res.json({ data });
  } catch (error) {
    console.error("Scheme list fetch error:", error);
    res.status(500).json({
      message: "Failed to fetch schemes",
      error: error.message,
    });
  }
};

/**
 * POST /api/scheme/add
 * Body: the FIELDS keys above + schemeActive ("true" / "false").
 *
 * Rules agreed with the operations team:
 *  - Required: Scheme Name, UCC / Strategy, Scheme Acronym, Scheme Type, Scheme Active.
 *  - Name, UCC / Strategy and Acronym must each be unique (case-insensitive).
 *  - Pool Bank Account and IFSC go together; a Dividend Bank Account needs a Dividend Bank Name.
 *  - Scheme Status is not asked for: it follows Scheme Active (Active / Inactive).
 *  - No format checks beyond the column lengths.
 */
export const addScheme = async (req, res) => {
  const body = req.body || {};
  const values = {};
  for (const key of Object.keys(FIELDS)) values[key] = text(body[key]);
  // UCC, acronym and IFSC codes are upper case in the loaded master.
  values.uccStrategy = values.uccStrategy.toUpperCase();
  values.schemeAcronym = values.schemeAcronym.toUpperCase();
  values.ifsc = values.ifsc.toUpperCase();
  const schemeActive = text(body.schemeActive).toLowerCase();

  const errors = {};
  for (const key of ["schemeName", "uccStrategy", "schemeAcronym", "schemeType"]) {
    if (!values[key]) errors[key] = `${FIELDS[key].label} is required.`;
  }
  if (schemeActive !== "true" && schemeActive !== "false") {
    errors.schemeActive = "Scheme Active is required.";
  }
  for (const [key, field] of Object.entries(FIELDS)) {
    if (!errors[key] && values[key].length > field.max) {
      errors[key] = `${field.label} can be at most ${field.max} characters.`;
    }
  }
  if (values.poolBankAccount && !values.ifsc) {
    errors.ifsc = "IFSC is required when Pool Bank Account is filled.";
  }
  if (values.ifsc && !values.poolBankAccount) {
    errors.poolBankAccount = "Pool Bank Account is required when IFSC is filled.";
  }
  if (values.dividendBankAccount && !values.dividendBankName) {
    errors.dividendBankName = "Dividend Bank Name is required when Dividend Bank Account is filled.";
  }

  if (Object.keys(errors).length) {
    return res.status(400).json({ message: "Please correct the highlighted fields.", errors });
  }

  try {
    const existing = await fetchSchemes(req.catalystApp.zcql());
    const taken = (key) =>
      existing.some((s) => s[key].toLowerCase() === values[key].toLowerCase());
    for (const key of ["schemeName", "uccStrategy", "schemeAcronym"]) {
      if (taken(key)) errors[key] = `${FIELDS[key].label} ${values[key]} already exists.`;
    }
    if (Object.keys(errors).length) {
      return res.status(409).json({ message: "This scheme already exists.", errors });
    }

    const row = {
      Scheme_Active: schemeActive,
      Scheme_Status: schemeActive === "true" ? "Active" : "Inactive",
    };
    for (const [key, field] of Object.entries(FIELDS)) {
      if (values[key]) row[field.column] = values[key];
    }
    await req.catalystApp.datastore().table("Scheme").insertRow(row);

    return res.status(201).json({
      data: { ...values, schemeActive, schemeStatus: row.Scheme_Status },
    });
  } catch (error) {
    console.error("Scheme add error:", error);
    res.status(500).json({
      message: "Failed to add scheme",
      error: error.message,
    });
  }
};
