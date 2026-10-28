import React, { useState, useEffect, useMemo } from "react";
import MainLayout from "../../layouts/MainLayout";
import { Pagination } from "../../components/common/CommonComponents";
import { BASE_URL } from "../../constant";
import "../SplitPage/SplitPage.css";
import "./ClientPage.css";
import "../TempTransactionUpload/tokens.css";
import "../TempTransactionUpload/TempTransaction.css";
import "../TempTransactionUpload/custody/CustodyMatch.css";
import "../../styles/ops-pages.css";

const PAGE_SIZE = 10;

const COLUMNS = [
  { key: "schemeName", label: "Scheme Name", className: "wrap" },
  { key: "schemeActive", label: "Scheme Active" },
  { key: "poolBankAccount", label: "Pool Bank Account" },
  { key: "ifsc", label: "IFSC" },
  { key: "uccStrategy", label: "UCC / Strategy" },
  { key: "dpid1", label: "DPID 1" },
  { key: "schemeAcronym", label: "Scheme Acronym" },
  { key: "schemeType", label: "Scheme Type" },
  { key: "schemeStatus", label: "Scheme Status" },
  { key: "dividendBankAccount", label: "Dividend Bank Account" },
  { key: "dividendBankName", label: "Dividend Bank Name" },
  { key: "dividendBankBranch", label: "Dividend Bank Branch", className: "wrap" },
];

const EMPTY_FORM = {
  schemeName: "",
  uccStrategy: "",
  schemeAcronym: "",
  schemeType: "",
  schemeActive: "true",
  poolBankAccount: "",
  ifsc: "",
  dpid1: "",
  dividendBankAccount: "",
  dividendBankName: "",
  dividendBankBranch: "",
};

/* Text inputs of the Add form; maxLength matches the Scheme table columns. */
const REQUIRED_INPUTS = [
  { key: "schemeName", label: "Scheme Name", maxLength: 255, placeholder: "e.g. PriceBridge - Upswing" },
  { key: "uccStrategy", label: "UCC / Strategy", maxLength: 50, placeholder: "e.g. AYANPBUS", upper: true },
  { key: "schemeAcronym", label: "Scheme Acronym", maxLength: 20, placeholder: "e.g. US", upper: true },
  { key: "schemeType", label: "Scheme Type", maxLength: 50, placeholder: "e.g. Discretionary" },
];

const BANK_INPUTS = [
  { key: "poolBankAccount", label: "Pool Bank Account", maxLength: 50 },
  { key: "ifsc", label: "IFSC", maxLength: 20, upper: true },
  { key: "dpid1", label: "DPID 1", maxLength: 20 },
  { key: "dividendBankAccount", label: "Dividend Bank Account", maxLength: 50 },
  { key: "dividendBankName", label: "Dividend Bank Name", maxLength: 100 },
  { key: "dividendBankBranch", label: "Dividend Bank Branch", maxLength: 255 },
];

function displayValue(key, val) {
  if (val === null || val === undefined || val === "") return "–";
  // The source file stores Scheme Active as true / false.
  if (key === "schemeActive") return String(val).toLowerCase() === "true" ? "Yes" : "No";
  return String(val);
}

/* Same rules as the server, so most mistakes show before anything is sent. */
function validateScheme(values) {
  const errors = {};
  if (!values.schemeName) errors.schemeName = "Scheme Name is required.";
  if (!values.uccStrategy) errors.uccStrategy = "UCC / Strategy is required.";
  if (!values.schemeAcronym) errors.schemeAcronym = "Scheme Acronym is required.";
  if (!values.schemeType) errors.schemeType = "Scheme Type is required.";
  if (values.poolBankAccount && !values.ifsc) {
    errors.ifsc = "IFSC is required when Pool Bank Account is filled.";
  }
  if (values.ifsc && !values.poolBankAccount) {
    errors.poolBankAccount = "Pool Bank Account is required when IFSC is filled.";
  }
  if (values.dividendBankAccount && !values.dividendBankName) {
    errors.dividendBankName = "Dividend Bank Name is required when Dividend Bank Account is filled.";
  }
  return errors;
}

function SchemePage() {
  const [schemes, setSchemes] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(1);

  /* Add Scheme modal */
  const [showAdd, setShowAdd] = useState(false);
  const [form, setForm] = useState(EMPTY_FORM);
  const [fieldErrors, setFieldErrors] = useState({});
  const [submitError, setSubmitError] = useState("");
  const [saving, setSaving] = useState(false);

  /* Load all schemes once; the table shows all by default. */
  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true);
      setError("");
      try {
        const res = await fetch(`${BASE_URL}/scheme/list`);
        const json = await res.json();
        if (cancelled) return;
        if (!res.ok) {
          setError(json.message || "Failed to load schemes.");
          setSchemes([]);
        } else {
          setSchemes(json.data || []);
        }
      } catch (err) {
        console.error("Scheme list fetch error:", err);
        if (!cancelled) {
          setError("Failed to fetch schemes.");
          setSchemes([]);
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  /* One search box, filters the table by scheme name, acronym or UCC / strategy. */
  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return schemes;
    return schemes.filter((s) => {
      const name = String(s.schemeName ?? "").toLowerCase();
      const acronym = String(s.schemeAcronym ?? "").toLowerCase();
      const ucc = String(s.uccStrategy ?? "").toLowerCase();
      return name.includes(q) || acronym.includes(q) || ucc.includes(q);
    });
  }, [schemes, search]);

  /* Reset to the first page whenever the filter changes. */
  useEffect(() => {
    setPage(1);
  }, [search]);

  const pageRows = useMemo(() => {
    const start = (page - 1) * PAGE_SIZE;
    return filtered.slice(start, start + PAGE_SIZE);
  }, [filtered, page]);

  const openAdd = () => {
    setForm(EMPTY_FORM);
    setFieldErrors({});
    setSubmitError("");
    setShowAdd(true);
  };

  const closeAdd = () => {
    if (saving) return;
    setShowAdd(false);
  };

  const handleFieldChange = (key, value) => {
    setForm((prev) => ({ ...prev, [key]: value }));
    setFieldErrors((prev) => ({ ...prev, [key]: "" }));
    setSubmitError("");
  };

  const handleAddSubmit = async (e) => {
    e.preventDefault();
    const values = {};
    for (const key of Object.keys(EMPTY_FORM)) values[key] = form[key].trim();

    const errors = validateScheme(values);
    if (Object.keys(errors).length) {
      setFieldErrors(errors);
      return;
    }

    setSaving(true);
    setSubmitError("");
    try {
      const res = await fetch(`${BASE_URL}/scheme/add`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(values),
      });
      const json = await res.json().catch(() => ({}));
      if (!res.ok) {
        setFieldErrors(json.errors || {});
        setSubmitError(json.message || "Failed to add scheme.");
        return;
      }

      const created = json.data;
      setSchemes((prev) =>
        [...prev, created].sort((a, b) => a.schemeName.localeCompare(b.schemeName)),
      );
      setShowAdd(false);
      window.alert(`New scheme ${created.schemeName} is created.`);
    } catch (err) {
      console.error("Scheme add error:", err);
      setSubmitError("Failed to add scheme. Please try again.");
    } finally {
      setSaving(false);
    }
  };

  const renderInput = (field) => (
    <div className="master-form-field" key={field.key}>
      <label htmlFor={`scheme-${field.key}`}>
        {field.label}
      </label>
      <input
        id={`scheme-${field.key}`}
        type="text"
        maxLength={field.maxLength}
        placeholder={field.placeholder}
        value={form[field.key]}
        onChange={(e) =>
          handleFieldChange(field.key, field.upper ? e.target.value.toUpperCase() : e.target.value)
        }
      />
      {fieldErrors[field.key] && (
        <p className="master-field-error">{fieldErrors[field.key]}</p>
      )}
    </div>
  );

  return (
    <MainLayout title="Master · Scheme">
      <div className="temp-transaction-container txn-ops ops-page client-page">
        {/* Filter */}
        <div className="temp-transaction-card">
          <div className="client-filter-card">
            <div className="cash-filter-field">
              <div className="account-code-search">
                <label className="search-label">Filter by Scheme Name, Acronym or UCC</label>
                <div className="search-input-wrapper">
                  <span className="search-icon">&#128269;</span>
                  <input
                    type="text"
                    className="search-input"
                    placeholder="Search Scheme Name, Acronym or UCC..."
                    value={search}
                    onChange={(e) => setSearch(e.target.value)}
                  />
                  {search && (
                    <span className="clear-icon" onClick={() => setSearch("")}>
                      &#10005;
                    </span>
                  )}
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Table */}
        {loading && <p className="cm-empty">Loading schemes...</p>}
        {!loading && error && (
          <div className="cm-verdict bad">{error}</div>
        )}

        {!loading && !error && (
          <div className="client-details-section">
            <div className="temp-transaction-card">
              <div className="client-details-header">
                <h3>Schemes</h3>
                <div className="master-header-actions">
                  <span className="count-pill">
                    Showing {filtered.length} of {schemes.length}
                  </span>
                  <button
                    type="button"
                    className="cm-primary master-add-btn"
                    onClick={openAdd}
                  >
                    <span className="master-add-icon" aria-hidden="true">+</span>
                    Add
                  </button>
                </div>
              </div>

              {filtered.length === 0 ? (
                <p className="cm-empty">No schemes match your filter.</p>
              ) : (
                <>
                  <div className="data-table-wrap">
                    <table className="data-table">
                      <thead>
                        <tr>
                          {COLUMNS.map((c) => (
                            <th key={c.key} className={c.className}>{c.label}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {pageRows.map((row, i) => (
                          <tr key={row.schemeName || i}>
                            {COLUMNS.map((c) => (
                              <td key={c.key} className={c.className}>{displayValue(c.key, row[c.key])}</td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>

                  <Pagination
                    currentPage={page}
                    pageSize={PAGE_SIZE}
                    totalRows={filtered.length}
                    onPageChange={setPage}
                  />
                </>
              )}
            </div>
          </div>
        )}

        {/* Add Scheme modal */}
        {showAdd && (
          <div className="master-modal-backdrop" onClick={closeAdd}>
            <div
              className="master-modal master-modal--wide"
              role="dialog"
              aria-modal="true"
              aria-labelledby="add-scheme-title"
              onClick={(e) => e.stopPropagation()}
            >
              <h3 id="add-scheme-title" className="ops-card-title">
                Add Scheme
              </h3>
              <form onSubmit={handleAddSubmit} noValidate>
                <div className="master-form-grid">
                  {REQUIRED_INPUTS.map((field) => renderInput(field))}

                  <div className="master-form-field">
                    <label htmlFor="scheme-schemeActive">
                      Scheme Active
                    </label>
                    <select
                      id="scheme-schemeActive"
                      value={form.schemeActive}
                      onChange={(e) => handleFieldChange("schemeActive", e.target.value)}
                    >
                      <option value="true">Yes</option>
                      <option value="false">No</option>
                    </select>
                    {fieldErrors.schemeActive && (
                      <p className="master-field-error">{fieldErrors.schemeActive}</p>
                    )}
                  </div>

                  {BANK_INPUTS.map((field) => renderInput(field))}
                </div>

                {submitError && <div className="cm-verdict bad">{submitError}</div>}

                <div className="ops-modal-actions">
                  <button
                    type="button"
                    className="cm-secondary"
                    onClick={closeAdd}
                    disabled={saving}
                  >
                    Cancel
                  </button>
                  <button type="submit" className="cm-primary" disabled={saving}>
                    {saving ? "Adding..." : "Add"}
                  </button>
                </div>
              </form>
            </div>
          </div>
        )}
      </div>
    </MainLayout>
  );
}

export default SchemePage;
