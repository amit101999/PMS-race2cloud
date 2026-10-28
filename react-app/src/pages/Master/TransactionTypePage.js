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
  { key: "tranType", label: "Tran Type" },
  { key: "tranDesc", label: "Tran Description", className: "wrap" },
];

const EMPTY_FORM = { tranType: "", tranDesc: "" };

function displayValue(val) {
  if (val === null || val === undefined || val === "") return "–";
  return String(val);
}

function TransactionTypePage() {
  const [types, setTypes] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(1);

  /* Add Transaction Type modal */
  const [showAdd, setShowAdd] = useState(false);
  const [form, setForm] = useState(EMPTY_FORM);
  const [fieldErrors, setFieldErrors] = useState({});
  const [submitError, setSubmitError] = useState("");
  const [saving, setSaving] = useState(false);

  /* Load all transaction types once; the table shows all by default. */
  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true);
      setError("");
      try {
        const res = await fetch(`${BASE_URL}/transaction-type/list`);
        const json = await res.json();
        if (cancelled) return;
        if (!res.ok) {
          setError(json.message || "Failed to load transaction types.");
          setTypes([]);
        } else {
          setTypes(json.data || []);
        }
      } catch (err) {
        console.error("Transaction type list fetch error:", err);
        if (!cancelled) {
          setError("Failed to fetch transaction types.");
          setTypes([]);
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  /* One search box, filters the table by type code or description. */
  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return types;
    return types.filter((t) => {
      const code = String(t.tranType ?? "").toLowerCase();
      const desc = String(t.tranDesc ?? "").toLowerCase();
      return code.includes(q) || desc.includes(q);
    });
  }, [types, search]);

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
    const tranType = form.tranType.trim();
    const tranDesc = form.tranDesc.trim();

    const errors = {};
    if (!tranType) errors.tranType = "Transaction type is required.";
    if (!tranDesc) errors.tranDesc = "Transaction description is required.";
    if (errors.tranType || errors.tranDesc) {
      setFieldErrors(errors);
      return;
    }

    setSaving(true);
    setSubmitError("");
    try {
      const res = await fetch(`${BASE_URL}/transaction-type/add`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tranType, tranDesc }),
      });
      const json = await res.json().catch(() => ({}));
      if (!res.ok) {
        setSubmitError(json.message || "Failed to add transaction type.");
        return;
      }

      const created = json.data;
      setTypes((prev) =>
        [...prev, created].sort((a, b) => a.tranType.localeCompare(b.tranType)),
      );
      setShowAdd(false);
      window.alert(`New transaction type ${created.tranType} is created.`);
    } catch (err) {
      console.error("Transaction type add error:", err);
      setSubmitError("Failed to add transaction type. Please try again.");
    } finally {
      setSaving(false);
    }
  };

  return (
    <MainLayout title="Master · Transaction Type">
      <div className="temp-transaction-container txn-ops ops-page client-page">
        {/* Filter */}
        <div className="temp-transaction-card">
          <div className="client-filter-card">
            <div className="cash-filter-field">
              <div className="account-code-search">
                <label className="search-label">Filter by Tran Type or Description</label>
                <div className="search-input-wrapper">
                  <span className="search-icon">&#128269;</span>
                  <input
                    type="text"
                    className="search-input"
                    placeholder="Search Tran Type or Description..."
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
        {loading && <p className="cm-empty">Loading transaction types...</p>}
        {!loading && error && (
          <div className="cm-verdict bad">{error}</div>
        )}

        {!loading && !error && (
          <div className="client-details-section">
            <div className="temp-transaction-card">
              <div className="client-details-header">
                <h3>Transaction Types</h3>
                <div className="master-header-actions">
                  <span className="count-pill">
                    Showing {filtered.length} of {types.length}
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
                <p className="cm-empty">No transaction types match your filter.</p>
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
                          <tr key={row.tranType || i}>
                            {COLUMNS.map((c) => (
                              <td key={c.key} className={c.className}>{displayValue(row[c.key])}</td>
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

        {/* Add Transaction Type modal */}
        {showAdd && (
          <div className="master-modal-backdrop" onClick={closeAdd}>
            <div
              className="master-modal"
              role="dialog"
              aria-modal="true"
              aria-labelledby="add-tran-type-title"
              onClick={(e) => e.stopPropagation()}
            >
              <h3 id="add-tran-type-title" className="ops-card-title">
                Add Transaction Type
              </h3>
              <form onSubmit={handleAddSubmit} noValidate>
                <div className="master-form-field">
                  <label htmlFor="tran-type-input">
                    Transaction Type <span className="master-required">*</span>
                  </label>
                  <input
                    id="tran-type-input"
                    type="text"
                    maxLength={20}
                    placeholder="e.g. BY-"
                    value={form.tranType}
                    onChange={(e) => handleFieldChange("tranType", e.target.value.toUpperCase())}
                    autoFocus
                  />
                  {fieldErrors.tranType && (
                    <p className="master-field-error">{fieldErrors.tranType}</p>
                  )}
                </div>

                <div className="master-form-field">
                  <label htmlFor="tran-desc-input">
                    Transaction Description <span className="master-required">*</span>
                  </label>
                  <input
                    id="tran-desc-input"
                    type="text"
                    maxLength={255}
                    placeholder="e.g. Buy"
                    value={form.tranDesc}
                    onChange={(e) => handleFieldChange("tranDesc", e.target.value)}
                  />
                  {fieldErrors.tranDesc && (
                    <p className="master-field-error">{fieldErrors.tranDesc}</p>
                  )}
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

export default TransactionTypePage;
