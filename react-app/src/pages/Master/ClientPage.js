import React, { useState, useEffect, useMemo, useRef } from "react";
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

const VIRTUAL_FIELD = "WS_Account_code";
const ACTUAL_FIELD = "Actual_Code";

/* The only columns shown, in display order. The API sends the same set. */
const COLUMNS = [
  { key: "WS_Account_code", label: "Virtual Code" },
  { key: "Actual_Code", label: "Actual Code" },
  { key: "Client_name", label: "Client Name" },
  { key: "Status", label: "Status" },
  { key: "Group_Name", label: "Group Name" },
  { key: "Scheme_Code", label: "Scheme Code" },
  { key: "Fund_Strategy_Code", label: "Fund Strategy Code" },
  { key: "Account_Type", label: "Account Type" },
  { key: "Client_Status", label: "Client Status" },
  { key: "DP_Client_Id", label: "DP Client ID" },
  { key: "Inception_Date", label: "Inception Date" },
];

function displayValue(val) {
  if (val === null || val === undefined || val === "") return "–";
  return String(val);
}

function ClientPage() {
  const [clients, setClients] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(1);
  const [showDropdown, setShowDropdown] = useState(false);
  const dropdownRef = useRef(null);

  /* Load every client once; the table shows all by default. */
  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true);
      setError("");
      try {
        const res = await fetch(`${BASE_URL}/client/list`);
        const json = await res.json();
        if (cancelled) return;
        if (!res.ok) {
          setError(json.message || "Failed to load clients.");
          setClients([]);
        } else {
          setClients(json.data || []);
        }
      } catch (err) {
        console.error("Client list fetch error:", err);
        if (!cancelled) {
          setError("Failed to fetch clients.");
          setClients([]);
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  /* Single search: match virtual code OR actual code (actual → all related virtuals). */
  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return clients;
    return clients.filter((r) => {
      const virtual = String(r[VIRTUAL_FIELD] ?? "").toLowerCase();
      const actual = String(r[ACTUAL_FIELD] ?? "").toLowerCase();
      return virtual.includes(q) || actual.includes(q);
    });
  }, [clients, search]);

  /* Reset to the first page whenever the filter changes. */
  useEffect(() => {
    setPage(1);
  }, [search]);

  /* Close the suggestion dropdown when clicking outside. */
  useEffect(() => {
    const handler = (e) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target)) {
        setShowDropdown(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  /* Autocomplete — distinct virtual codes from matches (includes actual-code hits). */
  const suggestions = useMemo(() => {
    const seen = new Set();
    const out = [];
    for (const r of filtered) {
      const code = String(r[VIRTUAL_FIELD] ?? "").trim();
      if (code && !seen.has(code)) {
        seen.add(code);
        out.push(code);
      }
      if (out.length >= 50) break;
    }
    return out;
  }, [filtered]);

  const handleSelectSuggestion = (code) => {
    setSearch(code);
    setShowDropdown(false);
  };

  const pageRows = useMemo(() => {
    const start = (page - 1) * PAGE_SIZE;
    return filtered.slice(start, start + PAGE_SIZE);
  }, [filtered, page]);

  return (
    <MainLayout title="Master · Client">
      <div className="temp-transaction-container txn-ops ops-page client-page">
        {/* Filter */}
        <div className="temp-transaction-card">
          <div className="client-filter-card">
            <div className="cash-filter-field">
              <div className="account-code-search" ref={dropdownRef}>
                <label className="search-label">Filter by Virtual Code</label>
                <div className="search-input-wrapper">
                  <span className="search-icon">&#128269;</span>
                  <input
                    type="text"
                    className="search-input"
                    placeholder="Search Virtual or Actual Code..."
                    value={search}
                    onChange={(e) => {
                      setSearch(e.target.value);
                      setShowDropdown(true);
                    }}
                    onFocus={() => setShowDropdown(true)}
                  />
                  {search && (
                    <span className="clear-icon" onClick={() => setSearch("")}>
                      &#10005;
                    </span>
                  )}
                </div>

                {showDropdown && suggestions.length > 0 && (
                  <div className="search-dropdown">
                    <div className="dropdown-header">Virtual Code</div>
                    <div className="dropdown-options">
                      {suggestions.map((code) => (
                        <div
                          key={code}
                          className="dropdown-option"
                          onClick={() => handleSelectSuggestion(code)}
                        >
                          {code}
                        </div>
                      ))}
                    </div>
                    <div className="dropdown-footer">
                      {suggestions.length} of {filtered.length} matches
                    </div>
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>

        {/* Table */}
        {loading && <p className="cm-empty">Loading clients...</p>}
        {!loading && error && (
          <div className="cm-verdict bad">{error}</div>
        )}

        {!loading && !error && (
          <div className="client-details-section">
            <div className="temp-transaction-card">
              <div className="client-details-header">
                <h3>Clients</h3>
                <span className="count-pill">
                  Showing {filtered.length} of {clients.length}
                </span>
              </div>

              {filtered.length === 0 ? (
                <p className="cm-empty">No clients match your filter.</p>
              ) : (
                <>
                  <div className="data-table-wrap">
                    <table className="data-table">
                      <thead>
                        <tr>
                          {COLUMNS.map((c) => (
                            <th key={c.key}>{c.label}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {pageRows.map((row, i) => (
                          <tr key={row.ROWID ?? i}>
                            {COLUMNS.map((c) => (
                              <td key={c.key}>{displayValue(row[c.key])}</td>
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
      </div>
    </MainLayout>
  );
}

export default ClientPage;
