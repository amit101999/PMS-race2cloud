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

/* Always shown first; the user cannot remove these two. */
const LOCKED_COLUMNS = [VIRTUAL_FIELD, ACTUAL_FIELD];

/* Shown until the user picks their own columns (and after "Reset to default"). */
const DEFAULT_COLUMNS = [
  "Client_name",
  "Status",
  "Group_Name",
  "Scheme_Code",
  "Fund_Strategy_Code",
  "Account_Type",
  "Client_Status",
  "DP_Client_Id",
  "Inception_Date",
];

/* Display names that differ from the column name with spaces for underscores. */
const COLUMN_LABELS = {
  WS_Account_code: "Virtual Code",
  Client_name: "Client Name",
  DP_Client_Id: "DP Client ID",
};

const labelFor = (column) => COLUMN_LABELS[column] || column.replace(/_/g, " ");

/* The user's chosen columns are kept in this browser only. */
const COLUMNS_STORAGE_KEY = "clientMaster.columns";

function loadSavedColumns() {
  try {
    const saved = JSON.parse(localStorage.getItem(COLUMNS_STORAGE_KEY));
    if (Array.isArray(saved) && saved.every((c) => typeof c === "string")) return saved;
  } catch {
    // Storage blocked or the saved value is not valid JSON: use the default.
  }
  return DEFAULT_COLUMNS;
}

function saveColumns(columns) {
  try {
    localStorage.setItem(COLUMNS_STORAGE_KEY, JSON.stringify(columns));
  } catch {
    // Storage blocked: the choice simply lasts until the page is reloaded.
  }
}

/** RFC 4180-style CSV cell (comma-safe). */
function csvEscapeCell(value) {
  const s = value == null ? "" : String(value);
  if (/[",\r\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function downloadCsv(filename, csvBody) {
  // The BOM makes Excel read the file as UTF-8.
  const blob = new Blob(["﻿" + csvBody], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

function displayValue(val) {
  if (val === null || val === undefined || val === "") return "–";
  return String(val);
}

function ClientPage() {
  const [clients, setClients] = useState([]);
  const [tableColumns, setTableColumns] = useState([]);
  const [chosenColumns, setChosenColumns] = useState(loadSavedColumns);
  const [showColumnPicker, setShowColumnPicker] = useState(false);
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
          setTableColumns(json.columns || []);
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

  /*
   * Locked columns first, then the user's choice in their order. A saved column
   * that no longer exists in the table is skipped.
   */
  const shownColumns = useMemo(() => {
    const available = new Set(tableColumns);
    const chosen = chosenColumns.filter(
      (c) => !LOCKED_COLUMNS.includes(c) && available.has(c),
    );
    return [...LOCKED_COLUMNS, ...chosen];
  }, [tableColumns, chosenColumns]);

  const addableColumns = useMemo(
    () => tableColumns.filter((c) => !shownColumns.includes(c)),
    [tableColumns, shownColumns],
  );

  const updateChosenColumns = (next) => {
    setChosenColumns(next);
    saveColumns(next);
  };

  const handleAddColumn = (column) => {
    if (!column) return;
    updateChosenColumns([...shownColumns.slice(LOCKED_COLUMNS.length), column]);
  };

  const handleRemoveColumn = (column) => {
    updateChosenColumns(
      shownColumns.slice(LOCKED_COLUMNS.length).filter((c) => c !== column),
    );
  };

  /* Exports every client matching the search, with the columns on screen. */
  const handleExport = () => {
    const header = shownColumns.map((c) => csvEscapeCell(labelFor(c))).join(",");
    const lines = filtered.map((row) =>
      shownColumns.map((c) => csvEscapeCell(row[c])).join(","),
    );
    const today = new Date().toISOString().slice(0, 10);
    downloadCsv(`Client_master_${today}.csv`, [header, ...lines].join("\r\n"));
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

            <button
              type="button"
              className="cm-secondary client-view-columns-btn"
              aria-expanded={showColumnPicker}
              disabled={loading || Boolean(error)}
              onClick={() => setShowColumnPicker((open) => !open)}
            >
              View Columns ({shownColumns.length})
              <span
                className={`client-view-columns-arrow${showColumnPicker ? " open" : ""}`}
                aria-hidden="true"
              >
                ▾
              </span>
            </button>
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
                <div className="client-header-actions">
                  <span className="count-pill">
                    Showing {filtered.length} of {clients.length}
                  </span>
                </div>
              </div>

              {showColumnPicker && (
                <div className="client-column-picker">
                  <div className="client-column-chips">
                    {shownColumns.map((c) =>
                      LOCKED_COLUMNS.includes(c) ? (
                        <span key={c} className="client-column-chip locked" title="Always shown">
                          {labelFor(c)}
                          <span className="client-column-lock">Fixed</span>
                        </span>
                      ) : (
                        <span key={c} className="client-column-chip">
                          {labelFor(c)}
                          <button
                            type="button"
                            className="client-column-remove"
                            aria-label={`Remove ${labelFor(c)}`}
                            onClick={() => handleRemoveColumn(c)}
                          >
                            −
                          </button>
                        </span>
                      ),
                    )}
                  </div>
                  <div className="client-column-actions">
                    <select
                      className="client-column-add"
                      value=""
                      disabled={addableColumns.length === 0}
                      onChange={(e) => handleAddColumn(e.target.value)}
                    >
                      <option value="">
                        {addableColumns.length === 0 ? "All columns shown" : "+ Add column"}
                      </option>
                      {addableColumns.map((c) => (
                        <option key={c} value={c}>
                          {labelFor(c)}
                        </option>
                      ))}
                    </select>
                    <button
                      type="button"
                      className="cm-secondary"
                      onClick={() => updateChosenColumns(DEFAULT_COLUMNS)}
                    >
                      Reset to default
                    </button>
                  </div>
                </div>
              )}

              {filtered.length === 0 ? (
                <p className="cm-empty">No clients match your filter.</p>
              ) : (
                <>
                  <div className="data-table-wrap">
                    <table className="data-table">
                      <thead>
                        <tr>
                          {shownColumns.map((c) => (
                            <th key={c}>{labelFor(c)}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {pageRows.map((row, i) => (
                          <tr key={row.ROWID ?? i}>
                            {shownColumns.map((c) => (
                              <td key={c}>{displayValue(row[c])}</td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>

                  <div className="client-table-footer">
                    <Pagination
                      currentPage={page}
                      pageSize={PAGE_SIZE}
                      totalRows={filtered.length}
                      onPageChange={setPage}
                    />
                    <button type="button" className="cm-primary" onClick={handleExport}>
                      Export
                    </button>
                  </div>
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
