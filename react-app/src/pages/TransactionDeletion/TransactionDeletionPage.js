import React, { useState, useEffect } from "react";
import MainLayout from "../../layouts/MainLayout";
import { Pagination } from "../../components/common/CommonComponents";
import { BASE_URL } from "../../constant";
import "../SplitPage/SplitPage.css";
import "../Master/ClientPage.css";
import "../CashBalancePage/CashBalancePage.css";
import "../TempTransactionUpload/tokens.css";
import "../TempTransactionUpload/TempTransaction.css";
import "../TempTransactionUpload/custody/CustodyMatch.css";
import "../../styles/ops-pages.css";
import "./TransactionDeletionPage.css";

const PAGE_SIZE = 50;

const EMPTY_FILTERS = {
  isin: "",
  securityCode: "",
  clientCode: "",
  tranDate: "",
  setDate: "",
};

const COLUMNS = [
  { key: "WS_Account_code", label: "Client Code" },
  { key: "TRANDATE", label: "Tran Date" },
  { key: "SETDATE", label: "Settle Date" },
  { key: "Tran_Type", label: "Type" },
  { key: "ISIN", label: "ISIN" },
  { key: "Security_code", label: "Security Code" },
  { key: "Security_Name", label: "Security Name", className: "wrap" },
  { key: "QTY", label: "Qty" },
  { key: "RATE", label: "Rate" },
  { key: "Net_Amount", label: "Net Amount" },
  { key: "CREATEDTIME", label: "Committed At", format: formatCommittedAt },
];

function displayValue(val) {
  if (val === null || val === undefined || val === "") return "–";
  return String(val);
}

// Catalyst returns CREATEDTIME as "YYYY-MM-DD HH:mm:ss:SSS"; drop the milliseconds.
// Any other shape is shown unchanged.
function formatCommittedAt(val) {
  const match = /^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}):\d{3}$/.exec(String(val ?? ""));
  return match ? match[1] : val;
}

function formatCount(n) {
  return Number(n || 0).toLocaleString("en-IN");
}

function TransactionDeletionPage() {
  const [dates, setDates] = useState([]);
  const [datesLoading, setDatesLoading] = useState(false);
  const [datesError, setDatesError] = useState("");
  const [selectedDate, setSelectedDate] = useState("");

  // Form values while typing vs. the filters the table is actually showing.
  const [filterInputs, setFilterInputs] = useState(EMPTY_FILTERS);
  const [appliedFilters, setAppliedFilters] = useState(EMPTY_FILTERS);

  const [page, setPage] = useState(1);
  const [rows, setRows] = useState([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  // Preview only: marks live in memory and are never sent to the server.
  // `allMarked` means every row for the current date + filters (all pages).
  const [markedIds, setMarkedIds] = useState(new Set());
  const [allMarked, setAllMarked] = useState(false);

  /* Load the commit dates once and pre-select the newest. */
  useEffect(() => {
    let cancelled = false;
    (async () => {
      setDatesLoading(true);
      setDatesError("");
      try {
        const res = await fetch(`${BASE_URL}/transaction/commit-dates`);
        const json = await res.json();
        if (cancelled) return;
        if (!res.ok) {
          setDatesError(json.message || "Failed to load commit dates.");
          return;
        }
        const list = json.dates || [];
        setDates(list);
        if (list.length > 0) setSelectedDate(list[0].date);
      } catch (err) {
        console.error("Commit dates fetch error:", err);
        if (!cancelled) setDatesError("Failed to fetch commit dates.");
      } finally {
        if (!cancelled) setDatesLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  /* Load one page of committed rows for the date + applied filters. */
  useEffect(() => {
    if (!selectedDate) return;
    let cancelled = false;
    (async () => {
      setLoading(true);
      setError("");
      try {
        const params = new URLSearchParams({
          date: selectedDate,
          page: String(page),
          pageSize: String(PAGE_SIZE),
        });
        for (const [key, value] of Object.entries(appliedFilters)) {
          if (value.trim()) params.set(key, value.trim());
        }
        const res = await fetch(`${BASE_URL}/transaction/committed?${params}`);
        const json = await res.json();
        if (cancelled) return;
        if (!res.ok) {
          setError(json.message || "Failed to load transactions.");
          setRows([]);
          setTotal(0);
        } else {
          setRows(json.rows || []);
          setTotal(json.total || 0);
        }
      } catch (err) {
        console.error("Committed transactions fetch error:", err);
        if (!cancelled) {
          setError("Failed to fetch transactions.");
          setRows([]);
          setTotal(0);
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [selectedDate, appliedFilters, page]);

  // Marks belong to one date + filter view, so changing either starts fresh.
  const clearMarks = () => {
    setMarkedIds(new Set());
    setAllMarked(false);
  };

  const handleDateChange = (e) => {
    setSelectedDate(e.target.value);
    setPage(1);
    clearMarks();
  };

  const handleApplyFilters = (e) => {
    e.preventDefault();
    setAppliedFilters(filterInputs);
    setPage(1);
    clearMarks();
  };

  const handleClearFilters = () => {
    setFilterInputs(EMPTY_FILTERS);
    setAppliedFilters(EMPTY_FILTERS);
    setPage(1);
    clearMarks();
  };

  const toggleRow = (rowId) => {
    setMarkedIds((prev) => {
      const next = new Set(prev);
      if (next.has(rowId)) next.delete(rowId);
      else next.add(rowId);
      return next;
    });
  };

  const handleRemoveDisplayed = () => {
    setMarkedIds((prev) => {
      const next = new Set(prev);
      for (const row of rows) next.add(String(row.ROWID));
      return next;
    });
  };

  const handleSelectAll = () => {
    const ok = window.confirm(
      `Mark all ${formatCount(total)} rows for ${selectedDate} (current filters) for removal?`,
    );
    if (ok) setAllMarked(true);
  };

  const markedCount = allMarked ? total : markedIds.size;

  return (
    <MainLayout title="Transaction Deletion">
      <div className="temp-transaction-container txn-ops ops-page client-page txn-deletion-page">
        <div className="cm-verdict warn">
          <span className="cm-verdict-lead">Preview only.</span>{" "}
          Removing rows here only marks them on screen. Nothing is deleted from the database.
        </div>

        {/* Commit date + filters */}
        <div className="temp-transaction-card">
          <form className="txn-deletion-filters" onSubmit={handleApplyFilters}>
            <div className="cash-filter-field">
              <label>Commit Date</label>
              <select
                value={selectedDate}
                onChange={handleDateChange}
                disabled={datesLoading || dates.length === 0}
              >
                {datesLoading && <option value="">Loading…</option>}
                {!datesLoading && dates.length === 0 && (
                  <option value="">No commits found</option>
                )}
                {dates.map((d) => (
                  <option key={d.date} value={d.date}>
                    {d.date} ({formatCount(d.count)} rows)
                  </option>
                ))}
              </select>
            </div>
            <div className="cash-filter-field">
              <label>ISIN / Security Name</label>
              <input
                type="text"
                value={filterInputs.isin}
                onChange={(e) => setFilterInputs({ ...filterInputs, isin: e.target.value })}
              />
            </div>
            <div className="cash-filter-field">
              <label>Security Code</label>
              <input
                type="text"
                value={filterInputs.securityCode}
                onChange={(e) =>
                  setFilterInputs({ ...filterInputs, securityCode: e.target.value })
                }
              />
            </div>
            <div className="cash-filter-field">
              <label>Client Code</label>
              <input
                type="text"
                value={filterInputs.clientCode}
                onChange={(e) =>
                  setFilterInputs({ ...filterInputs, clientCode: e.target.value })
                }
              />
            </div>
            <div className="cash-filter-field">
              <label>Transaction Date</label>
              <input
                type="date"
                value={filterInputs.tranDate}
                onChange={(e) => setFilterInputs({ ...filterInputs, tranDate: e.target.value })}
              />
            </div>
            <div className="cash-filter-field">
              <label>Settlement Date</label>
              <input
                type="date"
                value={filterInputs.setDate}
                onChange={(e) => setFilterInputs({ ...filterInputs, setDate: e.target.value })}
              />
            </div>
            <div className="cash-filter-field ops-field-end txn-deletion-filter-actions">
              <button type="submit" className="cm-primary" disabled={!selectedDate}>
                Apply
              </button>
              <button type="button" className="cm-secondary" onClick={handleClearFilters}>
                Clear
              </button>
            </div>
          </form>
          {datesError && <div className="cm-verdict bad">{datesError}</div>}
        </div>

        {/* Preview table */}
        {selectedDate && (
          <div className="client-details-section">
            <div className="temp-transaction-card">
              <div className="client-details-header">
                <h3>Committed on {selectedDate}</h3>
                <span className="count-pill">
                  {formatCount(total)} rows · {formatCount(markedCount)} marked for removal
                </span>
              </div>

              {loading && <p className="cm-empty">Loading transactions...</p>}
              {!loading && error && <div className="cm-verdict bad">{error}</div>}
              {!loading && !error && rows.length === 0 && (
                <p className="cm-empty">No transactions match your filters.</p>
              )}

              {!loading && !error && rows.length > 0 && (
                <>
                  <div className="data-table-wrap">
                    <table className="data-table">
                      <thead>
                        <tr>
                          {COLUMNS.map((c) => (
                            <th key={c.key} className={c.className}>{c.label}</th>
                          ))}
                          <th aria-label="Remove" />
                        </tr>
                      </thead>
                      <tbody>
                        {rows.map((row) => {
                          const rowId = String(row.ROWID);
                          const isMarked = allMarked || markedIds.has(rowId);
                          return (
                            <tr key={rowId} className={isMarked ? "txn-row-marked" : ""}>
                              {COLUMNS.map((c) => (
                                <td key={c.key} className={c.className}>
                                  {displayValue(c.format ? c.format(row[c.key]) : row[c.key])}
                                </td>
                              ))}
                              <td className="txn-row-action">
                                {isMarked && <span className="txn-marked-tag">To be deleted</span>}
                                <button
                                  type="button"
                                  className="txn-remove-btn"
                                  onClick={() => toggleRow(rowId)}
                                  disabled={allMarked}
                                  title={isMarked ? "Undo" : "Remove"}
                                >
                                  {isMarked ? "↺" : "✕"}
                                </button>
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>

                  <div className="txn-deletion-pagination">
                    <Pagination
                      currentPage={page}
                      pageSize={PAGE_SIZE}
                      totalRows={total}
                      onPageChange={setPage}
                    />
                  </div>

                  <div className="txn-deletion-actions">
                    <button
                      type="button"
                      className="cm-primary"
                      onClick={handleSelectAll}
                      disabled={allMarked}
                    >
                      Mark All ({formatCount(total)}) for Removal
                    </button>
                    <button
                      type="button"
                      className="cm-secondary"
                      onClick={handleRemoveDisplayed}
                      disabled={allMarked}
                    >
                      Remove displayed
                    </button>
                    <button
                      type="button"
                      className="cm-secondary"
                      onClick={clearMarks}
                      disabled={markedCount === 0}
                    >
                      Reset
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

export default TransactionDeletionPage;
