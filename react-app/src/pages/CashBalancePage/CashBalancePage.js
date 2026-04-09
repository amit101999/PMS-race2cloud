import React, { useState, useEffect, useRef, useMemo, useCallback } from "react";
import MainLayout from "../../layouts/MainLayout";
import { Card } from "../../components/common/CommonComponents";
import { useAccountCodes } from "../../hooks/GetAllCodes";
import { BASE_URL } from "../../constant";
import "./CashBalancePage.css";

function escapeCsvCell(value) {
  const s = value === null || value === undefined ? "" : String(value);
  if (/[",\r\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function formatINR(val) {
  if (val === null || val === undefined || val === "") return "–";
  const n = Number(val);
  if (isNaN(n)) return "–";
  return n.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function CashBalancePage() {
  const { clientOptions } = useAccountCodes();

  const [accountCode, setAccountCode] = useState("");
  const [accountSearch, setAccountSearch] = useState("");
  const [showAccountDropdown, setShowAccountDropdown] = useState(false);

  const [fromDate, setFromDate] = useState("");
  const [toDate, setToDate] = useState("");
  const [searchInput, setSearchInput] = useState("");

  const [rows, setRows] = useState([]);
  const [totalCount, setTotalCount] = useState(0);
  const [loading, setLoading] = useState(false);
  const [fetched, setFetched] = useState(false);

  const [currentPage, setCurrentPage] = useState(1);
  const pageSize = 25;

  const dropdownRef = useRef(null);

  useEffect(() => {
    const handler = (e) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target)) {
        setShowAccountDropdown(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  const filteredAccounts = useMemo(() => {
    return clientOptions.filter((opt) =>
      opt.label.toLowerCase().includes(accountSearch.toLowerCase())
    );
  }, [clientOptions, accountSearch]);

  const fetchData = useCallback(
    async (page = 1) => {
      if (!accountCode) return;
      setLoading(true);
      try {
        const params = new URLSearchParams({
          accountCode,
          page: String(page),
          pageSize: String(pageSize),
        });
        if (fromDate) params.set("fromDate", fromDate);
        if (toDate) params.set("toDate", toDate);
        if (searchInput.trim()) params.set("search", searchInput.trim());

        const res = await fetch(`${BASE_URL}/cash-balance/passbook?${params}`);
        const json = await res.json();

        setRows(json.data || []);
        setTotalCount(json.totalCount || 0);
        setCurrentPage(page);
        setFetched(true);
      } catch (err) {
        console.error("Cash passbook fetch error:", err);
        setRows([]);
        setTotalCount(0);
      } finally {
        setLoading(false);
      }
    },
    [accountCode, fromDate, toDate, searchInput, pageSize]
  );

  const handleFetch = () => {
    setCurrentPage(1);
    fetchData(1);
  };

  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));

  const closingBalance = useMemo(() => {
    if (!rows.length) return null;
    return rows[rows.length - 1];
  }, [rows]);

  const handleExport = () => {
    if (!rows.length) return;

    const headers = [
      "Date", "Account Code", "ISIN", "Security Name", "Tran Type",
      "QTY", "Rate", "Debit", "Credit", "Running Balance",
    ];

    const csvRows = rows.map((r) => [
      escapeCsvCell(r.Impact_Date),
      escapeCsvCell(r.Account_Code),
      escapeCsvCell(r.ISIN),
      escapeCsvCell(r.Security_Name),
      escapeCsvCell(r.Tran_Type),
      escapeCsvCell(r.QTY),
      escapeCsvCell(r.Rate),
      escapeCsvCell(r.Debit),
      escapeCsvCell(r.Credit),
      escapeCsvCell(r.Running_Balance),
    ]);

    const csv = [headers.join(","), ...csvRows.map((r) => r.join(","))].join("\n");
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `CashBalance_${accountCode}_${new Date().toISOString().split("T")[0]}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <MainLayout title="Cash Balance">
      <div className="cash-balance-page">
        {/* Filters */}
        <Card>
          <div className="filters-card">
            {/* Account Code Dropdown */}
            <div className="cash-filter-field">
              <label>Account Code</label>
              <div className="cash-account-wrapper" ref={dropdownRef}>
                <input
                  type="text"
                  placeholder="Search account..."
                  value={accountSearch}
                  onChange={(e) => {
                    setAccountSearch(e.target.value);
                    setShowAccountDropdown(true);
                  }}
                  onFocus={() => setShowAccountDropdown(true)}
                />
                {showAccountDropdown && filteredAccounts.length > 0 && (
                  <ul className="cash-account-dropdown">
                    {filteredAccounts.map((opt) => (
                      <li
                        key={opt.value}
                        onClick={() => {
                          setAccountCode(opt.value);
                          setAccountSearch(opt.label);
                          setShowAccountDropdown(false);
                          setFetched(false);
                          setRows([]);
                        }}
                      >
                        {opt.label}
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            </div>

            {/* From Date */}
            <div className="cash-filter-field">
              <label>From Date</label>
              <input
                type="date"
                value={fromDate}
                onChange={(e) => setFromDate(e.target.value)}
              />
            </div>

            {/* To Date */}
            <div className="cash-filter-field">
              <label>To Date</label>
              <input
                type="date"
                value={toDate}
                onChange={(e) => setToDate(e.target.value)}
              />
            </div>

            {/* Search */}
            <div className="cash-filter-field">
              <label>Search (Name / ISIN / Code)</label>
              <input
                type="text"
                placeholder="Search..."
                value={searchInput}
                onChange={(e) => setSearchInput(e.target.value)}
                onKeyDown={(e) => { if (e.key === "Enter") handleFetch(); }}
              />
            </div>

            {/* Fetch Button */}
            <div className="cash-filter-field" style={{ justifyContent: "flex-end" }}>
              <button
                className="cash-fetch-btn"
                onClick={handleFetch}
                disabled={!accountCode || loading}
              >
                {loading ? "Loading..." : "Fetch"}
              </button>
            </div>
          </div>
        </Card>

        {/* Summary Strip */}
        {fetched && closingBalance && (
          <div className="cash-summary-strip">
            <div className="cash-summary-card">
              <span className="label">Account</span>
              <span className="value">{accountCode}</span>
            </div>
            <div className="cash-summary-card">
              <span className="label">Closing Balance (Page)</span>
              <span className="value">{formatINR(closingBalance.Running_Balance)}</span>
            </div>
            <div className="cash-summary-card">
              <span className="label">Total Records</span>
              <span className="value">{totalCount.toLocaleString("en-IN")}</span>
            </div>
          </div>
        )}

        {/* Table */}
        {fetched && (
          <div className="cash-table-section">
            <Card>
              <div className="cash-table-header">
                <h3>Cash Passbook</h3>
                <span className="cash-row-count">
                  Showing {rows.length} of {totalCount} records
                </span>
              </div>

              {loading ? (
                <div className="cash-loading">Loading transactions...</div>
              ) : rows.length === 0 ? (
                <div className="cash-empty">No transactions found for the selected filters.</div>
              ) : (
                <>
                  <div className="cash-table-wrapper">
                    <table className="cash-table">
                      <thead>
                        <tr>
                          <th>#</th>
                          <th>Date</th>
                          <th>Account</th>
                          <th>ISIN</th>
                          <th>Security Name</th>
                          <th>Tran Type</th>
                          <th>QTY</th>
                          <th>Rate</th>
                          <th>Debit</th>
                          <th>Credit</th>
                          <th>Balance</th>
                        </tr>
                      </thead>
                      <tbody>
                        {rows.map((r, idx) => {
                          const bal = Number(r.Running_Balance) || 0;
                          return (
                            <tr key={idx}>
                              <td>{(currentPage - 1) * pageSize + idx + 1}</td>
                              <td>{r.Impact_Date || "–"}</td>
                              <td>{r.Account_Code || "–"}</td>
                              <td>{r.ISIN || "–"}</td>
                              <td>{r.Security_Name || "–"}</td>
                              <td>{r.Tran_Type || "–"}</td>
                              <td>{r.QTY ?? "–"}</td>
                              <td>{r.Rate != null ? formatINR(r.Rate) : "–"}</td>
                              <td className="cash-debit">
                                {r.Debit ? formatINR(r.Debit) : "–"}
                              </td>
                              <td className="cash-credit">
                                {r.Credit ? formatINR(r.Credit) : "–"}
                              </td>
                              <td className={bal >= 0 ? "cash-balance-positive" : "cash-balance-negative"}>
                                {formatINR(bal)}
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>

                  {/* Pagination */}
                  <div className="cash-pagination">
                    <button
                      disabled={currentPage <= 1}
                      onClick={() => fetchData(currentPage - 1)}
                    >
                      Prev
                    </button>
                    <span>
                      Page {currentPage} of {totalPages}
                    </span>
                    <button
                      disabled={currentPage >= totalPages}
                      onClick={() => fetchData(currentPage + 1)}
                    >
                      Next
                    </button>
                  </div>
                </>
              )}

              {/* Export */}
              {rows.length > 0 && (
                <div className="cash-export-bar">
                  <button
                    className="cash-export-btn"
                    onClick={handleExport}
                    disabled={loading}
                  >
                    Export CSV
                  </button>
                </div>
              )}
            </Card>
          </div>
        )}
      </div>
    </MainLayout>
  );
}

export default CashBalancePage;
