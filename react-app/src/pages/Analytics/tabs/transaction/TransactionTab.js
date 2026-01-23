import React, { useEffect, useState, useRef, useMemo } from "react";
import {
  Card,
  Pagination,
} from "../../../../components/common/CommonComponents";
import { BASE_URL } from "../../../../constant";
import "./TransactionTab.css";

const PAGE_SIZE_DEFAULT = 20;

const TransactionTab = ({ accountCode, asOnDate }) => {
  const [data, setData] = useState([]);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(PAGE_SIZE_DEFAULT);
  const [totalRows, setTotalRows] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const [secSearch, setSecSearch] = useState("");
  const [selectedSecurity, setSelectedSecurity] = useState("");
  const [secOptions, setSecOptions] = useState([]);
  const [showSecDropdown, setShowSecDropdown] = useState(false);
  const secDropdownRef = useRef(null);

  // Close dropdown on outside click
  useEffect(() => {
    const handler = (e) => {
      if (
        secDropdownRef.current &&
        !secDropdownRef.current.contains(e.target)
      ) {
        setShowSecDropdown(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  // Fetch transactions (filters: account, date, selected security)
  useEffect(() => {
    const fetchPage = async () => {
      setLoading(true);
      setError("");
      try {
        const params = new URLSearchParams({
          page,
          limit: pageSize,
          ...(accountCode ? { accountCode } : {}),
          ...(asOnDate ? { asOnDate } : {}),
          ...(selectedSecurity ? { securityName: selectedSecurity } : {}),
        });
        const res = await fetch(
          `${BASE_URL}/analytics/getPaginatedTransactions?${params.toString()}`
        );
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const json = await res.json();
        setData(json.data || []);
        setTotalRows(json.pagination?.total || 0);
      } catch (err) {
        setError(err.message || "Failed to load transactions");
      } finally {
        setLoading(false);
      }
    };
    fetchPage();
  }, [page, pageSize, accountCode, asOnDate, selectedSecurity]);

  // Fetch security options (on open/typing, debounce 400ms)
  useEffect(() => {
    const id = setTimeout(async () => {
      try {
        const params = new URLSearchParams({
          ...(secSearch ? { search: secSearch } : {}),
          ...(accountCode ? { accountCode } : {}),
        });
        const res = await fetch(
          `${BASE_URL}/analytics/getSecurityNameOptions?${params.toString()}`
        );
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const json = await res.json();
        setSecOptions(json.data || []);
      } catch {}
    }, 500);
    return () => clearTimeout(id);
  }, [secSearch, accountCode]);

  // Client-side contains filter over fetched options
  const filteredSecOptions = useMemo(() => {
    const q = secSearch.trim().toLowerCase();
    if (!q) return secOptions;
    return secOptions.filter((name) => name.toLowerCase().indexOf(q) !== -1);
  }, [secOptions, secSearch]);

  const formatNumber = (v) =>
    v === null || v === undefined || Number.isNaN(Number(v))
      ? "-"
      : Number(v).toLocaleString("en-IN", { maximumFractionDigits: 2 });

  return (
    <Card>
      <div className="transaction-tab-header">
        <h3>Transaction History</h3>

        <div className="security-name-search" ref={secDropdownRef}>
          <label className="search-label">Security Name</label>

          <div className="search-input-wrapper">
            <span className="search-icon">🔍</span>

            <input
              type="text"
              className="search-input"
              placeholder="Search Security Name..."
              value={secSearch}
              onChange={(e) => {
                setSecSearch(e.target.value);
                setShowSecDropdown(true);
              }}
              onFocus={() => setShowSecDropdown(true)}
            />

            {secSearch && (
              <span
                className="clear-icon"
                onClick={() => {
                  setSecSearch("");
                  setSelectedSecurity("");
                  setSecOptions([]);
                  setShowSecDropdown(false);
                  setPage(1);
                }}
              >
                ✕
              </span>
            )}

            <span className="arrow-icon">▾</span>
          </div>

          {showSecDropdown && (
            <div className="search-dropdown">
              <div className="dropdown-header">Search Security Name...</div>

              <div className="dropdown-options">
                {filteredSecOptions.map((name) => (
                  <div
                    key={name}
                    className="dropdown-option"
                    onClick={() => {
                      setSelectedSecurity(name);
                      setSecSearch(name);
                      setShowSecDropdown(false);
                      setPage(1);
                    }}
                  >
                    {name}
                  </div>
                ))}
              </div>

              <div className="dropdown-footer">
                {filteredSecOptions.length} option
                {filteredSecOptions.length === 1 ? "" : "s"}
              </div>
            </div>
          )}
        </div>
      </div>

      {loading && <div>Loading...</div>}
      {error && <div className="error-text">{error}</div>}

      {!loading && !error && (
        <div className="transaction-table-wrapper">
          <table className="transaction-table">
            <thead>
              <tr>
                <th>Date</th>
                <th>Type</th>
                <th>Security Name</th>
                <th>ISIN</th>
                <th className="num">Quantity</th>
                <th className="num">Price</th>
                <th className="num">Total Amount</th>
                <th>Cash Balance</th>
              </tr>
            </thead>
            <tbody>
              {data.length === 0 ? (
                <tr>
                  <td colSpan={7} className="empty-cell">
                    No transactions found
                  </td>
                </tr>
              ) : (
                data.map((t) => (
                  <tr key={t.rowId}>
                    <td>{t.date || "-"}</td>
                    <td>{t.type || "-"}</td>
                    <td>{t.securityName || "-"}</td>
                    <td>{t.isin || "-"}</td>
                    <td className="num">{formatNumber(t.quantity)}</td>
                    <td className="num">{formatNumber(t.price)}</td>
                    <td className="num">{formatNumber(t.totalAmount)}</td>
                    <td className="num">{formatNumber(t.cashBalance)}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      )}

      <Pagination
        currentPage={page}
        pageSize={pageSize}
        totalRows={totalRows}
        onPageChange={setPage}
        onPageSizeChange={(size) => {
          setPageSize(size);
          setPage(1);
        }}
      />
    </Card>
  );
};

export default TransactionTab;
