import React, { useState, useEffect, useRef, useMemo } from "react";
import MainLayout from "../../layouts/MainLayout.js";
import {
  Card,
  TextInput,
  Pagination,
} from "../../components/common/CommonComponents.js";
import TransactionPage from "../TransactionPage/TransactionPage.js";
import "./AnalyticsPage.css";
import { BASE_URL } from "../../constant.js";

function AnalyticsPage() {
  const [clientOptions, setClientOptions] = useState([]);
  const [holdings, setHoldings] = useState([]);
  const [loadingHoldings, setLoadingHoldings] = useState(false);
  const [selectedStock, setSelectedStock] = useState(null);
  const [accountCode, setAccountCode] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [showDropdown, setShowDropdown] = useState(false);
  const dropdownRef = useRef(null);
  const [viewMode, setViewMode] = useState("all"); // all | equity | cash
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  const [asOnDate, setAsOnDate] = useState("");

  useEffect(() => {
    fetchClientIds();
  }, []);

  useEffect(() => {
    const handleClickOutside = (e) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target)) {
        setShowDropdown(false);
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const fetchClientIds = async () => {
    try {
      const res = await fetch(`${BASE_URL}/analytics/getAllAccountCodes`);
      const data = await res.json();

      const options = data.data.map((row) => ({
        value: row.clientIds.WS_Account_code,
        label: row.clientIds.WS_Account_code,
      }));

      setClientOptions(options);
    } catch (err) {
      console.error("Failed to fetch account codes:", err);
    }
  };

  const fetchHoldings = async (code, date = asOnDate) => {
    if (!code) return;

    setAccountCode(code);
    if (date !== undefined) {
      setAsOnDate(date);
    }
    setLoadingHoldings(true);
    setHoldings([]);
    setSelectedStock(null);

    try {
      const params = new URLSearchParams({ accountCode: code });
      if (date) params.set("asOnDate", date);
      const res = await fetch(
        `${BASE_URL}/analytics/getHoldingsSummarySimple?${params.toString()}`
      );
      const data = await res.json();

      if (Array.isArray(data)) {
        console.log("Fetched holdings:", data);
        setHoldings(data);
      } else {
        console.error("Unexpected holdings response:", data);
        setHoldings([]);
      }
    } catch (err) {
      console.error("Failed to fetch holdings:", err);
    } finally {
      setLoadingHoldings(false);
    }
  };

  const filteredOptions = clientOptions.filter((opt) =>
    opt.label.toLowerCase().includes(searchQuery.toLowerCase())
  );

  const summaryCards = useMemo(
    () => [
      {
        key: "total",
        label: "Total",
        cost: "3.46 Cr",
        mktVal: "3.50 Cr",
        income: "54.73 K",
        gl: "4.49 L",
        glPct: "1.30 %",
        pctAssets: "100.00 %",
      },
      {
        key: "equity",
        label: "Equity",
        cost: "3.14 Cr",
        mktVal: "3.18 Cr",
        income: "54.73 K",
        gl: "4.49 L",
        glPct: "1.43 %",
        pctAssets: "90.89 %",
      },
      {
        key: "cash",
        label: "Cash and Equivalent",
        cost: "31.92 L",
        mktVal: "31.92 L",
        income: "–",
        gl: "–",
        glPct: "0.00 %",
        pctAssets: "9.11 %",
      },
    ],
    []
  );

  const displayedHoldings = useMemo(() => {
    if (viewMode === "cash") return [];
    return Array.isArray(holdings) ? holdings : [];
  }, [viewMode, holdings]);

  const rowsForTable =
    displayedHoldings.length === 0
      ? [
          {
            stockName: viewMode === "cash" ? "Cash and Equivalent" : "—",
            securityCode: "—",
            currentHolding: "—",
            avgPrice: "—",
            holdingValue: "—",
          },
        ]
      : displayedHoldings.map((h) => ({
          ...h,
          // For any fields not present originally, we keep them out or default to 0 later if needed
        }));

  const paginatedRows = useMemo(() => {
    const start = (currentPage - 1) * pageSize;
    return rowsForTable.slice(start, start + pageSize);
  }, [rowsForTable, currentPage, pageSize]);

  const formatNumber = (value) => {
    const n = Number(value);
    if (!Number.isFinite(n)) return "—";
    return n.toLocaleString("en-IN", {
      minimumFractionDigits: 0,
      maximumFractionDigits: 2,
    });
  };

  const handlePageChange = (page) => setCurrentPage(page);
  const handlePageSizeChange = (size) => {
    setPageSize(size);
    setCurrentPage(1);
  };

  const handleInputChange = (e) => {
    setSearchQuery(e.target.value);
    setShowDropdown(true);
  };

  const handleSelect = (option) => {
    setSearchQuery(option.label);
    setAccountCode(option.value);
    setShowDropdown(false);
    fetchHoldings(option.value, asOnDate);
  };

  const handleStockSelect = (stock) => {
    setSelectedStock({
      ...stock,
      accountCode: accountCode,
    });
  };

  return (
    <MainLayout title="Analytics Filters">
      <Card className="filters-card">
        <div className="filters-grid">
          <div className="account-code-search" ref={dropdownRef}>
            <label className="search-label">Account Code</label>
            <div className="search-input-wrapper">
              <span className="search-icon">🔍</span>
              <input
                type="text"
                className="search-input"
                placeholder="Search Account Code..."
                value={searchQuery}
                onChange={handleInputChange}
                onFocus={() => setShowDropdown(true)}
              />
              {searchQuery && (
                <span
                  className="clear-icon"
                  onClick={() => {
                    setSearchQuery("");
                    setAccountCode("");
                    setHoldings([]);
                    setSelectedStock(null);
                  }}
                >
                  ✕
                </span>
              )}
              <span className="arrow-icon">▾</span>
            </div>
            {showDropdown && filteredOptions.length > 0 && (
              <div className="search-dropdown">
                <div className="dropdown-header">Search Account Code...</div>
                <div className="dropdown-options">
                  {filteredOptions.map((opt) => (
                    <div
                      key={opt.value}
                      className="dropdown-option"
                      onClick={() => handleSelect(opt)}
                    >
                      {opt.label}
                    </div>
                  ))}
                </div>
                <div className="dropdown-footer">
                  {filteredOptions.length} of {clientOptions.length} options
                </div>
              </div>
            )}
          </div>

          <TextInput
            label="Filter by Date"
            type="date"
            value={asOnDate}
            onChange={(e) => {
              const selectedDate = e.target.value;
              setAsOnDate(selectedDate);
              setCurrentPage(1);

              if (accountCode) {
                fetchHoldings(accountCode, selectedDate);
              }
            }}
          />
        </div>
      </Card>

      {loadingHoldings && <p className="loading-text">Loading holdings...</p>}

      {/* Holding Summary Table */}
      <div className="holding-summary-table">
        <div className="summary-table-header">
          <h3>Holding Summary</h3>
          <span className="summary-sort-icon">⇅</span>
        </div>
        <div className="summary-table-wrapper">
          <table className="summary-table">
            <thead>
              <tr>
                <th>Description</th>
                <th>Cost</th>
                <th>Mkt Val</th>
                <th>Income</th>
                <th>G/L</th>
                <th>% G/L</th>
                <th>% Assets</th>
              </tr>
            </thead>
            <tbody>
              {summaryCards.map((c) => {
                const isActive =
                  (c.key === "total" && viewMode === "all") ||
                  (c.key === "equity" && viewMode === "equity") ||
                  (c.key === "cash" && viewMode === "cash");

                const handleRowClick = () => {
                  if (c.key === "total") setViewMode("all");
                  else if (c.key === "equity") setViewMode("equity");
                  else if (c.key === "cash") setViewMode("cash");
                };

                return (
                  <tr
                    key={c.key}
                    className={isActive ? "summary-row active" : "summary-row"}
                    onClick={handleRowClick}
                  >
                    <td className="summary-desc">
                      <span
                        className={`summary-dot ${c.key}`}
                        aria-hidden="true"
                      ></span>
                      {c.label}
                    </td>
                    <td>{c.cost}</td>
                    <td>{c.mktVal}</td>
                    <td>–</td>
                    <td>{c.gl}</td>
                    <td>{c.glPct}</td>
                    <td>{c.pctAssets}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Tabs */}
      <div className="holding-tabs">
        {["all", "equity", "cash"].map((m) => (
          <button
            key={m}
            className={`holding-tab ${viewMode === m ? "active" : ""}`}
            onClick={() => setViewMode(m)}
          >
            {m === "all" ? "All" : m === "equity" ? "Equity" : "Cash"}
          </button>
        ))}
      </div>

      {/* Holdings Table */}
      <div className="holdings-table-wrapper analytics-table-wrapper">
        <table className="analytics-table">
          <thead>
            <tr>
              <th>Security Name</th>
              <th>Security Code</th>
              <th>Security ISIN</th>
              <th>Current Holding</th>
              <th>Average Holding Value</th>
              <th>Holding Value</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {paginatedRows.map((row, i) => (
              <tr
                key={row.stockName + i}
                onClick={() =>
                  setSelectedStock({
                    ...row,
                    accountCode: accountCode,
                  })
                }
                style={{ cursor: "pointer" }}
              >
                <td>{row.stockName}</td>
                <td>{row.securityCode || "–"}</td>
                <td>{row.isin || "–"}</td>
                <td>{formatNumber(row.currentHolding)}</td>
                <td>{formatNumber(row.avgPrice)}</td>
                <td>{formatNumber(row.holdingValue)}</td>
                <td className="analytics-action-cell">
                  <button
                    type="button"
                    className="analytics-view-btn"
                    onClick={(e) => {
                      e.stopPropagation();
                      setSelectedStock({
                        ...row,
                        accountCode: accountCode,
                      });
                    }}
                  >
                    View
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      <div className="analytics-pagination">
        <Pagination
          currentPage={currentPage}
          pageSize={pageSize}
          totalRows={rowsForTable.length}
          onPageChange={handlePageChange}
          onPageSizeChange={handlePageSizeChange}
        />
      </div>

      {selectedStock && (
        <TransactionPage
          key={`${selectedStock.securityCode}-${asOnDate}`}
          stock={selectedStock}
          accountCode={accountCode}
          asOnDate={asOnDate}
          onClose={() => setSelectedStock(null)}
        />
      )}
    </MainLayout>
  );
}

export default AnalyticsPage;
