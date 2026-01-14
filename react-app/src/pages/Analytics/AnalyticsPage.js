import React, { useState, useEffect, useRef, useMemo } from "react";
import MainLayout from "../../layouts/MainLayout.js";
import { Card, TextInput } from "../../components/common/CommonComponents.js";
import TransactionPage from "../TransactionPage/TransactionPage.js";
import "./AnalyticsPage.css";
import { useAccountCodes } from "../../hooks/GetAllCodes.js";
import { useHoldings } from "../../hooks/GetHolding.js";

import Holdingtabs from "./tabs/holding/HoldingTab";
import Allocation from "./tabs/allocations/AllocationTab";
import Performance from "./tabs/performance/PerformanceTab";
import TransactionTab from "./tabs/transaction/TransactionTab";

function AnalyticsPage() {
  /* -------------------- DATA HOOKS -------------------- */
  const { clientOptions } = useAccountCodes();
  const { holdings, setHoldings, loadingHoldings, fetchHoldings } =
    useHoldings();

  /* -------------------- STATE -------------------- */
  const [accountCode, setAccountCode] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [showDropdown, setShowDropdown] = useState(false);
  const [asOnDate, setAsOnDate] = useState("");

  const [activeTab, setActiveTab] = useState("holding");
  const [selectedStock, setSelectedStock] = useState(null);

  const [viewMode, setViewMode] = useState("all");
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);

  const dropdownRef = useRef(null);
  /* -------------------- EFFECTS -------------------- */
  useEffect(() => {
    const handleClickOutside = (e) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target)) {
        setShowDropdown(false);
      }
    };

    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  /* -------------------- DERIVED DATA -------------------- */
  const filteredOptions = useMemo(() => {
    return clientOptions.filter((opt) =>
      opt.label.toLowerCase().includes(searchQuery.toLowerCase())
    );
  }, [clientOptions, searchQuery]);

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

  /* -------------------- HANDLERS -------------------- */
  const handleSearchChange = (e) => {
    setSearchQuery(e.target.value);
    setShowDropdown(true);
  };

  const handleAccountSelect = (option) => {
    setSearchQuery(option.label);
    setAccountCode(option.value);
    setShowDropdown(false);
    fetchHoldings(option.value, asOnDate);
  };

  const clearAccountSelection = () => {
    setSearchQuery("");
    setAccountCode("");
    setHoldings([]);
    setSelectedStock(null);
  };

  const handleDateChange = (e) => {
    const date = e.target.value;
    setAsOnDate(date);
    setCurrentPage(1);

    if (accountCode) {
      fetchHoldings(accountCode, date);
    }
  };

  const handlePageChange = (page) => setCurrentPage(page);

  const handlePageSizeChange = (size) => {
    setPageSize(size);
    setCurrentPage(1);
  };

  /* -------------------- TABS CONFIG -------------------- */
  const TAB_ITEMS = [
    { key: "holding", label: "Holding" },
    { key: "allocation", label: "Allocation" },
    { key: "performance", label: "Performance" },
    { key: "transaction", label: "Transaction" },
  ];

  const renderActiveTab = () => {
    switch (activeTab) {
      case "holding":
        return (
          <Holdingtabs
            viewMode={viewMode}
            setViewMode={setViewMode}
            holdings={holdings}
            accountCode={accountCode}
            asOnDate={asOnDate}
            selectedStock={selectedStock}
            setSelectedStock={setSelectedStock}
            pageSize={pageSize}
            currentPage={currentPage}
            onPageChange={handlePageChange}
            onPageSizeChange={handlePageSizeChange}
          />
        );

      case "allocation":
        return <Allocation />;

      case "performance":
        return <Performance />;

      case "transaction":
        return <TransactionTab accountCode={accountCode} asOnDate={asOnDate} />;

      default:
        return null;
    }
  };

  /* -------------------- JSX -------------------- */
  return (
    <MainLayout title="Analytics Filters">
      {/* Filters */}
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
                onChange={handleSearchChange}
                onFocus={() => setShowDropdown(true)}
              />

              {searchQuery && (
                <span className="clear-icon" onClick={clearAccountSelection}>
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
                      onClick={() => handleAccountSelect(opt)}
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
            onChange={handleDateChange}
          />
        </div>
      </Card>

      {/* Tabs */}
      <div className="analytics-tabs">
        {TAB_ITEMS.map((tab) => (
          <button
            key={tab.key}
            className={`analytics-tab ${activeTab === tab.key ? "active" : ""}`}
            onClick={() => setActiveTab(tab.key)}
          >
            {tab.label}
          </button>
        ))}
      </div>
      {/* Holding Summary – only for Holding tab */}
      {activeTab === "holding" && (
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
                {summaryCards.map((item) => (
                  <tr key={item.key} className="summary-row">
                    <td>
                      <div className="summary-desc">
                        <span className={`summary-dot ${item.key}`} />
                        {item.label}
                      </div>
                    </td>
                    <td>{item.cost}</td>
                    <td>{item.mktVal}</td>
                    <td>{item.income}</td>
                    <td>{item.gl}</td>
                    <td>{item.glPct}</td>
                    <td>{item.pctAssets}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Tab Content */}
      <div className="analytics-tab-content">{renderActiveTab()}</div>

      {loadingHoldings && <p className="loading-text">Loading holdings...</p>}

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
