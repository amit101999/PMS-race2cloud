import React, { useState, useEffect, useRef, useMemo } from "react";
import MainLayout from "../../layouts/MainLayout.js";
import {
  Card,
  TextInput,
  Pagination,
} from "../../components/common/CommonComponents.js";
import TransactionPage from "../TransactionPage/TransactionPage.js";
import "./AnalyticsPage.css";
import { useAccountCodes } from "../../hooks/GetAllCodes.js";
import { useHoldings } from "../../hooks/GetHolding.js";
import Holdingtabs from "./tabs/holding/HoldingTab";
import Allocation from "./tabs/allocations/AllocationTab";
import Performance from "./tabs/performance/PerformanceTab";
import TransactionTab from "./tabs/transaction/TransactionTab";

function AnalyticsPage() {
  const { clientOptions } = useAccountCodes();
  const { holdings, setHoldings, loadingHoldings, fetchHoldings } =
    useHoldings();

  const [selectedStock, setSelectedStock] = useState(null);
  const [accountCode, setAccountCode] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [showDropdown, setShowDropdown] = useState(false);
  const dropdownRef = useRef(null);
  const [viewMode, setViewMode] = useState("all");
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  const [asOnDate, setAsOnDate] = useState("");

  useEffect(() => {
    const handleClickOutside = (e) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target)) {
        setShowDropdown(false);
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const filteredOptions = clientOptions.filter((opt) =>
    opt.label.toLowerCase().includes(searchQuery.toLowerCase())
  );
  const handleInputChange = (e) => {
    setSearchQuery(e.target.value);
    setShowDropdown(true);
  };

  const handlePageChange = (page) => {
    setCurrentPage(page);
  };

  const handlePageSizeChange = (size) => {
    setPageSize(size);
    setCurrentPage(1);
  };

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

  const handleSelect = (option) => {
    setSearchQuery(option.label);
    setAccountCode(option.value);
    setShowDropdown(false);
    fetchHoldings(option.value, asOnDate);
  };
  const TAB_ITEMS = [
    {
      key: "holding",
      label: "Holding",
      component: () => (
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
      ),
    },
    { key: "allocation", label: "Allocation", component: Allocation },
    { key: "performance", label: "Performance", component: Performance },
    { key: "transaction", label: "Transaction", component: TransactionTab },
  ];
  const [activeTab, setActiveTab] = useState("holding");
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

      {/* Analytics Tabs */}
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

      {/* Active Tab Content */}
      <div className="analytics-tab-content">
        {(() => {
          const tab = TAB_ITEMS.find((t) => t.key === activeTab);
          if (!tab) return null;
          const Component = tab.component;
          return <Component />;
        })()}
      </div>

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
