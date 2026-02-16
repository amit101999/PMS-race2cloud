import React, { useEffect, useState, useRef, useMemo, useCallback } from "react";
import { Card } from "../../../../components/common/CommonComponents";
import { BASE_URL } from "../../../../constant";
import "./TransactionTab.css";

const PAGE_SIZE_DEFAULT = 20;

const TransactionTab = ({ accountCode, asOnDate }) => {
  const [data, setData] = useState([]);
  const [nextCursor, setNextCursor] = useState(null);
  const [prevCursor, setPrevCursor] = useState(null);
  const [hasNext, setHasNext] = useState(false);
  const [hasPrev, setHasPrev] = useState(false);
  const [limit] = useState(PAGE_SIZE_DEFAULT);
  const [loading, setLoading] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState("");

  const [secSearch, setSecSearch] = useState("");
  const [selectedSecurity, setSelectedSecurity] = useState("");
  const [secOptions, setSecOptions] = useState([]);
  const [showSecDropdown, setShowSecDropdown] = useState(false);
  const secDropdownRef = useRef(null);
  const initialLoadIdRef = useRef(0);

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

  const buildBaseParams = useCallback(() => {
    const params = {
      limit,
      direction: "next",
      accountCode,
      ...(asOnDate ? { asOnDate } : {}),
      ...(selectedSecurity ? { securityName: selectedSecurity } : {}),
    };
    return params;
  }, [limit, accountCode, asOnDate, selectedSecurity]);

  const fetchPage = useCallback(
    async (cursor, direction = "next") => {
      const params = new URLSearchParams({
        ...buildBaseParams(),
        direction,
      });
      if (cursor?.lastDate != null && cursor?.lastRowId != null) {
        params.set("lastDate", cursor.lastDate);
        params.set("lastRowId", String(cursor.lastRowId));
        params.set("lastPriority", String(cursor.lastPriority ?? 0));
      }
      const res = await fetch(
        `${BASE_URL}/analytics/getPaginatedTransactions?${params.toString()}`
      );
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return res.json();
    },
    [buildBaseParams]
  );

  // Initial load and refetch when filters change (only when account code is selected)
  useEffect(() => {
    if (!accountCode || !accountCode.trim()) {
      setData([]);
      setNextCursor(null);
      setPrevCursor(null);
      setHasNext(false);
      setHasPrev(false);
      setLoading(false);
      setError("");
      return;
    }
    const loadId = ++initialLoadIdRef.current;
    setLoading(true);
    setError("");
    const loadFirstPage = async () => {
      try {
        const json = await fetchPage(null, "next");
        if (loadId !== initialLoadIdRef.current) return;
        setData(json.data || []);
        setNextCursor(json.nextCursor ?? null);
        setPrevCursor(json.prevCursor ?? null);
        setHasNext(Boolean(json.hasNext));
        setHasPrev(Boolean(json.hasPrev));
      } catch (err) {
        if (loadId !== initialLoadIdRef.current) return;
        setError(err.message || "Failed to load transactions");
      } finally {
        if (loadId === initialLoadIdRef.current) setLoading(false);
      }
    };
    loadFirstPage();
  }, [accountCode, asOnDate, selectedSecurity, fetchPage]);

  const goNext = async () => {
    if (!nextCursor || !hasNext || loading || loadingMore) return;
    setLoadingMore(true);
    setError("");
    try {
      const json = await fetchPage(nextCursor, "next");
      setData(json.data || []);
      setNextCursor(json.nextCursor ?? null);
      setPrevCursor(json.prevCursor ?? null);
      setHasNext(Boolean(json.hasNext));
      setHasPrev(Boolean(json.hasPrev));
    } catch (err) {
      setError(err.message || "Failed to load next page");
    } finally {
      setLoadingMore(false);
    }
  };

  const goPrev = async () => {
    if (!prevCursor || !hasPrev || loading || loadingMore) return;
    setLoadingMore(true);
    setError("");
    try {
      const json = await fetchPage(prevCursor, "prev");
      setData(json.data || []);
      setNextCursor(json.nextCursor ?? null);
      setPrevCursor(json.prevCursor ?? null);
      setHasNext(Boolean(json.hasNext));
      setHasPrev(Boolean(json.hasPrev));
    } catch (err) {
      setError(err.message || "Failed to load previous page");
    } finally {
      setLoadingMore(false);
    }
  };

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

      {!accountCode || !accountCode.trim() ? (
        <div className="empty-cell" style={{ padding: 24, textAlign: "center", color: "#6b7280" }}>
          Please select an account code to view transactions.
        </div>
      ) : (
        <>
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
                <th className="num">STT</th>
                <th>Cash Balance</th>
              </tr>
            </thead>
            <tbody>
              {data.length === 0 ? (
                <tr>
                  <td colSpan={9} className="empty-cell">
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
                    <td className="num">{formatNumber(t.stt)}</td>
                    <td className="num">{formatNumber(t.cashBalance)}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      )}

      {!loading && !error && data.length > 0 && (
        <div className="transaction-pagination">
          <button
            type="button"
            className="pagination-btn pagination-prev"
            onClick={goPrev}
            disabled={!hasPrev || loadingMore}
          >
            ← Previous
          </button>
          <span className="pagination-info">
            {loadingMore ? "Loading…" : `Showing ${data.length} transaction${data.length === 1 ? "" : "s"}`}
          </span>
          <button
            type="button"
            className="pagination-btn pagination-next"
            onClick={goNext}
            disabled={!hasNext || loadingMore}
          >
            Next →
          </button>
        </div>
      )}
        </>
      )}
    </Card>
  );
};

export default TransactionTab;
