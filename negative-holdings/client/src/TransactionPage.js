// Copy of react-app/src/pages/TransactionPage/TransactionPage.js (the Analytics
// drill-down), with Short By and Real Holding added.
import { useState, useEffect } from "react";
import Pagination from "./Pagination";
import "./TransactionPage.css";

/** Rows of Transaction History shown per page. */
const PAGE_SIZE = 10;

// Badge tone (design-system.css) for each getRowClass() category.
const TYPE_BADGE_TONE = { buy: "buy", sell: "sell", bonus: "info" };

function getRowClass(type) {
  if (!type) return "";
  const typeUpper = String(type).toUpperCase();
  if (typeUpper.includes("BY") || typeUpper === "SQB" || typeUpper === "OPI") return "buy";
  if (
    typeUpper.includes("SL") ||
    typeUpper === "SQS" ||
    typeUpper === "OPO" ||
    typeUpper === "NF-"
  ) {
    return "sell";
  }
  if (typeUpper === "BONUS" || typeUpper === "DEMERGER" || typeUpper === "MERGER") return "bonus";
  return "";
}

function formatCurrency(value) {
  if (value === null || value === undefined || value === "") return "-";
  const num = Number(value);
  if (isNaN(num)) return "-";
  return `₹${num.toLocaleString("en-IN", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

function formatNumber(value) {
  if (value === null || value === undefined || value === "") return "-";
  const num = Number(value);
  if (isNaN(num)) return "-";
  return num.toLocaleString("en-IN");
}

function formatQuantity(value) {
  const num = Number(value);
  if (value === null || value === undefined || value === "" || isNaN(num)) return "-";
  const snapped = Math.abs(num) < 1e-6 ? 0 : num;
  return snapped.toLocaleString("en-IN", { maximumFractionDigits: 6 });
}

function formatAmount(value) {
  const num = Number(value);
  if (value === null || value === undefined || value === "" || isNaN(num)) return "-";
  return num.toLocaleString("en-IN", { maximumFractionDigits: 2 });
}

function TransactionPage({ stock, accountCode, asOnDate, onClose }) {
  const [transactions, setTransactions] = useState([]);
  const [totalBuy, setTotalBuy] = useState(0);
  const [totalSell, setTotalSell] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [currentPage, setCurrentPage] = useState(1);

  useEffect(() => {
    let cancelled = false;

    const fetchHistory = async () => {
      setLoading(true);
      setError(null);
      setCurrentPage(1);

      const params = new URLSearchParams({ accountCode, isin: stock.isin });
      if (asOnDate) params.set("asOnDate", asOnDate);

      try {
        const res = await fetch(`${process.env.REACT_APP_API_URL}/share-history?${params}`);
        if (!res.ok) throw new Error("Could not load the history. Please try again.");
        const payload = await res.json();
        if (cancelled) return;
        setTransactions(payload.rows || []);
        setTotalBuy(Number(payload.totalBuyQty) || 0);
        setTotalSell(Number(payload.totalSellQty) || 0);
      } catch (err) {
        if (cancelled) return;
        setError(err.message);
        setTransactions([]);
        setTotalBuy(0);
        setTotalSell(0);
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    fetchHistory();
    return () => {
      cancelled = true;
    };
  }, [stock.isin, accountCode, asOnDate]);

  // Worked out from the full list, not the visible page, so the cards don't
  // change as you page through.
  const lastTransaction = transactions[transactions.length - 1];
  const realHolding = lastTransaction?.realHolding;

  const showMergerDate = transactions.some(
    (t) => String(t.tranType || "").toUpperCase() === "MERGER",
  );
  const showDemergerDate = transactions.some(
    (t) => String(t.tranType || "").toUpperCase() === "DEMERGER",
  );
  const tableColSpan = 8 + (showMergerDate ? 1 : 0) + (showDemergerDate ? 1 : 0);

  // Paged on the screen only: the exact holding is a running total down the
  // whole ordered list.
  const totalRows = transactions.length;
  const totalPages = Math.max(1, Math.ceil(totalRows / PAGE_SIZE));
  const safePage = Math.min(currentPage, totalPages);
  const startIndex = (safePage - 1) * PAGE_SIZE;
  const pageTransactions = transactions.slice(startIndex, startIndex + PAGE_SIZE);

  return (
    <div className="hd-overlay">
      <div className="hd-container">
        <div className="hd-header">
          <h2>{stock.stockName}</h2>
          <button className="hd-close" onClick={onClose} aria-label="Close">
            ✕
          </button>
        </div>

        <div className="hd-summary">
          <div className="hd-card red">
            <p>EXACT HOLDING</p>
            <h3>{realHolding === null ? "Check by hand" : formatQuantity(realHolding)}</h3>
          </div>
          <div className="hd-card green">
            <p>TOTAL BUY</p>
            <h3>{formatNumber(totalBuy)}</h3>
          </div>
          <div className="hd-card red">
            <p>TOTAL SELL</p>
            <h3>{formatNumber(totalSell)}</h3>
          </div>
        </div>

        <h3 className="hd-table-title">Transaction History</h3>

        {loading ? (
          <p className="nh-empty">Loading...</p>
        ) : error ? (
          <div className="nh-note nh-note--bad">{error}</div>
        ) : (
          <>
            <div className="data-table-wrap hd-table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th className="date">Transaction Date</th>
                    <th className="date">Settlement Date</th>
                    {showMergerDate && <th className="date">Merger Date</th>}
                    {showDemergerDate && <th className="date">Demerger Date</th>}
                    <th className="status">Type</th>
                    <th className="num">Quantity</th>
                    <th className="num">Price</th>
                    <th className="num">Total Amount</th>
                    <th className="num">Exact Holding</th>
                    <th className="num">Short By</th>
                  </tr>
                </thead>
                <tbody>
                  {transactions.length === 0 ? (
                    <tr>
                      <td colSpan={tableColSpan} className="empty-cell">
                        No transactions found
                      </td>
                    </tr>
                  ) : (
                    pageTransactions.map((tx, idx) => (
                      <tr key={tx.rowId ?? startIndex + idx} className={tx.shortBy > 0 ? "is-warn" : ""}>
                        <td className="date">{tx.originalTrandate || tx.trandate || "-"}</td>
                        <td className="date">{tx.setdate || "-"}</td>
                        {showMergerDate && <td className="date">{tx.caDate || "-"}</td>}
                        {showDemergerDate && <td className="date">{tx.caDate || "-"}</td>}
                        <td className="status">
                          {tx.tranType ? (
                            <span
                              className={`badge badge--${TYPE_BADGE_TONE[getRowClass(tx.tranType)] || "neutral"}`}
                            >
                              {tx.tranType}
                            </span>
                          ) : (
                            "-"
                          )}
                        </td>
                        <td className="num">{formatQuantity(tx.qty)}</td>
                        <td className="num">{formatAmount(tx.price)}</td>
                        <td className="num">{formatCurrency(tx.netAmount)}</td>
                        <td className={tx.realHolding < 0 ? "num nh-negative" : "num"}>
                          {formatQuantity(tx.realHolding)}
                        </td>
                        <td className={tx.shortBy > 0 ? "num nh-negative" : "num"}>
                          {tx.shortBy > 0 ? formatQuantity(tx.shortBy) : ""}
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>

            {totalRows > 0 && (
              <div className="hd-pagination">
                <span className="hd-pagination-info">
                  Showing {startIndex + 1}–{Math.min(startIndex + PAGE_SIZE, totalRows)} of {totalRows}
                </span>
                <Pagination
                  currentPage={safePage}
                  pageSize={PAGE_SIZE}
                  totalRows={totalRows}
                  onPageChange={setCurrentPage}
                />
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}

export default TransactionPage;
