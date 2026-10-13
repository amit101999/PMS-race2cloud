import React, { useState, useEffect } from "react";
import "./TransactionPage.css";
import { BASE_URL } from "../../constant.js";
import { Pagination } from "../../components/common/CommonComponents";

/** Rows of Transaction History shown per page. */
const PAGE_SIZE = 10;

function TransactionPage({ stock, accountCode, asOnDate, onClose }) {
  const [transactions, setTransactions] = useState([]);
  const [totalBuy, setTotalBuy] = useState(0);
  const [totalSell, setTotalSell] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [currentPage, setCurrentPage] = useState(1);

  useEffect(() => {
    if (stock && accountCode && stock.isin) {
      fetchAllData();
    } else if (stock && accountCode && !stock.isin) {
      setError("ISIN not available for this stock");
      setLoading(false);
    }
  }, [stock, accountCode, asOnDate]);

  const fetchAllData = async () => {
    try {
      setLoading(true);
      setError(null);
      // A different stock (or as-on date) is a different list — start at page 1.
      setCurrentPage(1);

      const baseUrl = `${BASE_URL}/transaction`;
      const params = new URLSearchParams({
        accountCode,
        isin: stock.isin,
      });

      if (asOnDate) {
        params.set("asOnDate", asOnDate);
      }

      // One request: the history endpoint returns the buy/sell totals alongside
      // the rows, all three derived from a single Holdings scan. This used to be
      // three calls that each re-ran the same query.
      const historyRes = await fetch(
        `${baseUrl}/getStockTransactionHistory?${params.toString()}`
      );
      const payload = await historyRes.json();

      // Older deploys returned a bare array from this endpoint.
      const rows = Array.isArray(payload) ? payload : payload?.rows;
      setTransactions(Array.isArray(rows) ? rows : []);

      setTotalBuy(Number(payload?.totalBuyQty) || 0);
      setTotalSell(Number(payload?.totalSellQty) || 0);
    } catch (err) {
      setError(err.message);
      setTransactions([]);
      setTotalBuy(0);
      setTotalSell(0);
    } finally {
      setLoading(false);
    }
  };

  if (!stock) return null;

  // Badge tone (styles/design-system.css) for each getRowClass() category.
  const TYPE_BADGE_TONE = { buy: "buy", sell: "sell", bonus: "info" };

  const getRowClass = (type) => {
    if (!type) return "";
    const typeUpper = String(type).toUpperCase();
    if (typeUpper.includes("BY") || typeUpper === "SQB" || typeUpper === "OPI")  // previous: OPI styled as buy
      return "buy";
    if (
      typeUpper.includes("SL") ||
      typeUpper === "SQS" ||
      typeUpper === "OPO" ||
      typeUpper === "NF-"
    )
      return "sell";
    if (typeUpper === "BONUS") return "bonus";
    if (typeUpper === "DEMERGER") return "bonus";
    if (typeUpper === "MERGER") return "bonus";
    return "";
  };

  const formatCurrency = (value) => {
    if (value === null || value === undefined || value === "") return "-";
    const num = Number(value);
    if (isNaN(num)) return "-";
    return `₹${num.toLocaleString("en-IN", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    })}`;
  };

  const formatNumber = (value) => {
    if (value === null || value === undefined || value === "") return "-";
    const num = Number(value);
    if (isNaN(num)) return "-";
    return num.toLocaleString("en-IN");
  };

  // Shown the same way as the holdings list (HoldingCards), so both screens agree.
  // Quantities are not floored for corporate actions: a split keeps its fractions,
  // and bonus / merger / demerger quantities are already whole when calculated.
  const formatQuantity = (value) => {
    const num = Number(value);
    if (value === null || value === undefined || value === "" || isNaN(num)) return "-";
    const snapped = Math.abs(num) < 1e-6 ? 0 : num;
    return snapped.toLocaleString("en-IN", { maximumFractionDigits: 3 });
  };

  // Rounds to 2 decimals, never cuts off: cutting off showed 879.9999 as 879.99.
  const formatAmount = (value) => {
    const num = Number(value);
    if (value === null || value === undefined || value === "" || isNaN(num)) return "-";
    return num.toLocaleString("en-IN", { maximumFractionDigits: 2 });
  };

  // Everything below is deliberately derived from the FULL list, not the visible
  // page: the summary cards, the P/L total and the optional corporate-action
  // columns must stay identical whichever page you are on.
  const lastTransaction = transactions[transactions.length - 1];
  const currentHolding = lastTransaction?.holdings ?? 0;
  const totalProfitLoss = transactions.reduce(
    (sum, tx) => sum + (Number(tx.profitLoss) || 0),
    0
  );
  const avgPrice = lastTransaction?.averageCostOfHoldings || 0;

  // Only surface the Merger Date column for a security that actually merged
  // (i.e. has TYPE=MERGER rows). Hidden for everything else.
  const showMergerDate = transactions.some(
    (t) => String(t.tranType || "").toUpperCase() === "MERGER",
  );

  // Same idea for demergers: show the Demerger Date column (record date, from
  // Holdings.CA_DATE) only for a security that actually demerged.
  const showDemergerDate = transactions.some(
    (t) => String(t.tranType || "").toUpperCase() === "DEMERGER",
  );

  // Base table has 11 columns; each optional corporate-action date adds one.
  const tableColSpan =
    11 + (showMergerDate ? 1 : 0) + (showDemergerDate ? 1 : 0);

  // Paging is applied to the display only. The rows arrive in one call because
  // Holding / WAP / Holding Value / P L are running totals down the ordered
  // list — slicing them server-side would break every figure after row 10.
  const totalRows = transactions.length;
  const totalPages = Math.max(1, Math.ceil(totalRows / PAGE_SIZE));
  // Clamp instead of trusting state: a shorter list can arrive while a later
  // page is selected, which would otherwise render an empty table.
  const safePage = Math.min(currentPage, totalPages);
  const startIndex = (safePage - 1) * PAGE_SIZE;
  const pageTransactions = transactions.slice(startIndex, startIndex + PAGE_SIZE);

  return (
    <div className="hd-overlay">
      <div className="hd-container">
        <div className="hd-header">
          <h2>{stock.stockName}</h2>
          <button className="hd-close" onClick={onClose}>
            ✕
          </button>
        </div>

        <div className="hd-summary">
          <div className="hd-card purple">
            <p>CURRENT HOLDING</p>
            <h3>{formatQuantity(currentHolding)}</h3>
          </div>

          <div className="hd-card green">
            <p>TOTAL BUY</p>
            <h3>{formatNumber(totalBuy)}</h3>
          </div>

          <div className="hd-card red">
            <p>TOTAL SELL</p>
            <h3>{formatNumber(totalSell)}</h3>
          </div>

          <div className="hd-card profit">
            <p>PROFIT / LOSS</p>
            <h3>{formatCurrency(totalProfitLoss)}</h3>
          </div>
          <div className="hd-card blue">
            <p>WEIGHTED AVG BUY PRICE</p>
            <h3>{formatAmount(avgPrice)}</h3>
          </div>
        </div>

        <h3 className="hd-table-title">Transaction History</h3>

        {loading ? (
          <p className="cm-empty">Loading...</p>
        ) : error ? (
          <div className="cm-verdict bad">Error: {error}</div>
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
                  <th>ISIN</th>
                  <th className="num">Quantity</th>
                  <th className="num">Price</th>
                  <th className="num">Total Amount</th>
                  <th className="num">Holding</th>
                  <th className="num">WAP</th>
                  <th className="num">Holding Value</th>
                  <th className="num">P/L</th>
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
                    <tr key={startIndex + idx}>
                      <td className="date">{tx.originalTrandate || tx.trandate || "-"}</td>
                      <td className="date">{tx.setdate || "-"}</td>
                      {showMergerDate && <td className="date">{tx.caDate || "-"}</td>}
                      {showDemergerDate && <td className="date">{tx.caDate || "-"}</td>}
                      <td className="status">
                        {tx.tranType ? (
                          <span className={`badge badge--${TYPE_BADGE_TONE[getRowClass(tx.tranType)] || "neutral"}`}>
                            {tx.tranType}
                          </span>
                        ) : (
                          "-"
                        )}
                      </td>
                      <td>{tx.isin || "-"}</td>
                      <td className="num">
                        {formatQuantity(tx.qty)}
                        <span>
                          {/^BY-|SQB|OPI/i.test(String(tx.tranType || "")) &&
                          tx.isActive === false
                            ? "Inactive"
                            : ""}
                        </span>
                      </td>
                      <td className="num">{formatAmount(tx.price)}</td>
                      <td className="num">{formatCurrency(tx.netAmount)}</td>
                      <td className="num">{formatQuantity(tx.holdings)}</td>
                      <td className="num">
                        {formatAmount(tx.averageCostOfHoldings)}
                      </td>
                      <td className="num">{formatAmount(tx.costOfHoldings)}</td>
                      <td
                        className={
                          tx.profitLoss !== null && tx.profitLoss !== undefined
                            ? Number(tx.profitLoss) >= 0
                              ? "num positive"
                              : "num negative"
                            : "num"
                        }
                      >
                        {formatCurrency(tx.profitLoss)}
                      </td>
                      {/* <td>{formatCurrency(tx.cashBalance)}</td> */}
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>

          {totalRows > 0 && (
            <div className="hd-pagination">
              <span className="hd-pagination-info">
                Showing {startIndex + 1}–
                {Math.min(startIndex + PAGE_SIZE, totalRows)} of {totalRows}
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
