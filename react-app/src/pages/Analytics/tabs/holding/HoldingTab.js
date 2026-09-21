import React from "react";
import HoldingsGrid from "../../../Holding/HoldingCards";
import { Pagination } from "../../../../components/common/CommonComponents";

const HoldingTab = ({
  holdings = [],
  viewMode,
  setViewMode,
  accountCode,
  isIsinMode = false,
  isinReportRows = [],
  loadingIsinReport = false,
  selectedIsin = "",
  asOnDate,
  selectedStock,
  setSelectedStock,
  currentPage,
  pageSize,
  onPageChange,
  onPageSizeChange,
}) => {
  // Hide content until one of account code / ISIN is selected.
  if (!accountCode && !isIsinMode) return null;

  const formatNumber = (value, maxFractionDigits = 2) => {
    const n = Number(value);
    if (!Number.isFinite(n)) return "—";
    return n.toLocaleString("en-IN", { maximumFractionDigits: maxFractionDigits });
  };

  if (isIsinMode) {
    const rows = Array.isArray(isinReportRows) ? isinReportRows : [];
    const start = (currentPage - 1) * pageSize;
    const paginatedRows = rows.slice(start, start + pageSize);

    const openIsinRowTransactions = (row) => {
      if (!setSelectedStock) return;
      const virtualCode = String(row?.virtualCode ?? "").trim();
      if (!virtualCode) return;

      setSelectedStock({
        isin: row.isin || selectedIsin,
        stockName: row.stockName || selectedIsin,
        securityCode: row.securityCode || "",
        currentHolding: row.currentHolding,
        avgPrice: row.avgPrice,
        holdingValue: row.holdingValue,
        lastPrice: row.lastPrice,
        marketValue: row.marketValue,
        accountCode: virtualCode,
        asOnDate,
      });
    };

    return (
      <>
        <div className="holdings-wrapper">
          <h2 className="holdings-title">
            ISIN Report ({selectedIsin || "—"}) - Accounts ({rows.length})
          </h2>
          <div className="data-table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Virtual Code</th>
                  <th>Actual Code</th>
                  <th className="num">Quantity</th>
                  <th className="num">WAP</th>
                  <th className="num">Holding Value</th>
                  <th className="num">Last Price</th>
                  <th className="num">Market Value</th>
                  <th className="action"></th>
                </tr>
              </thead>
              <tbody>
                {loadingIsinReport && (
                  <tr>
                    <td colSpan="8" className="empty-cell">
                      Loading ISIN report...
                    </td>
                  </tr>
                )}
                {!loadingIsinReport && paginatedRows.length === 0 && (
                  <tr>
                    <td colSpan="8" className="empty-cell">
                      No accounts hold this ISIN
                    </td>
                  </tr>
                )}
                {!loadingIsinReport &&
                  paginatedRows.map((row, idx) => (
                    <tr
                      key={`${row.virtualCode || "virtual"}-${idx}`}
                      className="is-clickable"
                      onClick={() => openIsinRowTransactions(row)}
                    >
                      <td>{row.virtualCode || "—"}</td>
                      <td>{row.actualCode || "—"}</td>
                      <td className="num">{formatNumber(row.currentHolding, 3)}</td>
                      <td className="num">{formatNumber(row.avgPrice)}</td>
                      <td className="num">{formatNumber(row.holdingValue)}</td>
                      <td className="num">{formatNumber(row.lastPrice)}</td>
                      <td className="num">{formatNumber(row.marketValue)}</td>
                      <td className="action">
                        <button
                          type="button"
                          className="btn-action"
                          onClick={(e) => {
                            e.stopPropagation();
                            openIsinRowTransactions(row);
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
        </div>

        <div className="analytics-pagination">
          <Pagination
            currentPage={currentPage}
            pageSize={pageSize}
            totalRows={rows.length}
            onPageChange={onPageChange}
            onPageSizeChange={onPageSizeChange}
          />
        </div>
      </>
    );
  }

  const displayedHoldings = Array.isArray(holdings)
    ? holdings
    : [];

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
      : displayedHoldings;

  const start = (currentPage - 1) * pageSize;
  const paginatedRows = rowsForTable.slice(start, start + pageSize);

  return (
    <>
      {/* Holding Summary Table stays outside; here we keep tabs + grid */}
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

      <HoldingsGrid
        holdings={paginatedRows}
        onSelectStock={(stock) =>
          setSelectedStock({
            ...stock,
            accountCode: accountCode,
            asOnDate,
          })
        }
      />

      <div className="analytics-pagination">
        <Pagination
          currentPage={currentPage}
          pageSize={pageSize}
          totalRows={rowsForTable.length}
          onPageChange={onPageChange}
          onPageSizeChange={onPageSizeChange}
        />
      </div>
    </>
  );
};

export default HoldingTab;