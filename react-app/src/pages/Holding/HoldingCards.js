import React from "react";
import "./HoldingCard.css";

function HoldingsGrid({ holdings = [], onSelectStock }) {
  if (!holdings.length) {
    return (
      <div className="holdings-wrapper">
        <div className="data-table-wrap">
          <table className="data-table">
            <tbody>
              <tr>
                <td colSpan="9" className="empty-cell">
                  No holdings available
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    );
  }

  const formatNumber = (value) => {
    const n = Number(value);
    if (!Number.isFinite(n)) return "—";
    return n.toLocaleString("en-IN", {
      minimumFractionDigits: 0,
      maximumFractionDigits: 2,
    });
  };

  // Current holding is the net outcome of normal buys/sells and can be
  // fractional (e.g. 5.533). Show real decimals; snap sub-epsilon dust to 0.
  const formatQuantity = (value) => {
    const n = Number(value);
    if (!Number.isFinite(n)) return "—";
    const snapped = Math.abs(n) < 1e-6 ? 0 : n;
    return snapped.toLocaleString("en-IN", { maximumFractionDigits: 3 });
  };

  return (
    <div className="holdings-wrapper">
      <h2 className="holdings-title">Stock Holdings ({holdings.length})</h2>

      <div className="data-table-wrap">
        <table className="data-table">
          <thead>
            <tr>
              <th className="wrap">Security Name</th>
              <th>Security Code</th>
              <th>Security ISIN</th>
              <th className="num">Current Holding</th>
              <th className="num">Average Holding Value</th>
              <th className="num">Holding Value</th>
              <th className="num">Last Price</th>
              <th className="num">Market value</th>
              <th className="action"></th>
            </tr>
          </thead>

          <tbody>
            {holdings.map((item, index) => {
              const isSold = Number(item.currentHolding) === 0;

              return (
                <tr
                  key={`${item.stockName}-${index}`}
                  className={isSold ? "is-clickable sold-row" : "is-clickable"}
                  onClick={() => onSelectStock(item)}
                >
                  <td className="wrap security-name">{item.stockName}</td>
                  <td className="security-code">{item.securityCode || "—"}</td>
                  <td className="security-code">{item.isin || "—"}</td>
                  <td className="num holding-value">
                    {isSold && <span className="badge badge--sell sold-badge">FULLY SOLD</span>}
                    {formatQuantity(item.currentHolding)}
                  </td>
                  <td className="num">
                    {formatNumber(item.avgPrice)}
                  </td>
                  <td className="num">
                    {formatNumber(item.holdingValue)}
                  </td>
                  <td className="num">
                    {formatNumber(item.lastPrice)}
                  </td>
                  <td className="num">
                    {formatNumber(item.marketValue)}
                  </td>
                  <td className="action">
                    <button
                      type="button"
                      className="btn-action"
                      onClick={(e) => {
                        e.stopPropagation();
                        onSelectStock(item);
                      }}
                    >
                      View
                    </button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default HoldingsGrid;
