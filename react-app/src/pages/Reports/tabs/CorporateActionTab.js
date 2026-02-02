import React, { useState, useEffect } from "react";
import { BASE_URL } from "../../../constant.js";

function CorporateActionTab() {
  const [fromDate, setFromDate] = useState("");
  const [toDate, setToDate] = useState("");
  const [loading, setLoading] = useState(false);
  const [exportHistory, setExportHistory] = useState([]);

  /* ---------- LOAD LAST 10 EXPORTS (ON MOUNT) ---------- */
  useEffect(() => {
    const fetchHistory = async () => {
      try {
        const res = await fetch(
          `${BASE_URL}/export/corporate-action/history?limit=10`,
          { credentials: "include" }
        );
        const data = await res.json();
        if (Array.isArray(data)) setExportHistory(data);
      } catch (err) {
        console.error("Failed to load export history", err);
      }
    };
    fetchHistory();
  }, []);

  const handleExport = async () => {
    if (!fromDate || !toDate) {
      alert("Please select From Date and To Date");
      return;
    }
    if (fromDate > toDate) {
      alert("From Date must be before or equal to To Date");
      return;
    }

    try {
      setLoading(true);

      const params = new URLSearchParams();
      params.append("fromDate", fromDate);
      params.append("toDate", toDate);

      const response = await fetch(
        `${BASE_URL}/export/corporate-action/export?${params.toString()}`,
        { method: "GET", credentials: "include" }
      );

      if (!response.ok) {
        const data = await response.json().catch(() => ({}));
        throw new Error(data.message || data.error || "Export failed");
      }

      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `corporate-action-export-${fromDate}-${toDate}.csv`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      window.URL.revokeObjectURL(url);

      setExportHistory((prev) => [
        { fromDate, toDate, requestedAt: new Date().toISOString() },
        ...prev.slice(0, 9),
      ]);
      setLoading(false);
    } catch (error) {
      alert(error.message);
      setLoading(false);
    }
  };

  const handleHistoryDownload = async (row) => {
    try {
      const params = new URLSearchParams();
      params.append("fromDate", row.fromDate);
      params.append("toDate", row.toDate);
      const response = await fetch(
        `${BASE_URL}/export/corporate-action/export?${params.toString()}`,
        { method: "GET", credentials: "include" }
      );
      if (!response.ok) throw new Error("Download failed");
      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `corporate-action-export-${row.fromDate}-${row.toDate}.csv`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      window.URL.revokeObjectURL(url);
    } catch (err) {
      alert(err.message);
    }
  };

  return (
    <>
      <h3 className="section-heading">Corporate Action Export</h3>

      <div className="form-grid">
        <div className="form-field">
          <label>From Date</label>
          <input
            type="date"
            value={fromDate}
            onChange={(e) => setFromDate(e.target.value)}
          />
        </div>
        <div className="form-field">
          <label>To Date</label>
          <input
            type="date"
            value={toDate}
            onChange={(e) => setToDate(e.target.value)}
          />
        </div>
      </div>

      <div className="action-footer">
        <button
          className="export-btn"
          onClick={handleExport}
          disabled={loading}
        >
          {loading ? "Generating..." : "Export CSV"}
        </button>
      </div>

      {exportHistory.length > 0 && (
        <div className="export-jobs-container">
          <h4>Previous Export History</h4>
          <table className="export-table">
            <thead>
              <tr>
                <th>From Date</th>
                <th>To Date</th>
                <th>Exported At</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              {exportHistory.map((row, idx) => (
                <tr key={`${row.fromDate}-${row.toDate}-${row.requestedAt}-${idx}`}>
                  <td>{row.fromDate}</td>
                  <td>{row.toDate}</td>
                  <td>
                    {row.requestedAt
                      ? new Date(row.requestedAt).toLocaleString()
                      : "-"}
                  </td>
                  <td>
                    <button
                      className="export-btn"
                      onClick={() => handleHistoryDownload(row)}
                    >
                      Download
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  );
}

export default CorporateActionTab;