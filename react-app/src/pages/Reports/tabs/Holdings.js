import React, { useEffect, useRef, useState, useMemo } from "react";
import { useAccountCodes } from "../../../hooks/GetAllCodes.js";
import { BASE_URL } from "../../../constant.js";

function HoldingsTab() {
  const [exportType, setExportType] = useState("all");
  const [asOnDate, setAsOnDate] = useState("");
  const [accountCode, setAccountCode] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [showDropdown, setShowDropdown] = useState(false);
  const [loading, setLoading] = useState(false);
  const [downloadUrl, setDownloadUrl] = useState("");

  const dropdownRef = useRef(null);

  const { clientOptions } = useAccountCodes();

  // 🔹 Filtered options
  const filteredOptions = useMemo(() => {
    return clientOptions.filter((opt) =>
      opt.label.toLowerCase().includes(searchQuery.toLowerCase())
    );
  }, [clientOptions, searchQuery]);

  // 🔹 Close dropdown on outside click
  useEffect(() => {
    const handleClickOutside = (e) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target)) {
        setShowDropdown(false);
      }
    };

    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const handleAccountSelect = (option) => {
    setSearchQuery(option.label);
    setAccountCode(option.value);
    setShowDropdown(false);
  };

  const clearAccountSelection = () => {
    setSearchQuery("");
    setAccountCode("");
    setShowDropdown(false);
  };
  const pollExportStatus = async (fileName = "all-clients-export.csv") => {
    const interval = setInterval(async () => {
      try {
        const res = await fetch(
          `${BASE_URL}/export/check-status?fileName=${fileName}`,
          { credentials: "include" }
        );

        const data = await res.json();

        if (data.status === "READY") {
          clearInterval(interval);

          // 🔽 Now fetch download URL
          const downloadRes = await fetch(
            `${BASE_URL}/export/download?fileName=${fileName}`,
            { credentials: "include" }
          );

          const downloadData = await downloadRes.json();
          setDownloadUrl(downloadData.downloadUrl);
          setLoading(false);
        }
      } catch (err) {
        console.error("Polling failed:", err);
        clearInterval(interval);
        setLoading(false);
      }
    }, 8000); // poll every 8 sec
  };

  const handleExport = async () => {
    try {
      if (exportType === "single" && !accountCode) {
        alert("Please select an account code");
        return;
      }

      setLoading(true);
      setDownloadUrl("");

      // 🔹 SINGLE CLIENT (UNCHANGED)
      if (exportType === "single") {
        const params = new URLSearchParams();

        params.append("accountCode", accountCode);
        if (asOnDate) params.append("asOnDate", asOnDate);

        const response = await fetch(
          `${BASE_URL}/export/export-single?${params.toString()}`,
          {
            method: "GET",
            credentials: "include",
          }
        );

        const data = await response.json();
        if (!response.ok) throw new Error(data.message);

        setDownloadUrl(data.downloadUrl.signature);
        setLoading(false);
        return;
      }

      // 🔹 EXPORT ALL CLIENTS (FIXED: GET + params)
      const params = new URLSearchParams();
      if (asOnDate) params.append("asOnDate", asOnDate);

      const response = await fetch(
        `${BASE_URL}/export/export-all?${params.toString()}`,
        {
          method: "GET",
          credentials: "include",
        }
      );

      const data = await response.json();

      if (data.status === "READY") {
        // ✅ File already exists → download immediately
        setDownloadUrl(data.downloadUrl);
        setLoading(false);
      } else if (data.status === "QUEUED") {
        // ⏳ Start polling for fixed filename
        pollExportStatus("all-clients-export.csv");
      } else {
        throw new Error("Unexpected export status");
      }
    } catch (error) {
      alert(error.message);
      setLoading(false);
    }
  };

  return (
    <>
      <h3 className="section-heading">Holdings Export</h3>

      {/* Export Type */}
      <div className="export-type">
        <label>
          <input
            type="radio"
            checked={exportType === "all"}
            onChange={() => setExportType("all")}
          />
          Export All Clients
        </label>

        <label>
          <input
            type="radio"
            checked={exportType === "single"}
            onChange={() => setExportType("single")}
          />
          Export Single Client
        </label>
      </div>

      {/* Form */}
      <div className="form-grid">
        {exportType === "single" && (
          <div className="account-code-search" ref={dropdownRef}>
            <label className="search-label">Account Code</label>

            <div className="search-input-wrapper">
              <span className="search-icon">🔍</span>

              <input
                type="text"
                className="search-input"
                placeholder="Search Account Code..."
                value={searchQuery}
                onChange={(e) => {
                  setSearchQuery(e.target.value);
                  setShowDropdown(true);
                }}
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
        )}

        <div className="form-field">
          <label>As On Date</label>
          <input
            type="date"
            value={asOnDate}
            onChange={(e) => setAsOnDate(e.target.value)}
          />
        </div>
      </div>

      {/* Footer */}
      <div className="action-footer">
        {!downloadUrl ? (
          <button
            className="export-btn"
            onClick={handleExport}
            disabled={loading}
          >
            {loading ? "Generating..." : "Export"}
          </button>
        ) : (
          <a
            href={downloadUrl}
            className="export-btn"
            download
            target="_blank"
            rel="noreferrer"
          >
            Download CSV
          </a>
        )}
      </div>
    </>
  );
}

export default HoldingsTab;
