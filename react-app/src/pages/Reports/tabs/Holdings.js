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

  // ✅ Export All jobs (persisted from backend)
  const [exportJobs, setExportJobs] = useState([]);

  const dropdownRef = useRef(null);
  const { clientOptions } = useAccountCodes();

  /* ---------------- FILTERED OPTIONS (UNCHANGED) ---------------- */
  const filteredOptions = useMemo(() => {
    return clientOptions.filter((opt) =>
      opt.label.toLowerCase().includes(searchQuery.toLowerCase())
    );
  }, [clientOptions, searchQuery]);

  /* ---------------- CLOSE DROPDOWN (UNCHANGED) ---------------- */
  useEffect(() => {
    const handleClickOutside = (e) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target)) {
        setShowDropdown(false);
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  /* ---------------- ACCOUNT SELECT (UNCHANGED) ---------------- */
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

  /* ===================== EXPORT HANDLER ===================== */
  const handleExport = async () => {
    try {
      /* ---------- SINGLE CLIENT EXPORT (UNCHANGED) ---------- */
      if (exportType === "single") {
        if (!accountCode) {
          alert("Please select an account code");
          return;
        }

        setLoading(true);
        setDownloadUrl("");

        const params = new URLSearchParams();
        params.append("accountCode", accountCode);
        if (asOnDate) params.append("asOnDate", asOnDate);

        const response = await fetch(
          `${BASE_URL}/export/export-single?${params.toString()}`,
          { method: "GET", credentials: "include" }
        );

        const data = await response.json();
        if (!response.ok) throw new Error(data.message);

        setDownloadUrl(data.downloadUrl.signature);
        setLoading(false);
        return;
      }

      /* ---------- EXPORT ALL CLIENTS (JOB BASED) ---------- */
      if (!asOnDate) {
        alert("Please select As On Date");
        return;
      }

      setLoading(true);

      const response = await fetch(
        `${BASE_URL}/export/export-all?asOnDate=${asOnDate}`,
        { method: "GET", credentials: "include" }
      );

      const data = await response.json();

      // Merge job safely
      setExportJobs((prev) => {
        const exists = prev.find((j) => j.jobName === data.jobName);
        if (exists) {
          return prev.map((j) =>
            j.jobName === data.jobName ? { ...j, status: data.status } : j
          );
        }

        return [
          {
            jobName: data.jobName,
            asOnDate,
            status: data.status,
          },
          ...prev,
        ];
      });

      setLoading(false);
    } catch (error) {
      alert(error.message);
      setLoading(false);
    }
  };

  /* ===================== LOAD LAST 10 EXPORTS (ON MOUNT) ===================== */
  useEffect(() => {
    if (exportType !== "all") return;

    const fetchHistory = async () => {
      try {
        const res = await fetch(
          `${BASE_URL}/export/export-all/history?limit=10`,
          { credentials: "include" }
        );
        const data = await res.json();

        if (Array.isArray(data)) {
          setExportJobs(data);
        }
      } catch (err) {
        console.error("Failed to load export history", err);
      }
    };

    fetchHistory();
  }, [exportType]);

  /* ===================== SAFE POLLING (ONLY RUNNING JOBS) ===================== */
  useEffect(() => {
    if (exportJobs.length === 0) return;

    const interval = setInterval(async () => {
      const updated = await Promise.all(
        exportJobs.map(async (job) => {
          // ✅ NEVER overwrite completed / failed history
          if (job.status === "COMPLETED" || job.status === "FAILED") {
            return job;
          }

          const res = await fetch(
            `${BASE_URL}/export/check-status?asOnDate=${job.asOnDate}`,
            { credentials: "include" }
          );
          const data = await res.json();

          if (!data.status || data.status === "NOT_STARTED") {
            return job;
          }

          return { ...job, status: data.status };
        })
      );

      setExportJobs(updated);
    }, 15000);

    return () => clearInterval(interval);
  }, [exportJobs]);

  /* ===================== DOWNLOAD EXPORT ===================== */
  const handleExportAllDownload = async (date) => {
    const res = await fetch(`${BASE_URL}/export/download?asOnDate=${date}`, {
      credentials: "include",
    });
    const data = await res.json();
    window.open(data.downloadUrl.signature, "_blank");
  };

  /* ===================== UI ===================== */
  return (
    <>
      <h3 className="section-heading">Holdings Export</h3>

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

      {exportType === "all" && exportJobs.length > 0 && (
        <div className="export-jobs-container">
          <h4>Previous Export History</h4>

          <table className="export-table">
            <thead>
              <tr>
                <th>Job Name</th>
                <th>As On Date</th>
                <th>Status</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              {exportJobs.map((job) => (
                <tr key={job.jobName}>
                  <td>{job.jobName}</td>
                  <td>{job.asOnDate}</td>
                  <td>
                    <span
                      className={`export-status ${
                        job.status === "COMPLETED"
                          ? "completed"
                          : job.status === "FAILED"
                          ? "failed"
                          : "pending"
                      }`}
                    >
                      {job.status}
                    </span>
                  </td>
                  <td>
                    {job.status === "COMPLETED" ? (
                      <button
                        className="export-btn"
                        onClick={() => handleExportAllDownload(job.asOnDate)}
                      >
                        Download
                      </button>
                    ) : (
                      <span style={{ color: "#9ca3af" }}>-</span>
                    )}
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

export default HoldingsTab;
