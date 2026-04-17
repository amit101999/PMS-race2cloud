import React, { useEffect, useRef, useState, useMemo } from "react";
import { useAccountCodes } from "../../../hooks/GetAllCodes.js";
import { BASE_URL } from "../../../constant.js";

/** Local wall time as yyyy-mm-dd HH:mm:ss (not locale-specific). */
function formatExportTimestamp(iso) {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  const p = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

/**
 * @typedef {{
 *   jobName: string;
 *   accountCode?: string;
 *   asOnDate?: string;
 *   status: string;
 *   createdAt?: string;
 * }} CashExportJob
 */

function CashTab() {
  /** "all" = recent exports list (any account); "single" = start export for one account */
  const [exportType, setExportType] = useState("all");
  const [asOnDate, setAsOnDate] = useState("");
  const [accountCode, setAccountCode] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [showDropdown, setShowDropdown] = useState(false);
  const [loading, setLoading] = useState(false);

  /** @type {[CashExportJob[], React.Dispatch<React.SetStateAction<CashExportJob[]>>]} */
  const [exportJobs, setExportJobs] = useState([]);

  const dropdownRef = useRef(null);
  const exportJobsRef = useRef(exportJobs);
  exportJobsRef.current = exportJobs;
  const { clientOptions } = useAccountCodes();

  const filteredOptions = useMemo(() => {
    return clientOptions.filter((opt) =>
      opt.label.toLowerCase().includes(searchQuery.toLowerCase())
    );
  }, [clientOptions, searchQuery]);

  const sortedExportJobs = useMemo(() => {
    return [...exportJobs].sort((a, b) => {
      const ta = a.createdAt ? new Date(a.createdAt).getTime() : 0;
      const tb = b.createdAt ? new Date(b.createdAt).getTime() : 0;
      return tb - ta;
    });
  }, [exportJobs]);

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

  const fetchHistory = async () => {
    try {
      const res = await fetch(`${BASE_URL}/cash-balance/export/history?limit=10`, {
        credentials: "include",
      });
      const data = await res.json();
      if (Array.isArray(data)) {
        setExportJobs(data);
      }
    } catch (err) {
      console.error("Failed to load cash export history", err);
    }
  };

  const handlePrimaryAction = async () => {
    if (exportType === "all") {
      if (!asOnDate) {
        alert("Please select As On Date");
        return;
      }
      try {
        setLoading(true);
        const response = await fetch(
          `${BASE_URL}/export/cash-all?asOnDate=${encodeURIComponent(asOnDate)}`,
          { method: "GET", credentials: "include" }
        );
        const data = await response.json();
        if (!response.ok) throw new Error(data.message || "Failed to start export");

        const jobName = data.jobName;
        if (!jobName) throw new Error("No job name returned");

        setExportJobs((prev) => {
          const exists = prev.find((j) => j.jobName === jobName);
          if (exists) {
            return prev.map((j) =>
              j.jobName === jobName
                ? {
                    ...j,
                    status: data.status || j.status,
                    ...(data.createdAt ? { createdAt: data.createdAt } : {}),
                  }
                : j
            );
          }
          return [
            {
              jobName,
              asOnDate: data.asOnDate || asOnDate,
              status: data.status || "PENDING",
              createdAt: data.createdAt || new Date().toISOString(),
            },
            ...prev,
          ];
        });
      } catch (error) {
        alert(error instanceof Error ? error.message : "Export failed");
      } finally {
        setLoading(false);
      }
      return;
    }

    try {
      if (!accountCode) {
        alert("Please select an account code");
        return;
      }

      setLoading(true);

      const params = new URLSearchParams();
      params.append("accountCode", accountCode);
      if (asOnDate) params.append("asOnDate", asOnDate);

      const response = await fetch(`${BASE_URL}/cash-balance/export?${params.toString()}`, {
        method: "GET",
        credentials: "include",
      });
      const data = await response.json();
      if (!response.ok) throw new Error(data.message || "Failed to start export");

      const jobName = data.jobName;
      if (!jobName) throw new Error("No job name returned");

      setExportJobs((prev) => {
        const exists = prev.find((j) => j.jobName === jobName);
        if (exists) {
          return prev.map((j) =>
            j.jobName === jobName
              ? {
                  ...j,
                  status: data.status || j.status,
                  ...(data.createdAt ? { createdAt: data.createdAt } : {}),
                }
              : j
          );
        }
        return [
          {
            jobName,
            accountCode,
            status: data.status || "PENDING",
            createdAt: data.createdAt || new Date().toISOString(),
          },
          ...prev,
        ];
      });

      setLoading(false);
    } catch (error) {
      alert(error instanceof Error ? error.message : "Export failed");
      setLoading(false);
    }
  };

  useEffect(() => {
    if (exportType !== "all") return;
    fetchHistory();
  }, [exportType]);

  useEffect(() => {
    const terminalStatuses = ["COMPLETED", "FAILED", "ERROR"];

    const interval = setInterval(async () => {
      const currentJobs = exportJobsRef.current;
      if (!currentJobs.length) return;

      const hasRunningJobs = currentJobs.some((j) => !terminalStatuses.includes(j.status));
      if (!hasRunningJobs) return;

      const updated = await Promise.all(
        currentJobs.map(async (job) => {
          if (terminalStatuses.includes(job.status)) {
            return job;
          }

          try {
            const isAllClientsJob = job.jobName.startsWith("CC_");
            const url = isAllClientsJob
              ? `${BASE_URL}/export/cash-all/status?asOnDate=${encodeURIComponent(
                  job.asOnDate || job.jobName.slice(3)
                )}`
              : `${BASE_URL}/cash-balance/export/status?jobName=${encodeURIComponent(job.jobName)}`;

            const res = await fetch(url, { credentials: "include" });
            const data = await res.json();

            if (!data.status || data.status === "NOT_FOUND" || data.status === "NOT_STARTED") {
              return job;
            }

            return { ...job, status: data.status };
          } catch {
            return job;
          }
        })
      );

      setExportJobs(updated);
    }, 15000);

    return () => clearInterval(interval);
  }, []);

  /** Resolve Stratus presigned response (string or nested object). */
  const pickDownloadUrl = (json) => {
    const u = json?.downloadUrl;
    return (
      u?.signature?.signature ??
      u?.signature?.accessUrl ??
      (typeof u?.signature === "string" ? u.signature : null) ??
      u?.accessUrl ??
      (typeof u === "string" ? u : null) ??
      ""
    );
  };

  /** @param {CashExportJob | string} jobOrName */
  const handleCashDownload = async (jobOrName) => {
    try {
      const isJob = typeof jobOrName === "object" && jobOrName !== null;
      const jobName = isJob ? jobOrName.jobName : jobOrName;
      const isAllClientsJob = String(jobName).startsWith("CC_");

      const res = await fetch(
        isAllClientsJob
          ? `${BASE_URL}/export/cash-all/download?asOnDate=${encodeURIComponent(
              isJob && jobOrName.asOnDate ? jobOrName.asOnDate : String(jobName).slice(3)
            )}`
          : `${BASE_URL}/cash-balance/export/download?jobName=${encodeURIComponent(String(jobName))}`,
        { credentials: "include" }
      );
      const data = await res.json();
      if (!res.ok) {
        alert(data.message || data.status || "Download failed");
        return;
      }
      const url = pickDownloadUrl(data);
      if (url && typeof url === "string") {
        window.open(url, "_blank");
      } else {
        alert("No download URL returned. Check export job status.");
      }
    } catch (err) {
      console.error(err);
      alert("Failed to download export file");
    }
  };

  return (
    <>
      <h3 className="section-heading">Cash Export</h3>

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
        <button type="button" className="export-btn" onClick={handlePrimaryAction} disabled={loading}>
          {loading ? "Starting..." : "Export"}
        </button>
      </div>

      {exportType === "all" && exportJobs.length > 0 && (
        <div className="export-jobs-container">
          <h4>Previous Export History</h4>

          <table className="export-table">
            <thead>
              <tr>
                <th>File Name</th>
                <th>Timestamp</th>
                <th>Status</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              {sortedExportJobs.map((job) => (
                <tr key={job.jobName}>
                  <td>{job.jobName}</td>
                  <td>
                    {job.createdAt ? formatExportTimestamp(job.createdAt) : "—"}
                  </td>
                  <td>
                    <span
                      className={`export-status ${
                        job.status === "COMPLETED"
                          ? "completed"
                          : job.status === "FAILED" || job.status === "ERROR"
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
                        type="button"
                        className="export-btn"
                        onClick={() => handleCashDownload(job)}
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

export default CashTab;
