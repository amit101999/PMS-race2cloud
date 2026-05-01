import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import MainLayout from "../../layouts/MainLayout";
import { Card } from "../../components/common/CommonComponents";
import "../SplitPage/SplitPage.css";
import "../BonusPage/BonusPage.css";
import "./MergerPage.css";
import { BASE_URL } from "../../constant";

const MERGER_OLD_ISIN_PLACEHOLDER = "Type or pick ISIN — matched name shows here";

/** Normalize Security_List API row to { isin, securityCode, securityName } */
function normalizeSecurityRow(raw) {
  if (!raw || typeof raw !== "object") return null;
  const isin = String(raw.isin ?? raw.ISIN ?? "").trim();
  if (!isin) return null;
  return {
    isin,
    securityCode: String(raw.securityCode ?? raw.Security_Code ?? "").trim(),
    securityName: String(raw.securityName ?? raw.Security_Name ?? "").trim(),
  };
}

function escapeCsvCell(value) {
  const s = value === null || value === undefined ? "" : String(value);
  if (/[",\r\n]/.test(s)) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

/** Stable React key for a lot-level preview row. */
function lotRowKey(row, idx) {
  return `${row.accountCode}|${row.sourceType ?? ""}|${row.sourceRowId ?? ""}|${row.lotTrandate ?? ""}|${idx}`;
}

function MergerPage() {
  const [effectiveDate, setEffectiveDate] = useState("");
  const [recordDate, setRecordDate] = useState("");
  const [oldIsin, setOldIsin] = useState("");
  /** When false and old ISIN matches security list, input shows "Name -- ISIN" instead of raw code. */
  const [oldIsinFocused, setOldIsinFocused] = useState(false);
  const [newIsinInput, setNewIsinInput] = useState("");
  const [newSecurityCode, setNewSecurityCode] = useState("");
  const [newSecurityName, setNewSecurityName] = useState("");
  const [ratio1, setRatio1] = useState("");
  const [ratio2, setRatio2] = useState("");

  const [securities, setSecurities] = useState([]);
  const [loadError, setLoadError] = useState(null);
  const [formError, setFormError] = useState(null);
  const [apiError, setApiError] = useState(null);
  const [previewRows, setPreviewRows] = useState([]);
  const [previewMeta, setPreviewMeta] = useState(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [applyLoading, setApplyLoading] = useState(false);
  const [applySuccess, setApplySuccess] = useState(null);
  const [applyStatus, setApplyStatus] = useState(null);
  const applyPollRef = useRef(null);

  const PAGE_SIZE = 10;
  const [page, setPage] = useState(1);
  useEffect(() => setPage(1), [previewRows]);
  const totalPages = Math.max(1, Math.ceil(previewRows.length / PAGE_SIZE));
  const paginatedPreview = previewRows.slice(
    (page - 1) * PAGE_SIZE,
    page * PAGE_SIZE,
  );

  const oldIsinInputRef = useRef(null);
  const oldIsinPickingRef = useRef(false);

  const clearStatus = useCallback(() => {
    setFormError(null);
    setApiError(null);
    setApplySuccess(null);
    setApplyStatus(null);
  }, []);

  useEffect(() => {
    return () => {
      if (applyPollRef.current) {
        clearInterval(applyPollRef.current);
        applyPollRef.current = null;
      }
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoadError(null);
      const parseList = (data) => {
        const arr = Array.isArray(data?.data) ? data.data : [];
        return arr.map(normalizeSecurityRow).filter(Boolean);
      };

      const fetchList = async (path) => {
        const res = await fetch(`${BASE_URL}${path}`);
        const data = await res.json().catch(() => ({}));
        if (!res.ok) {
          throw new Error(data?.message || `HTTP ${res.status}`);
        }
        if (data.success === false && data.message) {
          throw new Error(data.message);
        }
        return parseList(data);
      };

      try {
        let list = [];
        try {
          list = await fetchList("/isin/security-list-isins");
        } catch {
          list = [];
        }
        if (list.length === 0) {
          list = await fetchList("/split/getAllSecuritiesList");
        }
        if (!cancelled) {
          setSecurities(list);
          if (list.length === 0) {
            setLoadError(
              "No securities loaded. Check API URL (constant.js), CORS, and Security_List data.",
            );
          }
        }
      } catch (err) {
        if (!cancelled) {
          setLoadError(err.message || "Could not load securities list");
          setSecurities([]);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const findSec = (isin) => {
    const t = String(isin ?? "").trim().toUpperCase();
    if (!t) return undefined;
    return securities.find((x) => String(x.isin ?? "").trim().toUpperCase() === t);
  };

  /** Blurred display when old ISIN matches list: "SecurityName -- ISIN". */
  const friendlyOldIsinInField = (sec) => {
    const name = String(sec.securityName ?? "").trim();
    const isin = String(sec.isin ?? "").trim().toUpperCase();
    if (!name) return isin;
    return `${name} -- ${isin}`;
  };

  const companyFromIsin = (isin) => {
    const sec = findSec(isin);
    return sec
      ? {
        securityName: sec.securityName,
        securityCode: sec.securityCode,
        isin: sec.isin,
      }
      : { isin: String(isin ?? "").trim().toUpperCase() };
  };

  const newIsinTrimmed = newIsinInput.trim().toUpperCase();
  const newCodeTrimmed = newSecurityCode.trim();
  const newNameTrimmed = newSecurityName.trim();
  const norm = (v) => String(v ?? "").trim().toUpperCase();
  const oldSecurity = findSec(oldIsin);
  const oldIsinInputValue =
    !oldIsinFocused && oldSecurity ? friendlyOldIsinInField(oldSecurity) : oldIsin;
  const oldIsinInputTitle =
    oldSecurity && !oldIsinFocused
      ? String(oldSecurity.isin ?? "").trim().toUpperCase()
      : undefined;

  /** Custom list (native datalist uses OS dark popup in many browsers). */
  const oldIsinSuggestions = useMemo(() => {
    if (!oldIsinFocused || securities.length === 0) return [];
    const q = String(oldIsin ?? "").trim().toLowerCase();
    const cap = 50;
    if (!q) return securities.slice(0, 30);
    return securities
      .filter((s) => {
        const isin = String(s.isin ?? "").toLowerCase();
        const name = String(s.securityName ?? "").toLowerCase();
        return isin.includes(q) || name.includes(q);
      })
      .slice(0, cap);
  }, [oldIsinFocused, oldIsin, securities]);

  const pickOldIsin = useCallback(
    (isin) => {
      clearStatus();
      oldIsinPickingRef.current = true;
      setOldIsin(String(isin ?? "").trim());
      setOldIsinFocused(false);
      oldIsinInputRef.current?.blur();
    },
    [clearStatus],
  );

  const validateForm = () => {
    if (!effectiveDate || !recordDate) {
      return "Effective date and record date are required.";
    }
    const o = norm(oldIsin);
    if (!o) {
      return "Enter or select the old ISIN.";
    }
    if (!newIsinTrimmed) {
      return "Enter the new company ISIN.";
    }
    if (!newCodeTrimmed) {
      return "Enter the security code for the new ISIN.";
    }
    if (!newNameTrimmed) {
      return "Enter the security name for the new ISIN.";
    }
    if (o === newIsinTrimmed) {
      return "New ISIN must differ from old ISIN.";
    }
    const r1 = Number(ratio1);
    const r2 = Number(ratio2);
    if (!Number.isFinite(r1) || r1 <= 0) {
      return "Ratio 1 must be a positive number.";
    }
    if (!Number.isFinite(r2) || r2 <= 0) {
      return "Ratio 2 must be a positive number.";
    }
    return null;
  };

  const buildMergerApiBody = () => {
    const o = norm(oldIsin);
    return {
      recordDateIso: recordDate,
      effectiveDateIso: effectiveDate,
      ratio1: Number(ratio1),
      ratio2: Number(ratio2),
      oldCompanies: [companyFromIsin(o)],
      mergeIntoNewCompany: {
        isin: newIsinTrimmed,
        securityCode: newCodeTrimmed,
        securityName: newNameTrimmed,
      },
    };
  };

  const handleFetchPreview = async () => {
    clearStatus();
    setPreviewRows([]);
    setPreviewMeta(null);
    const err = validateForm();
    if (err) {
      setFormError(err);
      return;
    }
    setPreviewLoading(true);
    try {
      const res = await fetch(`${BASE_URL}/merger/preview`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(buildMergerApiBody()),
      });
      const data = await res.json();
      if (!data.success) {
        setApiError(data.message || "Preview failed");
        return;
      }
      setPreviewRows(Array.isArray(data.data) ? data.data : []);
      setPreviewMeta(data.meta || null);
    } catch (e) {
      setApiError(e.message || "Preview request failed");
    } finally {
      setPreviewLoading(false);
    }
  };

  const stopApplyPoll = useCallback(() => {
    if (applyPollRef.current) {
      clearInterval(applyPollRef.current);
      applyPollRef.current = null;
    }
  }, []);

  /**
   * Poll /merger/apply-status every 3s. Stop on terminal status or after
   * MAX_POLL_ATTEMPTS to avoid forever spinning if the worker stalls.
   */
  const startApplyPoll = useCallback(
    (jobName) => {
      stopApplyPoll();
      const POLL_INTERVAL_MS = 3000;
      const MAX_POLL_ATTEMPTS = 200;
      let attempts = 0;

      const tick = async () => {
        attempts += 1;
        try {
          const url = `${BASE_URL}/merger/apply-status?jobName=${encodeURIComponent(jobName)}`;
          const res = await fetch(url);
          const data = await res.json().catch(() => ({}));
          if (!data?.success) {
            setApiError(data?.message || `Status check failed (HTTP ${res.status})`);
            stopApplyPoll();
            setApplyLoading(false);
            return;
          }
          const status = String(data.status || "").toUpperCase();
          setApplyStatus(status);

          if (status === "COMPLETED") {
            stopApplyPoll();
            setApplyLoading(false);
            setApplySuccess("Merger applied successfully.");
            return;
          }
          if (status === "FAILED" || status === "ERROR") {
            stopApplyPoll();
            setApplyLoading(false);
            setApiError(
              status === "FAILED"
                ? "Merger application failed. Check function logs in Catalyst."
                : "Merger application is stale or errored. Try again.",
            );
            return;
          }
          if (attempts >= MAX_POLL_ATTEMPTS) {
            stopApplyPoll();
            setApplyLoading(false);
            setApiError(
              "Merger is taking longer than expected. Refresh the page or check Catalyst job logs.",
            );
          }
        } catch (e) {
          stopApplyPoll();
          setApplyLoading(false);
          setApiError(e.message || "Status check error");
        }
      };

      tick();
      applyPollRef.current = setInterval(tick, POLL_INTERVAL_MS);
    },
    [stopApplyPoll],
  );

  const handleApply = async () => {
    clearStatus();
    const err = validateForm();
    if (err) {
      setFormError(err);
      return;
    }
    if (!previewRows.length) {
      setApiError("Run preview first.");
      return;
    }
    setApplyLoading(true);
    setApplyStatus("PENDING");
    try {
      const res = await fetch(`${BASE_URL}/merger/apply`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(buildMergerApiBody()),
      });
      const data = await res.json();
      if (!data.success) {
        setApiError(data.message || data.detail || "Apply failed");
        setApplyLoading(false);
        return;
      }
      const jobName = data.jobName;
      if (!jobName) {
        setApiError("Apply queued but no jobName returned.");
        setApplyLoading(false);
        return;
      }
      startApplyPoll(jobName);
    } catch (e) {
      setApiError(e.message || "Apply request failed");
      setApplyLoading(false);
    }
  };

  const fmtNum = (n) => (Number.isFinite(Number(n)) ? Number(n).toFixed(2) : "—");

  const handleExportMergerCsv = useCallback(() => {
    if (!previewRows.length) return;
    const o = norm(oldIsin);
    const headers = [
      "Account Code",
      "Old ISIN",
      "Effective Date",
      "WAP",
      "QTY",
      "Ratio 1",
      "Ratio 2",
      "New WAP",
      "New ISIN",
      "New QTY",
    ];
    const lines = [headers.map(escapeCsvCell).join(",")];
    for (const row of previewRows) {
      const oldIsinVal = row.oldIsin ?? (o || "");
      const newIsinVal = row.newIsin || newIsinTrimmed;
      const r1 = row.ratio1 ?? ratio1;
      const r2 = row.ratio2 ?? ratio2;
      lines.push(
        [
          row.accountCode,
          oldIsinVal,
          effectiveDate,
          row.lotCostPerShare ?? "",
          row.lotQty ?? "",
          r1,
          r2,
          row.newWAP ?? "",
          newIsinVal,
          row.newQty ?? "",
        ]
          .map(escapeCsvCell)
          .join(","),
      );
    }
    const csv = lines.join("\r\n");
    const safeDate = (recordDate || "nodate").replace(/[^\d-]/g, "") || "nodate";
    const safeIsin = (newIsinTrimmed || "merger").replace(/[^A-Za-z0-9_-]/g, "") || "merger";
    const filename = `merger-preview-lots_${safeDate}_${safeIsin}.csv`;
    const blob = new Blob(["\ufeff", csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    a.rel = "noopener";
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }, [
    previewRows,
    oldIsin,
    newIsinTrimmed,
    ratio1,
    ratio2,
    effectiveDate,
    recordDate,
  ]);

  return (
    <MainLayout title="Merger">
      <Card style={{ marginTop: 4 }}>
        <div className="merger-page">
          {loadError && (
            <div className="alert error" style={{ marginBottom: 12 }}>
              {loadError} (old ISIN suggestions may be empty until the list loads.)
            </div>
          )}
          {formError && <div className="alert error">{formError}</div>}
          {apiError && <div className="alert error">{apiError}</div>}
          {applySuccess && <div className="alert success">{applySuccess}</div>}

          <div className="split-card merger-split-card">
            <div className="merger-top-row">
              <div className="split-field">
                <label htmlFor="merger-effective">Effective date</label>
                <input
                  id="merger-effective"
                  type="date"
                  value={effectiveDate}
                  onChange={(e) => {
                    clearStatus();
                    setEffectiveDate(e.target.value);
                  }}
                />
              </div>
              <div className="split-field">
                <label htmlFor="merger-record">Record date</label>
                <input
                  id="merger-record"
                  type="date"
                  value={recordDate}
                  onChange={(e) => {
                    clearStatus();
                    setRecordDate(e.target.value);
                  }}
                />
              </div>
              <div className="split-field">
                <label htmlFor="merger-ratio1">Ratio 1</label>
                <input
                  id="merger-ratio1"
                  type="number"
                  min="0"
                  step="any"
                  placeholder="e.g. 1"
                  value={ratio1}
                  onChange={(e) => {
                    clearStatus();
                    setRatio1(e.target.value);
                  }}
                />
              </div>
              <div className="split-field">
                <label htmlFor="merger-ratio2">Ratio 2</label>
                <input
                  id="merger-ratio2"
                  type="number"
                  min="0"
                  step="any"
                  placeholder="e.g. 2"
                  value={ratio2}
                  onChange={(e) => {
                    clearStatus();
                    setRatio2(e.target.value);
                  }}
                />
              </div>
            </div>

            <div className="merger-section">
              <h3 className="merger-section-title">Old Company (Old ISIN)</h3>

              <div className="merger-companies-stack">
                <div className="split-field merger-select-field merger-old-isin-field">
                  <label htmlFor="merger-old-isin"></label>
                  <div className="merger-old-isin-wrap">
                    <input
                      ref={oldIsinInputRef}
                      id="merger-old-isin"
                      className="merger-text-input merger-old-isin-input"
                      autoComplete="off"
                      placeholder={MERGER_OLD_ISIN_PLACEHOLDER}
                      title={oldIsinInputTitle}
                      value={oldIsinInputValue}
                      aria-expanded={oldIsinFocused && oldIsinSuggestions.length > 0}
                      aria-controls="merger-old-isin-suggest"
                      role="combobox"
                      aria-autocomplete="list"
                      onFocus={() => {
                        clearStatus();
                        setOldIsinFocused(true);
                      }}
                      onBlur={(e) => {
                        if (oldIsinPickingRef.current) {
                          oldIsinPickingRef.current = false;
                          return;
                        }
                        setOldIsin(norm(e.target.value));
                        setOldIsinFocused(false);
                      }}
                      onChange={(e) => {
                        clearStatus();
                        setOldIsin(e.target.value);
                      }}
                    />
                    {oldIsinFocused && oldIsinSuggestions.length > 0 && (
                      <ul
                        id="merger-old-isin-suggest"
                        className="merger-old-isin-suggest"
                        role="listbox"
                      >
                        {oldIsinSuggestions.map((s) => (
                          <li
                            key={s.isin}
                            role="option"
                            className="merger-old-isin-suggest-item"
                            onMouseDown={(e) => {
                              e.preventDefault();
                              pickOldIsin(s.isin);
                            }}
                          >
                            <span className="merger-old-isin-suggest-code">{s.isin}</span>
                            {s.securityName ? (
                              <span className="merger-old-isin-suggest-name">
                                {s.securityName}
                              </span>
                            ) : null}
                          </li>
                        ))}
                      </ul>
                    )}
                  </div>
                </div>

                <h3 className="merger-section-title">Merge Into New Company (New ISIN)</h3>

                <div className="merger-new-company-fields">
                  <div className="split-field merger-new-isin-field">
                    <label htmlFor="merger-new-isin">New ISIN</label>
                    <input
                      id="merger-new-isin"
                      type="text"
                      className="merger-text-input"
                      autoComplete="off"
                      placeholder="New ISIN"
                      value={newIsinInput}
                      onChange={(e) => {
                        clearStatus();
                        setNewIsinInput(e.target.value);
                      }}
                    />
                  </div>
                  <div className="split-field merger-new-isin-field">
                    <label htmlFor="merger-new-security-code">Security code for new ISIN</label>
                    <input
                      id="merger-new-security-code"
                      type="text"
                      className="merger-text-input"
                      autoComplete="off"
                      placeholder="Security Code for New ISIN"
                      value={newSecurityCode}
                      onChange={(e) => {
                        clearStatus();
                        setNewSecurityCode(e.target.value);
                      }}
                    />
                  </div>
                  <div className="split-field merger-new-isin-field">
                    <label htmlFor="merger-new-security-name">Security name for new ISIN</label>
                    <input
                      id="merger-new-security-name"
                      type="text"
                      className="merger-text-input"
                      autoComplete="off"
                      placeholder="Security Name for New ISIN"
                      value={newSecurityName}
                      onChange={(e) => {
                        clearStatus();
                        setNewSecurityName(e.target.value);
                      }}
                    />
                  </div>
                </div>
              </div>
            </div>

            <button
              type="button"
              className="bonus-submit"
              disabled={previewLoading || validateForm() !== null}
              onClick={handleFetchPreview}
            >
              {previewLoading ? "Loading..." : "Fetch Affected Account"}
            </button>
          </div>
        </div>
      </Card>

      {previewRows.length > 0 && (
        <div className="bonus-preview-wrapper full-width">
          <div className="merger-preview-header-row">
            <div className="bonus-preview-actions">
              <button
                type="button"
                className="bonus-submit"
                onClick={handleExportMergerCsv}
              >
                Export CSV
              </button>
            </div>
          </div>

          {previewMeta ? (
            <div
              className="merger-preview-summary"
              style={{
                margin: "8px 0 12px 0",
                padding: "8px 12px",
                background: "#f4f7fb",
                border: "1px solid #d8dde5",
                borderRadius: 6,
                fontSize: 13,
                lineHeight: 1.6,
                display: "flex",
                gap: 18,
                flexWrap: "wrap",
              }}
            >
              <span><strong>Accounts affected:</strong> {previewMeta.accountsAffected ?? "—"}</span>
              <span><strong>Surviving lots:</strong> {previewMeta.survivingLots ?? "—"}</span>
              <span><strong>New shares total:</strong> {previewMeta.totalNewShares ?? "—"}</span>
              <span><strong>Carried cost:</strong> {fmtNum(previewMeta.totalCarriedCost)}</span>
              <span><strong>Skipped lots (round-to-0):</strong> {previewMeta.skippedZeroLots ?? 0}</span>
              <span><strong>Skipped cost basis:</strong> {fmtNum(previewMeta.skippedCostBasis)}</span>
            </div>
          ) : null}

          <div className="bonus-preview-table-wrapper merger-preview-table-scroll">
            <table className="bonus-preview-table merger-preview-detail-table">
              <thead>
                <tr>
                  <th>Account code</th>
                  <th>Old ISIN</th>
                  <th>Effective Date</th>
                  <th>WAP</th>
                  <th>QTY</th>
                  <th>Ratio 1</th>
                  <th>Ratio 2</th>
                  <th>New WAP</th>
                  <th>New ISIN</th>
                  <th>New QTY</th>
                </tr>
              </thead>
              <tbody>
                {paginatedPreview.map((row, idx) => {
                  const skipped = !!row.willSkip;
                  return (
                    <tr
                      key={lotRowKey(row, (page - 1) * PAGE_SIZE + idx)}
                      style={skipped ? { background: "#fff7f0", color: "#7a4a1a" } : undefined}
                      title={skipped ? "Lot rounds to 0 — will be skipped on apply." : undefined}
                    >
                      <td className="merger-account-cell">{row.accountCode}</td>
                      <td>{row.oldIsin ?? (norm(oldIsin) || "—")}</td>
                      <td>{effectiveDate || "—"}</td>
                      <td>{fmtNum(row.lotCostPerShare)}</td>
                      <td>{fmtNum(row.lotQty)}</td>
                      <td>{row.ratio1 ?? ratio1}</td>
                      <td>{row.ratio2 ?? ratio2}</td>
                      <td>{fmtNum(row.newWAP)}</td>
                      <td>{row.newIsin || newIsinTrimmed}</td>
                      <td>{row.newQty}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

          {totalPages > 1 && (
            <div className="pagination">
              <button
                type="button"
                disabled={page === 1}
                onClick={() => setPage(page - 1)}
              >
                Prev
              </button>
              <span>
                Page {page} of {totalPages}
              </span>
              <button
                type="button"
                disabled={page === totalPages}
                onClick={() => setPage(page + 1)}
              >
                Next
              </button>
            </div>
          )}

          <div className="bonus-preview-actions">
            <button
              type="button"
              className="bonus-submit"
              disabled={applyLoading}
              onClick={handleApply}
            >
              {applyLoading
                ? applyStatus === "RUNNING"
                  ? "Running... (this may take a few minutes)"
                  : "Queued..."
                : "Confirm & Apply Merger"}
            </button>
            {applyLoading && applyStatus ? (
              <span className="merger-job-status">Status: {applyStatus}</span>
            ) : null}
          </div>
        </div>
      )}
    </MainLayout>
  );
}

export default MergerPage;
