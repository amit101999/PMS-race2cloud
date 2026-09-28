import React, { useState, useEffect, useRef, useMemo } from "react";
import MainLayout from "../../layouts/MainLayout";
import DownloadButton from "../../components/common/DownloadButton";
import CustodianComparePanel from "../CustodianUploadPage/CustodianComparePanel";
import "./BonusPage.css";
import "../TempTransactionUpload/tokens.css";
import "../TempTransactionUpload/TempTransaction.css";
import "../TempTransactionUpload/custody/CustodyMatch.css";
import "../../styles/ops-pages.css";
import { BASE_URL } from "../../constant";
import { useJobNotifications } from "../../notifications/JobNotificationContext";

const numberFormat = new Intl.NumberFormat(undefined, { maximumFractionDigits: 4 });

/** Maps GET /bonus/apply-progress → the notification system's vocabulary. */
function parseBonusStatus(d) {
  const s = String(d?.status ?? "").toUpperCase();
  if (s === "COMPLETED") {
    return { status: "completed", message: "The bonus has been applied to all affected accounts." };
  }
  if (s === "COMPLETED_WITH_ERRORS") {
    return {
      status: "completed",
      message: "The bonus was applied, but a few accounts could not be updated.",
    };
  }
  if (s === "ERROR") {
    return { status: "failed", message: "The bonus could not be completed. Please try again." };
  }
  return { status: "in_progress", message: null };
}

function BonusPage() {
  /* ===========================
     FORM STATE
     =========================== */
  const [isin, setIsin] = useState("");
  const [securityCode, setSecurityCode] = useState("");
  const [securityName, setSecurityName] = useState("");
  const [ratio1, setRatio1] = useState("");
  const [ratio2, setRatio2] = useState("");
  const [recordDate, setRecordDate] = useState("");
  const [effectiveDate, setEffectiveDate] = useState("");

  // yyyy-mm-dd strings compare correctly as text.
  const datesOutOfOrder = Boolean(recordDate && effectiveDate && effectiveDate < recordDate);

  const [securities, setSecurities] = useState([]);
  const [searchQuery, setSearchQuery] = useState("");
  const [showDropdown, setShowDropdown] = useState(false);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [applying, setApplying] = useState(false);
  const [applySuccess, setApplySuccess] = useState(false);
  const [success, setSuccess] = useState(false);
  const [applyJobName, setApplyJobName] = useState(null);
  const [applyStatus, setApplyStatus] = useState(null);

  const [previewData, setPreviewData] = useState([]);
  const [step, setStep] = useState("form");
  /** Actual codes whose virtual-scheme rows are expanded in the preview table. */
  const [expandedActuals, setExpandedActuals] = useState(() => new Set());

  const [exportDownloadUrl, setExportDownloadUrl] = useState("");
  const [exportError, setExportError] = useState("");
  const [exportLoading, setExportLoading] = useState(false);

  /* ===========================
     GROUP BY ACTUAL CODE + PAGINATION
     =========================== */
  const PAGE_SIZE = 10;
  const [page, setPage] = useState(1);

  const previewGroups = useMemo(() => {
    const map = new Map();
    for (const row of previewData) {
      const actualKey = String(row.actualCode ?? "").trim() || "—";
      let group = map.get(actualKey);
      if (!group) {
        group = {
          actualCode: actualKey,
          isin: row.isin || "",
          virtuals: [],
          currentHolding: 0,
          bonusShares: 0,
          newHolding: 0,
          delta: 0,
        };
        map.set(actualKey, group);
      }
      group.virtuals.push(row);
      group.currentHolding += Number(row.currentHolding) || 0;
      group.bonusShares += Number(row.bonusShares) || 0;
      group.newHolding += Number(row.newHolding) || 0;
      group.delta += Number(row.delta) || 0;
      if (!group.isin && row.isin) group.isin = row.isin;
    }
    return [...map.values()];
  }, [previewData]);

  useEffect(() => {
    setPage(1);
    setExpandedActuals(new Set());
  }, [previewData]);

  const totalPages = Math.ceil(previewGroups.length / PAGE_SIZE) || 1;
  const paginatedGroups = previewGroups.slice(
    (page - 1) * PAGE_SIZE,
    page * PAGE_SIZE,
  );

  const toggleActualExpand = (actualCode) => {
    setExpandedActuals((prev) => {
      const next = new Set(prev);
      if (next.has(actualCode)) next.delete(actualCode);
      else next.add(actualCode);
      return next;
    });
  };

  const dropdownRef = useRef(null);

  const { trackJob, reportJob } = useJobNotifications();

  /* ===========================
     FETCH SECURITIES
     =========================== */
     useEffect(() => {
      let cancelled = false;
      (async () => {
        try {
          const res = await fetch(`${BASE_URL}/bonus/getAllSecuritiesList`);
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const data = await res.json();
          if (!cancelled && data.success) setSecurities(data.data);
        } catch (err) {
          if (!cancelled) setError(err.message || "Failed to load securities");
          setSecurities([]);
        }
      })();
      return () => { cancelled = true; };
    }, []);

  /* ===========================
     DROPDOWN CLOSE
     =========================== */
  useEffect(() => {
    const handler = (e) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target)) {
        setShowDropdown(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  /* ===========================
     POLL BONUS APPLY JOB STATUS
     =========================== */
  useEffect(() => {
    if (!applyJobName || !applying) return;

    const terminalStatuses = ["COMPLETED", "FAILED", "ERROR"];
    if (terminalStatuses.includes(applyStatus)) return;

    const interval = setInterval(async () => {
      try {
        const res = await fetch(
          `${BASE_URL}/bonus/apply-status?jobName=${encodeURIComponent(applyJobName)}`,
          { credentials: "include" }
        );
        const data = await res.json();

        if (!data.success) return;

        setApplyStatus(data.status);

        if (data.status === "COMPLETED") {
          clearInterval(interval);
          setApplySuccess(true);

          setTimeout(() => {
            setIsin("");
            setSearchQuery("");
            setSecurityCode("");
            setSecurityName("");
            setRatio1("");
            setRatio2("");
            setRecordDate("");
            setEffectiveDate("");
            setPreviewData([]);
            setStep("form");
            setExportDownloadUrl("");
            setExportError("");
            setApplying(false);
            setApplySuccess(false);
            setApplyJobName(null);
            setApplyStatus(null);
            setSuccess(true);
          }, 800);
        } else if (data.status === "FAILED" || data.status === "ERROR") {
          clearInterval(interval);
          setError("Bonus application failed. Please try again.");
          setApplying(false);
          setApplyJobName(null);
          setApplyStatus(null);
        }
      } catch {
        // ignore polling errors
      }
    }, 10000);

    return () => clearInterval(interval);
  }, [applyJobName, applying, applyStatus]);

  const q = searchQuery.toLowerCase();
  const filteredSecurities = securities.filter(
    (s) =>
      String(s?.isin ?? "").toLowerCase().includes(q) ||
      String(s?.securityCode ?? "").toLowerCase().includes(q) ||
      String(s?.securityName ?? "").toLowerCase().includes(q),
  );

  const handleSelectISIN = (sec) => {
    setIsin(sec.isin);
    setSearchQuery(sec.isin);
    setSecurityCode(sec.securityCode);
    setSecurityName(sec.securityName);
    setShowDropdown(false);
  };

  /* ===========================
     PREVIEW BONUS (FIFO)
     =========================== */
  const fetchPreview = async () => {
    setLoading(true);
    setError(null);

    const res = await fetch(`${BASE_URL}/bonus/preview`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        isin,
        ratio1: Number(ratio1),
        ratio2: Number(ratio2),
        recordDate, // holdings are counted as on this date
      }),
    });

    const data = await res.json();

    if (data.success) {
      setPreviewData(data.data || []);
      setStep("preview");
    } else {
      setError(data.message);
    }

    setLoading(false);
  };
  const downloadBonusExport = () => {
    if (!isin || !ratio1 || !ratio2 || !recordDate) return;

    const url =
      `${BASE_URL}/export/bonus-preview` +
      `?isin=${encodeURIComponent(isin)}` +
      `&ratio1=${encodeURIComponent(ratio1)}` +
      `&ratio2=${encodeURIComponent(ratio2)}` +
      `&recordDate=${encodeURIComponent(recordDate)}`;

    window.open(url, "_blank");
  };

  return (
    <MainLayout title="Stock Bonus">
      <div className="temp-transaction-container txn-ops ops-page">
      <div className="temp-transaction-card">

        <div aria-live="polite">
          {success && (
            <div className="cm-verdict ok">Bonus applied successfully.</div>
          )}
          {error && <div className="cm-verdict bad">{error}</div>}
        </div>

        <div className="bonus-card">
          {/* ISIN SEARCH */}
          <div className="account-code-search" ref={dropdownRef}>
            <label className="search-label" htmlFor="bonus-isin">ISIN</label>
            <div className="search-input-wrapper">
              <span className="search-icon" aria-hidden="true">🔍</span>
              <input
                id="bonus-isin"
                name="isin"
                type="text"
                className="search-input"
                placeholder="Search ISIN, code or name…"
                autoComplete="off"
                spellCheck={false}
                value={searchQuery}
                onChange={(e) => {
                  setSearchQuery(e.target.value);
                  setShowDropdown(true);
                }}
              />
              {searchQuery && (
                <button
                  type="button"
                  className="clear-icon"
                  aria-label="Clear ISIN"
                  onClick={() => {
                    setSearchQuery("");
                    setIsin("");
                    setSecurityCode("");
                    setSecurityName("");
                  }}
                >
                  ✕
                </button>
              )}
            </div>

            {showDropdown && filteredSecurities.length > 0 && (
              <div className="search-dropdown">
                <div className="dropdown-header">Search ISIN</div>
                <div className="dropdown-options">
                  {filteredSecurities.map((sec, idx) => (
                    <button
                      type="button"
                      key={`${sec.isin || "no-isin"}-${idx}`}
                      className="dropdown-option"
                      onClick={() => handleSelectISIN(sec)}
                    >
                      <strong translate="no">{sec.isin}</strong>
                      <div className="ops-option-sub">
                        {sec.securityCode} – {sec.securityName}
                      </div>
                    </button>
                  ))}
                </div>
                <div className="dropdown-footer">
                  {filteredSecurities.length} of {securities.length} ISINs
                </div>
              </div>
            )}
          </div>

          <div className="bonus-field">
            <label htmlFor="bonus-security-code">Security Code</label>
            <input id="bonus-security-code" value={securityCode} disabled />
          </div>

          <div className="bonus-field">
            <label htmlFor="bonus-security-name">Security Name</label>
            <input id="bonus-security-name" value={securityName} disabled />
          </div>

          <div className="bonus-field">
            <label htmlFor="bonus-ratio-held">
              Ratio <span className="ratio-hint">(For every held)</span>
            </label>
            <input
              id="bonus-ratio-held"
              name="ratio2"
              type="number"
              inputMode="decimal"
              min="0"
              step="any"
              value={ratio2}
              onChange={(e) => setRatio2(e.target.value)}
            />
          </div>

          <div className="bonus-field">
            <label htmlFor="bonus-ratio-shares">
              Ratio <span className="ratio-hint">(Bonus shares)</span>
            </label>
            <input
              id="bonus-ratio-shares"
              name="ratio1"
              type="number"
              inputMode="decimal"
              min="0"
              step="any"
              value={ratio1}
              onChange={(e) => setRatio1(e.target.value)}
            />
          </div>

          {/* DATES — Record Date drives every calculation; Effective Date is stored for reference. */}
          <div className="bonus-field">
            <label htmlFor="bonus-record-date">Record Date</label>
            <input
              id="bonus-record-date"
              name="recordDate"
              type="date"
              value={recordDate}
              onChange={(e) => setRecordDate(e.target.value)}
            />
          </div>

          <div className="bonus-field">
            <label htmlFor="bonus-effective-date">
              Effective Date <span className="ratio-hint">(Optional)</span>
            </label>
            <input
              id="bonus-effective-date"
              name="effectiveDate"
              type="date"
              value={effectiveDate}
              min={recordDate || undefined}
              aria-invalid={datesOutOfOrder || undefined}
              aria-describedby={datesOutOfOrder ? "bonus-date-error" : undefined}
              onChange={(e) => setEffectiveDate(e.target.value)}
            />
          </div>

          {datesOutOfOrder && (
            <div id="bonus-date-error" className="cm-verdict bad ops-form-message" role="alert">
              Effective Date cannot be before the Record Date. Pick a date on or after {recordDate}.
            </div>
          )}

          <div className="ops-form-actions">
          <button
            type="button"
            className="cm-primary"
            disabled={
              !isin ||
              !(Number(ratio1) > 0) ||
              !(Number(ratio2) > 0) ||
              !recordDate ||
              datesOutOfOrder ||
              loading
            }
            onClick={fetchPreview}
          >
            {loading ? "Fetching…" : "Fetch Affected Accounts"}
          </button>
          </div>
        </div>
      </div>

      {/* ===========================
         PREVIEW
         =========================== */}
      {step === "preview" && (
        <div className="temp-transaction-card">
          <h3 className="ops-card-title">Bonus Impact Preview</h3>

          {previewData.length === 0 ? (
            <p className="cm-empty">No accounts hold this security on the record date.</p>
          ) : (
            <>
              <div className="data-table-wrap">
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>Actual Code</th>
                      <th>ISIN</th>
                      <th className="num">Current Holding</th>
                      <th className="num">Bonus Shares</th>
                      <th className="num">New Holding</th>
                      <th className="num">Δ Change</th>
                    </tr>
                  </thead>

                  <tbody>
                    {paginatedGroups.map((group) => {
                      const expanded = expandedActuals.has(group.actualCode);
                      return (
                        <React.Fragment key={group.actualCode}>
                          <tr
                            className="is-clickable is-group"
                            onClick={() =>
                              toggleActualExpand(group.actualCode)
                            }
                          >
                            <td>
                              <button
                                type="button"
                                className="bonus-actual-toggle"
                                aria-expanded={expanded}
                                onClick={(e) => {
                                  e.stopPropagation();
                                  toggleActualExpand(group.actualCode);
                                }}
                              >
                                <span
                                  className="bonus-actual-chevron"
                                  aria-hidden
                                >
                                  {expanded ? "▾" : "▸"}
                                </span>
                                <span translate="no">{group.actualCode}</span>
                                <span className="badge badge--neutral">
                                  {group.virtuals.length} virtual
                                  {group.virtuals.length === 1 ? "" : "s"}
                                </span>
                              </button>
                            </td>
                            <td translate="no">{group.isin}</td>
                            <td className="num">
                              {numberFormat.format(Math.floor(Number(group.currentHolding) || 0))}
                            </td>
                            <td className="num">
                              {numberFormat.format(Math.floor(Number(group.bonusShares) || 0))}
                            </td>
                            <td className="num">
                              {numberFormat.format(Math.floor(Number(group.newHolding) || 0))}
                            </td>
                            <td className="num positive">
                              +{numberFormat.format(Math.floor(Number(group.delta) || 0))}
                            </td>
                          </tr>

                          {expanded &&
                            group.virtuals.map((row) => (
                              <tr
                                key={`${group.actualCode}-${row.accountCode}`}
                                className="is-child"
                              >
                                <td className="indent">
                                  <span className="badge badge--brand bonus-virtual-label">
                                    Virtual
                                  </span>
                                  <span translate="no">{row.accountCode}</span>
                                </td>
                                <td translate="no">{row.isin}</td>
                                <td className="num">
                                  {numberFormat.format(Math.floor(Number(row.currentHolding) || 0))}
                                </td>
                                <td className="num">{numberFormat.format(Number(row.bonusShares) || 0)}</td>
                                <td className="num">
                                  {numberFormat.format(Math.floor(Number(row.newHolding) || 0))}
                                </td>
                                <td className="num positive">
                                  +{numberFormat.format(Number(row.delta) || 0)}
                                </td>
                              </tr>
                            ))}
                        </React.Fragment>
                      );
                    })}
                  </tbody>
                </table>
              </div>

              {totalPages > 1 && (
                <div className="pagination">
                  <button
                    disabled={page === 1}
                    onClick={() => setPage(page - 1)}
                  >
                    Prev
                  </button>
                  <span>
                    Page {page} of {totalPages}
                  </span>
                  <button
                    disabled={page === totalPages}
                    onClick={() => setPage(page + 1)}
                  >
                    Next
                  </button>
                </div>
              )}

              {exportError && (
                <div className="cm-verdict bad" role="alert">{exportError}</div>
              )}

              <div className="bonus-preview-actions">
                {/* EXPORT - generates report */}
                <button
                  type="button"
                  className="btn-export"
                  disabled={exportLoading}
                  aria-busy={exportLoading}
                  onClick={async () => {
                    try {
                      setExportLoading(true);
                      setExportError("");
                      setExportDownloadUrl("");

                      const params = new URLSearchParams({
                        isin,
                        ratio1,
                        ratio2,
                        recordDate,
                      });

                      const res = await fetch(
                        `${BASE_URL}/bonus/export-preview?${params.toString()}`,
                        { credentials: "include" }
                      );

                      if (!res.ok) throw new Error("Export request failed");

                      const data = await res.json();

                      if (!data.success) {
                        throw new Error(data.message || "Export failed");
                      }

                      const signedUrl =
                        data.downloadUrl?.signature?.signature ??
                        data.downloadUrl?.signature;

                      if (!signedUrl) {
                        throw new Error("Download URL missing");
                      }

                      setExportDownloadUrl(signedUrl);
                    } catch (err) {
                      console.error(err);
                      setExportError("Could not export the bonus preview. Try Export again.");
                    } finally {
                      setExportLoading(false);
                    }
                  }}
                >
                  {exportLoading ? "Generating…" : "Export"}
                </button>

                {/* DOWNLOAD - opens generated report */}
                <DownloadButton
                  disabled={!exportDownloadUrl}
                  onClick={() => {
                    if (exportDownloadUrl) {
                      window.open(exportDownloadUrl, "_blank");
                    }
                  }}
                />

                {/* APPLY BONUS */}
                <button
                  className="cm-primary"
                  disabled={applying || datesOutOfOrder}
                  onClick={async () => {
                    setApplying(true);
                    setError(null);
                    setApplyStatus("SUBMITTING");

                    try {
                      const res = await fetch(`${BASE_URL}/bonus/apply`, {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({
                          isin,
                          ratio1: Number(ratio1),
                          ratio2: Number(ratio2),
                          recordDate,
                          effectiveDate,
                          securityCode,
                          securityName,
                        }),
                      });

                      const data = await res.json();

                      if (!data.success) {
                        setError(data.message || "Failed to apply bonus");
                        setApplying(false);
                        setApplyStatus(null);
                        return;
                      }

                      setApplyJobName(data.jobName);
                      setApplyStatus(data.status);

                      // Bell notification: "in progress" until the background
                      // holdings rebuild has actually finished.
                      const bonusLabel = `Stock bonus — ${isin || securityCode || "security"}`;
                      if (data.alreadyRunning) {
                        reportJob({
                          id: `bonus-already-${Date.now()}`,
                          label: bonusLabel,
                          status: "completed",
                          message: "A bonus for this security and date is already being applied.",
                        });
                      } else if (data.catalystJobName) {
                        trackJob({
                          id: data.catalystJobName,
                          label: bonusLabel,
                          message: "Your bonus task has been submitted.",
                          statusUrl: `${BASE_URL}/bonus/apply-progress?key=${encodeURIComponent(
                            data.catalystJobName,
                          )}`,
                          parseStatus: parseBonusStatus,
                          timeoutMs: 45 * 60 * 1000,
                        });
                      }
                    } catch (err) {
                      setError("Something went wrong");
                      setApplying(false);
                      setApplyStatus(null);
                    }
                  }}
                >
                  {applying
                    ? applyStatus === "COMPLETED"
                      ? "Bonus Applied!"
                      : "Applying Bonus…"
                    : "Confirm & Apply Bonus"}
                </button>
              </div>
            </>
          )}
        </div>
      )}
      {step === "preview" && (
        <CustodianComparePanel
          caType="bonus"
          isin={isin}
          securityName={securityName}
          ratio1={ratio1}
          ratio2={ratio2}
          recordDate={recordDate}
        />
      )}
      </div>
    </MainLayout>
  );
}

export default BonusPage;
