import React, { useState, useEffect, useRef, useMemo } from "react";
import MainLayout from "../../layouts/MainLayout";
import "./DividendPage.css";
import "../TempTransactionUpload/tokens.css";
import "../TempTransactionUpload/TempTransaction.css";
import "../TempTransactionUpload/custody/CustodyMatch.css";
import "../../styles/ops-pages.css";
import { BASE_URL } from "../../constant";
import { useJobNotifications } from "../../notifications/JobNotificationContext";

/** Maps GET /dividend/apply-status → the notification system's vocabulary. */
function parseDividendStatus(d) {
  const s = String(d?.status ?? "").toUpperCase();
  if (s === "COMPLETED") {
    return {
      status: "completed",
      message: "The dividend has been applied to all affected accounts.",
    };
  }
  if (s === "FAILED" || s === "ERROR") {
    return {
      status: "failed",
      message: "The dividend could not be completed. Please try again.",
    };
  }
  return { status: "in_progress", message: null };
}

/**
 * Friendly labels for the per-row reconciliation status produced by
 * /api/dividend/preview when a custodian file is uploaded. Keys here must
 * match the `status` strings emitted by the backend's computeStatus().
 */
const STATUS_LABEL = {
  ready: "Matched",
  mismatch: "Mismatch",
  partial: "Partial",
  overpaid: "Over-paid",
  already_paid: "Already paid",
  missing_in_system: "Missing in system",
  missing_in_file: "Missing in file",
};

// Badge tone (styles/design-system.css) for each reconciliation status.
const STATUS_TONE = {
  ready: "ok",
  mismatch: "warn",
  partial: "warn",
  overpaid: "bad",
  already_paid: "info",
  missing_in_system: "bad",
  missing_in_file: "bad",
};

/** RFC 4180-style CSV cell (comma-safe). */
function csvEscapeCell(value) {
  const s = value == null ? "" : String(value);
  if (/[",\r\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function triggerCsvDownload(filename, csvBody) {
  const bom = "\uFEFF";
  const blob = new Blob([bom + csvBody], {
    type: "text/csv;charset=utf-8;",
  });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.rel = "noopener";
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

function DividendPage() {
  const [symbol, setSymbol] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [companyName, setCompanyName] = useState("");
  const [isin, setIsin] = useState("");
  const [dividendType, setDividendType] = useState("Interim");
  const [rate, setRate] = useState("");
  const [unit, setUnit] = useState("Per Share");
  const [exDate, setExDate] = useState("");
  const [recordDate, setRecordDate] = useState("");
  const [paymentDate, setPaymentDate] = useState("");

  const [securities, setSecurities] = useState([]);
  const [showDropdown, setShowDropdown] = useState(false);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(false);
  const [dividendList, setDividendList] = useState([]);

  /*
   * Step 1 — system report (holding on the record date × rate, per account),
   * shown by "Fetch Affected Accounts". previewFetched tells an empty report
   * apart from one not fetched yet.
   */
  const [previewData, setPreviewData] = useState([]);
  const [previewFetched, setPreviewFetched] = useState(false);
  const [previewEmptyMessage, setPreviewEmptyMessage] = useState("");
  const PAGE_SIZE = 10;
  const [page, setPage] = useState(1);
  const [systemPage, setSystemPage] = useState(1);
  const [comparing, setComparing] = useState(false);

  /* ===========================
     CUSTODIAN-FILE RECONCILIATION STATE
     - reconEvents : array of events from /preview when a file is uploaded
     - reconSummary: counts + totals across all events
     - statusFilter: which status bucket the user is filtering on
     - activeEvent : index of selected event tab (for multi-rate dividends)
     - warnings    : soft warnings returned by the backend
     =========================== */
  const [reconEvents, setReconEvents] = useState(null);
  const [reconSummary, setReconSummary] = useState(null);
  const [statusFilter, setStatusFilter] = useState("all");
  const [activeEvent, setActiveEvent] = useState(0);
  const [warnings, setWarnings] = useState([]);

  const [applyJobName, setApplyJobName] = useState(null);
  const [applyStatus, setApplyStatus] = useState(null);

  /* The newest file from Corporate Actions -> Custodian Upload: { file, rowCount } for this ISIN. */
  const [custodianInfo, setCustodianInfo] = useState(null);
  const [custodianError, setCustodianError] = useState("");

  const dropdownRef = useRef(null);

  const { trackJob } = useJobNotifications();

  useEffect(() => setPage(1), [reconEvents, statusFilter, activeEvent]);
  useEffect(() => setSystemPage(1), [previewData]);

  /* Drill-down: which actual-account groups are currently expanded. */
  const [expandedKeys, setExpandedKeys] = useState(() => new Set());
  const toggleGroup = (key) =>
    setExpandedKeys((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });

  /* Currently selected event's reconciled rows. */
  const activeEventObj =
    reconEvents && reconEvents.length > 0
      ? reconEvents[Math.min(activeEvent, reconEvents.length - 1)]
      : null;

  /*
   * Aggregate the active event's rows into one group per actual account.
   * Grouping key = actualCode || accountCode, so virtual-code rows (whose
   * actualCode resolves to the real account) and rows booked directly under an
   * actual code (actualCode === "" → keyed by their own accountCode) fold into
   * the same group. Numeric columns are summed across children; rate is
   * per-share so it is shown once (or "—" if children disagree). Deltas are
   * derived from the group totals. Grouping is display-only — the raw child
   * rows are preserved for the drill-down, export and apply.
   */
  const roundTo2 = (n) => Math.round((Number(n) || 0) * 100) / 100;

  const systemTotalPages = Math.max(1, Math.ceil(previewData.length / PAGE_SIZE));
  const paginatedSystemRows = previewData.slice(
    (systemPage - 1) * PAGE_SIZE,
    systemPage * PAGE_SIZE,
  );
  const systemTotalAmount = roundTo2(
    previewData.reduce((sum, row) => sum + (Number(row.dividendAmount) || 0), 0),
  );

  const groupedReconRows = useMemo(() => {
    const rows = activeEventObj?.rows || [];
    const groups = new Map();
    for (const row of rows) {
      const key = String(row.actualCode || "").trim() || row.accountCode;
      let g = groups.get(key);
      if (!g) {
        g = {
          key,
          actualCode: String(row.actualCode || "").trim(),
          clientName: row.clientName || "",
          holdingSys: 0,
          holdingFile: 0,
          grossSys: 0,
          grossFile: 0,
          alreadyReceivedGross: 0,
          toReceiveNet: 0,
          rateSys: row.rateSys,
          rateFile: row.rateFile,
          rateSysMixed: false,
          rateFileMixed: false,
          statusCounts: {},
          children: [],
        };
        groups.set(key, g);
      }
      if (!g.actualCode && row.actualCode)
        g.actualCode = String(row.actualCode).trim();
      if (!g.clientName && row.clientName) g.clientName = row.clientName;
      g.holdingSys += Number(row.holdingSys) || 0;
      g.holdingFile += Number(row.holdingFile) || 0;
      g.grossSys += Number(row.grossSys) || 0;
      g.grossFile += Number(row.grossFile) || 0;
      g.alreadyReceivedGross += Number(row.alreadyReceivedGross) || 0;
      g.toReceiveNet += Number(row.toReceiveNet) || 0;
      if (row.rateSys !== g.rateSys) g.rateSysMixed = true;
      if (row.rateFile !== g.rateFile) g.rateFileMixed = true;
      g.statusCounts[row.status] = (g.statusCounts[row.status] || 0) + 1;
      g.children.push(row);
    }
    for (const g of groups.values()) {
      g.holdingSys = roundTo2(g.holdingSys);
      g.holdingFile = roundTo2(g.holdingFile);
      g.grossSys = roundTo2(g.grossSys);
      g.grossFile = roundTo2(g.grossFile);
      g.alreadyReceivedGross = roundTo2(g.alreadyReceivedGross);
      g.toReceiveNet = roundTo2(g.toReceiveNet);
      g.holdingDelta = roundTo2(g.holdingFile - g.holdingSys);
      g.grossDelta = roundTo2(g.grossFile - g.grossSys);
    }
    return [...groups.values()];
  }, [activeEventObj]);

  /*
   * Status filter: keep a whole group (all children, full totals) whenever at
   * least one of its children matches the active status chip.
   */
  const filteredGroups =
    statusFilter === "all"
      ? groupedReconRows
      : groupedReconRows.filter((g) => (g.statusCounts[statusFilter] || 0) > 0);

  const reconTotalPages = Math.max(
    1,
    Math.ceil(filteredGroups.length / PAGE_SIZE),
  );
  const paginatedGroups = filteredGroups.slice(
    (page - 1) * PAGE_SIZE,
    page * PAGE_SIZE,
  );

  useEffect(() => {
    fetchAllSecurities();
  }, []);

  // Which uploaded custodian file holds this dividend (ISIN + record date), once both are picked.
  useEffect(() => {
    setCustodianInfo(null);
    setCustodianError("");
    if (!isin || !recordDate) return undefined;

    let cancelled = false;
    const params = new URLSearchParams({ kind: "entitlement", caType: "dividend", isin, recordDate });
    fetch(`${BASE_URL}/custodian-upload/latest?${params.toString()}`)
      .then(async (res) => {
        const body = await res.json().catch(() => ({}));
        if (cancelled) return;
        if (!res.ok || !body.success) setCustodianError(body.message || "Could not read the custodian file.");
        else setCustodianInfo(body);
      })
      .catch(() => !cancelled && setCustodianError("Could not reach the server. Check the backend is running."));
    return () => {
      cancelled = true;
    };
  }, [isin, recordDate]);

  useEffect(() => {
    const handleClickOutside = (e) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target)) {
        setShowDropdown(false);
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  /* ===========================
     POLL DIVIDEND APPLY JOB STATUS
     =========================== */
  useEffect(() => {
    if (!applyJobName || !loading) return;

    const terminalStatuses = ["COMPLETED", "FAILED", "ERROR"];
    if (terminalStatuses.includes(applyStatus)) return;

    const interval = setInterval(async () => {
      try {
        const res = await fetch(
          `${BASE_URL}/dividend/apply-status?jobName=${encodeURIComponent(applyJobName)}`,
          { credentials: "include" },
        );
        const data = await res.json();
        if (!data.success) return;

        setApplyStatus(data.status);

        if (data.status === "COMPLETED") {
          clearInterval(interval);

          const entry = {
            securityCode: symbol,
            securityName: companyName,
            isin,
            dividendType,
            rate: rate || "-",
            unit,
            exDate,
            recordDate: recordDate || "-",
            paymentDate: paymentDate || "-",
          };
          setDividendList((prev) => [entry, ...prev]);
          setSuccess(true);

          setTimeout(() => {
            setSuccess(false);
            setPreviewData([]);
            setPreviewFetched(false);
            setPreviewEmptyMessage("");
            setReconEvents(null);
            setReconSummary(null);
            setWarnings([]);
            setStatusFilter("all");
            setActiveEvent(0);
            setSymbol("");
            setSearchQuery("");
            setCompanyName("");
            setIsin("");
            setRate("");
            setExDate("");
            setRecordDate("");
            setPaymentDate("");
            setLoading(false);
            setApplyJobName(null);
            setApplyStatus(null);
          }, 3000);
        } else if (data.status === "FAILED" || data.status === "ERROR") {
          clearInterval(interval);
          setError("Dividend application failed. Please try again.");
          setLoading(false);
          setApplyJobName(null);
          setApplyStatus(null);
        }
      } catch {
        /* ignore polling errors */
      }
    }, 10000);

    return () => clearInterval(interval);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [applyJobName, loading, applyStatus]);

  const fetchAllSecurities = async () => {
    try {
      const res = await fetch(`${BASE_URL}/dividend/getAllSecuritiesList`);
      const data = await res.json();
      if (data.success) setSecurities(data.data || []);
    } catch (err) {
      console.error("Failed to fetch securities", err);
    }
  };

  const filteredSecurities = securities.filter(
    (sec) =>
      sec.securityCode?.toLowerCase().includes(searchQuery.toLowerCase()) ||
      sec.securityName?.toLowerCase().includes(searchQuery.toLowerCase()) ||
      sec.isin?.toLowerCase().includes(searchQuery.toLowerCase()),
  );

  const handleSelectSecurity = (sec) => {
    setSymbol(sec.securityCode || "");
    setSearchQuery(sec.securityCode || sec.isin || "");
    setCompanyName(sec.securityName || "");
    setIsin(sec.isin || "");
    setShowDropdown(false);
  };

  const fetchPreview = async () => {
    setError(null);
    setPreviewEmptyMessage("");
    setReconEvents(null);
    setReconSummary(null);
    setPreviewData([]);
    setWarnings([]);
    setStatusFilter("all");
    setActiveEvent(0);
    /* Record Date is used for calculation (holdings as on record date); Ex-Date kept for UI/display */
    if (!isin || !recordDate || !rate || Number(rate) <= 0 || !paymentDate) {
      setError("ISIN, Record Date, Dividend Rate and Payment Date are required for preview.");
      return;
    }

    setPreviewFetched(false);
    setPreviewLoading(true);
    try {
      const data = await postPreview(false);
      if (data.success) {
        setPreviewData(data.data || []);
        setPreviewFetched(true);
      } else {
        setError(data.message || "Preview failed");
      }
    } catch (err) {
      console.error(err);
      setError("Failed to load preview");
    } finally {
      setPreviewLoading(false);
    }
  };

  /*
   * Step 2: compare the system report with the custodian file. This is the
   * only path to Apply — dividends are never applied from the system alone.
   */
  const runCompare = async () => {
    setError(null);
    setPreviewEmptyMessage("");
    setReconEvents(null);
    setReconSummary(null);
    setWarnings([]);
    setStatusFilter("all");
    setActiveEvent(0);

    setComparing(true);
    try {
      const data = await postPreview(true);
      if (data.success && Array.isArray(data.events)) {
        setReconEvents(data.events);
        setReconSummary(data.summary || null);
        setWarnings(Array.isArray(data.warnings) ? data.warnings : []);
        if (!data.events.length) {
          setPreviewEmptyMessage(
            "No accounts found in either system or custodian file.",
          );
        }
      } else {
        setError(data.message || "Comparison failed");
      }
    } catch (err) {
      console.error(err);
      setError("Failed to compare with the custodian file");
    } finally {
      setComparing(false);
    }
  };

  const postPreview = async (compare) => {
    const res = await fetch(`${BASE_URL}/dividend/preview`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        isin,
        recordDate,
        rate: Number(rate),
        paymentDate,
        compare,
      }),
    });
    return res.json();
  };

  /** System report CSV: the rows shown in the Dividend Preview table. */
  const handleExportSystemCsv = () => {
    const header = [
      "Account_Code",
      "Holding_Record_Date",
      "Dividend_Rate",
      "Payment_Date",
      "Dividend_Amount",
    ];
    const lines = [header.join(",")];
    for (const row of previewData) {
      lines.push(
        [
          row.accountCode,
          row.holdingAsOnRecordDate,
          row.rate,
          row.paymentDate,
          row.dividendAmount,
        ]
          .map(csvEscapeCell)
          .join(","),
      );
    }
    const safeIsin = String(isin || "isin").replace(/[^\w.-]+/g, "_");
    const safeDate = String(recordDate || "record").replace(/[^\d-]+/g, "");
    triggerCsvDownload(
      `Dividend_preview_${safeIsin}_${safeDate}.csv`,
      lines.join("\r\n"),
    );
  };

  /**
   * Single Export: reconciliation CSV aligned with the on-screen grid
   * (all CA events, all rows, all statuses; one Status column). Omits delta
   * columns. CA_Ref / event Rate omitted per product request.
   */
  const handleExportReconciliationCsv = () => {
    if (!reconEvents || reconEvents.length === 0) {
      setError(
        "Fetch affected accounts first — there is no reconciliation data to export.",
      );
      return;
    }
    const totalRows = reconEvents.reduce(
      (n, evt) => n + (Array.isArray(evt.rows) ? evt.rows.length : 0),
      0,
    );
    if (totalRows === 0) {
      setError("No reconciliation rows to export.");
      return;
    }
    setError(null);

    const header = [
      "Account_Code",
      "Client_Name",
      "Holding_PMS",
      "Holding_File",
      "Rate_PMS",
      "Rate_File",
      "Gross_PMS",
      "Gross_File",
      "Already_Received",
      "Cash_Credited",
      "To_Receive",
      "Status",
    ];
    const lines = [header.map(csvEscapeCell).join(",")];

    for (const evt of reconEvents) {
      const rows = Array.isArray(evt.rows) ? evt.rows : [];
      for (const row of rows) {
        if (!row) continue;
        const statusLabel = STATUS_LABEL[row.status] || row.status || "";
        lines.push(
          [
            row.accountCode ?? "",
            row.clientName ?? "",
            row.holdingSys ?? "",
            row.holdingFile ?? "",
            row.rateSys ?? "",
            row.rateFile ?? "",
            row.grossSys ?? "",
            row.grossFile ?? "",
            row.alreadyReceivedGross ?? "",
            row.alreadyCreditedCash ? "Yes" : "No",
            row.toReceiveNet ?? "",
            statusLabel,
          ]
            .map(csvEscapeCell)
            .join(","),
        );
      }
    }

    const safeIsin = String(isin || "isin").replace(/[^\w.-]+/g, "_");
    const safeDate = String(recordDate || "record").replace(/[^\d-]+/g, "");
    const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
    triggerCsvDownload(
      `Dividend_recon_${safeIsin}_${safeDate}_${stamp}.csv`,
      lines.join("\r\n"),
    );
  };

  /*
   * Apply dividend (called from preview section after fetch).
   *
   * Submits a Catalyst Job via the AppSail controller. The controller returns
   * { jobName, status: "PENDING" } immediately; the polling useEffect above
   * then polls /dividend/apply-status until COMPLETED / FAILED / ERROR.
   * `loading` stays true for the entire duration so the button shows "Applying…".
   *
   * mode:
   *   "system"  – legacy behaviour, no account filter, FIFO universe.
   *   "matched" – send only accounts with status === "ready".
   *   "all"     – send every reconciled account except already_paid /
   *               missing_in_system (those are unsafe to auto-apply).
   *
   * accountCodes: optional explicit allow-list. When omitted the handler
   *   derives it from the active reconciliation event using `mode`.
   */
  const handleApply = async (mode = "system", accountCodes = null) => {
    setError(null);
    if (!symbol || !companyName || !recordDate || !paymentDate) {
      setError("Security Code, Security Name, Record Date and Payment Date are required.");
      return;
    }
    if (!rate || Number(rate) <= 0) {
      setError("Dividend Rate is required and must be greater than 0.");
      return;
    }

    let codesToSend = accountCodes;
    if (!codesToSend && mode !== "system" && activeEventObj) {
      const safeStatuses =
        mode === "matched"
          ? new Set(["ready"])
          : new Set(["ready", "mismatch", "partial", "overpaid", "missing_in_file"]);
      codesToSend = activeEventObj.rows
        .filter((r) => safeStatuses.has(r.status))
        .map((r) => r.accountCode)
        .filter(Boolean);
      if (!codesToSend.length) {
        setError("No accounts to apply for the selected mode.");
        return;
      }
    }

    const payload = {
      isin,
      securityCode: symbol,
      securityName: companyName,
      rate: Number(rate),
      exDate,
      recordDate: recordDate || "",
      paymentDate: paymentDate || "",
      dividendType,
      applyMode: mode,
    };
    if (codesToSend && codesToSend.length) {
      payload.accountCodes = codesToSend;
    }

    setLoading(true);
    setApplyStatus(null);
    setApplyJobName(null);
    try {
      const res = await fetch(`${BASE_URL}/dividend/apply`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const data = await res.json();
      if (!data.success || !data.jobName) {
        setError(data.message || "Failed to apply dividend");
        setLoading(false);
        return;
      }
      setApplyJobName(data.jobName);
      setApplyStatus(data.status || "PENDING");

      // Bell notification: "in progress" until the background job finishes.
      // (UpdateDividendData is one self-contained job — no rebuild fan-out — so
      // the existing apply-status endpoint already reports the true end state.)
      trackJob({
        id: data.jobName,
        label: `Dividend — ${symbol || isin || "security"}`,
        message: "Your dividend task has been submitted.",
        statusUrl: `${BASE_URL}/dividend/apply-status?jobName=${encodeURIComponent(
          data.jobName,
        )}`,
        parseStatus: parseDividendStatus,
        timeoutMs: 45 * 60 * 1000,
      });
    } catch (err) {
      console.error(err);
      setError("Failed to apply dividend");
      setLoading(false);
    }
  };

  /*
   * Confirmation modal state for the two reconciliation Apply buttons.
   * confirmApply is null when no dialog is open, or { mode, count, gross }.
   */
  const [confirmApply, setConfirmApply] = useState(null);

  const requestApply = (mode) => {
    if (!activeEventObj) {
      setError("No reconciliation data — fetch affected accounts first.");
      return;
    }
    const matchedRows = activeEventObj.rows.filter((r) => r.status === "ready");
    const safeAllRows = activeEventObj.rows.filter((r) =>
      ["ready", "mismatch", "partial", "overpaid", "missing_in_file"].includes(r.status),
    );
    const rows = mode === "matched" ? matchedRows : safeAllRows;
    const gross = rows.reduce((s, r) => s + (Number(r.grossSys) || 0), 0);
    setConfirmApply({ mode, count: rows.length, gross });
  };

  const closeConfirm = () => setConfirmApply(null);
  const confirmAndApply = () => {
    if (!confirmApply) return;
    const { mode } = confirmApply;
    setConfirmApply(null);
    handleApply(mode);
  };

  return (
    <MainLayout title="Dividend">
      <div className="temp-transaction-container txn-ops ops-page">
      <div className="temp-transaction-card">
        <h3 className="ops-card-title">Add Dividend</h3>

        {success && (
          <div className="cm-verdict ok">Dividend applied successfully.</div>
        )}
        {error && <div className="cm-verdict bad">{error}</div>}
        {previewEmptyMessage && (
          <div className="cm-verdict warn">{previewEmptyMessage}</div>
        )}

        <form className="dividend-card" onSubmit={(e) => e.preventDefault()}>
          <div className="account-code-search" ref={dropdownRef}>
            <label className="search-label">Security Code</label>
            <div className="search-input-wrapper">
              <span className="search-icon">🔍</span>
              <input
                type="text"
                className="search-input"
                placeholder="Search security code or company..."
                value={searchQuery}
                onChange={(e) => {
                  setSearchQuery(e.target.value);
                  setShowDropdown(true);
                  if (!e.target.value) {
                    setSymbol("");
                    setCompanyName("");
                    setIsin("");
                  }
                }}
                onFocus={() => setShowDropdown(true)}
              />
              {searchQuery && (
                <span
                  className="clear-icon"
                  onClick={() => {
                    setSearchQuery("");
                    setSymbol("");
                    setCompanyName("");
                    setIsin("");
                  }}
                >
                  ✕
                </span>
              )}
              <span className="arrow-icon">▾</span>
            </div>
            {showDropdown && filteredSecurities.length > 0 && (
              <div className="search-dropdown">
                <div className="dropdown-header">Search Security Code</div>
                <div className="dropdown-options">
                  {filteredSecurities.map((sec) => (
                    <div
                      key={sec.isin || sec.securityCode}
                      className="dropdown-option"
                      onClick={() => handleSelectSecurity(sec)}
                    >
                      <strong>{sec.securityCode || sec.isin}</strong>
                      <div className="ops-option-sub">
                        {sec.securityName}
                      </div>
                    </div>
                  ))}
                </div>
                <div className="dropdown-footer">
                  {filteredSecurities.length} of {securities.length} securities
                </div>
              </div>
            )}
          </div>

          <div className="dividend-field">
            <label>ISIN</label>
            <input value={isin} disabled />
          </div>

          <div className="dividend-field">
            <label>Security Name</label>
            <input value={companyName} disabled />
          </div>

          <div className="dividend-field">
            <label>Dividend Type</label>
            <select
              value={dividendType}
              onChange={(e) => setDividendType(e.target.value)}
              className="dividend-select"
            >
              <option value="Interim">Interim</option>
              <option value="Special">Special</option>
              <option value="Final">Final</option>
              <option value="Interest">Interest</option>
            </select>
          </div>

          <div className="dividend-field">
            <label>Dividend Rate</label>
            <input
              type="number"
              step="any"
              placeholder="0"
              value={rate}
              onChange={(e) => setRate(e.target.value)}
            />
          </div>

          <div className="dividend-field">
            <label>Unit</label>
            <select
              value={unit}
              onChange={(e) => setUnit(e.target.value)}
              className="dividend-select"
            >
              <option value="Per Share">Per Share</option>
              <option value="Per Unit">Per Unit</option>
              <option value="Per Unit/Interest">Per Unit/Interest</option>
            </select>
          </div>

          <div className="dividend-field">
            <label>Ex-Date</label>
            <input
              type="date"
              value={exDate}
              onChange={(e) => setExDate(e.target.value)}
            />
          </div>

          <div className="dividend-field">
            <label>Record Date <span className="required-asterisk">*</span></label>
            <input
              type="date"
              value={recordDate}
              onChange={(e) => setRecordDate(e.target.value)}
              title="Used for eligibility calculation (holdings as on this date)"
            />
          </div>

          <div className="dividend-field">
            <label>Payment Date <span className="required-asterisk">*</span></label>
            <input
              type="date"
              value={paymentDate}
              onChange={(e) => setPaymentDate(e.target.value)}
              required
            />
          </div>

          {/* The custodian file is uploaded once on Corporate Actions -> Custodian Upload. */}
          {isin && (
            <div className="dividend-field dividend-field-full">
              {custodianError && <div className="cm-verdict bad">{custodianError}</div>}
              {custodianInfo && (
                <div className={custodianInfo.rowCount ? "cm-verdict ok" : "cm-verdict warn"}>
                  Custodian file fetched: <strong>{custodianInfo.file.fileName}</strong> · report date{" "}
                  {custodianInfo.file.businessDate} · uploaded {new Date(custodianInfo.file.timeMs).toLocaleString()} ·{" "}
                  {custodianInfo.rowCount} Dividend In Cash row(s) for <span translate="no">{isin}</span> with Record
                  Date {recordDate}
                </div>
              )}
            </div>
          )}

          <div className="dividend-actions">
            <button
              type="button"
              className="cm-primary"
              disabled={
                !isin ||
                !recordDate ||
                !rate ||
                Number(rate) <= 0 ||
                !paymentDate ||
                previewLoading
              }
              onClick={fetchPreview}
            >
              {previewLoading ? "Fetching…" : "Fetch Affected Accounts"}
            </button>
          </div>
        </form>
      </div>

      {/* ============================================================
          STEP 1 — SYSTEM REPORT. Holding on the record date × rate per
          account, from the system alone. It cannot be applied from here:
          Compare first, then apply from the reconciliation grid.
          ============================================================ */}
      {previewFetched && (
        <div className="temp-transaction-card dividend-preview-card">
          <div className="dividend-recon-header">
            <h3 className="ops-card-title">Dividend Preview</h3>
          </div>

          <div className="data-table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Account Code</th>
                  <th className="num">Holding (Record Date)</th>
                  <th className="num">Dividend Rate</th>
                  <th>Payment Date</th>
                  <th className="num">Dividend Amount</th>
                </tr>
              </thead>
              <tbody>
                {previewData.length === 0 && (
                  <tr>
                    <td colSpan={5} className="empty-cell">
                      No accounts hold this ISIN on the record date.
                    </td>
                  </tr>
                )}
                {paginatedSystemRows.map((row) => (
                  <tr key={row.accountCode}>
                    <td className="strong">{row.accountCode}</td>
                    <td className="num">{row.holdingAsOnRecordDate}</td>
                    <td className="num">{row.rate}</td>
                    <td>{row.paymentDate ?? "—"}</td>
                    <td className="num strong">{row.dividendAmount}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {previewData.length > 0 && (
            <p className="ops-upload-hint">
              {previewData.length} account(s) · total dividend ₹{systemTotalAmount}
            </p>
          )}

          {systemTotalPages > 1 && (
            <div className="dividend-pagination">
              <button
                type="button"
                disabled={systemPage === 1}
                onClick={() => setSystemPage(systemPage - 1)}
              >
                Prev
              </button>
              <span>
                Page {systemPage} of {systemTotalPages}
              </span>
              <button
                type="button"
                disabled={systemPage === systemTotalPages}
                onClick={() => setSystemPage(systemPage + 1)}
              >
                Next
              </button>
            </div>
          )}

          <div className="dividend-preview-actions">
            <button
              type="button"
              className="btn-export"
              disabled={previewData.length === 0}
              onClick={handleExportSystemCsv}
            >
              Export
            </button>
            <button
              type="button"
              className="cm-primary"
              disabled={!custodianInfo || comparing}
              title={
                !custodianInfo
                  ? custodianError ||
                    (recordDate
                      ? "Upload the custodian file on Corporate Actions → Custodian Upload first."
                      : "Enter the record date first.")
                  : ""
              }
              onClick={runCompare}
            >
              {comparing ? "Comparing…" : "Compare with Custodian File"}
            </button>
          </div>
        </div>
      )}

      {/* ============================================================
          STEP 2 — RECONCILIATION GRID, shown after "Compare with Custodian
          File". The only place a dividend can be applied from: we never
          apply dividends without the custodian's confirmation.
          ============================================================ */}
      {reconEvents && (
        <div className="temp-transaction-card dividend-preview-card">
          <div className="dividend-recon-header">
            <h3 className="ops-card-title">Custodian Reconciliation</h3>
          </div>

          {warnings.length > 0 && (
            <div className="cm-verdict warn">
              {warnings.map((w, i) => (
                <div key={i}>• {w}</div>
              ))}
            </div>
          )}

          {/* Event tabs (only when >1 event in the file) */}
          {reconEvents.length > 1 && (
            <div className="dividend-recon-tabs">
              {reconEvents.map((evt, idx) => (
                <button
                  key={`${evt.caRef || "-"}-${evt.rate}-${idx}`}
                  type="button"
                  className={
                    idx === activeEvent
                      ? "recon-tab recon-tab-active"
                      : "recon-tab"
                  }
                  onClick={() => {
                    setActiveEvent(idx);
                    setStatusFilter("all");
                  }}
                >
                  {evt.caRef ? `${evt.caRef} · ` : ""}₹{evt.rate}/share ·{" "}
                  {evt.rowCount} acc.
                </button>
              ))}
            </div>
          )}

          {/* Status filter chips */}
          {activeEventObj && reconSummary && (
            <div className="dividend-recon-filters">
              {[
                { key: "all", label: "All", count: activeEventObj.rowCount },
                {
                  key: "ready",
                  label: "Matched",
                  count: activeEventObj.rows.filter((r) => r.status === "ready").length,
                },
                {
                  key: "mismatch",
                  label: "Mismatch",
                  count: activeEventObj.rows.filter((r) => r.status === "mismatch").length,
                },
                {
                  key: "partial",
                  label: "Partial",
                  count: activeEventObj.rows.filter((r) => r.status === "partial").length,
                },
                {
                  key: "overpaid",
                  label: "Over-paid",
                  count: activeEventObj.rows.filter((r) => r.status === "overpaid").length,
                },
                {
                  key: "already_paid",
                  label: "Already paid",
                  count: activeEventObj.rows.filter((r) => r.status === "already_paid").length,
                },
                {
                  key: "missing_in_system",
                  label: "Missing in system",
                  count: activeEventObj.rows.filter((r) => r.status === "missing_in_system").length,
                },
                {
                  key: "missing_in_file",
                  label: "Missing in file",
                  count: activeEventObj.rows.filter((r) => r.status === "missing_in_file").length,
                },
              ]
                .filter((c) => c.key === "all" || c.count > 0)
                .map((c) => (
                  <button
                    key={c.key}
                    type="button"
                    className={
                      statusFilter === c.key
                        ? "recon-chip recon-chip-active"
                        : "recon-chip"
                    }
                    onClick={() => setStatusFilter(c.key)}
                  >
                    {c.label} ({c.count})
                  </button>
                ))}
            </div>
          )}

          <div className="data-table-wrap dividend-recon-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Account</th>
                  <th className="num">Holding<br />PMS / File / Diff</th>
                  <th className="num">Rate<br />PMS / File</th>
                  <th className="num">Gross<br />PMS / File / Diff</th>
                  <th className="num">Already<br />Received</th>
                  <th className="num">To Receive</th>
                  <th className="status">Status</th>
                </tr>
              </thead>
              <tbody>
                {paginatedGroups.length === 0 && (
                  <tr>
                    <td colSpan={7} className="empty-cell">
                      No rows in this filter.
                    </td>
                  </tr>
                )}
                {paginatedGroups.map((group) => {
                  const expanded = expandedKeys.has(group.key);
                  const deltaClass = (d) =>
                    d === 0 ? "muted" : d > 0 ? "positive" : "negative";
                  return (
                    <React.Fragment key={group.key}>
                      <tr
                        className="is-clickable is-group"
                        role="button"
                        onClick={() => toggleGroup(group.key)}
                        title={group.clientName || ""}
                      >
                        <td>
                          <div>
                            <span className="dividend-expand-toggle">
                              {expanded ? "▾" : "▸"}
                            </span>
                            {group.actualCode || group.key} (
                            {group.children.length})
                          </div>
                          {group.clientName && (
                            <div className="recon-account-subtitle">
                              {group.clientName}
                            </div>
                          )}
                        </td>
                        <td className="num">
                          {group.holdingSys} / {group.holdingFile} /{" "}
                          <span className={deltaClass(group.holdingDelta)}>
                            {group.holdingDelta > 0 ? "+" : ""}
                            {group.holdingDelta || 0}
                          </span>
                        </td>
                        <td className="num">
                          {group.rateSysMixed ? "—" : (group.rateSys ?? "—")} /{" "}
                          {group.rateFileMixed ? "—" : (group.rateFile ?? "—")}
                        </td>
                        <td className="num">
                          {group.grossSys} / {group.grossFile} /{" "}
                          <span className={deltaClass(group.grossDelta)}>
                            {group.grossDelta > 0 ? "+" : ""}
                            {group.grossDelta || 0}
                          </span>
                        </td>
                        <td className="num">{group.alreadyReceivedGross || 0}</td>
                        <td className="num">{group.toReceiveNet}</td>
                        <td className="status">
                          <span className="recon-status-breakdown">
                            {Object.entries(group.statusCounts).map(
                              ([status, count]) => (
                                <span
                                  key={status}
                                  className={`badge badge--${STATUS_TONE[status] || "neutral"}`}
                                >
                                  {count} {STATUS_LABEL[status] || status}
                                </span>
                              ),
                            )}
                          </span>
                        </td>
                      </tr>
                      {expanded &&
                        group.children.map((row) => (
                          <tr
                            key={`${group.key}::${row.accountCode}`}
                            className={`is-child recon-row-${row.status}`}
                            title={row.clientName || ""}
                          >
                            <td className="indent">
                              <div className="strong">
                                {row.accountCode}
                              </div>
                              {row.clientName && (
                                <div className="recon-account-subtitle">
                                  {row.clientName}
                                </div>
                              )}
                            </td>
                            <td className="num">
                              {row.holdingSys ?? "—"} / {row.holdingFile ?? "—"}{" "}
                              /{" "}
                              <span className={deltaClass(row.holdingDelta)}>
                                {row.holdingDelta > 0 ? "+" : ""}
                                {row.holdingDelta || 0}
                              </span>
                            </td>
                            <td className="num">
                              {row.rateSys ?? "—"} / {row.rateFile ?? "—"}
                            </td>
                            <td className="num">
                              {row.grossSys ?? "—"} / {row.grossFile ?? "—"} /{" "}
                              <span className={deltaClass(row.grossDelta)}>
                                {row.grossDelta > 0 ? "+" : ""}
                                {row.grossDelta || 0}
                              </span>
                            </td>
                            <td
                              className="num"
                              title={
                                row.alreadyCreditedCash
                                  ? "DIVIDEND cash credit recorded for this record date"
                                  : undefined
                              }
                            >
                              {row.alreadyReceivedGross || 0}
                            </td>
                            <td className="num strong">
                              {row.toReceiveNet}
                            </td>
                            <td className="status">
                              <span
                                className={`badge badge--${STATUS_TONE[row.status] || "neutral"}`}
                              >
                                {STATUS_LABEL[row.status] || row.status}
                              </span>
                            </td>
                          </tr>
                        ))}
                    </React.Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>

          {reconTotalPages > 1 && (
            <div className="dividend-pagination">
              <button
                type="button"
                disabled={page === 1}
                onClick={() => setPage(1)}
              >
                First
              </button>
              <button
                type="button"
                disabled={page === 1}
                onClick={() => setPage(page - 1)}
              >
                Prev
              </button>
              <span>
                Page {page} of {reconTotalPages}
              </span>
              <button
                type="button"
                disabled={page === reconTotalPages}
                onClick={() => setPage(page + 1)}
              >
                Next
              </button>
              <button
                type="button"
                disabled={page === reconTotalPages}
                onClick={() => setPage(reconTotalPages)}
              >
                Last
              </button>
            </div>
          )}

          {success && (
            <div className="cm-verdict ok">Dividend applied successfully.</div>
          )}

          <div className="dividend-preview-actions">
            <button
              type="button"
              className="btn-export"
              disabled={
                !reconEvents ||
                reconEvents.length === 0 ||
                reconEvents.every(
                  (e) => !Array.isArray(e.rows) || e.rows.length === 0,
                )
              }
              onClick={handleExportReconciliationCsv}
              title="Download reconciliation CSV (same rows as the grid; delta columns omitted)."
            >
              Export
            </button>

            {/*
             * v2 Apply controls:
             *   Primary  – Apply Matched Only (status === "ready")
             *   Secondary – Apply All Reconciled (everything except
             *               already_paid / missing_in_system)
             */}
            {(() => {
              const matchedRows = activeEventObj
                ? activeEventObj.rows.filter((r) => r.status === "ready")
                : [];
              const safeAllRows = activeEventObj
                ? activeEventObj.rows.filter((r) =>
                    [
                      "ready",
                      "mismatch",
                      "partial",
                      "overpaid",
                      "missing_in_file",
                    ].includes(r.status),
                  )
                : [];
              return (
                <>
                  <button
                    type="button"
                    className="cm-primary"
                    disabled={loading || matchedRows.length === 0}
                    onClick={() => requestApply("matched")}
                    title={
                      matchedRows.length === 0
                        ? "No matched rows in this event."
                        : `Apply ${matchedRows.length} matched row(s).`
                    }
                  >
                    {loading
                      ? "Applying…"
                      : `Apply Matched Only (${matchedRows.length})`}
                  </button>
                  <button
                    type="button"
                    className="cm-primary cm-primary--warn"
                    disabled={loading || safeAllRows.length === 0}
                    onClick={() => requestApply("all")}
                    title={
                      safeAllRows.length === 0
                        ? "No reconcilable rows in this event."
                        : `Apply ${safeAllRows.length} row(s) including mismatches.`
                    }
                  >
                    {loading
                      ? "Applying…"
                      : `Apply All Reconciled (${safeAllRows.length})`}
                  </button>
                </>
              );
            })()}
          </div>
        </div>
      )}

      {/* Confirmation modal for Apply Matched Only / Apply All Reconciled */}
      {confirmApply && (
        <div className="dividend-modal-backdrop" onClick={closeConfirm}>
          <div
            className="dividend-modal"
            onClick={(e) => e.stopPropagation()}
          >
            <h3 className="ops-card-title">
              {confirmApply.mode === "matched"
                ? "Apply Matched Only?"
                : "Apply All Reconciled?"}
            </h3>
            <p className="ops-modal-text">
              You are about to credit dividend to{" "}
              <strong>{confirmApply.count}</strong> account(s) for{" "}
              <strong>{symbol}</strong> ({isin}).
            </p>
            <p className="ops-modal-text">
              Estimated system gross:{" "}
              <strong>₹{confirmApply.gross.toFixed(2)}</strong>
            </p>
            {confirmApply.mode === "all" && (
              <div className="cm-verdict warn">
                This includes <em>mismatch</em>, <em>partial</em>,{" "}
                <em>over-paid</em> and <em>missing-in-file</em> accounts. Review
                the grid before continuing.
              </div>
            )}
            <p className="ops-modal-note">
              Already-paid and missing-in-system accounts are always skipped.
            </p>
            <div className="ops-modal-actions">
              <button
                type="button"
                className="cm-secondary"
                onClick={closeConfirm}
              >
                Cancel
              </button>
              <button
                type="button"
                className="cm-primary"
                onClick={confirmAndApply}
              >
                Confirm Apply
              </button>
            </div>
          </div>
        </div>
      )}

      {dividendList.length > 0 && (
        <div className="temp-transaction-card">
          <h3 className="ops-card-title">Previous Dividends</h3>
          <div className="data-table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Security Code</th>
                  <th className="wrap">Security Name</th>
                  <th>Type</th>
                  <th className="num">Dividend Rate</th>
                  <th>Unit</th>
                  <th className="date">Ex-Date</th>
                  <th className="date">Record Date</th>
                  <th className="date">Payment Date</th>
                </tr>
              </thead>
              <tbody>
                {dividendList.map((row, idx) => (
                  <tr key={idx}>
                    <td>{row.securityCode}</td>
                    <td className="wrap">{row.securityName}</td>
                    <td>{row.dividendType}</td>
                    <td className="num">{row.rate}</td>
                    <td>{row.unit}</td>
                    <td className="date">{row.exDate}</td>
                    <td className="date">{row.recordDate}</td>
                    <td className="date">{row.paymentDate ?? "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
      </div>
    </MainLayout>
  );
}

export default DividendPage;
