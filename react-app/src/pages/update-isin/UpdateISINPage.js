import React, { useState, useCallback, useEffect, useMemo, useRef } from "react";
import MainLayout from "../../layouts/MainLayout";
import { BASE_URL } from "../../constant";
import { useJobNotifications } from "../../notifications/JobNotificationContext";
import "./UpdateISINPage.css";

/** @typedef {'idle' | 'in_progress' | 'completed' | 'failure'} UpdateStatus */
/** @typedef {{ kind: 'success' | 'error', text: string } | null} BannerState */
/** @typedef {{ isin: string; securityCode: string; securityName: string }} SecurityListIsinRow */

/** Uses `constant.js` BASE_URL; override with REACT_APP_ISIN_API_BASE if needed. */
const ISIN_API_BASE =
  typeof process !== "undefined" &&
  process.env.REACT_APP_ISIN_API_BASE != null &&
  String(process.env.REACT_APP_ISIN_API_BASE).trim() !== ""
    ? String(process.env.REACT_APP_ISIN_API_BASE).trim().replace(/\/$/, "")
    : BASE_URL.replace(/\/$/, "");

/** @param {unknown} err @param {string} fallback */
const formatApiError = (err, fallback) => {
  if (err instanceof TypeError) {
    const m = String(err.message || "");
    if (/fetch|Failed to fetch|Load failed|NetworkError/i.test(m)) {
      return `Cannot reach the API at ${ISIN_API_BASE}. Confirm the backend is running on that URL and CORS allows this page’s origin.`;
    }
  }
  return err instanceof Error ? err.message : fallback;
};

const UpdateISINPage = () => {
  /** --- Update Script (top card): Old + New ISIN, Update button --- */
  const [oldIsin, setOldIsin] = useState("");
  const [oldIsinFocused, setOldIsinFocused] = useState(false);
  const oldIsinInputRef = useRef(null);
  const oldIsinPickingRef = useRef(false);
  const [scriptNewIsin, setScriptNewIsin] = useState("");
  /** @type {[UpdateStatus, React.Dispatch<React.SetStateAction<UpdateStatus>>]} */
  const [scriptStatus, setScriptStatus] = useState("idle");
  /** @type {[BannerState, React.Dispatch<React.SetStateAction<BannerState>>]} */
  const [scriptBanner, setScriptBanner] = useState(null);
  /** id of the currently tracked rename job in the shared notification system. */
  const [scriptJobId, setScriptJobId] = useState(null);

  /** --- New ISIN panel (bottom card): list-driven NEW ISIN + code + name, Apply --- */
  const [panelNewIsin, setPanelNewIsin] = useState("");
  const [panelNewIsinFocused, setPanelNewIsinFocused] = useState(false);
  const panelNewIsinInputRef = useRef(null);
  const panelNewIsinPickingRef = useRef(false);
  const [newSecurityCode, setNewSecurityCode] = useState("");
  const [newSecurityName, setNewSecurityName] = useState("");
  /** @type {[UpdateStatus, React.Dispatch<React.SetStateAction<UpdateStatus>>]} */
  const [applyStatus, setApplyStatus] = useState("idle");
  /** id of the currently tracked apply-new job in the shared notification system. */
  const [applyJobId, setApplyJobId] = useState(null);
  /** @type {[BannerState, React.Dispatch<React.SetStateAction<BannerState>>]} */
  const [applyBanner, setApplyBanner] = useState(null);

  /** @type {[SecurityListIsinRow[], React.Dispatch<React.SetStateAction<SecurityListIsinRow[]>>]} */
  const [securityListIsins, setSecurityListIsins] = useState([]);
  const [isinsLoading, setIsinsLoading] = useState(true);
  const [isinsLoadError, setIsinsLoadError] = useState(null);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setIsinsLoading(true);
      setIsinsLoadError(null);
      try {
        const res = await fetch(`${ISIN_API_BASE}/isin/security-list-isins`, {
          method: "GET",
          credentials: "include",
        });
        const body = await res.json().catch(() => ({}));
        if (!res.ok) {
          throw new Error(body?.message || `Failed to load ISINs (${res.status})`);
        }
        const list = Array.isArray(body?.data) ? body.data : [];
        if (!cancelled) {
          setSecurityListIsins(list);
        }
      } catch (e) {
        if (!cancelled) {
          setIsinsLoadError(formatApiError(e, "Failed to load Security List."));
          setSecurityListIsins([]);
        }
      } finally {
        if (!cancelled) setIsinsLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const { trackJob, jobs: notificationJobs } = useJobNotifications();

  // The shared notification context owns the actual polling (and keeps
  // running even if this page is left) — this just mirrors its result back
  // into the card's own status/banner so the inline UI stays in sync too.
  useEffect(() => {
    if (!scriptJobId) return;
    const job = notificationJobs.find((j) => j.id === scriptJobId);
    if (!job) return;

    if (job.status === "completed") {
      setScriptStatus("completed");
      setScriptBanner({
        kind: "success",
        text: job.message || "ISIN update completed — the database has been updated.",
      });
    } else if (job.status === "failed") {
      setScriptStatus("failure");
      setScriptBanner({
        kind: "error",
        text: job.message || "ISIN update failed partway through. Check the job logs before retrying.",
      });
    } else if (job.status === "partial_failure") {
      setScriptStatus("failure");
      setScriptBanner({
        kind: "error",
        text:
          job.message ||
          "Some tables could not be renamed. Check the notification panel for details.",
      });
    } else if (job.status === "timeout") {
      setScriptStatus("in_progress");
      setScriptBanner({
        kind: "success",
        text: job.message || "Still processing — check the notification panel for updates.",
      });
    }
  }, [notificationJobs, scriptJobId]);

  // Same pattern as the rename card above, for the Apply (New ISIN) job.
  useEffect(() => {
    if (!applyJobId) return;
    const job = notificationJobs.find((j) => j.id === applyJobId);
    if (!job) return;

    if (job.status === "completed") {
      setApplyStatus("completed");
      setApplyBanner({
        kind: "success",
        text: job.message || "New ISIN apply completed — Security Code/Name updated.",
      });
    } else if (job.status === "failed") {
      setApplyStatus("failure");
      setApplyBanner({
        kind: "error",
        text: job.message || "New ISIN apply failed. Check the job logs before retrying.",
      });
    } else if (job.status === "partial_failure") {
      setApplyStatus("failure");
      setApplyBanner({
        kind: "error",
        text:
          job.message ||
          "Some columns could not be updated. Check the notification panel for details.",
      });
    } else if (job.status === "timeout") {
      setApplyStatus("in_progress");
      setApplyBanner({
        kind: "success",
        text: job.message || "Still processing — check the notification panel for updates.",
      });
    }
  }, [notificationJobs, applyJobId]);

  const isinByKey = useMemo(() => {
    const m = new Map();
    for (const row of securityListIsins) {
      m.set(row.isin.toUpperCase(), row);
    }
    return m;
  }, [securityListIsins]);

  const oldIsinTrim = oldIsin.trim();
  const resolvedOldRow = oldIsinTrim
    ? isinByKey.get(oldIsinTrim.toUpperCase())
    : undefined;

  const panelNewIsinTrim = panelNewIsin.trim();
  const resolvedPanelNewRow = panelNewIsinTrim
    ? isinByKey.get(panelNewIsinTrim.toUpperCase())
    : undefined;

  /** Top card: Old ISIN suggestions */
  const suggestions = useMemo(() => {
    if (!oldIsinFocused || resolvedOldRow || !securityListIsins.length) return [];
    const q = oldIsinTrim.toUpperCase();
    if (!q) return securityListIsins.slice(0, 30);
    return securityListIsins
      .filter(
        (r) =>
          String(r.isin ?? "").toUpperCase().includes(q) ||
          String(r.securityName ?? "").toUpperCase().includes(q) ||
          String(r.securityCode ?? "").toUpperCase().includes(q)
      )
      .slice(0, 50);
  }, [oldIsinFocused, oldIsinTrim, resolvedOldRow, securityListIsins]);

  /** Bottom card: NEW ISIN suggestions from Security List */
  const panelNewSuggestions = useMemo(() => {
    if (!panelNewIsinFocused || resolvedPanelNewRow || !securityListIsins.length) return [];
    const q = panelNewIsinTrim.toUpperCase();
    if (!q) return securityListIsins.slice(0, 30);
    return securityListIsins
      .filter(
        (r) =>
          String(r.isin ?? "").toUpperCase().includes(q) ||
          String(r.securityName ?? "").toUpperCase().includes(q) ||
          String(r.securityCode ?? "").toUpperCase().includes(q)
      )
      .slice(0, 50);
  }, [panelNewIsinFocused, panelNewIsinTrim, resolvedPanelNewRow, securityListIsins]);

  const clearScriptTerminalStateOnEdit = useCallback(() => {
    if (scriptStatus === "completed" || scriptStatus === "failure") {
      setScriptStatus("idle");
      setScriptBanner(null);
    }
  }, [scriptStatus]);

  const clearApplyTerminalStateOnEdit = useCallback(() => {
    if (applyStatus === "completed" || applyStatus === "failure") {
      setApplyStatus("idle");
      setApplyBanner(null);
    }
  }, [applyStatus]);

  const pickOldIsin = useCallback(
    (isin) => {
      oldIsinPickingRef.current = true;
      setOldIsin(isin);
      clearScriptTerminalStateOnEdit();
      setOldIsinFocused(false);
      oldIsinInputRef.current?.blur();
    },
    [clearScriptTerminalStateOnEdit],
  );

  const pickPanelNewIsin = useCallback(
    (isin) => {
      panelNewIsinPickingRef.current = true;
      setPanelNewIsin(isin);
      clearApplyTerminalStateOnEdit();
      setPanelNewIsinFocused(false);
      panelNewIsinInputRef.current?.blur();
    },
    [clearApplyTerminalStateOnEdit],
  );

  /** Original “Update Script” behaviour: POST old + new from this card only. */
  const handleUpdate = async () => {
    setScriptBanner(null);

    const oldTrim = oldIsin.trim();
    const newTrim = scriptNewIsin.trim();

    if (!oldTrim || !newTrim) {
      setScriptStatus("failure");
      setScriptBanner({
        kind: "error",
        text: "Old ISIN and New ISIN are required.",
      });
      return;
    }
    if (oldTrim === newTrim) {
      setScriptStatus("failure");
      setScriptBanner({
        kind: "error",
        text: "Old ISIN and New ISIN must be different.",
      });
      return;
    }

    if (securityListIsins.length > 0 && !isinByKey.get(oldTrim.toUpperCase())) {
      setScriptStatus("failure");
      setScriptBanner({
        kind: "error",
        text: "Old ISIN must match Security List. Type the full ISIN or pick a suggestion.",
      });
      return;
    }

    setScriptStatus("in_progress");
    setScriptBanner({
      kind: "success",
      text: "ISIN update queued — checking progress…",
    });

    try {
      const res = await fetch(`${ISIN_API_BASE}/isin/update`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({ oldIsin: oldTrim, newIsin: newTrim }),
      });
      const data = await res.json().catch(() => ({}));

      if (!res.ok) {
        throw new Error(data?.message || `Request failed (${res.status})`);
      }

      const jobName = data?.catalystJobName;
      console.log("UpdateISIN job submitted, catalystJobName:", jobName, "jobId:", data?.jobId);

      if (jobName) {
        setScriptJobId(jobName);
        trackJob({
          id: jobName,
          label: `ISIN update: ${oldTrim} → ${newTrim}`,
          statusUrl: `${ISIN_API_BASE}/isin/job-status?jobName=${encodeURIComponent(jobName)}`,
        });
        // Tracking now lives in the shared notification system, so it keeps
        // running (and stays visible via the bell icon) even if this page
        // is left — the effect above mirrors its result back into this card.
      } else {
        // No status key to poll — fall back to reporting only that it was queued.
        setScriptStatus("completed");
        setScriptBanner({
          kind: "success",
          text: "ISIN update queued, but no job name was returned to track progress.",
        });
      }
    } catch (err) {
      setScriptStatus("failure");
      setScriptBanner({
        kind: "error",
        text: formatApiError(err, "Update failed."),
      });
    }
  };

  /**
   * Bottom panel: syncs Security_Code / Security_Name for the selected ISIN
   * across Security_List + Transaction. Per column: skip if already same,
   * otherwise update (handles NULL, empty, or different existing values).
   */
  const handleApply = async () => {
    setApplyBanner(null);

    const isinTrim = panelNewIsin.trim();
    const codeTrim = newSecurityCode.trim();
    const nameTrim = newSecurityName.trim();

    if (!isinTrim) {
      setApplyStatus("failure");
      setApplyBanner({
        kind: "error",
        text: "ISIN is required.",
      });
      return;
    }

    if (isinsLoading) {
      setApplyStatus("failure");
      setApplyBanner({
        kind: "error",
        text: "Please wait until the Security List finishes loading.",
      });
      return;
    }

    // Not found in the Security List we loaded → this Apply will CREATE a
    // brand-new security instead of updating an existing one. A fresh
    // record needs both fields; correcting an existing one only needs one.
    const isNewIsin =
      securityListIsins.length > 0 && !isinByKey.get(isinTrim.toUpperCase());

    if (isNewIsin) {
      if (!codeTrim || !nameTrim) {
        setApplyStatus("failure");
        setApplyBanner({
          kind: "error",
          text:
            "This ISIN doesn't exist yet. Security Code and Security Name are both required to create it.",
        });
        return;
      }
    } else if (!codeTrim && !nameTrim) {
      setApplyStatus("failure");
      setApplyBanner({
        kind: "error",
        text: "Provide at least one of Security Code or Security Name.",
      });
      return;
    }

    setApplyStatus("in_progress");
    setApplyBanner({
      kind: "success",
      text: isNewIsin
        ? "Creating new ISIN — checking progress…"
        : "New ISIN apply queued — checking progress…",
    });

    try {
      const res = await fetch(`${ISIN_API_BASE}/isin/apply-new`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({
          isin: isinTrim,
          securityCode: codeTrim,
          securityName: nameTrim,
        }),
      });
      const data = await res.json().catch(() => ({}));

      if (!res.ok) {
        throw new Error(data?.message || `Request failed (${res.status})`);
      }

      const jobName = data?.catalystJobName;
      console.log("ApplyNewISIN job submitted, catalystJobName:", jobName, "jobId:", data?.jobId);

      if (jobName) {
        setApplyJobId(jobName);
        trackJob({
          id: jobName,
          label: isNewIsin ? `Create ISIN: ${isinTrim}` : `New ISIN apply: ${isinTrim}`,
          statusUrl: `${ISIN_API_BASE}/isin/job-status?jobName=${encodeURIComponent(jobName)}`,
        });
        // Tracking now lives in the shared notification system — the effect
        // above mirrors its result back into this card once it settles.
      } else {
        setApplyStatus("completed");
        setApplyBanner({
          kind: "success",
          text: "New ISIN apply queued, but no job name was returned to track progress.",
        });
      }
    } catch (err) {
      setApplyStatus("failure");
      setApplyBanner({
        kind: "error",
        text: formatApiError(err, "Apply failed."),
      });
    }
  };

  const scriptBusy = scriptStatus === "in_progress";
  const applyBusy = applyStatus === "in_progress";

  const showScriptStatus =
    scriptStatus === "in_progress" ||
    scriptStatus === "completed" ||
    scriptStatus === "failure";
  const scriptStatusLabel =
    scriptStatus === "in_progress"
      ? "In progress"
      : scriptStatus === "completed"
        ? "Completed"
        : scriptStatus === "failure"
          ? "Failed"
          : "";

  const showApplyStatus =
    applyStatus === "in_progress" ||
    applyStatus === "completed" ||
    applyStatus === "failure";
  const applyStatusLabel =
    applyStatus === "in_progress"
      ? "In progress"
      : applyStatus === "completed"
        ? "Completed"
        : applyStatus === "failure"
          ? "Failed"
          : "";

  return (
    <MainLayout title="Update ISIN">
      <div className="update-isin-container">
        <div className="update-isin-card">
          <h3>Update Script</h3>
          <p className="update-isin-hint">
            Type the current ISIN (Security List). The security name appears when it matches.
            Then enter the new ISIN and run update.
          </p>

          {isinsLoadError && (
            <p className="update-isin-message update-isin-message--error" role="alert">
              {isinsLoadError} You can still type an ISIN; the server checks Security List.
            </p>
          )}

          <div className="update-isin-grid">
            <div className="update-isin-field">
              <label htmlFor="old-isin">Old ISIN</label>
              <div className="update-isin-combobox">
                <input
                  ref={oldIsinInputRef}
                  id="old-isin"
                  type="text"
                  value={oldIsin}
                  role="combobox"
                  aria-expanded={oldIsinFocused && suggestions.length > 0}
                  aria-controls="update-isin-suggest-list"
                  aria-autocomplete="list"
                  onChange={(e) => {
                    setOldIsin(e.target.value);
                    clearScriptTerminalStateOnEdit();
                  }}
                  onFocus={() => setOldIsinFocused(true)}
                  onBlur={() => {
                    if (oldIsinPickingRef.current) {
                      oldIsinPickingRef.current = false;
                      return;
                    }
                    setOldIsinFocused(false);
                  }}
                  placeholder={
                    isinsLoading ? "Loading Security List…" : "e.g. INE000A01020"
                  }
                  disabled={scriptBusy}
                  autoComplete="off"
                  aria-describedby="old-isin-resolution"
                  aria-invalid={
                    oldIsinTrim.length >= 10 &&
                    !resolvedOldRow &&
                    !isinsLoading &&
                    securityListIsins.length > 0
                  }
                />
                {suggestions.length > 0 && (
                  <ul
                    id="update-isin-suggest-list"
                    className="update-isin-suggestions"
                    role="listbox"
                  >
                    {suggestions.map((row) => (
                      <li key={row.isin} role="option">
                        <button
                          type="button"
                          className="update-isin-suggestion-btn"
                          onMouseDown={(e) => {
                            e.preventDefault();
                            pickOldIsin(row.isin);
                          }}
                        >
                          <span className="update-isin-suggestion-isin">{row.isin}</span>
                          {row.securityName ? (
                            <span className="update-isin-suggestion-name">
                              {row.securityName}
                            </span>
                          ) : null}
                        </button>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
              <div
                id="old-isin-resolution"
                className="update-isin-resolution"
                aria-live="polite"
              >
                {isinsLoading && !oldIsinTrim ? (
                  <span className="update-isin-resolution-muted">Loading securities…</span>
                ) : null}
                {resolvedOldRow ? (
                  <div className="update-isin-resolution-match">
                    <span className="update-isin-resolution-label">Security name</span>
                    <span className="update-isin-resolution-value">
                      {resolvedOldRow.securityName || "—"}
                    </span>
                    {resolvedOldRow.securityCode ? (
                      <span className="update-isin-resolution-meta">
                        Code: {resolvedOldRow.securityCode}
                      </span>
                    ) : null}
                  </div>
                ) : null}
                {oldIsinTrim &&
                !isinsLoading &&
                !resolvedOldRow &&
                suggestions.length === 0 ? (
                  <span className="update-isin-resolution-warn" role="status">
                    No exact match in Security List for this ISIN.
                  </span>
                ) : null}
              </div>
            </div>
            <div className="update-isin-field">
              <label htmlFor="script-new-isin">New ISIN</label>
              <input
                id="script-new-isin"
                type="text"
                value={scriptNewIsin}
                onChange={(e) => {
                  setScriptNewIsin(e.target.value);
                  clearScriptTerminalStateOnEdit();
                }}
                placeholder="e.g. INE000A01021"
                disabled={scriptBusy}
                autoComplete="off"
              />
            </div>
          </div>

          <div className="update-isin-actions">
            <button
              type="button"
              className="update-isin-button"
              onClick={handleUpdate}
              disabled={scriptBusy}
            >
              {scriptBusy ? "Updating…" : "Update"}
            </button>
          </div>

          {showScriptStatus && (
            <div className="update-isin-status-row" role="status" aria-live="polite">
              <span className="update-isin-status-inner">
                <span className="update-isin-status-label">Status</span>
                <span
                  className={`update-isin-status-value update-isin-status--${scriptStatus}`}
                >
                  {scriptStatusLabel}
                </span>
              </span>
            </div>
          )}

          {Boolean(scriptBanner?.text) && scriptBanner && (
            <p
              className={`update-isin-message ${scriptBanner.kind === "error"
                ? "update-isin-message--error"
                : "update-isin-message--success"
                }`}
            >
              {scriptBanner.text}
            </p>
          )}
        </div>

        <div className="update-isin-card update-isin-card--stack">
          <h3>New ISIN</h3>
          <p className="update-isin-hint">
            Pick NEW ISIN from the Security List, enter NEW Security Code and/or NEW Security
            Name, then Apply.
          </p>

          <div className="update-isin-apply-row">
            <div className="update-isin-field update-isin-field--apply-isin">
              <label htmlFor="panel-new-isin">NEW ISIN</label>
              <div className="update-isin-combobox">
                <input
                  ref={panelNewIsinInputRef}
                  id="panel-new-isin"
                  type="text"
                  value={panelNewIsin}
                  role="combobox"
                  aria-expanded={panelNewIsinFocused && panelNewSuggestions.length > 0}
                  aria-controls="update-isin-panel-new-suggest-list"
                  aria-autocomplete="list"
                  onChange={(e) => {
                    setPanelNewIsin(e.target.value);
                    clearApplyTerminalStateOnEdit();
                  }}
                  onFocus={() => setPanelNewIsinFocused(true)}
                  onBlur={() => {
                    if (panelNewIsinPickingRef.current) {
                      panelNewIsinPickingRef.current = false;
                      return;
                    }
                    setPanelNewIsinFocused(false);
                  }}
                  placeholder={
                    isinsLoading
                      ? "Loading Security List…"
                      : "NEW ISIN (fetch from Security List)"
                  }
                  disabled={applyBusy}
                  autoComplete="off"
                  aria-describedby="panel-new-isin-resolution"
                  aria-invalid={
                    panelNewIsinTrim.length >= 10 &&
                    !resolvedPanelNewRow &&
                    !isinsLoading &&
                    securityListIsins.length > 0
                  }
                />
                {panelNewSuggestions.length > 0 && (
                  <ul
                    id="update-isin-panel-new-suggest-list"
                    className="update-isin-suggestions"
                    role="listbox"
                  >
                    {panelNewSuggestions.map((row) => (
                      <li key={`panel-${row.isin}`} role="option">
                        <button
                          type="button"
                          className="update-isin-suggestion-btn"
                          onMouseDown={(e) => {
                            e.preventDefault();
                            pickPanelNewIsin(row.isin);
                          }}
                        >
                          <span className="update-isin-suggestion-isin">{row.isin}</span>
                          {row.securityName ? (
                            <span className="update-isin-suggestion-name">
                              {row.securityName}
                            </span>
                          ) : null}
                        </button>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            </div>

            <div className="update-isin-field update-isin-field--apply-code">
              <label htmlFor="new-security-code">NEW Security Code</label>
              <input
                id="new-security-code"
                type="text"
                value={newSecurityCode}
                onChange={(e) => {
                  setNewSecurityCode(e.target.value);
                  clearApplyTerminalStateOnEdit();
                }}
                placeholder="NEW Security Code"
                disabled={applyBusy}
                autoComplete="off"
              />
            </div>

            <div className="update-isin-field update-isin-field--apply-name">
              <label htmlFor="new-security-name">NEW Security Name</label>
              <input
                id="new-security-name"
                type="text"
                value={newSecurityName}
                onChange={(e) => {
                  setNewSecurityName(e.target.value);
                  clearApplyTerminalStateOnEdit();
                }}
                placeholder="NEW Security Name"
                disabled={applyBusy}
                autoComplete="off"
              />
            </div>

            <div className="update-isin-apply-row__btn">
              <button
                type="button"
                className="update-isin-button"
                onClick={handleApply}
                disabled={applyBusy}
              >
                {applyBusy ? "Applying…" : "Apply"}
              </button>
            </div>
          </div>

          <div
            id="panel-new-isin-resolution"
            className="update-isin-resolution update-isin-resolution--panel-new"
            aria-live="polite"
          >
            {isinsLoading && !panelNewIsinTrim ? (
              <span className="update-isin-resolution-muted">Loading securities…</span>
            ) : null}
            {resolvedPanelNewRow ? (
              <div className="update-isin-resolution-match">
                <span className="update-isin-resolution-label">Security name</span>
                <span className="update-isin-resolution-value">
                  {resolvedPanelNewRow.securityName || "—"}
                </span>
                {resolvedPanelNewRow.securityCode ? (
                  <span className="update-isin-resolution-meta">
                    Code: {resolvedPanelNewRow.securityCode}
                  </span>
                ) : null}
              </div>
            ) : null}
            {panelNewIsinTrim &&
            !isinsLoading &&
            !resolvedPanelNewRow &&
            panelNewSuggestions.length === 0 ? (
              <span className="update-isin-resolution-info" role="status">
                This ISIN doesn't exist yet — it will be created as a new security. Security
                Code and Security Name are both required.
              </span>
            ) : null}
          </div>

          {showApplyStatus && (
            <div className="update-isin-status-row" role="status" aria-live="polite">
              <span className="update-isin-status-inner">
                <span className="update-isin-status-label">Status</span>
                <span
                  className={`update-isin-status-value update-isin-status--${applyStatus}`}
                >
                  {applyStatusLabel}
                </span>
              </span>
            </div>
          )}

          {Boolean(applyBanner?.text) && applyBanner && (
            <p
              className={`update-isin-message ${applyBanner.kind === "error"
                ? "update-isin-message--error"
                : "update-isin-message--success"
                }`}
            >
              {applyBanner.text}
            </p>
          )}
        </div>
      </div>
    </MainLayout>
  );
};

export default UpdateISINPage;
