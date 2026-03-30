import React, { useState, useCallback, useEffect, useMemo } from "react";
import MainLayout from "../../layouts/MainLayout";
import { BASE_URL } from "../../constant";
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
  const [oldIsin, setOldIsin] = useState("");
  const [newIsin, setNewIsin] = useState("");
  /** @type {[SecurityListIsinRow[], React.Dispatch<React.SetStateAction<SecurityListIsinRow[]>>]} */
  const [securityListIsins, setSecurityListIsins] = useState([]);
  const [isinsLoading, setIsinsLoading] = useState(true);
  const [isinsLoadError, setIsinsLoadError] = useState(null);
  /** @type {[UpdateStatus, React.Dispatch<React.SetStateAction<UpdateStatus>>]} */
  const [status, setStatus] = useState("idle");
  /** @type {[BannerState, React.Dispatch<React.SetStateAction<BannerState>>]} */
  const [banner, setBanner] = useState(null);

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

  const suggestions = useMemo(() => {
    if (!oldIsinTrim || resolvedOldRow || !securityListIsins.length) return [];
    const q = oldIsinTrim.toUpperCase();
    return securityListIsins
      .filter(
        (r) =>
          r.isin.toUpperCase().includes(q) ||
          r.securityName.toUpperCase().includes(q) ||
          r.securityCode.toUpperCase().includes(q)
      )
      .slice(0, 12);
  }, [oldIsinTrim, resolvedOldRow, securityListIsins]);

  const clearTerminalStateOnEdit = useCallback(() => {
    if (status === "completed" || status === "failure") {
      setStatus("idle");
      setBanner(null);
    }
  }, [status]);

  const handleUpdate = async () => {
    setBanner(null);

    const oldTrim = oldIsin.trim();
    const newTrim = newIsin.trim();

    if (!oldTrim || !newTrim) {
      setStatus("failure");
      setBanner({
        kind: "error",
        text: "Old ISIN and New ISIN are required.",
      });
      return;
    }
    if (oldTrim === newTrim) {
      setStatus("failure");
      setBanner({
        kind: "error",
        text: "Old ISIN and New ISIN must be different.",
      });
      return;
    }

    if (securityListIsins.length > 0 && !isinByKey.get(oldTrim.toUpperCase())) {
      setStatus("failure");
      setBanner({
        kind: "error",
        text: "Old ISIN must match Security List. Type the full ISIN or pick a suggestion.",
      });
      return;
    }

    setStatus("in_progress");

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

      const jobId = data?.jobId ?? data?.job_id;
      console.log("UpdateISIN job submitted, jobId:", jobId);

      // Job is queued — Catalyst executes it within seconds.
      // We mark as completed immediately so the UI unblocks.
      setStatus("completed");
      setBanner({
        kind: "success",
        text: "ISIN update queued successfully. The database will be updated shortly.",
      });
    } catch (err) {
      setStatus("failure");
      setBanner({
        kind: "error",
        text: formatApiError(err, "Update failed."),
      });
    }
  };

  const showStatusRow =
    status === "in_progress" ||
    status === "completed" ||
    status === "failure";

  const statusLabel =
    status === "in_progress"
      ? "In progress"
      : status === "completed"
        ? "Completed"
        : status === "failure"
          ? "Failed"
          : "";

  const showBanner = Boolean(banner?.text);

  return (
    <MainLayout title="Update ISIN">
      <div className="update-isin-container">
        <div className="update-isin-card">
          <h3>Update ISIN</h3>
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
                  id="old-isin"
                  type="text"
                  value={oldIsin}
                  onChange={(e) => {
                    setOldIsin(e.target.value);
                    clearTerminalStateOnEdit();
                  }}
                  placeholder={
                    isinsLoading
                      ? "Loading Security List…"
                      : "e.g. INE000A01020"
                  }
                  disabled={status === "in_progress"}
                  autoComplete="off"
                  list="old-isin-datalist"
                  aria-describedby="old-isin-resolution"
                  aria-invalid={
                    oldIsinTrim.length >= 10 &&
                    !resolvedOldRow &&
                    !isinsLoading &&
                    securityListIsins.length > 0
                  }
                />
                <datalist id="old-isin-datalist">
                  {securityListIsins.map((row) => (
                    <option key={row.isin} value={row.isin}>
                      {row.securityName || row.securityCode || row.isin}
                    </option>
                  ))}
                </datalist>
                {suggestions.length > 0 && (
                  <ul className="update-isin-suggestions" role="listbox">
                    {suggestions.map((row) => (
                      <li key={row.isin} role="option">
                        <button
                          type="button"
                          className="update-isin-suggestion-btn"
                          onClick={() => {
                            setOldIsin(row.isin);
                            clearTerminalStateOnEdit();
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
                  <span className="update-isin-resolution-muted">
                    Loading securities…
                  </span>
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
              <label htmlFor="new-isin">New ISIN</label>
              <input
                id="new-isin"
                type="text"
                value={newIsin}
                onChange={(e) => {
                  setNewIsin(e.target.value);
                  clearTerminalStateOnEdit();
                }}
                placeholder="e.g. INE000A01021"
                disabled={status === "in_progress"}
                autoComplete="off"
              />
            </div>
          </div>

          <div className="update-isin-actions">
            <button
              type="button"
              className="update-isin-button"
              onClick={handleUpdate}
              disabled={status === "in_progress"}
            >
              {status === "in_progress" ? "Updating…" : "Update"}
            </button>
          </div>

          {showStatusRow && (
            <div
              className="update-isin-status-row"
              role="status"
              aria-live="polite"
            >
              <span className="update-isin-status-inner">
                <span className="update-isin-status-label">Status</span>
                <span
                  className={`update-isin-status-value update-isin-status--${status}`}
                >
                  {statusLabel}
                </span>
              </span>
            </div>
          )}

          {showBanner && banner && (
            <p
              className={`update-isin-message ${banner.kind === "error"
                ? "update-isin-message--error"
                : "update-isin-message--success"
                }`}
            >
              {banner.text}
            </p>
          )}
        </div>
      </div>
    </MainLayout>
  );
};

export default UpdateISINPage;
