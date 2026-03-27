import React, { useState, useCallback } from "react";
import MainLayout from "../../layouts/MainLayout";
import { BASE_URL } from "../../constant";
import "./UpdateISINPage.css";

/** @typedef {'idle' | 'in_progress' | 'completed' | 'failure'} UpdateStatus */
/** @typedef {{ kind: 'success' | 'error', text: string } | null} BannerState */


const UpdateISINPage = () => {
  const [oldIsin, setOldIsin] = useState("");
  const [newIsin, setNewIsin] = useState("");
  /** @type {[UpdateStatus, React.Dispatch<React.SetStateAction<UpdateStatus>>]} */
  const [status, setStatus] = useState("idle");
  /** @type {[BannerState, React.Dispatch<React.SetStateAction<BannerState>>]} */
  const [banner, setBanner] = useState(null);

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

    setStatus("in_progress");

    try {
      const res = await fetch(`${BASE_URL}/isin/update`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
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
        text: err instanceof Error ? err.message : "Update failed.",
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
            Enter the existing ISIN and the new ISIN, then run update.
          </p>

          <div className="update-isin-grid">
            <div className="update-isin-field">
              <label htmlFor="old-isin">Old ISIN</label>
              <input
                id="old-isin"
                type="text"
                value={oldIsin}
                onChange={(e) => {
                  setOldIsin(e.target.value);
                  clearTerminalStateOnEdit();
                }}
                placeholder="e.g. INE000A01020"
                disabled={status === "in_progress"}
                autoComplete="off"
              />
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
