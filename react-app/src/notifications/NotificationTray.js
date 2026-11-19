import React, { useEffect, useRef, useState } from "react";
import { useJobNotifications } from "./JobNotificationContext";
import "./NotificationTray.css";

const STATUS_META = {
  in_progress: { label: "In progress", cls: "job-status--progress" },
  completed: { label: "Completed", cls: "job-status--completed" },
  failed: { label: "Failed", cls: "job-status--failed" },
  partial_failure: { label: "Partially failed", cls: "job-status--partial" },
  timeout: { label: "Still running", cls: "job-status--timeout" },
};

/* How long a pop-up message stays, matching the slide-in, hold and slide-out in the CSS. */
const TOAST_MS = 3000;
/* A burst of updates (a tab woken after a while) shows only the newest few. */
const MAX_TOASTS = 3;

function relativeTime(ts) {
  const diffSec = Math.max(0, Math.round((Date.now() - ts) / 1000));
  if (diffSec < 5) return "just now";
  if (diffSec < 60) return `${diffSec}s ago`;
  const diffMin = Math.round(diffSec / 60);
  if (diffMin < 60) return `${diffMin}m ago`;
  const diffHr = Math.round(diffMin / 60);
  return `${diffHr}h ago`;
}

/**
 * Fixed top-right notification bell + dropdown, backed by JobNotificationContext.
 * Mount once (in App.js, inside the provider) so it's visible on every page.
 * Reusable as-is by any feature that calls useJobNotifications().trackJob(...).
 *
 * A new notification, or one whose status changes, also slides in from the right
 * as a short message under the bell and goes after TOAST_MS. It stays in the list.
 */
export default function NotificationTray() {
  const { jobs, loaded, dismissJob, clearAll } = useJobNotifications();
  const [open, setOpen] = useState(false);
  const [toasts, setToasts] = useState([]);
  const rootRef = useRef(null);
  // Status of each notification already seen; null until the server's first list
  // arrives, so the notifications already there when the page opens do not pop up.
  const seenRef = useRef(null);
  const timersRef = useRef([]);

  useEffect(() => {
    if (!loaded) return;
    const seen = seenRef.current;
    const newestOf = (list) => Math.max(0, ...list.map((j) => Number(j.updatedAt) || 0));
    seenRef.current = {
      statuses: new Map(jobs.map((j) => [j.rowId || j.id, j.status])),
      newest: Math.max(seen?.newest || 0, newestOf(jobs)),
    };
    if (seen === null) return;

    // Updated since the last list: an older notification that moves into the
    // list after a delete or Clear all is not news.
    const arrived = jobs
      .filter((j) => seen.statuses.get(j.rowId || j.id) !== j.status && (Number(j.updatedAt) || 0) > seen.newest)
      .slice(0, MAX_TOASTS);
    if (arrived.length === 0) return;

    const shown = arrived.map((j) => ({ ...j, toastKey: `${j.rowId || j.id}:${j.status}:${Date.now()}` }));
    setToasts((prev) => [...shown, ...prev].slice(0, MAX_TOASTS));
    for (const toast of shown) {
      timersRef.current.push(
        setTimeout(() => setToasts((prev) => prev.filter((t) => t.toastKey !== toast.toastKey)), TOAST_MS),
      );
    }
  }, [jobs, loaded]);

  useEffect(() => () => timersRef.current.forEach(clearTimeout), []);

  useEffect(() => {
    function onClickOutside(e) {
      if (rootRef.current && !rootRef.current.contains(e.target)) {
        setOpen(false);
      }
    }
    document.addEventListener("mousedown", onClickOutside);
    return () => document.removeEventListener("mousedown", onClickOutside);
  }, []);

  const activeCount = jobs.filter((j) => j.status === "in_progress").length;
  const attentionCount = jobs.filter(
    (j) => j.status === "failed" || j.status === "partial_failure" || j.status === "timeout",
  ).length;

  const badgeCount = jobs.length;
  const badgeTone = attentionCount > 0 ? "attention" : activeCount > 0 ? "progress" : "done";

  return (
    <div className="job-tray" ref={rootRef}>
      <button
        type="button"
        className="job-tray-bell"
        onClick={() => setOpen((v) => !v)}
        aria-label={`Notifications (${badgeCount})`}
        aria-expanded={open}
      >
        <BellIcon />
        {badgeCount > 0 && (
          <span className={`job-tray-badge job-tray-badge--${badgeTone}`}>{badgeCount}</span>
        )}
      </button>

      {!open && toasts.length > 0 && (
        <div className="job-toasts" aria-live="polite">
          {toasts.map((toast) => (
            <div key={toast.toastKey} className="job-toast" role="status">
              <span className="job-toast-icon">
                <BellIcon />
              </span>
              <div className="job-toast-text">
                <div className="job-toast-label">{toast.label}</div>
                {toast.message && <div className="job-toast-message">{toast.message}</div>}
              </div>
            </div>
          ))}
        </div>
      )}

      {open && (
        <div className="job-tray-panel" role="dialog" aria-label="Notifications">
          <div className="job-tray-panel-header">
            <span>Notifications</span>
            {jobs.length > 0 && (
              <button type="button" className="job-tray-clear" onClick={clearAll}>
                Clear all
              </button>
            )}
          </div>

          {jobs.length === 0 ? (
            <div className="job-tray-empty">Nothing to show right now.</div>
          ) : (
            <ul className="job-tray-list">
              {jobs.map((job) => {
                const meta = STATUS_META[job.status] || STATUS_META.in_progress;
                return (
                  <li key={job.rowId || job.id} className="job-tray-item">
                    <div className="job-tray-item-top">
                      <span className={`job-status-pill ${meta.cls}`}>
                        {job.status === "in_progress" && <span className="job-status-dot" />}
                        {meta.label}
                      </span>
                      <button
                        type="button"
                        className="job-tray-dismiss"
                        onClick={() => dismissJob(job.rowId)}
                        aria-label="Delete notification"
                        title="Delete"
                      >
                        ×
                      </button>
                    </div>
                    <div className="job-tray-item-label">{job.label}</div>
                    {job.message && <div className="job-tray-item-message">{job.message}</div>}
                    {Array.isArray(job.detail) && job.detail.length > 0 && (
                      <ul className="job-tray-item-detail">
                        {job.detail.map((d, i) => (
                          <li key={i}>
                            <b>{d.table}</b>
                            {d.error ? ` — ${d.error}` : ""}
                          </li>
                        ))}
                      </ul>
                    )}
                    <div className="job-tray-item-time">{relativeTime(job.updatedAt)}</div>
                  </li>
                );
              })}
            </ul>
          )}
        </div>
      )}
    </div>
  );
}

function BellIcon() {
  return (
    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" aria-hidden="true">
      <path
        d="M12 3a5.5 5.5 0 0 0-5.5 5.5v2.68c0 .53-.21 1.04-.59 1.41L4.5 14v1h15v-1l-1.41-1.41a2 2 0 0 1-.59-1.41V8.5A5.5 5.5 0 0 0 12 3Z"
        stroke="currentColor"
        strokeWidth="1.6"
        strokeLinejoin="round"
      />
      <path
        d="M9.5 18a2.5 2.5 0 0 0 5 0"
        stroke="currentColor"
        strokeWidth="1.6"
        strokeLinecap="round"
      />
    </svg>
  );
}
