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
 */
export default function NotificationTray() {
  const { jobs, dismissJob } = useJobNotifications();
  const [open, setOpen] = useState(false);
  const [flash, setFlash] = useState(false);
  const rootRef = useRef(null);
  const prevSignatureRef = useRef("");

  // Briefly flash the bell whenever the job list changes (new job, status
  // change) — a nudge, not a disappearing toast: nothing here auto-dismisses.
  useEffect(() => {
    const signature = jobs.map((j) => `${j.id}:${j.status}`).join("|");
    const changed = prevSignatureRef.current !== "" && signature !== prevSignatureRef.current;
    prevSignatureRef.current = signature;

    if (!changed) return;
    setFlash(true);
    const t = setTimeout(() => setFlash(false), 1200);
    return () => clearTimeout(t);
  }, [jobs]);

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
    <div className={`job-tray ${flash ? "job-tray--flash" : ""}`} ref={rootRef}>
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

      {open && (
        <div className="job-tray-panel" role="dialog" aria-label="Notifications">
          <div className="job-tray-panel-header">
            <span>Notifications</span>
          </div>

          {jobs.length === 0 ? (
            <div className="job-tray-empty">Nothing to show right now.</div>
          ) : (
            <ul className="job-tray-list">
              {jobs.map((job) => {
                const meta = STATUS_META[job.status] || STATUS_META.in_progress;
                return (
                  <li key={job.id} className="job-tray-item">
                    <div className="job-tray-item-top">
                      <span className={`job-status-pill ${meta.cls}`}>
                        {job.status === "in_progress" && <span className="job-status-dot" />}
                        {meta.label}
                      </span>
                      <button
                        type="button"
                        className="job-tray-dismiss"
                        onClick={() => dismissJob(job.id)}
                        aria-label="Dismiss notification"
                        title="Dismiss"
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
