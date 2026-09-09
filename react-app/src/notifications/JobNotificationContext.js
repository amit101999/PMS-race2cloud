import React, {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
} from "react";

/**
 * Generic background-job notification system.
 *
 * Any page can call `trackJob({ id, label, statusUrl })` when it kicks off a
 * long-running background job (an ISIN rename, a bulk export, a report
 * generation — anything with a "check status" endpoint). This provider owns
 * the poll loop itself, independent of whichever page is currently mounted,
 * so the job keeps being tracked — and is visible via <NotificationTray /> —
 * even after the user navigates away from the page that started it.
 *
 * Not specific to ISIN or any one feature: `statusUrl` + an optional
 * `parseStatus` is all a caller needs to supply. Mount <JobNotificationProvider>
 * once, above the router, so it survives route changes for the whole session.
 */

const JobNotificationContext = createContext(null);

const DEFAULT_POLL_INTERVAL_MS = 5000;
const DEFAULT_TIMEOUT_MS = 20 * 60 * 1000; // 20 minutes

/**
 * Default response parser — matches the shape returned by
 * GET /api/isin/job-status (jobStatus: SUCCESSFUL | FAILURE | PARTIAL_FAILURE
 * | IN_PROGRESS, plus an optional failedTargets list). Callers tracking a
 * different kind of job can pass their own `parseStatus(data)` instead.
 */
function defaultParseStatus(data) {
  const raw = String(data?.jobStatus ?? "").toUpperCase();
  if (raw === "SUCCESSFUL") {
    return { status: "completed", message: data?.message || null };
  }
  if (raw === "FAILURE") {
    return { status: "failed", message: data?.message || null };
  }
  if (raw === "PARTIAL_FAILURE") {
    const failedTargets = Array.isArray(data?.failedTargets) ? data.failedTargets : [];
    const names = failedTargets.map((f) => f?.table).filter(Boolean).join(", ");
    return {
      status: "partial_failure",
      message: names ? `Could not update: ${names}` : "Completed with some failures.",
      detail: failedTargets,
    };
  }
  return { status: "in_progress", message: null };
}

export function JobNotificationProvider({ children }) {
  const [jobs, setJobs] = useState([]); // most-recent-first
  const timersRef = useRef(new Map()); // id -> timeout handle
  const cancelledRef = useRef(new Set()); // ids whose poll loop should stop

  const stopPolling = useCallback((id) => {
    cancelledRef.current.add(id);
    const t = timersRef.current.get(id);
    if (t) {
      clearTimeout(t);
      timersRef.current.delete(id);
    }
  }, []);

  useEffect(() => {
    // Stop every poll loop if the provider itself ever unmounts (app close).
    const timers = timersRef.current;
    return () => {
      for (const t of timers.values()) clearTimeout(t);
      timers.clear();
    };
  }, []);

  const updateJob = useCallback((id, patch) => {
    setJobs((prev) => prev.map((j) => (j.id === id ? { ...j, ...patch, updatedAt: Date.now() } : j)));
  }, []);

  const trackJob = useCallback(
    ({
      id,
      label,
      statusUrl,
      message = null,
      interval = DEFAULT_POLL_INTERVAL_MS,
      timeoutMs = DEFAULT_TIMEOUT_MS,
      parseStatus = defaultParseStatus,
      fetchOptions = { method: "GET", credentials: "include" },
    }) => {
      if (!id || !statusUrl) return;

      // A fresh tracking call for this id restarts its poll loop.
      stopPolling(id);
      cancelledRef.current.delete(id);

      const startedAt = Date.now();
      const entry = {
        id,
        label: label || id,
        status: "in_progress",
        // Optional opening sentence shown while the job is still running.
        message: message ?? null,
        detail: null,
        startedAt,
        updatedAt: startedAt,
      };

      setJobs((prev) => [entry, ...prev.filter((j) => j.id !== id)]);

      const tick = async () => {
        if (cancelledRef.current.has(id)) return;

        try {
          const res = await fetch(statusUrl, fetchOptions);
          const data = await res.json().catch(() => ({}));
          if (!res.ok) {
            throw new Error(data?.message || `Status check failed (${res.status})`);
          }

          const parsed = parseStatus(data) || {};
          if (parsed.status && parsed.status !== "in_progress") {
            if (!cancelledRef.current.has(id)) {
              updateJob(id, {
                status: parsed.status,
                message: parsed.message ?? null,
                detail: parsed.detail ?? null,
              });
            }
            return; // terminal — stop polling
          }
        } catch (e) {
          // A single failed status check shouldn't end the whole flow — keep
          // polling until the timeout below.
          console.warn(`[JobNotifications] poll error for '${id}':`, e);
        }

        if (cancelledRef.current.has(id)) return;

        if (Date.now() - startedAt > timeoutMs) {
          updateJob(id, {
            status: "timeout",
            message: "Still processing — this is taking longer than usual. Check back later.",
          });
          return;
        }

        const t = setTimeout(tick, interval);
        timersRef.current.set(id, t);
      };

      tick();
    },
    [stopPolling, updateJob],
  );

  /**
   * Push a one-shot entry that has no "check status" endpoint to poll — for
   * work that finishes synchronously (an instant CSV export, say). Callers pass
   * a terminal `status` ('completed' | 'failed' | …) and a `message` sentence.
   */
  const reportJob = useCallback(({ id, label, status, message = null, detail = null }) => {
    if (!id || !status) return;
    stopPolling(id); // in case an id is reused
    const now = Date.now();
    const entry = {
      id,
      label: label || id,
      status,
      message,
      detail,
      startedAt: now,
      updatedAt: now,
    };
    setJobs((prev) => [entry, ...prev.filter((j) => j.id !== id)]);
  }, [stopPolling]);

  /** Remove a job from the tray (and stop polling it if still active). */
  const dismissJob = useCallback(
    (id) => {
      stopPolling(id);
      setJobs((prev) => prev.filter((j) => j.id !== id));
    },
    [stopPolling],
  );

  const value = { jobs, trackJob, reportJob, dismissJob };

  return (
    <JobNotificationContext.Provider value={value}>{children}</JobNotificationContext.Provider>
  );
}

export function useJobNotifications() {
  const ctx = useContext(JobNotificationContext);
  if (!ctx) {
    throw new Error("useJobNotifications must be used within a JobNotificationProvider");
  }
  return ctx;
}
