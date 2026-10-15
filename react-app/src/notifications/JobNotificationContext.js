import React, {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
} from "react";
import { BASE_URL } from "../constant";

/**
 * Background-job notification bell — backed by the server.
 *
 * Any page calls `trackJob({ id, label, message })` when it starts a
 * long-running background job, or `reportJob({ id, label, status, message })`
 * for work that finished right away. Both write one row to the `Notifications`
 * table (via /api/notifications), keyed by `id` (the job / track key).
 *
 * The list lives on the server, so it is the same for every admin and survives
 * a page refresh. This provider just polls `GET /api/notifications` (newest 5)
 * on a shared loop — faster while something is still running. The row itself is
 * settled to completed / failed server-side by reconciling against the `Jobs`
 * table each job already updates, so it finishes correctly even if the tab that
 * started it was closed.
 *
 * Mount <JobNotificationProvider> once, above the router.
 */

const JobNotificationContext = createContext(null);

const ENDPOINT = `${BASE_URL}/notifications`;
const ACTIVE_POLL_MS = 5000; // something is still running
const IDLE_POLL_MS = 20000; // everything settled — just keep the list fresh

function mapRow(n) {
  return {
    id: n.jobName || n.rowId, // pages match by their own job key
    rowId: n.rowId,
    label: n.title || n.jobName || "Task",
    message: n.message || null,
    status: n.status || "in_progress",
    updatedAt: n.updatedAt || Date.now(),
  };
}

export function JobNotificationProvider({ children }) {
  const [jobs, setJobs] = useState([]); // newest first, max 10
  const jobsRef = useRef([]);
  const timerRef = useRef(null);
  const mountedRef = useRef(true);

  useEffect(() => {
    jobsRef.current = jobs;
  }, [jobs]);

  const applyList = useCallback((list) => {
    if (!mountedRef.current) return;
    setJobs((Array.isArray(list) ? list : []).map(mapRow));
  }, []);

  const refresh = useCallback(async () => {
    try {
      const res = await fetch(ENDPOINT, { method: "GET", credentials: "include" });
      const data = await res.json().catch(() => ({}));
      if (res.ok && data?.success) applyList(data.notifications);
    } catch (e) {
      console.warn("[JobNotifications] refresh failed:", e);
    }
  }, [applyList]);

  // One shared poll loop for the whole app.
  useEffect(() => {
    mountedRef.current = true;
    const tick = async () => {
      if (document.visibilityState === "visible") await refresh();
      if (!mountedRef.current) return;
      const anyActive = jobsRef.current.some((j) => j.status === "in_progress");
      timerRef.current = setTimeout(tick, anyActive ? ACTIVE_POLL_MS : IDLE_POLL_MS);
    };
    tick();
    return () => {
      mountedRef.current = false;
      if (timerRef.current) clearTimeout(timerRef.current);
    };
  }, [refresh]);

  const post = useCallback(
    async (body) => {
      try {
        const res = await fetch(ENDPOINT, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          credentials: "include",
          body: JSON.stringify(body),
        });
        const data = await res.json().catch(() => ({}));
        if (res.ok && data?.success) applyList(data.notifications);
        else refresh();
      } catch (e) {
        console.warn("[JobNotifications] save failed:", e);
      }
    },
    [applyList, refresh],
  );

  /** Long-running job: shows "in progress" until the server settles the row. */
  const trackJob = useCallback(
    ({ id, label, message = null }) => {
      if (!id || !label) return;
      post({ jobName: String(id), title: label, message, status: "in_progress" });
    },
    [post],
  );

  /** Work that already finished: pass a terminal status ("completed" | "failed" | …). */
  const reportJob = useCallback(
    ({ id, label, status, message = null }) => {
      if (!id || !label || !status) return;
      post({ jobName: String(id), title: label, message, status });
    },
    [post],
  );

  const dismissJob = useCallback(
    (rowId) => {
      if (!rowId) return;
      fetch(`${ENDPOINT}/${encodeURIComponent(rowId)}`, {
        method: "DELETE",
        credentials: "include",
      })
        .then((r) => r.json())
        .catch(() => ({}))
        .then((data) => (data?.success ? applyList(data.notifications) : refresh()));
    },
    [applyList, refresh],
  );

  // The server clears every notification, not only the ones shown here.
  const clearAll = useCallback(() => {
    if (!jobsRef.current.length) return;
    fetch(`${ENDPOINT}/clear`, {
      method: "POST",
      credentials: "include",
    })
      .then((r) => r.json())
      .catch(() => ({}))
      .then((data) => (data?.success ? applyList(data.notifications) : refresh()));
  }, [applyList, refresh]);

  const value = { jobs, trackJob, reportJob, dismissJob, clearAll, refresh };

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
