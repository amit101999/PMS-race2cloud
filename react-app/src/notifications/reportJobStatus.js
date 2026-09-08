/**
 * Status parser for the Reports export endpoints.
 *
 * They report progress as a `status` string — PENDING / RUNNING / IN_PROGRESS /
 * COMPLETED / FAILED / ERROR / NO_DATA / NOT_STARTED — rather than the
 * jobStatus shape `defaultParseStatus` (in JobNotificationContext) understands.
 * This maps that vocabulary onto the notification system's
 * { status, message } result so any Reports tab can pass
 * `parseStatus: (d) => parseReportStatus(d)` to `trackJob`.
 */
export function parseReportStatus(data, { noDataMessage } = {}) {
  const s = String(data?.status ?? "").toUpperCase();

  if (s === "COMPLETED") {
    return { status: "completed", message: data?.message || null };
  }
  if (s === "NO_DATA") {
    return {
      status: "completed",
      message: noDataMessage || "Finished — no matching data for that selection.",
    };
  }
  if (s === "FAILED" || s === "ERROR") {
    return {
      status: "failed",
      message: data?.message || "Export failed. Check the export history and retry.",
    };
  }
  return { status: "in_progress", message: null };
}
