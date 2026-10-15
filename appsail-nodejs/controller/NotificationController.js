import {
  insertNotification,
  listRecentNotifications,
  deleteNotification,
  clearNotifications,
  pruneOldNotifications,
} from "../util/notifications.js";

function getZcql(req, res) {
  if (!req.catalystApp) {
    res.status(500).json({ success: false, message: "Catalyst app not initialized" });
    return null;
  }
  return req.catalystApp.zcql();
}

export async function getNotifications(req, res) {
  const zcql = getZcql(req, res);
  if (!zcql) return;
  try {
    pruneOldNotifications(zcql).catch(() => {});
    const notifications = await listRecentNotifications(zcql);
    res.json({ success: true, notifications });
  } catch (err) {
    console.error("[notifications] list failed:", err);
    res.status(500).json({ success: false, message: "Failed to load notifications" });
  }
}

export async function createNotification(req, res) {
  const zcql = getZcql(req, res);
  if (!zcql) return;
  const { jobName, title, message = "", status = "in_progress" } = req.body || {};
  if (!jobName || !title) {
    return res
      .status(400)
      .json({ success: false, message: "jobName and title are required" });
  }
  try {
    await insertNotification(zcql, { jobName, title, message, status });
    const notifications = await listRecentNotifications(zcql);
    res.json({ success: true, notifications });
  } catch (err) {
    console.error("[notifications] create failed:", err);
    res.status(500).json({ success: false, message: "Failed to save notification" });
  }
}

export async function removeNotification(req, res) {
  const zcql = getZcql(req, res);
  if (!zcql) return;
  try {
    await deleteNotification(zcql, req.params.id);
    const notifications = await listRecentNotifications(zcql);
    res.json({ success: true, notifications });
  } catch (err) {
    console.error("[notifications] delete failed:", err);
    res.status(500).json({ success: false, message: "Failed to delete notification" });
  }
}

export async function clearAllNotifications(req, res) {
  const zcql = getZcql(req, res);
  if (!zcql) return;
  try {
    await clearNotifications(zcql);
    const notifications = await listRecentNotifications(zcql);
    res.json({ success: true, notifications });
  } catch (err) {
    console.error("[notifications] clear failed:", err);
    res.status(500).json({ success: false, message: "Failed to clear notifications" });
  }
}
