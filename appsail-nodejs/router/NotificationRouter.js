import express from "express";
import {
  getNotifications,
  createNotification,
  removeNotification,
  clearAllNotifications,
} from "../controller/NotificationController.js";

const router = express.Router();

router.get("/", getNotifications);
router.post("/", createNotification);
router.post("/clear", clearAllNotifications);
router.delete("/:id", removeNotification);

export default router;
