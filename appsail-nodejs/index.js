import Express from "express";
const app = Express();
const port = process.env.X_ZOHO_CATALYST_LISTEN_PORT || 9000;
import cors from "cors";

import AnalyticsRouter from "./router/AnalyticsRouter.js";
import TransactionsRouter from "./router/TransactionRouter.js";
import DashboardRouter from "./router/DashboardRouter.js";
import SplitRouter from "./router/SplitRouter.js";
import ExportRouter from "./router/ExportRouter.js";
import catalyst from "zcatalyst-sdk-node";
import BhavRouter from "./router/BhavRouter.js";
app.use(cors());
app.use(Express.json());

app.use((req, res, next) => {
  try {
    const app = catalyst.initialize(req);
    req.catalystApp = app;
    next();
  } catch (err) {
    console.error("Catalyst initialization error:", err);
    req.catalystApp = null;
    next();
  }
});

app.get("/", (req, res) => {
  // Catalyst app is already available via middleware (req.catalystApp)
  res.status(200).json({
    status: "ok",
    service: "server",
    message: "Catalyst Express backend is running",
  });
});

app.use("/api/analytics", AnalyticsRouter);
app.use("/api/transaction", TransactionsRouter);
app.use("/api/dashboard", DashboardRouter);
app.use("/api/split", SplitRouter);
app.use("/api/export", ExportRouter);
app.use("/api/bhav", BhavRouter);
app.listen(port, () => {
  console.log(`Example app listening on port ${port}`);
  console.log(`http://localhost:${port}/`);
});
