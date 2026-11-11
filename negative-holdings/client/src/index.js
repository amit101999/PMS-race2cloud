import React from "react";
import { createRoot } from "react-dom/client";
import NegativeHoldingsPage from "./NegativeHoldingsPage";
import "./design-system.css";
import "./NegativeHoldingsPage.css";

createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <NegativeHoldingsPage />
  </React.StrictMode>,
);
