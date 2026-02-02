import React, { useState } from "react";
import MainLayout from "../../layouts/MainLayout";
import { Card } from "../../components/common/CommonComponents";
import "./ReportPage.css";
import HoldingsTab from "./tabs/Holdings";
import TransactionsTab from "./tabs/Transactionstab";
import TopClientsTab from "./tabs/TopClientsTab";
import CashTab from "./tabs/CashTab";
import CorporateActionTab from "./tabs/CorporateActionTab";

function ReportsPage() {
  const [activeTab, setActiveTab] = useState("holdings");

  const tabs = [
    { key: "holdings", label: "Holdings" },
    { key: "transactions", label: "Transactions" },
    { key: "cash", label: "Cash" },
    { key: "topClients", label: "Top Clients" },
    { key: "corporateAction", label: "Corporate Action" },
  ];

  const renderTab = () => {
    switch (activeTab) {
      case "holdings":
        return <HoldingsTab />;
      case "transactions":
        return <TransactionsTab />;
      case "cash":
        return <CashTab />;
      case "topClients":
        return <TopClientsTab />;
      case "corporateAction":
        return <CorporateActionTab />;
      default:
        return null;
    }
  };

  return (
    <MainLayout title="Reports">
      <Card className="reports-card">
        {/* Type of Report - Dropdown */}
        <div className="report-type-dropdown-wrap">
          <label className="report-type-label">Type of Report</label>
          <select
            className="report-type-select"
            value={activeTab}
            onChange={(e) => setActiveTab(e.target.value)}
            aria-label="Type of Report"
          >
            {tabs.map((tab) => (
              <option key={tab.key} value={tab.key}>
                {tab.label}
              </option>
            ))}
          </select>
        </div>

        {/* Tab Content */}
        <div className="reports-body">{renderTab()}</div>
      </Card>
    </MainLayout>
  );
}

export default ReportsPage;
