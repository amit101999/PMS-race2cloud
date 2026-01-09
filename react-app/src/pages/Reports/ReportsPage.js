import React, { useState } from "react";
import MainLayout from "../../layouts/MainLayout";
import { Card } from "../../components/common/CommonComponents";
import "./ReportPage.css";
import HoldingsTab from "./tabs/Holdings";
import TransactionsTab from "./tabs/Transactionstab";
import TopClientsTab from "./tabs/TopClientsTab";
import CashTab from "./tabs/CashTab";

function ReportsPage() {
  const [activeTab, setActiveTab] = useState("holdings");

  const tabs = [
    { key: "holdings", label: "Holdings" },
    { key: "transactions", label: "Transactions" },
    { key: "cash", label: "Cash" },
    { key: "topClients", label: "Top Clients" },
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
      default:
        return null;
    }
  };

  return (
    <MainLayout title="Reports">
      <Card className="reports-card">
        {/* Tabs */}
        <div className="reports-tabs">
          {tabs.map((tab) => (
            <button
              key={tab.key}
              className={`tab-item ${activeTab === tab.key ? "active" : ""}`}
              onClick={() => setActiveTab(tab.key)}
            >
              {tab.label}
            </button>
          ))}
        </div>

        {/* Tab Content */}
        <div className="reports-body">{renderTab()}</div>
      </Card>
    </MainLayout>
  );
}

export default ReportsPage;
