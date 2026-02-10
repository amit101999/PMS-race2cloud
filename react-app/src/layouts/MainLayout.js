import React from "react";
import { useLocation } from "react-router-dom";
import {
  PageLayout,
  Sidebar,
  Topbar,
} from "../components/common/CommonComponents";

function MainLayout({ title, rightContent, children }) {
  const location = useLocation();

  // Determine active key based on current route
  const getActiveKey = () => {
    if (location.pathname === "/analytics") {
      return "analytics";
    }
    if (location.pathname === "/split") {
      return "split";
    }
    if(location.pathname === "/bonus"){
      return "bonus";
    }
    if (location.pathname === "/dividend") {
      return "dividend";
    }
    if (location.pathname === "/bhav-copy") {
      return "bhav-copy";
    }
    if (location.pathname === "/transaction-upload") {
      return "transaction-upload";
    }
    if (location.pathname === "/reports") {
      return "reports";
    }

    return "dashboard";
  };

  const items = [
    { key: "dashboard", label: "Dashboard" },
    { key: "analytics", label: "Analytics" },
    { key: "corporate-actions", label: "Corporate Actions", children: [
      { key: "split", label: "Split" },
      { key: "bonus", label: "Bonus" },
      { key: "dividend", label: "Dividend" },
    ]},
    { key: "bhav-copy", label: "Bhav Copy" },
    { key: "transaction-upload", label: "Transaction Upload" },
    { key: "reports", label: "Reports" },
  ];

  const contentStyle = {
    paddingTop: 16,
    paddingBottom: 24,
    paddingLeft: 16, // space between sidebar and cards
    paddingRight: 12, // small inner right padding (less than before)
    width: "100%",
    maxWidth: "100%",
    boxSizing: "border-box",
    overflowX: "visible",
  };

  return (
    <PageLayout sidebar={<Sidebar items={items} activeKey={getActiveKey()} />}>
      <Topbar title={title} rightContent={rightContent} />
      <div style={contentStyle}>{children}</div>
    </PageLayout>
  );
}

export default MainLayout;
