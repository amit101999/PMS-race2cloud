import React from "react";
import MainLayout from "../../layouts/MainLayout";
import {
  SummaryCardsRow,
  ChartsRow,
  FiltersBar,
  RecentTradesSection,
} from "../../components/dashboard/DashboardComponents";
import { Button } from "../../components/common/CommonComponents";

function DashboardPage() {
  return (
    <MainLayout
      title="Stock Portfolio Dashboard"
      rightContent={
        <Button variant="secondary">Import File (Excel/CSV)</Button>
      }
    >
      <SummaryCardsRow />
      <ChartsRow />
      <FiltersBar />
      <RecentTradesSection />
      //{" "}
    </MainLayout>
  );
}

export default DashboardPage;
