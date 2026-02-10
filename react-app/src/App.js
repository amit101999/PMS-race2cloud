import "./App.css";
import { HashRouter, Routes, Route } from "react-router-dom";
import DashboardPage from "./pages/Dashboard/DashboardPage";
import AnalyticsPage from "./pages/Analytics/AnalyticsPage";
import SplitPage from "./pages/SplitPage/SplitPage";
import BhavCopyUploadPage from "./pages/BhavCopyUpload/BhavCopyUploadPage";
import TransactionUploadPage from "./pages/TransactionUpload/TransactionUploadPage";
import ReportsPage from "./pages/Reports/ReportsPage";
import BonusPage from "./pages/BonusPage/BonusPage";
import DividendPage from "./pages/Dividend/DividendPage";

function App() {
  return (
    <HashRouter>
      <Routes>
        <Route path="/" element={<DashboardPage />} />
        <Route path="/analytics" element={<AnalyticsPage />} />
        <Route path="/split" element={<SplitPage />} />
        <Route path="/bonus" element={<BonusPage />} />
        <Route path="/dividend" element={<DividendPage />} />
        <Route path="/bhav-copy" element={<BhavCopyUploadPage />} />
        <Route path="/transaction-upload" element={<TransactionUploadPage />} />
        <Route path="/reports" element={<ReportsPage />} />
      </Routes>
    </HashRouter>
  );
}

export default App;
