import "./App.css";
import { HashRouter, Routes, Route } from "react-router-dom";
import DashboardPage from "./pages/Dashboard/DashboardPage";
import AnalyticsPage from "./pages/Analytics/AnalyticsPage";
import SplitPage from "./pages/SplitPage/SplitPage";
import BhavCopyUploadPage from "./pages/BhavCopyUpload/BhavCopyUploadPage";
import TransactionUploadPage from "./pages/TransactionUpload/TransactionUploadPage";  

function App() {
  return (
    <HashRouter>
      <Routes>
        <Route path="/" element={<DashboardPage />} />
        <Route path="/analytics" element={<AnalyticsPage />} />
        <Route path="/split" element={<SplitPage />} />
        <Route path="/bhav-copy" element={<BhavCopyUploadPage />} />
        <Route path="/transaction-upload" element={<TransactionUploadPage />} />
      </Routes>
    </HashRouter>
  );
}

export default App;
