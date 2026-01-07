import "./App.css";
import { HashRouter, Routes, Route } from "react-router-dom";
import DashboardPage from "./pages/Dashboard/DashboardPage";
import AnalyticsPage from "./pages/Analytics/AnalyticsPage";
import SplitPage from "./pages/SplitPage/SplitPage";
import BhavCopyPage from "./pages/BhavCopyPage/BhavCopyPage";

function App() {
  return (
    <HashRouter>
      <Routes>
        <Route path="/" element={<DashboardPage />} />
        <Route path="/analytics" element={<AnalyticsPage />} />
        <Route path="/split" element={<SplitPage />} />
        <Route path="/bhav-copy" element={<BhavCopyPage />} />
      </Routes>
    </HashRouter>
  );
}

export default App;
