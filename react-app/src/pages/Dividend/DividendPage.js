import React, { useState, useEffect, useRef } from "react";
import MainLayout from "../../layouts/MainLayout";
import { Card } from "../../components/common/CommonComponents";
import "./DividendPage.css";
import { BASE_URL } from "../../constant";

function DividendPage() {
  const [symbol, setSymbol] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [companyName, setCompanyName] = useState("");
  const [isin, setIsin] = useState("");
  const [series, setSeries] = useState("EQ");
  const [purposeText, setPurposeText] = useState("");
  const [dividendType, setDividendType] = useState("Interim");
  const [amount, setAmount] = useState("");
  const [unit, setUnit] = useState("Per Share");
  const [faceValue, setFaceValue] = useState("");
  const [exDate, setExDate] = useState("");
  const [recordDate, setRecordDate] = useState("");

  const [securities, setSecurities] = useState([]);
  const [showDropdown, setShowDropdown] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(false);
  const [dividendList, setDividendList] = useState([]);

  const dropdownRef = useRef(null);

  useEffect(() => {
    fetchAllSecurities();
  }, []);

  useEffect(() => {
    const handleClickOutside = (e) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target)) {
        setShowDropdown(false);
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const fetchAllSecurities = async () => {
    try {
      const res = await fetch(`${BASE_URL}/dividend/getAllSecuritiesList`);
      const data = await res.json();
      if (data.success) setSecurities(data.data || []);
    } catch (err) {
      console.error("Failed to fetch securities", err);
    }
  };

  const filteredSecurities = securities.filter(
    (sec) =>
      sec.securityCode?.toLowerCase().includes(searchQuery.toLowerCase()) ||
      sec.securityName?.toLowerCase().includes(searchQuery.toLowerCase()) ||
      sec.isin?.toLowerCase().includes(searchQuery.toLowerCase()),
  );

  const handleSelectSecurity = (sec) => {
    setSymbol(sec.securityCode || "");
    setSearchQuery(sec.securityCode || sec.isin || "");
    setCompanyName(sec.securityName || "");
    setIsin(sec.isin || "");
    setShowDropdown(false);
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    setError(null);
    if (!symbol || !companyName || !exDate || !recordDate) {
      setError("Symbol, Company Name, Ex-Date and Record Date are required.");
      return;
    }
    if (!purposeText.trim()) {
      setError("Purpose is required.");
      return;
    }
    const purpose = purposeText.trim();
    const entry = {
      symbol,
      companyName,
      isin,
      series,
      purpose,
      dividendType,
      amount: amount || "-",
      unit,
      faceValue: faceValue || "-",
      exDate,
      recordDate,
    };
    setDividendList((prev) => [entry, ...prev]);
    setSuccess(true);
    setTimeout(() => setSuccess(false), 3000);
    setPurposeText("");
    setAmount("");
  };

  return (
    <MainLayout title="Dividend">
      <Card style={{ marginTop: 24 }}>
        <h2 style={{ fontSize: 18, fontWeight: 600, marginBottom: 20 }}>
          Add Dividend 
        </h2>

        {success && (
          <div className="alert success">Dividend entry added successfully.</div>
        )}
        {error && <div className="alert error">{error}</div>}

        <form className="dividend-card" onSubmit={handleSubmit}>
          <div className="account-code-search" ref={dropdownRef}>
            <label className="search-label">Symbol</label>
            <div className="search-input-wrapper">
              <span className="search-icon">🔍</span>
              <input
                type="text"
                className="search-input"
                placeholder="Search symbol or company..."
                value={searchQuery}
                onChange={(e) => {
                  setSearchQuery(e.target.value);
                  setShowDropdown(true);
                  if (!e.target.value) {
                    setSymbol("");
                    setCompanyName("");
                    setIsin("");
                  }
                }}
                onFocus={() => setShowDropdown(true)}
              />
              {searchQuery && (
                <span
                  className="clear-icon"
                  onClick={() => {
                    setSearchQuery("");
                    setSymbol("");
                    setCompanyName("");
                    setIsin("");
                  }}
                >
                  ✕
                </span>
              )}
              <span className="arrow-icon">▾</span>
            </div>
            {showDropdown && filteredSecurities.length > 0 && (
              <div className="search-dropdown">
                <div className="dropdown-header">Search Symbol</div>
                <div className="dropdown-options">
                  {filteredSecurities.map((sec) => (
                    <div
                      key={sec.isin || sec.securityCode}
                      className="dropdown-option"
                      onClick={() => handleSelectSecurity(sec)}
                    >
                      <strong>{sec.securityCode || sec.isin}</strong>
                      <div style={{ fontSize: 12, color: "#6b7280" }}>
                        {sec.securityName}
                      </div>
                    </div>
                  ))}
                </div>
                <div className="dropdown-footer">
                  {filteredSecurities.length} of {securities.length} securities
                </div>
              </div>
            )}
          </div>

          <div className="dividend-field">
            <label>ISIN</label>
            <input value={isin} disabled />
          </div>

          <div className="dividend-field">
            <label>Company Name</label>
            <input value={companyName} disabled />
          </div>

          <div className="dividend-field">
            <label>Series</label>
            <select
              value={series}
              onChange={(e) => setSeries(e.target.value)}
              className="dividend-select"
            >
              <option value="EQ">EQ</option>
              <option value="IV">IV</option>
            </select>
          </div>

          <div className="dividend-field dividend-field-full">
            <label>Purpose</label>
            <input
              type="text"
              placeholder="e.g. Interim Dividend - Rs 15 Per Share"
              value={purposeText}
              onChange={(e) => setPurposeText(e.target.value)}
            />
          </div>

          <div className="dividend-field">
            <label>Dividend Type</label>
            <select
              value={dividendType}
              onChange={(e) => setDividendType(e.target.value)}
              className="dividend-select"
            >
              <option value="Interim">Interim</option>
              <option value="Special">Special</option>
              <option value="Final">Final</option>
              <option value="Interest">Interest</option>
            </select>
          </div>

          <div className="dividend-field">
            <label>Amount</label>
            <input
              type="number"
              step="any"
              placeholder="0"
              value={amount}
              onChange={(e) => setAmount(e.target.value)}
            />
          </div>

          <div className="dividend-field">
            <label>Unit</label>
            <select
              value={unit}
              onChange={(e) => setUnit(e.target.value)}
              className="dividend-select"
            >
              <option value="Per Share">Per Share</option>
              <option value="Per Unit">Per Unit</option>
              <option value="Per Unit/Interest">Per Unit/Interest</option>
            </select>
          </div>

          <div className="dividend-field">
            <label>Face Value</label>
            <input
              type="number"
              min="0"
              step="any"
              placeholder="e.g. 1, 2, 10"
              value={faceValue}
              onChange={(e) => setFaceValue(e.target.value)}
            />
          </div>

          <div className="dividend-field">
            <label>Ex-Date</label>
            <input
              type="date"
              value={exDate}
              onChange={(e) => setExDate(e.target.value)}
            />
          </div>

          <div className="dividend-field">
            <label>Record Date</label>
            <input
              type="date"
              value={recordDate}
              onChange={(e) => setRecordDate(e.target.value)}
            />
          </div>

          <div className="dividend-actions">
            <button type="submit" className="dividend-submit" disabled={loading}>
              {loading ? "Saving..." : "Add Dividend"}
            </button>
          </div>
        </form>
      </Card>

      {dividendList.length > 0 && (
        <Card style={{ marginTop: 24 }}>
          <h3 style={{ fontSize: 16, fontWeight: 600, marginBottom: 16 }}>
            Dividend Entries
          </h3>
          <div className="dividend-table-wrapper">
            <table className="dividend-table">
              <thead>
                <tr>
                  <th>Symbol</th>
                  <th>Company Name</th>
                  <th>Series</th>
                  <th>Purpose</th>
                  <th>Type</th>
                  <th>Amount</th>
                  <th>Unit</th>
                  <th>Face Val</th>
                  <th>Ex-Date</th>
                  <th>Record Date</th>
                </tr>
              </thead>
              <tbody>
                {dividendList.map((row, idx) => (
                  <tr key={idx}>
                    <td>{row.symbol}</td>
                    <td>{row.companyName}</td>
                    <td>{row.series}</td>
                    <td>{row.purpose}</td>
                    <td>{row.dividendType}</td>
                    <td>{row.amount}</td>
                    <td>{row.unit}</td>
                    <td>{row.faceValue}</td>
                    <td>{row.exDate}</td>
                    <td>{row.recordDate}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      )}
    </MainLayout>
  );
}

export default DividendPage;
