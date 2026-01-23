
import React, { useState, useEffect, useRef } from "react";
import MainLayout from "../../layouts/MainLayout";
import { Card } from "../../components/common/CommonComponents";
import "./BonusPage.css";
import { BASE_URL } from "../../constant";

function BonusPage() {
  /* ===========================
     FORM STATE
     =========================== */
  const [isin, setIsin] = useState("");
  const [securityCode, setSecurityCode] = useState("");
  const [securityName, setSecurityName] = useState("");
  const [ratio1, setRatio1] = useState("");
  const [ratio2, setRatio2] = useState("");
  const [date, setDate] = useState("");

  const [securities, setSecurities] = useState([]);
  const [searchQuery, setSearchQuery] = useState("");
  const [showDropdown, setShowDropdown] = useState(false);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(false);

  const dropdownRef = useRef(null);

  /* ===========================
     FETCH ISIN + CODE + NAME
     =========================== */
  useEffect(() => {
    fetchAllSecurities();
  }, []);

  const fetchAllSecurities = async () => {
    try {
      const res = await fetch(`${BASE_URL}/bonus/getAllSecuritiesList`);
      const data = await res.json();
      if (data.success) {
        setSecurities(data.data);
      }
    } catch (err) {
      console.error("Failed to fetch securities", err);
    }
  };

  /* ===========================
     CLOSE DROPDOWN ON OUTSIDE CLICK
     =========================== */
  useEffect(() => {
    const handleClickOutside = (e) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target)) {
        setShowDropdown(false);
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  /* ===========================
     FILTER ISIN SEARCH
     =========================== */
  const filteredSecurities = securities.filter(
    (sec) =>
      sec.isin.toLowerCase().includes(searchQuery.toLowerCase()) ||
      sec.securityCode.toLowerCase().includes(searchQuery.toLowerCase()) ||
      sec.securityName.toLowerCase().includes(searchQuery.toLowerCase()),
  );

  /* ===========================
     HANDLE ISIN SELECTION
     =========================== */
  const handleSelectISIN = (sec) => {
    setIsin(sec.isin);
    setSearchQuery(sec.isin);
    setSecurityCode(sec.securityCode);
    setSecurityName(sec.securityName);
    setShowDropdown(false);
  };

  /* ===========================
     SUBMIT Bonus FORM
     =========================== */
  const handleSubmit = async () => {
    if (Number(ratio1) === Number(ratio2)) {
      setError("Bonus ratio cannot be the same");
      return;
    }

    try {
      setLoading(true);
      setError(null);
      setSuccess(false);

      const payload = {
        isin,
        securityCode,
        securityName,
        ratio1: Number(ratio1),
        ratio2: Number(ratio2),
        issueDate: date,
      };

      const res = await fetch(`${BASE_URL}/bonus/add`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.message || "Failed to apply bonus");
      }

      setSuccess(true);

      // Reset form
      setIsin("");
      setSearchQuery("");
      setSecurityCode("");
      setSecurityName("");
      setRatio1("");
      setRatio2("");
      setDate("");

      setTimeout(() => setSuccess(false), 3000);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <MainLayout title="Stock Bonus">
      <Card style={{ marginTop: 24 }}>
        <h2 style={{ fontSize: 18, fontWeight: 600, marginBottom: 20 }}>
          Add Stock Bonus
        </h2>

        {success && (
          <div className="alert success">Bonus saved successfully!</div>
        )}
        {error && <div className="alert error">{error}</div>}

        <div className="bonus-card">
          {/* ISIN SEARCH */}
          <div className="account-code-search" ref={dropdownRef}>
            <label className="search-label">ISIN</label>

            <div className="search-input-wrapper">
              <span className="search-icon">🔍</span>

              <input
                type="text"
                className="search-input"
                placeholder="Search ISIN..."
                value={searchQuery}
                onChange={(e) => {
                  setSearchQuery(e.target.value);
                  setShowDropdown(true);
                }}
                onFocus={() => setShowDropdown(true)}
              />

              {searchQuery && (
                <span
                  className="clear-icon"
                  onClick={() => {
                    setSearchQuery("");
                    setIsin("");
                    setSecurityCode("");
                    setSecurityName("");
                  }}
                >
                  ✕
                </span>
              )}

              <span className="arrow-icon">▾</span>
            </div>

            {showDropdown && filteredSecurities.length > 0 && (
              <div className="search-dropdown">
                <div className="dropdown-header">Search ISIN</div>

                <div className="dropdown-options">
                  {filteredSecurities.map((sec) => (
                    <div
                      key={sec.isin}
                      className="dropdown-option"
                      onClick={() => handleSelectISIN(sec)}
                    >
                      <strong>{sec.isin}</strong>
                      <div style={{ fontSize: 12, color: "#6b7280" }}>
                        {sec.securityCode} – {sec.securityName}
                      </div>
                    </div>
                  ))}
                </div>

                <div className="dropdown-footer">
                  {filteredSecurities.length} of {securities.length} ISINs
                </div>
              </div>
            )}
          </div>

          {/* AUTO FILLED */}
          <div className="bonus-field">
            <label>Security Code</label>
            <input value={securityCode} disabled />
          </div>

          <div className="bonus-field">
            <label>Security Name</label>
            <input value={securityName} disabled />
          </div>

          {/* RATIOS */}
          <div className="bonus-field">
            <label>Ratio 1</label>
            <input
              type="number"
              value={ratio1}
              onChange={(e) => setRatio1(e.target.value)}
            />
          </div>

          <div className="bonus-field">
            <label>Ratio 2</label>
            <input
              type="number"
              value={ratio2}
              onChange={(e) => setRatio2(e.target.value)}
            />
          </div>

          {/* DATE */}
          <div className="bonus-field">
            <label>Effective Date</label>
            <input
              type="date"
              value={date}
              onChange={(e) => setDate(e.target.value)}
            />
          </div>

          {/* ACTION */}
          <div className="bonus-actions">
            <button
              className="bonus-submit"
              disabled={
                !isin ||
                !ratio1 ||
                !ratio2 ||
                !date ||
                Number(ratio1) <= 0 ||
                Number(ratio2) <= 0 ||
                loading
              }
              onClick={handleSubmit}
            >
              {loading ? "Saving..." : "Apply Bonus"}
            </button>
          </div>
        </div>
      </Card>
    </MainLayout>
  );
}

export default BonusPage;
