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

  const [previewData, setPreviewData] = useState([]);
  const [step, setStep] = useState("form"); // form | preview

  /* ===========================
     PAGINATION STATE
     =========================== */
  const PAGE_SIZE = 10;
  const [page, setPage] = useState(1);

  const totalPages = Math.ceil(previewData.length / PAGE_SIZE);

  const paginatedPreview = previewData.slice(
    (page - 1) * PAGE_SIZE,
    page * PAGE_SIZE
  );

  useEffect(() => {
    setPage(1);
  }, [previewData]);

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
    return () =>
      document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  /* ===========================
     FILTER ISIN SEARCH
     =========================== */
  const filteredSecurities = securities.filter(
    (sec) =>
      sec.isin.toLowerCase().includes(searchQuery.toLowerCase()) ||
      sec.securityCode.toLowerCase().includes(searchQuery.toLowerCase()) ||
      sec.securityName.toLowerCase().includes(searchQuery.toLowerCase())
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

  return (
    <MainLayout title="Stock Bonus">
      <Card style={{ marginTop: 24 }}>
        <h2 style={{ fontSize: 18, fontWeight: 600, marginBottom: 20 }}>
          Add Stock Bonus
        </h2>

        {success && (
          <div className="alert success">Bonus applied successfully!</div>
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

          {/* FETCH PREVIEW */}
          <div className="fetch-actions">
            <button
              className="bonus-submit fetch-btn"
              disabled={!isin || Number(ratio1) <= 0 || !ratio2}
              onClick={async () => {
                setLoading(true);
                setError(null);

                const res = await fetch(`${BASE_URL}/bonus/preview`, {
                  method: "POST",
                  headers: { "Content-Type": "application/json" },
                  body: JSON.stringify({
                    isin,
                    ratio1: Number(ratio1),
                    ratio2: Number(ratio2),
                  }),
                });

                const data = await res.json();

                if (data.success) {
                  setPreviewData(data.data || []);
                  setStep("preview");
                } else {
                  setError(data.message);
                }

                setLoading(false);
              }}
            >
              Fetch Affected Accounts
            </button>
          </div>
        </div>
      </Card>

      {/* ===========================
         PREVIEW SECTION
         =========================== */}
      {step === "preview" && (
        <div className="bonus-preview-wrapper full-width">
          <h3 className="bonus-preview-title">Bonus Impact Preview</h3>

          {previewData.length === 0 ? (
            <div className="alert info">
              No accounts available for this bonus ratio.
            </div>
          ) : (
            <>
              <div className="bonus-preview-table-wrapper">
                <table className="bonus-preview-table">
                  <thead>
                    <tr>
                      <th>Account Code</th>
                      <th>ISIN</th>
                      <th>Account Name</th>
                      <th>Scheme</th>
                      <th>Exchange</th>
                      <th>Current Bonus</th>
                      <th>New Bonus</th>
                      <th>Δ Change</th>
                    </tr>
                  </thead>
                  <tbody>
                    {paginatedPreview.map((row) => (
                      <tr key={row.rowId}>
                        <td>{row.accountCode}</td>
                        <td>{isin}</td>
                        <td>{row.accountName}</td>
                        <td>{row.schemeName}</td>
                        <td>{row.exchange}</td>
                        <td>{row.oldBonusShare}</td>
                        <td>{row.newBonusShare}</td>
                        <td style={{ color: "#166534", fontWeight: 600 }}>
                          +{row.delta}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {totalPages > 1 && (
                <div className="pagination">
                  <button
                    disabled={page === 1}
                    onClick={() => setPage((p) => p - 1)}
                  >
                    Prev
                  </button>
                  <span>
                    Page {page} of {totalPages}
                  </span>
                  <button
                    disabled={page === totalPages}
                    onClick={() => setPage((p) => p + 1)}
                  >
                    Next
                  </button>
                </div>
              )}

              <div className="bonus-preview-actions">
                <button
                  className="bonus-submit"
                  onClick={async () => {
                    await fetch(`${BASE_URL}/bonus/apply`, {
                      method: "POST",
                      headers: { "Content-Type": "application/json" },
                      body: JSON.stringify({
                        isin,
                        ratio1: Number(ratio1),
                        ratio2: Number(ratio2),
                        exDate: date,
                      }),
                    });

                    setSuccess(true);
                    setStep("form");
                    setPreviewData([]);
                  }}
                >
                  Confirm & Apply Bonus
                </button>
              </div>
            </>
          )}
        </div>
      )}
    </MainLayout>
  );
}

export default BonusPage;
