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
  const [applying, setApplying] = useState(false);
  const [applySuccess, setApplySuccess] = useState(false);
  const [success, setSuccess] = useState(false);

  const [previewData, setPreviewData] = useState([]);
  const [step, setStep] = useState("form");

  /* ===========================
     PAGINATION
     =========================== */
  const PAGE_SIZE = 10;
  const [page, setPage] = useState(1);

  useEffect(() => setPage(1), [previewData]);

  const totalPages = Math.ceil(previewData.length / PAGE_SIZE);
  const paginatedPreview = previewData.slice(
    (page - 1) * PAGE_SIZE,
    page * PAGE_SIZE,
  );

  const dropdownRef = useRef(null);

  /* ===========================
     FETCH SECURITIES
     =========================== */
     useEffect(() => {
      let cancelled = false;
      (async () => {
        try {
          const res = await fetch(`${BASE_URL}/bonus/getAllSecuritiesList`);
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const data = await res.json();
          if (!cancelled && data.success) setSecurities(data.data);
        } catch (err) {
          if (!cancelled) setError(err.message || "Failed to load securities");
          setSecurities([]);
        }
      })();
      return () => { cancelled = true; };
    }, []);

  /* ===========================
     DROPDOWN CLOSE
     =========================== */
  useEffect(() => {
    const handler = (e) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target)) {
        setShowDropdown(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  const filteredSecurities = securities.filter(
    (s) =>
      s.isin.toLowerCase().includes(searchQuery.toLowerCase()) ||
      s.securityCode.toLowerCase().includes(searchQuery.toLowerCase()) ||
      s.securityName.toLowerCase().includes(searchQuery.toLowerCase()),
  );

  const handleSelectISIN = (sec) => {
    setIsin(sec.isin);
    setSearchQuery(sec.isin);
    setSecurityCode(sec.securityCode);
    setSecurityName(sec.securityName);
    setShowDropdown(false);
  };

  /* ===========================
     PREVIEW BONUS (FIFO)
     =========================== */
  const fetchPreview = async () => {
    setLoading(true);
    setError(null);

    const res = await fetch(`${BASE_URL}/bonus/preview`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        isin,
        ratio1: Number(ratio1),
        ratio2: Number(ratio2),
        exDate: date, // ✅ REQUIRED FOR FIFO
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
  };
  const downloadBonusExport = () => {
    if (!isin || !ratio1 || !ratio2 || !date) return;

    const url =
      `${BASE_URL}/export/bonus-preview` +
      `?isin=${encodeURIComponent(isin)}` +
      `&ratio1=${encodeURIComponent(ratio1)}` +
      `&ratio2=${encodeURIComponent(ratio2)}` +
      `&exDate=${encodeURIComponent(date)}`;

    window.open(url, "_blank");
  };

  return (
    <MainLayout title="Stock Bonus">
      <Card style={{ marginTop: 24 }}>
        <h2 style={{ fontSize: 18, fontWeight: 600 }}>Add Stock Bonus</h2>

        {success && (
          <div className="alert success">Bonus applied successfully</div>
        )}
        {error && <div className="alert error">{error}</div>}

        <div className="bonus-card">
          {/* ISIN SEARCH */}
          <div className="account-code-search" ref={dropdownRef}>
            <label className="search-label">ISIN</label>
            <div className="search-input-wrapper">
              <span className="search-icon">🔍</span>
              <input
                className="search-input"
                placeholder="Search ISIN..."
                value={searchQuery}
                onChange={(e) => {
                  setSearchQuery(e.target.value);
                  setShowDropdown(true);
                }}
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
            </div>

            {showDropdown && (
              <div className="search-dropdown">
                {filteredSecurities.map((sec) => (
                  <div
                    key={sec.isin}
                    className="dropdown-option"
                    onClick={() => handleSelectISIN(sec)}
                  >
                    <strong>{sec.isin}</strong>
                    <div style={{ fontSize: 12 }}>
                      {sec.securityCode} – {sec.securityName}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          <div className="bonus-field">
            <label>Security Code</label>
            <input value={securityCode} disabled />
          </div>

          <div className="bonus-field">
            <label>Security Name</label>
            <input value={securityName} disabled />
          </div>

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

          <div className="bonus-field">
            <label>Effective Date</label>
            <input
              type="date"
              value={date}
              onChange={(e) => setDate(e.target.value)}
            />
          </div>

          <button
            className="bonus-submit"
            disabled={!isin || ratio1 <= 0 || !ratio2 || !date || loading}
            onClick={fetchPreview}
          >
            Fetch Affected Accounts
          </button>
        </div>
      </Card>

      {/* ===========================
         PREVIEW
         =========================== */}
      {step === "preview" && (
        <div className="bonus-preview-wrapper full-width">
          <h3>Bonus Impact Preview</h3>

          {previewData.length === 0 ? (
            <div className="alert info">No accounts affected</div>
          ) : (
            <>
              <div className="bonus-preview-table-wrapper">
                <table className="bonus-preview-table">
                  <thead>
                    <tr>
                      <th>Account Code</th>
                      <th>ISIN</th>
                      <th>Current Holding</th>
                      <th>Bonus Shares</th>
                      <th>New Holding</th>
                      <th>Δ Change</th>
                    </tr>
                  </thead>

                  <tbody>
                    {paginatedPreview.map((row) => (
                      <tr key={row.accountCode}>
                        <td>{row.accountCode}</td>
                        <td>{row.isin}</td>
                        <td>{row.currentHolding}</td>
                        <td>{row.bonusShares}</td>
                        <td>{row.newHolding}</td>
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
                    onClick={() => setPage(page - 1)}
                  >
                    Prev
                  </button>
                  <span>
                    Page {page} of {totalPages}
                  </span>
                  <button
                    disabled={page === totalPages}
                    onClick={() => setPage(page + 1)}
                  >
                    Next
                  </button>
                </div>
              )}

              <div className="bonus-preview-actions">
                {/* DOWNLOAD CSV */}
                <button
  className="bonus-submit"
  onClick={async () => {
    try {
      const params = new URLSearchParams({
        isin,
        ratio1,
        ratio2,
        exDate: date,
      });

      const res = await fetch(
        `${BASE_URL}/bonus/export-preview?${params.toString()}`,
        { credentials: "include" }
      );

      if (!res.ok) throw new Error("Export request failed");

      const data = await res.json();

      if (!data.success) {
        throw new Error(data.message || "Export failed");
      }

      // ✅ CORRECT URL ACCESS
      const signedUrl = data.downloadUrl?.signature?.signature;

      if (!signedUrl) {
        throw new Error("Download URL missing");
      }

      // ✅ OPEN SIGNED STRATUS URL
      window.open(signedUrl, "_blank");

    } catch (err) {
      console.error(err);
      alert("Failed to download bonus preview CSV");
    }
  }}
>
  Download Preview CSV
</button>

                {/* APPLY BONUS */}
                <button
                  className="bonus-submit"
                  disabled={applying}
                  onClick={async () => {
                    setApplying(true);
                    setError(null);

                    try {
                      const res = await fetch(`${BASE_URL}/bonus/apply`, {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({
                          isin,
                          ratio1: Number(ratio1),
                          ratio2: Number(ratio2),
                          exDate: date,
                        }),
                      });

                      const data = await res.json();

                      if (!data.success) {
                        setError(data.message || "Failed to apply bonus");
                        setApplying(false);
                        return;
                      }

                      // ✅ SUCCESS
                      setApplySuccess(true);

                      setTimeout(() => {
                        // Clear form
                        setIsin("");
                        setSearchQuery("");
                        setSecurityCode("");
                        setSecurityName("");
                        setRatio1("");
                        setRatio2("");
                        setDate("");

                        // Clear preview
                        setPreviewData([]);
                        setStep("form");

                        setApplying(false);
                        setApplySuccess(false);
                        setSuccess(true);
                      }, 800);
                    } catch (err) {
                      setError("Something went wrong");
                      setApplying(false);
                    }
                  }}
                >
                  {applying ? "Applying Bonus..." : "Confirm & Apply Bonus"}
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
