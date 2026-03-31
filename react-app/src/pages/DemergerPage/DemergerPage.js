import React, { useCallback, useEffect, useState } from "react";
import MainLayout from "../../layouts/MainLayout";
import { Card } from "../../components/common/CommonComponents";
import "../SplitPage/SplitPage.css";
import "./DemergerPage.css";
import { BASE_URL } from "../../constant";

function DemergerPage() {
  const [effectiveDate, setEffectiveDate] = useState("");
  const [recordDate, setRecordDate] = useState("");
  const [ratio1, setRatio1] = useState("");
  const [ratio2, setRatio2] = useState("");
  const [oldIsin, setOldIsin] = useState("");
  const [newIsin, setNewIsin] = useState("");
  const [costSplitPercent, setCostSplitPercent] = useState("");

  const [securities, setSecurities] = useState([]);
  const [loadError, setLoadError] = useState(null);
  const [formError, setFormError] = useState(null);
  const [confirmed, setConfirmed] = useState(false);

  const clearStatus = useCallback(() => {
    setFormError(null);
    setConfirmed(false);
  }, []);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(`${BASE_URL}/split/getAllSecuritiesList`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        if (!cancelled && data.success && Array.isArray(data.data)) {
          setSecurities(data.data);
        }
      } catch (err) {
        if (!cancelled) {
          setLoadError(err.message || "Could not load securities list");
          setSecurities([]);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const getSecurityLabel = (isin) => {
    const s = securities.find((x) => x.isin === isin);
    if (!s) return isin || "—";
    return `${s.securityName} — ${s.isin}`;
  };

  const handleConfirmAndApply = () => {
    clearStatus();

    if (!effectiveDate || !recordDate) {
      setFormError("Effective date and record date are required.");
      return;
    }
    const r1 = Number(ratio1);
    const r2 = Number(ratio2);
    if (!Number.isFinite(r1) || !Number.isFinite(r2) || r1 <= 0 || r2 <= 0) {
      setFormError("Ratio 1 and Ratio 2 must be positive numbers.");
      return;
    }
    if (!oldIsin || !newIsin) {
      setFormError("Please select both old and new company (name + ISIN).");
      return;
    }
    if (oldIsin === newIsin) {
      setFormError("Old and new company must be different.");
      return;
    }
    const pct = Number(costSplitPercent);
    if (!Number.isFinite(pct) || pct < 0 || pct > 100) {
      setFormError("Cost of acquisition split must be a percentage between 0 and 100.");
      return;
    }

    const oldSec = securities.find((x) => x.isin === oldIsin);
    const newSec = securities.find((x) => x.isin === newIsin);

    const payload = {
      effectiveDate,
      recordDate,
      ratio1: r1,
      ratio2: r2,
      oldCompany: oldSec
        ? {
            securityName: oldSec.securityName,
            securityCode: oldSec.securityCode,
            isin: oldSec.isin,
          }
        : { isin: oldIsin },
      newCompany: newSec
        ? {
            securityName: newSec.securityName,
            securityCode: newSec.securityCode,
            isin: newSec.isin,
          }
        : { isin: newIsin },
      costAllocationToNewCompanyPercent: pct,
      costAllocationToOldCompanyPercent: Math.round((100 - pct) * 100) / 100,
    };

    // UI-only: no API call yet
    console.log("[Demerger] Confirm & Apply payload (preview):", payload);
    setConfirmed(true);
  };

  const remainderHint =
    costSplitPercent === "" || Number.isNaN(Number(costSplitPercent))
      ? null
      : Math.round((100 - Number(costSplitPercent)) * 100) / 100;

  return (
    <MainLayout title="Demerger">
      <Card style={{ marginTop: 4 }}>
        <div className="demerger-page">
          {loadError && (
            <div className="alert error" style={{ marginBottom: 12 }}>
              {loadError} (dropdowns may be empty until the list loads.)
            </div>
          )}
          {formError && <div className="alert error">{formError}</div>}
          {confirmed && (
            <div className="alert success">
              Form validated. Apply is not wired to the server yet — check the browser console for the
              payload preview.
            </div>
          )}

          <div className="split-card">
            <div className="split-field">
              <label htmlFor="demerger-effective">Effective date</label>
              <input
                id="demerger-effective"
                type="date"
                value={effectiveDate}
                onChange={(e) => {
                  clearStatus();
                  setEffectiveDate(e.target.value);
                }}
              />
            </div>
            <div className="split-field">
              <label htmlFor="demerger-record">Record date</label>
              <input
                id="demerger-record"
                type="date"
                value={recordDate}
                onChange={(e) => {
                  clearStatus();
                  setRecordDate(e.target.value);
                }}
              />
            </div>
            <div className="split-field">
              <label htmlFor="demerger-ratio1">Ratio 1</label>
              <input
                id="demerger-ratio1"
                type="number"
                min="0"
                step="any"
                placeholder="e.g. 1"
                value={ratio1}
                onChange={(e) => {
                  clearStatus();
                  setRatio1(e.target.value);
                }}
              />
            </div>
            <div className="split-field">
              <label htmlFor="demerger-ratio2">Ratio 2</label>
              <input
                id="demerger-ratio2"
                type="number"
                min="0"
                step="any"
                placeholder="e.g. 1"
                value={ratio2}
                onChange={(e) => {
                  clearStatus();
                  setRatio2(e.target.value);
                }}
              />
            </div>

            <div className="demerger-section">
              <h3 className="demerger-section-title">Companies</h3>
              <div className="split-field demerger-select-field">
                <label htmlFor="demerger-old">Old company (name + ISIN)</label>
                <select
                  id="demerger-old"
                  className="demerger-select"
                  value={oldIsin}
                  onChange={(e) => {
                    clearStatus();
                    setOldIsin(e.target.value);
                  }}
                >
                  <option value="">Select security…</option>
                  {securities.map((s) => (
                    <option key={`old-${s.isin}`} value={s.isin}>
                      {s.securityName} — {s.isin}
                    </option>
                  ))}
                </select>
              </div>
              <div className="split-field demerger-select-field">
                <label htmlFor="demerger-new">New company (name + ISIN)</label>
                <select
                  id="demerger-new"
                  className="demerger-select"
                  value={newIsin}
                  onChange={(e) => {
                    clearStatus();
                    setNewIsin(e.target.value);
                  }}
                >
                  <option value="">Select security…</option>
                  {securities.map((s) => (
                    <option key={`new-${s.isin}`} value={s.isin}>
                      {s.securityName} — {s.isin}
                    </option>
                  ))}
                </select>
              </div>
            </div>

            <div className="demerger-section">
              <h3 className="demerger-section-title">Cost of acquisition split</h3>
              <div className="split-field">
                <label htmlFor="demerger-cost-pct">
                  Allocation to new company (%)
                </label>
                <input
                  id="demerger-cost-pct"
                  type="number"
                  min="0"
                  max="100"
                  step="0.01"
                  placeholder="0 – 100"
                  value={costSplitPercent}
                  onChange={(e) => {
                    clearStatus();
                    setCostSplitPercent(e.target.value);
                  }}
                />
                {remainderHint !== null && (
                  <p
                    className={`demerger-cost-hint ${
                      remainderHint < 0 || remainderHint > 100 ? "warn" : "ok"
                    }`}
                  >
                    Remainder allocated to old company:{" "}
                    <strong>{Number.isFinite(remainderHint) ? `${remainderHint}%` : "—"}</strong>
                  </p>
                )}
              </div>
            </div>

            {(oldIsin || newIsin) && (
              <div className="demerger-section demerger-summary-inline">
                <p className="demerger-cost-hint">
                  <strong>Selected:</strong> Old — {getSecurityLabel(oldIsin)} · New —{" "}
                  {getSecurityLabel(newIsin)}
                </p>
              </div>
            )}

            <div className="demerger-actions">
              <button type="button" className="bonus-submit" onClick={handleConfirmAndApply}>
                Confirm and apply
              </button>
            </div>
          </div>
        </div>
      </Card>
    </MainLayout>
  );
}

export default DemergerPage;
