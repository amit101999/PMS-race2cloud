import React, { useState } from "react";
import MainLayout from "../../layouts/MainLayout";
import { Card, Table } from "../../components/common/CommonComponents";
import { BASE_URL } from "../../constant";
import "./TransactionUploadPage.css";

const RESULT_COLUMNS = [
  { key: "WS_Account_code", header: "Account Code" },
  { key: "ISIN", header: "ISIN" },
  { key: "Security_code", header: "Security Code" },
  { key: "Security_Name", header: "Security Name" },
  { key: "Tran_Type", header: "Tran Type" },
  { key: "transactionQty", header: "Transaction Qty" },
  { key: "oldQuantity", header: "Old Qty" },
  { key: "newQuantity", header: "New Qty" },
];

const COMPARISON_COLUMNS = [
  { key: "Report Date", header: "Report Date" },
  { key: "Holding Date", header: "Holding Date" },
  { key: "UCC", header: "UCC" },
  { key: "ISIN", header: "ISIN" },
  { key: "Qty Cust", header: "Qty Cust" },
  { key: "Qty FA", header: "Qty FA" },
  { key: "Diff", header: "Diff" },
  { key: "Mismatch Reason", header: "Mismatch Reason" },
  { key: "Matching", header: "Matching" },
  { key: "Security", header: "Security" },
  { key: "Client name", header: "Client name" },
  { key: "LTP Date", header: "LTP Date" },
];

function TransactionUploadPage() {
  const [transactionFile, setTransactionFile] = useState(null);
  const [custodianFile, setCustodianFile] = useState(null);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState("");
  const [comparisonResult, setComparisonResult] = useState(null);
  const [custodianLoading, setCustodianLoading] = useState(false);
  const [custodianError, setCustodianError] = useState("");

  const handleTransactionUpload = async (e) => {
    e.preventDefault();
    setError("");
    setResult(null);
    if (!transactionFile) {
      setError("Please choose a transaction CSV file first.");
      return;
    }

    const formData = new FormData();
    formData.append("file", transactionFile);

    setLoading(true);
    try {
      const res = await fetch(`${BASE_URL}/transaction-uploader/parse-data`, {
        method: "POST",
        body: formData,
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.message || data?.error || "Request failed");
      setResult(data);
    } catch (err) {
      setError(err.message || "Request failed");
    } finally {
      setLoading(false);
    }
  };

  const handleCustodianUpload = async (e) => {
    e.preventDefault();
    setCustodianError("");
    setComparisonResult(null);
    if (!custodianFile) {
      setCustodianError("Please choose a custodian file first.");
      return;
    }
    if (!result?.success || !result?.data?.length) {
      setCustodianError("Please upload and process the Transaction file first to get simulated holdings.");
      return;
    }

    const formData = new FormData();
    formData.append("file", custodianFile);
    formData.append("simulatedHoldings", JSON.stringify(result.data));

    setCustodianLoading(true);
    try {
      const res = await fetch(`${BASE_URL}/transaction-uploader/compare-custodian`, {
        method: "POST",
        body: formData,
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.message || data?.error || "Comparison failed");
      setComparisonResult(data);
    } catch (err) {
      setCustodianError(err.message || "Comparison failed");
    } finally {
      setCustodianLoading(false);
    }
  };

  return (
    <MainLayout title="Transaction Upload">
      <div className="upload-page">
        <div className="upload-sections">
          {/* Transaction file section */}
          <Card className="upload-section-card">
            <div className="upload-section-header">
              <h3 className="upload-section-title">Transaction File (.csv)</h3>
            </div>
            <form onSubmit={handleTransactionUpload} className="upload-section-form">
              <div className="upload-field-wrap">
                <input
                  className="upload-input"
                  type="file"
                  accept=".csv"
                  onChange={(e) => setTransactionFile(e.target.files?.[0] || null)}
                />
                {transactionFile && (
                  <span className="upload-filename">{transactionFile.name}</span>
                )}
              </div>
              <button
                type="submit"
                disabled={loading}
                className="upload-btn upload-btn-primary"
              >
                {loading ? "Uploading…" : "Upload Transaction"}
              </button>
            </form>
          </Card>

          {/* Custodian file section */}
          <Card className="upload-section-card">
            <form onSubmit={handleCustodianUpload} className="upload-section-form">
              <div className="upload-field-wrap">
                <label className="upload-label">Choose file (.csv)</label>
                <input
                  className="upload-input"
                  type="file"
                  accept=".csv,.xlsx,.xls"
                  onChange={(e) => setCustodianFile(e.target.files?.[0] || null)}
                />
                {custodianFile && (
                  <span className="upload-filename">{custodianFile.name}</span>
                )}
              </div>
              <button
                type="submit"
                disabled={custodianLoading || !result?.data?.length}
                className="upload-btn upload-btn-secondary"
              >
                {custodianLoading ? "Comparing…" : "Upload Custodian"}
              </button>
            </form>
            {custodianError && (
              <p className="upload-section-message upload-error">{custodianError}</p>
            )}
          </Card>
        </div>

        {error && <div className="upload-error">{error}</div>}

        {result && result.success && (
          <Card className="upload-result-card">
            <div className="upload-result">
              <p className="upload-result-message">✅ {result.message}</p>
              {result.count != null && (
                <p className="upload-result-count">Rows: {result.count}</p>
              )}
              {result.data && result.data.length > 0 ? (
                <div className="upload-result-table">
                  <Table columns={RESULT_COLUMNS} data={result.data} />
                </div>
              ) : (
                <p className="upload-result-empty">No rows to display.</p>
              )}
            </div>
          </Card>
        )}

        {comparisonResult && comparisonResult.success && (
          <Card className="upload-result-card">
            <div className="upload-result">
              <p className="upload-result-message">✅ {comparisonResult.message}</p>
              {comparisonResult.count != null && (
                <p className="upload-result-count">Rows: {comparisonResult.count}</p>
              )}
              {comparisonResult.data && comparisonResult.data.length > 0 ? (
                <div className="upload-result-table">
                  <Table columns={COMPARISON_COLUMNS} data={comparisonResult.data} />
                </div>
              ) : (
                <p className="upload-result-empty">No comparison rows to display.</p>
              )}
            </div>
          </Card>
        )}
      </div>
    </MainLayout>
  );
}

export default TransactionUploadPage;