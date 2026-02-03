import React, { useState, useEffect } from "react";
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

const COMPARISON_EDITABLE_KEYS = new Set([
  "Qty Cust",
  "Qty FA",
  "Mismatch Reason",
  "Matching",
]);

function TransactionUploadPage() {
  const [transactionFile, setTransactionFile] = useState(null);
  const [custodianFile, setCustodianFile] = useState(null);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState("");
  const [comparisonResult, setComparisonResult] = useState(null);
  const [comparisonData, setComparisonData] = useState([]);
  const [custodianLoading, setCustodianLoading] = useState(false);
  const [custodianError, setCustodianError] = useState("");
  const [finalUploadLoading, setFinalUploadLoading] = useState(false);
  const [finalUploadMessage, setFinalUploadMessage] = useState("");

  useEffect(() => {
    if (comparisonResult?.success && Array.isArray(comparisonResult.data)) {
      setComparisonData(comparisonResult.data.map((row) => ({ ...row })));
    } else {
      setComparisonData([]);
    }
  }, [comparisonResult]);

  const handleComparisonCellChange = (rowIndex, key, value) => {
    setComparisonData((prev) => {
      const next = prev.map((row, i) =>
        i === rowIndex ? { ...row, [key]: value } : row
      );
      return next;
    });
  };

  const handleUploadFinalTransaction = async () => {
    if (!transactionFile) {
      setFinalUploadMessage("Please select a transaction file first (use the Transaction File section above).");
      return;
    }
    setFinalUploadMessage("");
    setFinalUploadLoading(true);
    try {
      const formData = new FormData();
      formData.append("file", transactionFile);
      const res = await fetch(`${BASE_URL}/transaction-uploader/upload-transaction`, {
        method: "POST",
        body: formData,
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.message || data?.error || "Upload failed");
      setFinalUploadMessage(
        data?.jobId
          ? `File uploaded to Stratus. Import job created (${data.jobId}).`
          : "Transaction file uploaded to Stratus and import job started."
      );
    } catch (err) {
      setFinalUploadMessage(err.message || "Upload failed");
    } finally {
      setFinalUploadLoading(false);
    }
  };

  const downloadComparisonCsv = () => {
    if (!comparisonData.length) return;
    const headers = COMPARISON_COLUMNS.map((c) => c.key);
    const escape = (v) => `"${String(v ?? "").replace(/"/g, '""')}"`;
    const headerLine = headers.map(escape).join(",");
    const dataLines = comparisonData.map((row) =>
      headers.map((h) => escape(row[h])).join(",")
    );
    const csv = [headerLine, ...dataLines].join("\n");
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `comparison-report-${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

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
                <p className="upload-result-count">Rows: {comparisonData.length}</p>
              )}
              {comparisonData.length > 0 ? (
                <>
                  <div className="upload-result-table comparison-editable-table">
                    <table className="comparison-table">
                      <thead>
                        <tr>
                          {COMPARISON_COLUMNS.map((col) => (
                            <th key={col.key}>{col.header}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {comparisonData.map((row, rowIndex) => (
                          <tr key={rowIndex}>
                            {COMPARISON_COLUMNS.map((col) => (
                              <td key={col.key}>
                                {COMPARISON_EDITABLE_KEYS.has(col.key) ? (
                                  <input
                                    type="text"
                                    className="comparison-cell-input"
                                    value={row[col.key] ?? ""}
                                    onChange={(e) =>
                                      handleComparisonCellChange(rowIndex, col.key, e.target.value)
                                    }
                                  />
                                ) : (
                                  <span className="comparison-cell-readonly">
                                    {row[col.key] ?? ""}
                                  </span>
                                )}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  <div className="comparison-actions">
                    <button
                      type="button"
                      className="upload-btn upload-btn-primary"
                      onClick={downloadComparisonCsv}
                    >
                      Download report (CSV)
                    </button>
                    <button
                      type="button"
                      className="upload-btn upload-btn-secondary"
                      onClick={handleUploadFinalTransaction}
                      disabled={finalUploadLoading || !transactionFile}
                    >
                      {finalUploadLoading ? "Uploading…" : "Upload final transaction"}
                    </button>
                  </div>
                  {finalUploadMessage && (
                    <p
                      className={
                        finalUploadMessage.includes("uploaded") || finalUploadMessage.includes("job")
                          ? "upload-section-message"
                          : "upload-section-message upload-error"
                      }
                    >
                      {finalUploadMessage}
                    </p>
                  )}
                </>
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