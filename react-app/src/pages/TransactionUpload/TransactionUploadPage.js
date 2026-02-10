import React, { useState, useEffect, useRef } from "react";
import MainLayout from "../../layouts/MainLayout";
import { Card, Table } from "../../components/common/CommonComponents";
import { BASE_URL } from "../../constant";
import "./TransactionUploadPage.css";

const FIFO_PAGE_SIZE = 20;
const DIFF_PAGE_SIZE = 20;
const EXPORT_POLL_INTERVAL_MS = 2500;
const RECENT_EXPORTS_POLL_MS = 15000;

function formatExportLabel(jobName) {
  const match = String(jobName).match(/^DifferentialReport_(\d+)$/);
  if (!match) return jobName;
  const ts = parseInt(match[1], 10);
  const d = new Date(ts);
  return d.toLocaleString(undefined, {
    day: "numeric",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

const FIFO_COLUMNS = [
  { key: "WS_Account_code", header: "Account Code" },
  { key: "ISIN", header: "ISIN" },
  { key: "Security_code", header: "Security Code" },
  { key: "Security_Name", header: "Security Name" },
  { key: "Tran_Type", header: "Tran Type" },
  { key: "transactionQty", header: "Transaction Qty" },
  { key: "oldQuantity", header: "Old Qty" },
  { key: "newQuantity", header: "New Qty" },
];

const DIFF_COLUMNS = [
  { key: "UCC", header: "UCC" },
  { key: "ISIN", header: "ISIN" },
  { key: "Qty_FA", header: "Qty FA" },
  { key: "Qty_Cust", header: "Qty Cust" },
  { key: "Diff", header: "Diff" },
  { key: "Matching", header: "Matching" },
  { key: "Mismatch_Reason", header: "Mismatch Reason" },
  { key: "Security", header: "Security" },
  { key: "ClientName", header: "Client Name" },
  { key: "LTPDate", header: "LTP Date" },
  { key: "LTPPrice", header: "LTP Price" },
];

function TransactionUploadPage() {
  const [transactionFile, setTransactionFile] = useState(null);
  const [custodianFile, setCustodianFile] = useState(null);

  const [txUploadLoading, setTxUploadLoading] = useState(false);
  const [txUploadMessage, setTxUploadMessage] = useState("");
  const [txUploadError, setTxUploadError] = useState("");

  const [custUploadLoading, setCustUploadLoading] = useState(false);
  const [custUploadMessage, setCustUploadMessage] = useState("");
  const [custUploadError, setCustUploadError] = useState("");

  const [fifoPage, setFifoPage] = useState(1);
  const [fifoPageSize] = useState(FIFO_PAGE_SIZE);
  const [fifoTotalRows, setFifoTotalRows] = useState(0);
  const [fifoData, setFifoData] = useState([]);
  const [fifoLoading, setFifoLoading] = useState(false);
  const [fifoError, setFifoError] = useState("");

  const [diffPage, setDiffPage] = useState(1);
  const [diffPageSize] = useState(DIFF_PAGE_SIZE);
  const [diffTotalRows, setDiffTotalRows] = useState(0);
  const [diffData, setDiffData] = useState([]);
  const [diffLoading, setDiffLoading] = useState(false);
  const [diffError, setDiffError] = useState("");

  const [exportJobName, setExportJobName] = useState(null);
  const [exportStatus, setExportStatus] = useState(null);
  const [exportDownloadUrl, setExportDownloadUrl] = useState(null);
  const [exportError, setExportError] = useState("");
  const exportPollRef = useRef(null);

  const [recentExports, setRecentExports] = useState([]);
  const [recentExportsLoading, setRecentExportsLoading] = useState(false);

  const fifoTotalPages = Math.max(1, Math.ceil(fifoTotalRows / fifoPageSize));
  const diffTotalPages = Math.max(1, Math.ceil(diffTotalRows / diffPageSize));

  const fetchFifoPage = async (page = fifoPage) => {
    setFifoError("");
    setFifoLoading(true);
    try {
      const res = await fetch(
        `${BASE_URL}/transaction-uploader/fifo-page?page=${page}&pageSize=${fifoPageSize}`
      );
      const data = await res.json();
      if (!res.ok) throw new Error(data?.message || data?.error || "FIFO request failed");
      setFifoData(data.data || []);
      setFifoTotalRows(data.totalRows ?? 0);
      setFifoPage(data.page ?? page);
    } catch (err) {
      setFifoError(err.message || "Failed to load FIFO");
      setFifoData([]);
    } finally {
      setFifoLoading(false);
    }
  };

  const fetchDiffPage = async (page = diffPage) => {
    setDiffError("");
    setDiffLoading(true);
    try {
      const res = await fetch(
        `${BASE_URL}/transaction-uploader/diff-page?page=${page}&pageSize=${diffPageSize}`
      );
      const data = await res.json();
      if (!res.ok) throw new Error(data?.message || data?.error || "Diff request failed");
      setDiffData(data.data || []);
      setDiffTotalRows(data.totalRows ?? 0);
      setDiffPage(data.page ?? page);
    } catch (err) {
      setDiffError(err.message || "Failed to load comparison");
      setDiffData([]);
    } finally {
      setDiffLoading(false);
    }
  };

  const fetchRecentExports = async () => {
    setRecentExportsLoading(true);
    try {
      const listRes = await fetch(`${BASE_URL}/transaction-uploader/export-differential-report/list`);
      const listData = await listRes.json();
      if (!listRes.ok || !listData.jobs) {
        setRecentExports([]);
        return;
      }
      const jobs = listData.jobs || [];
      const withStatus = await Promise.all(
        jobs.map(async (j) => {
          const statusRes = await fetch(
            `${BASE_URL}/transaction-uploader/export-differential-report/status?jobName=${encodeURIComponent(j.jobName)}`
          );
          const statusData = await statusRes.json();
          return {
            jobName: j.jobName,
            status: statusData.status || j.status,
            downloadUrl: statusData.downloadUrl || null,
          };
        })
      );
      setRecentExports(withStatus);
    } catch {
      setRecentExports([]);
    } finally {
      setRecentExportsLoading(false);
    }
  };

  useEffect(() => {
    fetchRecentExports();
  }, []);

  const handleTransactionUpload = async (e) => {
    e.preventDefault();
    setTxUploadError("");
    setTxUploadMessage("");
    if (!transactionFile) {
      setTxUploadError("Please choose a transaction CSV file first.");
      return;
    }
    setTxUploadLoading(true);
    try {
      const formData = new FormData();
      formData.append("file", transactionFile);
      const res = await fetch(`${BASE_URL}/transaction-uploader/upload-temp-transaction`, {
        method: "POST",
        body: formData,
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.message || data?.error || "Upload failed");
      setTxUploadMessage(
        data?.jobId
          ? `File uploaded. Bulk insert started (job: ${data.jobId}). You can load FIFO results below.`
          : "Transaction file uploaded successfully."
      );
      setFifoPage(1);
      setFifoTotalRows(0);
      setFifoData([]);
    } catch (err) {
      setTxUploadError(err.message || "Upload failed");
    } finally {
      setTxUploadLoading(false);
    }
  };

  const handleCustodianUpload = async (e) => {
    e.preventDefault();
    setCustUploadError("");
    setCustUploadMessage("");
    if (!custodianFile) {
      setCustUploadError("Please choose a custodian file first.");
      return;
    }
    setCustUploadLoading(true);
    try {
      const formData = new FormData();
      formData.append("file", custodianFile);
      const res = await fetch(`${BASE_URL}/transaction-uploader/upload-temp-custodian`, {
        method: "POST",
        body: formData,
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.message || data?.error || "Upload failed");
      setCustUploadMessage(
        data?.jobId
          ? `File uploaded. Bulk insert started (job: ${data.jobId}). You can load comparison below.`
          : "Custodian file uploaded successfully."
      );
      setDiffPage(1);
      setDiffTotalRows(0);
      setDiffData([]);
    } catch (err) {
      setCustUploadError(err.message || "Upload failed");
    } finally {
      setCustUploadLoading(false);
    }
  };

  const handleLoadFifo = () => fetchFifoPage(1);
  const handleLoadDiff = () => fetchDiffPage(1);

  const startDifferentialExport = async () => {
    setExportError("");
    setExportStatus(null);
    setExportDownloadUrl(null);
    setExportJobName(null);
    try {
      const res = await fetch(`${BASE_URL}/transaction-uploader/export-differential-report`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.message || data?.error || "Failed to start export");
      const jobName = data.jobName;
      if (!jobName) throw new Error("No job name returned");
      setExportJobName(jobName);
      setExportStatus("PENDING");
      setRecentExports((prev) => [
        { jobName, status: "PENDING", downloadUrl: null },
        ...prev.filter((e) => e.jobName !== jobName),
      ]);
    } catch (err) {
      setExportError(err.message || "Failed to start export");
    }
  };

  useEffect(() => {
    if (exportStatus !== "PENDING" || !exportJobName) return;
    const jobName = exportJobName;
    const poll = async () => {
      try {
        const res = await fetch(
          `${BASE_URL}/transaction-uploader/export-differential-report/status?jobName=${encodeURIComponent(jobName)}`
        );
        const data = await res.json();
        if (!res.ok) {
          setExportStatus("FAILED");
          setExportError(data?.message || "Status check failed");
          setRecentExports((prev) =>
            prev.map((e) => (e.jobName === jobName ? { ...e, status: "FAILED" } : e))
          );
          return;
        }
        const status = data.status;
        if (status === "COMPLETED" && data.downloadUrl) {
          setExportDownloadUrl(data.downloadUrl);
          setExportStatus("COMPLETED");
          setRecentExports((prev) =>
            prev.map((e) =>
              e.jobName === jobName
                ? { ...e, status: "COMPLETED", downloadUrl: data.downloadUrl }
                : e
            )
          );
          return;
        }
        if (status === "COMPLETED") {
          setExportStatus("COMPLETED");
          setExportError("Download link expired or report older than 5 days.");
          setRecentExports((prev) =>
            prev.map((e) => (e.jobName === jobName ? { ...e, status: "COMPLETED" } : e))
          );
          return;
        }
        if (status === "FAILED" || status === "NOT_FOUND") {
          setExportStatus(status === "NOT_FOUND" ? "NOT_FOUND" : "FAILED");
          setExportError(status === "NOT_FOUND" ? "Report not found." : "Export failed.");
          setRecentExports((prev) =>
            prev.map((e) =>
              e.jobName === jobName ? { ...e, status: status === "NOT_FOUND" ? "NOT_FOUND" : "FAILED" } : e
            )
          );
          return;
        }
      } catch (err) {
        setExportStatus("FAILED");
        setExportError(err.message || "Status check failed");
        setRecentExports((prev) =>
          prev.map((e) => (e.jobName === jobName ? { ...e, status: "FAILED" } : e))
        );
      }
    };
    exportPollRef.current = setInterval(poll, EXPORT_POLL_INTERVAL_MS);
    poll();
    return () => {
      if (exportPollRef.current) clearInterval(exportPollRef.current);
    };
  }, [exportStatus, exportJobName]);

  const hasPendingExports = recentExports.some((e) => e.status === "PENDING");
  useEffect(() => {
    if (!hasPendingExports) return;
    const interval = setInterval(fetchRecentExports, RECENT_EXPORTS_POLL_MS);
    return () => clearInterval(interval);
  }, [hasPendingExports]);

  const exportInProgress = exportStatus === "PENDING";
  const exportReady = exportStatus === "COMPLETED" && exportDownloadUrl;

  const handleGetDownloadReport = (url) => {
    const toOpen = url || exportDownloadUrl;
    if (toOpen) window.open(toOpen, "_blank");
  };

  const downloadButtonLabel = exportInProgress
    ? "Preparing…"
    : exportReady
    ? "Get download report"
    : "Download full report (CSV)";

  return (
    <MainLayout title="Transaction Upload">
      <div className="upload-page">
        <div className="upload-sections">
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
                disabled={txUploadLoading}
                className="upload-btn upload-btn-primary"
              >
                {txUploadLoading ? "Uploading…" : "Upload Transaction"}
              </button>
            </form>
            {txUploadMessage && (
              <p className="upload-section-message">{txUploadMessage}</p>
            )}
            {txUploadError && (
              <p className="upload-section-message upload-error">{txUploadError}</p>
            )}
          </Card>

          <Card className="upload-section-card">
            <div className="upload-section-header">
              <h3 className="upload-section-title">Custodian File (.csv)</h3>
            </div>
            <form onSubmit={handleCustodianUpload} className="upload-section-form">
              <div className="upload-field-wrap">
                <label className="upload-label">Choose file (.csv)</label>
                <input
                  className="upload-input"
                  type="file"
                  accept=".csv"
                  onChange={(e) => setCustodianFile(e.target.files?.[0] || null)}
                />
                {custodianFile && (
                  <span className="upload-filename">{custodianFile.name}</span>
                )}
              </div>
              <button
                type="submit"
                disabled={custUploadLoading}
                className="upload-btn upload-btn-secondary"
              >
                {custUploadLoading ? "Uploading…" : "Upload Custodian"}
              </button>
            </form>
            {custUploadMessage && (
              <p className="upload-section-message">{custUploadMessage}</p>
            )}
            {custUploadError && (
              <p className="upload-section-message upload-error">{custUploadError}</p>
            )}
          </Card>
        </div>

        {/* FIFO section */}
        <Card className="upload-result-card">
          <div className="upload-result">
            <button
              type="button"
              className="upload-btn upload-btn-secondary"
              onClick={handleLoadFifo}
              disabled={fifoLoading}
            >
              {fifoLoading ? "Loading…" : "Load Holding report"}
            </button>
            {fifoError && (
              <p className="upload-section-message upload-error">{fifoError}</p>
            )}
            {fifoData.length > 0 && (
              <>
                <p className="upload-result-count">
                  Page {fifoPage} of {fifoTotalPages} ({fifoTotalRows} total rows)
                </p>
                <div className="upload-result-table">
                  <Table columns={FIFO_COLUMNS} data={fifoData} />
                </div>
                <div className="pagination-controls">
                  <button
                    type="button"
                    className="upload-btn upload-btn-secondary pagination-btn"
                    disabled={fifoPage <= 1 || fifoLoading}
                    onClick={() => fetchFifoPage(fifoPage - 1)}
                  >
                    Previous
                  </button>
                  <span className="pagination-info">
                    Page {fifoPage} of {fifoTotalPages}
                  </span>
                  <button
                    type="button"
                    className="upload-btn upload-btn-secondary pagination-btn"
                    disabled={fifoPage >= fifoTotalPages || fifoLoading}
                    onClick={() => fetchFifoPage(fifoPage + 1)}
                  >
                    Next
                  </button>
                </div>
              </>
            )}
           
          </div>
        </Card>

        {/* Diff / Comparison section */}
        <Card className="upload-result-card">
          <div className="upload-result">
            <h3 className="upload-result-title">Comparison (FA vs Custodian)</h3>
            <button
              type="button"
              className="upload-btn upload-btn-secondary"
              onClick={handleLoadDiff}
              disabled={diffLoading}
            >
              {diffLoading ? "Loading…" : "Load Comparison"}
            </button>
            {diffError && (
              <p className="upload-section-message upload-error">{diffError}</p>
            )}
            {diffData.length > 0 && (
              <>
                <p className="upload-result-count">
                  Page {diffPage} of {diffTotalPages} ({diffTotalRows} total rows)
                </p>
                <div className="upload-result-table comparison-table-wrap">
                  <table className="comparison-table">
                    <thead>
                      <tr>
                        {DIFF_COLUMNS.map((col) => (
                          <th key={col.key}>{col.header}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {diffData.map((row, rowIndex) => (
                        <tr
                          key={rowIndex}
                          className={
                            row.Matching === "No" || row.Mismatch_Reason
                              ? "diff-row-mismatch"
                              : ""
                          }
                        >
                          {DIFF_COLUMNS.map((col) => (
                            <td key={col.key}>{row[col.key] ?? ""}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <div className="pagination-controls">
                  <button
                    type="button"
                    className="upload-btn upload-btn-secondary pagination-btn"
                    disabled={diffPage <= 1 || diffLoading}
                    onClick={() => fetchDiffPage(diffPage - 1)}
                  >
                    Previous
                  </button>
                  <span className="pagination-info">
                    Page {diffPage} of {diffTotalPages}
                  </span>
                  <button
                    type="button"
                    className="upload-btn upload-btn-secondary pagination-btn"
                    disabled={diffPage >= diffTotalPages || diffLoading}
                    onClick={() => fetchDiffPage(diffPage + 1)}
                  >
                    Next
                  </button>
                </div>
                <div className="comparison-actions">
                  {!exportReady ? (
                    <button
                      type="button"
                      className="upload-btn upload-btn-primary"
                      onClick={startDifferentialExport}
                      disabled={exportInProgress || !diffData.length}
                    >
                      {downloadButtonLabel}
                    </button>
                  ) : (
                    <button
                      type="button"
                      className="upload-btn upload-btn-primary"
                      onClick={handleGetDownloadReport}
                    >
                      Get download report
                    </button>
                  )}
                  {exportError && (
                    <p className="upload-section-message upload-error">{exportError}</p>
                  )}
                </div>
              </>
            )}

            <div className="recent-exports-section">
              <h3 className="upload-result-title">Recent exports</h3>
              <p className="upload-section-message">
                Exports from the last 5 days. When an export completes, use &quot;Get download report&quot; to download.
              </p>
              <button
                type="button"
                className="upload-btn upload-btn-secondary"
                onClick={fetchRecentExports}
                disabled={recentExportsLoading}
              >
                {recentExportsLoading ? "Loading…" : "Refresh list"}
              </button>
              {recentExports.length === 0 && !recentExportsLoading && (
                <p className="upload-section-message">No recent exports.</p>
              )}
              {recentExports.length > 0 && (
                <ul className="recent-exports-list">
                  {recentExports.map((exp) => (
                    <li key={exp.jobName} className="recent-export-item">
                      <span className="recent-export-label">{formatExportLabel(exp.jobName)}</span>
                      <span
                        className={`recent-export-status status-${(exp.status || "").toLowerCase()}`}
                      >
                        {exp.status === "PENDING" && "Pending"}
                        {exp.status === "COMPLETED" && "Completed"}
                        {(exp.status === "FAILED" || exp.status === "NOT_FOUND") && "Failed"}
                      </span>
                      {exp.status === "COMPLETED" && exp.downloadUrl && (
                        <button
                          type="button"
                          className="upload-btn upload-btn-primary recent-export-download"
                          onClick={() => handleGetDownloadReport(exp.downloadUrl)}
                        >
                          Get download report
                        </button>
                      )}
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </div>
        </Card>
      </div>
    </MainLayout>
  );
}

export default TransactionUploadPage;
