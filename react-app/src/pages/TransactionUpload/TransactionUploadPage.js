import React, { useState, useRef, useEffect } from "react";
import MainLayout from "../../layouts/MainLayout";
import { Card, Table } from "../../components/common/CommonComponents";
import { BASE_URL } from "../../constant";
import "./TransactionUploadPage.css";

const LIMIT = 20;

const HOLDING_COLUMNS = [
  { key: "WS_Account_code", header: "Account Code" },
  { key: "ISIN", header: "ISIN" },
  { key: "Security_Name", header: "Security Name" },
  { key: "Tran_Type", header: "Tran Type" },
  { key: "transactionQty", header: "Transaction Qty" },
  { key: "oldQuantity", header: "Old Qty" },
  { key: "newQuantity", header: "New Qty" },
];

function TransactionUploadPage() {
  const [transactionFile, setTransactionFile] = useState(null);

  const [uploadLoading, setUploadLoading] = useState(false);
  const [uploadMessage, setUploadMessage] = useState("");
  const [uploadError, setUploadError] = useState("");

  const [holdingData, setHoldingData] = useState([]);
  const [holdingLoading, setHoldingLoading] = useState(false);
  const [holdingError, setHoldingError] = useState("");

  // 🔥 Cursor states
  const [cursor, setCursor] = useState(null);
  const [nextCursor, setNextCursor] = useState(null);
  const [cursorStack, setCursorStack] = useState([]);

  // Differential Report
  const [diffLoading, setDiffLoading] = useState(false);
  const [diffMessage, setDiffMessage] = useState("");
  const [diffError, setDiffError] = useState("");
  const [diffJobs, setDiffJobs] = useState([]);
  const pollRef = useRef(null);

  // Fetch history on mount
  useEffect(() => {
    fetchDiffHistory();
  }, []);

  // Poll only PENDING jobs every 10s
  useEffect(() => {
    const hasPending = diffJobs.some((j) => j.status === "PENDING");
    if (!hasPending) {
      if (pollRef.current) clearInterval(pollRef.current);
      pollRef.current = null;
      return;
    }

    pollRef.current = setInterval(async () => {
      const updated = await Promise.all(
        diffJobs.map(async (job) => {
          if (job.status === "COMPLETED" || job.status === "FAILED") return job;
          try {
            const res = await fetch(
              `${BASE_URL}/transaction-uploader/differential-report/status?jobName=${encodeURIComponent(job.jobName)}`
            );
            const data = await res.json();
            return data?.status ? { ...job, status: data.status } : job;
          } catch {
            return job;
          }
        })
      );
      setDiffJobs(updated);
    }, 10000);

    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, [diffJobs]);

  // ==============================
  // Upload Transaction File
  // ==============================
  const handleUpload = async (e) => {
    e.preventDefault();
    setUploadError("");
    setUploadMessage("");

    if (!transactionFile) {
      setUploadError("Please choose a transaction CSV file first.");
      return;
    }

    setUploadLoading(true);

    try {
      const formData = new FormData();
      formData.append("file", transactionFile);

      const res = await fetch(
        `${BASE_URL}/transaction-uploader/upload-transaction`,
        {
          method: "POST",
          body: formData,
        }
      );

      const data = await res.json();

      if (!res.ok) {
        throw new Error(data?.message || "Upload failed");
      }

      setUploadMessage(
        data?.jobId
          ? `File uploaded. Bulk insert started (job: ${data.jobId}).`
          : "File uploaded successfully."
      );

      // Reset holding state
      setHoldingData([]);
      setCursor(null);
      setNextCursor(null);
      setCursorStack([]);

    } catch (err) {
      setUploadError(err.message || "Upload failed");
    } finally {
      setUploadLoading(false);
    }
  };

  // ==============================
  // Fetch Holding (Cursor Based)
  // ==============================
  const fetchHolding = async (cursorValue = null, isNext = true) => {
    setHoldingError("");
    setHoldingLoading(true);

    try {
      const url = cursorValue
        ? `${BASE_URL}/transaction-uploader/load-holding?limit=${LIMIT}&cursor=${cursorValue}`
        : `${BASE_URL}/transaction-uploader/load-holding?limit=${LIMIT}`;

      const res = await fetch(url);
      const data = await res.json();

      if (!res.ok) {
        throw new Error(data?.message || "Failed to load holding");
      }

      setHoldingData(Array.isArray(data.data) ? data.data : []);
      setNextCursor(data.nextCursor || null);

      // Manage previous cursor stack
      if (isNext && cursorValue) {
        setCursorStack((prev) => [...prev, cursorValue]);
      }

      setCursor(cursorValue);

    } catch (err) {
      setHoldingError(err.message || "Failed to load holding");
      setHoldingData([]);
    } finally {
      setHoldingLoading(false);
    }
  };

  const handleLoadHolding = () => {
    setCursor(null);
    setCursorStack([]);
    fetchHolding(null);
  };

  const handleNext = () => {
    if (nextCursor) {
      fetchHolding(nextCursor, true);
    }
  };

  const handlePrevious = () => {
    if (cursorStack.length === 0) return;

    const previousCursor = cursorStack[cursorStack.length - 1];
    setCursorStack((prev) => prev.slice(0, -1));
    fetchHolding(previousCursor, false);
  };

  // ==============================
  // Differential Report: Generate → History → Download
  // ==============================

  const fetchDiffHistory = async () => {
    try {
      const res = await fetch(
        `${BASE_URL}/transaction-uploader/differential-report/history?limit=5`
      );
      const data = await res.json();
      if (Array.isArray(data)) setDiffJobs(data);
    } catch (err) {
      console.error("Failed to load differential history", err);
    }
  };

  const handleGenerateDifferentialReport = async () => {
    setDiffError("");
    setDiffMessage("");
    setDiffLoading(true);

    try {
      const res = await fetch(
        `${BASE_URL}/transaction-uploader/differential-report`,
        { method: "POST" }
      );
      const data = await res.json();

      if (!res.ok) {
        throw new Error(data?.message || "Failed to start report job");
      }

      if (data.status === "COMPLETED") {
        setDiffMessage("Report already exists for today. You can download it below.");
      } else {
        setDiffMessage("Report job started. Status will update automatically.");
      }

      fetchDiffHistory();
    } catch (err) {
      setDiffError(err.message || "Failed to generate differential report");
    } finally {
      setDiffLoading(false);
    }
  };

  const handleDiffDownload = async (jobName) => {
    setDiffError("");
    try {
      const res = await fetch(
        `${BASE_URL}/transaction-uploader/differential-report/download?jobName=${encodeURIComponent(jobName)}`
      );
      const data = await res.json();

      if (!res.ok || !data?.downloadUrl) {
        throw new Error(data?.message || "Download not available");
      }

      const url = typeof data.downloadUrl === "string" ? data.downloadUrl : data.downloadUrl?.signature;
      if (!url) throw new Error("Download not available");
      window.open(url, "_blank");
    } catch (err) {
      setDiffError(err.message || "Failed to download report");
    }
  };

  return (
    <MainLayout title="Transaction Upload">
      <div className="upload-page">

        {/* Upload Section */}
        <Card className="upload-section-card">
          <h3>Transaction File (.csv)</h3>

          <form onSubmit={handleUpload}>
            <input
              type="file"
              accept=".csv"
              onChange={(e) =>
                setTransactionFile(e.target.files?.[0] || null)
              }
            />

            <button
              type="submit"
              disabled={uploadLoading}
              className="upload-btn upload-btn-primary"
            >
              {uploadLoading ? "Uploading…" : "Upload Transaction"}
            </button>
          </form>

          {uploadMessage && (
            <p className="upload-section-message">{uploadMessage}</p>
          )}

          {uploadError && (
            <p className="upload-section-message upload-error">
              {uploadError}
            </p>
          )}
        </Card>

        {/* Load Holding Section */}
        <Card className="upload-result-card">
          <button
            type="button"
            className="upload-btn upload-btn-secondary"
            onClick={handleLoadHolding}
            disabled={holdingLoading}
          >
            {holdingLoading ? "Loading…" : "Load Holding"}
          </button>

          {holdingError && (
            <p className="upload-section-message upload-error">
              {holdingError}
            </p>
          )}

          {holdingData.length > 0 && (
            <>
              <div className="upload-result-table">
                <Table columns={HOLDING_COLUMNS} data={holdingData} />
              </div>

              {/* Cursor Pagination Controls */}
              <div className="pagination-controls">
                <button
                  type="button"
                  className="upload-btn upload-btn-secondary"
                  disabled={cursorStack.length === 0 || holdingLoading}
                  onClick={handlePrevious}
                >
                  Previous
                </button>

                <button
                  type="button"
                  className="upload-btn upload-btn-secondary"
                  disabled={!nextCursor || holdingLoading}
                  onClick={handleNext}
                >
                  Next
                </button>
              </div>
            </>
          )}
        </Card>

        {/* Differential Report (FA vs Custodian) */}
        <Card className="upload-result-card">
          <h3>Differential Report (FA vs Custodian)</h3>

          <button
            type="button"
            className="upload-btn upload-btn-secondary"
            onClick={handleGenerateDifferentialReport}
            disabled={diffLoading}
          >
            {diffLoading ? "Submitting…" : "Generate Differential Report"}
          </button>

          {diffMessage && (
            <p className="upload-section-message">{diffMessage}</p>
          )}
          {diffError && (
            <p className="upload-section-message upload-error">{diffError}</p>
          )}

          {diffJobs.length > 0 && (
            <div className="diff-history-container">
              <h4>Previous Comparison Reports</h4>

              <table className="diff-history-table">
                <thead>
                  <tr>
                    <th>Job Name</th>
                    <th>Date</th>
                    <th>Status</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {diffJobs.map((job) => (
                    <tr key={job.jobName}>
                      <td>{job.jobName}</td>
                      <td>{job.date}</td>
                      <td>
                        <span
                          className={`diff-status-badge ${
                            job.status === "COMPLETED"
                              ? "completed"
                              : job.status === "FAILED"
                              ? "failed"
                              : "pending"
                          }`}
                        >
                          {job.status}
                        </span>
                      </td>
                      <td>
                        {job.status === "COMPLETED" ? (
                          <button
                            className="diff-download-btn"
                            onClick={() => handleDiffDownload(job.jobName)}
                          >
                            Download
                          </button>
                        ) : (
                          <span style={{ color: "#9ca3af" }}>—</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Card>

      </div>
    </MainLayout>
  );
}

export default TransactionUploadPage;
