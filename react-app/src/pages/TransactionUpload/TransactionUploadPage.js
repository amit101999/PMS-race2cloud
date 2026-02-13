import React, { useState } from "react";
import MainLayout from "../../layouts/MainLayout";
import { Card } from "../../components/common/CommonComponents";
import { BASE_URL } from "../../constant";
import "./TransactionUploadPage.css";

function TransactionUploadPage() {
  const [transactionFile, setTransactionFile] = useState(null);
  const [uploadLoading, setUploadLoading] = useState(false);
  const [uploadMessage, setUploadMessage] = useState("");
  const [uploadError, setUploadError] = useState("");

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
          ? `File uploaded to Stratus. Bulk insert to Transaction table started (job: ${data.jobId}).`
          : "File uploaded successfully."
      );
    } catch (err) {
      setUploadError(err.message || "Upload failed");
    } finally {
      setUploadLoading(false);
    }
  };

  return (
    <MainLayout title="Transaction Upload">
      <div className="upload-page">
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
      </div>
    </MainLayout>
  );
}

export default TransactionUploadPage;
