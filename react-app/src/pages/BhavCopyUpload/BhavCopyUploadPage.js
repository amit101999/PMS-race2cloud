
import React, { useState } from "react";
import MainLayout from "../../layouts/MainLayout";
import { Card } from "../../components/common/CommonComponents";
import { BASE_URL } from "../../constant";
import "./BhavCopyUploadPage.css";

function BhavCopyPage() {
  const [file, setFile] = useState(null);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState("");

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError("");
    setResult(null);
    if (!file) {
      setError("Please choose a CSV file.");
      return;
    }

    const formData = new FormData();
    formData.append("file", file);

    setLoading(true);
    try {
      const res = await fetch(`${BASE_URL}/bhav/upload-bhav`, {
        method: "POST",
        body: formData,
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.message || "Upload failed");
      setResult(data);
    } catch (err) {
      setError(err.message || "Upload failed");
    } finally {
      setLoading(false);
    }
  };

  return (
    <MainLayout title="Bhav Copy Upload">
      <Card className="upload-card">
        <div className="upload-wrapper">
          <form onSubmit={handleSubmit} className="upload-form">
            <div className="upload-field">
              <label className="upload-label">CSV File</label>
              <input
                className="upload-input"
                type="file"
                accept=".csv"
                onChange={(e) => setFile(e.target.files?.[0] || null)}
              />
            </div>
            <button type="submit" disabled={loading} className="upload-button">
              {loading ? "Uploading..." : "Upload & Import"}
            </button>
          </form>

          {error && <div className="upload-error">{error}</div>}

          {result && (
            <div className="upload-result">
              <p>✅ {result.message}</p>
              <p>File: {result.fileName}</p>
              <p>Bucket: {result.bucket}</p>
              {result.jobId && <p>Job ID: {result.jobId}</p>}
            </div>
          )}
        </div>
      </Card>
    </MainLayout>
  );
}

export default BhavCopyPage;