// src/pages/BhavCopyPage/BhavCopyPage.js
import React, { useState } from "react";
import MainLayout from "../../layouts/MainLayout";
import { Card } from "../../components/common/CommonComponents";
import { BASE_URL } from "../../constant";

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
      <Card style={{ padding: "20px", display: "flex", flexDirection: "column", gap: "12px" }}>
        <form
          onSubmit={handleSubmit}
          style={{ display: "flex", gap: "12px", alignItems: "center", flexWrap: "wrap" }}
        >
          <div style={{ display: "flex", flexDirection: "column", gap: "6px" }}>
            <label style={{ fontWeight: 600 }}>CSV File</label>
            <input
              type="file"
              accept=".csv"
              onChange={(e) => setFile(e.target.files?.[0] || null)}
            />
          </div>
          <button
            type="submit"
            disabled={loading}
            style={{
              background: "#4f46e5",
              color: "#fff",
              border: "none",
              borderRadius: "8px",
              padding: "10px 16px",
              cursor: loading ? "not-allowed" : "pointer",
              opacity: loading ? 0.7 : 1,
            }}
          >
            {loading ? "Uploading..." : "Upload & Import"}
          </button>
        </form>

        {error && <div style={{ color: "#dc2626", fontWeight: 600 }}>{error}</div>}

        {result && (
          <div style={{ background: "#f3f4f6", borderRadius: "8px", padding: "12px 14px" }}>
            <p>✅ {result.message}</p>
            <p>File: {result.fileName}</p>
            <p>Bucket: {result.bucket}</p>
            {result.jobId && <p>Job ID: {result.jobId}</p>}
            
          </div>
        )}
      </Card>
    </MainLayout>
  );
}

export default BhavCopyPage;