
import React, { useState } from "react";
import MainLayout from "../../layouts/MainLayout";
import { FileGlyph, UploadGlyph } from "../TempTransactionUpload/custody/CustodyMatchPanel";
import { BASE_URL } from "../../constant";
import "../TempTransactionUpload/tokens.css";
import "../TempTransactionUpload/TempTransaction.css";
import "../TempTransactionUpload/custody/CustodyMatch.css";
import "../../styles/ops-pages.css";

function BhavCopyPage() {
  const [file, setFile] = useState(null);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState("");
  const [dragActive, setDragActive] = useState(false);

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
      <div className="temp-transaction-container txn-ops ops-page">
        <div className="temp-transaction-card">
          <form onSubmit={handleSubmit}>
            {/* Same upload box as Holding Comparison. The file input covers the
                whole box, so both clicking and dropping a file go through onChange;
                the drag handlers only toggle the highlight. */}
            <div className="ops-upload">
              <label>CSV File</label>
              <div
                className={dragActive ? "up-zone is-drag" : "up-zone"}
                onDragEnter={() => setDragActive(true)}
                onDragLeave={() => setDragActive(false)}
                onDrop={() => setDragActive(false)}
              >
                <input
                  className="up-zone-input"
                  type="file"
                  accept=".csv"
                  aria-label="Upload Bhav Copy CSV"
                  onChange={(e) => setFile(e.target.files?.[0] || null)}
                />
                <div className="up-zone-visual">
                  <UploadGlyph />
                  <span className="up-zone-title">
                    {file ? "Replace Bhav Copy file" : "Upload Bhav Copy file"}
                  </span>
                  <span className="up-zone-sub">
                    Drag &amp; drop or <span className="up-zone-browse">Browse</span>
                  </span>
                </div>
              </div>

              {file ? (
                <div className="up-filelist">
                  <div className="up-file-row">
                    <FileGlyph />
                    <div className="up-file-main">
                      <span className="up-file-name" title={file.name}>
                        {file.name}
                      </span>
                    </div>
                  </div>
                </div>
              ) : null}
            </div>

            <div className="ops-form-actions">
              <button type="submit" disabled={loading} className="cm-primary">
                {loading ? "Uploading..." : "Upload & Import"}
              </button>
            </div>
          </form>

          {error && <div className="cm-verdict bad">{error}</div>}

          {result && (
            <div className="cm-verdict ok">
              <p className="cm-verdict-lead">{result.message}</p>
              <p className="cm-verdict-sub">File: {result.fileName}</p>
              <p className="cm-verdict-sub">Bucket: {result.bucket}</p>
              {result.jobId && <p className="cm-verdict-sub">Job ID: {result.jobId}</p>}
            </div>
          )}
        </div>
      </div>
    </MainLayout>
  );
}

export default BhavCopyPage;