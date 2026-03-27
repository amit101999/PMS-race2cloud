import React, { useState } from "react";
import MainLayout from "../../layouts/MainLayout";
import { BASE_URL } from "../../constant";
import "./TempTransaction.css";

const TempTransaction = () => {
    const [file, setFile] = useState(null);
    const [uploadLoading, setUploadLoading] = useState(false);
    const [uploadMessage, setUploadMessage] = useState("");
    const [uploadError, setUploadError] = useState("");

    const handleFileChange = (e) => {
        setFile(e.target.files[0] || null);
    };

    const handleUpload = async () => {
        setUploadError("");
        setUploadMessage("");

        if (!file) {
            setUploadError("Please choose a transaction CSV file first.");
            return;
        }

        setUploadLoading(true);

        try {
            const formData = new FormData();
            formData.append("file", file);

            const res = await fetch(
                `${BASE_URL}/transaction-uploader/upload-temp-file`,
                {
                    method: "POST",
                    body: formData,
                }
            );

            const data = await res.json();

            if (!res.ok) {
                throw new Error(data?.message || "Upload failed");
            }

            setUploadMessage("File uploaded successfully.");
        } catch (err) {
            setUploadError(err.message || "Upload failed");
        } finally {
            setUploadLoading(false);
        }
    };

    return (
        <MainLayout title="Transaction Upload">
            <div className="temp-transaction-container">
                <div className="temp-transaction-card">
                    <h3>Transaction File (.csv)</h3>
                    <div className="upload-controls">
                        <div className="file-input-wrapper">
                            <input
                                type="file"
                                accept=".csv"
                                onChange={handleFileChange}
                                id="file-upload"
                            />
                        </div>
                        <button
                            className="upload-button"
                            onClick={handleUpload}
                            disabled={!file || uploadLoading}
                        >
                            {uploadLoading ? "Uploading..." : "Upload Transaction"}
                        </button>
                    </div>

                    {uploadMessage && (
                        <p style={{ marginTop: '20px', color: '#059669', fontSize: '14px', padding: '10px', backgroundColor: '#f0fdf4', borderRadius: '8px' }}>
                            {uploadMessage}
                        </p>
                    )}

                    {uploadError && (
                        <p style={{ marginTop: '20px', color: '#b91c1c', fontSize: '14px', padding: '10px', backgroundColor: '#fee2e2', borderRadius: '8px' }}>
                            {uploadError}
                        </p>
                    )}
                </div>
            </div>
        </MainLayout>
    );
};

export default TempTransaction;