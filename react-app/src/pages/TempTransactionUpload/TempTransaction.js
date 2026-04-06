import React, { useState } from "react";
import MainLayout from "../../layouts/MainLayout";
import { BASE_URL } from "../../constant";
import "./TempTransaction.css";

/**
 * @typedef {Object} HeaderMismatch
 * @property {number} columnIndex
 * @property {string} expected
 * @property {string} actual
 */

/**
 * @typedef {Object} UploadErrorDetail
 * @property {string} message
 * @property {string} [code]
 * @property {string} [kind]
 * @property {string[]} [missingHeaders]
 * @property {HeaderMismatch[]} [mismatches]
 * @property {number} [expectedCount]
 * @property {number} [actualCount]
 * @property {string[]} [expectedHeaders]
 * @property {string} [hint]
 * @property {string[]} [fields]
 * @property {string[]} [headerNamesToFix]
 * @property {number} [row]
 * @property {number} [dataRowIndex]
 * @property {number} [sampleRowsValidated]
 */

const TempTransaction = () => {
    const [file, setFile] = useState(null);
    const [uploadLoading, setUploadLoading] = useState(false);
    const [uploadMessage, setUploadMessage] = useState("");
    const [uploadError, setUploadError] = useState("");
    /** @type {[UploadErrorDetail | null, React.Dispatch<React.SetStateAction<UploadErrorDetail | null>>]} */
    const [uploadErrorDetail, setUploadErrorDetail] = useState(null);

    const handleFileChange = (e) => {
        setFile(e.target.files[0] || null);
        setUploadError("");
        setUploadErrorDetail(null);
        setUploadMessage("");
    };

    const handleUpload = async () => {
        setUploadError("");
        setUploadErrorDetail(null);
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

            let data = {};
            try {
                const text = await res.text();
                if (text) {
                    data = JSON.parse(text);
                }
            } catch {
                data = {};
            }

            if (!res.ok) {
                /** @type {UploadErrorDetail} */
                const detail = {
                    message:
                        data?.message ||
                        `Upload failed (${res.status}). Please try again.`,
                    code: data?.code,
                    kind: data?.kind,
                    missingHeaders: data?.missingHeaders,
                    mismatches: data?.mismatches,
                    expectedCount: data?.expectedCount,
                    actualCount: data?.actualCount,
                    expectedHeaders: data?.expectedHeaders,
                    hint: data?.hint,
                    fields: data?.fields,
                    headerNamesToFix: data?.headerNamesToFix,
                    row: data?.row,
                    dataRowIndex: data?.dataRowIndex,
                    sampleRowsValidated: data?.sampleRowsValidated,
                };
                setUploadErrorDetail(detail);
                setUploadError(detail.message);
                return;
            }

            setUploadMessage("File uploaded successfully.");
        } catch (err) {
            const isNetwork =
                err instanceof TypeError ||
                String(err?.message || "").toLowerCase().includes("failed to fetch");
            setUploadError(
                isNetwork
                    ? "Could not reach the upload server."
                    : err.message || "Upload failed"
            );
            setUploadErrorDetail(null);
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
                        <div
                            role="alert"
                            style={{
                                marginTop: "20px",
                                color: "#b91c1c",
                                fontSize: "14px",
                                padding: "12px",
                                backgroundColor: "#fee2e2",
                                borderRadius: "8px",
                            }}
                        >
                            <p style={{ margin: "0 0 8px", whiteSpace: "pre-line" }}>
                                {uploadError}
                            </p>
                            {uploadErrorDetail?.code === "WRONG_COLUMN_ORDER" &&
                            uploadErrorDetail?.kind === "COUNT" &&
                            uploadErrorDetail.expectedCount != null &&
                            uploadErrorDetail.actualCount != null ? (
                                <p style={{ margin: "8px 0 0", fontSize: "13px" }}>
                                    <strong>Template columns:</strong>{" "}
                                    {uploadErrorDetail.expectedCount} &nbsp;|&nbsp;{" "}
                                    <strong>Your row 1 columns:</strong>{" "}
                                    {uploadErrorDetail.actualCount}
                                </p>
                            ) : null}
                            {uploadErrorDetail?.mismatches?.length ? (
                                <div style={{ marginTop: "12px" }}>
                                    <strong style={{ display: "block", marginBottom: "8px" }}>
                                        Wrong column order or header name — fix these positions
                                        (column # = left to right in row 1):
                                    </strong>
                                    <table
                                        style={{
                                            width: "100%",
                                            borderCollapse: "collapse",
                                            fontSize: "13px",
                                            background: "#fef2f2",
                                            borderRadius: "6px",
                                            overflow: "hidden",
                                        }}
                                    >
                                        <thead>
                                            <tr style={{ textAlign: "left", background: "#fecaca" }}>
                                                <th style={{ padding: "8px" }}>#</th>
                                                <th style={{ padding: "8px" }}>Expected header</th>
                                                <th style={{ padding: "8px" }}>Your header</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {uploadErrorDetail.mismatches.map((m) => (
                                                <tr key={m.columnIndex}>
                                                    <td style={{ padding: "8px", fontWeight: 600 }}>
                                                        {m.columnIndex}
                                                    </td>
                                                    <td style={{ padding: "8px" }}>
                                                        <code
                                                            style={{
                                                                background: "#fee2e2",
                                                                padding: "2px 6px",
                                                                borderRadius: "4px",
                                                            }}
                                                        >
                                                            {m.expected}
                                                        </code>
                                                    </td>
                                                    <td style={{ padding: "8px" }}>
                                                        <code
                                                            style={{
                                                                background: "#fecaca",
                                                                padding: "2px 6px",
                                                                borderRadius: "4px",
                                                            }}
                                                        >
                                                            {m.actual || "(empty)"}
                                                        </code>
                                                    </td>
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            ) : null}
                            {uploadErrorDetail?.missingHeaders?.length ? (
                                <div style={{ marginTop: "10px" }}>
                                    <strong style={{ display: "block", marginBottom: "6px" }}>
                                        Update row 1 — use these headers exactly (same spelling and
                                        capital letters):
                                    </strong>
                                    <ul
                                        style={{
                                            margin: 0,
                                            paddingLeft: "1.25rem",
                                            lineHeight: 1.5,
                                        }}
                                    >
                                        {uploadErrorDetail.missingHeaders.map((h) => (
                                            <li key={h}>
                                                <code
                                                    style={{
                                                        background: "#fecaca",
                                                        padding: "2px 6px",
                                                        borderRadius: "4px",
                                                        fontSize: "13px",
                                                    }}
                                                >
                                                    {h}
                                                </code>
                                            </li>
                                        ))}
                                    </ul>
                                </div>
                            ) : null}
                            {uploadErrorDetail?.fields?.length ? (
                                <div style={{ margin: "10px 0 0", fontSize: "13px" }}>
                                    <p style={{ margin: "0 0 6px" }}>
                                        <strong>Date columns to fix:</strong>{" "}
                                        {uploadErrorDetail.fields.map((f) => (
                                            <code
                                                key={f}
                                                style={{
                                                    background: "#fecaca",
                                                    padding: "2px 6px",
                                                    borderRadius: "4px",
                                                    marginRight: "6px",
                                                }}
                                            >
                                                {f}
                                            </code>
                                        ))}
                                    </p>
                                    {uploadErrorDetail.code === "INVALID_DATE_FORMAT" ? (
                                        <p style={{ margin: 0, opacity: 0.95 }}>
                                            {uploadErrorDetail.sampleRowsValidated != null ? (
                                                <span>
                                                    Checked first{" "}
                                                    <strong>{uploadErrorDetail.sampleRowsValidated}</strong>{" "}
                                                    data row(s).{" "}
                                                </span>
                                            ) : null}
                                            {uploadErrorDetail.dataRowIndex != null &&
                                            uploadErrorDetail.row != null ? (
                                                <span>
                                                    Issue in sample data row{" "}
                                                    <strong>{uploadErrorDetail.dataRowIndex}</strong> (CSV line{" "}
                                                    <strong>{uploadErrorDetail.row}</strong>).
                                                </span>
                                            ) : uploadErrorDetail.row != null ? (
                                                <span>CSV line {uploadErrorDetail.row}.</span>
                                            ) : null}
                                        </p>
                                    ) : null}
                                </div>
                            ) : null}
                            {uploadErrorDetail?.hint &&
                            !uploadErrorDetail?.expectedHeaders?.length ? (
                                <p
                                    style={{
                                        marginTop: "12px",
                                        marginBottom: 0,
                                        padding: "12px",
                                        background: "#fff7ed",
                                        border: "1px solid #fed7aa",
                                        borderRadius: "8px",
                                        fontSize: "13px",
                                        color: "#9a3412",
                                    }}
                                >
                                    <strong>Action required:</strong> {uploadErrorDetail.hint}
                                </p>
                            ) : null}
                            {uploadErrorDetail?.expectedHeaders?.length ? (
                                <details style={{ marginTop: "12px", fontSize: "13px" }}>
                                    <summary style={{ cursor: "pointer", fontWeight: 600 }}>
                                        Official header order (1 → {uploadErrorDetail.expectedHeaders.length})
                                    </summary>
                                    <pre
                                        style={{
                                            margin: "8px 0 0",
                                            padding: "10px",
                                            background: "#fef2f2",
                                            borderRadius: "6px",
                                            overflowX: "auto",
                                            fontSize: "12px",
                                            lineHeight: 1.5,
                                        }}
                                    >
                                        {uploadErrorDetail.expectedHeaders
                                            .map((h, i) => `${i + 1}. ${h}`)
                                            .join("\n")}
                                    </pre>
                                    {uploadErrorDetail.hint ? (
                                        <p style={{ margin: "8px 0 0", opacity: 0.9 }}>
                                            {uploadErrorDetail.hint}
                                        </p>
                                    ) : null}
                                </details>
                            ) : null}
                        </div>
                    )}
                </div>
            </div>
        </MainLayout>
    );
};

export default TempTransaction;