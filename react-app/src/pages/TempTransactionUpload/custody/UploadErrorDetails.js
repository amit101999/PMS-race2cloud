import React from "react";

/**
 * Structured upload errors.
 *
 * Lifted out of TempTransaction.js so the custody-matching uploads and the
 * existing import render failures identically — the users already know how to
 * read this block, and two different error panels on one screen would be worse
 * than either alone. The header/date branches are the original code moved as-is;
 * the row-error and blocker branches are new.
 *
 * @param {{ message: string, detail?: object|null }} props
 */
const UploadErrorDetails = ({ message, detail }) => {
    if (!message && !detail) return null;

    return (
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
            {message ? (
                <p style={{ margin: "0 0 8px", whiteSpace: "pre-line" }}>{message}</p>
            ) : null}

            {/* Gate 1: every reason matching cannot start yet. */}
            {detail?.blockers?.length ? (
                <ul style={{ margin: "8px 0 0", paddingLeft: "1.25rem", lineHeight: 1.6 }}>
                    {detail.blockers.map((b, i) => (
                        <li key={`${b.code}-${b.scheme || i}`}>{b.message}</li>
                    ))}
                </ul>
            ) : null}

            {detail?.code === "WRONG_COLUMN_ORDER" &&
            detail?.expectedCount != null &&
            detail?.actualCount != null ? (
                <p style={{ margin: "8px 0 0", fontSize: "13px" }}>
                    <strong>Template columns:</strong> {detail.expectedCount} &nbsp;|&nbsp;{" "}
                    <strong>Your row 1 columns:</strong> {detail.actualCount}
                </p>
            ) : null}

            {detail?.rowErrors?.length ? (
                <div style={{ marginTop: "12px" }}>
                    <strong style={{ display: "block", marginBottom: "8px" }}>
                        Rows that could not be read
                        {detail.rowErrorCount != null ? ` (${detail.rowErrorCount} total)` : ""}:
                    </strong>
                    <div style={{ maxHeight: "260px", overflowY: "auto", borderRadius: "6px" }}>
                        <table
                            style={{
                                width: "100%",
                                borderCollapse: "collapse",
                                fontSize: "13px",
                                background: "#fef2f2",
                            }}
                        >
                            <thead>
                                <tr style={{ textAlign: "left", background: "#fecaca" }}>
                                    <th style={{ padding: "6px 8px" }}>Row</th>
                                    <th style={{ padding: "6px 8px" }}>Column</th>
                                    <th style={{ padding: "6px 8px" }}>Value</th>
                                    <th style={{ padding: "6px 8px" }}>Problem</th>
                                </tr>
                            </thead>
                            <tbody>
                                {detail.rowErrors.map((e, i) => (
                                    <tr key={`${e.row}-${e.code}-${i}`}>
                                        <td style={{ padding: "6px 8px", fontWeight: 600, whiteSpace: "nowrap" }}>
                                            {e.row}
                                        </td>
                                        <td style={{ padding: "6px 8px", whiteSpace: "nowrap" }}>
                                            {e.column || "—"}
                                        </td>
                                        <td style={{ padding: "6px 8px", wordBreak: "break-all" }}>
                                            {e.value === "" || e.value == null ? "(empty)" : String(e.value)}
                                        </td>
                                        <td style={{ padding: "6px 8px" }}>{e.message}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                    {detail.moreRowErrors > 0 ? (
                        <p style={{ margin: "8px 0 0", fontSize: "13px", opacity: 0.9 }}>
                            …and {detail.moreRowErrors} more. Fix these first — the rest may share
                            the same cause.
                        </p>
                    ) : null}
                </div>
            ) : null}

            {detail?.fields?.length ? (
                <div style={{ margin: "10px 0 0", fontSize: "13px" }}>
                    <p style={{ margin: "0 0 6px" }}>
                        <strong>Date columns to fix:</strong>{" "}
                        {detail.fields.map((f) => (
                            <code key={f} style={{ background: "#fecaca", padding: "2px 6px", borderRadius: "4px", marginRight: "6px" }}>
                                {f}
                            </code>
                        ))}
                    </p>
                    {detail.code === "INVALID_DATE_FORMAT" ? (
                        <p style={{ margin: 0, opacity: 0.95 }}>
                            {detail.sampleRowsValidated != null ? (
                                <span>
                                    Checked first <strong>{detail.sampleRowsValidated}</strong> data
                                    row(s).{" "}
                                </span>
                            ) : null}
                            {detail.dataRowIndex != null && detail.row != null ? (
                                <span>
                                    Issue in sample data row <strong>{detail.dataRowIndex}</strong>{" "}
                                    (CSV line <strong>{detail.row}</strong>).
                                </span>
                            ) : detail.row != null ? (
                                <span>CSV line {detail.row}.</span>
                            ) : null}
                        </p>
                    ) : null}
                </div>
            ) : null}

            {detail?.hint ? (
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
                    {detail.hint}
                </p>
            ) : null}

        </div>
    );
};

export default UploadErrorDetails;
