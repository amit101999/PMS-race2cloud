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
        <div role="alert" className="cm-error-block">
            {message ? <p className="cm-error-message">{message}</p> : null}

            {/* Gate 1: every reason matching cannot start yet. */}
            {detail?.blockers?.length ? (
                <ul className="cm-error-list">
                    {detail.blockers.map((b, i) => (
                        <li key={`${b.code}-${b.scheme || i}`}>{b.message}</li>
                    ))}
                </ul>
            ) : null}

            {detail?.code === "WRONG_COLUMN_ORDER" &&
            detail?.expectedCount != null &&
            detail?.actualCount != null ? (
                <p className="cm-error-note">
                    <strong>Template columns:</strong> {detail.expectedCount} &nbsp;|&nbsp;{" "}
                    <strong>Your row 1 columns:</strong> {detail.actualCount}
                </p>
            ) : null}

            {detail?.rowErrors?.length ? (
                <div className="cm-error-rows">
                    <strong className="cm-error-rows-title">
                        Rows that could not be read
                        {detail.rowErrorCount != null ? ` (${detail.rowErrorCount} total)` : ""}:
                    </strong>
                    <div className="cm-error-rows-scroll">
                        <table className="data-table">
                            <thead>
                                <tr>
                                    <th className="num">Row</th>
                                    <th>Column</th>
                                    <th>Value</th>
                                    <th className="wrap">Problem</th>
                                </tr>
                            </thead>
                            <tbody>
                                {detail.rowErrors.map((e, i) => (
                                    <tr key={`${e.row}-${e.code}-${i}`}>
                                        <td className="num mono">{e.row}</td>
                                        <td className="mono">{e.column || "—"}</td>
                                        <td>{e.value === "" || e.value == null ? "(empty)" : String(e.value)}</td>
                                        <td className="wrap">{e.message}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                    {detail.moreRowErrors > 0 ? (
                        <p className="cm-error-note">
                            …and {detail.moreRowErrors} more. Fix these first — the rest may share
                            the same cause.
                        </p>
                    ) : null}
                </div>
            ) : null}

            {detail?.fields?.length ? (
                <div className="cm-error-fields">
                    <p className="cm-error-note">
                        <strong>Date columns to fix:</strong>{" "}
                        {detail.fields.map((f) => (
                            <code key={f} className="cm-error-code">
                                {f}
                            </code>
                        ))}
                    </p>
                    {detail.code === "INVALID_DATE_FORMAT" ? (
                        <p className="cm-error-note">
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

            {detail?.hint ? <p className="cm-error-hint">{detail.hint}</p> : null}
        </div>
    );
};

export default UploadErrorDetails;
