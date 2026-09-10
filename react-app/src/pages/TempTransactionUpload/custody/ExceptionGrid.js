import React, { useCallback, useEffect, useState } from "react";

import UploadErrorDetails from "./UploadErrorDetails";
import * as api from "./api";

/* Matches Recent Uploads: a short preview, no pagination. The full report is
   the Export, which is the file the run actually produced. */
const PREVIEW_ROWS = 10;

const qty = (n) =>
    n === null || n === undefined || Number.isNaN(Number(n))
        ? "—"
        : Number(n).toLocaleString(undefined, { maximumFractionDigits: 4 });

/**
 * The differential report.
 *
 * Every row here is a difference somebody has to chase. The export is the file
 * the run actually produced — it carries the virtual codes and both reference
 * numbers needed to chase a break in the source files.
 */
const ExceptionGrid = ({ run, onCountChange }) => {
    const [data, setData] = useState({ rows: [], total: 0 });
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [exporting, setExporting] = useState(false);

    const runId = run?.runId;
    /*
     * S5 writes the exceptions before S6 sets the terminal status, so "no longer
     * running" is the point at which they are guaranteed to be readable. It is
     * also a dependency of the fetch: runId does not change when a run finishes,
     * so without this the grid keeps the empty result it read while the job was
     * still working and only fills in after a reload.
     */
    const isRunning = Boolean(run?.isRunning);

    const load = useCallback(async () => {
        if (!runId || isRunning) return;
        setLoading(true);
        setError(null);
        try {
            const res = await api.getExceptions(runId, { page: 1, size: PREVIEW_ROWS });
            setData({ rows: res.rows, total: res.total });
            if (onCountChange) onCountChange(res.total);
        } catch (err) {
            setError({ message: err.message, detail: err.detail });
            setData({ rows: [], total: 0 });
        } finally {
            setLoading(false);
        }
    }, [runId, isRunning, onCountChange]);

    useEffect(() => {
        load();
    }, [load]);

    const handleExport = async () => {
        setExporting(true);
        setError(null);
        try {
            const res = await api.exportExceptions(runId);
            if (res.downloadUrl) {
                window.open(res.downloadUrl, "_blank", "noopener,noreferrer");
            } else if (res.rows) {
                // The stored difference file was unavailable, so the server sent the
                // rows instead. Build the CSV here rather than leaving the user
                // with nothing to hand to whoever has to fix the break — using the
                // column list the server sent, so a rebuilt export is the same
                // report as a stored one.
                const cols = res.columns || Object.keys(res.rows[0] || {});
                const headers = res.headers || cols;
                const esc = (v) => {
                    const s = v === null || v === undefined ? "" : String(v);
                    return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
                };
                const csv = [
                    headers.join(","),
                    ...res.rows.map((r) => cols.map((c) => esc(r[c])).join(",")),
                ].join("\n");
                const url = URL.createObjectURL(new Blob([csv], { type: "text/csv" }));
                const a = document.createElement("a");
                a.href = url;
                a.download = `difference-${run.businessDate}-${runId}.csv`;
                a.click();
                URL.revokeObjectURL(url);
            }
        } catch (err) {
            setError({ message: err.message, detail: err.detail });
        } finally {
            setExporting(false);
        }
    };

    if (!runId || run.isRunning) return null;
    if (run.counts.exceptions === 0) return null;

    return (
        <div className="temp-transaction-card upload-history-card">
            <div className="cm-card-head">
                <div>
                    <h3>
                        Exception details{" "}
                        <span className="cm-count">({run.counts.exceptions.toLocaleString()})</span>
                    </h3>
                </div>
                <button
                    type="button"
                    className="upload-history-download-btn"
                    onClick={handleExport}
                    disabled={exporting}
                >
                    {exporting ? "Preparing…" : "Export"}
                </button>
            </div>

            <div className="upload-history-table-wrapper">
                <table className="upload-history-table">
                    <thead>
                        <tr>
                            <th>Actual code</th>
                            <th>Scheme</th>
                            <th>ISIN</th>
                            <th>Date</th>
                            <th>Type</th>
                            <th className="num">FA qty</th>
                            <th className="num">Custody qty</th>
                            <th className="num">Difference</th>
                            <th>Exception</th>
                            <th>Status</th>
                        </tr>
                    </thead>
                    <tbody>
                        {loading && data.rows.length === 0 ? (
                            <tr>
                                <td colSpan={10} className="empty-cell">Loading…</td>
                            </tr>
                        ) : data.rows.length === 0 ? (
                            <tr>
                                <td colSpan={10} className="empty-cell">No exceptions.</td>
                            </tr>
                        ) : (
                            data.rows.map((r) => {
                                return (
                                    <tr key={r.ROWID}>
                                        <td className="mono">{r.actualCode}</td>
                                        <td>{r.scheme || "—"}</td>
                                        <td className="mono">{r.isin}</td>
                                        <td className="mono">{r.tranDate}</td>
                                        <td>{r.tranType}</td>
                                        <td className="num">{qty(r.faQuantity)}</td>
                                        <td className="num">{qty(r.custodyQuantity)}</td>
                                        <td className={`num ${Number(r.difference) === 0 ? "" : "diff"}`}>
                                            {Number(r.difference) > 0 ? "+" : ""}
                                            {qty(r.difference)}
                                        </td>
                                        <td>
                                            <span className={`cm-tag ${r.exceptionType.toLowerCase()}`}>
                                            {api.EXCEPTION_LABEL[r.exceptionType] || r.exceptionType}
                                            </span>
                                        </td>
                                        <td>
                                            <span className={`cm-pill ${r.status === "RESOLVED" ? "ok" : "wait"}`}>
                                            {r.status}
                                            </span>
                                        </td>
                                    </tr>
                                );
                            })
                        )}
                    </tbody>
                </table>
            </div>

            {data.total > data.rows.length ? (
                <p className="cm-preview-note">
                    Showing the first {data.rows.length} of {data.total.toLocaleString()}. Export for
                    the full report.
                </p>
            ) : null}

            {error ? <UploadErrorDetails message={error.message} detail={error.detail} /> : null}
        </div>
    );
};

export default ExceptionGrid;
