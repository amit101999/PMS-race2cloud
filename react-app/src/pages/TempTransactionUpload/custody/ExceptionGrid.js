import React, { useCallback, useEffect, useState } from "react";

import UploadErrorDetails from "./UploadErrorDetails";
import StatusBadge, { STATUS } from "./StatusBadge";
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
    // "" = every type. The server filters, so the preview holds the first rows
    // of the chosen type rather than whichever of the first ten happen to match.
    const [typeFilter, setTypeFilter] = useState("");

    const runId = run?.runId;

    // A new run has its own set of types; keep a filter from the old one and the
    // grid could open on a type this run does not have.
    useEffect(() => {
        setTypeFilter("");
    }, [runId]);
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
            const res = await api.getExceptions(runId, {
                page: 1,
                size: PREVIEW_ROWS,
                exceptionType: typeFilter,
            });
            setData({ rows: res.rows, total: res.total });
            if (onCountChange && !typeFilter) onCountChange(res.total);
        } catch (err) {
            setError({ message: err.message, detail: err.detail });
            setData({ rows: [], total: 0 });
        } finally {
            setLoading(false);
        }
    }, [runId, isRunning, typeFilter, onCountChange]);

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

    // Only the types this run actually has, in the fixed filter order.
    const byType = run.byType || {};
    const typesPresent = api.EXCEPTION_TYPES.filter((t) => byType[t] > 0);

    return (
        <div className="temp-transaction-card upload-history-card cm-exc" id="exception-details">
            <div className="cm-exc-head">
                <div>
                    <div className="cm-card-head-title">
                        <h3>Exception details</h3>
                        <StatusBadge tone={STATUS.WARN}>
                            {run.counts.exceptions.toLocaleString()} open
                        </StatusBadge>
                    </div>
                    <p className="cm-exc-sub">
                        Records where fund accounting and custody do not agree. Review each one
                        before committing the transaction file.
                    </p>
                </div>
                <button
                    type="button"
                    className="btn-export"
                    onClick={handleExport}
                    disabled={exporting}
                    aria-busy={exporting}
                >
                    {exporting ? "Preparing…" : "Export"}
                </button>
            </div>

            {typesPresent.length > 1 ? (
                <div className="cm-exc-filters" role="group" aria-label="Filter by exception type">
                    <button
                        type="button"
                        className={typeFilter === "" ? "cm-exc-chip is-active" : "cm-exc-chip"}
                        aria-pressed={typeFilter === ""}
                        onClick={() => setTypeFilter("")}
                    >
                        All
                        <span className="cm-exc-chip-count">{run.counts.exceptions.toLocaleString()}</span>
                    </button>
                    {typesPresent.map((t) => (
                        <button
                            key={t}
                            type="button"
                            className={`cm-exc-chip ${t.toLowerCase()}${typeFilter === t ? " is-active" : ""}`}
                            aria-pressed={typeFilter === t}
                            onClick={() => setTypeFilter(t)}
                        >
                            <span className="cm-exc-dot" aria-hidden="true" />
                            {api.EXCEPTION_LABEL[t] || t}
                            <span className="cm-exc-chip-count">{byType[t].toLocaleString()}</span>
                        </button>
                    ))}
                </div>
            ) : null}

            <div className="data-table-wrap cm-exc-wrap">
                <table className="data-table data-table--spread">
                    <thead>
                        <tr>
                            <th>Actual code</th>
                            <th>ISIN / Scheme</th>
                            <th className="date">Date</th>
                            <th className="status">Type</th>
                            <th className="num">FA qty</th>
                            <th className="num">Custody qty</th>
                            <th className="num">Difference</th>
                            <th className="status">Exception</th>
                            <th className="status">Status</th>
                        </tr>
                    </thead>
                    <tbody className={loading ? "cm-exc-loading" : undefined}>
                        {loading && data.rows.length === 0 ? (
                            <tr>
                                <td colSpan={9} className="empty-cell">Loading exceptions…</td>
                            </tr>
                        ) : data.rows.length === 0 ? (
                            <tr>
                                <td colSpan={9} className="empty-cell">No exceptions of this type.</td>
                            </tr>
                        ) : (
                            data.rows.map((r) => {
                                const difference = Number(r.difference);
                                return (
                                    <tr key={r.ROWID} className={`cm-exc-row ${r.exceptionType.toLowerCase()}`}>
                                        <td className="mono strong">{r.actualCode}</td>
                                        <td>
                                            <div className="mono">{r.isin}</div>
                                            {r.scheme ? <div className="cm-exc-scheme">{r.scheme}</div> : null}
                                        </td>
                                        <td className="date">{r.tranDate}</td>
                                        <td className="status">
                                            {r.tranType ? (
                                                <span className={`badge badge--${String(r.tranType).toLowerCase()}`}>
                                                    {r.tranType}
                                                </span>
                                            ) : (
                                                "—"
                                            )}
                                        </td>
                                        <td className={Number(r.faQuantity) === 0 ? "num muted" : "num"}>
                                            {qty(r.faQuantity)}
                                        </td>
                                        <td className={Number(r.custodyQuantity) === 0 ? "num muted" : "num"}>
                                            {qty(r.custodyQuantity)}
                                        </td>
                                        <td className="num">
                                            {difference === 0 || Number.isNaN(difference) ? (
                                                <span className="muted">{qty(r.difference)}</span>
                                            ) : (
                                                <span className="negative">
                                                    {difference > 0 ? "+" : ""}
                                                    {qty(r.difference)}
                                                </span>
                                            )}
                                        </td>
                                        <td className="status">
                                            <span className={`badge cm-tag ${r.exceptionType.toLowerCase()}`}>
                                                {api.EXCEPTION_LABEL[r.exceptionType] || r.exceptionType}
                                            </span>
                                        </td>
                                        <td className="status">
                                            <StatusBadge tone={r.status === "RESOLVED" ? STATUS.OK : STATUS.WARN}>
                                                {r.status === "RESOLVED" ? "Resolved" : "Open"}
                                            </StatusBadge>
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
                    Showing <strong>{data.rows.length}</strong> of{" "}
                    <strong>{data.total.toLocaleString()}</strong>. Export the difference report for the
                    full list.
                </p>
            ) : null}

            {error ? <UploadErrorDetails message={error.message} detail={error.detail} /> : null}
        </div>
    );
};

export default ExceptionGrid;
