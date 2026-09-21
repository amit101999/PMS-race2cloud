import React, { useCallback, useEffect, useState } from "react";

import UploadErrorDetails from "../../TempTransactionUpload/custody/UploadErrorDetails";
import * as api from "./api";

const qty = (n) =>
    n === null || n === undefined || Number.isNaN(Number(n))
        ? "—"
        : Number(n).toLocaleString(undefined, { maximumFractionDigits: 4 });

/**
 * A sample of the differences: the first 10 rows of the report.
 * The full report is the Export, which downloads the run's difference CSV.
 */
const ExceptionGrid = ({ run }) => {
    const [rows, setRows] = useState([]);
    const [loading, setLoading] = useState(false);
    const [exporting, setExporting] = useState(false);
    const [error, setError] = useState(null);

    const runId = run?.runId;
    const show =
        Boolean(runId) && !run.isRunning && run.hasReport && run.counts.exceptions > 0;

    const load = useCallback(async () => {
        if (!show) return;
        setLoading(true);
        setError(null);
        try {
            const res = await api.getExceptions(runId);
            setRows(res.rows);
        } catch (err) {
            setError({ message: err.message, detail: err.detail });
            setRows([]);
        } finally {
            setLoading(false);
        }
    }, [show, runId]);

    useEffect(() => {
        load();
    }, [load]);

    const handleExport = async () => {
        setExporting(true);
        setError(null);
        try {
            const res = await api.exportExceptions(runId);
            window.open(res.downloadUrl, "_blank", "noopener,noreferrer");
        } catch (err) {
            setError({ message: err.message, detail: err.detail });
        } finally {
            setExporting(false);
        }
    };

    if (!show) return null;

    return (
        <div className="temp-transaction-card upload-history-card">
            <div className="cm-card-head">
                <h3>
                    Differences <span className="cm-count">({run.counts.exceptions.toLocaleString()})</span>
                </h3>
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

            <div className="data-table-wrap">
                <table className="data-table data-table--spread">
                    <thead>
                        <tr>
                            <th>Actual Code</th>
                            <th>ISIN</th>
                            <th className="date">Date</th>
                            <th className="num">FA Holding Qty</th>
                            <th className="num">Custody Qty</th>
                            <th className="num">Difference</th>
                            <th className="status">Exception</th>
                        </tr>
                    </thead>
                    <tbody>
                        {loading && rows.length === 0 ? (
                            <tr>
                                <td colSpan={7} className="empty-cell">Loading…</td>
                            </tr>
                        ) : rows.length === 0 ? (
                            <tr>
                                <td colSpan={7} className="empty-cell">No differences to show.</td>
                            </tr>
                        ) : (
                            rows.map((r) => (
                                <tr key={`${r.accountCode}|${r.isin}`}>
                                    <td className="mono">{r.accountCode}</td>
                                    <td className="mono" title={r.securityName}>{r.isin}</td>
                                    <td className="date">{r.asOfDate || "—"}</td>
                                    <td className="num">{qty(r.faQuantity)}</td>
                                    <td className="num">{qty(r.custodyQuantity)}</td>
                                    <td className="num">
                                        {Number(r.difference) > 0 ? "+" : ""}
                                        {qty(r.difference)}
                                    </td>
                                    <td className="status">
                                        <span className={`badge cm-tag ${r.exceptionType.toLowerCase()}`}>
                                            {api.EXCEPTION_LABEL[r.exceptionType] || r.exceptionType}
                                        </span>
                                    </td>
                                </tr>
                            ))
                        )}
                    </tbody>
                </table>
            </div>

            {error ? <UploadErrorDetails message={error.message} detail={error.detail} /> : null}
        </div>
    );
};

export default ExceptionGrid;
