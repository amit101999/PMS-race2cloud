import React, { useCallback, useEffect, useState } from "react";

import StatusBadge, { STATUS } from "./StatusBadge";
import DownloadButton from "../../../components/common/DownloadButton";
import * as api from "./api";

const when = (ms) =>
    ms ? new Date(ms).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" }) : "—";

/**
 * The dates a run was asked to compare. Empty means it was not limited, so it
 * covered whatever was in the files. Long lists are trimmed; the cell's tooltip
 * carries the full set.
 */
const datesLabel = (dates) => {
    if (!dates || dates.length === 0) return "All dates";
    if (dates.length <= 3) return dates.join(", ");
    return `${dates.slice(0, 2).join(", ")} +${dates.length - 2} more`;
};

/**
 * Past difference reports.
 *
 * Only runs that found something. A clean run leaves nothing worth revisiting,
 * so listing it would bury the reports that do need attention in a list of ones
 * that do not.
 */
const DifferenceReports = ({ refreshToken }) => {
    const [reports, setReports] = useState([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState("");
    const [busyKey, setBusyKey] = useState("");

    const load = useCallback(async () => {
        setLoading(true);
        setError("");
        try {
            const res = await api.getReports(10);
            setReports(res.reports || []);
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    }, []);

    useEffect(() => {
        load();
    }, [load, refreshToken]);

    const download = async (report) => {
        setBusyKey(report.runId);
        setError("");
        try {
            const res = await api.getDownloadUrl(report.differenceKey);
            window.open(res.downloadUrl, "_blank", "noopener,noreferrer");
        } catch (err) {
            setError(err.message);
        } finally {
            setBusyKey("");
        }
    };

    return (
        <div className="temp-transaction-card upload-history-card">
            <h3>Previous difference reports</h3>

            {error ? (
                <p className="upload-history-error" role="alert">
                    {error}
                </p>
            ) : null}

            <div className="data-table-wrap">
                <table className="data-table">
                    <thead>
                        <tr>
                            <th className="date">Matched on</th>
                            <th>Report</th>
                            <th>Dates compared</th>
                            <th className="num">Not matching</th>
                            <th className="num">Compared</th>
                            <th className="action">Download</th>
                        </tr>
                    </thead>
                    <tbody>
                        {loading && reports.length === 0 ? (
                            <tr>
                                <td colSpan={6} className="empty-cell">
                                    Loading…
                                </td>
                            </tr>
                        ) : reports.length === 0 ? (
                            <tr>
                                <td colSpan={6} className="empty-cell">
                                    {error ? "—" : "No difference reports yet."}
                                </td>
                            </tr>
                        ) : (
                            reports.map((r) => (
                                <tr key={r.runId}>
                                    <td className="date">{when(r.matchedAtMs)}</td>
                                    <td className="truncate mono" title={r.reportName}>
                                        {r.reportName || r.runId}
                                        {/* A later run for the same date took over as the
                                            one that can be imported. The report still
                                            describes what it compared. */}
                                        {r.superseded ? (
                                            <span className="cm-report-old">
                                                <StatusBadge tone={STATUS.NEUTRAL}>Superseded</StatusBadge>
                                            </span>
                                        ) : null}
                                    </td>
                                    <td title={(r.dates || []).join(", ") || "All dates"}>
                                        {datesLabel(r.dates)}
                                    </td>
                                    <td className="num cm-notmatching">
                                        {r.notMatching.toLocaleString()}
                                    </td>
                                    <td className="num">{r.comparedKeyCount.toLocaleString()}</td>
                                    <td className="action">
                                        {r.differenceKey ? (
                                            <DownloadButton
                                                onClick={() => download(r)}
                                                busy={busyKey === r.runId}
                                            />
                                        ) : (
                                            "—"
                                        )}
                                    </td>
                                </tr>
                            ))
                        )}
                    </tbody>
                </table>
            </div>
        </div>
    );
};

export default DifferenceReports;
