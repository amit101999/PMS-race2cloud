import React, { useCallback, useEffect, useState } from "react";
import MainLayout from "../../layouts/MainLayout";
import { BASE_URL } from "../../constant";
import CustodyMatchPanel from "./custody/CustodyMatchPanel";
import DifferenceReports from "./custody/DifferenceReports";
import PreviousCustodianUploads from "./custody/PreviousCustodianUploads";
import ExceptionGrid from "./custody/ExceptionGrid";
import GateTwo from "./custody/GateTwo";
import { todayIso } from "./custody/api";
import "./TempTransaction.css";

/** Show only the most recent N uploads — no pagination. */
const HISTORY_LIMIT = 5;

/** "1.23 MB" / "456 KB" / "78 B" — null/undefined → "—". */
function formatBytes(bytes) {
    if (bytes == null || Number.isNaN(Number(bytes))) return "—";
    const n = Number(bytes);
    if (n < 1024) return `${n} B`;
    if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
    if (n < 1024 * 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(2)} MB`;
    return `${(n / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

/** ISO string → "12 May 2026, 4:53 pm" (locale). */
function formatUploadedAt(iso) {
    if (!iso) return "—";
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return "—";
    return d.toLocaleString();
}

/**
 * Transaction Upload.
 *
 * The direct "pick a file and import" control that used to live here is gone on
 * purpose: nothing reaches production until custody matching comes back clean,
 * and a button that skips the match would make the whole gate advisory. The
 * import now runs server-side behind Gate 2 (POST /run/:runId/proceed), which is
 * what makes the block real rather than a `disabled` attribute in the DOM.
 *
 * Recent Uploads below is unchanged.
 */
const TempTransaction = () => {
    const [businessDate, setBusinessDate] = useState(todayIso());
    const [run, setRun] = useState(null);
    /* The custodian history list an upload hands back, so the new file shows at once. */
    const [custodyHistory, setCustodyHistory] = useState(null);

    /** @type {[Array<{ key: string; originalFileName: string; uploadedAt: string; uploadedAtMs: number; sizeBytes: number; }>, React.Dispatch<React.SetStateAction<any[]>>]} */
    const [historyItems, setHistoryItems] = useState([]);
    const [historyLoading, setHistoryLoading] = useState(false);
    const [historyError, setHistoryError] = useState("");
    const [downloadingKey, setDownloadingKey] = useState("");

    const refreshHistory = useCallback(async () => {
        setHistoryLoading(true);
        setHistoryError("");
        try {
            const res = await fetch(
                `${BASE_URL}/transaction-uploader/upload-history?limit=${HISTORY_LIMIT}`
            );
            const text = await res.text();
            let data = {};
            try {
                data = text ? JSON.parse(text) : {};
            } catch {
                throw new Error(
                    "Could not read upload history (server did not return JSON)."
                );
            }
            if (!res.ok || data?.success === false) {
                throw new Error(
                    data?.message || `Failed to load upload history (${res.status}).`
                );
            }
            setHistoryItems(Array.isArray(data.items) ? data.items.slice(0, HISTORY_LIMIT) : []);
        } catch (err) {
            const isNetwork =
                err instanceof TypeError ||
                String(err?.message || "").toLowerCase().includes("failed to fetch");
            setHistoryError(
                isNetwork
                    ? "Could not reach the upload server."
                    : err.message || "Failed to load upload history"
            );
            setHistoryItems([]);
        } finally {
            setHistoryLoading(false);
        }
    }, []);

    useEffect(() => {
        refreshHistory();
    }, [refreshHistory]);

    // A completed run adds a row to the history below, so refresh when one
    // reaches a terminal state rather than making the user reload the page.
    const runId = run?.runId;
    const runIsRunning = run?.isRunning;

    useEffect(() => {
        if (runId && !runIsRunning) refreshHistory();
    }, [runId, runIsRunning, refreshHistory]);

    const handleHistoryDownload = useCallback(async (item) => {
        if (!item?.key) return;
        setDownloadingKey(item.key);
        try {
            const res = await fetch(
                `${BASE_URL}/transaction-uploader/upload-history/download?key=${encodeURIComponent(item.key)}`
            );
            const text = await res.text();
            let data = {};
            try {
                data = text ? JSON.parse(text) : {};
            } catch {
                throw new Error("Could not prepare the download.");
            }
            if (!res.ok || data?.success === false || !data?.downloadUrl) {
                throw new Error(data?.message || "Could not prepare the download.");
            }
            window.open(data.downloadUrl, "_blank", "noopener,noreferrer");
        } catch (err) {
            setHistoryError(err.message || "Could not prepare the download.");
        } finally {
            setDownloadingKey("");
        }
    }, []);

    return (
        <MainLayout title="Transaction Upload">
            <div className="temp-transaction-container">
                <CustodyMatchPanel
                    businessDate={businessDate}
                    onBusinessDateChange={setBusinessDate}
                    onRunChange={setRun}
                    onCustodyUploaded={setCustodyHistory}
                />

                <ExceptionGrid run={run} />

                <GateTwo run={run} onImported={refreshHistory} />

                <div className="temp-transaction-card upload-history-card">
                    <h3>Recent Uploads</h3>

                    {historyError && (
                        <p className="upload-history-error" role="alert">
                            {historyError}
                        </p>
                    )}

                    <div className="upload-history-table-wrapper">
                        <table className="upload-history-table">
                            <thead>
                                <tr>
                                    <th>Uploaded date</th>
                                    <th>File name</th>
                                    <th className="num">Size</th>
                                    <th className="action">Download</th>
                                </tr>
                            </thead>
                            <tbody>
                                {historyLoading && historyItems.length === 0 ? (
                                    <tr>
                                        <td colSpan={4} className="empty-cell">
                                            Loading…
                                        </td>
                                    </tr>
                                ) : historyItems.length === 0 ? (
                                    <tr>
                                        <td colSpan={4} className="empty-cell">
                                            {historyError ? "—" : "No uploads yet."}
                                        </td>
                                    </tr>
                                ) : (
                                    historyItems.map((item) => (
                                        <tr key={item.key}>
                                            <td>{formatUploadedAt(item.uploadedAt)}</td>
                                            <td className="filename-cell" title={item.originalFileName}>
                                                {item.originalFileName}
                                            </td>
                                            <td className="num">{formatBytes(item.sizeBytes)}</td>
                                            <td className="action">
                                                <button
                                                    type="button"
                                                    className="upload-history-download-btn"
                                                    onClick={() => handleHistoryDownload(item)}
                                                    disabled={downloadingKey === item.key}
                                                >
                                                    {downloadingKey === item.key ? "Preparing…" : "Download"}
                                                </button>
                                            </td>
                                        </tr>
                                    ))
                                )}
                            </tbody>
                        </table>
                    </div>
                </div>

                <PreviousCustodianUploads latest={custodyHistory} />

                {/* Keyed off the run so a match that has just finished with
                    differences shows up without a reload. */}
                <DifferenceReports refreshToken={run?.runId ? `${run.runId}:${run.status}` : ""} />
            </div>
        </MainLayout>
    );
};

export default TempTransaction;
