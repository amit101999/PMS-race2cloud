import React, { useCallback, useEffect, useState } from "react";
import MainLayout from "../../layouts/MainLayout";
import { BASE_URL } from "../../constant";
import CustodyMatchPanel from "./custody/CustodyMatchPanel";
import DifferenceReports from "./custody/DifferenceReports";
import PreviousCustodianUploads from "./custody/PreviousCustodianUploads";
import ExceptionGrid from "./custody/ExceptionGrid";
import GateTwo from "./custody/GateTwo";
import DownloadButton from "./custody/DownloadButton";
import { todayIso } from "./custody/api";
import "./tokens.css";
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

const WORKFLOW_STEPS = [
    { key: "upload", label: "Upload" },
    { key: "validate", label: "Validate" },
    { key: "match", label: "Match" },
    { key: "review", label: "Review Exceptions" },
    { key: "commit", label: "Commit" },
];

/**
 * Where this business date actually is in the workflow — derived from the
 * real file/run state the panels already hold, never hardcoded. Mirrors the
 * gates already enforced server-side (Gate 1: both files staged; Gate 2:
 * match run reached a terminal state) rather than inventing new rules.
 */
function deriveStage({ faReady, custodyReady, run }) {
    if (!faReady || !custodyReady) return 0;
    if (!run) return 1;
    if (run.isRunning) return 2;
    if (run.status === "FAILED") return 2;
    if (run.status === "COMPLETED_WITH_EXCEPTIONS") return 3;
    return 4; // COMPLETED_CLEAN, or a terminal state already handed to the importer
}

/**
 * Transaction Upload — now framed as one reconciliation workflow rather than
 * a stack of independent admin cards. The status bar and stepper below are
 * read-only reflections of state the child panels already own (`day`, `run`);
 * nothing here duplicates a fetch or invents a count.
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
    const [day, setDay] = useState(null);
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

    /* Where the workflow stepper points — derived from real state only. */
    const faReady = (day?.fa?.raw || []).length > 0;
    const custodyReady = (day?.custody?.files || []).length > 0;
    const currentStage = deriveStage({ faReady, custodyReady, run });

    return (
        <MainLayout title="Transaction Upload">
            <div className="temp-transaction-container txn-ops">
                <div className="txo-tracker-bar">
                <ol className="txo-tracker" aria-label="Reconciliation workflow">
                    {WORKFLOW_STEPS.map((step, i) => {
                        const reached = i <= currentStage;
                        return (
                            <li
                                key={step.key}
                                className={
                                    i === currentStage
                                        ? "is-current"
                                        : i < currentStage
                                            ? "is-done"
                                            : "is-pending"
                                }
                            >
                                <div className="txo-tracker-row">
                                    {i !== 0 ? (
                                        <span
                                            className={reached ? "txo-tracker-line is-filled" : "txo-tracker-line"}
                                        />
                                    ) : null}
                                    <span className="txo-tracker-node" aria-hidden="true">
                                        {reached ? (
                                            <svg width="9" height="7" viewBox="0 0 9 7" fill="none">
                                                <path
                                                    d="M1 3.5L3.2 5.7L8 1"
                                                    stroke="white"
                                                    strokeWidth="1.4"
                                                    strokeLinecap="round"
                                                    strokeLinejoin="round"
                                                />
                                            </svg>
                                        ) : null}
                                    </span>
                                    {i !== WORKFLOW_STEPS.length - 1 ? (
                                        <span
                                            className={
                                                i < currentStage ? "txo-tracker-line is-filled" : "txo-tracker-line"
                                            }
                                        />
                                    ) : null}
                                </div>
                                <span className="txo-tracker-label">{step.label}</span>
                            </li>
                        );
                    })}
                </ol>

                <label className="cm-date">
                    <span>Business date</span>
                    <input
                        type="date"
                        value={businessDate}
                        onChange={(e) => setBusinessDate(e.target.value)}
                        disabled={run?.isRunning}
                    />
                </label>
                </div>

                <CustodyMatchPanel
                    businessDate={businessDate}
                    onBusinessDateChange={setBusinessDate}
                    onRunChange={setRun}
                    onDayChange={setDay}
                    onCustodyUploaded={setCustodyHistory}
                />

                <ExceptionGrid run={run} />

                <GateTwo run={run} onImported={refreshHistory} />

                <div className="txo-section-head txo-section-head--tight">
                    <h2>History</h2>
                </div>

                <div className="temp-transaction-card upload-history-card">
                    <div className="upload-history-card-head">
                        <h3>Recent uploads</h3>
                        {historyItems.length > 0 && (
                            <span className="count-pill">
                                {historyItems.length} of {HISTORY_LIMIT}
                            </span>
                        )}
                    </div>

                    {historyError && (
                        <p className="upload-history-error" role="alert">
                            {historyError}
                        </p>
                    )}

                    <div className="upload-history-table-wrapper">
                        <table className="upload-history-table upload-history-table--compact">
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
                                            {historyError ? "—" : "Nothing uploaded yet — files you send will appear here."}
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
                                                <DownloadButton
                                                    onClick={() => handleHistoryDownload(item)}
                                                    busy={downloadingKey === item.key}
                                                />
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
