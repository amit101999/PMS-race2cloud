import React, { useCallback, useEffect, useMemo, useState } from "react";
import MainLayout from "../../layouts/MainLayout";
import { BASE_URL } from "../../constant";
import CommitHistory from "./custody/CommitHistory";
import CustodyMatchPanel from "./custody/CustodyMatchPanel";
import { KIND, todayIso } from "./custody/api";
import DifferenceReports from "./custody/DifferenceReports";
import ExceptionGrid from "./custody/ExceptionGrid";
// import GateTwo from "./custody/GateTwo"; — old Commit, replaced by PreCommitPanel
import PreCommitPanel from "./custody/PreCommitPanel";
import OpiOpoCommitPanel from "./custody/OpiOpoCommitPanel";
import WorkflowTracker from "./WorkflowTracker";
import DownloadButton from "../../components/common/DownloadButton";
import "./tokens.css";
import "./TempTransaction.css";

/** "1.23 MB" / "456 KB" / "78 B" — null/undefined → "—". */
function formatBytes(bytes) {
    if (bytes == null || Number.isNaN(Number(bytes))) return "—";
    const n = Number(bytes);
    if (n < 1024) return `${n} B`;
    if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
    if (n < 1024 * 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(2)} MB`;
    return `${(n / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

/** Epoch ms → "12 May 2026, 4:53 pm" (locale). */
function formatPreCommittedAt(ms) {
    return ms ? new Date(ms).toLocaleString() : "—";
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
function deriveStage({ faReady, custodyReady, runs }) {
    if (!faReady || !custodyReady) return 0;
    if (runs.length === 0) return 1;
    // Several dates can be on screen; the stepper follows the one furthest behind.
    if (runs.some((r) => r.isRunning || r.status === "FAILED")) return 2;
    if (runs.some((r) => r.status === "COMPLETED_WITH_EXCEPTIONS")) return 3;
    return 4; // COMPLETED_CLEAN, or a terminal state already handed to the importer
}

const TABS = [
    { key: "equity", label: "Equity" },
    { key: "mutualFunds", label: "Mutual Funds" },
    { key: "cashBalance", label: "Cash" },
    { key: "opiOpo", label: "OPI/OPO" },
];

/** One Commit block per date that has a file on the Mutual Funds tab. */
function commitDatesOf(files, runIdPrefix) {
    const dates = [...new Set(files.map((f) => f.businessDate).filter(Boolean))].sort();
    return dates.map((date) => ({
        runId: `${runIdPrefix}-${date}`,
        businessDate: date,
        status: "READY",
        isRunning: false,
    }));
}

/**
 * Transaction Upload — four tabs.
 *
 * Equity: upload the FA and custodian files of a date, match them, then
 * Commit. A re-uploaded file replaces or adds to the date's trades by
 * transaction ID. A date's first Commit just adds its trades; a later one cleans
 * up the whole date before loading it again, so our books always equal the
 * latest files (EquityReload.js on the server).
 *
 * Mutual Funds: the same upload, T+N window and Commit as Equity (kind
 * MUTUAL_FUND on the server), but with no custodian file and no match. Each tab
 * touches only its own trades and keeps its own window and history. Cash
 * Balance is not built yet.
 *
 * OPI/OPO: shares moved in (OPI) or out (OPO) from another system. One file
 * may hold many trade dates and is committed once as a whole (OpiOpoUpload.js on
 * the server): no file date, no Transaction ID, no change window, and holdings
 * rebuilt but not cash.
 *
 * The old single-page upload is kept below, commented out.
 */
const TempTransaction = () => {
    const [activeTab, setActiveTab] = useState("equity");
    /* The upload date, picked by the user. Every file uploaded is recorded under
       it, and Match offers the dates of the files recorded under it. */
    const [uploadDate, setUploadDate] = useState("");
    /* One match run per date on screen, oldest first. */
    const [runs, setRuns] = useState([]);
    const [day, setDay] = useState(null);
    /* A Pre-commit moved on, so the FA card's file statuses and the consolidated files are out of date. */
    const [preCommitTick, setPreCommitTick] = useState(0);
    const handlePreCommitProgress = useCallback(() => setPreCommitTick((tick) => tick + 1), []);

    /* Mutual Funds and OPI/OPO tabs: the files on each card. Each of their dates gets a Commit block. */
    const [mfFiles, setMfFiles] = useState([]);
    const mfCommitDates = useMemo(() => commitDatesOf(mfFiles, "mf"), [mfFiles]);
    /* The Commit blocks re-read their dates whenever a file is added or removed, not only
       when a date appears: a second file for an already committed date makes it Ready again. */
    const mfDatesKey = `${mfCommitDates.map((r) => r.businessDate).join(",")}|${mfFiles.map((f) => f.key).join(",")}`;
    /* OPI/OPO tab: the files on its card. Each file gets its own Commit block. */
    const [opiOpoFiles, setOpiOpoFiles] = useState([]);

    const [historyItems, setHistoryItems] = useState([]);
    const [historyLoading, setHistoryLoading] = useState(false);
    const [historyError, setHistoryError] = useState("");
    const [downloadingKey, setDownloadingKey] = useState("");

    const refreshHistory = useCallback(async () => {
        setHistoryLoading(true);
        setHistoryError("");
        try {
            const res = await fetch(`${BASE_URL}/custody-match/reload/consolidated-files`);
            const text = await res.text();
            let data = {};
            try {
                data = text ? JSON.parse(text) : {};
            } catch {
                throw new Error(
                    "Could not read the consolidated files (server did not return JSON)."
                );
            }
            if (!res.ok || data?.success === false) {
                throw new Error(
                    data?.message || `Failed to load the consolidated files (${res.status}).`
                );
            }
            setHistoryItems(Array.isArray(data.items) ? data.items : []);
        } catch (err) {
            const isNetwork =
                err instanceof TypeError ||
                String(err?.message || "").toLowerCase().includes("failed to fetch");
            setHistoryError(
                isNetwork
                    ? "Could not reach the upload server."
                    : err.message || "Failed to load the consolidated files"
            );
            setHistoryItems([]);
        } finally {
            setHistoryLoading(false);
        }
    }, []);

    useEffect(() => {
        refreshHistory();
    }, [refreshHistory, preCommitTick]);

    const runsKey = runs.map((r) => `${r.runId}:${r.status}`).join(",");
    const anyRunning = runs.some((r) => r.isRunning);

    useEffect(() => {
        if (runsKey && !anyRunning) refreshHistory();
    }, [runsKey, anyRunning, refreshHistory]);

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

    const faReady = (day?.fa?.raw || []).length > 0;
    const custodyReady = (day?.custody?.files || []).length > 0;
    const currentStage = deriveStage({ faReady, custodyReady, runs });

    return (
        <MainLayout title="Transaction Upload">
            <div className="temp-transaction-container txn-ops txn-fixed-head-page">
                {/* Tabs and the workflow steps stay fixed; only the content below them scrolls. */}
                <div className="txn-sticky-head">
                    <div className="txn-tabs">
                        <div className="txn-tabs-list" role="tablist">
                            {TABS.map((tab) => (
                                <button
                                    key={tab.key}
                                    type="button"
                                    role="tab"
                                    aria-selected={activeTab === tab.key}
                                    className={`txn-tab ${activeTab === tab.key ? "active" : ""}`}
                                    onClick={() => setActiveTab(tab.key)}
                                >
                                    {tab.label}
                                </button>
                            ))}
                        </div>

                        {/* Mutual Funds and OPI/OPO have no workflow steps, so their upload
                            date sits in the tab bar instead of a bar of its own. */}
                        {activeTab === "mutualFunds" || activeTab === "opiOpo" ? (
                            <label className="cm-date txn-tabs-date">
                                <span>Upload date</span>
                                <input
                                    type="date"
                                    value={uploadDate}
                                    max={todayIso()}
                                    onChange={(e) => setUploadDate(e.target.value)}
                                />
                            </label>
                        ) : null}
                    </div>

                    {activeTab === "equity" ? (
                        <div className="txo-tracker-bar txo-tracker-bar--static">
                            <WorkflowTracker
                                steps={WORKFLOW_STEPS}
                                currentStage={currentStage}
                                label="Reconciliation workflow"
                            />

                            <label className="cm-date">
                                <span>Upload date</span>
                                <input
                                    type="date"
                                    value={uploadDate}
                                    max={todayIso()}
                                    onChange={(e) => setUploadDate(e.target.value)}
                                    disabled={anyRunning}
                                />
                            </label>
                        </div>
                    ) : null}
                </div>

                <div className="txn-scroll-body">
                {activeTab === "mutualFunds" ? (
                    <>
                        {/* Each tab gets its own card (key), so switching tabs never carries
                            one tab's date, file type or window over to another. */}
                        <CustodyMatchPanel
                            key="mutualFunds"
                            reloadMode
                            kind={KIND.MUTUAL_FUND}
                            uploadDate={uploadDate}
                            onFaFilesChange={setMfFiles}
                            preCommitTick={preCommitTick}
                        />

                        <PreCommitPanel
                            key={mfDatesKey}
                            runs={mfCommitDates}
                            kind={KIND.MUTUAL_FUND}
                            onProgressChange={handlePreCommitProgress}
                        />

                        <div className="txo-section-head txo-section-head--tight">
                            <h2>History</h2>
                        </div>
                        <CommitHistory refreshToken={preCommitTick} kind={KIND.MUTUAL_FUND} />
                    </>
                ) : activeTab === "opiOpo" ? (
                    <>
                        <CustodyMatchPanel
                            key="opiOpo"
                            reloadMode
                            kind={KIND.OPI_OPO}
                            uploadDate={uploadDate}
                            onFaFilesChange={setOpiOpoFiles}
                            preCommitTick={preCommitTick}
                        />

                        <OpiOpoCommitPanel files={opiOpoFiles} onProgressChange={handlePreCommitProgress} />

                        <div className="txo-section-head txo-section-head--tight">
                            <h2>History</h2>
                        </div>
                        <CommitHistory refreshToken={preCommitTick} kind={KIND.OPI_OPO} />
                    </>
                ) : activeTab === "cashBalance" ? (
                    <div className="temp-transaction-card cm-tab-empty">
                        Cash Balance upload is not available yet.
                    </div>
                ) : (
                    <>
                        <CustodyMatchPanel
                            key="equity"
                            reloadMode
                            uploadDate={uploadDate}
                            onRunChange={setRuns}
                            onDayChange={setDay}
                            preCommitTick={preCommitTick}
                        />

                        <ExceptionGrid runs={runs} />

                        {/* Keyed by the runs shown, so one date's result never carries over to another. */}
                        <PreCommitPanel
                            key={runs.map((r) => r.runId).join(",")}
                            runs={runs}
                            onProgressChange={handlePreCommitProgress}
                        />

                        <div className="txo-section-head txo-section-head--tight">
                            <h2>History</h2>
                        </div>

                        <div className="temp-transaction-card upload-history-card">
                            <div className="upload-history-card-head">
                                <h3>Consolidated files</h3>
                                {historyItems.length > 0 && (
                                    <span className="count-pill">
                                        {historyItems.length} date{historyItems.length === 1 ? "" : "s"}
                                    </span>
                                )}
                            </div>

                            {historyError && (
                                <p className="upload-history-error" role="alert">
                                    {historyError}
                                </p>
                            )}

                            <div className="data-table-wrap">
                                <table className="data-table data-table--spread">
                                    <thead>
                                        <tr>
                                            <th className="date">Trade date</th>
                                            <th className="date">Committed on</th>
                                            <th className="truncate">File name</th>
                                            <th className="num">Size</th>
                                            <th className="action">Download</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {historyLoading && historyItems.length === 0 ? (
                                            <tr>
                                                <td colSpan={5} className="empty-cell">
                                                    Loading…
                                                </td>
                                            </tr>
                                        ) : historyItems.length === 0 ? (
                                            <tr>
                                                <td colSpan={5} className="empty-cell">
                                                    {historyError ? "—" : "Nothing committed yet. Each date's consolidated file will appear here."}
                                                </td>
                                            </tr>
                                        ) : (
                                            historyItems.map((item) => (
                                                <tr key={item.key}>
                                                    <td className="date">{item.date}</td>
                                                    <td className="date">{formatPreCommittedAt(item.preCommittedAtMs)}</td>
                                                    <td className="truncate mono" title={item.fileName}>
                                                        {item.fileName}
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

                        <DifferenceReports refreshToken={runsKey} />

                        <CommitHistory refreshToken={preCommitTick} />
                    </>
                )}
                </div>
            </div>
        </MainLayout>
    );
};

export default TempTransaction;

/*
 * ─── Old Transaction Upload page, commented out ─────────────────────────────
 * Replaced by the tabs above. Kept so the old screen can be brought back.
 */
// /**
//  * Transaction Upload — now framed as one reconciliation workflow rather than
//  * a stack of independent admin cards. The status bar and stepper below are
//  * read-only reflections of state the child panels already own (`day`, `runs`);
//  * nothing here duplicates a fetch or invents a count.
//  *
//  * The direct "pick a file and import" control that used to live here is gone on
//  * purpose: nothing reaches production until custody matching comes back clean,
//  * and a button that skips the match would make the whole gate advisory. The
//  * import now runs server-side behind Gate 2 (POST /runs/proceed), which is
//  * what makes the block real rather than a `disabled` attribute in the DOM.
//  *
//  * Recent Uploads below is unchanged.
//  */
// const LegacyTempTransaction = () => {
//     /* The upload date, picked by the user. Every file uploaded is recorded under
//        it, Match offers only the files recorded under it, and uploading and the
//        Match button both stay disabled until it is picked. A match never changes
//        it. Each upload card has its own date (the day its file is for). */
//     const [uploadDate, setUploadDate] = useState("");
//     /* One match run per date on screen, oldest first. */
//     const [runs, setRuns] = useState([]);
//     const [day, setDay] = useState(null);
//     /* The custodian history list an upload hands back, so the new file shows at once. */
//     const [custodyHistory, setCustodyHistory] = useState(null);
//
//     /** @type {[Array<{ key: string; originalFileName: string; uploadedAt: string; uploadedAtMs: number; sizeBytes: number; }>, React.Dispatch<React.SetStateAction<any[]>>]} */
//     const [historyItems, setHistoryItems] = useState([]);
//     const [historyLoading, setHistoryLoading] = useState(false);
//     const [historyError, setHistoryError] = useState("");
//     const [downloadingKey, setDownloadingKey] = useState("");
//
//     const refreshHistory = useCallback(async () => {
//         setHistoryLoading(true);
//         setHistoryError("");
//         try {
//             const res = await fetch(
//                 `${BASE_URL}/transaction-uploader/upload-history?limit=${HISTORY_LIMIT}`
//             );
//             const text = await res.text();
//             let data = {};
//             try {
//                 data = text ? JSON.parse(text) : {};
//             } catch {
//                 throw new Error(
//                     "Could not read upload history (server did not return JSON)."
//                 );
//             }
//             if (!res.ok || data?.success === false) {
//                 throw new Error(
//                     data?.message || `Failed to load upload history (${res.status}).`
//                 );
//             }
//             setHistoryItems(Array.isArray(data.items) ? data.items.slice(0, HISTORY_LIMIT) : []);
//         } catch (err) {
//             const isNetwork =
//                 err instanceof TypeError ||
//                 String(err?.message || "").toLowerCase().includes("failed to fetch");
//             setHistoryError(
//                 isNetwork
//                     ? "Could not reach the upload server."
//                     : err.message || "Failed to load upload history"
//             );
//             setHistoryItems([]);
//         } finally {
//             setHistoryLoading(false);
//         }
//     }, []);
//
//     useEffect(() => {
//         refreshHistory();
//     }, [refreshHistory]);
//
//     // A completed run adds a row to the history below, so refresh when one
//     // reaches a terminal state rather than making the user reload the page.
//     // Changes whenever a run on screen starts or finishes.
//     const runsKey = runs.map((r) => `${r.runId}:${r.status}`).join(",");
//     const anyRunning = runs.some((r) => r.isRunning);
//
//     useEffect(() => {
//         if (runsKey && !anyRunning) refreshHistory();
//     }, [runsKey, anyRunning, refreshHistory]);
//
//     const handleHistoryDownload = useCallback(async (item) => {
//         if (!item?.key) return;
//         setDownloadingKey(item.key);
//         try {
//             const res = await fetch(
//                 `${BASE_URL}/transaction-uploader/upload-history/download?key=${encodeURIComponent(item.key)}`
//             );
//             const text = await res.text();
//             let data = {};
//             try {
//                 data = text ? JSON.parse(text) : {};
//             } catch {
//                 throw new Error("Could not prepare the download.");
//             }
//             if (!res.ok || data?.success === false || !data?.downloadUrl) {
//                 throw new Error(data?.message || "Could not prepare the download.");
//             }
//             window.open(data.downloadUrl, "_blank", "noopener,noreferrer");
//         } catch (err) {
//             setHistoryError(err.message || "Could not prepare the download.");
//         } finally {
//             setDownloadingKey("");
//         }
//     }, []);
//
//     /* Where the workflow stepper points — derived from real state only. */
//     const faReady = (day?.fa?.raw || []).length > 0;
//     const custodyReady = (day?.custody?.files || []).length > 0;
//     const currentStage = deriveStage({ faReady, custodyReady, runs });
//
//     return (
//         <MainLayout title="Transaction Upload">
//             <div className="temp-transaction-container txn-ops">
//                 <div className="txo-tracker-bar">
//                 <WorkflowTracker
//                     steps={WORKFLOW_STEPS}
//                     currentStage={currentStage}
//                     label="Reconciliation workflow"
//                 />
//
//                 <label className="cm-date">
//                     <span>Upload date</span>
//                     <input
//                         type="date"
//                         value={uploadDate}
//                         max={todayIso()}
//                         onChange={(e) => setUploadDate(e.target.value)}
//                         disabled={anyRunning}
//                     />
//                 </label>
//                 </div>
//
//                 <CustodyMatchPanel
//                     uploadDate={uploadDate}
//                     onRunChange={setRuns}
//                     onDayChange={setDay}
//                     onCustodyUploaded={setCustodyHistory}
//                 />
//
//                 <ExceptionGrid runs={runs} />
//
//                 {/* Keyed by the runs shown, so a commit's result never carries
//                     over to a different set of dates. */}
//                 <GateTwo
//                     key={runs.map((r) => r.runId).join(",")}
//                     runs={runs}
//                     onImported={refreshHistory}
//                 />
//
//                 <div className="txo-section-head txo-section-head--tight">
//                     <h2>History</h2>
//                 </div>
//
//                 <div className="temp-transaction-card upload-history-card">
//                     <div className="upload-history-card-head">
//                         <h3>Recent uploads</h3>
//                         {historyItems.length > 0 && (
//                             <span className="count-pill">
//                                 {historyItems.length} of {HISTORY_LIMIT}
//                             </span>
//                         )}
//                     </div>
//
//                     {historyError && (
//                         <p className="upload-history-error" role="alert">
//                             {historyError}
//                         </p>
//                     )}
//
//                     <div className="data-table-wrap">
//                         <table className="data-table data-table--spread">
//                             <thead>
//                                 <tr>
//                                     <th className="date">Uploaded date</th>
//                                     <th className="truncate">File name</th>
//                                     <th className="num">Size</th>
//                                     <th className="action">Download</th>
//                                 </tr>
//                             </thead>
//                             <tbody>
//                                 {historyLoading && historyItems.length === 0 ? (
//                                     <tr>
//                                         <td colSpan={4} className="empty-cell">
//                                             Loading…
//                                         </td>
//                                     </tr>
//                                 ) : historyItems.length === 0 ? (
//                                     <tr>
//                                         <td colSpan={4} className="empty-cell">
//                                             {historyError ? "—" : "Nothing uploaded yet — files you send will appear here."}
//                                         </td>
//                                     </tr>
//                                 ) : (
//                                     historyItems.map((item) => (
//                                         <tr key={item.key}>
//                                             <td className="date">{formatUploadedAt(item.uploadedAt)}</td>
//                                             <td className="truncate mono" title={item.originalFileName}>
//                                                 {item.originalFileName}
//                                             </td>
//                                             <td className="num">{formatBytes(item.sizeBytes)}</td>
//                                             <td className="action">
//                                                 <DownloadButton
//                                                     onClick={() => handleHistoryDownload(item)}
//                                                     busy={downloadingKey === item.key}
//                                                 />
//                                             </td>
//                                         </tr>
//                                     ))
//                                 )}
//                             </tbody>
//                         </table>
//                     </div>
//                 </div>
//
//                 <PreviousCustodianUploads latest={custodyHistory} />
//
//                 {/* Keyed off the run so a match that has just finished with
//                     differences shows up without a reload. */}
//                 <DifferenceReports refreshToken={runsKey} />
//             </div>
//         </MainLayout>
//     );
// };
