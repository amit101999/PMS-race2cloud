import React, { useCallback, useEffect, useState } from "react";

import { FileGlyph, UploadGlyph } from "../../TempTransactionUpload/custody/CustodyMatchPanel";
import FieldMappingPanel from "../../TempTransactionUpload/custody/FieldMappingPanel";
import StatusBadge, { STATUS } from "../../TempTransactionUpload/custody/StatusBadge";
import UploadErrorDetails from "../../TempTransactionUpload/custody/UploadErrorDetails";
import WorkflowTracker from "../../TempTransactionUpload/WorkflowTracker";
import * as api from "./api";
import { useJobNotifications } from "../../../notifications/JobNotificationContext";

const RUN_POLL_MS = 3000;
const FA_POLL_MS = 5000;

const when = (ms) =>
    ms ? new Date(ms).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" }) : "—";

const WORKFLOW_STEPS = [
    { key: "upload", label: "Upload" },
    { key: "fa", label: "FA Holdings" },
    { key: "compare", label: "Compare" },
    { key: "review", label: "Review Differences" },
];

const bytes = (n) => (n >= 1024 * 1024 ? `${(n / (1024 * 1024)).toFixed(2)} MB` : `${(n / 1024).toFixed(1)} KB`);

/**
 * Date, custodian file, FA holdings generation and Run Comparison.
 *
 * The server re-checks Gate 1 on every run request; the disabled button here is
 * only so the operator can see what is still missing.
 */
const HoldingComparisonPanel = ({
    businessDate,
    onBusinessDateChange,
    run,
    onRunChange,
    onFilesChanged,
    onFileRemoved,
}) => {
    const [custodyFile, setCustodyFile] = useState(null);
    const { trackJob, reportJob } = useJobNotifications();
    const [fa, setFa] = useState(null);
    const [loading, setLoading] = useState(false);

    const [uploading, setUploading] = useState(false);
    const [removing, setRemoving] = useState(false);
    const [uploadError, setUploadError] = useState(null);
    const [uploadNote, setUploadNote] = useState("");
    const [dragActive, setDragActive] = useState(false);
    /* A file whose headers did not match, waiting on the mapping popup. */
    const [mappingFile, setMappingFile] = useState(null);

    const [generating, setGenerating] = useState(false);
    const [faError, setFaError] = useState("");

    const [starting, setStarting] = useState(false);
    const [runError, setRunError] = useState(null);
    const [runNote, setRunNote] = useState("");

    const loadDay = useCallback(async () => {
        setLoading(true);
        try {
            const res = await api.getDay(businessDate);
            // Stratus listings lag a fresh upload by ~20 seconds, so a file this
            // screen just uploaded is kept if the listing does not show it yet.
            setCustodyFile((current) =>
                current?.businessDate === businessDate && (!res.custodyFile || current.timeMs > res.custodyFile.timeMs)
                    ? current
                    : res.custodyFile,
            );
            setFa(res.fa);
            onRunChange(res.run);
            setRunError(null);
        } catch (err) {
            setRunError({ message: err.message, detail: err.detail });
        } finally {
            setLoading(false);
        }
    }, [businessDate, onRunChange]);

    useEffect(() => {
        setCustodyFile(null);
        setFa(null);
        setUploadError(null);
        setUploadNote("");
        setFaError("");
        setRunNote("");
        loadDay();
    }, [loadDay]);

    /* ── polling ──────────────────────────────────────────────────────── */

    const runId = run?.runId;
    const runIsRunning = Boolean(run?.isRunning);

    useEffect(() => {
        if (!runId || !runIsRunning) return undefined;
        const timer = setInterval(async () => {
            try {
                const res = await api.getRun(runId);
                onRunChange(res.run);
            } catch (err) {
                setRunError({ message: err.message, detail: err.detail });
            }
        }, RUN_POLL_MS);
        return () => clearInterval(timer);
    }, [runId, runIsRunning, onRunChange]);

    // Keep polling until the snapshot is listed: a COMPLETED job can precede the
    // listing by a few seconds, and Gate 1 needs the file itself.
    const faStatus = fa?.status || "NOT_STARTED";
    const faMoving =
        api.FA_IN_PROGRESS.includes(faStatus) || (faStatus === "COMPLETED" && !fa?.latestSnapshot);

    useEffect(() => {
        if (!faMoving) return undefined;
        const timer = setInterval(async () => {
            try {
                setFa(await api.getFaStatus(businessDate));
                // A check that failed on a passing network error should not stay on screen.
                setFaError("");
            } catch (err) {
                setFaError(err.message);
            }
        }, FA_POLL_MS);
        return () => clearInterval(timer);
    }, [faMoving, businessDate]);

    // A new snapshot belongs in the Previous FA Holdings list straight away.
    const snapshotKey = fa?.latestSnapshot?.key;
    useEffect(() => {
        if (snapshotKey) onFilesChanged();
    }, [snapshotKey, onFilesChanged]);

    /* ── custodian file ───────────────────────────────────────────────── */

    /*
     * Picking a file does not upload it. As on Transaction Upload, the file is
     * held while the Field Mapping popup confirms which column is which, and is
     * sent only from the popup's Upload button once the mapping is saved.
     */
    const handleFilePicked = (file) => {
        setUploadError(null);
        setUploadNote("");
        setMappingFile(file);
    };

    /** Called by the popup. Throws on failure so the popup shows the error and stays open. */
    const uploadMappedFile = async () => {
        setUploading(true);
        setUploadError(null);
        try {
            const res = await api.uploadCustodyFile(mappingFile, businessDate);
            setCustodyFile(res.file);
            setUploadNote(`${res.message} ${res.holdingCount.toLocaleString()} holding(s) read.`);
            reportJob({
                id: `hc-upload-${Date.now()}`,
                label: `Custodian holdings uploaded — ${mappingFile.name}`,
                status: "completed",
                message: `${res.holdingCount.toLocaleString()} holding(s) read for ${businessDate}.`,
            });
            onFilesChanged();
            // A new file supersedes earlier runs for the date.
            if (run) onRunChange(null);
            return res;
        } catch (err) {
            // Row problems are listed in full on the page; the popup shows the summary.
            if (err.detail?.rowErrors?.length) setUploadError({ message: err.message, detail: err.detail });
            throw err;
        } finally {
            setUploading(false);
        }
    };

    const removeCustodyFile = async () => {
        if (
            !window.confirm(
                `Remove ${custodyFile.fileName} (${custodyFile.businessDate})?

` +
                    "Earlier uploads stay in Previous Custodian Holdings Uploads, but none is staged until you upload the correct file.",
            )
        ) {
            return;
        }
        setRemoving(true);
        setUploadError(null);
        setUploadNote("");
        try {
            const res = await api.deleteCustodyFile(custodyFile.key);
            onFileRemoved(res.removedKeys);
            setCustodyFile(null);
            setUploadNote(res.message);
            if (run) onRunChange(null);
        } catch (err) {
            setUploadError({ message: err.message, detail: err.detail });
        } finally {
            setRemoving(false);
        }
    };

    /* ── FA holdings ──────────────────────────────────────────────────── */

    const handleGenerate = async () => {
        setGenerating(true);
        setFaError("");
        try {
            await api.generateFaHoldings(businessDate);
            // HCFA_<date> is the Jobs row the generator updates; the "@<ms>" part
            // makes each generation its own bell entry (see notifications.js).
            trackJob({
                id: `HCFA_${businessDate}@${Date.now()}`,
                label: `FA holdings — ${businessDate}`,
                message: "Generating FA holdings in the background.",
            });
            setFa(await api.getFaStatus(businessDate));
            if (run) onRunChange(null);
        } catch (err) {
            setFaError(err.message);
        } finally {
            setGenerating(false);
        }
    };

    /* ── gate 1 ───────────────────────────────────────────────────────── */

    // Both inputs are needed: the custodian file and a finished FA holdings snapshot.
    const faReady = faStatus === "COMPLETED" && Boolean(fa?.latestSnapshot);
    const inputsReady = Boolean(custodyFile) && faReady;
    const canRun = inputsReady && !runIsRunning && !starting && !uploading;

    const handleRun = async () => {
        setStarting(true);
        setRunError(null);
        setRunNote("");
        try {
            const res = await api.startRun(businessDate, custodyFile?.key);
            onRunChange(res.run);
            setRunNote(res.reused ? res.message : "");
            if (res.reused && !res.run?.isRunning) {
                reportJob({
                    id: `hc-run-reused-${Date.now()}`,
                    label: `Holding comparison — ${businessDate}`,
                    status: "completed",
                    message: res.message || "These files were already compared; showing that result.",
                });
            } else if (res.run?.runId) {
                // The runId (HC-<ms>) is the bell's key: the server settles it from the run row.
                trackJob({
                    id: res.run.runId,
                    label: `Holding comparison — ${businessDate}`,
                    message: "Comparing custodian and FA holdings in the background.",
                });
            }
        } catch (err) {
            setRunError({ message: err.message, detail: err.detail });
        } finally {
            setStarting(false);
        }
    };

    const busy = runIsRunning || uploading || removing || generating || starting;

    /* Where the progress bar points — worked out from the same state as the button above. */
    let currentStage = 3; // a finished comparison: review its differences
    if (!custodyFile) currentStage = 0;
    else if (!faReady) currentStage = 1;
    else if (!run || run.isRunning || run.status === "FAILED" || run.status === "SUPERSEDED") currentStage = 2;

    return (
        <>
            <div className="txo-tracker-bar">
                <WorkflowTracker
                    steps={WORKFLOW_STEPS}
                    currentStage={currentStage}
                    label="Holding comparison workflow"
                />

                <label className="cm-date">
                    <span>Date</span>
                    <input
                        type="date"
                        value={businessDate}
                        onChange={(e) => e.target.value && onBusinessDateChange(e.target.value)}
                        disabled={busy}
                    />
                </label>
            </div>

            <div className="txo-section-head">
                <h2>Files</h2>
            </div>

            {/* Same upload layout as Transaction Upload: a drop zone, then the staged file as a row. */}
            <div className="cm-split">
                <div className="cm-col">
                    <h3 className="cm-col-title">Custodian Consolidated Holdings</h3>

                    <div className="temp-transaction-card cm-filecard up-card">
                        <div
                            className={dragActive ? "up-zone is-drag" : "up-zone"}
                            onDragEnter={(e) => {
                                e.preventDefault();
                                if (!busy) setDragActive(true);
                            }}
                            onDragOver={(e) => e.preventDefault()}
                            onDragLeave={() => setDragActive(false)}
                            onDrop={(e) => {
                                e.preventDefault();
                                setDragActive(false);
                                if (busy) return;
                                const file = e.dataTransfer.files && e.dataTransfer.files[0];
                                if (file) handleFilePicked(file);
                            }}
                        >
                            <input
                                type="file"
                                accept=".csv"
                                className="up-zone-input"
                                aria-label="Upload custodian holdings file"
                                disabled={busy}
                                onChange={(e) => {
                                    const file = e.target.files[0];
                                    e.target.value = "";
                                    if (file) handleFilePicked(file);
                                }}
                            />
                            <div className="up-zone-visual">
                                <UploadGlyph />
                                <span className="up-zone-title">
                                    {uploading
                                        ? "Validating…"
                                        : removing
                                          ? "Removing…"
                                          : custodyFile
                                            ? "Replace custodian file"
                                            : "Upload custodian file"}
                                </span>
                                <span className="up-zone-sub">
                                    Drag &amp; drop or <span className="up-zone-browse">Browse</span>
                                </span>
                            </div>
                        </div>

                        {uploadNote ? <p className="cm-sub">{uploadNote}</p> : null}
                        {uploadError ? (
                            <UploadErrorDetails message={uploadError.message} detail={uploadError.detail} />
                        ) : null}

                        {custodyFile ? (
                            <div className="up-filelist-wrap">
                                <div className="up-filelist-head">Custodian file</div>
                                <div className="up-filelist">
                                    <div className="up-file-row">
                                        <FileGlyph />
                                        <div className="up-file-main">
                                            <span className="up-file-name" title={custodyFile.fileName}>
                                                {custodyFile.fileName}
                                            </span>
                                            <span className="up-file-meta">
                                                {custodyFile.businessDate} · {bytes(custodyFile.sizeBytes)} · Uploaded{" "}
                                                {when(custodyFile.timeMs)}
                                            </span>
                                        </div>
                                        <StatusBadge tone={STATUS.OK}>Ready</StatusBadge>
                                        <button
                                            type="button"
                                            className="up-file-remove"
                                            onClick={removeCustodyFile}
                                            disabled={busy}
                                            aria-label={`Remove ${custodyFile.fileName}`}
                                            title="Remove this file"
                                        >
                                            &times;
                                        </button>
                                    </div>
                                </div>
                            </div>
                        ) : loading ? (
                            <p className="cm-empty">Loading…</p>
                        ) : null}
                    </div>
                </div>

                <div className="cm-col">
                    <h3 className="cm-col-title">FA Holdings</h3>

                    <div className="temp-transaction-card cm-filecard up-card">
                        {/* FA holdings are generated from our own data rather than uploaded,
                            so this side has a button where the other has a drop zone. */}
                        <div className="up-controls">
                            <button
                                type="button"
                                className="cm-primary"
                                onClick={handleGenerate}
                                disabled={busy || api.FA_IN_PROGRESS.includes(faStatus)}
                            >
                                {api.FA_IN_PROGRESS.includes(faStatus)
                                    ? "Generating…"
                                    : fa?.latestSnapshot
                                      ? "Regenerate FA Holdings"
                                      : "Generate FA Holdings"}
                            </button>
                            <span className={faStatus === "COMPLETED" ? "cm-staged" : "cm-staged muted"}>
                                {api.FA_STATUS_LABEL[faStatus] || faStatus}
                            </span>
                        </div>

                        {fa?.message || faError ? (
                            <p className="cm-verdict bad">{faError || fa.message}</p>
                        ) : null}

                        {fa?.latestSnapshot ? (
                            <div className="up-filelist-wrap">
                                <div className="up-filelist-head">Generated file</div>
                                <div className="up-filelist">
                                    <div className="up-file-row">
                                        <FileGlyph />
                                        <div className="up-file-main">
                                            <span className="up-file-name" title={fa.latestSnapshot.fileName}>
                                                {fa.latestSnapshot.fileName}
                                            </span>
                                            <span className="up-file-meta">
                                                {fa.latestSnapshot.businessDate} · {bytes(fa.latestSnapshot.sizeBytes)} ·
                                                Generated {when(fa.latestSnapshot.timeMs)}
                                            </span>
                                        </div>
                                        <StatusBadge tone={STATUS.OK}>Ready</StatusBadge>
                                    </div>
                                </div>
                            </div>
                        ) : (
                            <p className="cm-empty">
                                {loading ? "Loading…" : "No FA holdings generated for this date."}
                            </p>
                        )}
                    </div>
                </div>
            </div>

            <div className="temp-transaction-card">
                <div className="cm-gate-row">
                    <button type="button" className="cm-primary" onClick={handleRun} disabled={!canRun}>
                        {runIsRunning || starting ? "Comparing…" : "Run Comparison"}
                    </button>
                </div>

                {run ? (
                    <div className="cm-progress">
                        <div className="cm-progress-head">
                            <strong>{run.stageDetail || run.status}</strong>
                            <span className="cm-run-id">{run.runId}</span>
                        </div>

                        {!run.isRunning && run.status !== "FAILED" ? (
                            <div className="cm-stats">
                                <span>{run.counts.custodyRows.toLocaleString()} custody rows</span>
                                <span>{run.counts.faRows.toLocaleString()} FA rows</span>
                                <span>{run.counts.totalKeys.toLocaleString()} holdings compared</span>
                            </div>
                        ) : null}

                        {run.status === "COMPLETED_CLEAN" ? (
                            <p className="cm-verdict ok">Holdings agree — no differences.</p>
                        ) : null}

                        {run.status === "COMPLETED_WITH_EXCEPTIONS" ? (
                            <p className="cm-verdict bad">
                                {run.counts.exceptions.toLocaleString()} difference(s):{" "}
                                {run.counts.quantityMismatch.toLocaleString()} quantity mismatch,{" "}
                                {run.counts.missingInCustody.toLocaleString()} missing in custody,{" "}
                                {run.counts.missingInFa.toLocaleString()} missing in FA.
                            </p>
                        ) : null}

                        {run.status === "SUPERSEDED" ? (
                            <p className="cm-verdict bad">
                                The custodian file, FA holdings or field mapping changed after this comparison.
                                Run the comparison again.
                            </p>
                        ) : null}

                        {run.status === "FAILED" ? (
                            <p className="cm-verdict bad">
                                The comparison failed{run.error ? `: ${run.error.message}` : "."}
                            </p>
                        ) : null}
                    </div>
                ) : null}

                {runNote ? (
                    <div className="cm-reused" role="status">
                        {runNote}
                    </div>
                ) : null}
                {runError ? <UploadErrorDetails message={runError.message} detail={runError.detail} /> : null}
            </div>

            <FieldMappingPanel
                open={Boolean(mappingFile)}
                file={mappingFile}
                requestedType={api.CUSTODY_PROFILE_TYPE}
                types={[api.CUSTODY_PROFILE_TYPE]}
                autofill
                showHints={false}
                onUpload={uploadMappedFile}
                onClose={() => setMappingFile(null)}
            />
        </>
    );
};

export default HoldingComparisonPanel;
