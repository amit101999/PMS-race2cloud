import React, { useCallback, useEffect, useState } from "react";

import FieldMappingPanel from "../../TempTransactionUpload/custody/FieldMappingPanel";
import UploadErrorDetails from "../../TempTransactionUpload/custody/UploadErrorDetails";
import * as api from "./api";

const RUN_POLL_MS = 3000;
const FA_POLL_MS = 5000;

const when = (ms) =>
    ms ? new Date(ms).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" }) : "—";

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
    const [fa, setFa] = useState(null);
    const [loading, setLoading] = useState(false);

    const [uploading, setUploading] = useState(false);
    const [removing, setRemoving] = useState(false);
    const [uploadError, setUploadError] = useState(null);
    const [uploadNote, setUploadNote] = useState("");
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
            setFa(await api.getFaStatus(businessDate));
            if (run) onRunChange(null);
        } catch (err) {
            setFaError(err.message);
        } finally {
            setGenerating(false);
        }
    };

    /* ── gate 1 ───────────────────────────────────────────────────────── */

    const blockers = [];
    if (!custodyFile) blockers.push("Upload the custodian holdings file for this date.");
    if (faStatus !== "COMPLETED" || !fa?.latestSnapshot) blockers.push("Generate FA holdings for this date.");
    const canRun = blockers.length === 0 && !runIsRunning && !starting && !uploading;

    const handleRun = async () => {
        setStarting(true);
        setRunError(null);
        setRunNote("");
        try {
            const res = await api.startRun(businessDate, custodyFile?.key);
            onRunChange(res.run);
            setRunNote(res.reused ? res.message : "");
        } catch (err) {
            setRunError({ message: err.message, detail: err.detail });
        } finally {
            setStarting(false);
        }
    };

    const busy = runIsRunning || uploading || removing || generating || starting;

    return (
        <>
            <div className="cm-date-bar">
                <label className="cm-date">
                    <span>Business date</span>
                    <input
                        type="date"
                        value={businessDate}
                        onChange={(e) => e.target.value && onBusinessDateChange(e.target.value)}
                        disabled={busy}
                    />
                </label>
            </div>

            <div className="cm-split cm-split--even">
                <div className="cm-col">
                    <div className="cm-col-head">
                        <h3>Custodian Consolidated Holdings</h3>
                    </div>

                    <div className="temp-transaction-card">
                        {custodyFile ? (
                            <div className="data-table-wrap">
                            <table className="data-table">
                                <thead>
                                    <tr>
                                        <th className="date">Business date</th>
                                        <th>File</th>
                                        <th className="date">Uploaded</th>
                                        <th className="action" aria-label="Remove" />
                                    </tr>
                                </thead>
                                <tbody>
                                    <tr>
                                        <td className="date">{custodyFile.businessDate}</td>
                                        <td className="truncate" title={custodyFile.fileName}>
                                            {custodyFile.fileName}
                                            <span className="cm-file-size">{bytes(custodyFile.sizeBytes)}</span>
                                        </td>
                                        <td className="date">{when(custodyFile.timeMs)}</td>
                                        <td className="action">
                                            <button
                                                type="button"
                                                className="cm-remove"
                                                onClick={removeCustodyFile}
                                                disabled={busy}
                                                aria-label={`Remove ${custodyFile.fileName}`}
                                                title="Remove this file"
                                            >
                                                ×
                                            </button>
                                        </td>
                                    </tr>
                                </tbody>
                            </table>
                            </div>
                        ) : (
                            <p className="cm-empty">
                                {loading ? "Loading…" : "No custodian file uploaded for this date."}
                            </p>
                        )}

                        <div className="cm-addfile">
                            <span className="cm-addfile-title">
                                {custodyFile ? "Replace custodian file" : "Add custodian file"}
                            </span>
                            <input
                                type="file"
                                accept=".csv"
                                disabled={busy}
                                onChange={(e) => {
                                    const file = e.target.files[0];
                                    e.target.value = "";
                                    if (file) handleFilePicked(file);
                                }}
                            />
                            {uploading ? <span className="cm-addfile-busy">Validating…</span> : null}
                            {removing ? <span className="cm-addfile-busy">Removing…</span> : null}
                        </div>

                        {uploadNote ? <p className="cm-sub">{uploadNote}</p> : null}
                        {uploadError ? (
                            <UploadErrorDetails message={uploadError.message} detail={uploadError.detail} />
                        ) : null}
                    </div>
                </div>

                <div className="cm-col">
                    <div className="cm-col-head">
                        <h3>FA Holdings</h3>
                    </div>

                    <div className="temp-transaction-card">
                        {fa?.latestSnapshot ? (
                            <div className="data-table-wrap">
                            <table className="data-table">
                                <thead>
                                    <tr>
                                        <th className="date">Business date</th>
                                        <th>File</th>
                                        <th className="date">Generated</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    <tr>
                                        <td className="date">{fa.latestSnapshot.businessDate}</td>
                                        <td className="truncate" title={fa.latestSnapshot.fileName}>
                                            {fa.latestSnapshot.fileName}
                                            <span className="cm-file-size">{bytes(fa.latestSnapshot.sizeBytes)}</span>
                                        </td>
                                        <td className="date">{when(fa.latestSnapshot.timeMs)}</td>
                                    </tr>
                                </tbody>
                            </table>
                            </div>
                        ) : (
                            <p className="cm-empty">
                                {loading ? "Loading…" : "No FA holdings generated for this date."}
                            </p>
                        )}

                        <div className="cm-addfile">
                            <span className="cm-addfile-title">
                                {fa?.latestSnapshot ? "Regenerate FA holdings" : "Generate FA holdings"}
                            </span>
                            <div className="cm-fa">
                                <button
                                    type="button"
                                    className="cm-primary"
                                    onClick={handleGenerate}
                                    disabled={busy || api.FA_IN_PROGRESS.includes(faStatus)}
                                >
                                    {api.FA_IN_PROGRESS.includes(faStatus) ? "Generating…" : "Generate FA Holdings"}
                                </button>
                                <span className={faStatus === "COMPLETED" ? "cm-staged" : "cm-staged muted"}>
                                    {api.FA_STATUS_LABEL[faStatus] || faStatus}
                                </span>
                            </div>
                        </div>

                        {fa?.message || faError ? (
                            <p className="cm-verdict bad">{faError || fa.message}</p>
                        ) : null}
                    </div>
                </div>
            </div>

            <div className="temp-transaction-card">
                <div className="cm-gate-row">
                    <button type="button" className="cm-primary" onClick={handleRun} disabled={!canRun}>
                        {runIsRunning || starting ? "Comparing…" : "Run Comparison"}
                    </button>

                    {blockers.length > 0 && !runIsRunning ? (
                        <ul className="cm-blockers">
                            {blockers.map((b) => (
                                <li key={b}>{b}</li>
                            ))}
                        </ul>
                    ) : null}
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
