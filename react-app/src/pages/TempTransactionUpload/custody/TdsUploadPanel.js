import React, { useCallback, useEffect, useRef, useState } from "react";

import FieldMappingPanel from "./FieldMappingPanel";
import UploadErrorDetails from "./UploadErrorDetails";
import StatusBadge, { STATUS } from "./StatusBadge";
import { FileGlyph, UploadGlyph } from "./CustodyMatchPanel";
import * as api from "./api";
import { useJobNotifications } from "../../../notifications/JobNotificationContext";
import "./CustodyMatch.css";

const toDisplayDate = (iso) => (iso ? iso.split("-").reverse().join("-") : "");

/**
 * TDS tab of Transaction Upload: the mutual fund TDS files of a trade date.
 * Each file is saved with the upload date picked in the tab bar (uploadDate).
 *
 * Several files can be picked at once and a date can take any number of files.
 * The first file of a batch goes through the TDS field mapping window, so the
 * operator confirms the mapping; the rest are uploaded straight after it, and
 * the server checks each one against the same mapping. For now the files are
 * only kept (bucket tds-files, one folder per trade date); nothing in our books
 * reads them.
 */
const TdsUploadPanel = ({ uploadDate }) => {
    const { reportJob } = useJobNotifications();
    const [tradeDate, setTradeDate] = useState("");
    const [files, setFiles] = useState([]);
    const [loading, setLoading] = useState(false);
    /* The file waiting in the mapping window, and the rest of its batch. */
    const [pending, setPending] = useState(null);
    const [busyText, setBusyText] = useState("");
    const [errors, setErrors] = useState([]);
    const [removingKey, setRemovingKey] = useState("");
    const [dragActive, setDragActive] = useState(false);
    const inputRef = useRef(null);
    /* Set once the mapping window's file is saved, so its closing goes on to the rest of the batch. */
    const firstSavedRef = useRef(false);

    const loadFiles = useCallback(async () => {
        if (!tradeDate) {
            setFiles([]);
            return;
        }
        setLoading(true);
        try {
            const res = await api.getTdsFiles(tradeDate);
            setFiles(res.files || []);
        } catch (err) {
            setErrors([{ message: err.message, detail: err.detail }]);
        } finally {
            setLoading(false);
        }
    }, [tradeDate]);

    useEffect(() => {
        setErrors([]);
        loadFiles();
    }, [loadFiles]);

    const busy = Boolean(pending) || Boolean(busyText);
    const dropDisabled = !uploadDate || !tradeDate || busy;

    const handlePicked = (fileList) => {
        const picked = Array.from(fileList || []);
        if (picked.length === 0 || dropDisabled) return;
        setErrors([]);
        firstSavedRef.current = false;
        setPending({ file: picked[0], rest: picked.slice(1), date: tradeDate, uploadDate });
    };

    /* What the mapping window calls for the batch's first file. Throws, so the window shows the reason. */
    const uploadFirst = async () => {
        const res = await api.uploadTdsFile({ file: pending.file, date: pending.date, uploadDate: pending.uploadDate });
        setFiles(res.files || []);
        firstSavedRef.current = true;
        return res;
    };

    /* The mapping window closed: after a saved first file, upload the rest; otherwise drop the batch. */
    const handleMappingClosed = async () => {
        const batch = pending;
        setPending(null);
        if (!batch || !firstSavedRef.current) return;

        const failed = [];
        for (let i = 0; i < batch.rest.length; i++) {
            const file = batch.rest[i];
            setBusyText(`Uploading ${i + 2} of ${batch.rest.length + 1}: ${file.name}`);
            try {
                const res = await api.uploadTdsFile({ file, date: batch.date, uploadDate: batch.uploadDate });
                setFiles(res.files || []);
            } catch (err) {
                failed.push({ message: `${file.name}: ${err.message}`, detail: err.detail });
            }
        }
        setBusyText("");
        setErrors(failed);

        const saved = batch.rest.length + 1 - failed.length;
        reportJob({
            id: `txn-tds-upload-${Date.now()}`,
            label: saved === 1 ? "TDS file uploaded" : "TDS files uploaded",
            status: failed.length > 0 ? "failed" : "completed",
            message:
                `${saved} TDS file${saved === 1 ? "" : "s"} saved for ${toDisplayDate(batch.date)}.` +
                (failed.length > 0 ? ` ${failed.length} refused.` : ""),
        });
    };

    const removeFile = async (file) => {
        if (!window.confirm(`Remove the TDS file ${file.fileName} from ${toDisplayDate(tradeDate)}?`)) return;
        setRemovingKey(file.key);
        setErrors([]);
        try {
            const res = await api.deleteTdsFile(tradeDate, file.key);
            setFiles(res.files || []);
            reportJob({
                id: `txn-tds-remove-${Date.now()}`,
                label: "TDS file removed",
                status: "completed",
                message: res.message,
            });
        } catch (err) {
            setErrors([{ message: err.message, detail: err.detail }]);
        } finally {
            setRemovingKey("");
        }
    };

    const downloadFile = async (file) => {
        setErrors([]);
        try {
            const res = await api.getTdsDownloadUrl(file.key);
            window.open(res.downloadUrl, "_blank", "noopener,noreferrer");
        } catch (err) {
            setErrors([{ message: err.message, detail: err.detail }]);
        }
    };

    let zoneText = "Upload TDS files";
    if (!uploadDate) zoneText = "Pick the upload date at the top first";
    else if (!tradeDate) zoneText = "Pick the trade date first";
    else if (busyText) zoneText = busyText;

    return (
        <>
            <div className="txo-section-head">
                <h2>TDS Files</h2>
            </div>

            <div className="cm-split cm-split--single">
                <div className="cm-col">
                    <h3 className="cm-col-title">Mutual Funds TDS</h3>

                    <div className="temp-transaction-card cm-filecard up-card">
                        <div className="up-controls">
                            <input
                                type="date"
                                className="up-select"
                                aria-label="Trade date the TDS files are for"
                                title="Trade date the TDS files are for"
                                value={tradeDate}
                                max={api.todayIso()}
                                disabled={busy}
                                onChange={(e) => setTradeDate(e.target.value)}
                            />
                        </div>

                        <div
                            className={dragActive ? "up-zone is-drag" : "up-zone"}
                            onDragEnter={(e) => {
                                e.preventDefault();
                                if (!dropDisabled) setDragActive(true);
                            }}
                            onDragOver={(e) => e.preventDefault()}
                            onDragLeave={() => setDragActive(false)}
                            onDrop={(e) => {
                                e.preventDefault();
                                setDragActive(false);
                                handlePicked(e.dataTransfer.files);
                            }}
                        >
                            <input
                                ref={inputRef}
                                type="file"
                                accept=".csv"
                                multiple
                                className="up-zone-input"
                                aria-label="Upload TDS files"
                                disabled={dropDisabled}
                                onChange={(e) => {
                                    const picked = e.target.files;
                                    handlePicked(picked);
                                    e.target.value = "";
                                }}
                            />
                            <div className="up-zone-visual">
                                <UploadGlyph />
                                <span className="up-zone-title">{zoneText}</span>
                                <span className="up-zone-sub">
                                    Drag &amp; drop or <span className="up-zone-browse">Browse</span> · several files at once
                                </span>
                            </div>
                        </div>

                        {errors.map((err, i) => (
                            <UploadErrorDetails key={i} message={err.message} detail={err.detail} />
                        ))}

                        {tradeDate ? (
                            <div className="up-filelist-wrap">
                                <div className="up-filelist-head">
                                    TDS files for {toDisplayDate(tradeDate)}
                                    {files.length > 0 ? ` (${files.length})` : ""}
                                </div>
                                {loading && files.length === 0 ? (
                                    <p className="up-window-note">Loading…</p>
                                ) : files.length === 0 ? (
                                    <p className="up-window-note">No TDS file uploaded for this date yet.</p>
                                ) : (
                                    <div className="up-filelist">
                                        {files.map((f) => (
                                            <div className="up-file-row" key={f.key}>
                                                <FileGlyph />
                                                <div className="up-file-main">
                                                    <button
                                                        type="button"
                                                        className="up-file-name up-file-link"
                                                        title={`Download ${f.fileName}`}
                                                        onClick={() => downloadFile(f)}
                                                    >
                                                        {f.fileName}
                                                    </button>
                                                    <span className="up-file-meta">
                                                        TDS · {toDisplayDate(tradeDate)} · {api.formatBytes(f.sizeBytes)} ·
                                                        uploaded {toDisplayDate(f.uploadedOn)}
                                                    </span>
                                                </div>
                                                {removingKey === f.key ? (
                                                    <StatusBadge tone={STATUS.PROCESSING}>Removing…</StatusBadge>
                                                ) : (
                                                    <button
                                                        type="button"
                                                        className="up-file-remove"
                                                        onClick={() => removeFile(f)}
                                                        disabled={busy || Boolean(removingKey)}
                                                        aria-label={`Remove ${f.fileName}`}
                                                    >
                                                        &times;
                                                    </button>
                                                )}
                                            </div>
                                        ))}
                                    </div>
                                )}
                            </div>
                        ) : null}
                    </div>
                </div>
            </div>

            {/* The batch's first file waits here until the TDS mapping is confirmed;
                closing without uploading drops the whole batch. */}
            <FieldMappingPanel
                open={Boolean(pending)}
                file={pending?.file || null}
                requestedType={api.PROFILE_TYPE.FA_MUTUAL_FUND_TDS}
                types={[api.PROFILE_TYPE.FA_MUTUAL_FUND_TDS]}
                pickedDate={pending?.date || null}
                onUpload={uploadFirst}
                onClose={handleMappingClosed}
                showHints={false}
            />
        </>
    );
};

export default TdsUploadPanel;
