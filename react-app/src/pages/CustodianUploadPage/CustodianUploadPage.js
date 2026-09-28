import React, { useEffect, useState } from "react";

import { BASE_URL } from "../../constant";
import MainLayout from "../../layouts/MainLayout";
import { ApiError, PROFILE_TYPE, formatBytes, todayIso } from "../TempTransactionUpload/custody/api";
import { FileGlyph, UploadGlyph } from "../TempTransactionUpload/custody/CustodyMatchPanel";
import FieldMappingPanel from "../TempTransactionUpload/custody/FieldMappingPanel";
import StatusBadge, { STATUS } from "../TempTransactionUpload/custody/StatusBadge";
import UploadErrorDetails from "../TempTransactionUpload/custody/UploadErrorDetails";
import "../TempTransactionUpload/tokens.css";
import "../TempTransactionUpload/TempTransaction.css";
import "../TempTransactionUpload/custody/CustodyMatch.css";

const ROOT = `${BASE_URL}/custodian-upload`;

/** fetch + JSON, throwing ApiError with the server's body attached so the popup can show it. */
async function request(path, options) {
    let res;
    try {
        res = await fetch(`${ROOT}${path}`, options);
    } catch (err) {
        throw new ApiError("Could not reach the server. Check that the backend is running.", {
            detail: { code: "NETWORK", cause: String(err) },
        });
    }
    const body = await res.json().catch(() => ({}));
    if (!res.ok || body?.success === false) {
        throw new ApiError(body?.message || `Request failed (${res.status}).`, { status: res.status, detail: body });
    }
    return body;
}

const when = (ms) => new Date(ms).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" });

/**
 * Corporate Actions -> Custodian Upload.
 *
 * As on Holding Comparison, picking a file does not upload it: the Field
 * Mapping popup opens first, and the file is sent from the popup's Upload
 * button once the mapping is saved. The file is only stored for now.
 */
const CustodianUploadPage = () => {
    const [businessDate, setBusinessDate] = useState(todayIso);
    const [storedFile, setStoredFile] = useState(null);
    const [loading, setLoading] = useState(false);
    const [uploading, setUploading] = useState(false);
    const [removing, setRemoving] = useState(false);
    const [dragActive, setDragActive] = useState(false);
    /* A picked file waiting on the mapping popup. */
    const [mappingFile, setMappingFile] = useState(null);
    const [note, setNote] = useState("");
    const [error, setError] = useState(null);

    useEffect(() => {
        let cancelled = false;
        setStoredFile(null);
        setNote("");
        setError(null);
        setLoading(true);
        request(`/day/${businessDate}`)
            .then((res) => !cancelled && setStoredFile(res.file))
            .catch((err) => !cancelled && setError({ message: err.message, detail: err.detail }))
            .finally(() => !cancelled && setLoading(false));
        return () => {
            cancelled = true;
        };
    }, [businessDate]);

    const removeStoredFile = async () => {
        if (!window.confirm(`Remove ${storedFile.fileName} (${storedFile.businessDate})?`)) return;
        setRemoving(true);
        setError(null);
        setNote("");
        try {
            await request(`/file?key=${encodeURIComponent(storedFile.key)}`, { method: "DELETE" });
            setStoredFile(null);
        } catch (err) {
            setError({ message: err.message, detail: err.detail });
        } finally {
            setRemoving(false);
        }
    };

    const handleFilePicked = (file) => {
        setError(null);
        setNote("");
        setMappingFile(file);
    };

    /** Called by the popup. Throws on failure so the popup shows the error and stays open. */
    const uploadMappedFile = async () => {
        setUploading(true);
        try {
            const form = new FormData();
            form.append("file", mappingFile);
            form.append("businessDate", businessDate);
            const res = await request("/file", { method: "POST", body: form });
            setStoredFile(res.file);
            setNote(`${res.message} ${res.rowCount.toLocaleString()} row(s) read.`);
            return res;
        } finally {
            setUploading(false);
        }
    };

    const busy = uploading || removing;

    return (
        <MainLayout title="Custodian Upload">
            <div className="temp-transaction-container txn-ops">
                <div className="txo-tracker-bar txo-tracker-bar--static">
                    <label className="cm-date">
                        <span>Date</span>
                        <input
                            type="date"
                            value={businessDate}
                            onChange={(e) => e.target.value && setBusinessDate(e.target.value)}
                            disabled={busy}
                        />
                    </label>
                </div>

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
                            aria-label="Upload custodian corporate actions file"
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
                                    ? "Uploading…"
                                    : removing
                                      ? "Removing…"
                                      : storedFile
                                        ? "Replace custodian file"
                                        : "Upload CA Custodian File"}
                            </span>
                            <span className="up-zone-sub">
                                Drag &amp; drop or <span className="up-zone-browse">Browse</span>
                            </span>
                        </div>
                    </div>

                    {note ? <p className="cm-sub">{note}</p> : null}
                    {error ? <UploadErrorDetails message={error.message} detail={error.detail} /> : null}

                    {storedFile ? (
                        <div className="up-filelist-wrap">
                            <div className="up-filelist-head">Custodian file</div>
                            <div className="up-filelist">
                                <div className="up-file-row">
                                    <FileGlyph />
                                    <div className="up-file-main">
                                        <span className="up-file-name" title={storedFile.fileName}>
                                            {storedFile.fileName}
                                        </span>
                                        <span className="up-file-meta">
                                            {storedFile.businessDate} · {formatBytes(storedFile.sizeBytes)} · Uploaded{" "}
                                            {when(storedFile.timeMs)}
                                        </span>
                                    </div>
                                    <StatusBadge tone={STATUS.OK}>Ready</StatusBadge>
                                    <button
                                        type="button"
                                        className="up-file-remove"
                                        onClick={removeStoredFile}
                                        disabled={busy}
                                        aria-label={`Remove ${storedFile.fileName}`}
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

                <FieldMappingPanel
                    open={Boolean(mappingFile)}
                    file={mappingFile}
                    requestedType={PROFILE_TYPE.CUSTODY_CORP_ACTION}
                    types={[PROFILE_TYPE.CUSTODY_CORP_ACTION]}
                    autofill
                    showHints={false}
                    onUpload={uploadMappedFile}
                    onClose={() => setMappingFile(null)}
                />
            </div>
        </MainLayout>
    );
};

export default CustodianUploadPage;
