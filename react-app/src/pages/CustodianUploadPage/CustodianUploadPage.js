import React, { useEffect, useState } from "react";

import DownloadButton from "../../components/common/DownloadButton";
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

/** A stored file's table import, as a badge. Modules can read the file only once it is Ready. */
const IMPORT_BADGE = {
    DONE: [STATUS.OK, "Ready"],
    IMPORTING: [STATUS.PROCESSING, "Importing…"],
    FAILED: [STATUS.BAD, "Import failed"],
};
const NOT_IMPORTED_BADGE = [STATUS.WARN, "Not imported"];
const IMPORT_POLL_MS = 10000;

/**
 * One custodian file: its upload box, the file stored for the date, and its
 * Field Mapping popup.
 *
 * As on Holding Comparison, picking a file does not upload it: the Field
 * Mapping popup opens first, and the file is sent from the popup's Upload
 * button once the mapping is saved.
 *
 * @param {"entitlement"|"allocation"} kind   which file (the backend's folder and mapping)
 * @param {string} title                       e.g. "Custodian Ex-Entitlement File"
 * @param {string} listHeading                 heading over the stored file
 * @param {string} profileType                 the mapping type the popup edits
 * @param {string} businessDate                yyyy-mm-dd from the page's Date picker
 * @param {(kind: string, busy: boolean) => void} onBusyChange  lets the page lock the Date picker
 * @param {(kind: string, change: {added?: object, removed?: object}) => void} onFileChange  keeps Previous Uploads current
 */
const CustodianFileCard = ({ kind, title, listHeading, profileType, businessDate, onBusyChange, onFileChange }) => {
    const [storedFile, setStoredFile] = useState(null);
    const [loading, setLoading] = useState(false);
    const [uploading, setUploading] = useState(false);
    const [removing, setRemoving] = useState(false);
    const [dragActive, setDragActive] = useState(false);
    /* A picked file waiting on the mapping popup. */
    const [mappingFile, setMappingFile] = useState(null);
    const [note, setNote] = useState("");
    const [error, setError] = useState(null);

    const busy = uploading || removing;

    useEffect(() => {
        onBusyChange(kind, busy);
    }, [kind, busy, onBusyChange]);

    useEffect(() => {
        let cancelled = false;
        setStoredFile(null);
        setNote("");
        setError(null);
        setLoading(true);
        request(`/day/${businessDate}?kind=${kind}`)
            .then((res) => !cancelled && setStoredFile(res.file))
            .catch((err) => !cancelled && setError({ message: err.message, detail: err.detail }))
            .finally(() => !cancelled && setLoading(false));
        return () => {
            cancelled = true;
        };
    }, [businessDate, kind]);

    // The rows are imported in the background after an upload; ask again until it finishes.
    const importing = storedFile?.importStatus === "IMPORTING";
    useEffect(() => {
        if (!importing) return undefined;
        const timer = setInterval(() => {
            request(`/day/${businessDate}?kind=${kind}`)
                .then((res) =>
                    // The listing lags a fresh upload, so only take news about the file shown.
                    setStoredFile((prev) => (res.file && prev && res.file.key === prev.key ? res.file : prev)),
                )
                .catch(() => {});
        }, IMPORT_POLL_MS);
        return () => clearInterval(timer);
    }, [importing, businessDate, kind]);

    const removeStoredFile = async () => {
        if (!window.confirm(`Remove ${storedFile.fileName} (${storedFile.businessDate})?`)) return;
        setRemoving(true);
        setError(null);
        setNote("");
        try {
            await request(`/file?key=${encodeURIComponent(storedFile.key)}`, { method: "DELETE" });
            onFileChange(kind, { removed: storedFile });
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

    /**
     * Called by the popup. Throws on failure so the popup shows the error and
     * stays open; for the same file uploaded before (DUPLICATE_FILE) the popup
     * asks, and calls again with replace = true.
     */
    const uploadMappedFile = async ({ replace } = {}) => {
        setUploading(true);
        try {
            const form = new FormData();
            form.append("file", mappingFile);
            form.append("businessDate", businessDate);
            form.append("kind", kind);
            if (replace) form.append("replace", "true");
            const res = await request("/file", { method: "POST", body: form });
            setStoredFile(res.file);
            onFileChange(kind, { added: res.file });
            setNote(`${res.message} ${res.rowCount.toLocaleString()} row(s) read.`);
            return res;
        } finally {
            setUploading(false);
        }
    };

    return (
        <div className="temp-transaction-card cm-filecard up-card">
            <h3>Upload {title}</h3>

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
                    aria-label={`Upload ${title}`}
                    disabled={busy}
                    onChange={(e) => {
                        const file = e.target.files[0];
                        e.target.value = "";
                        if (file) handleFilePicked(file);
                    }}
                />
                <div className="up-zone-visual">
                    <UploadGlyph />
                    {busy ? (
                        <span className="up-zone-title">{uploading ? "Uploading…" : "Removing…"}</span>
                    ) : (
                        <span className="up-zone-sub">
                            Drag &amp; drop or <span className="up-zone-browse">Browse</span>
                            {storedFile ? " to replace" : ""}
                        </span>
                    )}
                </div>
            </div>

            {/* The stored file comes straight after the box and the messages after it,
                so the two cards' file rows line up whatever length the messages are. */}
            {storedFile ? (
                <div className="up-filelist-wrap">
                    <div className="up-filelist-head">{listHeading}</div>
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
                            <StatusBadge tone={(IMPORT_BADGE[storedFile.importStatus] || NOT_IMPORTED_BADGE)[0]}>
                                {(IMPORT_BADGE[storedFile.importStatus] || NOT_IMPORTED_BADGE)[1]}
                            </StatusBadge>
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

            {note ? <p className="cm-sub">{note}</p> : null}
            {storedFile?.importStatus === "FAILED" ? (
                <UploadErrorDetails message={storedFile.importError || "The file's rows could not be imported. Remove it and upload it again."} />
            ) : null}
            {error ? <UploadErrorDetails message={error.message} detail={error.detail} /> : null}

            <FieldMappingPanel
                open={Boolean(mappingFile)}
                file={mappingFile}
                requestedType={profileType}
                types={[profileType]}
                autofill
                showHints={false}
                onUpload={uploadMappedFile}
                onClose={() => setMappingFile(null)}
            />
        </div>
    );
};

/**
 * The last 5 uploads of one custodian file kind, for download (HISTORY_LIMIT on the server).
 *
 * `change` is the card's latest upload or removal. Stratus's listing lags a
 * write or delete by around twenty seconds, so it is applied to the list here
 * rather than re-fetching straight away.
 */
const PreviousUploads = ({ kind, title, change }) => {
    const [items, setItems] = useState([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState("");
    const [busyKey, setBusyKey] = useState("");

    useEffect(() => {
        let cancelled = false;
        setLoading(true);
        request(`/history?kind=${kind}`)
            .then((res) => !cancelled && setItems(res.items || []))
            .catch((err) => !cancelled && setError(err.message))
            .finally(() => !cancelled && setLoading(false));
        return () => {
            cancelled = true;
        };
    }, [kind]);

    useEffect(() => {
        if (change?.added) {
            setItems((prev) => [change.added, ...prev.filter((i) => i.key !== change.added.key)].slice(0, 5));
        }
        if (change?.removed) {
            // Removing a file deletes every upload of this kind for its date.
            setItems((prev) => prev.filter((i) => i.businessDate !== change.removed.businessDate));
        }
    }, [change]);

    const download = async (item) => {
        setBusyKey(item.key);
        setError("");
        try {
            const res = await request(`/download?key=${encodeURIComponent(item.key)}`);
            window.open(res.downloadUrl, "_blank", "noopener,noreferrer");
        } catch (err) {
            setError(err.message);
        } finally {
            setBusyKey("");
        }
    };

    return (
        <div className="temp-transaction-card upload-history-card">
            <h3>{title}</h3>

            {error ? (
                <p className="upload-history-error" role="alert">
                    {error}
                </p>
            ) : null}

            <div className="data-table-wrap">
                <table className="data-table data-table--spread">
                    <thead>
                        <tr>
                            <th className="date">Uploaded on</th>
                            <th className="date">Business date</th>
                            <th className="truncate">File name</th>
                            <th className="num">Size</th>
                            <th className="action">Download</th>
                        </tr>
                    </thead>
                    <tbody>
                        {loading && items.length === 0 ? (
                            <tr>
                                <td colSpan={5} className="empty-cell">Loading…</td>
                            </tr>
                        ) : items.length === 0 ? (
                            <tr>
                                <td colSpan={5} className="empty-cell">{error ? "—" : "No files uploaded yet."}</td>
                            </tr>
                        ) : (
                            items.map((item) => (
                                <tr key={item.key}>
                                    <td className="date">{when(item.timeMs)}</td>
                                    <td className="date">{item.businessDate}</td>
                                    <td className="truncate mono" title={item.fileName}>
                                        {item.fileName}
                                    </td>
                                    <td className="num">{formatBytes(item.sizeBytes)}</td>
                                    <td className="action">
                                        <DownloadButton onClick={() => download(item)} busy={busyKey === item.key} />
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

/**
 * Corporate Actions -> Custodian Upload.
 *
 * Two custodian files per date, each mapped and stored on its own:
 *   - Entitlement ("EX Entitlement Vs Benefit Collection combo"), read by Split / Bonus / Dividend.
 *   - Allocation ("Corporate Benefit Collection"), stored for the modules to read.
 */
const CustodianUploadPage = () => {
    const [businessDate, setBusinessDate] = useState(todayIso);
    /* Which cards are mid-upload / mid-remove; the Date picker is locked while any is. */
    const [busyKinds, setBusyKinds] = useState({});

    const onBusyChange = React.useCallback(
        (kind, busy) => setBusyKinds((prev) => (prev[kind] === busy ? prev : { ...prev, [kind]: busy })),
        [],
    );
    const anyBusy = Object.values(busyKinds).some(Boolean);

    /* Each card's latest upload or removal, passed to its Previous Uploads table. */
    const [fileChanges, setFileChanges] = useState({});
    const onFileChange = React.useCallback((kind, change) => setFileChanges((prev) => ({ ...prev, [kind]: change })), []);

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
                            disabled={anyBusy}
                        />
                    </label>
                </div>

                {/* Each card in its own column, as on Transaction Upload, so the
                    card-after-card top margin does not push the right one down. */}
                <div className="cm-split">
                    <div className="cm-col">
                        <CustodianFileCard
                            kind="entitlement"
                            title="Custodian Ex-Entitlement File"
                            listHeading="Custodian entitlement file"
                            profileType={PROFILE_TYPE.CUSTODY_CORP_ACTION}
                            businessDate={businessDate}
                            onBusyChange={onBusyChange}
                            onFileChange={onFileChange}
                        />
                    </div>
                    <div className="cm-col">
                        <CustodianFileCard
                            kind="allocation"
                            title="Custodian Benefit Collection File"
                            listHeading="Custodian allocation file (Corporate Benefit Collection)"
                            profileType={PROFILE_TYPE.CUSTODY_CORP_ALLOCATION}
                            businessDate={businessDate}
                            onBusyChange={onBusyChange}
                            onFileChange={onFileChange}
                        />
                    </div>
                </div>

                <PreviousUploads
                    kind="entitlement"
                    title="Previous Custodian Ex-Entitlement File Uploads"
                    change={fileChanges.entitlement}
                />
                <PreviousUploads
                    kind="allocation"
                    title="Previous Custodian Benefit Collection File Uploads"
                    change={fileChanges.allocation}
                />
            </div>
        </MainLayout>
    );
};

export default CustodianUploadPage;
