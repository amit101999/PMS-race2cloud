import React, { useCallback, useEffect, useState } from "react";

import DownloadButton from "../../../components/common/DownloadButton";
import * as api from "./api";

const when = (ms) =>
    ms ? new Date(ms).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" }) : "—";

/**
 * The last five custodian files uploaded, for download.
 *
 * These are copies kept apart from the day's working files, so a file that was
 * replaced, removed or cleaned up since is still listed here.
 *
 * `latest` is the list an upload response carries. Stratus's listing lags a
 * fresh write by around twenty seconds, so re-fetching straight after an upload
 * would often miss the file just added; the upload's own answer does not.
 */
const PreviousCustodianUploads = ({ latest }) => {
    const [items, setItems] = useState([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState("");
    const [busyKey, setBusyKey] = useState("");

    const load = useCallback(async () => {
        setLoading(true);
        setError("");
        try {
            const res = await api.getCustodyHistory();
            setItems(res.items || []);
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    }, []);

    useEffect(() => {
        load();
    }, [load]);

    useEffect(() => {
        if (latest) setItems(latest);
    }, [latest]);

    const download = async (item) => {
        setBusyKey(item.key);
        setError("");
        try {
            const res = await api.getDownloadUrl(item.key);
            window.open(res.downloadUrl, "_blank", "noopener,noreferrer");
        } catch (err) {
            setError(err.message);
        } finally {
            setBusyKey("");
        }
    };

    return (
        <div className="temp-transaction-card upload-history-card">
            <h3>Previous custodian uploads</h3>

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
                            <th className="truncate">File name</th>
                            <th>Scheme</th>
                            <th>Type</th>
                            <th className="date">Date</th>
                            <th className="action">Download</th>
                        </tr>
                    </thead>
                    <tbody>
                        {loading && items.length === 0 ? (
                            <tr>
                                <td colSpan={6} className="empty-cell">
                                    Loading…
                                </td>
                            </tr>
                        ) : items.length === 0 ? (
                            <tr>
                                <td colSpan={6} className="empty-cell">
                                    {error ? "—" : "No custodian files uploaded yet."}
                                </td>
                            </tr>
                        ) : (
                            items.map((item) => (
                                <tr key={item.key}>
                                    <td className="date">{when(item.uploadedAtMs)}</td>
                                    <td className="truncate mono" title={item.fileName}>
                                        {item.fileName}
                                    </td>
                                    <td>{item.scheme}</td>
                                    <td>{api.FILE_TYPE_LABEL[item.fileType] || item.fileType}</td>
                                    <td className="date">{item.businessDate}</td>
                                    <td className="action">
                                        <DownloadButton
                                            onClick={() => download(item)}
                                            busy={busyKey === item.key}
                                        />
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

export default PreviousCustodianUploads;
