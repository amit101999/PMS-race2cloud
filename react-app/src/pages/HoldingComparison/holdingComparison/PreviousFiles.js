import React, { useCallback, useEffect, useState } from "react";

import * as api from "./api";

const when = (ms) =>
    ms ? new Date(ms).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" }) : "—";

const bytes = (n) => (n >= 1024 * 1024 ? `${(n / (1024 * 1024)).toFixed(2)} MB` : `${(n / 1024).toFixed(1)} KB`);

/**
 * The last 10 files of one kind, for download.
 *
 * Used for all three history lists (custodian uploads, FA holdings, difference
 * reports) — they differ only in title and which endpoint lists them.
 *
 * @param {{ title: string, load: () => Promise<{items: object[]}>, emptyText: string, refreshToken: any, hiddenKeys?: Set<string> }} props
 */
const PreviousFiles = ({ title, load, emptyText, refreshToken, hiddenKeys }) => {
    const [items, setItems] = useState([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState("");
    const [busyKey, setBusyKey] = useState("");

    const refresh = useCallback(async () => {
        setLoading(true);
        setError("");
        try {
            const res = await load();
            setItems((res.items || []).filter((item) => !hiddenKeys?.has(item.key)));
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    }, [load, hiddenKeys]);

    useEffect(() => {
        refresh();
    }, [refresh, refreshToken]);

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
            <h3>{title}</h3>

            {error ? (
                <p className="upload-history-error" role="alert">
                    {error}
                </p>
            ) : null}

            <div className="upload-history-table-wrapper">
                <table className="upload-history-table">
                    <thead>
                        <tr>
                            <th>Created on</th>
                            <th>Business date</th>
                            <th>File name</th>
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
                                <td colSpan={5} className="empty-cell">{error ? "—" : emptyText}</td>
                            </tr>
                        ) : (
                            items.map((item) => (
                                <tr key={item.key}>
                                    <td>{when(item.timeMs)}</td>
                                    <td>{item.businessDate}</td>
                                    <td className="filename-cell" title={item.fileName}>
                                        {item.fileName}
                                    </td>
                                    <td className="num">{bytes(item.sizeBytes)}</td>
                                    <td className="action">
                                        <button
                                            type="button"
                                            className="upload-history-download-btn"
                                            onClick={() => download(item)}
                                            disabled={busyKey === item.key}
                                        >
                                            {busyKey === item.key ? "Preparing…" : "Download"}
                                        </button>
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

export default PreviousFiles;
