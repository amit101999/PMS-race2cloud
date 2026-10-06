import React, { useCallback, useEffect, useState } from "react";
import DownloadButton from "../../../components/common/DownloadButton";

import * as api from "./api";

const when = (ms) =>
    ms ? new Date(ms).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" }) : "—";

const bytes = (n) => (n >= 1024 * 1024 ? `${(n / (1024 * 1024)).toFixed(2)} MB` : `${(n / 1024).toFixed(1)} KB`);

/**
 * The last 10 files of one kind, for download.
 *
 * Used for all three history lists (custodian uploads, FA holdings, difference
 * reports) — they differ only in title and which endpoint lists them. The Split
 * page reuses it with its own download endpoint and date label.
 *
 * @param {{ title: string, load: () => Promise<{items: object[]}>, emptyText: string, refreshToken: any,
 *   hiddenKeys?: Set<string>, getDownloadUrl?: (key: string) => Promise<{downloadUrl: string}>, dateLabel?: string }} props
 */
const PreviousFiles = ({
    title,
    load,
    emptyText,
    refreshToken,
    hiddenKeys,
    getDownloadUrl = api.getDownloadUrl,
    dateLabel = "Date",
}) => {
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
            const res = await getDownloadUrl(item.key);
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
                            <th className="date">Created on</th>
                            <th className="date">{dateLabel}</th>
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
                                <td colSpan={5} className="empty-cell">{error ? "—" : emptyText}</td>
                            </tr>
                        ) : (
                            items.map((item) => (
                                <tr key={item.key}>
                                    <td className="date">{when(item.timeMs)}</td>
                                    <td className="date">{item.businessDate}</td>
                                    <td className="truncate mono" title={item.fileName}>
                                        {item.fileName}
                                    </td>
                                    <td className="num">{bytes(item.sizeBytes)}</td>
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

export default PreviousFiles;
