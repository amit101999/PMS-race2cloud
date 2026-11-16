import React, { useCallback, useEffect, useState } from "react";

import StatusBadge, { STATUS } from "./StatusBadge";
import * as api from "./api";

const when = (ms) =>
    ms ? new Date(ms).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" }) : "—";

const TYPE_LABEL = { FIRST: "First commit", RECOMMIT: "Re-commit" };

const STATUS_BADGE = {
    RUNNING: { tone: STATUS.PROCESSING, label: "Running" },
    COMPLETED: { tone: STATUS.OK, label: "Completed" },
    FAILED: { tone: STATUS.BAD, label: "Could not finish" },
};

/** Every Commit of one tab (Equity, or Mutual Funds with kind MUTUAL_FUND), newest first. A date committed twice has two rows. */
const CommitHistory = ({ refreshToken, kind = api.KIND.EQUITY }) => {
    const [items, setItems] = useState([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState("");

    const load = useCallback(async () => {
        setLoading(true);
        setError("");
        try {
            const res = await api.getCommitHistory(kind);
            setItems(res.items || []);
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    }, [kind]);

    useEffect(() => {
        load();
    }, [load, refreshToken]);

    return (
        <div className="temp-transaction-card upload-history-card">
            <h3>Commit history</h3>

            {error ? (
                <p className="upload-history-error" role="alert">
                    {error}
                </p>
            ) : null}

            <div className="data-table-wrap">
                <table className="data-table data-table--spread">
                    <thead>
                        <tr>
                            <th className="date">Committed on</th>
                            <th className="date">Trade date</th>
                            <th>Type</th>
                            <th className="num">Trades before</th>
                            <th className="num">Trades after</th>
                            <th className="num">Clients</th>
                            <th className="truncate">Files</th>
                            <th>Status</th>
                        </tr>
                    </thead>
                    <tbody>
                        {loading && items.length === 0 ? (
                            <tr>
                                <td colSpan={8} className="empty-cell">
                                    Loading…
                                </td>
                            </tr>
                        ) : items.length === 0 ? (
                            <tr>
                                <td colSpan={8} className="empty-cell">
                                    {error ? "—" : "No commits yet."}
                                </td>
                            </tr>
                        ) : (
                            items.map((item) => {
                                const badge = STATUS_BADGE[item.status] || { tone: STATUS.NEUTRAL, label: item.status };
                                return (
                                    <tr key={item.id}>
                                        <td className="date">{when(item.startedAtMs)}</td>
                                        <td className="date">{item.date}</td>
                                        <td>{TYPE_LABEL[item.commitType] || item.commitType}</td>
                                        <td className="num">{item.tradesBefore.toLocaleString()}</td>
                                        <td className="num">{item.tradesAfter.toLocaleString()}</td>
                                        <td className="num">{item.clients.toLocaleString()}</td>
                                        <td className="truncate mono" title={item.fileNames}>
                                            {item.fileNames || "—"}
                                        </td>
                                        <td>
                                            <StatusBadge tone={badge.tone}>{badge.label}</StatusBadge>
                                        </td>
                                    </tr>
                                );
                            })
                        )}
                    </tbody>
                </table>
            </div>
        </div>
    );
};

export default CommitHistory;
