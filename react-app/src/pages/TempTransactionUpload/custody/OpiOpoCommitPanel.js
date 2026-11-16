import React, { useCallback, useEffect, useState } from "react";

import StatusBadge, { STATUS } from "./StatusBadge";
import UploadErrorDetails from "./UploadErrorDetails";
import * as api from "./api";
import { useJobNotifications } from "../../../notifications/JobNotificationContext";

const POLL_MS = 5000;

const STEP_LABEL = {
    INSERT: "Adding the entries",
    REBUILD: "Queuing the holdings update",
    WAITING: "Updating holdings",
};

const show = (iso) => (iso ? iso.split("-").reverse().join("-") : "");

/**
 * Commit on the OPI/OPO tab: one block per file on the card. A file may hold
 * many trade dates, and one Commit adds all of its entries and updates the
 * holdings of their clients. A file is committed once; cash is never changed.
 * The server side is OpiOpoUpload.js.
 *
 * @param {{ key: string, uploadedAtMs: number, originalFileName: string }[]} files the files on the card
 */
const OpiOpoCommitPanel = ({ files, onProgressChange }) => {
    const [infos, setInfos] = useState({});
    const [busyId, setBusyId] = useState("");
    const [errors, setErrors] = useState({});
    const { reportJob, refresh: refreshNotifications } = useJobNotifications();

    const idsKey = files.map((f) => String(f.uploadedAtMs)).join(",");

    const load = useCallback(async () => {
        if (!idsKey) return;
        const results = await Promise.all(
            idsKey.split(",").map((id) =>
                api
                    .getOpiOpoFile(id)
                    .then((r) => ({ id, ...r }))
                    .catch((err) => ({ id, loadError: err.message })),
            ),
        );
        setInfos(Object.fromEntries(results.map((r) => [r.id, r])));
    }, [idsKey]);

    useEffect(() => {
        load();
    }, [load]);

    const anyRunning = Object.values(infos).some((i) => i.progress?.status === "RUNNING");
    useEffect(() => {
        if (!anyRunning) return undefined;
        const timer = setInterval(load, POLL_MS);
        return () => clearInterval(timer);
    }, [anyRunning, load]);

    const progressKey = Object.values(infos)
        .map((i) => `${i.id}:${i.progress?.status || ""}`)
        .join(",");
    useEffect(() => {
        if (progressKey && onProgressChange) onProgressChange();
    }, [progressKey, onProgressChange]);

    if (files.length === 0) return null;

    const setError = (id, error) => setErrors((prev) => ({ ...prev, [id]: error }));

    const handleCommit = async (id, file) => {
        const lines = [
            `Commit ${file.fileName}?`,
            "",
            `${file.rowCount.toLocaleString()} entries (${file.opiCount} OPI, ${file.opoCount} OPO) from ${show(file.firstDate)} to ${show(file.lastDate)} are added to our books.`,
            `The holdings of ${file.clients} client(s) are updated. Cash does not change.`,
            "",
            "A file is committed only once.",
        ];
        if (!window.confirm(lines.join("\n"))) return;

        setBusyId(id);
        setError(id, null);
        try {
            await api.commitOpiOpoFile(id);
            refreshNotifications();
            await load();
        } catch (err) {
            setError(id, { message: err.message, detail: err.detail });
            reportJob({
                id: `opi-commit-failed-${Date.now()}`,
                label: `OPI/OPO commit — ${file.fileName}`,
                status: "failed",
                message: "Commit could not start. See the Commit section for the reason.",
            });
        } finally {
            setBusyId("");
        }
    };

    const handleRetry = async (id) => {
        setBusyId(id);
        setError(id, null);
        try {
            await api.retryOpiOpoFile(id);
            await load();
        } catch (err) {
            setError(id, { message: err.message, detail: err.detail });
        } finally {
            setBusyId("");
        }
    };

    return (
        <>
            <div className="txo-section-head txo-section-head--tight">
                <h2>Commit</h2>
            </div>

            <div className="temp-transaction-card cm-gate cm-commit">
                {files.map((f) => {
                    const id = String(f.uploadedAtMs);
                    const info = infos[id];
                    const file = info?.file;
                    const p = info?.progress;
                    const committed = p?.status === "COMPLETED";

                    return (
                        <div key={id} className="cm-precommit">
                            <div className="cm-commit-head">
                                <strong>{f.originalFileName}</strong>
                                {!info ? (
                                    <StatusBadge tone={STATUS.NEUTRAL}>Loading…</StatusBadge>
                                ) : info.loadError ? (
                                    <StatusBadge tone={STATUS.BAD}>Could not read</StatusBadge>
                                ) : p?.status === "RUNNING" ? (
                                    <StatusBadge tone={STATUS.PROCESSING}>Committing</StatusBadge>
                                ) : p?.status === "FAILED" ? (
                                    <StatusBadge tone={STATUS.BAD}>Could not finish</StatusBadge>
                                ) : committed ? (
                                    <StatusBadge tone={STATUS.OK}>Committed</StatusBadge>
                                ) : (
                                    <StatusBadge tone={STATUS.OK}>Ready to commit</StatusBadge>
                                )}
                            </div>

                            {info?.loadError ? <p className="cm-blockers">{info.loadError}</p> : null}

                            {file ? (
                                <p className="cm-verdict-sub">
                                    {file.rowCount.toLocaleString()} entries ({file.opiCount} OPI, {file.opoCount} OPO) ·{" "}
                                    {file.dateCount} trade date(s), {show(file.firstDate)} to {show(file.lastDate)} ·{" "}
                                    {file.clients} client(s)
                                </p>
                            ) : null}

                            {p?.status === "RUNNING" ? (
                                <p className="cm-verdict-sub">
                                    {STEP_LABEL[p.step] || p.step}
                                    {p.step === "WAITING" && p.rebuild
                                        ? ` — ${p.rebuild.done} of ${p.rebuild.total} job(s) done for ${p.rebuild.clients} client(s).`
                                        : "…"}
                                </p>
                            ) : null}

                            {p?.status === "FAILED" ? (
                                <div className="cm-verdict bad">
                                    <p className="cm-verdict-lead">Commit could not finish. {p.error}</p>
                                    <button
                                        type="button"
                                        className="cm-secondary"
                                        disabled={busyId === id || !p.canRetry}
                                        onClick={() => handleRetry(id)}
                                    >
                                        Try again
                                    </button>
                                </div>
                            ) : null}

                            {committed ? (
                                <div className="cm-verdict ok">
                                    <p className="cm-verdict-lead">
                                        {Number(p.inserted || 0).toLocaleString()} entries –{" "}
                                        {p.rebuild?.clients || 0} client(s) updated.
                                    </p>
                                </div>
                            ) : null}

                            {file && !p ? (
                                <div className="cm-gate-row">
                                    <button
                                        type="button"
                                        className="cm-primary"
                                        disabled={busyId === id}
                                        onClick={() => handleCommit(id, file)}
                                    >
                                        {busyId === id ? "Starting…" : "Commit"}
                                    </button>
                                </div>
                            ) : null}

                            {errors[id] ? (
                                <UploadErrorDetails message={errors[id].message} detail={errors[id].detail} />
                            ) : null}
                        </div>
                    );
                })}
            </div>
        </>
    );
};

export default OpiOpoCommitPanel;
