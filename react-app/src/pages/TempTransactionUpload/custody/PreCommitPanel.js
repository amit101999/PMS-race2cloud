import React, { useCallback, useEffect, useState } from "react";

import StatusBadge, { STATUS } from "./StatusBadge";
import UploadErrorDetails from "./UploadErrorDetails";
import * as api from "./api";
import { useJobNotifications } from "../../../notifications/JobNotificationContext";

const POLL_MS = 5000;

const STEP_LABEL = {
    TRANSACTION: "Removing the date's trades",
    HOLDINGS: "Removing the holdings of the affected clients",
    CASH: "Removing the cash passbook of the affected clients",
    MASTERS: "Removing unused automatic master rows",
    INSERT: "Loading the trades",
    REBUILD: "Queuing the holdings and cash rebuild",
    WAITING: "Rebuilding holdings and cash",
};

/* A first commit removes nothing: it adds the trades and builds holdings and cash for them. */
const FIRST_COMMIT_STEP_LABEL = {
    INSERT: "Adding the trades",
    REBUILD: "Queuing the holdings and cash update",
    WAITING: "Updating holdings and cash",
};

/**
 * Equity tab — Commit, one block per matched date.
 *
 * A date's first commit just adds its trades. A later one cleans up the whole date
 * (trades, holdings, cash, unused automatic master rows) and loads the final list
 * again, so our books equal the latest files.
 * Open match differences do not block it; the confirmation states them.
 */
const PreCommitPanel = ({ runs, onProgressChange }) => {
    const [dates, setDates] = useState({});
    const [busyDate, setBusyDate] = useState("");
    const [errors, setErrors] = useState({});
    const { reportJob, refresh: refreshNotifications } = useJobNotifications();

    const finished = runs.filter((r) => !r.isRunning && r.status !== "FAILED");
    const datesKey = finished.map((r) => r.businessDate).join(",");

    const load = useCallback(async () => {
        if (!datesKey) return;
        const results = await Promise.all(
            datesKey.split(",").map((d) => api.getEquityDate(d).catch((err) => ({ date: d, loadError: err.message }))),
        );
        setDates(Object.fromEntries(results.map((r) => [r.date, r])));
    }, [datesKey]);

    useEffect(() => {
        load();
    }, [load]);

    const anyRunning = Object.values(dates).some((d) => d.progress?.status === "RUNNING");
    useEffect(() => {
        if (!anyRunning) return undefined;
        const timer = setInterval(load, POLL_MS);
        return () => clearInterval(timer);
    }, [anyRunning, load]);

    const progressKey = Object.values(dates)
        .map((d) => `${d.date}:${d.progress?.status || ""}`)
        .join(",");
    useEffect(() => {
        if (progressKey && onProgressChange) onProgressChange();
    }, [progressKey, onProgressChange]);

    if (finished.length === 0) return null;

    const setError = (date, error) => setErrors((prev) => ({ ...prev, [date]: error }));

    const handlePreCommit = async (run, info) => {
        const s = info.summary;
        const open = run.counts.exceptions || 0;
        const lines = [
            `Commit ${run.businessDate}?`,
            "",
            ...(s.bookedCount === 0
                ? [`${s.finalCount.toLocaleString()} trade(s) are added to our books, and the holdings and cash of the ${s.affectedClients} client(s) are updated.`]
                : [
                      `All ${s.bookedCount.toLocaleString()} equity trade(s) of this date are removed from our books, with the holdings and cash of the ${s.affectedClients} affected client(s).`,
                      `Then ${s.finalCount.toLocaleString()} trade(s) are loaded and holdings and cash are rebuilt.`,
                  ]),
        ];
        if (open > 0) lines.push("", `${open.toLocaleString()} match difference(s) are still open.`);
        if (!window.confirm(lines.join("\n"))) return;

        let confirmBigRemoval = false;
        if (s.bigRemoval) {
            const share = Math.round((s.counts.removed / Math.max(s.bookedCount, 1)) * 100);
            confirmBigRemoval = window.confirm(
                `${s.counts.removed} of ${s.bookedCount} trades (${share}%) will be removed. Are you sure?`,
            );
            if (!confirmBigRemoval) return;
        }

        setBusyDate(run.businessDate);
        setError(run.businessDate, null);
        try {
            await api.preCommitEquityDate(run.businessDate, { runId: run.runId, confirmBigRemoval });
            refreshNotifications();
            await load();
        } catch (err) {
            setError(run.businessDate, { message: err.message, detail: err.detail });
            reportJob({
                id: `txn-precommit-failed-${Date.now()}`,
                label: `Commit — ${run.businessDate.split("-").reverse().join("-")}`,
                status: "failed",
                message: "Commit could not start. See the Commit section for the reason.",
            });
        } finally {
            setBusyDate("");
        }
    };

    const handleRetry = async (date) => {
        setBusyDate(date);
        setError(date, null);
        try {
            await api.retryEquityDate(date);
            await load();
        } catch (err) {
            setError(date, { message: err.message, detail: err.detail });
        } finally {
            setBusyDate("");
        }
    };

    return (
        <>
            <div className="txo-section-head txo-section-head--tight">
                <h2>Commit</h2>
            </div>

            <div className="temp-transaction-card cm-gate cm-commit">
                {finished.map((run) => {
                    const info = dates[run.businessDate];
                    const date = run.businessDate;
                    const p = info?.progress;
                    const summary = info?.summary;
                    const thisRunDone = p?.status === "COMPLETED" && p.runId === run.runId;
                    // No pending FA file: the match only checked our books, so nothing is loaded.
                    const booksOnly = summary?.runId === run.runId && summary.pendingKeys.length === 0;

                    return (
                        <div key={run.runId} className="cm-precommit">
                            <div className="cm-commit-head">
                                <strong className="mono">{date}</strong>
                                {!info ? (
                                    <StatusBadge tone={STATUS.NEUTRAL}>Loading…</StatusBadge>
                                ) : p?.status === "RUNNING" ? (
                                    <StatusBadge tone={STATUS.PROCESSING}>Committing</StatusBadge>
                                ) : p?.status === "FAILED" ? (
                                    <StatusBadge tone={STATUS.BAD}>Could not finish</StatusBadge>
                                ) : thisRunDone ? (
                                    <StatusBadge tone={STATUS.OK}>Committed</StatusBadge>
                                ) : booksOnly ? (
                                    <StatusBadge tone={run.counts.exceptions > 0 ? STATUS.WARN : STATUS.OK}>
                                        Books checked
                                    </StatusBadge>
                                ) : summary ? (
                                    <StatusBadge tone={run.counts.exceptions > 0 ? STATUS.WARN : STATUS.OK}>
                                        Ready to commit
                                    </StatusBadge>
                                ) : (
                                    <StatusBadge tone={STATUS.NEUTRAL}>Run the match again</StatusBadge>
                                )}
                            </div>

                            {info?.loadError ? <p className="cm-blockers">{info.loadError}</p> : null}

                            {p?.status === "RUNNING" ? (
                                <p className="cm-verdict-sub">
                                    {(p.firstCommit ? FIRST_COMMIT_STEP_LABEL : STEP_LABEL)[p.step] || p.step}
                                    {p.step === "WAITING" && p.rebuild
                                        ? ` — ${p.rebuild.done} of ${p.rebuild.total} job(s) done for ${p.rebuild.clients} client(s).`
                                        : "…"}
                                </p>
                            ) : null}

                            {p?.status === "FAILED" ? (
                                <div className="cm-verdict bad">
                                    <p className="cm-verdict-lead">
                                        Commit for {date} could not finish. {p.error}
                                    </p>
                                    <button
                                        type="button"
                                        className="cm-secondary"
                                        disabled={busyDate === date || !p.canRetry}
                                        onClick={() => handleRetry(date)}
                                    >
                                        Try again
                                    </button>
                                </div>
                            ) : null}

                            {thisRunDone ? (
                                <div className="cm-verdict ok">
                                    <p className="cm-verdict-lead">
                                        {date} – {Number(p.inserted || 0).toLocaleString()} trades –{" "}
                                        {p.rebuild?.clients || 0} client(s) rebuilt.
                                    </p>
                                </div>
                            ) : null}

                            {booksOnly && (!p || p.status === "COMPLETED") ? (
                                <p className="cm-verdict-sub">
                                    No new FA file, so there is nothing to commit. Our{" "}
                                    {summary.bookedCount.toLocaleString()} booked trade(s) were checked against the custodian
                                    files.{" "}
                                    {run.counts.exceptions > 0
                                        ? `${run.counts.exceptions} match difference(s) still open.`
                                        : "No open match differences."}
                                </p>
                            ) : !p || p.status === "COMPLETED" ? (
                                summary && summary.runId === run.runId ? (
                                    <>
                                        {/* Only the button: open differences turn it amber, and the
                                            confirmation states the trade counts before anything runs. */}
                                        <div className="cm-gate-row">
                                            <button
                                                type="button"
                                                className={run.counts.exceptions > 0 ? "cm-primary cm-primary--warn" : "cm-primary"}
                                                disabled={busyDate === date}
                                                onClick={() => handlePreCommit(run, info)}
                                            >
                                                {busyDate === date ? "Starting…" : "Commit"}
                                            </button>
                                        </div>
                                    </>
                                ) : !thisRunDone && info && !info.loadError ? (
                                    <p className="cm-blockers">
                                        The files of {date} changed after this match. Run the match again.
                                    </p>
                                ) : null
                            ) : null}

                            {errors[date] ? (
                                <UploadErrorDetails message={errors[date].message} detail={errors[date].detail} />
                            ) : null}
                        </div>
                    );
                })}
            </div>
        </>
    );
};

export default PreCommitPanel;
