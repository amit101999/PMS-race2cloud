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
 * Commit, one block per date. Equity: one per matched date. Mutual Funds
 * (kind MUTUAL_FUND): one per date with a mutual fund file on the card; there is
 * no match, so a date is ready once a new file is waiting.
 *
 * A date's first commit just adds its trades. A later one cleans up the whole date
 * (trades, holdings, cash, unused automatic master rows) and loads the final list
 * again, so our books equal the latest files. Each tab touches only its own trades.
 * Open match differences do not block it; the confirmation states them.
 *
 * @param {{ runId: string, businessDate: string, status: string, isRunning?: boolean,
 *           counts?: { exceptions: number } }[]} runs Equity's match runs, or one entry per
 *           Mutual Funds date (runId "mf-<date>", no exceptions)
 */
const PreCommitPanel = ({ runs, onProgressChange, kind = api.KIND.EQUITY }) => {
    const isMf = kind === api.KIND.MUTUAL_FUND;
    /* Mutual Funds has no match: a waiting file is all a Commit needs. */
    const noMatch = isMf;
    const tradeWord = isMf ? "mutual fund" : "equity";
    const rebuilt = "holdings and cash";
    const [dates, setDates] = useState({});
    const [busyDate, setBusyDate] = useState("");
    const [errors, setErrors] = useState({});
    const { reportJob, refresh: refreshNotifications } = useJobNotifications();

    const finished = runs.filter((r) => !r.isRunning && r.status !== "FAILED");
    const datesKey = finished.map((r) => r.businessDate).join(",");

    const load = useCallback(async () => {
        if (!datesKey) return;
        const results = await Promise.all(
            datesKey.split(",").map((d) => api.getEquityDate(d, kind).catch((err) => ({ date: d, loadError: err.message }))),
        );
        setDates(Object.fromEntries(results.map((r) => [r.date, r])));
    }, [datesKey, kind]);

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
        const open = run.counts?.exceptions || 0;
        const lines = [
            `Commit ${run.businessDate}?`,
            "",
            ...(s.bookedCount === 0
                ? [`${s.finalCount.toLocaleString()} trade(s) are added to our books, and the ${rebuilt} of the ${s.affectedClients} client(s) are updated.`]
                : [
                      `All ${s.bookedCount.toLocaleString()} ${tradeWord} trade(s) of this date are removed from our books, with the ${rebuilt} of the ${s.affectedClients} affected client(s).`,
                      `Then ${s.finalCount.toLocaleString()} trade(s) are loaded and ${rebuilt} are rebuilt.`,
                  ]),
        ];
        if (open > 0) lines.push("", `${open.toLocaleString()} match difference(s) are still open.`);
        const w = info.window;
        if (w && !w.fixed) {
            const show = (iso) => iso.split("-").reverse().join("-");
            lines.push(
                "",
                w.lastDay < api.todayIso()
                    ? `Window T+${w.days} ended on ${show(w.lastDay)}. This commit is final: the date is locked straight after it.`
                    : `Window T+${w.days} is fixed now: the date can be re-uploaded and re-committed till ${show(w.lastDay)}.`,
            );
        }
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
            await api.preCommitEquityDate(run.businessDate, { runId: noMatch ? "" : run.runId, confirmBigRemoval, kind });
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
            await api.retryEquityDate(date, kind);
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
                    // Mutual Funds and OPI/OPO: done once committed and no newer file is waiting.
                    const thisRunDone = noMatch
                        ? p?.status === "COMPLETED" && !summary
                        : p?.status === "COMPLETED" && p.runId === run.runId;
                    // No pending FA file: the match only checked our books, so nothing is loaded.
                    const booksOnly = !noMatch && summary?.runId === run.runId && summary.pendingKeys.length === 0;
                    const readyToCommit = noMatch ? Boolean(summary) : Boolean(summary && summary.runId === run.runId);
                    const exceptions = run.counts?.exceptions || 0;
                    // Past its change window: booked for good, so no Commit. A commit that is
                    // running or stopped still shows, so it can finish (Try again).
                    const dateLocked = Boolean(info?.window?.locked);
                    // Still marked running, but nothing has moved for a long time (the server
                    // process stopped, or its background work could not go on). The server
                    // then allows Try again (canRetry), so it is shown like a stopped commit.
                    const stalled = p?.status === "RUNNING" && Boolean(p.canRetry);
                    const stopped = p?.status === "FAILED" || stalled;

                    return (
                        <div key={run.runId} className="cm-precommit">
                            <div className="cm-commit-head">
                                <strong className="mono">{date}</strong>
                                {!info ? (
                                    <StatusBadge tone={STATUS.NEUTRAL}>Loading…</StatusBadge>
                                ) : dateLocked && p?.status !== "RUNNING" && p?.status !== "FAILED" ? (
                                    <StatusBadge tone={STATUS.NEUTRAL}>Locked</StatusBadge>
                                ) : stopped ? (
                                    <StatusBadge tone={STATUS.BAD}>Could not finish</StatusBadge>
                                ) : p?.status === "RUNNING" ? (
                                    <StatusBadge tone={STATUS.PROCESSING}>Committing</StatusBadge>
                                ) : p?.status === "FAILED" ? (
                                    <StatusBadge tone={STATUS.BAD}>Could not finish</StatusBadge>
                                ) : thisRunDone ? (
                                    <StatusBadge tone={STATUS.OK}>Committed</StatusBadge>
                                ) : booksOnly ? (
                                    <StatusBadge tone={exceptions > 0 ? STATUS.WARN : STATUS.OK}>
                                        Books checked
                                    </StatusBadge>
                                ) : summary ? (
                                    <StatusBadge tone={exceptions > 0 ? STATUS.WARN : STATUS.OK}>
                                        Ready to commit
                                    </StatusBadge>
                                ) : noMatch ? (
                                    <StatusBadge tone={STATUS.NEUTRAL}>No new file</StatusBadge>
                                ) : (
                                    <StatusBadge tone={STATUS.NEUTRAL}>Run the match again</StatusBadge>
                                )}
                            </div>

                            {info?.loadError ? <p className="cm-blockers">{info.loadError}</p> : null}

                            {p?.status === "RUNNING" && !stalled ? (
                                <p className="cm-verdict-sub">
                                    {(p.firstCommit ? FIRST_COMMIT_STEP_LABEL : STEP_LABEL)[p.step] || p.step}
                                    {p.step === "WAITING" && p.rebuild
                                        ? ` — ${p.rebuild.done} of ${p.rebuild.total} job(s) done for ${p.rebuild.clients} client(s).`
                                        : "…"}
                                </p>
                            ) : null}

                            {stopped ? (
                                <div className="cm-verdict bad">
                                    <p className="cm-verdict-lead">
                                        Commit for {date} could not finish.{" "}
                                        {stalled
                                            ? "It stopped making progress, so it can be continued from where it stopped."
                                            : p.error}
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

                            {dateLocked && (!p || p.status === "COMPLETED") ? (
                                <p className="cm-verdict-sub">{info.window.message}</p>
                            ) : booksOnly && (!p || p.status === "COMPLETED") ? (
                                <p className="cm-verdict-sub">
                                    No new FA file, so there is nothing to commit. Our{" "}
                                    {summary.bookedCount.toLocaleString()} booked trade(s) were checked against the custodian
                                    files.{" "}
                                    {exceptions > 0
                                        ? `${exceptions} match difference(s) still open.`
                                        : "No open match differences."}
                                </p>
                            ) : !p || p.status === "COMPLETED" ? (
                                readyToCommit ? (
                                    <>
                                        {/* Only the button: open differences turn it amber, and the
                                            confirmation states the trade counts before anything runs. */}
                                        <div className="cm-gate-row">
                                            <button
                                                type="button"
                                                className={exceptions > 0 ? "cm-primary cm-primary--warn" : "cm-primary"}
                                                disabled={busyDate === date}
                                                onClick={() => handlePreCommit(run, info)}
                                            >
                                                {busyDate === date ? "Starting…" : "Commit"}
                                            </button>
                                        </div>
                                    </>
                                ) : noMatch ? (
                                    !thisRunDone && info && !info.loadError ? (
                                        <p className="cm-verdict-sub">
                                            No new {tradeWord} file for {date}. Upload one to commit it again.
                                        </p>
                                    ) : null
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
