import React, { useState } from "react";

import UploadErrorDetails from "./UploadErrorDetails";
import StatusBadge, { STATUS } from "./StatusBadge";
import * as api from "./api";
import { useJobNotifications } from "../../../notifications/JobNotificationContext";

/**
 * Gate 2 — the import.
 *
 * Commits every date on screen that is not committed yet, together, as ONE
 * import: the server joins their transaction files into one file, so holdings
 * and cash are updated once. Separate imports would overlap and count each
 * other's trades.
 *
 * Open exceptions no longer block this. The match reports what the two sides
 * disagree about; whether that should stop the day is the operator's decision.
 * The screen states the count and nothing more — the confirmation is where the
 * consequences are spelled out, because that is the point of no return.
 */
const GateTwo = ({ runs, onImported }) => {
    const [busy, setBusy] = useState(false);
    const [result, setResult] = useState(null);
    const [error, setError] = useState(null);
    const { trackJob, reportJob } = useJobNotifications();

    if (runs.length === 0 || runs.some((r) => r.isRunning)) return null;

    // Dates committed earlier stay listed but are never sent again.
    const toCommit = runs.filter((r) => !r.alreadyImported);
    const allCommitted = toCommit.length === 0;
    const dates = toCommit.map((r) => r.businessDate).join(", ");
    const totalRows = toCommit.reduce((sum, r) => sum + r.counts.faRows, 0);
    const notReconciled = toCommit.filter((r) => !r.canProceed);

    const handleImport = async () => {
        const lines = [
            `Commit ${totalRows.toLocaleString()} transaction row(s) for ${dates}?`,
            "",
        ];
        if (toCommit.length > 1) {
            lines.push("All these dates are committed together, as one import.", "");
        }
        for (const r of notReconciled) {
            const open = r.counts.exceptions || 0;
            lines.push(
                open > 0
                    ? `${r.businessDate} HAS NOT RECONCILED. ${open.toLocaleString()} exception(s) are still open and will be imported as they stand.`
                    : `${r.businessDate}: the match is ${r.status}, so the two sides were never fully compared.`,
            );
        }
        if (notReconciled.length > 0) lines.push("");
        lines.push(
            "This runs the existing import: the file is written to production, the " +
                "Transaction table is loaded, and the downstream FIFO, holdings and cash " +
                "jobs are queued.",
        );

        if (!window.confirm(lines.join("\n"))) return;

        setBusy(true);
        setError(null);
        setResult(null);
        try {
            const res = await api.proceedToImport(toCommit.map((r) => r.runId));
            setResult(res);
            // The import writes its end state to the Jobs row named pipelineJobName,
            // which is what the bell reconciles against.
            if (res.pipelineJobName) {
                trackJob({
                    id: res.pipelineJobName,
                    label: `Transaction import — ${dates}`,
                    message: `Importing ${totalRows.toLocaleString()} row(s). Holdings and cash balance update afterwards.`,
                });
            }
            if (onImported) onImported(res);
        } catch (err) {
            setError({ message: err.message, detail: err.detail });
            reportJob({
                id: `txn-import-failed-${Date.now()}`,
                label: `Transaction import — ${dates}`,
                status: "failed",
                message: err.message || "The import could not be started.",
            });
        } finally {
            setBusy(false);
        }
    };

    const committed = allCommitted || Boolean(result);
    const ready = notReconciled.length === 0;

    return (
        <>
            <div className="txo-section-head txo-section-head--tight">
                <h2>Commit</h2>
            </div>

            <div className="temp-transaction-card cm-gate cm-commit">
                <div className="cm-commit-head">
                    <StatusBadge tone={committed || ready ? STATUS.OK : STATUS.WARN}>
                        {committed ? "Committed" : ready ? "Ready to commit" : "Needs attention"}
                    </StatusBadge>
                    <div className="cm-commit-summary">
                        {runs.map((r) => {
                            const open = r.counts.exceptions || 0;
                            return (
                                <p key={r.runId}>
                                    <strong className="mono">{r.businessDate}</strong>
                                    {" · "}
                                    {r.counts.faRows.toLocaleString()} transaction row(s)
                                    {r.alreadyImported
                                        ? " · already committed"
                                        : open > 0
                                            ? ` · ${open.toLocaleString()} exception(s) still open`
                                            : r.canProceed
                                                ? " · matched with no open exceptions"
                                                : ` · match ${r.status}, never fully compared`}
                                    .
                                </p>
                            );
                        })}
                    </div>
                </div>

                <div className="cm-gate-row">
                    <button
                        type="button"
                        className={ready ? "cm-primary" : "cm-primary cm-primary--warn"}
                        onClick={handleImport}
                        disabled={busy || committed}
                    >
                        {busy ? "Committing…" : "Commit Transaction Files"}
                    </button>

                    {allCommitted && !result ? (
                        <p className="cm-unreconciled">
                            <strong>Already imported.</strong> Every date shown has been handed to
                            the import pipeline — committing again is blocked to avoid a duplicate
                            load.
                        </p>
                    ) : null}
                </div>

                {result ? (
                    <div className="cm-verdict ok">
                        <p className="cm-verdict-lead">
                            {result.message || "Import started."}
                        </p>
                        {result.bulkJobId ? (
                            <p className="cm-verdict-sub">
                                Bulk write job <code>{result.bulkJobId}</code> is running. The
                                downstream jobs are queued when it finishes.
                            </p>
                        ) : null}
                        {result.warning ? <p className="cm-verdict-sub">{result.warning}</p> : null}
                    </div>
                ) : null}

                {error ? <UploadErrorDetails message={error.message} detail={error.detail} /> : null}
            </div>
        </>
    );
};

export default GateTwo;
