import React, { useState } from "react";

import UploadErrorDetails from "./UploadErrorDetails";
import * as api from "./api";

/**
 * Gate 2 — the import.
 *
 * Open exceptions no longer block this. The match reports what the two sides
 * disagree about; whether that should stop the day is the operator's decision.
 * The screen states the count and nothing more — the confirmation is where the
 * consequences are spelled out, because that is the point of no return.
 */
const GateTwo = ({ run, onImported }) => {
    const [busy, setBusy] = useState(false);
    const [result, setResult] = useState(null);
    const [error, setError] = useState(null);

    if (!run || run.isRunning) return null;

    const reconciled = run.canProceed;
    const openExceptions = run.counts.exceptions || 0;
    const alreadyImported = run.alreadyImported;

    const handleImport = async () => {
        const lines = [
            `Import ${run.counts.faRows.toLocaleString()} transaction row(s) for ${run.businessDate}?`,
            "",
        ];
        if (!reconciled) {
            lines.push(
                openExceptions > 0
                    ? `THIS DAY HAS NOT RECONCILED. ${openExceptions.toLocaleString()} exception(s) are still open and will be imported as they stand.`
                    : `This run is ${run.status}, so the two sides were never fully compared.`,
                "",
            );
        }
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
            const res = await api.proceedToImport(run.runId);
            setResult(res);
            if (onImported) onImported(res);
        } catch (err) {
            setError({ message: err.message, detail: err.detail });
        } finally {
            setBusy(false);
        }
    };

    return (
        <div className="temp-transaction-card cm-gate">
            <h3>Upload Transaction File</h3>
            <p className="cm-sub">
                {openExceptions.toLocaleString()} record(s) not matching.
            </p>

            <div className="cm-gate-row">
                <button
                    type="button"
                    className={reconciled ? "cm-primary" : "cm-primary cm-primary--warn"}
                    onClick={handleImport}
                    disabled={busy || alreadyImported || Boolean(result)}
                >
                    {busy ? "Importing…" : "Commit the Transaction Files"}
                </button>

                {/* A run that never finished comparing is a different thing from one
                    that finished and found differences, and the count alone cannot
                    say which it was. */}
                {!reconciled && openExceptions === 0 && !alreadyImported && !result ? (
                    <p className="cm-unreconciled">
                        This run is {run.status}, so the two sides were never fully compared.
                    </p>
                ) : null}

                {alreadyImported && !result ? (
                    <p className="cm-unreconciled">
                        <strong>Already imported.</strong> This run's file has been handed to
                        the import pipeline.
                    </p>
                ) : null}
            </div>

            {result ? (
                <div className="cm-verdict ok">
                    <p style={{ margin: 0 }}>
                        {result.message || "Import started."}
                    </p>
                    {result.bulkJobId ? (
                        <p style={{ margin: "6px 0 0", fontSize: "12.5px" }}>
                            Bulk write job <code>{result.bulkJobId}</code> is running. The
                            downstream jobs are queued when it finishes.
                        </p>
                    ) : null}
                </div>
            ) : null}

            {error ? <UploadErrorDetails message={error.message} detail={error.detail} /> : null}
        </div>
    );
};

export default GateTwo;
