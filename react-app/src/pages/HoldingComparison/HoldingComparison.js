import React, { useCallback, useState } from "react";

import MainLayout from "../../layouts/MainLayout";
import { todayIso } from "../TempTransactionUpload/custody/api";
import ExceptionGrid from "./holdingComparison/ExceptionGrid";
import HoldingComparisonPanel from "./holdingComparison/HoldingComparisonPanel";
import PreviousFiles from "./holdingComparison/PreviousFiles";
import * as api from "./holdingComparison/api";
import "../TempTransactionUpload/tokens.css";
import "../TempTransactionUpload/TempTransaction.css";
import "../TempTransactionUpload/custody/CustodyMatch.css";

/**
 * Holding Comparison — do our holdings and the custodian's agree on a date?
 *
 * Diagnostic only: nothing here writes to production. The output is the
 * difference preview, the downloadable report and the three history lists.
 */
const HoldingComparison = () => {
    const [businessDate, setBusinessDate] = useState(todayIso());
    const [run, setRun] = useState(null);
    /* Bumped whenever a file is added, so the history lists re-read. */
    const [filesVersion, setFilesVersion] = useState(0);

    /*
     * Removed files, hidden from the custodian history list. Stratus keeps
     * listing a deleted object for about twenty seconds, so re-reading the list
     * alone would put the removed row straight back.
     */
    const [removedKeys, setRemovedKeys] = useState(() => new Set());

    const handleFilesChanged = useCallback(() => setFilesVersion((v) => v + 1), []);
    const handleFileRemoved = useCallback((keys) => {
        setRemovedKeys((prev) => new Set([...prev, ...keys]));
        setFilesVersion((v) => v + 1);
    }, []);

    // A finished run has just written its difference report.
    const reportToken = run && !run.isRunning ? `${run.runId}:${run.status}` : "";

    return (
        <MainLayout title="Holding Comparison">
            <div className="temp-transaction-container txn-ops">
                <HoldingComparisonPanel
                    businessDate={businessDate}
                    onBusinessDateChange={setBusinessDate}
                    run={run}
                    onRunChange={setRun}
                    onFilesChanged={handleFilesChanged}
                    onFileRemoved={handleFileRemoved}
                />

                <ExceptionGrid run={run} />

                <PreviousFiles
                    title="Previous Difference Holdings Reports"
                    load={api.getDifferenceReports}
                    emptyText="No difference reports yet."
                    refreshToken={reportToken}
                />
                <PreviousFiles
                    title="Previous Custodian Holdings Uploads"
                    load={api.getCustodyHistory}
                    emptyText="No custodian holdings files uploaded yet."
                    refreshToken={filesVersion}
                    hiddenKeys={removedKeys}
                />
                <PreviousFiles
                    title="Previous FA Holdings Reports"
                    load={api.getFaHistory}
                    emptyText="No FA holdings generated yet."
                    refreshToken={filesVersion}
                />
            </div>
        </MainLayout>
    );
};

export default HoldingComparison;
