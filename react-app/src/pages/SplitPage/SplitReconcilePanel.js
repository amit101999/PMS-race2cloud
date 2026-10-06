import React, { useEffect, useState } from "react";

import { BASE_URL } from "../../constant";
import "../CustodianUploadPage/CustodianComparePanel.css";

/**
 * Comparison result → badge colour and the label shown on screen. The CSV keeps the raw status.
 * "Matched" is only the comparison's verdict; an account becomes Received (its
 * saved status) on Confirm & Apply.
 */
const STATUS_VIEW = {
    MATCHED: { badge: "badge--ok", label: "Matched" },
    QTY_MISMATCH: { badge: "badge--bad", label: "Qty Mismatch" },
    AMOUNT_MISMATCH: { badge: "badge--bad", label: "Amount Mismatch" },
    MISSING_IN_CUSTODY: { badge: "badge--warn", label: "Missing in Custodian" },
    NOT_IN_BENEFIT_FILE: { badge: "badge--warn", label: "Not in Benefit File" },
    MISSING_IN_FA: { badge: "badge--warn", label: "Missing in FA" },
};
const MATCHED_STATUSES = ["MATCHED"];

const numberFormat = new Intl.NumberFormat(undefined, { maximumFractionDigits: 4 });
const dateTimeFormat = new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" });

const qty = (v) => (v === null || v === undefined ? "—" : numberFormat.format(v));

/**
 * "FA / Custodian / Diff" in one cell, like the Dividend grid. `custodian` is the
 * custodian's figure (its holding / CA qty, or what it says was received). The
 * difference is custodian − FA with its sign; a side the client is missing from
 * shows "—" and counts as zero.
 */
const CompareCell = ({ fa, custodian }) => {
    const diff = Math.round(((custodian ?? 0) - (fa ?? 0)) * 10000) / 10000;
    const diffClass = diff === 0 ? "muted" : diff > 0 ? "positive" : "negative";
    return (
        <td className="num">
            {qty(fa)} / {qty(custodian)} /{" "}
            <span className={diffClass}>
                {diff > 0 ? "+" : ""}
                {numberFormat.format(diff)}
            </span>
        </td>
    );
};

const StatusBadge = ({ status }) => {
    const view = STATUS_VIEW[status] || { badge: "badge--neutral", label: status };
    return <span className={`badge ${view.badge}`}>{view.label}</span>;
};

/**
 * Split and Bonus pages: the two-stage custodian reconciliation of the
 * previewed split / bonus.
 *
 *   NEW        → compare with the Ex-Entitlement file, then "Send as Receivable"
 *   RECEIVABLE → compare with the Corporate Benefit Collection file, then "Confirm & Apply"
 *   RECEIVED   → nothing left to do
 *
 * `stage` comes from the preview, so re-fetching the preview moves the panel on.
 *
 * The Dividend page uses only the RECEIVABLE / RECEIVED stages (its stage 1 is
 * its own reconciliation grid). A dividend is paid in cash, so its stage 2
 * compares amounts instead of quantities.
 *
 * @param {"split"|"bonus"|"dividend"} caType  which page; its API is /api/<caType>/...
 */
const SplitReconcilePanel = ({
    caType = "split",
    stage,
    isin,
    securityCode,
    securityName,
    ratio1,
    ratio2,
    recordDate,
    effectiveDate,
    confirming,
    onConfirm,
    onReceivableSent,
    onCompared,
}) => {
    const isBenefitStage = stage === "RECEIVABLE";
    const isDividend = caType === "dividend";
    const fileLabel = isBenefitStage ? "Corporate Benefit Collection file" : "Ex-Entitlement file";

    const [fileInfo, setFileInfo] = useState(null); // { file, rowCount }
    const [fileError, setFileError] = useState("");
    const [comparing, setComparing] = useState(false);
    const [sending, setSending] = useState(false);
    const [result, setResult] = useState(null);
    const [compareError, setCompareError] = useState("");

    // Which uploaded custodian file holds this event (ISIN + record date) for this stage.
    useEffect(() => {
        setFileInfo(null);
        setFileError("");
        if (!isin || !recordDate || stage === "RECEIVED") return undefined;

        let cancelled = false;
        const params = new URLSearchParams(
            isBenefitStage
                ? { kind: "allocation", caType, isin, recordDate }
                : { kind: "entitlement", caType, isin, recordDate },
        );
        fetch(`${BASE_URL}/custodian-upload/latest?${params.toString()}`)
            .then(async (res) => {
                const body = await res.json().catch(() => ({}));
                if (cancelled) return;
                if (!res.ok || !body.success) setFileError(body.message || `Could not read the ${fileLabel}.`);
                else setFileInfo(body);
            })
            .catch(() => !cancelled && setFileError("Could not reach the server. Check the backend is running."));
        return () => {
            cancelled = true;
        };
    }, [caType, isin, recordDate, stage, isBenefitStage, fileLabel]);

    // A result only describes the inputs and stage it was run with.
    useEffect(() => {
        setResult(null);
        setCompareError("");
    }, [isin, ratio1, ratio2, recordDate, stage]);

    const busy = comparing || sending || confirming;
    const canCompare = Boolean(isin && recordDate && fileInfo?.rowCount) && !busy;

    const postJson = async (path, body) => {
        const res = await fetch(`${BASE_URL}/${caType}/${path}`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok || !data.success) throw new Error(data.message || "The request failed. Try again.");
        return data;
    };

    const runCompare = async () => {
        setComparing(true);
        setResult(null);
        setCompareError("");
        try {
            const body = await postJson(
                isBenefitStage ? "compare-benefit" : "compare-custodian",
                isDividend
                    ? { isin, securityName, recordDate }
                    : { isin, securityName, ratio1: Number(ratio1), ratio2: Number(ratio2), recordDate },
            );
            setResult(body);
            onCompared();
        } catch (err) {
            setCompareError(err.message === "Failed to fetch" ? "Could not reach the server. Check the backend is running." : err.message);
        } finally {
            setComparing(false);
        }
    };

    const sendReceivable = async () => {
        setSending(true);
        setCompareError("");
        try {
            const body = await postJson("send-receivable", {
                isin,
                securityCode,
                securityName,
                ratio1: Number(ratio1),
                ratio2: Number(ratio2),
                recordDate,
                effectiveDate,
            });
            window.alert(`${body.message} The Status column now shows Receivable for these accounts.`);
            onReceivableSent();
        } catch (err) {
            setCompareError(err.message);
        } finally {
            setSending(false);
        }
    };

    if (!isin) return null;

    if (stage === "RECEIVED") {
        return (
            <div className="temp-transaction-card cc-panel">
                <h3 className="ops-card-title">Custodian Reconciliation</h3>
                <div className="cm-verdict ok">
                    This {caType} has been received and applied. The Status and Received Date columns above show each
                    account.
                </div>
            </div>
        );
    }

    const counts = result?.counts;
    let summary = [];
    if (counts && isBenefitStage) {
        summary = [
            { key: "total", label: "Total", value: counts.total, badge: "badge--neutral" },
            { key: "matched", label: "Matched", value: counts.matched, badge: "badge--ok" },
            isDividend
                ? { key: "amount", label: "Amount Mismatch", value: counts.amountMismatch, badge: "badge--bad" }
                : { key: "qty", label: "Qty Mismatch", value: counts.qtyMismatch, badge: "badge--bad" },
            { key: "notInFile", label: "Not in Benefit File", value: counts.notInBenefitFile, badge: "badge--warn" },
            { key: "fa", label: "Missing in FA", value: counts.missingInFa, badge: "badge--warn" },
        ];
    } else if (counts) {
        summary = [
            { key: "total", label: "Total", value: counts.total, badge: "badge--neutral" },
            { key: "matched", label: "Matched", value: counts.matched, badge: "badge--ok" },
            { key: "qty", label: "Qty Mismatch", value: counts.qtyMismatch, badge: "badge--bad" },
            { key: "custody", label: "Missing in Custodian", value: counts.missingInCustody, badge: "badge--warn" },
            { key: "fa", label: "Missing in FA", value: counts.missingInFa, badge: "badge--warn" },
        ];
    }
    const compareLabel = isBenefitStage
        ? "Compare with Corporate Benefit Collection"
        : "Compare with Custodian Ex-Entitlement";

    return (
        <div className="temp-transaction-card cc-panel">
            <h3 className="ops-card-title">
                {isBenefitStage ? "Stage 2" : "Stage 1"} · {compareLabel}
            </h3>

            <div aria-live="polite">
                {fileError ? <div className="cm-verdict bad">{fileError}</div> : null}
                {fileInfo ? (
                    <div className={fileInfo.rowCount ? "cm-verdict ok" : "cm-verdict warn"}>
                        {fileLabel} fetched: <strong className="cc-file-name">{fileInfo.file.fileName}</strong> ·
                        report date {fileInfo.file.businessDate} · uploaded{" "}
                        {dateTimeFormat.format(new Date(fileInfo.file.timeMs))} ·{" "}
                        {numberFormat.format(fileInfo.rowCount)} row(s) for <span translate="no">{isin}</span> with
                        Record Date {recordDate}
                    </div>
                ) : null}
            </div>

            <div className="cc-actions">
                <button type="button" className="cm-primary" disabled={!canCompare} onClick={runCompare}>
                    {comparing ? "Comparing…" : compareLabel}
                </button>
            </div>

            <div aria-live="polite">
                {compareError ? <div className="cm-verdict bad cc-message">{compareError}</div> : null}

                {result ? (
                    <>
                        {result.warnings.map((w) => (
                            <div key={w} className="cm-verdict warn cc-message">
                                {w}
                            </div>
                        ))}

                        <div className="cc-summary">
                            <ul className="cc-chips" aria-label="Comparison summary">
                                {summary.map((s) => (
                                    <li key={s.key} className={`badge ${s.badge} cc-chip`}>
                                        {s.label}: <strong>{numberFormat.format(s.value)}</strong>
                                    </li>
                                ))}
                            </ul>
                            <button
                                type="button"
                                className="btn-export"
                                disabled={!result.downloadUrl}
                                onClick={() => window.open(result.downloadUrl, "_blank", "noopener")}
                            >
                                Export
                            </button>
                        </div>

                        {result.rows.length === 0 ? (
                            <p className="cm-empty">No clients to compare for this security and record date.</p>
                        ) : (
                            <div className="data-table-wrap">
                                {/* Columns spread evenly, like the history tables. */}
                                <table className="data-table data-table--spread cc-table">
                                    <thead>
                                        {isBenefitStage && isDividend ? (
                                            <tr>
                                                <th scope="col">Account Code</th>
                                                <th scope="col">CA Ref No</th>
                                                <th scope="col" className="num">Amount</th>
                                                <th scope="col" className="num">CA Tax</th>
                                                <th scope="col" className="num">Received Amount</th>
                                                <th scope="col">Received Date</th>
                                                <th scope="col">Status</th>
                                            </tr>
                                        ) : isBenefitStage ? (
                                            <tr>
                                                <th scope="col">Account Code</th>
                                                <th scope="col">CA Ref No</th>
                                                <th scope="col" className="num">
                                                    CA Qty
                                                    <br />
                                                    FA / Received / Diff
                                                </th>
                                                <th scope="col" className="num">Balance Qty</th>
                                                <th scope="col">Received Date</th>
                                                <th scope="col">Status</th>
                                            </tr>
                                        ) : (
                                            <tr>
                                                <th scope="col">Account Code</th>
                                                <th scope="col">Record Date</th>
                                                <th scope="col" className="num">
                                                    Holding
                                                    <br />
                                                    FA / Custodian / Diff
                                                </th>
                                                <th scope="col" className="num">
                                                    CA Qty
                                                    <br />
                                                    FA / Custodian / Diff
                                                </th>
                                                <th scope="col">Status</th>
                                            </tr>
                                        )}
                                    </thead>
                                    <tbody>
                                        {result.rows.map((r) => {
                                            const isBreak = !MATCHED_STATUSES.includes(r.status);
                                            if (isBenefitStage && isDividend) {
                                                return (
                                                    <tr key={r.accountCode} className={isBreak ? "cc-row-break" : undefined}>
                                                        <td translate="no">{r.accountCode}</td>
                                                        <td title={r.caRefNo}>{r.caRefNo || "—"}</td>
                                                        <td className="num">{qty(r.faAmount)}</td>
                                                        <td className="num">{qty(r.caTaxAmt)}</td>
                                                        <td className="num">{qty(r.receivedAmount)}</td>
                                                        <td>{r.receivedDate || "—"}</td>
                                                        <td>
                                                            <StatusBadge status={r.status} />
                                                        </td>
                                                    </tr>
                                                );
                                            }
                                            return isBenefitStage ? (
                                                <tr key={r.accountCode} className={isBreak ? "cc-row-break" : undefined}>
                                                    <td translate="no">{r.accountCode}</td>
                                                    <td title={r.caRefNo}>{r.caRefNo || "—"}</td>
                                                    <CompareCell fa={r.faCaQty} custodian={r.receivedQty} />
                                                    <td className="num">{qty(r.balanceQty)}</td>
                                                    <td>{r.receivedDate || "—"}</td>
                                                    <td>
                                                        <StatusBadge status={r.status} />
                                                    </td>
                                                </tr>
                                            ) : (
                                                <tr key={r.accountCode} className={isBreak ? "cc-row-break" : undefined}>
                                                    <td translate="no">{r.accountCode}</td>
                                                    <td>{r.recordDate}</td>
                                                    <CompareCell fa={r.faHolding} custodian={r.custodianHolding} />
                                                    <CompareCell fa={r.faCaQty} custodian={r.custodianCaQty} />
                                                    <td>
                                                        <StatusBadge status={r.status} />
                                                    </td>
                                                </tr>
                                            );
                                        })}
                                    </tbody>
                                </table>
                            </div>
                        )}
                        {counts.total > result.rows.length ? (
                            <p className="cm-sub">
                                Showing {result.rows.length} of {numberFormat.format(counts.total)} clients, differences
                                first. Export for the full file.
                            </p>
                        ) : null}

                        <div className="cc-actions cc-actions--below">
                            {isBenefitStage ? (
                                <button type="button" className="cm-primary" disabled={busy} onClick={onConfirm}>
                                    {confirming ? "Applying…" : "Confirm & Apply"}
                                </button>
                            ) : (
                                <button type="button" className="cm-primary" disabled={busy} onClick={sendReceivable}>
                                    {sending ? "Sending…" : "Send as Receivable"}
                                </button>
                            )}
                        </div>
                    </>
                ) : null}
            </div>
        </div>
    );
};

export default SplitReconcilePanel;
