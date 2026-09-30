import React, { useEffect, useState } from "react";

import { BASE_URL } from "../../constant";
import "../CustodianUploadPage/CustodianComparePanel.css";

/** Status → badge colour and the label shown on screen. The CSV keeps the raw status. */
const STATUS_VIEW = {
    MATCHED: { badge: "badge--ok", label: "Matched" },
    RECEIVED: { badge: "badge--ok", label: "Received" },
    QTY_MISMATCH: { badge: "badge--bad", label: "Qty Mismatch" },
    MISSING_IN_CUSTODY: { badge: "badge--warn", label: "Missing in Custodian" },
    NOT_IN_BENEFIT_FILE: { badge: "badge--warn", label: "Not in Benefit File" },
    MISSING_IN_FA: { badge: "badge--warn", label: "Missing in FA" },
};
const MATCHED_STATUSES = ["MATCHED", "RECEIVED"];

const numberFormat = new Intl.NumberFormat(undefined, { maximumFractionDigits: 4 });
const dateTimeFormat = new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" });

const qty = (v) => (v === null || v === undefined ? "—" : numberFormat.format(v));

const StatusBadge = ({ status }) => {
    const view = STATUS_VIEW[status] || { badge: "badge--neutral", label: status };
    return <span className={`badge ${view.badge}`}>{view.label}</span>;
};

/**
 * Split page: the two-stage custodian reconciliation of the previewed split.
 *
 *   NEW        → compare with the Ex-Entitlement file, then "Send as Receivable"
 *   RECEIVABLE → compare with the Corporate Benefit Collection file, then "Confirm & Apply"
 *   RECEIVED   → nothing left to do
 *
 * `stage` comes from the preview, so re-fetching the preview moves the panel on.
 */
const SplitReconcilePanel = ({
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
    const fileLabel = isBenefitStage ? "Corporate Benefit Collection file" : "Ex-Entitlement file";

    const [fileInfo, setFileInfo] = useState(null); // { file, rowCount }
    const [fileError, setFileError] = useState("");
    const [comparing, setComparing] = useState(false);
    const [sending, setSending] = useState(false);
    const [result, setResult] = useState(null);
    const [compareError, setCompareError] = useState("");

    // Which custodian file this stage will use.
    useEffect(() => {
        setFileInfo(null);
        setFileError("");
        if (!isin || stage === "RECEIVED") return undefined;

        let cancelled = false;
        const params = new URLSearchParams(isBenefitStage ? { kind: "allocation", isin } : { caType: "split", isin });
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
    }, [isin, stage, isBenefitStage, fileLabel]);

    // A result only describes the inputs and stage it was run with.
    useEffect(() => {
        setResult(null);
        setCompareError("");
    }, [isin, ratio1, ratio2, recordDate, stage]);

    const busy = comparing || sending || confirming;
    const canCompare = Boolean(isin && recordDate && fileInfo?.rowCount) && !busy;

    const postJson = async (path, body) => {
        const res = await fetch(`${BASE_URL}/split/${path}`, {
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
            const body = await postJson(isBenefitStage ? "compare-benefit" : "compare-custodian", {
                isin,
                securityName,
                ratio1: Number(ratio1),
                ratio2: Number(ratio2),
                recordDate,
            });
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
                    This split has been received and applied. The Status and Received Date columns above show each account.
                </div>
            </div>
        );
    }

    const counts = result?.counts;
    let summary = [];
    if (counts && isBenefitStage) {
        summary = [
            { key: "total", label: "Total", value: counts.total, badge: "badge--neutral" },
            { key: "received", label: "Received", value: counts.received, badge: "badge--ok" },
            { key: "qty", label: "Qty Mismatch", value: counts.qtyMismatch, badge: "badge--bad" },
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
                        uploaded {dateTimeFormat.format(new Date(fileInfo.file.timeMs))} ·{" "}
                        {numberFormat.format(fileInfo.rowCount)} row(s) for <span translate="no">{isin}</span>
                        {fileInfo.rowCount ? "" : ". Upload a file that includes this security to compare."}
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
                                <table className="data-table cc-table">
                                    <thead>
                                        {isBenefitStage ? (
                                            <tr>
                                                <th scope="col">Account Code</th>
                                                <th scope="col">CA Ref No</th>
                                                <th scope="col" className="num">FA CA Qty</th>
                                                <th scope="col" className="num">Custodian CA Qty</th>
                                                <th scope="col" className="num">Received Qty</th>
                                                <th scope="col" className="num">Balance Qty</th>
                                                <th scope="col">Received Date</th>
                                                <th scope="col" className="num">Difference</th>
                                                <th scope="col">Status</th>
                                            </tr>
                                        ) : (
                                            <tr>
                                                <th scope="col">Account Code</th>
                                                <th scope="col">Record Date</th>
                                                <th scope="col" className="num">FA Holding</th>
                                                <th scope="col" className="num">Custodian Holding</th>
                                                <th scope="col" className="num">FA CA Qty</th>
                                                <th scope="col" className="num">Custodian CA Qty</th>
                                                <th scope="col" className="num">Difference</th>
                                                <th scope="col">Status</th>
                                            </tr>
                                        )}
                                    </thead>
                                    <tbody>
                                        {result.rows.map((r) => {
                                            const isBreak = !MATCHED_STATUSES.includes(r.status);
                                            const diff = Math.max(r.difference, r.caQtyDifference || 0);
                                            const diffClass = isBreak ? "num cc-diff" : "num cc-diff-zero";
                                            return isBenefitStage ? (
                                                <tr key={r.accountCode} className={isBreak ? "cc-row-break" : undefined}>
                                                    <td translate="no">{r.accountCode}</td>
                                                    <td>{r.caRefNo || "—"}</td>
                                                    <td className="num">{qty(r.faCaQty)}</td>
                                                    <td className="num">{qty(r.custodianCaQty)}</td>
                                                    <td className="num">{qty(r.receivedQty)}</td>
                                                    <td className="num">{qty(r.balanceQty)}</td>
                                                    <td>{r.receivedDate || "—"}</td>
                                                    <td className={diffClass}>{qty(r.difference)}</td>
                                                    <td>
                                                        <StatusBadge status={r.status} />
                                                    </td>
                                                </tr>
                                            ) : (
                                                <tr key={r.accountCode} className={isBreak ? "cc-row-break" : undefined}>
                                                    <td translate="no">{r.accountCode}</td>
                                                    <td>{r.recordDate}</td>
                                                    <td className="num">{qty(r.faHolding)}</td>
                                                    <td className="num">{qty(r.custodianHolding)}</td>
                                                    <td className="num">{qty(r.faCaQty)}</td>
                                                    <td className="num">{qty(r.custodianCaQty)}</td>
                                                    <td className={diffClass}>{qty(diff)}</td>
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

                        <div className="cc-actions">
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
