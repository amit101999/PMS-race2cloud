import React, { useEffect, useState } from "react";

import { BASE_URL } from "../../constant";
import "./CustodianComparePanel.css";

/** Status → badge colour and the label shown on screen. The CSV keeps the raw status. */
const STATUS_VIEW = {
    MATCHED: { badge: "badge--ok", label: "Matched" },
    QTY_MISMATCH: { badge: "badge--bad", label: "Qty Mismatch" },
    MISSING_IN_CUSTODY: { badge: "badge--warn", label: "Missing in Custodian" },
    MISSING_IN_FA: { badge: "badge--warn", label: "Missing in FA" },
};

const numberFormat = new Intl.NumberFormat(undefined, { maximumFractionDigits: 4 });
const dateTimeFormat = new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" });

const qty = (v) => (v === null || v === undefined ? "—" : numberFormat.format(v));

/**
 * Split / Bonus pages: compares the page's preview (holding on the record date)
 * with the newest file uploaded on Corporate Actions -> Custodian Upload.
 * The pages render it only once the affected accounts have been fetched.
 *
 * @param {"split"|"bonus"} caType  which rows of the custodian file apply
 */
const CustodianComparePanel = ({ caType, isin, securityName, ratio1, ratio2, recordDate }) => {
    const [fileInfo, setFileInfo] = useState(null); // { file, rowCount }
    const [fileError, setFileError] = useState("");
    const [comparing, setComparing] = useState(false);
    const [result, setResult] = useState(null);
    const [compareError, setCompareError] = useState("");

    // Which custodian file applies, as soon as an ISIN is picked.
    useEffect(() => {
        setFileInfo(null);
        setFileError("");
        if (!isin) return undefined;

        let cancelled = false;
        const params = new URLSearchParams({ caType, isin });
        fetch(`${BASE_URL}/custodian-upload/latest?${params.toString()}`)
            .then(async (res) => {
                const body = await res.json().catch(() => ({}));
                if (cancelled) return;
                if (!res.ok || !body.success) setFileError(body.message || "Could not read the custodian file.");
                else setFileInfo(body);
            })
            .catch(() => !cancelled && setFileError("Could not reach the server. Check the backend is running."));
        return () => {
            cancelled = true;
        };
    }, [caType, isin]);

    // A result only describes the inputs it was run with.
    useEffect(() => {
        setResult(null);
        setCompareError("");
    }, [caType, isin, ratio1, ratio2, recordDate]);

    const canCompare =
        Boolean(isin && recordDate && fileInfo?.rowCount) && Number(ratio1) > 0 && Number(ratio2) > 0 && !comparing;

    const runCompare = async () => {
        setComparing(true);
        setResult(null);
        setCompareError("");
        try {
            const res = await fetch(`${BASE_URL}/${caType}/compare-custodian`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ isin, securityName, ratio1: Number(ratio1), ratio2: Number(ratio2), recordDate }),
            });
            const body = await res.json().catch(() => ({}));
            if (!res.ok || !body.success) setCompareError(body.message || "The comparison failed. Try again.");
            else setResult(body);
        } catch {
            setCompareError("Could not reach the server. Check the backend is running.");
        } finally {
            setComparing(false);
        }
    };

    if (!isin) return null;

    const typeLabel = caType === "split" ? "Split" : "Bonus";
    const counts = result?.counts;
    const summary = counts
        ? [
              { key: "total", label: "Total", value: counts.total, badge: "badge--neutral" },
              { key: "matched", label: "Matched", value: counts.matched, badge: "badge--ok" },
              { key: "qty", label: "Qty Mismatch", value: counts.qtyMismatch, badge: "badge--bad" },
              { key: "custody", label: "Missing in Custodian", value: counts.missingInCustody, badge: "badge--warn" },
              { key: "fa", label: "Missing in FA", value: counts.missingInFa, badge: "badge--warn" },
          ]
        : [];

    return (
        <div className="temp-transaction-card cc-panel">
            <h3 className="ops-card-title">Compare with Custodian File</h3>

            <div aria-live="polite">
                {fileError ? <div className="cm-verdict bad">{fileError}</div> : null}
                {fileInfo ? (
                    <div className={fileInfo.rowCount ? "cm-verdict ok" : "cm-verdict warn"}>
                        Custodian file fetched: <strong className="cc-file-name">{fileInfo.file.fileName}</strong> ·
                        uploaded {dateTimeFormat.format(new Date(fileInfo.file.timeMs))} ·{" "}
                        {numberFormat.format(fileInfo.rowCount)} {typeLabel} row(s) for{" "}
                        <span translate="no">{isin}</span>
                        {fileInfo.rowCount ? "" : ". Upload a file that includes this security to compare."}
                    </div>
                ) : null}
            </div>

            <div className="cc-actions">
                {!recordDate ? <span className="cm-sub">Enter the Record Date to compare.</span> : null}
                <button type="button" className="cm-primary" disabled={!canCompare} onClick={runCompare}>
                    {comparing ? "Comparing…" : "Compare with Custodian"}
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
                                        <tr>
                                            <th scope="col">ISIN</th>
                                            <th scope="col">ISIN Name</th>
                                            <th scope="col">Account Code</th>
                                            <th scope="col">Record Date</th>
                                            <th scope="col" className="num">FA Holding</th>
                                            <th scope="col" className="num">Custodian Holding</th>
                                            <th scope="col" className="num">Difference</th>
                                            <th scope="col">Status</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {result.rows.map((r) => {
                                            const view = STATUS_VIEW[r.status] || { badge: "badge--neutral", label: r.status };
                                            const isBreak = r.status !== "MATCHED";
                                            return (
                                                <tr key={r.accountCode} className={isBreak ? "cc-row-break" : undefined}>
                                                    <td translate="no">{r.isin}</td>
                                                    <td className="cc-name" title={r.isinName || undefined}>
                                                        {r.isinName || "—"}
                                                    </td>
                                                    <td translate="no">{r.accountCode}</td>
                                                    <td>{r.recordDate}</td>
                                                    <td className="num">{qty(r.faHolding)}</td>
                                                    <td className="num">{qty(r.custodianHolding)}</td>
                                                    <td className={isBreak ? "num cc-diff" : "num cc-diff-zero"}>
                                                        {qty(r.difference)}
                                                    </td>
                                                    <td>
                                                        <span className={`badge ${view.badge}`}>{view.label}</span>
                                                    </td>
                                                </tr>
                                            );
                                        })}
                                    </tbody>
                                </table>
                            </div>
                        )}
                    </>
                ) : null}
            </div>
        </div>
    );
};

export default CustodianComparePanel;
