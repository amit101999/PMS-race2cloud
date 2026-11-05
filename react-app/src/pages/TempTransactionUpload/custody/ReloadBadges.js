import React, { useState } from "react";

import StatusBadge, { STATUS } from "./StatusBadge";
import * as api from "./api";

const BADGES = [
    { key: "same", label: "Same", tone: STATUS.NEUTRAL },
    { key: "changed", label: "Changed", tone: STATUS.WARN },
    { key: "new", label: "New", tone: STATUS.OK },
    { key: "duplicate", label: "Duplicate", tone: STATUS.WARN },
    { key: "repeated", label: "Repeated", tone: STATUS.NEUTRAL },
    { key: "removed", label: "Removed", tone: STATUS.BAD },
];

const tradeLine = (t) =>
    `ID ${t.txnId || "(none)"} · ${t.account} · ${t.isin} · ${t.tranType} · qty ${t.quantity} @ ${t.rate}`;

/**
 * Same · Changed · New · Duplicate · Repeated · Removed for an Equity upload or a match.
 * Duplicate means already in our books with the same values; Repeated means the
 * same trade more than once in the file. The counts are complete; the lists
 * underneath are the first few of each kind. With `duplicatesKey` every
 * duplicate can be exported.
 */
const ReloadBadges = ({ counts, samples, duplicatesKey = "" }) => {
    const [open, setOpen] = useState(false);
    const [exporting, setExporting] = useState(false);
    const [exportError, setExportError] = useState("");
    if (!counts) return null;

    const hasSamples =
        samples && ["changed", "removed", "duplicate", "repeated"].some((k) => (samples[k] || []).length > 0);

    const exportDuplicates = async () => {
        setExporting(true);
        setExportError("");
        try {
            const res = await api.getDownloadUrl(duplicatesKey);
            window.open(res.downloadUrl, "_blank", "noopener,noreferrer");
        } catch (err) {
            setExportError(err.message);
        } finally {
            setExporting(false);
        }
    };

    return (
        <div className="cm-reload-badges">
            <div className="cm-reload-badge-row">
                {/* Only the kinds the file actually has; a row of zeros says nothing. */}
                {BADGES.filter((b) => counts[b.key] > 0).map((b) => (
                    <StatusBadge key={b.key} tone={b.tone}>
                        {b.label} {Number(counts[b.key]).toLocaleString()}
                    </StatusBadge>
                ))}
                {hasSamples ? (
                    <button type="button" className="cm-linkbtn" onClick={() => setOpen((v) => !v)}>
                        {open ? "Hide details" : "Show details"}
                    </button>
                ) : null}
                {duplicatesKey && counts.duplicate > 0 ? (
                    <button type="button" className="cm-linkbtn" onClick={exportDuplicates} disabled={exporting}>
                        {exporting ? "Preparing…" : `Export ${counts.duplicate.toLocaleString()} duplicate(s)`}
                    </button>
                ) : null}
            </div>

            {exportError ? <p className="cm-blockers">{exportError}</p> : null}

            {open && hasSamples ? (
                <div className="cm-reload-samples">
                    {(samples.changed || []).map((t) => (
                        <p key={`c-${t.txnId}`}>
                            <strong>Changed</strong> {tradeLine(t)} —{" "}
                            {t.fields.map((f) => `${f.field}: ${f.from || "blank"} → ${f.to || "blank"}`).join(", ")}
                        </p>
                    ))}
                    {(samples.removed || []).map((t, i) => (
                        <p key={`r-${t.txnId}-${i}`}>
                            <strong>Removed</strong> {tradeLine(t)}
                        </p>
                    ))}
                    {(samples.duplicate || []).map((t, i) => (
                        <p key={`d-${t.txnId}-${i}`}>
                            <strong>Duplicate</strong> {tradeLine(t)} — already in our books
                        </p>
                    ))}
                    {(samples.repeated || []).map((t, i) => (
                        <p key={`p-${t.txnId}-${i}`}>
                            <strong>Repeated</strong> {tradeLine(t)} — merged into one row
                        </p>
                    ))}
                </div>
            ) : null}
        </div>
    );
};

export default ReloadBadges;
