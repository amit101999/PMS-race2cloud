import React from "react";

/**
 * One status vocabulary, used everywhere on this page — the top status bar,
 * the workflow stepper, and every table row. A run, a file and a stage all
 * report through the same five states so an operator learns the system once.
 */
export const STATUS = {
    OK: "ok",
    WARN: "warn",
    BAD: "bad",
    PROCESSING: "processing",
    NEUTRAL: "neutral",
};

const GLYPH = {
    ok: "✓",
    warn: "⚠",
    bad: "✕",
    processing: "●",
    neutral: "–",
};

/** @param {{ tone: string, children: React.ReactNode }} props */
const StatusBadge = ({ tone = STATUS.NEUTRAL, children }) => (
    <span className={`badge badge--${tone}`}>
        <span className="badge-glyph" aria-hidden="true">
            {GLYPH[tone] || GLYPH.neutral}
        </span>
        {children}
    </span>
);

export default StatusBadge;
