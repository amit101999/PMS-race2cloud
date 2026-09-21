import React from "react";

const ArrowDown = () => (
    <svg width="12" height="12" viewBox="0 0 16 16" fill="none" aria-hidden="true">
        <path
            d="M8 2.5v10M4 8.5l4 4 4-4"
            stroke="currentColor"
            strokeWidth="1.6"
            strokeLinecap="round"
            strokeLinejoin="round"
        />
    </svg>
);

/**
 * The one Download button used across the app. Styling lives in
 * styles/design-system.css (.btn-download).
 *
 * Pass `href` when the file already has a URL — it renders a real link so the
 * browser's own download/new-tab behaviour is kept; otherwise pass `onClick`.
 */
const DownloadButton = ({ onClick, href, busy = false, disabled = false }) => {
    if (href) {
        return (
            <a className="btn-download" href={href} download target="_blank" rel="noreferrer">
                <ArrowDown />
                <span>Download</span>
            </a>
        );
    }

    return (
        <button
            type="button"
            className="btn-download"
            onClick={onClick}
            disabled={busy || disabled}
            aria-busy={busy}
        >
            {busy ? <span className="btn-spinner" aria-hidden="true" /> : <ArrowDown />}
            <span>{busy ? "Preparing…" : "Download"}</span>
        </button>
    );
};

export default DownloadButton;
