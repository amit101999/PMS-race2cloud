import React from "react";

/**
 * The one download action used in every history table on this page — Recent
 * Uploads, Previous Custodian Uploads, Difference Reports.
 */
const DownloadButton = ({ onClick, busy = false }) => (
    <button
        type="button"
        className="dl-btn"
        onClick={onClick}
        disabled={busy}
        aria-label={busy ? "Preparing download" : "Download"}
    >
        {busy ? (
            <span className="dl-spinner" aria-hidden="true" />
        ) : (
            <svg width="12" height="12" viewBox="0 0 16 16" fill="none" aria-hidden="true">
                <path
                    d="M8 2v9m0 0L4.5 7.5M8 11l3.5-3.5M3 14h10"
                    stroke="currentColor"
                    strokeWidth="1.4"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                />
            </svg>
        )}
        <span>Download</span>
    </button>
);

export default DownloadButton;
