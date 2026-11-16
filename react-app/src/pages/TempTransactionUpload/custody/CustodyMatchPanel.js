import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";

import FieldMappingPanel from "./FieldMappingPanel";
import UploadErrorDetails from "./UploadErrorDetails";
import StatusBadge, { STATUS } from "./StatusBadge";
import ReloadBadges from "./ReloadBadges";
import * as api from "./api";
import { useJobNotifications } from "../../../notifications/JobNotificationContext";
import "./CustodyMatch.css";

/**
 * The custodian slot a user drops a file into, as a mapping profile.
 *
 * Two names for one thing: the upload endpoint takes a fileType, the mapping
 * takes a profile type. Kept as one pair of lookups so a third spelling cannot
 * appear somewhere else.
 */
const PROFILE_FOR_FILE_TYPE = {
    [api.CUSTODY_FILE_TYPE.RESIDENTIAL]: api.PROFILE_TYPE.CUSTODY_RESIDENTIAL,
    [api.CUSTODY_FILE_TYPE.NDPMS]: api.PROFILE_TYPE.CUSTODY_NDPMS,
    [api.CUSTODY_FILE_TYPE.MUTUAL_FUND]: api.PROFILE_TYPE.CUSTODY_MUTUAL_FUND,
};

const FILE_TYPE_FOR_PROFILE = {
    [api.PROFILE_TYPE.CUSTODY_RESIDENTIAL]: api.CUSTODY_FILE_TYPE.RESIDENTIAL,
    [api.PROFILE_TYPE.CUSTODY_NDPMS]: api.CUSTODY_FILE_TYPE.NDPMS,
    [api.PROFILE_TYPE.CUSTODY_MUTUAL_FUND]: api.CUSTODY_FILE_TYPE.MUTUAL_FUND,
};

/** The same two names for the transaction file: upload type <-> mapping profile. */
const PROFILE_FOR_FA_FILE_TYPE = {
    [api.FA_FILE_TYPE.REGULAR]: api.PROFILE_TYPE.FA,
    [api.FA_FILE_TYPE.MUTUAL_FUND]: api.PROFILE_TYPE.FA_MUTUAL_FUND,
    // OPI/OPO files have the Equity columns without a Transaction ID, so they have their own mapping.
    [api.FA_FILE_TYPE.OPI_OPO]: api.PROFILE_TYPE.FA_OPI_OPO,
};

const FA_FILE_TYPE_FOR_PROFILE = {
    [api.PROFILE_TYPE.FA]: api.FA_FILE_TYPE.REGULAR,
    [api.PROFILE_TYPE.FA_MUTUAL_FUND]: api.FA_FILE_TYPE.MUTUAL_FUND,
    [api.PROFILE_TYPE.FA_OPI_OPO]: api.FA_FILE_TYPE.OPI_OPO,
};

/** How often the screen asks the job how it is getting on. */
const POLL_MS = 2500;

/** Sentinel for the "name a scheme the list does not have yet" option. */
const NEW_SCHEME = "__NEW_SCHEME__";

/** The add-a-file form: what the next upload will be filed under. */
const blankDraft = () => ({
    scheme: "",
    /** true while the user is naming a scheme the list does not have yet */
    newScheme: false,
    fileType: api.CUSTODY_FILE_TYPE.RESIDENTIAL,
});

/** "2026-08-05" on its own, or "2026-08-05 → 2026-08-20" when a file spans days. */
const rangeLabel = (file) =>
    file.coveredFrom === file.coveredTo
        ? file.coveredFrom
        : `${file.coveredFrom} → ${file.coveredTo}`;

/**
 * Short, actionable phrasing for the error codes a failed run can carry.
 * Falls back to the backend's own message for anything not listed here — never
 * invents a reason the server did not give.
 */
const FRIENDLY_RUN_ERROR = {
    FA_FILE_MISSING: "Fund Accounting file is missing. Upload a file to continue.",
};

const uploadedAtLabel = (ms) =>
    ms ? new Date(ms).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" }) : "—";

/**
 * What a staged transaction file covers — the span of transaction dates
 * (DATEPUR_ACQUI) its rows carry, which is what identifies the file to the
 * user. The business date above only says which run it was filed under.
 *
 * Files staged before that range was recorded on the key fall back to when they
 * were uploaded, which is all such a key knows.
 */
const faDateLabel = (file) =>
    file.coveredFrom && file.coveredTo
        ? rangeLabel(file)
        : `Uploaded ${uploadedAtLabel(file.uploadedAtMs)}`;

/** The upload drop-zone's icon — an arrow into a tray, used by both panels. */
export const UploadGlyph = () => (
    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" aria-hidden="true">
        <path
            d="M12 15V4M12 4l-4.5 4.5M12 4l4.5 4.5"
            stroke="currentColor"
            strokeWidth="1.6"
            strokeLinecap="round"
            strokeLinejoin="round"
        />
        <path
            d="M4 15v3a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-3"
            stroke="currentColor"
            strokeWidth="1.6"
            strokeLinecap="round"
            strokeLinejoin="round"
        />
    </svg>
);

/** A small document icon for a staged file row, used by both panels. */
export const FileGlyph = () => (
    <svg width="15" height="15" viewBox="0 0 16 16" fill="none" aria-hidden="true">
        <path
            d="M4 1.5h4.5L12 5v9a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1V2.5a1 1 0 0 1 1-1Z"
            stroke="currentColor"
            strokeWidth="1.1"
            strokeLinejoin="round"
        />
        <path d="M8.5 1.5V5H12" stroke="currentColor" strokeWidth="1.1" strokeLinejoin="round" />
    </svg>
);

/**
 * Columns the file and the mapping disagree about. The file was accepted — these
 * are not errors — but a column we ignored, or a field with nowhere to read
 * from, is exactly the kind of mismatch that otherwise shows up much later as an
 * unexplained blank in the report.
 */
const HeaderWarnings = ({ warnings }) => {
    if (!warnings || warnings.length === 0) return null;

    const ignored = warnings.filter((w) => w.code === "UNUSED_FILE_COLUMN");
    const blank = warnings.filter((w) => w.code === "MISSING_OPTIONAL_COLUMN");
    const other = warnings.filter(
        (w) => w.code !== "UNUSED_FILE_COLUMN" && w.code !== "MISSING_OPTIONAL_COLUMN",
    );

    return (
        <div className="cm-warnings" role="status">
            <strong>File accepted, with {warnings.length} note(s).</strong>
            {ignored.length > 0 ? (
                <p>
                    Ignored — not mapped to anything:{" "}
                    <span className="cm-warn-cols">
                        {ignored.map((w) => w.column).join(", ")}
                    </span>
                </p>
            ) : null}
            {blank.length > 0 ? (
                <p>
                    Will be blank — no column in this file feeds them:{" "}
                    <span className="cm-warn-cols">
                        {blank.map((w) => w.column).join(", ")}
                    </span>
                </p>
            ) : null}
            {other.map((w, i) => (
                <p key={i}>
                    {w.row ? `Row ${w.row}: ` : ""}
                    {w.message}
                </p>
            ))}
        </div>
    );
};

/** Why a date cannot be ticked, or "" when both sides have a file. */
const missingSideMessage = (d) => {
    if (d.closed) return d.closed;
    if (d.faFiles.length === 0 && d.custodyFiles.length === 0) return "No files are present for this date.";
    if (d.custodyFiles.length === 0) return "Custodian file is not present for this date.";
    if (d.faFiles.length === 0 && !d.faOptional) return "FA file is not present for this date.";
    return "";
};

/** A locked trade date: lock icon and one short line, styled as information. */
const LockedNotice = ({ message }) => (
    <div className="cm-reload-result cm-locked-note" role="status">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" aria-hidden="true">
            <rect x="5" y="10.5" width="14" height="10" rx="2" stroke="currentColor" strokeWidth="1.8" />
            <path d="M8 10.5V7.5a4 4 0 0 1 8 0v3" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
        </svg>
        <p>{message}</p>
    </div>
);

/** "2026-09-25" -> "25-09-2026", the way dates are read on this screen. */
const toDisplayDate = (iso) => (iso ? iso.split("-").reverse().join("-") : "");

/** "3 schemes" style summary of a date's custodian files. */
const custodySummary = (files) => {
    const schemes = new Set(files.map((f) => f.scheme)).size;
    return `${files.length} file(s), ${schemes} scheme${schemes === 1 ? "" : "s"}`;
};

/**
 * Which dates to match. Every date with an uploaded file is listed; one match
 * runs per ticked date. A date missing either side is shown unticked and
 * disabled, with the reason, so it is clear what still has to be uploaded.
 */
const MatchDialog = ({ dates, loading, error, selected, onToggle, onAll, onNone, onRun, onClose }) => {
    const chosen = dates.filter((d) => selected.has(d.date));

    return (
        <>
            <div className="cm-scrim" onClick={onClose} />
            <div className="cm-dialog" role="dialog" aria-label="Choose dates to match">
                <h3>Which dates should be matched?</h3>

                {loading ? <p className="cm-muted">Loading the uploaded files…</p> : null}
                {error ? <div className="cm-dialog-error">{error}</div> : null}

                {!loading && !error && dates.length === 0 ? (
                    <p className="cm-muted">
                        No files uploaded yet. Upload an FA file and a custodian file for the same date.
                    </p>
                ) : null}

                {!loading && !error && dates.length > 0 ? (
                    <>
                        <div className="cm-dialog-actions">
                            <button type="button" className="cm-linkbtn" onClick={onAll}>
                                Select all ready
                            </button>
                            <button type="button" className="cm-linkbtn" onClick={onNone}>
                                Clear
                            </button>
                        </div>

                        <ul className="cm-datelist">
                            {dates.map((d) => {
                                const missing = missingSideMessage(d);
                                return (
                                    <li key={d.date} className={missing ? "is-blocked" : ""}>
                                        <label>
                                            <input
                                                type="checkbox"
                                                checked={!missing && selected.has(d.date)}
                                                disabled={Boolean(missing)}
                                                onChange={() => onToggle(d.date)}
                                            />
                                            <span className="cm-date-label">{d.date}</span>
                                            <span className="cm-date-counts">
                                                FA{" "}
                                                {d.faFiles.length > 0
                                                    ? `✓ (${d.faFiles.length} file(s))`
                                                    : d.faOptional
                                                      ? "our books"
                                                      : "✗"}{" "}
                                                ·
                                                Custodian{" "}
                                                {d.custodyFiles.length > 0 ? `✓ (${custodySummary(d.custodyFiles)})` : "✗"}
                                            </span>
                                        </label>
                                        {missing ? (
                                            <span className="cm-date-note cm-date-note--error">{missing}</span>
                                        ) : null}
                                    </li>
                                );
                            })}
                        </ul>

                        <p className="cm-dialog-summary">
                            {chosen.length === 0
                                ? "Pick at least one date."
                                : `${chosen.length} date(s) — one match runs for each.`}
                        </p>
                    </>
                ) : null}

                <div className="cm-dialog-foot">
                    <button type="button" className="cm-secondary" onClick={onClose}>
                        Cancel
                    </button>
                    <button
                        type="button"
                        className="cm-primary"
                        onClick={onRun}
                        disabled={loading || Boolean(error) || chosen.length === 0}
                    >
                        Match {chosen.length} date(s)
                    </button>
                </div>
            </div>
        </>
    );
};

const ASK_REASON_TEXT = {
    NO_IDS: "Some trades of this date were booked before transaction IDs existed, so they can only be matched by their details.",
    NEW_IDS_LOOK_OLD: "None of this file's IDs are booked for the date, but most rows look like trades already booked. The file may have been exported again with new IDs.",
    PARTIAL: "Only part of the date's IDs are in this file.",
};

/**
 * Equity tab: the system could not tell whether the new file replaces the date's
 * trades or adds to them, so the operator chooses. Each button says what it does.
 */
const AskModeDialog = ({ ask, busy, onChoose, onClose }) => {
    const { res } = ask;
    const option = (choice, title, comparison) => (
        <div className="cm-ask-option">
            <h4>{title}</h4>
            <p className="cm-muted">
                The date will have {comparison.totalAfter.toLocaleString()} trade(s) (now {comparison.totalBefore.toLocaleString()}).
            </p>
            <ReloadBadges counts={comparison.counts} samples={comparison.samples} duplicatesKey={comparison.duplicatesKey} />
            <button type="button" className="cm-primary" disabled={busy} onClick={() => onChoose(choice)}>
                {choice === "REPLACE" ? "Replace file" : "Add as extra file"}
            </button>
        </div>
    );

    return (
        <>
            <div className="cm-scrim" onClick={busy ? undefined : onClose} />
            <div className="cm-dialog" role="dialog" aria-label="Replace or add">
                <h3>Replace the date's trades, or add to them?</h3>
                <p>
                    <strong className="mono">{res.fileName}</strong> for {res.businessDate}.{" "}
                    {ASK_REASON_TEXT[res.reason] || ""}
                </p>
                {option("REPLACE", "Replace file", res.options.replace)}
                {option("ADD", "Add as extra file", res.options.add)}
                <div className="cm-dialog-foot">
                    <button type="button" className="cm-secondary" onClick={onClose} disabled={busy}>
                        Cancel — do not upload
                    </button>
                </div>
            </div>
        </>
    );
};

/**
 * Equity tab: an uploaded FA file repeats trades already in our books (same
 * Transaction ID, same values). Says how many, offers them as a download, and
 * how many of the file's records go into the system. Wording only: Commit still
 * cleans up the date and loads the whole file.
 */
const DuplicatesNotice = ({ comparison }) => {
    const [busy, setBusy] = useState(false);
    const [error, setError] = useState("");
    const { same, changed, new: added, duplicate } = comparison.counts;
    // The file's distinct records; a row repeated inside the file counts once.
    const fileRecords = same + changed + added + duplicate;

    const exportDuplicates = async () => {
        setBusy(true);
        setError("");
        try {
            const res = await api.getDownloadUrl(comparison.duplicatesKey);
            window.open(res.downloadUrl, "_blank", "noopener,noreferrer");
        } catch (err) {
            setError(err.message);
        } finally {
            setBusy(false);
        }
    };

    return (
        <>
            <p>
                <strong>{duplicate.toLocaleString()} duplicate record(s)</strong> — already in our books with the same
                Transaction ID.{" "}
                {comparison.duplicatesKey ? (
                    <button type="button" className="cm-linkbtn" onClick={exportDuplicates} disabled={busy}>
                        {busy ? "Preparing…" : "Export duplicates"}
                    </button>
                ) : null}
            </p>
            <p>{(fileRecords - duplicate).toLocaleString()} record(s) will be updated in the system.</p>
            {error ? <p className="cm-blockers">{error}</p> : null}
        </>
    );
};

/**
 * Equity tab: a custodian file overlaps the lines stored for its date, scheme and
 * type. Keeping both would count the repeated lines twice, so it replaces them or
 * it is not uploaded.
 */
const ReplaceCustodyDialog = ({ ask, busy, onReplace, onClose }) => {
    const { res } = ask;
    return (
        <>
            <div className="cm-scrim" onClick={busy ? undefined : onClose} />
            <div className="cm-dialog" role="dialog" aria-label="Replace custodian file">
                <h3>Replace custodian file?</h3>
                <p>
                    A {res.scheme} custodian file is already uploaded for {toDisplayDate(res.businessDate)}. Do you want
                    to replace it with <strong className="mono">{res.fileName}</strong>?
                </p>
                <p>Your existing transactions will not be affected.</p>
                <div className="cm-dialog-foot">
                    <button type="button" className="cm-secondary" onClick={onClose} disabled={busy}>
                        Cancel
                    </button>
                    <button type="button" className="cm-primary" onClick={onReplace} disabled={busy}>
                        {busy ? "Replacing…" : "Replace file"}
                    </button>
                </div>
            </div>
        </>
    );
};

/**
 * Stage 1 + Stage 2 of the gated workflow, plus Gate 1 and the progress banner.
 *
 * The exception grid and Gate 2 live in a separate component; this one hands the
 * finished run up to the parent through `onRunChange`.
 */
const CustodyMatchPanel = ({
    uploadDate,
    onRunChange,
    onCustodyUploaded,
    onDayChange,
    /* Equity tab: uploads go through /reload (Add / Replace by transaction ID,
       badges), Match builds the date's final list, and only Equity files are taken. */
    reloadMode = false,
    /* Changes when a Pre-commit starts or finishes, so the FA file statuses are read again. */
    preCommitTick = 0,
    /* With reloadMode: the tab. MUTUAL_FUND is the Mutual Funds tab, which shows only the
       Fund Accounting card (no custodian file, no match) and takes mutual fund files only.
       OPI_OPO is the OPI/OPO tab: the same single card, Equity-layout files whose rows are
       all OPI or OPO, and no change window. */
    kind = api.KIND.EQUITY,
    /* Mutual Funds and OPI/OPO tabs: told the tab's files listed on the card, so the page can offer their dates for Commit. */
    onFaFilesChange,
}) => {
    const isMf = reloadMode && kind === api.KIND.MUTUAL_FUND;
    const isOpiOpo = reloadMode && kind === api.KIND.OPI_OPO;
    /* Mutual Funds and OPI/OPO: no custodian file and no match, only the Fund Accounting card. */
    const faOnly = isMf || isOpiOpo;
    /* Schemes already used, from the stored custodian keys. Each entry knows its
       own layout, so choosing a scheme fills the account type as well. */
    const [schemes, setSchemes] = useState([]);

    /* The custodian list is whatever is stored for the date — the screen shows a
       row per file rather than holding its own copy, so an upload, a replace and
       a remove all end the same way: reload the day. */
    const [draft, setDraft] = useState(blankDraft());
    const [addBusy, setAddBusy] = useState(false);
    const [addError, setAddError] = useState(null);
    const [addWarnings, setAddWarnings] = useState([]);
    /* Equity tab: what the last custodian upload did, and the upload waiting on
       the operator's Replace. */
    const [custodyResult, setCustodyResult] = useState(null);
    const [custodyAsk, setCustodyAsk] = useState(null);
    /*
     * Files removed during this visit. Stratus deletes the object at once but
     * its listing index lags by roughly twenty seconds, so every reload inside
     * that window still reports the file — without this the row a user just
     * removed would reappear until they refreshed the page by hand.
     *
     * Keys carry the upload timestamp, so a re-uploaded file gets a new one and
     * is never hidden by a stale entry here.
     */
    const [removedKeys, setRemovedKeys] = useState(() => new Set());
    /* The file whose delete request is in flight, so its row can say so. */
    const [removingKey, setRemovingKey] = useState(null);
    const { trackJob, reportJob, refresh: refreshNotifications } = useJobNotifications();
    const [day, setDay] = useState(null);
    const [dayLoading, setDayLoading] = useState(false);

    const [faBusy, setFaBusy] = useState(false);
    const [faError, setFaError] = useState(null);
    /* A file can be accepted and still have something worth saying about it —
       a column we ignored, a field with nowhere to read from. The server has
       always sent these; the screen was dropping them. */
    const [faWarnings, setFaWarnings] = useState([]);
    /* Equity tab: what the last FA upload did (mode and badges), and the
       upload waiting on the operator's Replace / Add choice. */
    const [faResult, setFaResult] = useState(null);
    const [ask, setAsk] = useState(null);

    /* One run per date shown, oldest date first. */
    const [runs, setRuns] = useState([]);
    const [runError, setRunError] = useState(null);
    const [starting, setStarting] = useState(false);
    /* Set when Match handed back a report that already existed. */
    const [reusedNote, setReusedNote] = useState("");
    /* The failed run whose technical details are open, if any. */
    const [failDetailsRunId, setFailDetailsRunId] = useState("");
    const anyRunning = runs.some((r) => r.isRunning);

    /* Every date with uploaded files, from the file table. Backs the summary
       and the dialog Match opens. */
    const [matchDates, setMatchDates] = useState([]);
    const [pickerOpen, setPickerOpen] = useState(false);
    const [pickerLoading, setPickerLoading] = useState(false);
    const [pickerError, setPickerError] = useState("");
    const [selectedDates, setSelectedDates] = useState(new Set());

    /*
     * The file waiting on its mapping.
     *
     * Picking a file no longer uploads it. It is held here while the mapping
     * panel confirms which column feeds which field, and goes up only when that
     * panel says so — which is what makes the mapping a gate rather than a
     * screen somebody may or may not have visited.
     */
    const [pending, setPending] = useState(null);

    /** Which transaction file the next pick is. Stays put between uploads, as the custodian type does. */
    const [faFileType, setFaFileType] = useState(() => {
        if (isMf) return api.FA_FILE_TYPE.MUTUAL_FUND;
        if (isOpiOpo) return api.FA_FILE_TYPE.OPI_OPO;
        return api.FA_FILE_TYPE.REGULAR;
    });

    /* Purely visual — whether a file is currently being dragged over each
       drop zone. Never affects what gets uploaded or how. */
    const [custodyDragActive, setCustodyDragActive] = useState(false);
    const [faDragActive, setFaDragActive] = useState(false);

    const faInputRef = useRef(null);
    const pollRef = useRef(null);

    /* ── loading a date ───────────────────────────────────────────────── */

    /* The file dates whose match results are shown, oldest first. Set only when
       the user starts a match (every ticked date), so picking an upload date
       never shows an earlier result on its own. */
    const [resultDates, setResultDates] = useState([]);

    // A new upload date starts with no result on screen.
    useEffect(() => {
        setResultDates([]);
    }, [uploadDate]);

    const loadDay = useCallback(async () => {
        // No date chosen (the screen starts that way, or it was cleared): there
        // is nothing to show, and the previous date's files must not linger.
        if (resultDates.length === 0 || !uploadDate) {
            setDay(null);
            setRuns([]);
            return;
        }
        setDayLoading(true);
        try {
            // Runs belong to an upload date: the result for each file date under it.
            const results = await Promise.all(
                resultDates.map((date) => api.getLatestRun(date, uploadDate)),
            );
            // Only the stepper reads the day, so the first date's is enough.
            setDay(results[0].day);
            // A superseded run was matched against files or a mapping that have
            // since changed, so its result no longer describes this date. Treat
            // the date as not yet matched rather than showing a stale result.
            setRuns(
                results
                    .map((res) => res.run)
                    .filter((r) => r && r.status !== "SUPERSEDED"),
            );
            setRunError(null);
        } catch (err) {
            setRunError({ message: err.message, detail: err.detail });
        } finally {
            setDayLoading(false);
        }
    }, [resultDates, uploadDate]);

    useEffect(() => {
        loadDay();
    }, [loadDay]);

    /** Throws, so the dialog can show the error; other callers ignore it. */
    const loadMatchDates = useCallback(async () => {
        const res = await api.getMatchDates();
        setMatchDates(res.dates || []);
        return res.dates || [];
    }, []);

    useEffect(() => {
        loadMatchDates().catch(() => {
            // The summary just stays empty; opening the dialog retries and shows the error.
        });
    }, [loadMatchDates]);

    /*
     * The date each card's file is for, separate from each other and from the
     * upload date at the top. Each card lists the files saved under the upload
     * date: all of them, or only its own date's once that date is picked. No
     * upload date, no list.
     */
    const [custodyDate, setCustodyDate] = useState("");
    const [custodyDay, setCustodyDay] = useState(null);
    const [faDate, setFaDate] = useState("");
    const [faDay, setFaDay] = useState(null);

    /* Equity tab: each trade date's change window from the server,
       { [date]: { days, fixed, lastDay, locked, message } }. */
    const [windows, setWindows] = useState({});
    const [windowCustomOpen, setWindowCustomOpen] = useState(false);
    const [windowCustomText, setWindowCustomText] = useState("");
    const [windowSaving, setWindowSaving] = useState(false);

    /* The locked text for a trade date, or "" while it is open (or not read yet). */
    const lockedMessage = (date) => {
        if (!reloadMode || !date) return "";
        const w = windows[date];
        return w && w.locked ? w.message : "";
    };

    const loadCustodyDay = useCallback(async () => {
        if (!uploadDate) {
            setCustodyDay(null);
            return;
        }
        try {
            setCustodyDay(await api.getUploadedFiles(uploadDate, custodyDate));
        } catch (err) {
            setAddError({ message: err.message, detail: err.detail });
        }
    }, [custodyDate, uploadDate]);

    const loadFaDay = useCallback(async () => {
        if (!uploadDate) {
            setFaDay(null);
            return;
        }
        try {
            setFaDay(await api.getUploadedFiles(uploadDate, faDate));
        } catch (err) {
            setFaError({ message: err.message, detail: err.detail });
        }
    }, [faDate, uploadDate]);

    useEffect(() => {
        loadCustodyDay();
    }, [loadCustodyDay]);

    useEffect(() => {
        loadFaDay();
    }, [loadFaDay]);

    /* The page shell shows a compact status summary above this panel and
       needs the same `day` this panel already loads — exposed rather than
       re-fetched, so there is exactly one place that reads storage for a date. */
    useEffect(() => {
        if (onDayChange) onDayChange(day);
    }, [day, onDayChange]);

    /**
     * The schemes the picker offers.
     *
     * Derived from the files already filed, so it CHANGES as files are added —
     * which is why it cannot be read once on mount. Uploading the first file for
     * a scheme is exactly the moment the list stops being empty, and reading it
     * only at mount left the row showing the "new scheme" text box until the
     * user reloaded the page by hand.
     */
    const loadSchemes = useCallback(async () => {
        try {
            const res = await api.getSchemes();
            setSchemes(res.schemes || []);
        } catch {
            // A missing scheme list must not take the panel down; the row simply
            // offers nothing to pick until the call succeeds.
        }
    }, []);

    useEffect(() => {
        loadSchemes();
    }, [loadSchemes]);

    useEffect(() => {
        if (onRunChange) onRunChange(runs);
    }, [runs, onRunChange]);

    /* ── polling ──────────────────────────────────────────────────────── */

    // A string, so the effect restarts only when the set of moving runs changes.
    const runningIds = runs
        .filter((r) => r.isRunning)
        .map((r) => r.runId)
        .join(",");

    useEffect(() => {
        // Poll only while a job is actually moving, and always clear on unmount
        // so a navigation away does not leave a timer calling into a dead screen.
        if (!runningIds) {
            if (pollRef.current) {
                clearInterval(pollRef.current);
                pollRef.current = null;
            }
            return undefined;
        }

        pollRef.current = setInterval(async () => {
            try {
                const latest = await Promise.all(
                    runningIds.split(",").map((id) => api.getRun(id)),
                );
                const byId = new Map(latest.map((res) => [res.run?.runId, res.run]));
                setRuns((prev) =>
                    prev
                        .map((r) => (byId.has(r.runId) ? byId.get(r.runId) : r))
                        // A file or mapping changed mid-run: same rule as loadDay, hide the stale run.
                        .filter((r) => r && r.status !== "SUPERSEDED"),
                );
            } catch (err) {
                setRunError({ message: err.message, detail: err.detail });
            }
        }, POLL_MS);

        return () => {
            if (pollRef.current) {
                clearInterval(pollRef.current);
                pollRef.current = null;
            }
        };
    }, [runningIds]);

    /* ── custodian files ──────────────────────────────────────────────── */

    /* Custodian files stored for the custodian card's date. */
    const storedFiles = useMemo(
        () => (custodyDay?.custody?.files || []).filter((f) => !removedKeys.has(f.key)),
        [custodyDay, removedKeys],
    );

    /*
     * Transaction files stored for the FA card's date. A day holds at most one
     * of each type, so this is the regular file, the mutual fund file, or both.
     */
    const faFiles = useMemo(
        () =>
            (faDay?.fa?.raw || []).filter(
                // Each tab lists only its own files: Equity the regular ones, Mutual Funds the mutual fund ones.
                (f) => !removedKeys.has(f.key) && (!reloadMode || f.fileType === faFileType),
            ),
        [faDay, removedKeys, reloadMode, faFileType],
    );

    useEffect(() => {
        if (onFaFilesChange) onFaFilesChange(faFiles);
    }, [faFiles, onFaFilesChange]);

    /*
     * Equity tab: the FA files still waiting for a Pre-commit. The others are
     * already in our books, so they are not "Ready", and removing one does not
     * take its trades out. Null until read.
     */
    const [pendingFaKeys, setPendingFaKeys] = useState(null);
    useEffect(() => {
        if (!reloadMode) return undefined;
        let stale = false;
        // OPI/OPO: a file is waiting until its own Commit starts.
        const read = isOpiOpo
            ? Promise.all(faFiles.map((f) => api.getOpiOpoFile(f.uploadedAtMs).then((r) => ({ key: f.key, r })))).then(
                  (results) => results.filter(({ r }) => !r.progress).map(({ key }) => key),
              )
            : Promise.all([...new Set(faFiles.map((f) => f.businessDate))].map((d) => api.getEquityDate(d, kind))).then(
                  (results) => results.flatMap((r) => r.pendingFiles.map((p) => p.key)),
              );
        read
            .then((keys) => {
                if (!stale) setPendingFaKeys(new Set(keys));
            })
            .catch(() => {
                // The list still shows; only the status is unknown until the next read.
                if (!stale) setPendingFaKeys(null);
            });
        return () => {
            stale = true;
        };
    }, [reloadMode, isOpiOpo, faFiles, preCommitTick, kind]);

    /*
     * The file dates with files saved under the upload date picked at the top.
     * Each upload date is its own set: a file date is ready only when BOTH an FA
     * and a custodian file were saved under this upload date.
     */
    /* Equity tab: a date is matched against every custodian file of that date,
       whatever upload date it came under. */
    const custodyFilesByDate = new Map();
    for (const d of matchDates) {
        custodyFilesByDate.set(d.date, [...(custodyFilesByDate.get(d.date) || []), ...d.custodyFiles]);
    }
    const uploadDayDates = (uploadDate ? matchDates.filter((d) => d.uploadDate === uploadDate) : []).map(
        (d) => {
            if (!reloadMode) return d;
            const custodyFiles = custodyFilesByDate.get(d.date) || [];
            // A date past its change window cannot be matched either.
            const closed = lockedMessage(d.date);
            // No pending FA file is fine here: the match then checks our books.
            // A date with neither is refused by the server, which can see the books.
            return { ...d, custodyFiles, faOptional: true, closed, canMatch: custodyFiles.length > 0 && !closed };
        },
    );
    /* Every trade date on screen: both cards' dates, the listed files and the Match dates. */
    const windowDatesKey = [
        faDate,
        custodyDate,
        ...uploadDayDates.map((d) => d.date),
        ...(faDay?.fa?.raw || []).map((f) => f.businessDate),
        ...(custodyDay?.custody?.files || []).map((f) => f.businessDate),
    ]
        .filter(Boolean)
        .sort()
        .filter((d, i, all) => d !== all[i - 1])
        .join(",");

    /* Read again after a commit, which fixes a date's window. OPI/OPO dates have no window. */
    useEffect(() => {
        if (!reloadMode || isOpiOpo || !windowDatesKey) return undefined;
        let cancelled = false;
        api.getDateWindows(windowDatesKey.split(","), kind)
            .then((found) => {
                if (!cancelled) setWindows((prev) => ({ ...prev, ...found }));
            })
            .catch((err) => console.warn("Could not read the change windows:", err.message));
        return () => {
            cancelled = true;
        };
    }, [reloadMode, isOpiOpo, windowDatesKey, preCommitTick, kind]);

    useEffect(() => {
        setWindowCustomOpen(false);
    }, [faDate]);

    const saveFaWindow = async (days) => {
        if (!Number.isInteger(days) || days < 1 || days > api.MAX_WINDOW_DAYS) {
            setFaError({ message: `The window must be a whole number of days from 1 to ${api.MAX_WINDOW_DAYS}.` });
            return;
        }
        setWindowSaving(true);
        setFaError(null);
        try {
            const res = await api.saveDateWindow(faDate, days, kind);
            setWindows((prev) => ({ ...prev, [faDate]: res.window }));
            setWindowCustomOpen(false);
        } catch (err) {
            setFaError({ message: err.message, detail: err.detail });
            // Fixed meanwhile by a commit: show the fixed window instead of the dropdown.
            if (err.detail?.window) setWindows((prev) => ({ ...prev, [faDate]: err.detail.window }));
        } finally {
            setWindowSaving(false);
        }
    };

    const readyDates = uploadDayDates.filter((d) => d.canMatch);
    const readyDateCount = readyDates.length;
    // A closed date is not waiting for anything; it is counted on its own.
    const closedDates = uploadDayDates.filter((d) => d.closed);
    const waitingDateCount = uploadDayDates.length - readyDateCount - closedDates.length;

    /**
     * Send a custodian file that the mapping panel has cleared.
     *
     * Throws rather than handling its own errors: the panel started this upload
     * and is still on screen, so it is the panel that shows what went wrong —
     * including the "already stored for this date" prompt, which used to bounce
     * the user out to a window.confirm here.
     */
    const uploadCustody = async (file, { fileType, replace = false }) => {
        const scheme = draft.scheme.trim();
        setAddBusy(true);
        setAddError(null);
        setAddWarnings([]);
        setCustodyResult(null);
        try {
            const upload = reloadMode ? api.uploadEquityCustodyFile : api.uploadCustodyFile;
            const res = await upload({
                file,
                scheme,
                fileType,
                businessDate: custodyDate,
                uploadDate,
                replace,
            });
            // Nothing is stored: the operator answers first, or it is already uploaded.
            if (res.stored === false) {
                if (res.mode === "ASK") setCustodyAsk({ file, fileType, res });
                else setCustodyResult(res);
                return res;
            }
            setCustodyResult(reloadMode ? res : null);
            setAddWarnings(res.warnings || []);
            reportJob({
                id: `txn-custody-upload-${Date.now()}`,
                label: "Custodian file uploaded",
                status: "completed",
                message: `${file.name} (${scheme}) for ${toDisplayDate(custodyDate)}.`,
            });
            // The scheme and type stay selected: adding several days for one
            // scheme is the case this screen exists for, and clearing after
            // every file would mean re-picking both each time.
            // The results date and the date list are reloaded too: the new file
            // can supersede a shown result and change which dates are ready.
            await Promise.all([loadCustodyDay(), loadDay(), loadMatchDates().catch(() => {})]);
            // This upload may be the first file for its scheme, in which case
            // the scheme only exists from now on. Re-read so the picker offers
            // it without waiting for a page reload.
            await loadSchemes();
            /*
             * A scheme that was being typed is a known one now that a file has
             * been filed under it. Without this the row stays a text box even
             * though the list beneath it has just gained the entry — the scheme
             * is kept selected, only the control it is shown in changes.
             */
            setDraft((d) => (d.newScheme ? { ...d, newScheme: false } : d));
            // Null when the server could not keep the history copy; the list
            // then stays as it was rather than being emptied.
            if (onCustodyUploaded && res.uploadHistory) onCustodyUploaded(res.uploadHistory);
            return res;
        } finally {
            setAddBusy(false);
        }
    };

    /**
     * Both sides remove a file the same way, so they share this. `onError` is
     * the only difference: the message belongs beside the file that failed.
     */
    const removeFile = async (file, what, onError, note = "") => {
        if (
            !window.confirm(
                `Remove ${what} from this run?

` +
                    note +
                    "It may take 2–3 minutes to be deleted from storage. The notification bell " +
                    "says when it is gone.",
            )
        ) {
            return;
        }
        onError(null);
        setRemovingKey(file.key);
        try {
            await api.deleteUploadedFile(file.key);
            setRemovedKeys((prev) => new Set(prev).add(file.key));
            await Promise.all([
                loadCustodyDay(),
                loadFaDay(),
                loadDay(),
                loadMatchDates().catch(() => {}),
            ]);
        } catch (err) {
            onError({ message: err.message, detail: err.detail });
        } finally {
            // The server writes the bell entry: "in progress" now, "removed" once
            // Stratus stops listing the file. Show it straight away.
            refreshNotifications();
            setRemovingKey(null);
        }
    };

    /* The upload notes describe the last uploaded file, so they go once the
       user confirms removing a file or moves to another date / file type. */
    const removeCustodyFile = (file) =>
        removeFile(
            file,
            `${file.originalFileName} (${file.scheme}, ${rangeLabel(file)})`,
            (error) => {
                setAddWarnings([]);
                setAddError(error);
            },
        );

    const changeCustodyDate = (date) => {
        setAddWarnings([]);
        setCustodyDate(date);
    };

    /* ── transaction file ─────────────────────────────────────────────── */

    /* The upload notes describe the last uploaded file, so they go once the
       user confirms removing a file or moves to another date / file type. */
    const removeFaFile = (file, inBooks) =>
        removeFile(
            file,
            file.originalFileName,
            (error) => {
                setFaWarnings([]);
                setFaError(error);
            },
            inBooks ? "This file is already committed. Its trades stay in our books.\n\n" : "",
        );

    const changeFaDate = (date) => {
        setFaWarnings([]);
        setFaDate(date);
    };

    /** Same contract as uploadCustody: the mapping panel owns the errors. */
    const uploadFa = async (file, { fileType, replace = false, choice = "", businessDate = faDate }) => {
        setFaBusy(true);
        setFaError(null);
        setFaWarnings([]);
        setFaResult(null);
        try {
            if (isOpiOpo) {
                const res = await api.uploadOpiOpoFile({ file, uploadDate });
                setFaWarnings(res.warnings || []);
                reportJob({
                    id: `txn-fa-upload-${Date.now()}`,
                    label: "OPI/OPO file uploaded",
                    status: "completed",
                    message: res.message,
                });
                await loadFaDay();
                return res;
            }
            if (reloadMode) {
                const res = await api.uploadEquityFaFile({ file, businessDate, uploadDate, choice, kind });
                // Nothing is stored yet: the operator picks Replace or Add first.
                if (res.mode === "ASK") {
                    setAsk({ file, res });
                    return res;
                }
                setFaResult(res);
                setFaWarnings(res.warnings || []);
                reportJob({
                    id: `txn-fa-upload-${Date.now()}`,
                    label: res.mode === "REPLACE" ? "Transaction file replaced" : "Transaction file uploaded",
                    status: "completed",
                    message: `${file.name} for ${toDisplayDate(res.businessDate)}.`,
                });
                await Promise.all([loadFaDay(), loadDay(), loadMatchDates().catch(() => {})]);
                return res;
            }
            const res = await api.uploadFaFile({
                file,
                businessDate: faDate,
                uploadDate,
                fileType,
                replace,
            });
            setFaWarnings(res.warnings || []);
            reportJob({
                id: `txn-fa-upload-${Date.now()}`,
                label: "Transaction file uploaded",
                status: "completed",
                message: `${file.name} for ${toDisplayDate(faDate)}.`,
            });
            await Promise.all([loadFaDay(), loadDay(), loadMatchDates().catch(() => {})]);
            return res;
        } finally {
            setFaBusy(false);
        }
    };

    /**
     * What the mapping panel calls once every field has been answered.
     *
     * `profileType` comes back from the panel rather than from the draft, so a
     * layout the user corrected there — a Residential file dropped into the
     * NDPMS slot — is uploaded as what it actually is.
     */
    const uploadPending = async ({ profileType, replace }) => {
        if (!pending) return null;
        if (pending.kind === "fa") {
            return uploadFa(pending.file, {
                fileType: FA_FILE_TYPE_FOR_PROFILE[profileType] || pending.fileType,
                replace,
            });
        }
        return uploadCustody(pending.file, {
            fileType: FILE_TYPE_FOR_PROFILE[profileType] || pending.fileType,
            replace,
        });
    };

    /* The operator chose Replace for a custodian file that overlaps the stored lines. */
    const replaceCustodyFile = async () => {
        const { file, fileType } = custodyAsk;
        try {
            await uploadCustody(file, { fileType, replace: true });
        } catch (err) {
            setAddError({ message: err.message, detail: err.detail });
        } finally {
            setCustodyAsk(null);
        }
    };

    /* The operator answered the Replace / Add question; the file goes up now. */
    const chooseAskOption = async (choice) => {
        const { file, res } = ask;
        try {
            await uploadFa(file, { fileType: faFileType, choice, businessDate: res.businessDate });
        } catch (err) {
            setFaError({ message: err.message, detail: err.detail });
        } finally {
            setAsk(null);
        }
    };

    /* ─── gate 1 ───────── */

    /* Enabled only when an upload date is picked and a file uploaded that day
       leaves some date with both an FA and a custodian file. The date list is
       reloaded after every upload and remove, so this follows them. */
    const gateOneOpen = readyDateCount > 0 && !anyRunning && !starting;

    let blockerMessage = "";
    if (!uploadDate) {
        blockerMessage = "Choose the upload date at the top to match the files uploaded that day.";
    } else if (uploadDayDates.length === 0) {
        blockerMessage = `No files were uploaded on ${toDisplayDate(uploadDate)}.`;
    } else if (readyDateCount === 0 && closedDates.length === uploadDayDates.length) {
        blockerMessage =
            closedDates.length === 1
                ? `${closedDates[0].closed} It can no longer be matched or committed.`
                :`These dates can no longer be matched or committed: ${closedDates
                      .map((d) => toDisplayDate(d.date))
                      .join(", ")}.`;
    } else if (readyDateCount === 0) {
        // On the Equity tab only the custodian side can be missing: without a new FA
        // file the match checks our books.
        blockerMessage = reloadMode
            ? "To run the match, upload a custodian file for the same trade date as the FA file."
            : "To run the match, upload an FA file and a custodian file for the same trade date.";
    }

    /**
     * One match per ticked date, started one after another. Each run is tracked
     * in the bell, and the screen shows every ticked date's result together.
     * Reports with differences are also listed under Previous reports.
     */
    const startMatch = async (dates) => {
        setPickerOpen(false);
        setStarting(true);
        setRunError(null);
        setReusedNote("");

        const failures = [];
        const reused = [];
        // Every date that has a run now, started or reused, in date order.
        const started = [];

        for (const date of dates) {
            try {
                const res = await (reloadMode ? api.startEquityRun : api.startRun)(date, uploadDate);
                started.push({ date, runId: res.runId });
                if (res.reused) {
                    /* Nothing was re-run: the same files can only say the same thing. */
                    reused.push(date);
                    reportJob({
                        id: `txn-match-reused-${date}-${Date.now()}`,
                        label: `Matching — ${toDisplayDate(date)}`,
                        status: "completed",
                        message: "Already matched with these files. Showing that result.",
                    });
                } else {
                    // The runId is the bell's key: the server settles it from the run's own status.
                    trackJob({
                        id: res.runId,
                        label: `Matching — ${toDisplayDate(date)}`,
                        message: "Matching FA and custodian files.",
                    });
                }
            } catch (err) {
                failures.push(`${date}: ${err.message}`);
            }
        }

        try {
            if (started.length > 0) {
                const statuses = await Promise.all(started.map((s) => api.getRun(s.runId)));
                setRuns(statuses.map((s) => s.run).filter(Boolean));
                setResultDates(started.map((s) => s.date));
            }
            if (reused.length > 0) {
                setReusedNote(
                    `Already matched, so not run again: ${reused.join(", ")}. Upload or change a file to match a date again.`,
                );
            }
            if (failures.length > 0) {
                setRunError({ message: `Could not start: ${failures.join(" · ")}` });
            }
        } catch (err) {
            setRunError({ message: err.message, detail: err.detail });
        } finally {
            setStarting(false);
        }
    };

    /* Always re-read the dates on open: files may have changed since the screen loaded. */
    const handleMatch = async () => {
        setPickerLoading(true);
        setPickerError("");
        setSelectedDates(new Set());
        setPickerOpen(true);
        try {
            await loadMatchDates();
        } catch (err) {
            setPickerError(err.message);
        } finally {
            setPickerLoading(false);
        }
    };

    const toggleDate = (date) =>
        setSelectedDates((prev) => {
            const next = new Set(prev);
            if (next.has(date)) next.delete(date);
            else next.add(date);
            return next;
        });

    const handleRetry = async (run) => {
        try {
            await api.retryRun(run.runId);
            trackJob({
                id: run.runId,
                label: `Matching — ${toDisplayDate(run.businessDate)}`,
                message: "Matching again.",
            });
            const status = await api.getRun(run.runId);
            setRuns((prev) => prev.map((r) => (r.runId === run.runId ? status.run : r)));
        } catch (err) {
            setRunError({ message: err.message, detail: err.detail });
        }
    };

    /*
     * One entry per scheme for the picker. A scheme filed under both layouts
     * appears once; its most recent layout is the one that gets pre-selected,
     * which the user can still override with the dropdown beside it.
     */
    const schemeOptions = useMemo(() => {
        const bySeen = new Map();
        for (const s of schemes) {
            if (!bySeen.has(s.scheme)) bySeen.set(s.scheme, s);
        }
        return [...bySeen.values()];
    }, [schemes]);

    /**
     * Choosing a known scheme also sets the layout it was last filed under.
     *
     * NEW_SCHEME switches the row to a text box instead. The list is built from
     * schemes already filed, so without this a first-ever upload — or the first
     * one after a day is cleared — would have nothing to pick and no way to
     * proceed.
     */
    /*
     * The dropdown is always the control; the text box appears only when the
     * user asks for it with "+ New scheme…".
     *
     * It used to flip to a text box whenever the list was empty, which made the
     * row change shape depending on how much had been filed — and on a fresh
     * system, where the list is always empty, the dropdown was never seen at
     * all. An empty list still offers "+ New scheme…", so there is no state in
     * which the row cannot be filled in.
     */
    const typingScheme = draft.newScheme;

    const pickScheme = (value) => {
        setAddError(null);
        if (value === NEW_SCHEME) {
            setDraft((d) => ({ ...d, newScheme: true, scheme: "" }));
            return;
        }
        const known = schemeOptions.find((s) => s.scheme === value);
        setDraft((d) => ({
            ...d,
            newScheme: false,
            scheme: value,
            ...(known ? { fileType: known.fileType } : {}),
        }));
    };

    /*
     * The one path a custodian file reaches the mapping gate through, whether
     * it arrived via Browse (the file input's onChange) or via a drop onto
     * the zone — both call this, so there is exactly one place that decides
     * whether a picked file is even eligible to be mapped.
     */
    const handleCustodyFilePicked = async (f) => {
        if (!f) return;
        if (!draft.scheme.trim()) {
            setAddError({ message: "Choose a scheme before choosing a file." });
            return;
        }
        setAddError(null);
        const profileType = PROFILE_FOR_FILE_TYPE[draft.fileType];
        setAddBusy(true);
        try {
            const preview = await api.previewHeaders(f, profileType);
            if (preview?.detectedType && preview.detectedType !== profileType) {
                setAddError({
                    message: `Wrong account type — ${api.FILE_TYPE_LABEL[draft.fileType]} selected, but this file looks like a ${api.PROFILE_LABEL[preview.detectedType]} file.`,
                });
                return;
            }
        } catch (err) {
            setAddError({ message: err.message, detail: err.detail });
            return;
        } finally {
            setAddBusy(false);
        }
        setPending({ kind: "custody", file: f, fileType: draft.fileType, profileType });
    };

    /**
     * Same idea for the transaction file: Browse and drop both land here.
     * An Equity file picked as Mutual Funds (or the reverse) is refused before
     * the mapping window opens; the server tells them apart by TDS / Stamp Duty.
     */
    const handleFaFilePicked = async (f) => {
        if (!f) return;
        setFaError(null);
        const profileType = PROFILE_FOR_FA_FILE_TYPE[faFileType];
        /* Mutual Funds tab: a redemption-only file can have TDS and Stamp Duty empty in
           every row, so the values cannot tell it from an Equity file. The server checks
           that the TDS and Stamp Duty columns are there instead. */
        if (isMf) {
            setPending({ kind: "fa", file: f, fileType: faFileType, profileType });
            return;
        }
        setFaBusy(true);
        try {
            const preview = await api.previewHeaders(f, profileType);
            if (preview?.detectedType && preview.detectedType !== profileType) {
                setFaError({
                    message:
                        faFileType === api.FA_FILE_TYPE.MUTUAL_FUND
                            ? "Wrong file type — Mutual Funds selected, but TDS and Stamp Duty are empty in every row, so it is an Equity transaction file."
                            : "Wrong file type — this file has TDS / Stamp Duty amounts, so it is a Mutual Funds transaction file. Upload it on the Mutual Funds tab.",
                });
                return;
            }
        } catch (err) {
            setFaError({ message: err.message, detail: err.detail });
            return;
        } finally {
            setFaBusy(false);
        }
        setPending({ kind: "fa", file: f, fileType: faFileType, profileType });
    };

    const custodyZoneDisabled = addBusy || anyRunning;
    const faZoneDisabled = faBusy || anyRunning;

    /*
     * The date a card's file is for. Every row must carry it, and the file is
     * stored under it, so no file can be picked until it is chosen. Locked while
     * that card's file is waiting on its mapping or uploading, so a file cannot
     * be sent under one date while the card shows another.
     */
    const renderDateInput = (label, value, setValue, locked) => (
        <input
            type="date"
            className="up-select"
            aria-label={label}
            title={label}
            value={value}
            onChange={(e) => setValue(e.target.value)}
            disabled={locked}
        />
    );
    /* A file needs both dates: the upload date at the top (recorded as when it
       was uploaded) and its card's date (the day its rows are for). */
    /* Equity tab: a trade date past its working-day change window takes no files.
       The server refuses them too; this only says so before a file is picked. */
    const custodyClosed = lockedMessage(custodyDate);
    const faClosed = lockedMessage(faDate);
    /* The FA date's window, and what its dropdown shows: a preset, or Custom for any other number. */
    const faWindow = faDate ? windows[faDate] : null;
    // Before a date is picked (or while its window loads) the dropdown shows the default, disabled.
    const faWindowDays = faWindow ? faWindow.days : api.DEFAULT_WINDOW_DAYS;
    let faWindowChoice = "custom";
    if (!windowCustomOpen && api.WINDOW_PRESETS.includes(faWindowDays)) faWindowChoice = String(faWindowDays);
    const custodyDropDisabled = custodyZoneDisabled || !custodyDate || !uploadDate || Boolean(custodyClosed);
    // OPI/OPO: a file may hold many dates, so it needs only the upload date.
    const faDropDisabled = faZoneDisabled || (!faDate && !isOpiOpo) || !uploadDate || Boolean(faClosed);

    const zoneTitle = (busy, cardDate, readyText, closed = "") => {
        if (busy) return "Validating…";
        if (!uploadDate) return "Choose the upload date at the top first";
        if (!cardDate) return "Choose a date first";
        if (closed) return "This date is locked";
        return readyText;
    };

    /* "· uploaded 25-09-2026" on a file row, when the file table knows it. */
    const uploadedLabel = (f) => (f.uploadedOn ? ` · uploaded ${toDisplayDate(f.uploadedOn)}` : "");

    /* Equity tab: a file of a trade date past its change window can no longer be removed. */
    const closedFile = (f) => Boolean(lockedMessage(f.businessDate));

    /* ── render ───────────────────────────────────────────────────────── */

    return (
        <>
            <div className="txo-section-head">
                <h2>Files &amp; Validation</h2>
            </div>

            {/* The transaction file on the left, custody on the right: the two
                things Gate 1 needs, side by side rather than one below the other. */}
            <div className={faOnly ? "cm-split cm-split--single" : "cm-split"}>
              <div className="cm-col">
                <h3 className="cm-col-title">Fund Accounting</h3>

                <div className="temp-transaction-card cm-filecard up-card">
                    {/* Which transaction file this is. Mutual fund transactions
                        arrive in their own fund-accounting file and are read
                        through their own mapping. */}
                    <div className="up-controls">
                        <select
                            className="up-select"
                            value={faFileType}
                            onChange={(e) => {
                                setFaError(null);
                                setFaWarnings([]);
                                setFaFileType(e.target.value);
                            }}
                            disabled={faZoneDisabled}
                        >
                            {/* The Equity tab takes Equity files only, the Mutual Funds tab mutual fund files only. */}
                            {isOpiOpo ? (
                                <>
                                    <option value={api.FA_FILE_TYPE.OPI_OPO}>{api.FA_FILE_TYPE_LABEL.OPI_OPO}</option>
                                    {/* Not built yet: the mutual fund OPI/OPO file layout is not known. */}
                                    <option value="" disabled>
                                        Mutual Funds (coming later)
                                    </option>
                                </>
                            ) : null}
                            {faOnly ? null : (
                                <option value={api.FA_FILE_TYPE.REGULAR}>
                                    {api.FA_FILE_TYPE_LABEL.REGULAR}
                                </option>
                            )}
                            {reloadMode && !isMf ? null : (
                                <option value={api.FA_FILE_TYPE.MUTUAL_FUND}>
                                    {api.FA_FILE_TYPE_LABEL.MUTUAL_FUND}
                                </option>
                            )}
                        </select>

                        {/* OPI/OPO: a file may hold many trade dates, so there is no date to pick. */}
                        {isOpiOpo
                            ? null
                            : renderDateInput(
                                  "Date the transaction file is for",
                                  faDate,
                                  changeFaDate,
                                  faBusy || pending?.kind === "fa",
                              )}

                        {/* The date's change window: chosen until its first commit fixes it. OPI/OPO has none. */}
                        {reloadMode && !isOpiOpo && !faWindow?.fixed ? (
                            <>
                                <select
                                    className="up-select up-select--window"
                                    aria-label="How many working days after the trade date it can be re-uploaded and re-committed"
                                    title="How many working days after the trade date it can be re-uploaded and re-committed"
                                    value={faWindowChoice}
                                    disabled={!faWindow || faBusy || windowSaving}
                                    onChange={(e) => {
                                        if (e.target.value === "custom") {
                                            setWindowCustomText(String(faWindowDays));
                                            setWindowCustomOpen(true);
                                        } else {
                                            saveFaWindow(Number(e.target.value));
                                        }
                                    }}
                                >
                                    {api.WINDOW_PRESETS.map((n) => (
                                        <option key={n} value={n}>
                                            T+{n}
                                        </option>
                                    ))}
                                    <option value="custom">Custom…</option>
                                </select>
                                {windowCustomOpen ? (
                                    <span className="up-window-custom">
                                        T+
                                        <input
                                            type="number"
                                            className="up-select up-window-days"
                                            aria-label="Custom number of working days"
                                            min={1}
                                            max={api.MAX_WINDOW_DAYS}
                                            step={1}
                                            value={windowCustomText}
                                            disabled={windowSaving}
                                            onChange={(e) => setWindowCustomText(e.target.value)}
                                            onKeyDown={(e) => {
                                                if (e.key === "Enter") saveFaWindow(Number(windowCustomText));
                                            }}
                                        />
                                        <button
                                            type="button"
                                            className="cm-secondary"
                                            disabled={windowSaving}
                                            onClick={() => saveFaWindow(Number(windowCustomText))}
                                        >
                                            Save
                                        </button>
                                    </span>
                                ) : null}
                            </>
                        ) : null}
                    </div>

                    {reloadMode && faWindow && faWindow.fixed && !faWindow.locked ? (
                        <p className="up-window-note">{faWindow.message}</p>
                    ) : null}

                    <div
                        className={faDragActive ? "up-zone is-drag" : "up-zone"}
                        onDragEnter={(e) => {
                            e.preventDefault();
                            if (!faDropDisabled) setFaDragActive(true);
                        }}
                        onDragOver={(e) => e.preventDefault()}
                        onDragLeave={() => setFaDragActive(false)}
                        onDrop={(e) => {
                            e.preventDefault();
                            setFaDragActive(false);
                            if (faDropDisabled) return;
                            const f = e.dataTransfer.files && e.dataTransfer.files[0];
                            if (f) handleFaFilePicked(f);
                        }}
                    >
                        <input
                            ref={faInputRef}
                            type="file"
                            accept=".csv"
                            className="up-zone-input"
                            aria-label="Upload transaction file"
                            disabled={faDropDisabled}
                            onChange={(e) => {
                                const f = e.target.files[0];
                                e.target.value = "";
                                handleFaFilePicked(f);
                            }}
                        />
                        <div className="up-zone-visual">
                            <UploadGlyph />
                            <span className="up-zone-title">
                                {zoneTitle(faBusy, isOpiOpo ? uploadDate : faDate, "Upload transaction file", faClosed)}
                            </span>
                            <span className="up-zone-sub">
                                Drag &amp; drop or <span className="up-zone-browse">Browse</span>
                            </span>
                        </div>
                    </div>

                    {faClosed ? <LockedNotice message={faClosed} /> : null}

                    {faError ? <UploadErrorDetails message={faError.message} detail={faError.detail} /> : null}
                    <HeaderWarnings warnings={faWarnings} />

                    {/* Shown only when the file repeats trades already in our books, or
                        removes many of them. The upload itself works the same either way. */}
                    {faResult && (faResult.comparison.counts.duplicate > 0 || faResult.bigRemoval) ? (
                        <div className="cm-reload-result" role="status">
                            {faResult.comparison.counts.duplicate > 0 ? (
                                <DuplicatesNotice comparison={faResult.comparison} />
                            ) : null}
                            {faResult.bigRemoval ? (
                                <p className="cm-unreconciled">
                                    <strong>Check this file.</strong> It removes{" "}
                                    {faResult.comparison.counts.removed.toLocaleString()} of the date's{" "}
                                    {faResult.comparison.totalBefore.toLocaleString()} trades. It may be a wrong or half file.
                                </p>
                            ) : null}
                        </div>
                    ) : null}

                    {faFiles.length > 0 ? (
                        <div className="up-filelist-wrap">
                            <div className="up-filelist-head">Uploaded file</div>
                            <div className="up-filelist">
                                {faFiles.map((f) => {
                                    const inBooks = pendingFaKeys !== null && !pendingFaKeys.has(f.key);
                                    return (
                                        <div className="up-file-row" key={f.key}>
                                            <FileGlyph />
                                            <div className="up-file-main">
                                                <span className="up-file-name" title={f.originalFileName}>
                                                    {f.originalFileName}
                                                </span>
                                                <span className="up-file-meta">
                                                    {api.FA_FILE_TYPE_LABEL[f.fileType] || f.fileType} ·{" "}
                                                    {faDateLabel(f)} · {api.formatBytes(f.sizeBytes)}
                                                    {uploadedLabel(f)}
                                                </span>
                                            </div>
                                            {removingKey === f.key ? (
                                                <StatusBadge tone={STATUS.PROCESSING}>Removing…</StatusBadge>
                                            ) : closedFile(f) ? (
                                                <StatusBadge tone={STATUS.NEUTRAL}>Locked</StatusBadge>
                                            ) : inBooks ? null : (
                                                <StatusBadge tone={STATUS.OK}>Ready</StatusBadge>
                                            )}
                                            {/* OPI/OPO: a committed file stays, so it can never be uploaded again. */}
                                            {closedFile(f) || (isOpiOpo && inBooks) ? null : (
                                                <button
                                                    type="button"
                                                    className="up-file-remove"
                                                    onClick={() => removeFaFile(f, inBooks)}
                                                    disabled={faBusy || anyRunning || Boolean(removingKey)}
                                                    aria-label={`Remove ${f.originalFileName}`}
                                                >
                                                    &times;
                                                </button>
                                            )}
                                        </div>
                                    );
                                })}
                            </div>
                        </div>
                    ) : null}
                </div>
              </div>

              {/* Mutual Funds and OPI/OPO: no custodian file. */}
              {faOnly ? null : (
              <div className="cm-col">
                <h3 className="cm-col-title">Custodian Files</h3>

                <div className="temp-transaction-card cm-filecard up-card">
                    <div className="up-controls">
                        {typingScheme ? (
                            <input
                                className="up-select up-select--text"
                                type="text"
                                placeholder="New scheme code"
                                value={draft.scheme}
                                onChange={(e) => {
                                    setAddError(null);
                                    setDraft((d) => ({ ...d, scheme: e.target.value }));
                                }}
                                disabled={custodyZoneDisabled}
                            />
                        ) : (
                            <select
                                className="up-select"
                                value={draft.scheme}
                                onChange={(e) => pickScheme(e.target.value)}
                                disabled={custodyZoneDisabled}
                            >
                                <option value="">Scheme…</option>
                                {schemeOptions.map((s) => (
                                    <option key={s.scheme} value={s.scheme}>
                                        {s.scheme}
                                    </option>
                                ))}
                                <option value={NEW_SCHEME}>+ New scheme…</option>
                            </select>
                        )}

                        {draft.newScheme ? (
                            <button
                                type="button"
                                className="cm-scheme-back"
                                onClick={() => pickScheme("")}
                                disabled={custodyZoneDisabled}
                            >
                                choose from list
                            </button>
                        ) : null}

                        <select
                            className="up-select"
                            value={draft.fileType}
                            onChange={(e) => {
                                setAddError(null);
                                setAddWarnings([]);
                                setDraft((d) => ({ ...d, fileType: e.target.value }));
                            }}
                            disabled={custodyZoneDisabled}
                        >
                            <option value={api.CUSTODY_FILE_TYPE.RESIDENTIAL}>
                                {api.FILE_TYPE_LABEL.RESIDENTIAL}
                            </option>
                            <option value={api.CUSTODY_FILE_TYPE.NDPMS}>
                                {api.FILE_TYPE_LABEL.NDPMS}
                            </option>
                            {/* Mutual funds are not matched any more; see the Mutual Funds tab.
                            <option value={api.CUSTODY_FILE_TYPE.MUTUAL_FUND}>
                                {api.FILE_TYPE_LABEL.MUTUAL_FUND}
                            </option>
                            */}
                        </select>

                        {renderDateInput(
                            "Date the custodian file is for",
                            custodyDate,
                            changeCustodyDate,
                            addBusy || pending?.kind === "custody",
                        )}
                    </div>

                    <div
                        className={custodyDragActive ? "up-zone is-drag" : "up-zone"}
                        onDragEnter={(e) => {
                            e.preventDefault();
                            if (!custodyDropDisabled) setCustodyDragActive(true);
                        }}
                        onDragOver={(e) => e.preventDefault()}
                        onDragLeave={() => setCustodyDragActive(false)}
                        onDrop={(e) => {
                            e.preventDefault();
                            setCustodyDragActive(false);
                            if (custodyDropDisabled) return;
                            const f = e.dataTransfer.files && e.dataTransfer.files[0];
                            if (f) handleCustodyFilePicked(f);
                        }}
                    >
                        <input
                            type="file"
                            accept=".csv"
                            className="up-zone-input"
                            aria-label="Upload custodian file"
                            disabled={custodyDropDisabled}
                            onChange={(e) => {
                                const f = e.target.files[0];
                                e.target.value = "";
                                handleCustodyFilePicked(f);
                            }}
                        />
                        <div className="up-zone-visual">
                            <UploadGlyph />
                            <span className="up-zone-title">
                                {zoneTitle(addBusy, custodyDate, "Upload custodian file", custodyClosed)}
                            </span>
                            <span className="up-zone-sub">
                                Drag &amp; drop or <span className="up-zone-browse">Browse</span>
                            </span>
                        </div>
                    </div>

                    {custodyClosed ? <LockedNotice message={custodyClosed} /> : null}

                    {addError ? (
                        <UploadErrorDetails message={addError.message} detail={addError.detail} />
                    ) : null}
                    <HeaderWarnings warnings={addWarnings} />

                    {/* A stored file shows in the list below, so it needs no message. Only a
                        file that was not stored, or a Replace whose clean-up failed, says so. */}
                    {custodyResult && (!custodyResult.stored || custodyResult.cleanupError) ? (
                        <div className="cm-reload-result" role="status">
                            {!custodyResult.stored ? (
                                <p>
                                    <strong>{api.CUSTODY_MODE_LABEL[custodyResult.mode] || custodyResult.mode}</strong> —{" "}
                                    {custodyResult.fileName} for {custodyResult.scheme} on {custodyResult.businessDate}. Nothing
                                    was stored. To drop some lines, remove the stored file and upload the right one.
                                </p>
                            ) : null}
                            {custodyResult.cleanupError ? (
                                <p className="cm-unreconciled">{custodyResult.cleanupError}</p>
                            ) : null}
                        </div>
                    ) : null}

                    {/* One row per uploaded file. Two files of the same type for the
                        same scheme are a duplicate only when they cover the same
                        dates; anything else is another row. */}
                    {storedFiles.length > 0 ? (
                        <div className="up-filelist-wrap">
                            <div className="up-filelist-head">Custodian files</div>
                            <div className="up-filelist">
                                {storedFiles.map((f) => (
                                    <div className="up-file-row" key={f.key}>
                                        <FileGlyph />
                                        <div className="up-file-main">
                                            <span className="up-file-name" title={f.originalFileName}>
                                                {f.originalFileName}
                                            </span>
                                            <span className="up-file-meta">
                                                {f.scheme} · {api.FILE_TYPE_LABEL[f.fileType] || f.fileType} ·{" "}
                                                {rangeLabel(f)} · {api.formatBytes(f.sizeBytes)}
                                                {uploadedLabel(f)}
                                            </span>
                                        </div>
                                        {removingKey === f.key ? (
                                            <StatusBadge tone={STATUS.PROCESSING}>Removing…</StatusBadge>
                                        ) : closedFile(f) ? (
                                            <StatusBadge tone={STATUS.NEUTRAL}>Locked</StatusBadge>
                                        ) : (
                                            <StatusBadge tone={STATUS.OK}>Ready</StatusBadge>
                                        )}
                                        {closedFile(f) ? null : (
                                            <button
                                                type="button"
                                                className="up-file-remove"
                                                onClick={() => removeCustodyFile(f)}
                                                disabled={anyRunning || Boolean(removingKey)}
                                                aria-label={`Remove ${f.originalFileName}`}
                                            >
                                                &times;
                                            </button>
                                        )}
                                    </div>
                                ))}
                            </div>
                        </div>
                    ) : null}
                </div>
              </div>
              )}
            </div>

            {/* The Mutual Funds and OPI/OPO tabs have no custodian file, so nothing is matched. */}
            {faOnly ? null : (
            <>
            <div className="txo-section-head txo-section-head--tight">
                <h2>Reconciliation</h2>
            </div>

            <div className="temp-transaction-card cm-gate">
                <div className="cm-gate-summary">
                    <div className="cm-gate-fact">
                        <span className="cm-gate-fact-label">Ready to match</span>
                        <span className="cm-gate-fact-value">
                            {readyDateCount} date{readyDateCount === 1 ? "" : "s"}
                        </span>
                    </div>
                    <div className="cm-gate-fact">
                        <span className="cm-gate-fact-label">Waiting for a file</span>
                        <span className="cm-gate-fact-value">
                            {waitingDateCount} date{waitingDateCount === 1 ? "" : "s"}
                        </span>
                    </div>
                    {closedDates.length > 0 ? (
                        <div className="cm-gate-fact">
                            <span className="cm-gate-fact-label">Locked</span>
                            <span className="cm-gate-fact-value">
                                {closedDates.length} date{closedDates.length === 1 ? "" : "s"}
                            </span>
                        </div>
                    ) : null}
                </div>

                <div className="cm-gate-row">
                    <button
                        type="button"
                        className="cm-primary"
                        onClick={handleMatch}
                        disabled={!gateOneOpen}
                    >
                        {anyRunning || starting ? "Matching…" : "Run FA vs Custody Matching"}
                    </button>

                    {/* One short sentence, not the raw blocker list — a greyed-out
                        button with no explanation is the thing users complain about,
                        but a paragraph of reasons is just as unhelpful in a hurry. */}
                    {blockerMessage && !anyRunning ? (
                        <p className="cm-blockers">{blockerMessage}</p>
                    ) : null}
                </div>

                {/* One block per date shown, oldest first. */}
                {runs.map((run) => (
                    <div key={run.runId} className={`cm-progress ${run.status.toLowerCase()}`}>
                        {/* One row: date, status and counts. The run ID shows only under a failure's details. */}
                        <div className="cm-progress-head cm-run-head">
                            <div className="cm-run-summary">
                                <strong className="cm-run-date mono">{run.businessDate}</strong>
                                <span className="cm-run-status">
                                    {run.isRunning ? (
                                        <StatusBadge tone={STATUS.PROCESSING}>{run.stageDetail || run.status}</StatusBadge>
                                    ) : run.status === "COMPLETED_CLEAN" ? (
                                        <StatusBadge tone={STATUS.OK}>Matched</StatusBadge>
                                    ) : run.status === "COMPLETED_WITH_EXCEPTIONS" ? null : run.status === "FAILED" ? (
                                        <StatusBadge tone={STATUS.BAD}>Failed</StatusBadge>
                                    ) : (
                                        <StatusBadge tone={STATUS.NEUTRAL}>{run.status}</StatusBadge>
                                    )}
                                </span>
                                <div className="cm-stats">
                                    <span><strong className="mono">{run.counts.faRows.toLocaleString()}</strong> FA rows</span>
                                    <span><strong className="mono">{run.counts.custodyRows.toLocaleString()}</strong> custody rows</span>
                                    <span><strong className="mono">{run.counts.resolved.toLocaleString()}</strong> resolved to actual codes</span>
                                    {run.counts.unmapped > 0 ? (
                                        <span className="warn"><strong className="mono">{run.counts.unmapped}</strong> unmapped</span>
                                    ) : null}
                                    {run.counts.totalKeys > 0 ? (
                                        <span>
                                            <strong className="mono">
                                                {run.counts.comparedKeys.toLocaleString()} / {run.counts.totalKeys.toLocaleString()}
                                            </strong>{" "}
                                            keys compared
                                        </span>
                                    ) : null}
                                </div>
                            </div>
                        </div>

                        {run.isRunning ? (
                            <div className="cm-bar">
                                <div className="cm-bar-fill" style={{ width: `${run.progressPercent}%` }} />
                            </div>
                        ) : null}

                        {run.status === "COMPLETED_CLEAN" ? (
                            <div className="cm-verdict ok">
                                <p className="cm-verdict-lead">
                                    Matched — no differences between fund accounting and custody.
                                    The transaction file is ready to commit.
                                </p>
                            </div>
                        ) : null}

                        {run.status === "FAILED" ? (
                            <div className="cm-verdict bad">
                                <p className="cm-verdict-lead">
                                    {FRIENDLY_RUN_ERROR[run.error?.code] ||
                                        run.error?.message ||
                                        "Matching failed. Try again, or view details."}
                                </p>
                                <div className="cm-verdict-actions">
                                    <button type="button" className="cm-secondary" onClick={() => handleRetry(run)}>
                                        Retry
                                    </button>
                                    <button
                                        type="button"
                                        className="cm-linkbtn"
                                        onClick={() =>
                                            setFailDetailsRunId((id) => (id === run.runId ? "" : run.runId))
                                        }
                                    >
                                        {failDetailsRunId === run.runId ? "Hide details" : "View details"}
                                    </button>
                                </div>
                                {failDetailsRunId === run.runId ? (
                                    <p className="cm-technical-ref">
                                        {run.error?.message ? <span>{run.error.message}<br /></span> : null}
                                        Stage {run.stage} · <span className="mono">{run.runId}</span>
                                        {run.error?.code ? <span className="mono"> · {run.error.code}</span> : null}
                                    </p>
                                ) : null}
                            </div>
                        ) : null}
                    </div>
                ))}

                {reusedNote ? <div className="cm-reused" role="status">{reusedNote}</div> : null}

                {runError ? <UploadErrorDetails message={runError.message} detail={runError.detail} /> : null}
                {dayLoading && runs.length === 0 ? <p className="cm-sub">Loading…</p> : null}
            </div>
            </>
            )}

            {pickerOpen ? (
                <MatchDialog
                    dates={uploadDayDates}
                    loading={pickerLoading}
                    error={pickerError}
                    selected={selectedDates}
                    onToggle={toggleDate}
                    onAll={() =>
                        setSelectedDates(new Set(readyDates.map((d) => d.date)))
                    }
                    onNone={() => setSelectedDates(new Set())}
                    onRun={() => startMatch([...selectedDates].sort())}
                    onClose={() => setPickerOpen(false)}
                />
            ) : null}

            {ask ? (
                <AskModeDialog ask={ask} busy={faBusy} onChoose={chooseAskOption} onClose={() => setAsk(null)} />
            ) : null}

            {custodyAsk ? (
                <ReplaceCustodyDialog
                    ask={custodyAsk}
                    busy={addBusy}
                    onReplace={replaceCustodyFile}
                    onClose={() => setCustodyAsk(null)}
                />
            ) : null}

            {/* The gate. A file picked above sits here until every field has
                been answered; closing without uploading simply drops it.
                onSaved refreshes the day in place rather than remounting this
                panel, which would take the modal down mid-upload. */}
            <FieldMappingPanel
                open={Boolean(pending)}
                file={pending?.file || null}
                requestedType={pending?.profileType || null}
                // The Mutual Funds tab maps mutual fund files only, the OPI/OPO tab OPI/OPO files only.
                {...(isMf ? { types: [api.PROFILE_TYPE.FA_MUTUAL_FUND] } : {})}
                {...(isOpiOpo ? { types: [api.PROFILE_TYPE.FA_OPI_OPO] } : {})}
                pickedDate={faOnly ? faDate : custodyDate}
                onUpload={uploadPending}
                onClose={() => setPending(null)}
                onSaved={loadDay}
                showHints={false}
            />
        </>
    );
};

export default CustodyMatchPanel;
