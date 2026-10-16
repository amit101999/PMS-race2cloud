import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";

import FieldMappingPanel from "./FieldMappingPanel";
import UploadErrorDetails from "./UploadErrorDetails";
import StatusBadge, { STATUS } from "./StatusBadge";
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
};

const FA_FILE_TYPE_FOR_PROFILE = {
    [api.PROFILE_TYPE.FA]: api.FA_FILE_TYPE.REGULAR,
    [api.PROFILE_TYPE.FA_MUTUAL_FUND]: api.FA_FILE_TYPE.MUTUAL_FUND,
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
    if (d.faFiles.length === 0 && d.custodyFiles.length === 0) return "No files are present for this date.";
    if (d.custodyFiles.length === 0) return "Custodian file is not present for this date.";
    if (d.faFiles.length === 0) return "FA file is not present for this date.";
    return "";
};

/** "2026-09-25" -> "25-09-2026", the way dates are read on this screen. */
const toDisplayDate = (iso) => (iso ? iso.split("-").reverse().join("-") : "");

/** The "Last uploaded" line under an upload zone. */
const LastUpload = ({ date }) => (
    <p className="cm-muted cm-last-upload">
        {date ? `Last uploaded: ${toDisplayDate(date)}` : "No files uploaded yet"}
    </p>
);

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
                                                FA {d.faFiles.length > 0 ? `✓ (${d.faFiles.length} file(s))` : "✗"} ·
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
}) => {
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

    const [run, setRun] = useState(null);
    const [runError, setRunError] = useState(null);
    const [starting, setStarting] = useState(false);
    /* Set when Match handed back a report that already existed. */
    const [reusedNote, setReusedNote] = useState("");
    const [showFailDetails, setShowFailDetails] = useState(false);

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
    const [faFileType, setFaFileType] = useState(api.FA_FILE_TYPE.REGULAR);

    /* Purely visual — whether a file is currently being dragged over each
       drop zone. Never affects what gets uploaded or how. */
    const [custodyDragActive, setCustodyDragActive] = useState(false);
    const [faDragActive, setFaDragActive] = useState(false);

    const faInputRef = useRef(null);
    const pollRef = useRef(null);

    /* ── loading a date ───────────────────────────────────────────────── */

    /* The file date whose match result is shown. Set only when the user starts
       a match (the first ticked date), so picking an upload date never shows an
       earlier result on its own. */
    const [resultDate, setResultDate] = useState("");

    // A new upload date starts with no result on screen.
    useEffect(() => {
        setResultDate("");
    }, [uploadDate]);

    const loadDay = useCallback(async () => {
        // No date chosen (the screen starts that way, or it was cleared): there
        // is nothing to show, and the previous date's files must not linger.
        if (!resultDate || !uploadDate) {
            setDay(null);
            setRun(null);
            return;
        }
        setDayLoading(true);
        try {
            // Runs belong to an upload date: the result for this file date under it.
            const res = await api.getLatestRun(resultDate, uploadDate);
            setDay(res.day);
            // A superseded run was matched against files or a mapping that have
            // since changed, so its result no longer describes this date. Treat
            // the date as not yet matched rather than showing a stale result.
            setRun(res.run?.status === "SUPERSEDED" ? null : res.run);
            setRunError(null);
        } catch (err) {
            setRunError({ message: err.message, detail: err.detail });
        } finally {
            setDayLoading(false);
        }
    }, [resultDate, uploadDate]);

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
        if (onRunChange) onRunChange(run);
    }, [run, onRunChange]);

    /* ── polling ──────────────────────────────────────────────────────── */

    const runId = run?.runId;
    const runIsRunning = run?.isRunning;

    useEffect(() => {
        // Poll only while the job is actually moving, and always clear on unmount
        // so a navigation away does not leave a timer calling into a dead screen.
        if (!runId || !runIsRunning) {
            if (pollRef.current) {
                clearInterval(pollRef.current);
                pollRef.current = null;
            }
            return undefined;
        }

        pollRef.current = setInterval(async () => {
            try {
                const res = await api.getRun(runId);
                // A file or mapping changed mid-run: same rule as loadDay, hide the stale run.
                setRun(res.run?.status === "SUPERSEDED" ? null : res.run);
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
    }, [runId, runIsRunning]);

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
        () => (faDay?.fa?.raw || []).filter((f) => !removedKeys.has(f.key)),
        [faDay, removedKeys],
    );

    /*
     * The file dates with files saved under the upload date picked at the top.
     * Each upload date is its own set: a file date is ready only when BOTH an FA
     * and a custodian file were saved under this upload date.
     */
    const uploadDayDates = uploadDate
        ? matchDates.filter((d) => d.uploadDate === uploadDate)
        : [];
    const readyDates = uploadDayDates.filter((d) => d.canMatch);
    const readyDateCount = readyDates.length;
    const waitingDateCount = uploadDayDates.length - readyDateCount;

    /*
     * The most recent day a file on each side was uploaded, from the file table.
     * Shown on each card whatever its date box says, so saved files are visible
     * even before a date is picked.
     */
    const latestUploadedOn = (filesKey) => {
        let latest = "";
        for (const d of matchDates) {
            if (d[filesKey].length > 0 && d.uploadDate > latest) latest = d.uploadDate;
        }
        return latest;
    };
    const custodyLastUpload = latestUploadedOn("custodyFiles");
    const faLastUpload = latestUploadedOn("faFiles");

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
        try {
            const res = await api.uploadCustodyFile({
                file,
                scheme,
                fileType,
                businessDate: custodyDate,
                uploadDate,
                replace,
            });
            setAddWarnings(res.warnings || []);
            reportJob({
                id: `txn-custody-upload-${Date.now()}`,
                label: `Custodian file uploaded — ${file.name}`,
                status: "completed",
                message: `${scheme} · ${api.FILE_TYPE_LABEL[fileType] || fileType} is ready for matching.`,
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
    const removeFile = async (file, what, onError) => {
        if (
            !window.confirm(
                `Remove ${what} from this run?

` +
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
    const removeFaFile = (file) =>
        removeFile(file, file.originalFileName, (error) => {
            setFaWarnings([]);
            setFaError(error);
        });

    const changeFaDate = (date) => {
        setFaWarnings([]);
        setFaDate(date);
    };

    /** Same contract as uploadCustody: the mapping panel owns the errors. */
    const uploadFa = async (file, { fileType, replace = false }) => {
        setFaBusy(true);
        setFaError(null);
        setFaWarnings([]);
        try {
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
                label: `Transaction file uploaded — ${file.name}`,
                status: "completed",
                message: `${api.FA_FILE_TYPE_LABEL[fileType] || fileType} file is ready for matching.`,
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

    /* ─── gate 1 ───────── */

    /* Enabled only when an upload date is picked and a file uploaded that day
       leaves some date with both an FA and a custodian file. The date list is
       reloaded after every upload and remove, so this follows them. */
    const gateOneOpen = readyDateCount > 0 && !run?.isRunning && !starting;

    let blockerMessage = "";
    if (!uploadDate) {
        blockerMessage = "Choose the upload date at the top to match the files uploaded that day.";
    } else if (uploadDayDates.length === 0) {
        blockerMessage = `No files were uploaded on ${toDisplayDate(uploadDate)}.`;
    } else if (readyDateCount === 0) {
        blockerMessage = `No date uploaded on ${toDisplayDate(uploadDate)} has both an FA and a custodian file yet.`;
    }

    /**
     * One match per ticked date, started one after another. Each run is tracked
     * in the bell; the screen then shows the first date's result. Reports with
     * differences are also listed under Previous reports.
     */
    const startMatch = async (dates) => {
        setPickerOpen(false);
        setStarting(true);
        setRunError(null);
        setReusedNote("");

        const failures = [];
        const reused = [];
        let firstStarted = null;

        for (const date of dates) {
            try {
                const res = await api.startRun(date, uploadDate);
                if (!firstStarted) firstStarted = { date, runId: res.runId };
                if (res.reused) {
                    /* Nothing was re-run: the same files can only say the same thing. */
                    reused.push(date);
                    reportJob({
                        id: `txn-match-reused-${date}-${Date.now()}`,
                        label: `FA vs Custody matching — ${date}`,
                        status: "completed",
                        message: res.message || "Already matched; showing that report.",
                    });
                } else {
                    // The runId is the bell's key: the server settles it from the run's own status.
                    trackJob({
                        id: res.runId,
                        label: `FA vs Custody matching — ${date}`,
                        message: "Matching is running in the background.",
                    });
                }
            } catch (err) {
                failures.push(`${date}: ${err.message}`);
            }
        }

        try {
            if (firstStarted) {
                const status = await api.getRun(firstStarted.runId);
                setRun(status.run);
                setResultDate(firstStarted.date);
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

    const handleRetry = async () => {
        try {
            await api.retryRun(run.runId);
            trackJob({
                id: run.runId,
                label: `FA vs Custody matching — ${run.businessDate}`,
                message: "Retrying the match in the background.",
            });
            const status = await api.getRun(run.runId);
            setRun(status.run);
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
        setFaBusy(true);
        try {
            const preview = await api.previewHeaders(f, profileType);
            if (preview?.detectedType && preview.detectedType !== profileType) {
                setFaError({
                    message:
                        faFileType === api.FA_FILE_TYPE.MUTUAL_FUND
                            ? "Wrong file type — Mutual Funds selected, but TDS and Stamp Duty are empty in every row, so it is an Equity transaction file."
                            : "Wrong file type — Equity selected, but this file has TDS / Stamp Duty amounts, so it is a Mutual Funds transaction file.",
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

    const custodyZoneDisabled = addBusy || Boolean(run?.isRunning);
    const faZoneDisabled = faBusy || Boolean(run?.isRunning);

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
    const custodyDropDisabled = custodyZoneDisabled || !custodyDate || !uploadDate;
    const faDropDisabled = faZoneDisabled || !faDate || !uploadDate;

    const zoneTitle = (busy, cardDate, readyText) => {
        if (busy) return "Validating…";
        if (!uploadDate) return "Choose the upload date at the top first";
        if (!cardDate) return "Choose a date first";
        return readyText;
    };

    /* "· uploaded 25-09-2026" on a file row, when the file table knows it. */
    const uploadedLabel = (f) => (f.uploadedOn ? ` · uploaded ${toDisplayDate(f.uploadedOn)}` : "");

    /* ── render ───────────────────────────────────────────────────────── */

    return (
        <>
            <div className="txo-section-head">
                <h2>Files &amp; Validation</h2>
            </div>

            {/* Custody on the left, the transaction file on the right: the two
                things Gate 1 needs, side by side rather than one below the other. */}
            <div className="cm-split">
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
                            <option value={api.CUSTODY_FILE_TYPE.MUTUAL_FUND}>
                                {api.FILE_TYPE_LABEL.MUTUAL_FUND}
                            </option>
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
                                {zoneTitle(addBusy, custodyDate, "Upload custodian file")}
                            </span>
                            <span className="up-zone-sub">
                                Drag &amp; drop or <span className="up-zone-browse">Browse</span>
                            </span>
                        </div>
                    </div>

                    <LastUpload date={custodyLastUpload} />

                    {addError ? (
                        <UploadErrorDetails message={addError.message} detail={addError.detail} />
                    ) : null}
                    <HeaderWarnings warnings={addWarnings} />

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
                                        ) : (
                                            <StatusBadge tone={STATUS.OK}>Ready</StatusBadge>
                                        )}
                                        <button
                                            type="button"
                                            className="up-file-remove"
                                            onClick={() => removeCustodyFile(f)}
                                            disabled={run?.isRunning || Boolean(removingKey)}
                                            aria-label={`Remove ${f.originalFileName}`}
                                        >
                                            &times;
                                        </button>
                                    </div>
                                ))}
                            </div>
                        </div>
                    ) : null}
                </div>
              </div>

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
                            <option value={api.FA_FILE_TYPE.REGULAR}>
                                {api.FA_FILE_TYPE_LABEL.REGULAR}
                            </option>
                            <option value={api.FA_FILE_TYPE.MUTUAL_FUND}>
                                {api.FA_FILE_TYPE_LABEL.MUTUAL_FUND}
                            </option>
                        </select>

                        {renderDateInput(
                            "Date the transaction file is for",
                            faDate,
                            changeFaDate,
                            faBusy || pending?.kind === "fa",
                        )}
                    </div>

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
                                {zoneTitle(faBusy, faDate, "Upload transaction file")}
                            </span>
                            <span className="up-zone-sub">
                                Drag &amp; drop or <span className="up-zone-browse">Browse</span>
                            </span>
                        </div>
                    </div>

                    <LastUpload date={faLastUpload} />

                    {faError ? <UploadErrorDetails message={faError.message} detail={faError.detail} /> : null}
                    <HeaderWarnings warnings={faWarnings} />

                    {faFiles.length > 0 ? (
                        <div className="up-filelist-wrap">
                            <div className="up-filelist-head">Uploaded file</div>
                            <div className="up-filelist">
                                {faFiles.map((f) => (
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
                                        ) : (
                                            <StatusBadge tone={STATUS.OK}>Ready</StatusBadge>
                                        )}
                                        <button
                                            type="button"
                                            className="up-file-remove"
                                            onClick={() => removeFaFile(f)}
                                            disabled={faBusy || run?.isRunning || Boolean(removingKey)}
                                            aria-label={`Remove ${f.originalFileName}`}
                                        >
                                            &times;
                                        </button>
                                    </div>
                                ))}
                            </div>
                        </div>
                    ) : null}
                </div>
              </div>
            </div>

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
                </div>

                <div className="cm-gate-row">
                    <button
                        type="button"
                        className="cm-primary"
                        onClick={handleMatch}
                        disabled={!gateOneOpen}
                    >
                        {run?.isRunning || starting ? "Matching…" : "Run FA vs Custody Matching"}
                    </button>

                    {/* One short sentence, not the raw blocker list — a greyed-out
                        button with no explanation is the thing users complain about,
                        but a paragraph of reasons is just as unhelpful in a hurry. */}
                    {blockerMessage && !run?.isRunning ? (
                        <p className="cm-blockers">{blockerMessage}</p>
                    ) : null}
                </div>

                {run ? (
                    <div className={`cm-progress ${run.status.toLowerCase()}`}>
                        {/* One row: status and counts on the left, the run ID badge on the right. */}
                        <div className="cm-progress-head cm-run-head">
                            <div className="cm-run-summary">
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
                            <span className="cm-run-badge mono" title="Run ID">{run.runId}</span>
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
                                    <button type="button" className="cm-secondary" onClick={handleRetry}>
                                        Retry
                                    </button>
                                    <button
                                        type="button"
                                        className="cm-linkbtn"
                                        onClick={() => setShowFailDetails((v) => !v)}
                                    >
                                        {showFailDetails ? "Hide details" : "View details"}
                                    </button>
                                </div>
                                {showFailDetails ? (
                                    <p className="cm-technical-ref">
                                        {run.error?.message ? <span>{run.error.message}<br /></span> : null}
                                        Stage {run.stage} · <span className="mono">{run.runId}</span>
                                        {run.error?.code ? <span className="mono"> · {run.error.code}</span> : null}
                                    </p>
                                ) : null}
                            </div>
                        ) : null}
                    </div>
                ) : null}

                {reusedNote ? <div className="cm-reused" role="status">{reusedNote}</div> : null}

                {runError ? <UploadErrorDetails message={runError.message} detail={runError.detail} /> : null}
                {dayLoading && !run ? <p className="cm-sub">Loading…</p> : null}
            </div>

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

            {/* The gate. A file picked above sits here until every field has
                been answered; closing without uploading simply drops it.
                onSaved refreshes the day in place rather than remounting this
                panel, which would take the modal down mid-upload. */}
            <FieldMappingPanel
                open={Boolean(pending)}
                file={pending?.file || null}
                requestedType={pending?.profileType || null}
                pickedDate={custodyDate}
                onUpload={uploadPending}
                onClose={() => setPending(null)}
                onSaved={loadDay}
                showHints={false}
            />
        </>
    );
};

export default CustodyMatchPanel;
