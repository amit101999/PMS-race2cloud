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

/**
 * Which dates to compare.
 *
 * Only shown when the files actually cover more than one day — with a single
 * date there is nothing to choose, and a dialog asking a question with one
 * answer is just a step. Every date starts selected, so the default is the same
 * run you would have got before.
 */
const DatePicker = ({ dates, loading, error, selected, onToggle, onAll, onNone, onRun, onClose }) => {
    const chosen = dates.filter((d) => selected.has(d.date));
    const faRows = chosen.reduce((n, d) => n + d.faRows, 0);
    const custodyRows = chosen.reduce((n, d) => n + (d.custodyRows || 0), 0);

    return (
        <>
            <div className="cm-scrim" onClick={onClose} />
            <div className="cm-dialog" role="dialog" aria-label="Choose dates to compare">
                <h3>Which dates should this run compare?</h3>

                {loading ? <p className="cm-muted">Reading the files…</p> : null}
                {error ? <div className="cm-dialog-error">{error}</div> : null}

                {!loading && !error ? (
                    <>
                        <div className="cm-dialog-actions">
                            <button type="button" className="cm-linkbtn" onClick={onAll}>
                                Select all
                            </button>
                            <button type="button" className="cm-linkbtn" onClick={onNone}>
                                Clear
                            </button>
                        </div>

                        <ul className="cm-datelist">
                            {dates.map((d) => (
                                <li key={d.date}>
                                    <label>
                                        <input
                                            type="checkbox"
                                            checked={selected.has(d.date)}
                                            onChange={() => onToggle(d.date)}
                                        />
                                        <span className="cm-date-label">{d.date}</span>
                                        <span className="cm-date-counts">
                                            {d.faRows.toLocaleString()} FA row(s) ·{" "}
                                            {(d.custodyRows || 0).toLocaleString()} custody row(s)
                                            {" "}in {d.custodyFiles} file(s)
                                        </span>
                                    </label>
                                    {d.custodyFiles === 0 ? (
                                        <span className="cm-date-note">
                                            nothing in the custodian files for this day —
                                            everything on this day will report as missing in
                                            custody
                                        </span>
                                    ) : null}
                                    {d.faRows === 0 ? (
                                        <span className="cm-date-note">
                                            nothing in the transaction file for this day
                                        </span>
                                    ) : null}
                                </li>
                            ))}
                        </ul>

                        <p className="cm-dialog-summary">
                            {chosen.length === 0
                                ? "Pick at least one date."
                                : `${chosen.length} date(s) — ${faRows.toLocaleString()} FA row(s), ${custodyRows.toLocaleString()} custody row(s).`}
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
    businessDate,
    onBusinessDateChange,
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

    /* The date chooser, opened by Match when the files span more than one day. */
    const [pickerOpen, setPickerOpen] = useState(false);
    const [pickerDates, setPickerDates] = useState([]);
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

    const loadDay = useCallback(async () => {
        if (!businessDate) return;
        setDayLoading(true);
        try {
            const res = await api.getLatestRun(businessDate);
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
    }, [businessDate]);

    useEffect(() => {
        loadDay();
    }, [loadDay]);

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

    const storedFiles = useMemo(
        () => (day?.custody?.files || []).filter((f) => !removedKeys.has(f.key)),
        [day, removedKeys],
    );

    /*
     * Every staged transaction file. A day holds at most one of each type, so
     * this is the regular file, the mutual fund file, or both.
     */
    const faFiles = useMemo(
        () => (day?.fa?.raw || []).filter((f) => !removedKeys.has(f.key)),
        [day, removedKeys],
    );

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
                businessDate,
                replace,
            });
            /* Sharing days with a file already stored is not an error — both are
               kept — but those days would then be counted twice, so it is said
               alongside the column notes rather than swallowed. */
            setAddWarnings([
                ...(res.warnings || []),
                ...(res.overlapWarning ? [{ message: res.overlapWarning }] : []),
            ]);
            reportJob({
                id: `txn-custody-upload-${Date.now()}`,
                label: `Custodian file uploaded — ${file.name}`,
                status: "completed",
                message: `${scheme} · ${api.FILE_TYPE_LABEL[fileType] || fileType} is ready for matching.`,
            });
            // The scheme and type stay selected: adding several days for one
            // scheme is the case this screen exists for, and clearing after
            // every file would mean re-picking both each time.
            await loadDay();
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
            await loadDay();
        } catch (err) {
            onError({ message: err.message, detail: err.detail });
        } finally {
            // The server writes the bell entry: "in progress" now, "removed" once
            // Stratus stops listing the file. Show it straight away.
            refreshNotifications();
            setRemovingKey(null);
        }
    };

    const removeCustodyFile = (file) =>
        removeFile(
            file,
            `${file.originalFileName} (${file.scheme}, ${rangeLabel(file)})`,
            setAddError,
        );

    /* ── transaction file ─────────────────────────────────────────────── */

    /** Same contract as uploadCustody: the mapping panel owns the errors. */
    const uploadFa = async (file, { fileType, replace = false }) => {
        setFaBusy(true);
        setFaError(null);
        setFaWarnings([]);
        try {
            const res = await api.uploadFaFile({ file, businessDate, fileType, replace });
            setFaWarnings(res.warnings || []);
            reportJob({
                id: `txn-fa-upload-${Date.now()}`,
                label: `Transaction file uploaded — ${file.name}`,
                status: "completed",
                message: `${api.FA_FILE_TYPE_LABEL[fileType] || fileType} file is ready for matching.`,
            });
            if (res.businessDate !== businessDate && onBusinessDateChange) {
                onBusinessDateChange(res.businessDate);
            }
            await loadDay();
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

    const blockers = [];
    if (faFiles.length === 0) blockers.push("No transaction file staged.");
    if (storedFiles.length === 0) blockers.push("No custodian files uploaded.");

    const gateOneOpen = blockers.length === 0 && !run?.isRunning && !starting;

    /** Start the run. `dates` empty means every date the files contain. */
    const startMatch = async (dates) => {
        setPickerOpen(false);
        setStarting(true);
        setRunError(null);
        setReusedNote("");
        try {
            const res = await api.startRun(businessDate, undefined, dates);
            const status = await api.getRun(res.runId);
            setRun(status.run);
            if (res.reused) {
                reportJob({
                    id: `txn-match-reused-${Date.now()}`,
                    label: `FA vs Custody matching — ${businessDate}`,
                    status: "completed",
                    message: res.message || "These dates were already matched; showing that report.",
                });
            } else {
                // The runId is the bell's key: the server settles it from the run's own status.
                trackJob({
                    id: res.runId,
                    label: `FA vs Custody matching — ${businessDate}`,
                    message: "Matching is running in the background.",
                });
            }
            /* Nothing was re-run: these dates already had a report, and the same
               files over the same dates can only say the same thing. */
            setReusedNote(res.reused ? res.message : "");
        } catch (err) {
            setRunError({ message: err.message, detail: err.detail });
        } finally {
            setStarting(false);
        }
    };

    /*
     * Ask the server what dates the files cover. One date means there is nothing
     * to choose, so the run starts straight away rather than showing a dialog
     * with a single tick in it.
     */
    const handleMatch = async () => {
        setPickerLoading(true);
        setPickerError("");
        setPickerOpen(true);
        try {
            const res = await api.getRunDates(businessDate);
            const dates = res.dates || [];
            setPickerDates(dates);
            setSelectedDates(new Set(dates.map((d) => d.date)));
            if (dates.length <= 1) {
                await startMatch([]);
                return;
            }
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

    /** Same idea for the transaction file: Browse and drop both land here. */
    const handleFaFilePicked = (f) => {
        if (!f) return;
        setFaError(null);
        setPending({
            kind: "fa",
            file: f,
            fileType: faFileType,
            profileType: PROFILE_FOR_FA_FILE_TYPE[faFileType],
        });
    };

    const custodyZoneDisabled = addBusy || Boolean(run?.isRunning);
    const faZoneDisabled = faBusy || Boolean(run?.isRunning);

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
                    </div>

                    <div
                        className={custodyDragActive ? "up-zone is-drag" : "up-zone"}
                        onDragEnter={(e) => {
                            e.preventDefault();
                            if (!custodyZoneDisabled) setCustodyDragActive(true);
                        }}
                        onDragOver={(e) => e.preventDefault()}
                        onDragLeave={() => setCustodyDragActive(false)}
                        onDrop={(e) => {
                            e.preventDefault();
                            setCustodyDragActive(false);
                            if (custodyZoneDisabled) return;
                            const f = e.dataTransfer.files && e.dataTransfer.files[0];
                            if (f) handleCustodyFilePicked(f);
                        }}
                    >
                        <input
                            type="file"
                            accept=".csv"
                            className="up-zone-input"
                            aria-label="Upload custodian file"
                            disabled={custodyZoneDisabled}
                            onChange={(e) => {
                                const f = e.target.files[0];
                                e.target.value = "";
                                handleCustodyFilePicked(f);
                            }}
                        />
                        <div className="up-zone-visual">
                            <UploadGlyph />
                            <span className="up-zone-title">
                                {addBusy ? "Validating…" : "Upload custodian file"}
                            </span>
                            <span className="up-zone-sub">
                                Drag &amp; drop or <span className="up-zone-browse">Browse</span>
                            </span>
                        </div>
                    </div>

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
                    </div>

                    <div
                        className={faDragActive ? "up-zone is-drag" : "up-zone"}
                        onDragEnter={(e) => {
                            e.preventDefault();
                            if (!faZoneDisabled) setFaDragActive(true);
                        }}
                        onDragOver={(e) => e.preventDefault()}
                        onDragLeave={() => setFaDragActive(false)}
                        onDrop={(e) => {
                            e.preventDefault();
                            setFaDragActive(false);
                            if (faZoneDisabled) return;
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
                            disabled={faZoneDisabled}
                            onChange={(e) => {
                                const f = e.target.files[0];
                                e.target.value = "";
                                handleFaFilePicked(f);
                            }}
                        />
                        <div className="up-zone-visual">
                            <UploadGlyph />
                            <span className="up-zone-title">
                                {faBusy ? "Validating…" : "Upload transaction file"}
                            </span>
                            <span className="up-zone-sub">
                                Drag &amp; drop or <span className="up-zone-browse">Browse</span>
                            </span>
                        </div>
                    </div>

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
                                            onClick={() => removeFile(f, f.originalFileName, setFaError)}
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
                        <span className="cm-gate-fact-label">Fund accounting</span>
                        <span className="cm-gate-fact-value">
                            {faFiles.length > 0
                                ? `${faFiles.length} file${faFiles.length === 1 ? "" : "s"} ready`
                                : "Not staged"}
                        </span>
                    </div>
                    <div className="cm-gate-fact">
                        <span className="cm-gate-fact-label">Custodian</span>
                        <span className="cm-gate-fact-value">
                            {storedFiles.length > 0
                                ? `${storedFiles.length} file${storedFiles.length === 1 ? "" : "s"} ready`
                                : "Not staged"}
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
                    {blockers.length > 0 && !run?.isRunning ? (
                        <p className="cm-blockers">
                            {faFiles.length === 0 && storedFiles.length === 0
                                ? "Upload a Fund Accounting file and at least one Custodian file."
                                : faFiles.length === 0
                                    ? "Upload a Fund Accounting file to continue."
                                    : "Upload at least one Custodian file to continue."}
                        </p>
                    ) : null}
                </div>

                {run ? (
                    <div className={`cm-progress ${run.status.toLowerCase()}`}>
                        <div className="cm-progress-head">
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
                            <span className="cm-run-id mono">{run.runId}</span>
                        </div>

                        {run.isRunning ? (
                            <div className="cm-bar">
                                <div className="cm-bar-fill" style={{ width: `${run.progressPercent}%` }} />
                            </div>
                        ) : null}

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
                <DatePicker
                    dates={pickerDates}
                    loading={pickerLoading}
                    error={pickerError}
                    selected={selectedDates}
                    onToggle={toggleDate}
                    onAll={() => setSelectedDates(new Set(pickerDates.map((d) => d.date)))}
                    onNone={() => setSelectedDates(new Set())}
                    onRun={() =>
                        startMatch(
                            // Everything selected is the same as no filter at all.
                            selectedDates.size === pickerDates.length ? [] : [...selectedDates],
                        )
                    }
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
                onUpload={uploadPending}
                onClose={() => setPending(null)}
                onSaved={loadDay}
                showHints={false}
            />
        </>
    );
};

export default CustodyMatchPanel;
