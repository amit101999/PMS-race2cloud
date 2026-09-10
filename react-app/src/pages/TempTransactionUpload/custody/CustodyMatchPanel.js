import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";

import UploadErrorDetails from "./UploadErrorDetails";
import * as api from "./api";
import "./CustodyMatch.css";

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
const CustodyMatchPanel = ({ businessDate, onBusinessDateChange, onRunChange }) => {
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

    /* The date chooser, opened by Match when the files span more than one day. */
    const [pickerOpen, setPickerOpen] = useState(false);
    const [pickerDates, setPickerDates] = useState([]);
    const [pickerLoading, setPickerLoading] = useState(false);
    const [pickerError, setPickerError] = useState("");
    const [selectedDates, setSelectedDates] = useState(new Set());

    const faInputRef = useRef(null);
    const pollRef = useRef(null);

    /* ── loading a date ───────────────────────────────────────────────── */

    const loadDay = useCallback(async () => {
        if (!businessDate) return;
        setDayLoading(true);
        try {
            const res = await api.getLatestRun(businessDate);
            setDay(res.day);
            setRun(res.run);
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

    useEffect(() => {
        let cancelled = false;
        api
            .getSchemes()
            .then((res) => {
                if (!cancelled) setSchemes(res.schemes || []);
            })
            // A missing scheme list must not take the panel down; the row simply
            // offers nothing to pick until the call succeeds.
            .catch(() => {});
        return () => {
            cancelled = true;
        };
    }, []);

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
                setRun(res.run);
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

    /* Newest first, so the first one still present is the staged file. */
    const faFile = useMemo(
        () => (day?.fa?.raw || []).find((f) => !removedKeys.has(f.key)) || null,
        [day, removedKeys],
    );

    const uploadCustody = async (file, replace = false) => {
        if (!file) return;
        const scheme = draft.scheme.trim();
        if (!scheme) {
            setAddError({ message: "Choose a scheme before choosing a file." });
            return;
        }

        setAddBusy(true);
        setAddError(null);
        setAddWarnings([]);
        try {
            const res = await api.uploadCustodyFile({
                file,
                scheme,
                fileType: draft.fileType,
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
            // The scheme and type stay selected: adding several days for one
            // scheme is the case this screen exists for, and clearing after
            // every file would mean re-picking both each time.
            await loadDay();
        } catch (err) {
            if (err.detail?.code === "FILE_ALREADY_EXISTS") {
                const ok = window.confirm(
                    `${err.message}\n\nOnly that one file is replaced — the other schemes ` +
                        "and the other dates in this run are left alone.\n\nReplace it?",
                );
                if (ok) {
                    await uploadCustody(file, true);
                    return;
                }
                setAddBusy(false);
                return;
            }
            setAddError({ message: err.message, detail: err.detail });
        } finally {
            setAddBusy(false);
        }
    };

    /**
     * Both sides remove a file the same way, so they share this. `onError` is
     * the only difference: the message belongs beside the file that failed.
     */
    const removeFile = async (file, what, onError) => {
        if (!window.confirm(`Remove ${what} from this run?`)) return;
        onError(null);
        try {
            await api.deleteUploadedFile(file.key);
            setRemovedKeys((prev) => new Set(prev).add(file.key));
            await loadDay();
        } catch (err) {
            onError({ message: err.message, detail: err.detail });
        }
    };

    const removeCustodyFile = (file) =>
        removeFile(
            file,
            `${file.originalFileName} (${file.scheme}, ${rangeLabel(file)})`,
            setAddError,
        );

    /* ── transaction file ─────────────────────────────────────────────── */

    const uploadFa = async (file, replace = false) => {
        if (!file) return;
        setFaBusy(true);
        setFaError(null);
        setFaWarnings([]);
        try {
            const res = await api.uploadFaFile({ file, businessDate, replace });
            setFaWarnings(res.warnings || []);
            if (res.businessDate !== businessDate && onBusinessDateChange) {
                onBusinessDateChange(res.businessDate);
            }
            await loadDay();
        } catch (err) {
            if (err.detail?.code === "FILE_ALREADY_EXISTS") {
                const existing = err.detail.existing?.[0];
                const ok = window.confirm(
                    `${err.message}\n\nStored file: ${existing?.fileName || "unknown"}\n\n` +
                        "Delete it and stage this one instead?",
                );
                if (ok) {
                    await uploadFa(file, true);
                    return;
                }
                setFaBusy(false);
                return;
            }
            setFaError({ message: err.message, detail: err.detail });
            if (faInputRef.current) faInputRef.current.value = "";
        } finally {
            setFaBusy(false);
        }
    };

    /* ── gate 1 ───────────────────────────────────────────────────────── */

    const blockers = [];
    if (!faFile) blockers.push("No transaction file staged.");
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
    /* Nothing filed yet means there is nothing to pick from — type it instead. */
    const typingScheme = draft.newScheme || schemeOptions.length === 0;

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

    /* ── render ───────────────────────────────────────────────────────── */

    return (
        <>
            {/* Custody on the left, the transaction file on the right: the two
                things Gate 1 needs, side by side rather than one below the other. */}
            <div className="cm-split">
              <div className="cm-col">
                {/* Heading and date sit above the panel, not inside it. */}
                <div className="cm-col-head">
                    <h3>Custody Matching — Deal &amp; Allocation</h3>
                    <label className="cm-date">
                        <span>Transaction date</span>
                        <input
                            type="date"
                            value={businessDate}
                            onChange={(e) => onBusinessDateChange(e.target.value)}
                            disabled={run?.isRunning}
                        />
                    </label>
                </div>

                <div className="temp-transaction-card">
                {/* One row per uploaded file. Two files of the same type for the
                    same scheme are a duplicate only when they cover the same
                    dates; anything else is another row. */}
                {storedFiles.length === 0 ? (
                    <p className="cm-empty">
                        {dayLoading ? "Loading…" : "No custodian files yet."}
                    </p>
                ) : (
                    <table className="upload-history-table cm-filetable">
                        <thead>
                            <tr>
                                <th>Scheme</th>
                                <th>Type</th>
                                <th>Dates covered</th>
                                <th>File</th>
                                <th className="action" aria-label="Remove" />
                            </tr>
                        </thead>
                        <tbody>
                            {storedFiles.map((f) => (
                                <tr key={f.key}>
                                    <td className="cm-cell-scheme">{f.scheme}</td>
                                    <td>{api.FILE_TYPE_LABEL[f.fileType] || f.fileType}</td>
                                    <td className="cm-cell-dates">{rangeLabel(f)}</td>
                                    <td className="cm-cell-file" title={f.originalFileName}>
                                        {f.originalFileName}
                                        <span className="cm-file-size">
                                            {api.formatBytes(f.sizeBytes)}
                                        </span>
                                    </td>
                                    <td className="action">
                                        <button
                                            type="button"
                                            className="cm-remove"
                                            onClick={() => removeCustodyFile(f)}
                                            disabled={run?.isRunning}
                                            aria-label={`Remove ${f.originalFileName}`}
                                        >
                                            ×
                                        </button>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                )}

                <div className="cm-addfile">
                    <span className="cm-addfile-title">Add custodian file</span>
                    <div className="cm-addfile-controls">
                        {typingScheme ? (
                            <input
                                className="cm-scheme"
                                type="text"
                                placeholder="New scheme code"
                                value={draft.scheme}
                                onChange={(e) => {
                                    setAddError(null);
                                    setDraft((d) => ({ ...d, scheme: e.target.value }));
                                }}
                                disabled={addBusy || run?.isRunning}
                            />
                        ) : (
                            <select
                                className="cm-scheme"
                                value={draft.scheme}
                                onChange={(e) => pickScheme(e.target.value)}
                                disabled={addBusy || run?.isRunning}
                            >
                                <option value="">Select scheme…</option>
                                {schemeOptions.map((s) => (
                                    <option key={s.scheme} value={s.scheme}>
                                        {s.scheme}
                                    </option>
                                ))}
                                <option value={NEW_SCHEME}>+ New scheme…</option>
                            </select>
                        )}

                        {draft.newScheme && schemeOptions.length > 0 ? (
                            <button
                                type="button"
                                className="cm-scheme-back"
                                onClick={() => pickScheme("")}
                                disabled={addBusy || run?.isRunning}
                            >
                                choose from list
                            </button>
                        ) : null}

                        <select
                            className="cm-type"
                            value={draft.fileType}
                            onChange={(e) => {
                                setAddError(null);
                                setDraft((d) => ({ ...d, fileType: e.target.value }));
                            }}
                            disabled={addBusy || run?.isRunning}
                        >
                            <option value={api.CUSTODY_FILE_TYPE.RESIDENTIAL}>
                                {api.FILE_TYPE_LABEL.RESIDENTIAL}
                            </option>
                            <option value={api.CUSTODY_FILE_TYPE.NDPMS}>
                                {api.FILE_TYPE_LABEL.NDPMS}
                            </option>
                        </select>

                        <input
                            type="file"
                            accept=".csv"
                            disabled={addBusy || run?.isRunning}
                            onChange={(e) => {
                                const f = e.target.files[0];
                                e.target.value = "";
                                uploadCustody(f);
                            }}
                        />
                    </div>
                    {addBusy ? <span className="cm-addfile-busy">Checking the file…</span> : null}
                </div>

                {addError ? (
                    <UploadErrorDetails message={addError.message} detail={addError.detail} />
                ) : null}

                <HeaderWarnings warnings={addWarnings} />
                </div>
              </div>

              <div className="cm-col">
                <div className="cm-col-head">
                    <h3>Fund Accounting Daily Transaction File</h3>
                </div>

                <div className="temp-transaction-card">

                <div className="cm-fa">
                    <input
                        ref={faInputRef}
                        type="file"
                        accept=".csv"
                        disabled={faBusy || run?.isRunning}
                        onChange={(e) => {
                            const f = e.target.files[0];
                            e.target.value = "";
                            uploadFa(f);
                        }}
                    />
                    {faFile ? (
                        <>
                            <span className="cm-staged">
                                Staged — not yet imported: {faFile.originalFileName} ·{" "}
                                {api.formatBytes(faFile.sizeBytes)}
                            </span>
                            <button
                                type="button"
                                className="cm-remove"
                                onClick={() =>
                                    removeFile(faFile, faFile.originalFileName, setFaError)
                                }
                                disabled={faBusy || run?.isRunning}
                                aria-label={`Remove ${faFile.originalFileName}`}
                            >
                                ×
                            </button>
                        </>
                    ) : (
                        <span className="cm-staged muted">
                            {faBusy ? "Validating…" : "No transaction file staged."}
                        </span>
                    )}
                </div>

                {faError ? <UploadErrorDetails message={faError.message} detail={faError.detail} /> : null}
                <HeaderWarnings warnings={faWarnings} />
                </div>
              </div>
            </div>

            <div className="temp-transaction-card cm-gate">
                <div className="cm-gate-row">
                    <button
                        type="button"
                        className="cm-primary"
                        onClick={handleMatch}
                        disabled={!gateOneOpen}
                    >
                        {run?.isRunning || starting ? "Matching…" : "Match FA vs Custody"}
                    </button>

                    {/* A greyed-out button with no explanation is the thing users complain
                        about; say exactly what is still missing. */}
                    {blockers.length > 0 && !run?.isRunning ? (
                        <ul className="cm-blockers">
                            {blockers.map((b) => (
                                <li key={b}>{b}</li>
                            ))}
                        </ul>
                    ) : null}
                </div>

                {run ? (
                    <div className={`cm-progress ${run.status.toLowerCase()}`}>
                        <div className="cm-progress-head">
                            <strong>{run.stageDetail || run.status}</strong>
                            <span className="cm-run-id">{run.runId}</span>
                        </div>

                        {run.isRunning ? (
                            <div className="cm-bar">
                                <div className="cm-bar-fill" style={{ width: `${run.progressPercent}%` }} />
                            </div>
                        ) : null}

                        <div className="cm-stats">
                            <span>{run.counts.faRows.toLocaleString()} FA rows</span>
                            <span>{run.counts.custodyRows.toLocaleString()} custody rows</span>
                            <span>{run.counts.resolved.toLocaleString()} resolved to actual codes</span>
                            {run.counts.unmapped > 0 ? (
                                <span className="warn">{run.counts.unmapped} unmapped</span>
                            ) : null}
                            {run.counts.totalKeys > 0 ? (
                                <span>
                                    {run.counts.comparedKeys.toLocaleString()} /{" "}
                                    {run.counts.totalKeys.toLocaleString()} keys compared
                                </span>
                            ) : null}
                        </div>

                        {run.status === "COMPLETED_CLEAN" ? (
                            <p className="cm-verdict ok">
                                Matched — no differences. The transaction file can now be imported.
                            </p>
                        ) : null}

                        {run.status === "COMPLETED_WITH_EXCEPTIONS" ? (
                            <p className="cm-verdict bad">
                                {run.counts.exceptions.toLocaleString()} record(s) not matching.
                            </p>
                        ) : null}

                        {run.status === "FAILED" ? (
                            <div className="cm-verdict bad">
                                <p style={{ margin: 0 }}>
                                    The run failed{run.error ? `: ${run.error.message}` : "."}
                                </p>
                                <button type="button" className="cm-secondary" onClick={handleRetry}>
                                    Retry from stage {run.stage}
                                </button>
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
        </>
    );
};

export default CustodyMatchPanel;
