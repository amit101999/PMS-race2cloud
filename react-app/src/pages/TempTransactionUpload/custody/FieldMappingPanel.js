import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
    KIND_SHORT,
    PROFILE_LABEL,
    SOURCE_BY,
    TRANSFORM_LABEL,
    deleteFieldMap,
    getFieldMaps,
    isColumnCompatible,
    normHeader,
    previewHeaders,
    saveFieldMap,
} from "./api";
import UploadErrorDetails from "./UploadErrorDetails";
import "./FieldMapping.css";

/**
 * Field Mapping — the gate in front of every upload.
 *
 * Picking a file no longer uploads it. The file's columns are read first, this
 * panel opens, and the file goes up only once every field has been mapped and
 * the mapping saved. That ordering is the point: a file whose columns nobody
 * has confirmed is a file nobody can vouch for, and by the time it reaches the
 * Transaction table it is too late to ask.
 *
 * Headings are CHOSEN, never typed, and nothing is pre-filled from a guess. The
 * options come from the file itself, so a heading that does not exist cannot be
 * entered — and every field is answered by a person who looked at it.
 *
 * Second time round the saved mapping IS pre-filled, because it was answered by
 * that same person. Anything in it that no longer fits the file — a column the
 * custodian renamed, or one whose contents changed shape — is cleared and
 * listed at the top rather than carried forward on trust.
 */

const FILE_CHOICES = [
    "FA",
    "FA_MUTUAL_FUND",
    "CUSTODY_RESIDENTIAL",
    "CUSTODY_NDPMS",
    "CUSTODY_MUTUAL_FUND",
];

const FILE_BLURB = {
    FA: "The daily transaction file you import. Whatever layout you map here is rewritten into the standard 26-column format before import, so nothing downstream changes.",
    FA_MUTUAL_FUND:
        "The mutual fund fund-accounting file. It carries the same 26 fields as the regular transaction file under one different heading, so it is mapped on its own. Its rows are matched only against the Mutual Funds custodian files.",
    CUSTODY_RESIDENTIAL: "The deal allocation file your custodian sends for Resident accounts.",
    CUSTODY_NDPMS: "The deal allocation file your custodian sends for NRE/NRO/NDPMS accounts.",
    CUSTODY_MUTUAL_FUND: "The Mutual Funds transaction file your custodian sends (Subscription / Redemption). Matched only against mutual fund rows in the transaction file.",
};

const NONE = "NONE";

/**
 * The dropdown value for "not present in this file".
 *
 * Distinct from the blank option, which still means UNANSWERED. A layout can
 * carry columns a given custodian simply does not send, and with every field
 * required the only alternative would be a dead end: the screen demanding an
 * answer no column in the file can give. Never offered for a match-key field —
 * the comparison is built from those, and the server refuses to store one as
 * absent.
 */
const ABSENT = "__ABSENT__";

const transformOf = (source) => source?.transform || NONE;

/**
 * A sample value as the field will receive it. For ContractNo read as a date,
 * the first 8 digits are ddmmyyyy (210820261 -> 21-08-2026); the server does the
 * real parsing, this only shows the user what their choice produces.
 */
const previewValue = (sample, transform) => {
    if (transform !== "DATE_FROM_FIRST_8") return sample;
    const digits = String(sample).match(/^(\d{2})(\d{2})(\d{4})/);
    return digits ? `${sample} → ${digits[1]}-${digits[2]}-${digits[3]}` : sample;
};

/**
 * Why a column cannot feed a field, in one short line. The server checks every
 * row, so the first row that does not fit is named when it is known.
 */
const mismatchReason = (column, entry, transform) => {
    const hit =
        transform && transform !== NONE
            ? column.transformMismatch?.[transform]
            : column.mismatch?.[entry.kind];
    const needs = KIND_SHORT[entry.kind] || entry.kind;
    if (hit) return `Row ${hit.row}: "${hit.value}" is not ${needs}.`;
    return `"${column.label}" is not ${needs}.`;
};

/* ------------------------------------------------------- source <-> dropdown */

/**
 * Which dropdown value shows a saved source, or "" when it no longer resolves.
 *
 * A miss is never smoothed over with a near match: the field is cleared and
 * reported, because silently moving a mapping onto a column nobody chose is
 * exactly the failure this screen exists to prevent.
 */
const valueForSource = (source, columns) => {
    if (!source) return "";
    if (source.by === SOURCE_BY.ABSENT) return ABSENT;

    if (source.by === SOURCE_BY.INDEX) {
        return columns.some((c) => c.index === source.index) ? String(source.index) : "";
    }

    const hit = columns.find((c) => normHeader(c.name) === normHeader(source.name));
    return hit ? String(hit.index) : "";
};

/**
 * Build the source a picked column should be stored as.
 *
 * A repeated heading is addressed by POSITION, anything else by NAME. The FA
 * template carries three columns literally called "filler" and the first two
 * are Net Rate and Net Amount, so a name would not say which. A unique name is
 * the better record for every other column: it survives the custodian
 * reordering their export, which a position does not.
 */
const sourceForColumn = (column, transform) =>
    column.duplicate
        ? { by: SOURCE_BY.INDEX, index: column.index, transform }
        : { by: SOURCE_BY.NAME, name: column.name, transform };

const sameSource = (a, b) => {
    if (!a || !b) return a === b;
    if (a.by !== b.by) return false;
    if (transformOf(a) !== transformOf(b)) return false;
    if (a.by === SOURCE_BY.ABSENT) return true;
    return a.by === SOURCE_BY.INDEX
        ? a.index === b.index
        : normHeader(a.name) === normHeader(b.name);
};

/** What a saved source named, for the "was ..." half of a cleared-field message. */
const describeSource = (source) =>
    source.by === SOURCE_BY.INDEX ? `column ${source.index + 1}` : source.name;

/**
 * Carry a saved mapping onto the file in front of us, dropping what no longer fits.
 *
 * Three things can be wrong with a stored answer, and all of them are the
 * user's to re-answer rather than ours to guess at:
 *
 *   the column is gone        the custodian renamed or removed it
 *   the column changed shape  it is there, but now holds something this field
 *                             cannot use — trade numbers under a date field
 *   it was never answered     saved by an older version of this screen, which
 *                             allowed a field to be marked as not present
 *
 * @returns {{draft:object, cleared:Array<{label:string,reason:string}>}}
 */
const reconcile = (saved, entries, columns) => {
    const draft = {};
    const cleared = [];

    for (const entry of entries) {
        const source = saved?.[entry.field];
        if (!source) continue;

        /*
         * "Not present in this file" is carried forward, not cleared. It is a
         * deliberate answer from the person who mapped this layout, and asking
         * for it again on every upload would be asking about a decision that
         * has not changed.
         */
        if (source.by === SOURCE_BY.ABSENT) {
            draft[entry.field] = source;
            continue;
        }

        const value = valueForSource(source, columns);
        if (value === "") {
            cleared.push({
                label: entry.label,
                reason: `"${describeSource(source)}" is not a column in this file`,
            });
            continue;
        }

        const column = columns.find((c) => c.index === Number(value));
        if (!isColumnCompatible(column, entry, transformOf(source))) {
            cleared.push({
                label: entry.label,
                reason: mismatchReason(column, entry, transformOf(source)),
            });
            continue;
        }

        draft[entry.field] = source;
    }

    return { draft, cleared };
};

/* ------------------------------------------------------------------ one row */

function MappingRow({ entry, source, columns, takenBy, onChange, registerRef, fieldError }) {
    const transforms = entry.allowTransforms || [NONE];
    const transform = transformOf(source);
    const value = valueForSource(source, columns);

    const absent = value === ABSENT;
    const selectedColumn =
        value && !absent ? columns.find((c) => c.index === Number(value)) || null : null;

    /*
     * A column another field has taken is not offered again — except to a field
     * flagged allowShared. NDPMS has no date column at all: Transaction Date is
     * the first eight digits of ContractNo, so ContractNo genuinely feeds both
     * that field and Trade / Contract Ref.
     */
    const available = columns.filter((c) => {
        const owner = takenBy.get(c.index);
        return !owner || owner === entry.field || entry.allowShared;
    });

    /*
     * Every remaining column is offered, not only the ones that fit. Detection
     * reads a handful of rows, so it can be wrong about a column that is blank
     * or unusual early in the file — and a field left with no compatible column
     * would be a dead end no amount of re-picking escapes. A wrong choice is
     * caught below instead, where it can be explained.
     */
    const badPick = selectedColumn && !isColumnCompatible(selectedColumn, entry, transform);

    const handle = (next) => {
        if (next === "") return onChange(entry.field, null);
        if (next === ABSENT) {
            return onChange(entry.field, { by: SOURCE_BY.ABSENT, transform: NONE });
        }
        const column = columns.find((c) => c.index === Number(next));
        if (!column) return;

        /*
         * Pick the reading that fits the column. NDPMS has no date column:
         * Transaction Date is the first 8 digits of ContractNo, which as-is is
         * just a number. Keeping "as-is" would turn the row red for a choice
         * that is correct, so switch to the transform the column does fit.
         */
        let nextTransform = transform;
        if (!isColumnCompatible(column, entry, transform)) {
            const fitting = transforms.find((t) => isColumnCompatible(column, entry, t));
            if (fitting) nextTransform = fitting;
        }
        onChange(entry.field, sourceForColumn(column, nextTransform));
    };

    return (
        <tr className="fm-row">
            <td className="fm-cell-field">
                <div className="fm-fieldname">{entry.label}</div>
            </td>

            <td className="fm-cell-source">
                <div className="fm-inputs">
                    <select
                        ref={(el) => registerRef && registerRef(entry.field, el)}
                        className={badPick || fieldError ? "fm-select fm-select--bad" : "fm-select"}
                        value={value}
                        onChange={(e) => handle(e.target.value)}
                        disabled={columns.length === 0}
                    >
                        {/* Deliberately unanswered by default: the list holds the
                            file's columns and nothing else, so a field nobody has
                            answered stays visibly unanswered rather than silently
                            taking whichever column comes first. */}
                        <option value="">Select column</option>
                        {available.map((c) => (
                            <option key={c.index} value={c.index}>
                                {c.label}
                            </option>
                        ))}
                        {/* Never for a match-key field: the comparison is built
                            from those five, and the server refuses to store one
                            as absent. */}
                        {entry.matchKey ? null : (
                            <option value={ABSENT}>— not present in this file —</option>
                        )}
                    </select>

                    {transforms.length > 1 ? (
                        <select
                            className="fm-transform"
                            value={transform}
                            onChange={(e) => {
                                if (!source) return;
                                onChange(entry.field, { ...source, transform: e.target.value });
                            }}
                        >
                            {transforms.map((t) => (
                                <option key={t} value={t}>
                                    {TRANSFORM_LABEL[t] || t}
                                </option>
                            ))}
                        </select>
                    ) : null}
                </div>

                {badPick ? (
                    <p className="fm-rowerror" role="alert">
                        {mismatchReason(selectedColumn, entry, transform)}
                    </p>
                ) : fieldError ? (
                    <p className="fm-rowerror" role="alert">
                        {fieldError}
                    </p>
                ) : null}

                {absent ? (
                    <p className="fm-samples">
                        This file does not carry this column — it will be left blank.
                    </p>
                ) : null}

                {/* A few real values from the picked column, so the user can see the
                    choice is right instead of trusting the heading alone. */}
                {!badPick && selectedColumn?.samples?.length > 0 ? (
                    <p className="fm-samples">
                        e.g.{" "}
                        {selectedColumn.samples
                            .slice(0, 3)
                            .map((sample) => previewValue(sample, transform))
                            .join(", ")}
                    </p>
                ) : null}
            </td>
        </tr>
    );
}

/* ---------------------------------------------------------------- the panel */

/**
 * @param {object}   props
 * @param {boolean}  props.open
 * @param {File}     [props.file]          the file about to be uploaded; absent = view-only
 * @param {string}   [props.requestedType] the slot the user dropped it into
 * @param {Function} [props.onUpload]      ({ profileType, replace }) => Promise, throws ApiError
 * @param {Function} props.onClose
 * @param {Function} [props.onSaved]
 */
export default function FieldMappingPanel({
    open,
    file = null,
    requestedType = null,
    onUpload = null,
    onClose,
    onSaved,
}) {
    const [maps, setMaps] = useState([]);
    const [chosen, setChosen] = useState(null);
    const [draft, setDraft] = useState({});
    const [cleared, setCleared] = useState([]);
    const [preview, setPreview] = useState(null);

    const [loading, setLoading] = useState(false);
    const [saving, setSaving] = useState(false);
    const [uploading, setUploading] = useState(false);
    const [clearing, setClearing] = useState(false);
    const [error, setError] = useState("");
    const [notice, setNotice] = useState("");
    const [conflict, setConflict] = useState(null);
    /** A rejected upload, kept whole so the row-by-row reasons can be shown, not just the summary line. */
    const [uploadError, setUploadError] = useState(null);
    /** field -> message, shown under that row only after a Save attempt finds it empty. */
    const [fieldErrors, setFieldErrors] = useState({});

    const uploadMode = Boolean(file && onUpload);

    /* One DOM node per field, so a failed Save attempt can scroll to and focus
       the first thing still missing instead of leaving the user to hunt for it. */
    const fieldNodes = useRef({});
    const registerFieldRef = useCallback((field, node) => {
        if (node) fieldNodes.current[field] = node;
        else delete fieldNodes.current[field];
    }, []);

    const reset = () => {
        setChosen(null);
        setDraft({});
        setCleared([]);
        setPreview(null);
        setError("");
        setUploadError(null);
        setNotice("");
        setConflict(null);
        setFieldErrors({});
    };

    /**
     * Seed the draft for one file type.
     *
     * Blank only when nothing is stored for this type. `isDefault` means the
     * server had nothing saved and handed back the built-in layout — a guess
     * about the file, not an answer about it, and never a starting point, so
     * the first mapping is always made by hand.
     *
     * Once saved it is reused on every later upload, including after a refresh:
     * the layout of these files does not change between uploads, and re-typing
     * the same 14 answers each time would be work with no decision in it. What
     * still gets re-checked is the mapping against THIS file — see reconcile.
     */
    const seedDraft = useCallback((map, cols) => {
        if (!map) {
            setDraft({});
            setCleared([]);
            return;
        }

        // View-only (no file, so no columns): show what is stored, for reference.
        // Nothing can be saved or uploaded from here, so re-confirming would be
        // asking about a decision this screen is not offering to change.
        if (cols.length === 0) {
            setDraft(map.isDefault ? {} : { ...map.profile });
            setCleared([]);
            return;
        }

        if (map.isDefault) {
            setDraft({});
            setCleared([]);
            return;
        }

        const { draft: next, cleared: dropped } = reconcile(map.profile, map.fields, cols);
        setDraft(next);
        setCleared(dropped);
    }, []);

    const load = useCallback(async () => {
        setLoading(true);
        setError("");
        try {
            const [mapRes, previewRes] = await Promise.all([
                getFieldMaps(),
                file ? previewHeaders(file, requestedType || "") : Promise.resolve(null),
            ]);

            const loaded = mapRes.maps || [];
            setMaps(loaded);
            setPreview(previewRes);

            /*
             * The slot the user picked wins; detection only fills in when they
             * did not say. A file in the wrong slot would otherwise mislabel the
             * scheme on every exception it goes on to produce, so the mismatch
             * is surfaced below rather than silently resolved either way.
             */
            const type = requestedType || previewRes?.detectedType || null;
            if (type) {
                setChosen(type);
                seedDraft(
                    loaded.find((m) => m.type === type),
                    previewRes?.columns || [],
                );
            }
            return loaded;
        } catch (err) {
            setError(err.message);
            return [];
        } finally {
            setLoading(false);
        }
    }, [file, requestedType, seedDraft]);

    useEffect(() => {
        if (open) load();
        else reset();
    }, [open, load]);

    const active = useMemo(() => maps.find((m) => m.type === chosen) || null, [maps, chosen]);
    // Memoised: a fresh [] each render would re-run every memo below it.
    const columns = useMemo(() => preview?.columns || [], [preview]);
    const entries = useMemo(() => active?.fields || [], [active]);

    /** Switching file type starts again from whatever is saved for that type. */
    const choose = (type) => {
        setChosen(type);
        seedDraft(
            maps.find((m) => m.type === type),
            columns,
        );
        setError("");
        setUploadError(null);
        setNotice("");
    };

    /** column index -> the field holding it, so a taken column stops being offered. */
    const takenBy = useMemo(() => {
        const map = new Map();
        for (const entry of entries) {
            const value = valueForSource(draft[entry.field], columns);
            // A field answered as absent holds no column, so it claims none.
            if (value === "" || value === ABSENT) continue;
            // A shared field never claims the column, so the field it borrows
            // from keeps it and no third field can take it.
            if (entry.allowShared) continue;
            map.set(Number(value), entry.field);
        }
        return map;
    }, [entries, draft, columns]);

    const badFields = useMemo(
        () =>
            entries.filter((entry) => {
                const value = valueForSource(draft[entry.field], columns);
                // Absent points at no column, so there is nothing to be wrong.
                if (value === "" || value === ABSENT) return false;
                const column = columns.find((c) => c.index === Number(value));
                return (
                    column && !isColumnCompatible(column, entry, transformOf(draft[entry.field]))
                );
            }),
        [entries, draft, columns],
    );

    const unansweredCount = entries.filter(
        (entry) => valueForSource(draft[entry.field], columns) === "",
    ).length;

    const complete = entries.length > 0 && unansweredCount === 0 && badFields.length === 0;

    const dirty = useMemo(() => {
        if (!active) return false;

        /*
         * Nothing is stored for this file type yet.
         *
         * `active.profile` is then the BUILT-IN layout, which is a guess about
         * the file rather than an answer about it. A user whose columns happen
         * to match it — a Mutual Funds file sending UCC, SourceMFISIN,
         * TradeDate … — would otherwise finish mapping, find the draft equal to
         * that guess, and be handed Upload with nothing ever written to the
         * bucket. The first mapping is always a change, so it always has to be
         * saved.
         */
        if (active.isDefault) return true;

        const keys = new Set([...Object.keys(active.profile || {}), ...Object.keys(draft)]);
        for (const k of keys) if (!sameSource(active.profile?.[k], draft[k])) return true;
        return false;
    }, [active, draft]);

    /*
     * Upload is reachable only through a saved mapping.
     *
     * Save stays on screen, disabled, until every field is mapped, so the button
     * the user is working towards is the one in front of them. Upload takes its
     * place once there is nothing left to save, which for a second upload of an
     * unchanged file is immediately.
     */
    const showUpload = uploadMode && complete && !dirty;

    const setField = (field, source) => {
        setNotice("");
        setFieldErrors((prev) => {
            if (!prev[field]) return prev;
            const next = { ...prev };
            delete next[field];
            return next;
        });
        setDraft((prev) => {
            const next = { ...prev };
            if (source) next[field] = source;
            else delete next[field];
            return next;
        });
    };

    /**
     * Runs when Save is clicked rather than continuously: a field is only ever
     * called out once the user has tried to move on, not while they are still
     * working through the list. Finds the first field still empty or pointing
     * at an incompatible column, scrolls to it, focuses it, and stops there —
     * fixing that one and clicking Save again reaches the next, if any.
     */
    const validateBeforeSave = () => {
        for (const entry of entries) {
            const value = valueForSource(draft[entry.field], columns);
            if (value === "" ) {
                const node = fieldNodes.current[entry.field];
                if (node) {
                    node.scrollIntoView({ behavior: "smooth", block: "center" });
                    node.focus();
                }
                setFieldErrors({ [entry.field]: "This field is required." });
                return false;
            }
            if (value !== ABSENT) {
                const column = columns.find((c) => c.index === Number(value));
                if (column && !isColumnCompatible(column, entry, transformOf(draft[entry.field]))) {
                    const node = fieldNodes.current[entry.field];
                    if (node) {
                        node.scrollIntoView({ behavior: "smooth", block: "center" });
                        node.focus();
                    }
                    setFieldErrors({
                        [entry.field]: mismatchReason(column, entry, transformOf(draft[entry.field])),
                    });
                    return false;
                }
            }
        }
        setFieldErrors({});
        return true;
    };

    const requestClose = () => {
        if (
            dirty &&
            !window.confirm("Close without saving? Your changes to this mapping will be lost.")
        ) {
            return;
        }
        onClose();
    };

    /**
     * Save the mapping. Nothing else.
     *
     * The file is NOT sent here — Save stores the profile and that is all;
     * uploading is the separate step that follows. The response carries the
     * stored profile back, so it is read straight from there rather than
     * re-fetching: a reload would also re-send the whole file to /preview just
     * to be told the same columns it was told a moment ago.
     */
    const onSave = async () => {
        if (!validateBeforeSave()) return;
        setSaving(true);
        setError("");
        setUploadError(null);
        setNotice("");
        try {
            const res = await saveFieldMap(chosen, draft, "");

            // `dirty` is measured against this, so replacing it with what the
            // server actually stored is what lets Upload take over the button.
            if (res.map) {
                setMaps((prev) => prev.map((m) => (m.type === chosen ? res.map : m)));
                setDraft({ ...res.map.profile });
            }
            setCleared([]);
            setNotice(res.message || "Mapping saved.");

            // Saving supersedes every stored run, so the page behind this panel
            // is now showing a stale verdict and needs to re-read.
            if (onSaved) onSaved(res);
        } catch (err) {
            setError(err.message);
        } finally {
            setSaving(false);
        }
    };

    /**
     * Delete the saved mapping for this file type and start again from blank.
     *
     * Deleted on the server straight away rather than on the next Save, so
     * what the screen shows and what is stored cannot disagree if the user
     * closes before saving. Upload drops back to Save until every field is
     * mapped again.
     */
    const onClearAll = async () => {
        if (
            !window.confirm(
                `Delete the saved mapping for ${PROFILE_LABEL[chosen] || chosen}?\n\n` +
                    "Every field will be emptied and you will need to map and save them again " +
                    "before uploading. This affects every future file of this type.",
            )
        ) {
            return;
        }

        setClearing(true);
        setError("");
        setNotice("");
        setConflict(null);
        try {
            const res = await deleteFieldMap(chosen);
            if (res.map) {
                setMaps((prev) => prev.map((m) => (m.type === chosen ? res.map : m)));
            }
            setDraft({});
            setCleared([]);
            setNotice(res.message || "Mapping cleared.");

            // Clearing supersedes stored runs just as saving does.
            if (onSaved) onSaved(res);
        } catch (err) {
            setError(err.message);
        } finally {
            setClearing(false);
        }
    };

    const runUpload = async (replace) => {
        setUploading(true);
        setError("");
        setUploadError(null);
        setConflict(null);
        try {
            await onUpload({ profileType: chosen, replace });
            // The page below now owns the result; this panel is done.
            onClose();
        } catch (err) {
            // The file is already stored for this date. Asked and answered here
            // rather than back on the page, so the user is not bounced out of
            // the modal half way through an upload they started in it.
            if (err.detail?.code === "FILE_ALREADY_EXISTS") setConflict(err.detail);
            else setUploadError({ message: err.message, detail: err.detail });
        } finally {
            setUploading(false);
        }
    };

    if (!open) return null;

    const typeMismatch =
        preview?.detectedType && requestedType && preview.detectedType !== requestedType;

    const fileKindLabel =
        chosen === "FA" || chosen === "FA_MUTUAL_FUND" ? "Fund accounting file" : "Custodian file";

    return (
        <>
            <div className="fm-scrim" onClick={requestClose} />
            <aside className="fm-panel" role="dialog" aria-label="Field mapping">
                <header className="fm-panel-head">
                    <div>
                        {!uploadMode && chosen ? (
                            <button type="button" className="fm-back" onClick={() => reset()}>
                                &larr; All files
                            </button>
                        ) : null}
                        {chosen ? <p className="fm-kicker">Field Mapping</p> : null}
                        <h3>{chosen ? PROFILE_LABEL[chosen] : "Field Mapping"}</h3>
                        {file ? (
                            <div className="fm-fileinfo">
                                <svg
                                    width="13"
                                    height="13"
                                    viewBox="0 0 16 16"
                                    fill="none"
                                    aria-hidden="true"
                                    className="fm-fileinfo-icon"
                                >
                                    <path
                                        d="M4 1.5h4.5L12 5v9a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1V2.5a1 1 0 0 1 1-1Z"
                                        stroke="currentColor"
                                        strokeWidth="1.1"
                                        strokeLinejoin="round"
                                    />
                                    <path
                                        d="M8.5 1.5V5H12"
                                        stroke="currentColor"
                                        strokeWidth="1.1"
                                        strokeLinejoin="round"
                                    />
                                </svg>
                                <span className="fm-fileinfo-text">
                                    <span className="fm-fileinfo-kind">{fileKindLabel}</span>
                                    <span className="fm-fileinfo-name" title={file.name}>
                                        {file.name}
                                    </span>
                                </span>
                            </div>
                        ) : null}
                    </div>
                    <button
                        type="button"
                        className="fm-close"
                        onClick={requestClose}
                        aria-label="Close"
                    >
                        &times;
                    </button>
                </header>

                <div className="fm-panel-body">
                    {error ? <div className="fm-error">{error}</div> : null}
                    {uploadError ? (
                        <UploadErrorDetails
                            message={uploadError.message}
                            detail={uploadError.detail}
                        />
                    ) : null}
                    {notice ? <div className="fm-notice">{notice}</div> : null}
                    {loading ? <p className="fm-muted">Reading the file&hellip;</p> : null}

                    {conflict ? (
                        <div className="fm-conflict">
                            <p>{conflict.message}</p>
                            <div className="fm-conflict-actions">
                                <button
                                    type="button"
                                    className="fm-primary"
                                    onClick={() => runUpload(true)}
                                    disabled={uploading}
                                >
                                    {uploading ? "Replacing…" : "Replace it"}
                                </button>
                                <button
                                    type="button"
                                    className="fm-ghost"
                                    onClick={() => setConflict(null)}
                                    disabled={uploading}
                                >
                                    Keep the stored file
                                </button>
                            </div>
                        </div>
                    ) : null}

                    {!chosen && !loading ? (
                        <>
                            <h4 className="fm-steptitle">Which file are you mapping?</h4>
                            <div className="fm-choices">
                                {FILE_CHOICES.map((type) => {
                                    const map = maps.find((m) => m.type === type);
                                    return (
                                        <button
                                            key={type}
                                            type="button"
                                            className="fm-choice"
                                            onClick={() => choose(type)}
                                            disabled={!map}
                                        >
                                            <span className="fm-choice-name">
                                                {PROFILE_LABEL[type] || type}
                                            </span>
                                            <span className="fm-choice-blurb">
                                                {FILE_BLURB[type]}
                                            </span>
                                            <span
                                                className={
                                                    map && !map.isDefault
                                                        ? "fm-badge fm-badge--custom"
                                                        : "fm-badge"
                                                }
                                            >
                                                {map && !map.isDefault
                                                    ? "Mapped"
                                                    : "Not mapped yet"}
                                            </span>
                                        </button>
                                    );
                                })}
                            </div>
                        </>
                    ) : null}

                    {/*
                      * A file type the server does not know about.
                      *
                      * The panel is otherwise silent about it: with no map there are no
                      * field rows and Save is disabled, which reads as a missing button
                      * rather than as the server being out of date.
                      */}
                    {chosen && !active && !loading ? (
                        <div className="fm-error" role="alert">
                            <strong>
                                {PROFILE_LABEL[chosen] || chosen} is not available on the server
                                yet.
                            </strong>{" "}
                            Its mapping cannot be mapped or saved until the server is updated to a
                            build that knows this file type.
                        </div>
                    ) : null}

                    {chosen && active ? (
                        <>
                            {preview ? (
                                <div
                                    className={
                                        typeMismatch ? "fm-detect fm-detect--warn" : "fm-detect"
                                    }
                                >
                                    <span>
                                        {preview.detectedType
                                            ? `Detected: ${PROFILE_LABEL[preview.detectedType]}`
                                            : "Layout could not be detected from the columns"}
                                        {typeMismatch
                                            ? ` — but this was uploaded as ${PROFILE_LABEL[requestedType]}.`
                                            : ""}
                                    </span>
                                    <select
                                        className="fm-transform"
                                        value={chosen}
                                        onChange={(e) => choose(e.target.value)}
                                    >
                                        {FILE_CHOICES.map((t) => (
                                            <option key={t} value={t}>
                                                {PROFILE_LABEL[t]}
                                            </option>
                                        ))}
                                    </select>
                                </div>
                            ) : null}

                            {cleared.length > 0 ? (
                                <div className="fm-error" role="alert">
                                    <strong>
                                        {cleared.length} field(s) from the saved mapping no longer
                                        fit this file and have been cleared.
                                    </strong>
                                    <ul className="fm-cleared-list">
                                        {cleared.map((c) => (
                                            <li key={c.label}>
                                                <strong>{c.label}</strong> &mdash; {c.reason}
                                            </li>
                                        ))}
                                    </ul>
                                    Pick a column for each one below, then save the mapping again.
                                </div>
                            ) : null}

                            {columns.length === 0 && !loading ? (
                                <p className="fm-muted">
                                    This is the saved mapping. To change it, start an upload &mdash;
                                    the choices are built from the columns in the file you pick.
                                </p>
                            ) : null}

                            <table className="fm-table">
                                <thead>
                                    <tr>
                                        <th>What we store</th>
                                        <th>The column in your file</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {entries.map((entry) => (
                                        <MappingRow
                                            key={entry.field}
                                            entry={entry}
                                            source={draft[entry.field]}
                                            columns={columns}
                                            takenBy={takenBy}
                                            onChange={setField}
                                            registerRef={registerFieldRef}
                                            fieldError={fieldErrors[entry.field]}
                                        />
                                    ))}
                                </tbody>
                            </table>

                            <p className="fm-footnote">
                                Saving changes how every future file of this type is read, so any
                                date already matched has to be matched again before it can be
                                imported.
                            </p>
                        </>
                    ) : null}
                </div>

                {chosen ? (
                    <footer className="fm-panel-foot">
                        {uploadMode && columns.length > 0 ? (
                            <button
                                type="button"
                                className="fm-danger"
                                onClick={onClearAll}
                                disabled={clearing || saving || uploading || loading}
                                title="Delete the saved mapping for this file type and map every field again"
                            >
                                <svg width="13" height="13" viewBox="0 0 16 16" fill="none" aria-hidden="true">
                                    <path
                                        d="M3 4.5h10M6.5 4.5V3a1 1 0 0 1 1-1h1a1 1 0 0 1 1 1v1.5M4.5 4.5l.6 8.4a1 1 0 0 0 1 .9h3.8a1 1 0 0 0 1-.9l.6-8.4"
                                        stroke="currentColor"
                                        strokeWidth="1.2"
                                        strokeLinecap="round"
                                        strokeLinejoin="round"
                                    />
                                </svg>
                                {clearing ? "Clearing…" : "Clear all"}
                            </button>
                        ) : null}

                        <div className="fm-actions-right">
                            {showUpload ? (
                                <span className="fm-ready">Mapping saved &mdash; ready to upload.</span>
                            ) : null}

                            <button type="button" className="fm-ghost" onClick={requestClose}>
                                Close
                            </button>

                            {columns.length === 0 ? null : showUpload ? (
                                <button
                                    type="button"
                                    className="fm-primary"
                                    onClick={() => runUpload(false)}
                                    disabled={uploading || loading}
                                >
                                    {uploading ? "Uploading…" : "Upload"}
                                </button>
                            ) : (
                                <button
                                    type="button"
                                    className="fm-primary"
                                    onClick={onSave}
                                    disabled={!dirty || saving || loading}
                                >
                                    {saving ? "Saving…" : "Save mapping"}
                                </button>
                            )}
                        </div>
                    </footer>
                ) : null}
            </aside>
        </>
    );
}
