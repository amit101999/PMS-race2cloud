import React, { useCallback, useEffect, useMemo, useState } from "react";

import {
    PROFILE_HELP,
    PROFILE_LABEL,
    SOURCE_BY,
    TRANSFORM_LABEL,
    getFieldMaps,
    saveFieldMap,
} from "./api";
import "./FieldMapping.css";

/**
 * Field Mapping — a panel on the Transaction Upload screen, opened from the
 * button in the top bar.
 *
 * Two steps. First, which of the three files you are mapping. Then, for that
 * file, one row per field we store, with the heading YOUR file uses beside it.
 *
 * The field list on the left is fixed and cannot be edited: five of those rows
 * build the match key (Actual Code + ISIN + Transaction Date + Transaction Type,
 * compared on Quantity), so renaming them would leave the comparison with
 * nothing to compare. The headings on the right are yours to type.
 */

const FILE_CHOICES = ["FA", "CUSTODY_RESIDENTIAL", "CUSTODY_NDPMS"];

const FILE_BLURB = {
    FA: "The daily transaction file you import. Whatever layout you map here is rewritten into the standard 26-column format before import, so nothing downstream changes.",
    CUSTODY_RESIDENTIAL: "The deal allocation file your custodian sends for Resident accounts.",
    CUSTODY_NDPMS: "The deal allocation file your custodian sends for NRE / NRO (NDPMS) accounts.",
};

const sameSource = (a, b) => {
    if (!a || !b) return a === b;
    if (a.by !== b.by) return false;
    if ((a.transform || "NONE") !== (b.transform || "NONE")) return false;
    return a.by === SOURCE_BY.INDEX ? a.index === b.index : a.name === b.name;
};

/** A field counts as mapped only when it actually names a column. */
const isFilled = (source) => {
    if (!source) return false;
    if (source.by === SOURCE_BY.INDEX) return Number.isInteger(source.index) && source.index >= 0;
    return String(source.name || "").trim() !== "";
};

/* ------------------------------------------------------------------ one row */

function MappingRow({ entry, source, savedHeading, standardHeading, onChange }) {
    const transforms = entry.allowTransforms || ["NONE"];

    /*
     * An index-addressed entry (the FA template's three "filler" columns) shows
     * the heading that sits at that position, but the entry STAYS index-based
     * until the user actually types over it. Converting it on load would let a
     * save that never touched this row move Net Amount onto the first "filler".
     */
    const value =
        source && source.by === SOURCE_BY.INDEX ? savedHeading || "" : source?.name || "";

    const isEdited = Boolean(standardHeading) && value !== standardHeading;

    return (
        <tr className={entry.required ? "fm-row fm-row--required" : "fm-row"}>
            <td className="fm-cell-field">
                <div className="fm-fieldname">
                    {entry.label}
                    {entry.required ? (
                        <span className="fm-req" title="Part of the match key — cannot be left blank">
                            required
                        </span>
                    ) : null}
                </div>
                <div className="fm-builtin">
                    Standard: <code>{standardHeading || "not mapped"}</code>
                </div>
            </td>

            <td className="fm-cell-source">
                <div className="fm-inputs">
                    <input
                        className="fm-text"
                        type="text"
                        placeholder={
                            entry.required
                                ? "Heading used in your file"
                                : "Heading used in your file (leave blank to skip)"
                        }
                        value={value}
                        onChange={(e) =>
                            onChange(entry.field, {
                                by: SOURCE_BY.NAME,
                                name: e.target.value,
                                transform: source?.transform || "NONE",
                            })
                        }
                    />

                    {isEdited ? (
                        <button
                            type="button"
                            className="fm-revert"
                            title="Put this field back to the standard heading"
                            onClick={() => onChange(entry.field, "__STANDARD__")}
                        >
                            revert
                        </button>
                    ) : null}
                </div>

                {source && transforms.length > 1 ? (
                    <select
                        className="fm-transform"
                        value={source.transform || "NONE"}
                        onChange={(e) =>
                            onChange(entry.field, { ...source, transform: e.target.value })
                        }
                    >
                        {transforms.map((t) => (
                            <option key={t} value={t}>
                                {TRANSFORM_LABEL[t] || t}
                            </option>
                        ))}
                    </select>
                ) : null}
            </td>
        </tr>
    );
}

/* ---------------------------------------------------------------- the panel */

export default function FieldMappingPanel({ open, onClose, onSaved }) {
    const [maps, setMaps] = useState([]);
    const [chosen, setChosen] = useState(null);
    const [draft, setDraft] = useState({});

    const [loading, setLoading] = useState(false);
    const [saving, setSaving] = useState(false);
    const [error, setError] = useState("");
    const [notice, setNotice] = useState("");

    const load = useCallback(async () => {
        setLoading(true);
        setError("");
        try {
            const res = await getFieldMaps();
            setMaps(res.maps || []);
            return res.maps || [];
        } catch (err) {
            setError(err.message);
            return [];
        } finally {
            setLoading(false);
        }
    }, []);

    useEffect(() => {
        if (open) load();
    }, [open, load]);

    const active = useMemo(() => maps.find((m) => m.type === chosen) || null, [maps, chosen]);

    /** Picking a file starts a fresh draft from whatever is saved for it. */
    const choose = (type) => {
        const map = maps.find((m) => m.type === type);
        setChosen(type);
        setDraft(map ? { ...map.profile } : {});
        setError("");
        setNotice("");
    };

    const backToChoice = () => {
        setChosen(null);
        setDraft({});
        setNotice("");
        setError("");
    };

    const requiredFields = useMemo(
        () => (active?.fields || []).filter((f) => f.required),
        [active],
    );
    const mappedRequired = requiredFields.filter((f) => isFilled(draft[f.field])).length;
    const allRequiredMapped =
        requiredFields.length > 0 && mappedRequired === requiredFields.length;

    const dirty = useMemo(() => {
        if (!active) return false;
        const keys = new Set([...Object.keys(active.profile || {}), ...Object.keys(draft)]);
        for (const k of keys) if (!sameSource(active.profile?.[k], draft[k])) return true;
        return false;
    }, [active, draft]);

    const setField = (field, source) => {
        setNotice("");
        setDraft((prev) => {
            const next = { ...prev };
            if (source === "__STANDARD__") {
                // Restore the shipped entry verbatim, index-addressing included.
                const std = active?.builtIn?.[field];
                if (std) next[field] = { ...std };
                else delete next[field];
            } else if (source) {
                next[field] = source;
            } else {
                delete next[field];
            }
            return next;
        });
    };

    /** Closing with unsaved edits is easy to do by mis-click; make it deliberate. */
    const requestClose = () => {
        if (dirty && !window.confirm("Close without saving? Your changes to this mapping will be lost.")) {
            return;
        }
        backToChoice();
        onClose();
    };

    const onSave = async () => {
        setSaving(true);
        setError("");
        setNotice("");
        try {
            /*
             * Drop blank headings. An empty box means "we do not read this
             * field", which is a valid choice for anything not required — and
             * required fields cannot reach here blank, because Save is disabled
             * until they are filled.
             */
            const clean = {};
            for (const [field, source] of Object.entries(draft)) {
                if (isFilled(source)) clean[field] = source;
            }

            const res = await saveFieldMap(chosen, clean, "");
            setNotice(res.message || "Mapping saved.");
            const fresh = await load();
            const updated = fresh.find((m) => m.type === chosen);
            if (updated) setDraft({ ...updated.profile });
            if (onSaved) onSaved(res);
        } catch (err) {
            setError(err.message);
        } finally {
            setSaving(false);
        }
    };

    const onUseStandard = () => {
        if (!active) return;
        setNotice("Standard headings loaded. Nothing is saved until you press Save mapping.");
        setError("");
        setDraft({ ...active.builtIn });
    };

    if (!open) return null;

    return (
        <>
            <div className="fm-scrim" onClick={requestClose} />
            <aside className="fm-panel" role="dialog" aria-label="Field mapping">
                <header className="fm-panel-head">
                    <div>
                        {chosen ? (
                            <button type="button" className="fm-back" onClick={backToChoice}>
                                ← All files
                            </button>
                        ) : null}
                        <h3>{chosen ? PROFILE_LABEL[chosen] : "Field Mapping"}</h3>
                        <p className="fm-sub">
                            {chosen
                                ? PROFILE_HELP[chosen]
                                : "Tell us the heading each field uses in your version of the file. Every scheme that sends this kind of file uses the same mapping."}
                        </p>
                    </div>
                    <button
                        type="button"
                        className="fm-close"
                        onClick={requestClose}
                        aria-label="Close"
                    >
                        ×
                    </button>
                </header>

                <div className="fm-panel-body">
                    {error ? <div className="fm-error">{error}</div> : null}
                    {notice ? <div className="fm-notice">{notice}</div> : null}
                    {loading ? <p className="fm-muted">Loading…</p> : null}

                    {/* ── step 1 ─────────────────────────────────────────── */}
                    {!chosen ? (
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
                                            <span className="fm-choice-blurb">{FILE_BLURB[type]}</span>
                                            <span
                                                className={
                                                    map && !map.matchesBuiltIn
                                                        ? "fm-badge fm-badge--custom"
                                                        : "fm-badge"
                                                }
                                            >
                                                {map && !map.matchesBuiltIn
                                                    ? "Custom headings"
                                                    : "Standard headings"}
                                            </span>
                                        </button>
                                    );
                                })}
                            </div>
                        </>
                    ) : null}

                    {/* ── step 2 ─────────────────────────────────────────── */}
                    {chosen && active ? (
                        <>
                            <div className="fm-toolbar">
                                <button type="button" className="fm-ghost" onClick={onUseStandard}>
                                    Use the standard headings
                                </button>
                            </div>

                            <div className={allRequiredMapped ? "fm-count fm-count--ok" : "fm-count"}>
                                {mappedRequired} of {requiredFields.length} required fields mapped
                                {dirty ? <span className="fm-dirty">unsaved changes</span> : null}
                            </div>

                            {active.updatedAtMs ? (
                                <p className="fm-muted fm-saved">
                                    Last saved {new Date(active.updatedAtMs).toLocaleString()}
                                    {active.updatedBy ? ` by ${active.updatedBy}` : ""}
                                </p>
                            ) : (
                                <p className="fm-muted fm-saved">Using the standard headings.</p>
                            )}

                            <table className="fm-table">
                                <thead>
                                    <tr>
                                        <th>What we store</th>
                                        <th>The heading in your file</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {active.fields.map((entry) => (
                                        <MappingRow
                                            key={entry.field}
                                            entry={entry}
                                            source={draft[entry.field]}
                                            savedHeading={active.headings?.[entry.field]}
                                            standardHeading={active.standardHeadings?.[entry.field]}
                                            onChange={setField}
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
                        <div className="fm-actions-right">
                            {!allRequiredMapped ? (
                                <span className="fm-blocker">
                                    Fill in all {requiredFields.length} required fields to save.
                                </span>
                            ) : null}
                            <button type="button" className="fm-ghost" onClick={requestClose}>
                                Close
                            </button>
                            <button
                                type="button"
                                className="fm-primary"
                                onClick={onSave}
                                disabled={!allRequiredMapped || saving || loading || !dirty}
                            >
                                {saving ? "Saving…" : "Save mapping"}
                            </button>
                        </div>
                    </footer>
                ) : null}
            </aside>
        </>
    );
}
