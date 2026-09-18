/**
 * Custody Matching — user-defined field mapping (read, preview, save).
 *
 * Three profiles, one per file type, stored as small JSON objects in the bucket
 * outside the date folders. Until somebody saves one, every read returns the
 * built-in default, which reproduces the previously hardcoded column names
 * exactly — so this endpoint existing changes nothing on its own.
 *
 * Error bodies follow the same { success:false, code, message, hint } shape the
 * rest of the custody controllers use, so the screen's existing error renderer
 * works unchanged.
 */

import csvParser from "csv-parser";
import { Readable } from "stream";

import { FA_HEADERS } from "../../util/custodyMatch/constants.js";
import { detectCustodyFormat } from "../../util/custodyMatch/parseCustody.js";
import {
  FIELD_CATALOG,
  FORMAT_TO_PROFILE_TYPE,
  PROFILE_TYPE,
  PROFILE_TYPES,
  TRANSFORM,
  checkProfileComplete,
  defaultProfile,
  describeColumns,
  fromStoredProfile,
  isDefaultProfile,
  readHeaderColumns,
  toStoredProfile,
  validateProfile,
} from "../../util/custodyMatch/fieldMap.js";
import {
  SOURCE_BY,
} from "../../util/custodyMatch/fieldMap.js";
import {
  deleteFieldMapBlob,
  loadFieldMapProfiles,
  readAllFieldMapBlobs,
  readFieldMapBlob,
  writeFieldMapBlob,
} from "../../util/custodyMatch/storage.js";
import { supersedeRunsForDate } from "../../util/custodyMatch/runStore.js";
import { listStoredDates } from "../../util/custodyMatch/storage.js";

const fail = (res, status, body) => res.status(status).json({ success: false, ...body });

const noApp = (res) =>
  fail(res, 500, { code: "CATALYST_NOT_INITIALIZED", message: "Catalyst not initialized" });

const badType = (res, type) =>
  fail(res, 400, {
    code: "UNKNOWN_PROFILE_TYPE",
    message: `"${type}" is not a known file type.`,
    expected: PROFILE_TYPES,
  });

/**
 * field -> the column heading, as a plain string the screen can show in a text
 * box. Index-addressed entries are translated back to the heading that sits at
 * that position in the 26-column template, so the mapping screen can be a
 * simple "our field <- your heading" list without knowing the template.
 */
const headingsFor = (type, profile) => {
  const out = {};
  for (const [field, source] of Object.entries(profile || {})) {
    if (!source) continue;
    if (source.by === SOURCE_BY.INDEX) {
      out[field] =
        type === PROFILE_TYPE.FA && FA_HEADERS[source.index] !== undefined
          ? FA_HEADERS[source.index]
          : `column ${source.index + 1}`;
    } else {
      out[field] = source.name;
    }
  }
  return out;
};

/** What the screen needs to render one tab: the catalog, the saved mapping, and where it came from. */
const viewFor = (type, blob) => {
  const read = fromStoredProfile(type, blob);
  const builtIn = defaultProfile(type);
  return {
    type,
    fields: FIELD_CATALOG[type],
    profile: read.profile,
    // The shipped layout, so the screen can show what "go with the current"
    // actually means and offer a per-field revert.
    builtIn,
    // Both as plain headings, for the text boxes and the "Standard:" line.
    headings: headingsFor(type, read.profile),
    standardHeadings: headingsFor(type, builtIn),
    isDefault: read.isDefault,
    updatedAtMs: read.updatedAtMs,
    updatedBy: read.updatedBy,
    // Flagged separately from isDefault: a saved profile can still be identical
    // to the built-in one, and the screen should not claim it was customised.
    matchesBuiltIn: isDefaultProfile(type, read.profile),
    // Fields nobody has answered yet. A mapping stored before every field
    // became required loads fine and simply arrives with gaps, which is exactly
    // the state the screen renders as empty rows.
    missingFields: checkProfileComplete(type, read.profile).missing,
  };
};

/**
 * GET /api/custody-match/field-map
 *
 * All three profiles in one call. The mapping drawer needs every tab up front —
 * three round trips to render one panel is three chances to half-load it.
 */
export const getAllFieldMaps = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) return noApp(res);

    const blobs = await readAllFieldMapBlobs(app, PROFILE_TYPES);

    return res.json({
      success: true,
      transforms: TRANSFORM,
      profileTypes: PROFILE_TYPES,
      maps: PROFILE_TYPES.map((type) => viewFor(type, blobs[type])),
    });
  } catch (err) {
    console.error("getAllFieldMaps failed:", err);
    return fail(res, 500, {
      code: "FIELD_MAP_READ_FAILED",
      message: `Could not read the field mappings: ${err.message}`,
    });
  }
};

/** GET /api/custody-match/field-map/:type */
export const getFieldMap = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) return noApp(res);

    const type = String(req.params.type || "").toUpperCase();
    if (!PROFILE_TYPES.includes(type)) return badType(res, type);

    const blob = await readFieldMapBlob(app, type);
    return res.json({ success: true, transforms: TRANSFORM, map: viewFor(type, blob) });
  } catch (err) {
    console.error("getFieldMap failed:", err);
    return fail(res, 500, {
      code: "FIELD_MAP_READ_FAILED",
      message: `Could not read the field mapping: ${err.message}`,
    });
  }
};

/**
 * Supersede every run for a date still holding files.
 *
 * Called after a mapping changes, because a clean run produced under the old
 * mapping must not keep Gate 2 open for data the new one reads differently.
 * Best-effort on purpose: the mapping change has already happened by this point,
 * and failing the request would tell the user it had not. A stale run is a
 * smaller problem than a mapping change the user believes did not go through.
 */
const supersedeStoredDates = async (app) => {
  const superseded = [];
  const supersedeErrors = [];
  try {
    for (const date of await listStoredDates(app)) {
      try {
        await supersedeRunsForDate(app, date);
        superseded.push(date);
      } catch (e) {
        supersedeErrors.push({ date, message: e.message });
      }
    }
  } catch (e) {
    supersedeErrors.push({ date: null, message: e.message });
  }
  return { superseded, supersedeErrors };
};

/**
 * PUT /api/custody-match/field-map/:type
 * Body: { profile: { field: {by, name|index, transform} }, updatedBy? }
 *
 * A profile that cannot build the match key is refused. Saving one that can is
 * still a change to how every future file is READ, so every run for a date whose
 * files are still in the bucket is superseded: a clean run produced under the
 * old mapping must not keep Gate 2 open for data the new mapping reads
 * differently.
 */
export const saveFieldMap = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) return noApp(res);

    const type = String(req.params.type || "").toUpperCase();
    if (!PROFILE_TYPES.includes(type)) return badType(res, type);

    const profile = req.body?.profile;
    if (!profile || typeof profile !== "object") {
      return fail(res, 400, {
        code: "MISSING_PROFILE",
        message: "No mapping was sent.",
        hint: "Send { profile: { field: { by, name|index, transform } } }.",
      });
    }

    const check = validateProfile(type, profile);
    if (!check.ok) {
      return fail(res, 400, {
        code: "INVALID_PROFILE",
        message: `The mapping cannot be saved: ${check.errors.map((e) => e.message).join(" ")}`,
        mapErrors: check.errors,
        hint: "Every field marked required must point at a column.",
      });
    }

    /*
     * Completeness is enforced at save, not in validateProfile.
     *
     * validateProfile answers "can this mapping be used", which a profile saved
     * before every field became required still can. This answers "has the user
     * finished" — a stricter question, and the right one to ask of something
     * arriving from the mapping screen. Answering "not present in this file"
     * counts as answered, so a narrow file is still saveable.
     */
    const complete = checkProfileComplete(type, profile);
    if (!complete.ok) {
      return fail(res, 400, {
        code: "INCOMPLETE_PROFILE",
        message: `${complete.missing.length} field(s) have not been answered yet.`,
        missingFields: complete.missing,
        hint: "Pick a column for each one, or mark it as not present in this file.",
      });
    }

    const blob = toStoredProfile(type, profile, {
      updatedBy: req.body?.updatedBy || "",
      updatedAtMs: Date.now(),
    });
    const key = await writeFieldMapBlob(app, type, blob);

    const { superseded, supersedeErrors } = await supersedeStoredDates(app);

    return res.json({
      success: true,
      key,
      map: viewFor(type, blob),
      supersededDates: superseded,
      supersedeErrors,
      message:
        superseded.length > 0
          ? `Mapping saved. ${superseded.length} stored date(s) must be matched again before they can be imported.`
          : "Mapping saved.",
    });
  } catch (err) {
    console.error("saveFieldMap failed:", err);
    return fail(res, 500, {
      code: "FIELD_MAP_WRITE_FAILED",
      message: `Could not save the field mapping: ${err.message}`,
    });
  }
};

/**
 * DELETE /api/custody-match/field-map/:type
 *
 * Removes the saved mapping so the user can map every field again from scratch.
 * Only the one file type is touched. Afterwards reads fall back to the built-in
 * default, and the mapping screen shows every field as unanswered.
 *
 * Stored runs are superseded for the same reason Save does it: files already
 * matched were read under the mapping that no longer exists.
 */
export const deleteFieldMap = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) return noApp(res);

    const type = String(req.params.type || "").toUpperCase();
    if (!PROFILE_TYPES.includes(type)) return badType(res, type);

    const deleted = await deleteFieldMapBlob(app, type);

    // Nothing was stored, so no run was produced under a custom mapping.
    const { superseded, supersedeErrors } = deleted
      ? await supersedeStoredDates(app)
      : { superseded: [], supersedeErrors: [] };

    return res.json({
      success: true,
      deleted,
      map: viewFor(type, null),
      supersededDates: superseded,
      supersedeErrors,
      message:
        superseded.length > 0
          ? `Mapping cleared. ${superseded.length} stored date(s) must be matched again before they can be imported.`
          : "Mapping cleared.",
    });
  } catch (err) {
    console.error("deleteFieldMap failed:", err);
    return fail(res, 500, {
      code: "FIELD_MAP_DELETE_FAILED",
      message: `Could not clear the field mapping: ${err.message}`,
    });
  }
};

/**
 * Every data row as POSITIONAL arrays.
 *
 * All rows, not a sample: a column that is fine for five rows and holds "abc"
 * on row 600 must not be accepted by the mapping only to fail at upload.
 *
 * `headers: false` is the whole point. csv-parser keyed by heading would
 * collapse the FA template's three "filler" columns into one, and those are
 * exactly the columns the mapping screen exists to tell apart.
 *
 * Uses the same CSV reader the parsers use rather than splitting on the
 * separator, so a quoted comma inside a Description cell does not shift every
 * column to its right and make the type check judge the wrong data.
 */
const readDataRows = (buffer, separator) =>
  new Promise((resolve, reject) => {
    const rows = [];
    Readable.from(buffer)
      .pipe(csvParser({ headers: false, separator, skipLines: 1 }))
      // headers:false yields { "0": "...", "1": "..." } — restore the order.
      .on("data", (row) => rows.push(Object.keys(row).map((k) => row[k])))
      .on("end", () => resolve(rows))
      .on("error", reject);
  });

/**
 * POST /api/custody-match/field-map/preview  (multipart, field name "file")
 *
 * Reads row 1 plus every data row and hands back the columns, so the
 * mapping screen can offer the user's real headings as a dropdown AND say what
 * each column actually holds. The file is never stored: this is a look at the
 * shape, not an upload.
 *
 * Columns carry their position and a disambiguated label because the FA template
 * repeats the name "filler" three times and picking the right one is the point.
 */
export const previewHeaders = async (req, res) => {
  try {
    const file = req.files?.file;
    if (!file) {
      return fail(res, 400, {
        code: "NO_FILE",
        message: "No file was received.",
        hint: 'Attach the sample file as the form field "file".',
      });
    }

    const name = String(file.name || "");
    if (!/\.csv$/i.test(name)) {
      return fail(res, 400, {
        code: "NOT_CSV",
        message: `"${name}" is not a .csv file.`,
      });
    }

    const { separator, columns } = readHeaderColumns(file.data);
    if (columns.length === 0) {
      return fail(res, 400, {
        code: "NO_HEADER",
        message: "The first row is blank, so the columns cannot be read.",
        hint: "Row 1 must carry the column headings.",
      });
    }

    const requestedType = String(req.body?.type || "").toUpperCase();
    const asked = PROFILE_TYPES.includes(requestedType) ? requestedType : null;

    /*
     * What the columns say this file IS, as opposed to the slot it was dropped
     * into. A custodian file in the wrong slot mislabels the scheme on every
     * exception it goes on to produce, so the screen opens the detected layout
     * and lets the user override rather than discovering it at upload time.
     *
     * FA is never auto-detected: detectCustodyFormat only knows the three
     * custodian layouts, and the FA file arrives through its own control.
     */
    let detectedType = null;
    if (asked !== PROFILE_TYPE.FA) {
      try {
        /*
         * Detected against the saved mappings, exactly as the upload does.
         * A layout is identified by the column it reads the account code from,
         * and a custodian who calls that column CPCode rather than the built-in
         * CPCode_UCC is only recognised through their mapping. Without the
         * mappings a file in the wrong slot reads as "layout unknown", which
         * is indistinguishable from a file we have no opinion about.
         */
        const profiles = req.catalystApp ? await loadFieldMapProfiles(req.catalystApp) : null;
        const detected = detectCustodyFormat(file.data, { profiles });
        detectedType = detected.format ? FORMAT_TO_PROFILE_TYPE[detected.format] || null : null;
      } catch {
        // Detection is a convenience. A file it cannot place still maps fine
        // under whichever layout the user picked.
      }
    }

    const type = asked || detectedType;

    let sampleRows = [];
    try {
      sampleRows = await readDataRows(file.data, separator);
    } catch (sampleErr) {
      /*
       * Without rows every column reads as "unknown", which describeColumns
       * treats as compatible with everything. The mapping still works, and the
       * upload still checks every row, so this should not block the screen.
       */
      console.warn("previewHeaders: could not read data rows:", sampleErr.message);
    }

    const described = describeColumns(columns, sampleRows, { type });

    return res.json({
      success: true,
      fileName: name,
      separator,
      columns: described,
      sampleRowCount: sampleRows.length,
      duplicateNames: [...new Set(columns.filter((c) => c.duplicate).map((c) => c.name))],
      type,
      // Both reported so the screen can show "Detected: NRE/NRO — change".
      requestedType: asked,
      detectedType,
    });
  } catch (err) {
    console.error("previewHeaders failed:", err);
    return fail(res, 500, {
      code: "PREVIEW_FAILED",
      message: `Could not read the file's header row: ${err.message}`,
    });
  }
};

export default {
  getAllFieldMaps,
  getFieldMap,
  saveFieldMap,
  deleteFieldMap,
  previewHeaders,
};
