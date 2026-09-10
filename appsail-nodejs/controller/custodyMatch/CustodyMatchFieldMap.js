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

import { FA_HEADERS } from "../../util/custodyMatch/constants.js";
import {
  FIELD_CATALOG,
  PROFILE_TYPE,
  PROFILE_TYPES,
  TRANSFORM,
  defaultProfile,
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

    const blob = toStoredProfile(type, profile, {
      updatedBy: req.body?.updatedBy || "",
      updatedAtMs: Date.now(),
    });
    const key = await writeFieldMapBlob(app, type, blob);

    /*
     * Supersede every date still holding files. Best-effort on purpose: the
     * mapping IS saved by this point, and failing the request would tell the
     * user their edit was lost when it was not. A stale run is a smaller problem
     * than a mapping the user believes did not save.
     */
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
 * POST /api/custody-match/field-map/preview  (multipart, field name "file")
 *
 * Reads ONLY row 1 and hands back the columns, so the mapping screen offers the
 * user's real headings instead of asking them to type names. The file is never
 * stored: this is a look at the shape, not an upload.
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
    const type = PROFILE_TYPES.includes(requestedType) ? requestedType : null;

    return res.json({
      success: true,
      fileName: name,
      separator,
      columns,
      duplicateNames: [...new Set(columns.filter((c) => c.duplicate).map((c) => c.name))],
      // When the caller says which layout this sample is for, suggest a mapping
      // by matching headings against the built-in names. The user still confirms.
      suggested: type ? suggestProfile(type, columns) : null,
      type,
    });
  } catch (err) {
    console.error("previewHeaders failed:", err);
    return fail(res, 500, {
      code: "PREVIEW_FAILED",
      message: `Could not read the file's header row: ${err.message}`,
    });
  }
};

/**
 * Seed a mapping from a sample file by matching each field's default column name
 * against the file's headings. Fields with no match are left unset so the screen
 * shows them as still needing a choice, rather than guessing.
 */
const suggestProfile = (type, columns) => {
  const norm = (h) =>
    String(h || "").trim().toLowerCase().replace(/[\s._\-/()]+/g, "");
  const byNorm = new Map();
  columns.forEach((c) => {
    if (!byNorm.has(norm(c.name))) byNorm.set(norm(c.name), c);
  });

  const base = defaultProfile(type);
  const out = {};

  for (const [field, source] of Object.entries(base)) {
    if (source.by === "name") {
      const hit = byNorm.get(norm(source.name));
      if (hit) out[field] = { by: "name", name: hit.name, transform: source.transform };
    } else if (source.index < columns.length) {
      // Index-addressed defaults only carry over when the file is at least as
      // wide as the template; otherwise the position means something else.
      out[field] = { by: "index", index: source.index, transform: source.transform };
    }
  }

  return out;
};

export default {
  getAllFieldMaps,
  getFieldMap,
  saveFieldMap,
  previewHeaders,
};
