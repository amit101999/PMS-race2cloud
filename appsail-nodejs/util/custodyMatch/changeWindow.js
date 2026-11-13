/**
 * Where each trade date's change window (T+N) is kept: reload/<date>/window.json
 * in the temporary-files bucket, as { days, fixed }. `fixed` turns true at the
 * date's first commit. The rules that use it are in equityReload.js.
 * AppSail only: storage.js is copied into the CustodyMatchRunner /
 * CustodyMatchCleanup functions, which do not need this.
 */

import { getBucket, getJson, objectExists, putJson } from "./storage.js";
import { DEFAULT_WINDOW_DAYS, parseWindowDays } from "./equityReload.js";

const windowKey = (date) => `reload/${date}/window.json`;

/**
 * The trade date's window record, or null while nothing is chosen and nothing is
 * booked. A date with trades booked before windows existed counts as T+150, fixed.
 * A read that fails is passed on: guessing could open a date that is locked for good.
 *
 * @param {string} date YYYY-MM-DD, already checked by the caller
 */
export const readDateWindow = async (catalystApp, date) => {
  const bucket = getBucket(catalystApp);
  const key = windowKey(date);
  if (await objectExists(bucket, key)) {
    const stored = await getJson(bucket, key);
    return { days: parseWindowDays(stored.days) || DEFAULT_WINDOW_DAYS, fixed: Boolean(stored.fixed) };
  }
  const booked = await catalystApp
    .zcql()
    .executeZCQLQuery(`SELECT ROWID FROM Transaction WHERE TRANDATE = '${date}' LIMIT 1`);
  return booked.length > 0 ? { days: DEFAULT_WINDOW_DAYS, fixed: true } : null;
};

export const saveDateWindow = (catalystApp, date, record) =>
  putJson(getBucket(catalystApp), windowKey(date), { days: record.days, fixed: record.fixed, savedAtMs: Date.now() });
