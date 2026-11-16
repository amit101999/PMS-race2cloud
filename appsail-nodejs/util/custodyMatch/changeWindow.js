/**
 * Where each trade date's change window (T+N) is kept, one per tab, in the
 * temporary-files bucket as { days, fixed }:
 *   reload/<date>/window.json      Equity tab
 *   reload-mf/<date>/window.json   Mutual Funds tab
 * `fixed` turns true at the tab's first commit of the date, so a commit on one
 * tab never fixes or locks the other tab's window. The rules that use it are in
 * equityReload.js.
 * AppSail only: storage.js is copied into the CustodyMatchRunner /
 * CustodyMatchCleanup functions, which do not need this.
 */

import { getBucket, getJson, objectExists, putJson } from "./storage.js";
import { DEFAULT_WINDOW_DAYS, isBookedMutualFund, parseWindowDays } from "./equityReload.js";

const windowKey = (date, mutualFund) => `${mutualFund ? "reload-mf" : "reload"}/${date}/window.json`;

const PAGE = 300;

/** Does the date have at least one booked trade of this tab? Stops at the first one found. */
const hasBookedTrades = async (catalystApp, date, mutualFund) => {
  const zcql = catalystApp.zcql();
  let lastRowId = "0";
  for (;;) {
    const rows = (
      await zcql.executeZCQLQuery(
        `SELECT ROWID, ISIN, EXCHG, Security_Type FROM Transaction WHERE TRANDATE = '${date}' AND ROWID > ${lastRowId} ORDER BY ROWID ASC LIMIT ${PAGE}`,
      )
    ).map((r) => r.Transaction || r);
    if (rows.some((row) => isBookedMutualFund(row) === mutualFund)) return true;
    if (rows.length < PAGE) return false;
    lastRowId = String(rows[rows.length - 1].ROWID);
  }
};

/**
 * The trade date's window record for one tab, or null while nothing is chosen
 * and nothing is booked. A date with this tab's trades booked before windows
 * existed counts as T+150, fixed. A read that fails is passed on: guessing could
 * open a date that is locked for good.
 *
 * @param {string} date YYYY-MM-DD, already checked by the caller
 * @param {{ mutualFund?: boolean }} [opts] true for the Mutual Funds tab
 */
export const readDateWindow = async (catalystApp, date, { mutualFund = false } = {}) => {
  const bucket = getBucket(catalystApp);
  const key = windowKey(date, mutualFund);
  if (await objectExists(bucket, key)) {
    const stored = await getJson(bucket, key);
    return { days: parseWindowDays(stored.days) || DEFAULT_WINDOW_DAYS, fixed: Boolean(stored.fixed) };
  }
  return (await hasBookedTrades(catalystApp, date, mutualFund)) ? { days: DEFAULT_WINDOW_DAYS, fixed: true } : null;
};

export const saveDateWindow = (catalystApp, date, record, { mutualFund = false } = {}) =>
  putJson(getBucket(catalystApp), windowKey(date, mutualFund), {
    days: record.days,
    fixed: record.fixed,
    savedAtMs: Date.now(),
  });
