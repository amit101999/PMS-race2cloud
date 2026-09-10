/**
 * Shared progress tracking for corporate-action "apply" flows (stock split,
 * stock bonus) that fan out into background RebuildHoldingtable jobs.
 *
 * Model — no dedicated table:
 *   - A parent `Jobs` row, jobName = the track key, holds the overall state:
 *     RUNNING → COMPLETED | COMPLETED_WITH_ERRORS | ERROR.
 *   - Each background "piece" (one rebuild chunk, or a placeholder handed off by
 *     a nested job) registers a row in `JobStatusPerAccount`
 *     (jobName = track key, WS_Account_code = piece id) as RUNNING, then flips it
 *     to SUCCESS / FAILED when it ends.
 *   - Whichever piece finishes last (no RUNNING rows left for the key) settles
 *     the parent `Jobs` row.
 *
 * Look-ups are exact-match on `jobName` (many rows per key) — never a `LIKE`
 * prefix scan, which ZCQL cannot do.
 */

const STALE_MS = 60 * 60 * 1000; // 1h — a parent stuck RUNNING is force-settled to ERROR

const escSql = (v) => String(v ?? "").replace(/'/g, "''");

const parseCatalystTime = (ct) => {
  if (!ct) return 0;
  const ms = new Date(String(ct).replace(/:(\d{3})$/, ".$1")).getTime();
  return Number.isNaN(ms) ? 0 : ms;
};

/** Register a RUNNING piece row under `trackKey`. Best-effort (logs, never throws). */
export async function insertCaTrackPiece(zcql, trackKey, pieceId, tag = "CA_APPLY") {
  try {
    await zcql.executeZCQLQuery(
      `INSERT INTO JobStatusPerAccount (jobType, WS_Account_code, status, lastError, jobName) ` +
        `VALUES ('${escSql(tag)}', '${escSql(pieceId)}', 'RUNNING', '', '${escSql(trackKey)}')`,
    );
  } catch (e) {
    console.warn(`[caJobProgress] track piece insert failed (${pieceId}):`, e.message);
  }
}

/**
 * Read the overall state for one track key. Also self-settles: if the parent
 * still says RUNNING but no piece is RUNNING, it finalizes here (belt and
 * suspenders for a lost finalize race); if RUNNING past the stale window it is
 * force-marked ERROR.
 *
 * @returns {Promise<"NOT_STARTED"|"RUNNING"|"COMPLETED"|"COMPLETED_WITH_ERRORS"|"ERROR">}
 */
export async function readCaJobProgress(zcql, key) {
  const parentRows = await zcql.executeZCQLQuery(
    `SELECT status, CREATEDTIME FROM Jobs WHERE jobName = '${escSql(key)}' LIMIT 1`,
  );
  if (!parentRows?.length) return "NOT_STARTED";

  let status = String(parentRows[0].Jobs?.status ?? parentRows[0].status ?? "").toUpperCase();
  const createdTime = parentRows[0].Jobs?.CREATEDTIME ?? parentRows[0].CREATEDTIME;

  if (status === "RUNNING") {
    const running = await zcql.executeZCQLQuery(
      `SELECT status FROM JobStatusPerAccount WHERE jobName = '${escSql(key)}' AND status = 'RUNNING' LIMIT 1`,
    );
    if (!running?.length) {
      const failed = await zcql.executeZCQLQuery(
        `SELECT status FROM JobStatusPerAccount WHERE jobName = '${escSql(key)}' AND status = 'FAILED' LIMIT 1`,
      );
      status = failed?.length ? "COMPLETED_WITH_ERRORS" : "COMPLETED";
      try {
        await zcql.executeZCQLQuery(
          `UPDATE Jobs SET status = '${status}' WHERE jobName = '${escSql(key)}'`,
        );
      } catch (e) {
        console.warn("[caJobProgress] settle failed:", e.message);
      }
    } else if (Date.now() - parseCatalystTime(createdTime) > STALE_MS) {
      status = "ERROR";
      try {
        await zcql.executeZCQLQuery(
          `UPDATE Jobs SET status = 'ERROR' WHERE jobName = '${escSql(key)}'`,
        );
      } catch (e) {
        console.warn("[caJobProgress] stale-mark failed:", e.message);
      }
    }
  }

  return status;
}
