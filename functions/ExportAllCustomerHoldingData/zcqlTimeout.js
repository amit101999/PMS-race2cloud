/**
 * The Catalyst ZCQL client has been observed to hang indefinitely on some
 * calls instead of rejecting (see comments in holdingsFromTable.js and
 * index.js). A hung `await` never throws, so `withRetry` in index.js never
 * gets a chance to retry and the whole 15-minute Job just times out with no
 * further logs. Racing every query against a timeout turns a silent hang
 * into a real rejection that the existing retry/error-count/baton-pass logic
 * can act on.
 */
const DEFAULT_TIMEOUT_MS = 20_000;

async function queryWithTimeout(zcql, sql, timeoutMs = DEFAULT_TIMEOUT_MS) {
  let timer;
  try {
    return await Promise.race([
      zcql.executeZCQLQuery(sql),
      new Promise((_, reject) => {
        timer = setTimeout(
          () => reject(new Error(`ZCQL query timed out after ${timeoutMs}ms`)),
          timeoutMs
        );
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

module.exports = { queryWithTimeout };
