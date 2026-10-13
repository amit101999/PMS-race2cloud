/**
 * Cal_CB_Per_TNX: DELETE + full rebuild of Cash_Balance_Per_Transaction per account.
 *
 * - Reads cash-affecting rows from Transaction, plus RECEIVED Dividend_Record rows
 *   (the DELETE below also removes the DIVIDEND credits UpdateDividendData wrote,
 *   so the rebuild must put them back or the dividend cash is lost).
 * - DELETE all passbook rows for the account (repeat DELETE until COUNT=0; Catalyst ~300/round), then rebuild from 2020 onwards in 6-month chunks.
 * - Accounts: job param accountCodesJson, pairsJson (account column), or LEGACY_CLIENT_IDS.
 */

const catalyst = require("zcatalyst-sdk-node");

const BATCH_SIZE = 300;

const LEGACY_CLIENT_IDS = [
  
  ];
const GLOBAL_START = "2020-01-01";
// The last chunk has no end date, so trades after today (e.g. a T+1 settlement)
// and every future year are always included. A fixed end year silently dropped
// anything later from a rebuild.
const OPEN_END = "9999-12-31";
const CHUNK_MONTHS = 6;

const CASH_ADD = [
  "CS+",
  "SL+",
  "CSI",
  "IN1",
  "IN+",
  "DIO",
  "DI0",
  "DI1",
  "OI1",
  "DIS",
  "SQS",
];
const CASH_SUBTRACT = [
  "BY-",
  "CS-",
  "MGF",
  "E22",
  "E01",
  "CUS",
  "E23",
  "MGE",
  "E10",
  "PRF",
  "NF-",
  "SQB",
  "TDO",
  "TDI",
];

const esc = (s) => String(s ?? "").replace(/'/g, "''");

function getJobParams(jobRequest) {
  try {
    if (jobRequest && typeof jobRequest.getAllJobParams === "function") {
      return jobRequest.getAllJobParams() || {};
    }
  } catch (e) {
    console.warn("getJobParams:", e.message);
  }
  return {};
}

function parseAccountCodesFromJob(jobRequest) {
  try {
    const p = getJobParams(jobRequest);
    const raw = p.accountCodesJson ?? p.account_codes_json;
    if (raw == null || raw === "") return [];
    const arr = typeof raw === "string" ? JSON.parse(raw) : raw;
    if (!Array.isArray(arr) || arr.length === 0) return [];
    const out = [];
    const seen = new Set();
    for (const item of arr) {
      let acc = "";
      if (typeof item === "string") {
        acc = item.trim();
      } else if (Array.isArray(item) && item.length >= 1) {
        acc = String(item[0] ?? "").trim();
      } else if (item && typeof item === "object") {
        acc = String(
          item.wsAccountCode ?? item.account ?? item.WS_Account_code ?? "",
        ).trim();
      }
      if (!acc || seen.has(acc)) continue;
      seen.add(acc);
      out.push(acc);
    }
    return out.sort((a, b) => a.localeCompare(b));
  } catch (e) {
    console.warn("parseAccountCodesFromJob:", e.message);
    return [];
  }
}

function parseAccountCodesFromPairsJson(jobRequest) {
  try {
    const p = getJobParams(jobRequest);
    const raw = p.pairsJson ?? p.pairs_json;
    if (raw == null || raw === "") return [];
    const arr = typeof raw === "string" ? JSON.parse(raw) : raw;
    if (!Array.isArray(arr) || arr.length === 0) return [];
    const seen = new Set();
    for (const item of arr) {
      let acc = "";
      if (Array.isArray(item) && item.length >= 1) {
        acc = String(item[0] ?? "").trim();
      } else if (item && typeof item === "object") {
        acc = String(item.wsAccountCode ?? item.account ?? "").trim();
      }
      if (acc) seen.add(acc);
    }
    return [...seen].sort((a, b) => a.localeCompare(b));
  } catch (e) {
    console.warn("parseAccountCodesFromPairsJson:", e.message);
    return [];
  }
}

function sortCashEvents(allEvents) {
  allEvents.sort((a, b) => {
    const dA = new Date(a.impactDate).getTime();
    const dB = new Date(b.impactDate).getTime();
    if (dA !== dB) return dA - dB;
    // A dividend goes after the other entries of its record date — where
    // UpdateDividendData places it — so a rebuild reproduces the same passbook.
    const divA = a.type === "DIVIDEND";
    const divB = b.type === "DIVIDEND";
    if (divA !== divB) return divA ? 1 : -1;
    if (a.isInflow !== b.isInflow) return a.isInflow ? -1 : 1;
    if (a.executionPriority !== b.executionPriority) {
      return a.executionPriority - b.executionPriority;
    }
    return String(a.rowId).localeCompare(String(b.rowId));
  });
}

/* Cash deliberately does NOT follow the holdings date rule (trade date, OPI on
   settlement). It takes a conservative cash view: money arriving counts from its
   settlement date (mapInflow), money leaving from its trade date (mapOutflow).
   OPI / OPO move shares, not cash, so there is no OPI branch here.
   Keep in sync with the twin in functions/Cal_CB_Append_TxnUpload/index.js. */
function mapInflow(t) {
  return {
    rowId: t.ROWID,
    trandate: t.TRANDATE || t.Setdate || "",
    setdate: t.SETDATE || t.Setdate || t.TRANDATE || "",
    executionPriority: Number(t.executionPriority) ?? 999,
    type: t.Tran_Type || "",
    securityName: t.Security_Name || "",
    netAmount: Number(t.Net_Amount) || 0,
    impactDate: (t.SETDATE || t.Setdate || t.TRANDATE || "").toString().slice(0, 10),
    isInflow: true,
    qty: Number(t.QTY) || 0,
    price: Number(t.NETRATE) || 0,
    isin: String(t.ISIN ?? "").trim() || "",
    stt: Number(t.STT || t.Stt) || 0,
  };
}

function mapOutflow(t) {
  return {
    rowId: t.ROWID,
    trandate: t.TRANDATE || t.Setdate || "",
    setdate: t.SETDATE || t.Setdate || t.TRANDATE || "",
    executionPriority: Number(t.executionPriority) ?? 999,
    type: t.Tran_Type || "",
    securityName: t.Security_Name || "",
    netAmount: Number(t.Net_Amount) || 0,
    impactDate: (t.TRANDATE || t.Trandate || t.Setdate || "").toString().slice(0, 10),
    isInflow: false,
    qty: Number(t.QTY) || 0,
    price: Number(t.NETRATE) || 0,
    isin: String(t.ISIN ?? "").trim() || "",
    stt: Number(t.STT || t.Stt) || 0,
  };
}

async function fetchBatched(zcql, baseQuery, mapper) {
  const rows = [];
  // ZCQL's OFFSET is 1-based, so each page repeats the previous page's last
  // row. Skip it, or that transaction is applied to the cash balance twice.
  const seenRowIds = new Set();
  let offset = 0;
  while (true) {
    const query = baseQuery + ` LIMIT ${BATCH_SIZE} OFFSET ${offset}`;
    const batch = await zcql.executeZCQLQuery(query);
    if (!batch || batch.length === 0) break;
    for (const row of batch) {
      const t = row.Transaction || row;
      if (t.ROWID != null) {
        if (seenRowIds.has(t.ROWID)) continue;
        seenRowIds.add(t.ROWID);
      }
      rows.push(mapper(t));
    }
    if (batch.length < BATCH_SIZE) break;
    offset += BATCH_SIZE;
  }
  return rows;
}

/**
 * Every dividend already credited to the account, as passbook cash-in events.
 * Same row shape UpdateDividendData inserts: both dates = record date, amount =
 * cash actually credited (dividend minus tax), price = rate, quantity = holding.
 * Paged by ROWID keyset (ZCQL OFFSET is 1-based and repeats rows).
 */
async function fetchReceivedDividendEvents(zcql, accountCode) {
  const events = [];
  let lastRowId = "0";
  while (true) {
    const batch = await zcql.executeZCQLQuery(`
      SELECT ROWID, ISIN, RecordDate, Rate, Holding, Dividend_Amount, Tax_Amount, Net_Amount, Security_Code
      FROM Dividend_Record
      WHERE WS_Account_code = '${esc(accountCode)}'
        AND Status = 'RECEIVED'
        AND ROWID > ${lastRowId}
      ORDER BY ROWID ASC
      LIMIT ${BATCH_SIZE}
    `);
    if (!batch || batch.length === 0) break;
    for (const row of batch) {
      const d = row.Dividend_Record || row;
      lastRowId = String(d.ROWID);
      const recordDate = String(d.RecordDate || "").slice(0, 10);
      if (!recordDate) continue;
      const credited =
        d.Net_Amount != null && d.Net_Amount !== ""
          ? Number(d.Net_Amount)
          : (Number(d.Dividend_Amount) || 0) - (Number(d.Tax_Amount) || 0);
      events.push({
        rowId: d.ROWID,
        trandate: recordDate,
        setdate: recordDate,
        executionPriority: 999,
        type: "DIVIDEND",
        securityName: d.Security_Code || "",
        netAmount: Math.round((Number(credited) || 0) * 100) / 100,
        impactDate: recordDate,
        isInflow: true,
        qty: Number(d.Holding) || 0,
        price: Number(d.Rate) || 0,
        isin: String(d.ISIN ?? "").trim(),
        stt: 0,
      });
    }
    if (batch.length < BATCH_SIZE) break;
  }
  return events;
}

function generateDateChunks(startISO, endISO, months) {
  const chunks = [];
  let cursor = new Date(startISO + "T00:00:00Z");
  const end = new Date(endISO + "T00:00:00Z");

  while (cursor <= end) {
    const chunkStart = cursor.toISOString().split("T")[0];
    const next = new Date(cursor);
    next.setUTCMonth(next.getUTCMonth() + months);
    next.setUTCDate(next.getUTCDate() - 1);
    const chunkEnd =
      next > end ? end.toISOString().split("T")[0] : next.toISOString().split("T")[0];

    chunks.push({ start: chunkStart, end: chunkEnd });

    const nextStart = new Date(next);
    nextStart.setUTCDate(nextStart.getUTCDate() + 1);
    cursor = nextStart;
  }
  return chunks;
}

async function fetchChunkEvents(zcql, accountCode, startDate, endDate) {
  const inflowTypesList = CASH_ADD.map((t) => `'${esc(t)}'`).join(", ");
  const outflowTypesList = CASH_SUBTRACT.map((t) => `'${esc(t)}'`).join(", ");

  const inflowQuery = `
    SELECT ROWID, TRANDATE, SETDATE, executionPriority, Tran_Type, Security_Name, Net_Amount, QTY, WS_client_id, NETRATE, ISIN, STT
    FROM Transaction
    WHERE WS_Account_code = '${esc(accountCode)}'
      AND Tran_Type IN (${inflowTypesList})
      AND SETDATE >= '${startDate}' AND SETDATE <= '${endDate}'
    ORDER BY SETDATE ASC, executionPriority ASC, ROWID ASC
  `;

  const outflowQuery = `
    SELECT ROWID, TRANDATE, SETDATE, executionPriority, Tran_Type, Security_Name, Net_Amount, QTY, WS_client_id, NETRATE, ISIN, STT
    FROM Transaction
    WHERE WS_Account_code = '${esc(accountCode)}'
      AND Tran_Type IN (${outflowTypesList})
      AND TRANDATE >= '${startDate}' AND TRANDATE <= '${endDate}'
    ORDER BY TRANDATE ASC, executionPriority ASC, ROWID ASC
  `;

  const [inflowRows, outflowRows] = await Promise.all([
    fetchBatched(zcql, inflowQuery, mapInflow),
    fetchBatched(zcql, outflowQuery, mapOutflow),
  ]);

  return { inflowRows, outflowRows };
}

async function insertPassbookEvents(zcql, accountCode, events, state) {
  let { balanceP, sequence, usedOpeningCsPlus } = state;
  let inserted = 0;

  for (const row of events) {
    const { trandate, setdate, type, securityName, netAmount, isInflow, qty, price, isin, stt } =
      row;

    const sttP = Math.round(Math.abs(Number(stt ?? 0)) * 100) || 0;
    const amtP = Math.round(Math.abs(netAmount) * 100);

    if (!usedOpeningCsPlus && type === "CS+") {
      usedOpeningCsPlus = true;
      balanceP = amtP - sttP;
    } else if (isInflow) {
      balanceP += amtP - sttP;
    } else {
      balanceP -= amtP + sttP;
    }

    const txDate = String(trandate).slice(0, 10);
    const setDateStr = String(setdate).slice(0, 10);

    await zcql.executeZCQLQuery(`
      INSERT INTO Cash_Balance_Per_Transaction
      (Account_Code, Transaction_Type, Transaction_Date, Settlement_Date, Price, Cash_Balance, Security_Name, ISIN, Quantity, Total_Amount, STT, Sequence)
      VALUES (
        '${esc(accountCode)}',
        '${esc(type)}',
        '${txDate}',
        '${setDateStr}',
        ${Number(price)},
        ${Number((balanceP / 100).toFixed(2))},
        '${esc(securityName)}',
        '${esc(isin)}',
        ${Math.round(Number(qty ?? 0) || 0)},
        ${Number(netAmount)},
        ${Number(stt ?? 0)},
        ${sequence + 1}
      )
    `);
    sequence++;
    inserted++;
  }

  return {
    inserted,
    finalBalance: Number((balanceP / 100).toFixed(2)),
    balanceP,
    sequence,
    usedOpeningCsPlus,
  };
}

const DELETE_LOOP_MAX_ROUNDS = 500;

async function countPassbookRowsForAccount(zcql, accountCode) {
  const rows = await zcql.executeZCQLQuery(`
    SELECT COUNT(ROWID) FROM Cash_Balance_Per_Transaction
    WHERE Account_Code = '${esc(accountCode)}'
  `);
  const r = rows?.[0]?.Cash_Balance_Per_Transaction || rows?.[0] || {};
  return Number(
    r["COUNT(ROWID)"] ?? r.cnt ?? r["count"] ?? Object.values(r)[0] ?? 0,
  );
}

/**
 * Catalyst caps ~300 rows per DELETE — repeat plain DELETE until COUNT is 0.
 */
async function deleteCashPassbookForAccount(zcql, accountCode) {
  let rounds = 0;
  let remaining = await countPassbookRowsForAccount(zcql, accountCode);
  const initial = remaining;

  if (remaining === 0) {
    console.log(`[${accountCode}] passbook delete: already empty`);
    return { deletedRounds: 0, initialCount: 0 };
  }

  while (remaining > 0) {
    rounds++;
    if (rounds > DELETE_LOOP_MAX_ROUNDS) {
      throw new Error(
        `Passbook delete for ${accountCode} exceeded ${DELETE_LOOP_MAX_ROUNDS} rounds (${remaining} row(s) left)`,
      );
    }

    await zcql.executeZCQLQuery(`
      DELETE FROM Cash_Balance_Per_Transaction
      WHERE Account_Code = '${esc(accountCode)}'
    `);

    const after = await countPassbookRowsForAccount(zcql, accountCode);
    if (after >= remaining) {
      throw new Error(
        `Passbook delete stalled for ${accountCode}: ${remaining} row(s) before DELETE, ${after} after`,
      );
    }
    remaining = after;
  }

  console.log(
    `[${accountCode}] passbook delete done: cleared ${initial} row(s) in ${rounds} DELETE round(s)`,
  );
  return { deletedRounds: rounds, initialCount: initial };
}

/** DELETE all passbook rows for account, then full rebuild from Transaction + received dividends (6-month chunks from 2020). */
async function rebuildCashPassbookForAccount(zcql, accountCode) {
  // Read the dividends before deleting anything, so a read failure leaves the
  // existing passbook untouched.
  const dividendEvents = await fetchReceivedDividendEvents(zcql, accountCode);

  await deleteCashPassbookForAccount(zcql, accountCode);

  const verifyEmpty = await countPassbookRowsForAccount(zcql, accountCode);
  if (verifyEmpty > 0) {
    throw new Error(
      `Passbook not empty after delete for ${accountCode}: ${verifyEmpty} row(s) remain`,
    );
  }

  const today = new Date().toISOString().split("T")[0];
  const chunks = generateDateChunks(GLOBAL_START, today, CHUNK_MONTHS);
  chunks[chunks.length - 1].end = OPEN_END;
  let state = { balanceP: 0, sequence: 0, usedOpeningCsPlus: false };
  let isFirstChunk = true;
  let totalInserted = 0;

  for (let ci = 0; ci < chunks.length; ci++) {
    const { start, end } = chunks[ci];

    const { inflowRows, outflowRows } = await fetchChunkEvents(
      zcql,
      accountCode,
      start,
      end,
    );

    const chunkDividends = dividendEvents.filter(
      (e) => e.impactDate >= start && e.impactDate <= end,
    );
    const allEvents = [...inflowRows, ...outflowRows, ...chunkDividends];
    if (allEvents.length === 0) {
      continue;
    }

    sortCashEvents(allEvents);

    if (isFirstChunk) {
      const firstImpactDate = allEvents[0].impactDate;
      const firstDayCsPlus = allEvents.find(
        (e) => e.impactDate === firstImpactDate && e.type === "CS+",
      );
      if (firstDayCsPlus) {
        state.balanceP = Math.round(Math.abs(firstDayCsPlus.netAmount) * 100);
        state.usedOpeningCsPlus = false;
      }
      isFirstChunk = false;
    }

    const result = await insertPassbookEvents(zcql, accountCode, allEvents, state);
    state = {
      balanceP: result.balanceP,
      sequence: result.sequence,
      usedOpeningCsPlus: result.usedOpeningCsPlus,
    };
    totalInserted += result.inserted;
  }

  return {
    inserted: totalInserted,
    finalBalance: Number((state.balanceP / 100).toFixed(2)),
  };
}

function resolveAccountList(jobRequest) {
  let accounts = parseAccountCodesFromJob(jobRequest);
  if (accounts.length === 0) {
    accounts = parseAccountCodesFromPairsJson(jobRequest);
  }
  if (accounts.length > 0) {
    return { accounts, mode: "scoped" };
  }
  if (LEGACY_CLIENT_IDS.length > 0) {
    return { accounts: [...LEGACY_CLIENT_IDS], mode: "legacy" };
  }
  return { accounts: [], mode: "empty" };
}

module.exports = async (jobRequest, context) => {
  const catalystApp = catalyst.initialize(context);
  const zcql = catalystApp.zcql();

  const startedAt = Date.now();
  const { accounts, mode } = resolveAccountList(jobRequest);
  const counters = { accounts: 0, rows: 0, errors: 0 };

  try {
    if (accounts.length === 0) {
      console.warn("Cal_CB_Per_TNX: no accounts to process — exiting.");
      context.closeWithSuccess();
      return;
    }

    console.log(
      `Cal_CB_Per_TNX: ${accounts.length} account(s) | mode=${mode} | run=fullRebuild (Transaction + received dividends)`,
    );

    for (let ai = 0; ai < accounts.length; ai++) {
      const accountCode = accounts[ai];
      counters.accounts++;
      console.log(`\n===== Account ${ai + 1}/${accounts.length}: ${accountCode} =====`);

      try {
        const result = await rebuildCashPassbookForAccount(zcql, accountCode);
        counters.rows += result.inserted;

        console.log(
          `Account ${accountCode} done: ${result.inserted} row(s) inserted (full rebuild), ` +
            `final balance=${result.finalBalance}`,
        );
      } catch (err) {
        counters.errors++;
        console.error(`[${accountCode}] cash passbook failed:`, err.message);
      }
    }

    console.log(
      `\nCal_CB_Per_TNX completed in ${Date.now() - startedAt}ms: ` +
        `${counters.accounts} account(s), ${counters.rows} row(s) inserted, ` +
        `${counters.errors} error(s).`,
    );

    if (counters.errors > 0) {
      context.closeWithFailure();
      return;
    }
    context.closeWithSuccess();
  } catch (error) {
    console.error("Cal_CB_Per_TNX failed:", error);
    context.closeWithFailure();
  }
};
