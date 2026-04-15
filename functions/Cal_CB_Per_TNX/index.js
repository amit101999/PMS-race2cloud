/**
 * Cal_CB_Per_TNX: Passbook-style cash balance from Transaction + Dividend_Record.
 * Inflows use SETDATE, outflows use TRANDATE. Writes to Cash_Balance_Per_Transaction.
 *
 * Processes all accounts in CLIENT_IDS from 2020-01-01 through 2026-12-31 in 6-month chunks.
 * Empty chunks are skipped (continue); all chunks through GLOBAL_END are visited.
 * Balance and sequence carry forward across chunks.
 */

const catalyst = require("zcatalyst-sdk-node");

const BATCH_SIZE = 300;
const CLIENT_IDS = [
"AYAN051","AYAN053","AYAN012","AYAN048","NROAYAN09","203NREAYAN","202NREAYAN","AYAN055","AYAN031","AYAN056",
];
const GLOBAL_START = "2020-01-01";
/** Fixed inclusive end date for all runs (process every half-year up to this date). */
const GLOBAL_END = "2026-12-31";
const CHUNK_MONTHS = 6;

const CASH_ADD = ["CS+", "SL+", "CSI", "IN1", "IN+", "DIO", "DI0", "DI1", "OI1", "DIS", "SQS"];
const CASH_SUBTRACT = ["BY-", "CS-", "MGF", "E22", "E01", "CUS", "E23", "MGE", "E10", "PRF", "NF-", "SQB", "TDO", "TDI"];

const esc = (s) => String(s ?? "").replace(/'/g, "''");

function generateDateChunks(startISO, endISO, months) {
  const chunks = [];
  let cursor = new Date(startISO + "T00:00:00Z");
  const end = new Date(endISO + "T00:00:00Z");

  while (cursor <= end) {
    const chunkStart = cursor.toISOString().split("T")[0];
    const next = new Date(cursor);
    next.setUTCMonth(next.getUTCMonth() + months);
    next.setUTCDate(next.getUTCDate() - 1);
    const chunkEnd = next > end
      ? end.toISOString().split("T")[0]
      : next.toISOString().split("T")[0];

    chunks.push({ start: chunkStart, end: chunkEnd });

    const nextStart = new Date(next);
    nextStart.setUTCDate(nextStart.getUTCDate() + 1);
    cursor = nextStart;
  }
  return chunks;
}

async function fetchBatched(zcql, baseQuery, mapper, rowKey = "Transaction") {
  const rows = [];
  let offset = 0;
  while (true) {
    const query = baseQuery + ` LIMIT ${BATCH_SIZE} OFFSET ${offset}`;
    const batch = await zcql.executeZCQLQuery(query);
    if (!batch || batch.length === 0) break;
    for (const row of batch) {
      const t = row[rowKey] || row;
      rows.push(mapper(t));
    }
    if (batch.length < BATCH_SIZE) break;
    offset += BATCH_SIZE;
  }
  return rows;
}

function fetchChunkEvents(zcql, accountCode, startDate, endDate) {
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

  const dividendQuery = `
    SELECT ROWID, ISIN, Security_Code, WS_Account_code, Holding, Rate, Dividend_Amount, PaymentDate
    FROM Dividend_Record
    WHERE WS_Account_code = '${esc(accountCode)}'
      AND PaymentDate >= '${startDate}' AND PaymentDate <= '${endDate}'
    ORDER BY PaymentDate ASC, ROWID ASC
  `;

  const mapInflow = (t) => ({
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
    clientCode: String(t.WS_client_id ?? "").trim() || "",
    price: Number(t.NETRATE) || 0,
    isin: String(t.ISIN ?? "").trim() || "",
    stt: Number(t.STT || t.Stt) || 0,
  });

  const mapOutflow = (t) => ({
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
    clientCode: String(t.WS_client_id ?? "").trim() || "",
    price: Number(t.NETRATE) || 0,
    isin: String(t.ISIN ?? "").trim() || "",
    stt: Number(t.STT || t.Stt) || 0,
  });

  const mapDividend = (d) => ({
    rowId: d.ROWID,
    trandate: (d.PaymentDate || "").toString().slice(0, 10),
    setdate: (d.PaymentDate || "").toString().slice(0, 10),
    executionPriority: 550,
    type: "DIVIDEND",
    securityName: String(d.Security_Code ?? "").trim() || "",
    netAmount: Number(d.Dividend_Amount) || 0,
    impactDate: (d.PaymentDate || "").toString().slice(0, 10),
    isInflow: true,
    qty: Number(d.Holding) || 0,
    clientCode: "",
    price: Number(d.Rate) || 0,
    isin: String(d.ISIN ?? "").trim() || "",
    stt: 0,
  });

  return Promise.all([
    fetchBatched(zcql, inflowQuery, mapInflow, "Transaction"),
    fetchBatched(zcql, outflowQuery, mapOutflow, "Transaction"),
    fetchBatched(zcql, dividendQuery, mapDividend, "Dividend_Record"),
  ]);
}

module.exports = async (jobRequest, context) => {
  const catalystApp = catalyst.initialize(context);
  const zcql = catalystApp.zcql();

  try {
    const chunks = generateDateChunks(GLOBAL_START, GLOBAL_END, CHUNK_MONTHS);
    console.log(`Cal_CB_Per_TNX: ${CLIENT_IDS.length} account(s), ${chunks.length} chunk(s) from ${GLOBAL_START} to ${GLOBAL_END}`);

    for (let ai = 0; ai < CLIENT_IDS.length; ai++) {
      const accountCode = CLIENT_IDS[ai];
      console.log(`\n===== Account ${ai + 1}/${CLIENT_IDS.length}: ${accountCode} =====`);

      let balance = 0;
      let sequence = 1;
      let isFirstChunk = true;
      let usedOpeningCsPlus = false;
      let totalInserted = 0;

      for (let ci = 0; ci < chunks.length; ci++) {
        const { start, end } = chunks[ci];
        console.log(`  Chunk ${ci + 1}/${chunks.length}: ${start} → ${end}`);

        const [inflowRows, outflowRows, dividendRows] = await fetchChunkEvents(zcql, accountCode, start, end);

        const allEvents = [...inflowRows, ...outflowRows, ...dividendRows];
        if (allEvents.length === 0) {
          console.log(`  Chunk ${ci + 1}: 0 events — skipping`);
          continue;
        }

        allEvents.sort((a, b) => {
          const dA = new Date(a.impactDate).getTime();
          const dB = new Date(b.impactDate).getTime();
          if (dA !== dB) return dA - dB;
          if (a.isInflow !== b.isInflow) return a.isInflow ? -1 : 1;
          if (a.executionPriority !== b.executionPriority) return a.executionPriority - b.executionPriority;
          return String(a.rowId).localeCompare(String(b.rowId));
        });

        if (isFirstChunk) {
          const firstImpactDate = allEvents[0].impactDate;
          const firstDayCsPlus = allEvents.find((e) => e.impactDate === firstImpactDate && e.type === "CS+");
          if (firstDayCsPlus) {
            balance = firstDayCsPlus.netAmount;
          }
          isFirstChunk = false;
        }

        for (const row of allEvents) {
          const { trandate, setdate, type, securityName, netAmount, isInflow, qty, price, isin, stt } = row;

          if (!usedOpeningCsPlus && type === "CS+") {
            usedOpeningCsPlus = true;
            balance = netAmount;
          } else if (isInflow) {
            balance += netAmount;
          } else {
            balance -= netAmount;
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
              ${Number(balance)},
              '${esc(securityName)}',
              '${esc(isin)}',
              ${Math.round(Number(qty ?? 0) || 0)},
              ${Number(netAmount)},
              ${Number(stt ?? 0)},
              ${sequence}
            )
          `);
          sequence++;
        }

        totalInserted += allEvents.length;
        console.log(`  Chunk ${ci + 1}: ${allEvents.length} event(s), balance=${balance.toFixed(2)}, seq=${sequence - 1}`);
      }

      console.log(`Account ${accountCode} done: ${totalInserted} row(s), final balance=${balance.toFixed(2)}`);
    }

    console.log(`\nCal_CB_Per_TNX completed for all ${CLIENT_IDS.length} accounts.`);
    context.closeWithSuccess();
  } catch (error) {
    console.error("Cal_CB_Per_TNX failed:", error);
    context.closeWithFailure();
  }
};
