const { Readable } = require("stream");
const { fetchStockTransactions } = require("./transactions");
const { fetchBonusesForStock } = require("./Bonuses");
const { fetchSplitForStock } = require("./split");

const esc = (s) => String(s).replace(/'/g, "''");

const BUCKET_NAME = "upload-data-bucket";
const BULK_WRITE_MAX = 100000;
const POLL_INTERVAL_MS = 5000;
const POLL_MAX_WAIT_MS = 600000;

function isBuy(t) {
  return /^BY-|SQB|OPI/i.test(String(t));
}
function isSell(t) {
  return /^SL\+|SQS|OPO|NF-/i.test(String(t));
}

function normalizeDate(d) {
  if (!d) return "9999-12-31";
  const s = String(d).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  const [y, m, day] = s.split("-").map(Number);
  if (!y || !m || !day) return "9999-12-31";
  const fullYear = y < 100 ? 2000 + y : y;
  return `${fullYear}-${String(m).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

function getIsinsForAccount(zcql, accountCode) {
  return (async () => {
    const seen = new Set();
    const list = [];
    let offset = 0;
    const limit = 250;

    while (true) {
      try {
        const q = `
          SELECT ISIN FROM Transaction
          WHERE WS_Account_code = '${esc(accountCode)}'
          ORDER BY ISIN ASC
          LIMIT ${limit} OFFSET ${offset}
        `;
        const rows = await zcql.executeZCQLQuery(q);
        if (!rows || rows.length === 0) break;

        for (const row of rows) {
          const r = row.Transaction || row;
          const isin = (r.ISIN || "").toString().trim();
          if (isin && !seen.has(isin)) {
            seen.add(isin);
            list.push(isin);
          }
        }
        if (rows.length < limit) break;
        offset += rows.length;
      } catch (err) {
        console.error(`Error fetching ISINs for ${accountCode} at offset ${offset}:`, err);
        break;
      }
    }

    return list;
  })();
}

/**
 * Buy → settlement date (SETDATE); Sell → transaction date (TRANDATE).
 */
function getTxnEventDate(t) {
  const setDate = t.setdate || t.SETDATE;
  const tradeDate = t.trandate || t.TRANDATE;
  return isBuy(t.tranType ?? t.Tran_Type ?? "") ? setDate || tradeDate : tradeDate || setDate;
}

/**
 * Merge transactions, bonuses, splits; sort by date; compute running holdings.
 * Consistent with the Analytics holding page FIFO engine (quantity-only version).
 */
function calculateRunningQuantity(transactions, bonuses, splits) {
  const events = [];

  for (const t of transactions || []) {
    events.push({
      type: "TXN",
      date: normalizeDate(getTxnEventDate(t)),
      data: t,
    });
  }
  for (const b of bonuses || []) {
    events.push({
      type: "BONUS",
      date: normalizeDate(b.exDate || b.ExDate),
      data: b,
    });
  }
  for (const s of splits || []) {
    events.push({
      type: "SPLIT",
      date: normalizeDate(s.issueDate || s.date),
      data: s,
    });
  }

  events.sort((a, b) => {
    const d = a.date.localeCompare(b.date);
    if (d !== 0) return d;
    const order = { TXN: 0, BONUS: 1, SPLIT: 2 };
    return (order[a.type] ?? 3) - (order[b.type] ?? 3);
  });

  let holdings = 0;
  const output = [];

  for (const e of events) {
    if (e.type === "TXN") {
      const t = e.data;
      const qty = Math.abs(Number(t.qty ?? t.QTY) || 0);
      if (!qty) continue;

      const tranType = t.tranType ?? t.Tran_Type ?? "";
      const price = Number(t.netrate ?? t.NETRATE) || 0;
      const netAmount = Number(t.netAmount ?? t.Net_Amount) || 0;

      if (
        String(tranType).toUpperCase() === "OPI" &&
        qty == 1 &&
        price === 0 &&
        netAmount === 0
      ) {
        continue;
      }

      if (isBuy(tranType)) holdings += qty;
      if (isSell(tranType)) {
        holdings -= qty;
        if (holdings < 0) holdings = 0;
      }

      output.push({
        trandate: e.date,
        tranType,
        qty,
        holdings,
      });
    } else if (e.type === "BONUS") {
      const qty = Number(e.data.bonusShare ?? e.data.BonusShare) || 0;
      if (!qty) continue;
      holdings += qty;
      output.push({
        trandate: e.date,
        tranType: "BONUS",
        qty,
        holdings,
      });
    } else if (e.type === "SPLIT") {
      const r1 = Number(e.data.ratio1 ?? e.data.Ratio1) || 0;
      const r2 = Number(e.data.ratio2 ?? e.data.Ratio2) || 0;
      if (r1 && r2) holdings = holdings * (r2 / r1);
      if (holdings < 0) holdings = 0;
      output.push({
        trandate: e.date,
        tranType: "SPLIT",
        qty: 0,
        holdings,
      });
    }
  }

  return output;
}

function buildCsvContent(rows) {
  const header = "WS_Account_code,ISIN,TRANDATE,Tran_Type,QTY,HOLDINGS\n";
  const body = rows
    .map(
      (r) =>
        `${r.WS_Account_code},${r.ISIN},${r.TRANDATE},${r.Tran_Type},${r.QTY},${r.HOLDINGS}`
    )
    .join("\n");
  return header + body;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function pollBulkWriteJob(bulkWrite, jobId) {
  const start = Date.now();
  while (Date.now() - start < POLL_MAX_WAIT_MS) {
    await sleep(POLL_INTERVAL_MS);
    try {
      const res = await bulkWrite.getStatus(jobId);
      const st = (res.status || "").toUpperCase();
      console.log(`[Backfill] Bulk write job ${jobId} status: ${st}`);
      if (st === "COMPLETED") return res;
      if (st === "FAILED") {
        throw new Error(`Bulk write job ${jobId} failed: ${JSON.stringify(res)}`);
      }
    } catch (err) {
      if (err.message?.includes("failed")) throw err;
      console.error(`[Backfill] Error polling job ${jobId}:`, err);
    }
  }
  throw new Error(`Bulk write job ${jobId} timed out after ${POLL_MAX_WAIT_MS}ms`);
}

const ALL_ACCOUNT_CODES = ["AYAN002"];
// const ALL_ACCOUNT_CODES = [
//   "AYAN002","AYAN001","AYAN009","AYAN010","AYAN006","AYAN005","AYAN007","AYAN016",
//   "AYAN004","AYAN011","NROAYAN03","AYAN008","NROAYAN01","AYAN021","AYAN019","AYAN003",
//   "AYAN029","AYAN028","AYAN030","AYAN015","AYAN018","AYAN035","AYAN034","AYAN033",
//   "AYAN032","AYAN040","AYAN027","AYAN014","AYAN046","AYAN022","AYAN020","AYAN043",
//   "AYAN044","AYAN025","AYAN013","AYAN038","AYAN049","AYAN037","AYAN047","AYAN024",
//   "AYAN023","HCAYAN005","AYAN041","AYAN050","AYAN042","HCAYAN003","HCAYAN004","AYAN045",
//   "NROAYAN05","AYAN026","AYAN051","AYAN053","AYAN012","AYAN048","NROAYAN09","203NREAYAN",
//   "202NREAYAN","AYAN055","AYAN031","AYAN056","HCAYAN002","AYAN057","205NREAYAN","HCAYAN001",
//   "AYAN017","206NREAYAN","NROAYAN10","NROAYAN04","NROAYAN02","201NREAYAN","208NREAYAN",
//   "212NREAYAN","NROAYAN12","AYAN059","AYAN060","211NREAYAN","NROAYAN11","213NREAYAN",
//   "214NREAYAN","210NREAYAN","AYAN063","AYAN058","NROAYAN08","HCAYAN012","AYAN064",
//   "HCNRO01","NROAYAN07","AYAN065","AYAN039","HCAYAN006","HCAYAN010","HCAYAN013",
//   "HCAYAN011","HCAYAN008","AYAN036","AYAN066","AYAN067","AYAN068","HCAYAN009","AYAN052",
//   "IMFAYAN01","WAYAN01","WAYAN02","AYAN061","AYAN071","HCAYAN007","AYAN074","AYAN070",
//   "AYAN069","217NREAYAN","IMFAYAN03","HCAYAN015","IMFAYAN04","HCAYAN017","HCAYAN014",
//   "216NREAYAN","HCNRO03","218NREAYAN","AYAN077","HCNRO02","IMFAYAN06","209NREAYAN",
//   "AYAN076","AYAN083","IMFAYAN07","AYAN090","NROAYAN16","IMFAYAN02","AYAN091","AYAN092",
//   "AYAN093","HCAYAN021","AYAN089","AYAN082","219NREAYAN","HCAYAN019","AYAN094","AYAN095",
//   "NROAYAN17","NROAYAN14","AYAN087","AYAN075","AYAN098","AYAN099","HCAYAN022","AYAN096",
//   "AYAN097","AYAN086","222NREAYAN","NROAYAN19","223NREAYAN","IMFAYAN08","AYAN101","AYAN100",
//   "AYAN103","AYAN088","AYAN102","AYAN062","AYAN106","AYAN107","AYAN104","AYAN108",
//   "AYAN085","AYAN109","AYAN110","225NREAYAN","NROAYAN21","AYAN105","NROAYAN06","HCAYAN020",
//   "ELAYAN002","ELNRO01","ELNRO02","HCAYAN018","ELAYAN003","ELAYAN005","HCAYAN024",
//   "IMFAYAN11","226NREAYAN","NROAYAN22","ELAYAN009","AYAN114","HCAYAN025","AYAN080",
//   "ELNRO03","ELAYAN010","IMFAYAN12","IMFAYAN14","ELAYAN012","ELAYAN001","ELAYAN008",
//   "ELNRO05","ELNRO04","ELNRE01","ELAYAN017","TAYAN001","ELAYAN016","ELAYAN018","ELAYAN020",
//   "AYAN116","IMFAYAN15","AYAN118","IMFAYAN16","AYAN081","AYAN117","HCAYAN026","ELAYAN011",
//   "AYAN119","AYAN121","230NREAYAN","NROAYAN25","ELAYAN022","ELAYAN023","AYAN122",
//   "IMFAYAN18","IMFAYAN19","ELNRO06","ELAYAN021","228NREAYAN","NROAYAN24","TAYAN004",
//   "AYAN123","IMFAYAN21","TAYAN003","NROAYAN26","AYAN125","AYAN126","AYAN127","AYAN128",
//   "ELAYAN025","AYAN112","NROAYAN28","TNRO01","TAYAN005","TAYAN007","AYAN129","AYAN132",
//   "TNRE01","TAYAN008","TNRO02","ELNRE02","ELNRO07","AYAN133","HCAYAN029","ELAYAN028",
//   "231NREAYAN","AYAN134","TAYAN006","AYAN135","NROAYAN27","IMFAYAN22","TAYAN010","AYAN124",
//   "ELAYAN029","AYAN130","HCAYAN023","TAYAN013","AYAN111","TAYAN015","NROAYAN32",
//   "233NREAYAN","ELNRO09","TAYAN012","IMFAYAN10","IMFAYAN20","ELNRE04","AYAN140",
//   "ELAYAN030","IMFAYAN25","AYAN139","AYAN138","ELNRE05","ELAYAN031","AYAN141","AYAN144",
//   "AYAN142","HCNRO04","AYAN143","232NREAYAN","AYAN146","TAYAN018","TAYAN014","AYAN115",
//   "AYAN147","HCAYAN030","IMFAYAN27","AYAN148","237NREAYAN","AYAN150","AYAN153","ELAYAN035",
//   "AYAN131","AYAN152","ELAYAN006","NROAYAN34","AYAN154","NROAYAN33","234NREAYAN",
//   "239NREAYAN","ELAYAN036","AYAN155","AYAN158","HCAYAN032","AYAN156","AYAN160","TMAYAN001",
//   "HCAYAN033","AYAN159","240NREAYAN","AYAN164","AYAN162","AYAN157","ELAYAN037",
//   "241NREAYAN","ELPCNRE01","ELPCNRO01","AYAN163","AYAN166","HCAYAN036","HCAYAN034",
//   "AYAN168","HCAYAN035","ELAYAN038","AYAN171","TAYAN020","221NREAYAN","HCAYAN038",
//   "AYAN170","243NREAYAN","AYAN175","ELAYAN040","AYAN172","AYAN176","AYAN173","AYAN179",
//   "AYAN177","TAYAN021","AYAN178","NROAYAN31","ELAYAN041","NROAYAN41","ELNRO10","AYAN180",
//   "AYAN174","AYAN181","247NREAYAN","ELNRE06","AYAN182","ELNRO11","NROAYAN18","TNRO03",
//   "AYAN183","AYAN184","244NREAYAN","248NREAYAN","NROAYAN48","TMAYAN003","AYAN185",
//   "ELAYAN043","ELAYAN045","ELAYAN044","ELAYAN042","TAYAN019","ELNRO12","ELAYAN027",
//   "HCAYAN040","TAYAN025","HCAYAN037","TMAYAN002","TAYAN022","ELAYAN046","AYAN186",
//   "TAYAN023","TAYAN028","AYAN187","NROAYAN49","ELNRO13","HCAYAN042","AYAN188","HCAYAN043",
//   "AYAN189","HCAYAN044","AYAN194","NROAYAN50","TAYAN030","ELAYAN048","AYAN196","AYAN113",
//   "ELAYAN050","AYAN197","AYAN199","TAYAN029","ELAYAN051","AYAN198","AYAN195","ELAYAN032",
//   "HCAYAN047","AYAN200","IMFAYAN29","NROAYAN51","ELNRO14","ELAYAN052","AYAN201",
//   "IMFAYAN30","SVAYAN002","SVAYAN001","SVAYAN003","AYAN202","ELAYAN053","TAYAN031",
//   "HCAYAN049","250NREAYAN","ELNRE07","TAYAN032","HCAYAN050","ELAYAN054","HCAYAN048",
//   "HCNRE01","HCAYAN051","SVAYAN004","HCAYAN053","NROAYAN53","HCNRO05","ELAYAN055",
//   "AYAN204","ND010","ND07","HCAYAN054","ELAYAN056","AYAN206","AYAN207","IMFAYAN32",
//   "TMAYAN005","AYAN205","ND01","ND09","TMAYAN007","TMNRE01","HCAYAN056","HCAYAN057",
//   "TMAYAN008","TAYAN033","HCAYAN058","IMFAYAN33","HCAYAN061","HCAYAN059","252NREAYAN",
//   "SVAYAN005","ND03","HCNRE02",
// ];

async function runQuantityBackfill(catalystApp) {
  const zcql = catalystApp.zcql();
  const stratus = catalystApp.stratus();
  const datastore = catalystApp.datastore();
  const bucket = stratus.bucket(BUCKET_NAME);

  const totalAccounts = ALL_ACCOUNT_CODES.length;
  const allRows = [];

  /* ============ PHASE 1: Compute all holdings ============ */
  console.log(`[Backfill] Phase 1: Computing holdings for ${totalAccounts} accounts...`);

  for (let i = 0; i < totalAccounts; i++) {
    const accountCode = ALL_ACCOUNT_CODES[i];
    if (!accountCode) continue;

    console.log(`[Backfill] Processing account ${i + 1}/${totalAccounts}: ${accountCode}`);

    const isins = await getIsinsForAccount(zcql, accountCode);
    console.log(`[Backfill] Found ${isins.length} ISINs for ${accountCode}`);

    for (const isin of isins) {
      try {
        const transactions = await fetchStockTransactions({
          zcql,
          tableName: "Transaction",
          accountCode,
          isin,
        });
        const bonuses = await fetchBonusesForStock({
          zcql,
          accountCode,
          isin,
        });
        const splits = await fetchSplitForStock({
          zcql,
          isin,
          tableName: "Split",
        });

        const ledger = calculateRunningQuantity(transactions, bonuses, splits);

        for (const row of ledger) {
          allRows.push({
            WS_Account_code: accountCode,
            ISIN: isin,
            TRANDATE: row.trandate,
            Tran_Type: row.tranType,
            QTY: Number(row.qty) || 0,
            HOLDINGS: Number(row.holdings) || 0,
          });
        }
      } catch (isinErr) {
        console.error(
          `[Backfill] Error processing ISIN ${isin} for ${accountCode}:`,
          isinErr
        );
      }
    }
  }

  console.log(`[Backfill] Phase 1 complete. Total rows computed: ${allRows.length}`);

  if (!allRows.length) {
    console.log("[Backfill] No rows to write. Done.");
    return;
  }

  /* ============ PHASE 2: Build CSV(s) and upload to Stratus ============ */
  console.log("[Backfill] Phase 2: Building and uploading CSV files to Stratus...");

  const timestamp = Date.now();
  const csvFileNames = [];

  for (let start = 0; start < allRows.length; start += BULK_WRITE_MAX) {
    const chunk = allRows.slice(start, start + BULK_WRITE_MAX);
    const chunkIndex = Math.floor(start / BULK_WRITE_MAX);
    const fileName = `backfill/Daily_Holding_${timestamp}_part${chunkIndex}.csv`;

    const csvContent = buildCsvContent(chunk);
    const readableStream = Readable.from([csvContent]);

    console.log(
      `[Backfill] Uploading CSV part ${chunkIndex} (${chunk.length} rows, ~${Math.round(csvContent.length / 1024)} KB)`
    );

    await bucket.putObject(fileName, readableStream, {
      overwrite: true,
      contentType: "text/csv",
    });

    csvFileNames.push(fileName);
    console.log(`[Backfill] Uploaded: ${fileName}`);
  }

  /* ============ PHASE 3: Bulk write from each CSV file ============ */
  console.log("[Backfill] Phase 3: Starting bulk write jobs...");

  for (let i = 0; i < csvFileNames.length; i++) {
    const fileName = csvFileNames[i];
    console.log(`[Backfill] Creating bulk write job ${i + 1}/${csvFileNames.length} for: ${fileName}`);

    const bulkWrite = datastore
      .table("Daily_Holding_Quantity")
      .bulkJob("write");

    const job = await bulkWrite.createJob(
      { bucket_name: BUCKET_NAME, object_key: fileName },
      { operation: "insert" }
    );

    console.log(
      `[Backfill] Bulk write job created: ${job.job_id}, status: ${job.status}`
    );

    await pollBulkWriteJob(bulkWrite, job.job_id);
    console.log(`[Backfill] Bulk write job ${job.job_id} completed.`);
  }

  console.log(
    `[Backfill] All done. ${csvFileNames.length} CSV file(s) uploaded, ${allRows.length} total rows written.`
  );
}

module.exports = { runQuantityBackfill };
