/**
 * Cal_CB_Per_TNX: Passbook-style cash balance from Transaction table.
 * Two queries: inflow by SETDATE, outflow by TRANDATE.
 * Exports passbook data to CSV, uploads to Stratus bucket "passbook-files", then inserts into Cash_Balance_Per_Transaction.
 * Holding column is not inserted (left as null for future use).
 *
 * @param {import("./types/job").JobRequest} jobRequest
 * @param {import("./types/job").Context} context
 */

const { Readable } = require("stream");
const catalyst = require("zcatalyst-sdk-node");

/** Escape a value for CSV (quote if contains comma, newline, or double quote) */
function csvEscape(val) {
  const s = String(val ?? "");
  if (/[,"\r\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}
console.log("Cal_CB_Per_TNX started");
const ALL_ACCOUNT_CODES = ["AYAN002"];
// const ALL_ACCOUNT_CODES = [
//   "AYAN002", "AYAN001", "AYAN009", "AYAN010", "AYAN006", "AYAN005", "AYAN007", "AYAN016",
//   "AYAN004", "AYAN011", "NROAYAN03", "AYAN008", "NROAYAN01", "AYAN021", "AYAN019", "AYAN003",
//   "AYAN029", "AYAN028", "AYAN030", "AYAN015", "AYAN018", "AYAN035", "AYAN034", "AYAN033",
//   "AYAN032", "AYAN040", "AYAN027", "AYAN014", "AYAN046", "AYAN022", "AYAN020", "AYAN043",
//   "AYAN044", "AYAN025", "AYAN013", "AYAN038", "AYAN049", "AYAN037", "AYAN047", "AYAN024",
//   "AYAN023", "HCAYAN005", "AYAN041", "AYAN050", "AYAN042", "HCAYAN003", "HCAYAN004", "AYAN045",
//   "NROAYAN05", "AYAN026", "AYAN051", "AYAN053", "AYAN012", "AYAN048", "NROAYAN09", "203NREAYAN",
//   "202NREAYAN", "AYAN055", "AYAN031", "AYAN056", "HCAYAN002", "AYAN057", "205NREAYAN", "HCAYAN001",
//   "AYAN017", "206NREAYAN", "NROAYAN10", "NROAYAN04", "NROAYAN02", "201NREAYAN", "208NREAYAN",
//   "212NREAYAN", "NROAYAN12", "AYAN059", "AYAN060", "211NREAYAN", "NROAYAN11", "213NREAYAN",
//   "214NREAYAN", "210NREAYAN", "AYAN063", "AYAN058", "NROAYAN08", "HCAYAN012", "AYAN064",
//   "HCNRO01", "NROAYAN07", "AYAN065", "AYAN039", "HCAYAN006", "HCAYAN010", "HCAYAN013",
//   "HCAYAN011", "HCAYAN008", "AYAN036", "AYAN066", "AYAN067", "AYAN068", "HCAYAN009", "AYAN052",
//   "IMFAYAN01", "WAYAN01", "WAYAN02", "AYAN061", "AYAN071", "HCAYAN007", "AYAN074", "AYAN070",
//   "AYAN069", "217NREAYAN", "IMFAYAN03", "HCAYAN015", "IMFAYAN04", "HCAYAN017", "HCAYAN014",
//   "216NREAYAN", "HCNRO03", "218NREAYAN", "AYAN077", "HCNRO02", "IMFAYAN06", "209NREAYAN",
//   "AYAN076", "AYAN083", "IMFAYAN07", "AYAN090", "NROAYAN16", "IMFAYAN02", "AYAN091", "AYAN092",
//   "AYAN093", "HCAYAN021", "AYAN089", "AYAN082", "219NREAYAN", "HCAYAN019", "AYAN094", "AYAN095",
//   "NROAYAN17", "NROAYAN14", "AYAN087", "AYAN075", "AYAN098", "AYAN099", "HCAYAN022", "AYAN096",
//   "AYAN097", "AYAN086", "222NREAYAN", "NROAYAN19", "223NREAYAN", "IMFAYAN08", "AYAN101", "AYAN100",
//   "AYAN103", "AYAN088", "AYAN102", "AYAN062", "AYAN106", "AYAN107", "AYAN104", "AYAN108",
//   "AYAN085", "AYAN109", "AYAN110", "225NREAYAN", "NROAYAN21", "AYAN105", "NROAYAN06", "HCAYAN020",
//   "ELAYAN002", "ELNRO01", "ELNRO02", "HCAYAN018", "ELAYAN003", "ELAYAN005", "HCAYAN024",
//   "IMFAYAN11", "226NREAYAN", "NROAYAN22", "ELAYAN009", "AYAN114", "HCAYAN025", "AYAN080",
//   "ELNRO03", "ELAYAN010", "IMFAYAN12", "IMFAYAN14", "ELAYAN012", "ELAYAN001", "ELAYAN008",
//   "ELNRO05", "ELNRO04", "ELNRE01", "ELAYAN017", "TAYAN001", "ELAYAN016", "ELAYAN018", "ELAYAN020",
//   "AYAN116", "IMFAYAN15", "AYAN118", "IMFAYAN16", "AYAN081", "AYAN117", "HCAYAN026", "ELAYAN011",
//   "AYAN119", "AYAN121", "230NREAYAN", "NROAYAN25", "ELAYAN022", "ELAYAN023", "AYAN122",
//   "IMFAYAN18", "IMFAYAN19", "ELNRO06", "ELAYAN021", "228NREAYAN", "NROAYAN24", "TAYAN004",
//   "AYAN123", "IMFAYAN21", "TAYAN003", "NROAYAN26", "AYAN125", "AYAN126", "AYAN127", "AYAN128",
//   "ELAYAN025", "AYAN112", "NROAYAN28", "TNRO01", "TAYAN005", "TAYAN007", "AYAN129", "AYAN132",
//   "TNRE01", "TAYAN008", "TNRO02", "ELNRE02", "ELNRO07", "AYAN133", "HCAYAN029", "ELAYAN028",
//   "231NREAYAN", "AYAN134", "TAYAN006", "AYAN135", "NROAYAN27", "IMFAYAN22", "TAYAN010", "AYAN124",
//   "ELAYAN029", "AYAN130", "HCAYAN023", "TAYAN013", "AYAN111", "TAYAN015", "NROAYAN32",
//   "233NREAYAN", "ELNRO09", "TAYAN012", "IMFAYAN10", "IMFAYAN20", "ELNRE04", "AYAN140",
//   "ELAYAN030", "IMFAYAN25", "AYAN139", "AYAN138", "ELNRE05", "ELAYAN031", "AYAN141", "AYAN144",
//   "AYAN142", "HCNRO04", "AYAN143", "232NREAYAN", "AYAN146", "TAYAN018", "TAYAN014", "AYAN115",
//   "AYAN147", "HCAYAN030", "IMFAYAN27", "AYAN148", "237NREAYAN", "AYAN150", "AYAN153", "ELAYAN035",
//   "AYAN131", "AYAN152", "ELAYAN006", "NROAYAN34", "AYAN154", "NROAYAN33", "234NREAYAN",
//   "239NREAYAN", "ELAYAN036", "AYAN155", "AYAN158", "HCAYAN032", "AYAN156", "AYAN160", "TMAYAN001",
//   "HCAYAN033", "AYAN159", "240NREAYAN", "AYAN164", "AYAN162", "AYAN157", "ELAYAN037",
//   "241NREAYAN", "ELPCNRE01", "ELPCNRO01", "AYAN163", "AYAN166", "HCAYAN036", "HCAYAN034",
//   "AYAN168", "HCAYAN035", "ELAYAN038", "AYAN171", "TAYAN020", "221NREAYAN", "HCAYAN038",
//   "AYAN170", "243NREAYAN", "AYAN175", "ELAYAN040", "AYAN172", "AYAN176", "AYAN173", "AYAN179",
//   "AYAN177", "TAYAN021", "AYAN178", "NROAYAN31", "ELAYAN041", "NROAYAN41", "ELNRO10", "AYAN180",
//   "AYAN174", "AYAN181", "247NREAYAN", "ELNRE06", "AYAN182", "ELNRO11", "NROAYAN18", "TNRO03",
//   "AYAN183", "AYAN184", "244NREAYAN", "248NREAYAN", "NROAYAN48", "TMAYAN003", "AYAN185",
//   "ELAYAN043", "ELAYAN045", "ELAYAN044", "ELAYAN042", "TAYAN019", "ELNRO12", "ELAYAN027",
//   "HCAYAN040", "TAYAN025", "HCAYAN037", "TMAYAN002", "TAYAN022", "ELAYAN046", "AYAN186",
//   "TAYAN023", "TAYAN028", "AYAN187", "NROAYAN49", "ELNRO13", "HCAYAN042", "AYAN188", "HCAYAN043",
//   "AYAN189", "HCAYAN044", "AYAN194", "NROAYAN50", "TAYAN030", "ELAYAN048", "AYAN196", "AYAN113",
//   "ELAYAN050", "AYAN197", "AYAN199", "TAYAN029", "ELAYAN051", "AYAN198", "AYAN195", "ELAYAN032",
//   "HCAYAN047", "AYAN200", "IMFAYAN29", "NROAYAN51", "ELNRO14", "ELAYAN052", "AYAN201",
//   "IMFAYAN30", "SVAYAN002", "SVAYAN001", "SVAYAN003", "AYAN202", "ELAYAN053", "TAYAN031",
//   "HCAYAN049", "250NREAYAN", "ELNRE07", "TAYAN032", "HCAYAN050", "ELAYAN054", "HCAYAN048",
//   "HCNRE01", "HCAYAN051", "SVAYAN004", "HCAYAN053", "NROAYAN53", "HCNRO05", "ELAYAN055",
//   "AYAN204", "ND010", "ND07", "HCAYAN054", "ELAYAN056", "AYAN206", "AYAN207", "IMFAYAN32",
//   "TMAYAN005", "AYAN205", "ND01", "ND09", "TMAYAN007", "TMNRE01", "HCAYAN056", "HCAYAN057",
//   "TMAYAN008", "TAYAN033", "HCAYAN058", "IMFAYAN33", "HCAYAN061", "HCAYAN059", "252NREAYAN",
//   "SVAYAN005", "ND03", "HCNRE02",
// ];
const BATCH_SIZE = 300;

/** Year range (inclusive) */
const YEAR_START = 2022;
const YEAR_END = 2025;

/**
 * Three parts per year: Part1 Jan–Apr, Part2 May–Aug, Part3 Sep– Dec.
 * Returns array of { startDate, endDate } for each period from YEAR_START to YEAR_END.
 */
function getDateRanges() {
  const ranges = [];
  for (let year = YEAR_START; year <= YEAR_END; year++) {
    ranges.push(
      { startDate: `${year}-01-01`, endDate: `${year}-04-30` },   // Part 1: 1 Jan – 30 Apr
      { startDate: `${year}-05-01`, endDate: `${year}-08-31` },   // Part 2: 1 May – 31 Aug
      { startDate: `${year}-09-01`, endDate: `${year}-12-31` },   // Part 3: 1 Sep – 31 Dec
    );
  }
  return ranges;
}

// Cash inflow types (money in on settlement date)
const CASH_ADD = [
  "CS+", "SL+", "CSI", "IN1", "IN+", "DIO", "DI1", "OI1", "DIS", "SQS", "D10", "TDO", "RDO"];
// Cash outflow types (money out on transaction date)
const CASH_SUBTRACT = [
  "BY-", "CS-", "MGF", "E22", "E01", "CUS", "E23", "MGE", "E10", "PRF", "NF-", "SQB", "TDI"];

const esc = (s) => String(s ?? "").replace(/'/g, "''");

/** Run a batched query until no more rows; map each row with mapper; return combined array */
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

module.exports = async (jobRequest, context) => {
  const catalystApp = catalyst.initialize(context);
  const zcql = catalystApp.zcql();
  const stratus = catalystApp.stratus();
  const bucket = stratus.bucket("passbook-files");

  try {
    const dateRanges = getDateRanges();
    console.log(`Cal_CB_Per_TNX: ${dateRanges.length} period(s) (${YEAR_START}–${YEAR_END}, 4 parts per year)`);

    const csvRows = [];
    const csvHeader = "Account_Code,Period_Start,Period_End,Date,Description,Transaction_Type,Debit,Credit,Balance";
    csvRows.push(csvHeader);
    /** Collected passbook rows for DB insert after bucket upload (each has accountCode attached) */
    const allPassbookRowsForDb = [];

    for (const ACCOUNT_CODE of ALL_ACCOUNT_CODES) {

      console.log(`Processing account: ${ACCOUNT_CODE}`);

      for (const { startDate: START_DATE, endDate: END_DATE } of dateRanges) {
        console.log(`Cal_CB_Per_TNX started for account ${ACCOUNT_CODE} from ${START_DATE} till ${END_DATE} (inflow=SETDATE, outflow=TRANDATE)`);

        const inflowTypesList = CASH_ADD.map((t) => `'${esc(t)}'`).join(", ");
        const outflowTypesList = CASH_SUBTRACT.map((t) => `'${esc(t)}'`).join(", ");

        // Query 1: Cash inflow — filter by SETDATE, types = CASH_ADD
        const inflowQueryBase = `
      SELECT ROWID, TRANDATE, SETDATE, executionPriority, Tran_Type, Security_Name, Net_Amount, QTY, WS_client_id, NETRATE, ISIN, STT
      FROM Transaction
      WHERE WS_Account_code = '${esc(ACCOUNT_CODE)}'
        AND Tran_Type IN (${inflowTypesList})
        AND SETDATE >= '${START_DATE}'
        AND SETDATE <= '${END_DATE}'
      ORDER BY SETDATE ASC, executionPriority ASC, ROWID ASC
    `;
        const inflowRows = await fetchBatched(zcql, inflowQueryBase, (t) => ({
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
        }));

        // Query 2: Cash outflow — filter by TRANDATE, types = CASH_SUBTRACT
        const outflowQueryBase = `
      SELECT ROWID, TRANDATE, SETDATE, executionPriority, Tran_Type, Security_Name, Net_Amount, QTY, WS_client_id, NETRATE, ISIN, STT
      FROM Transaction
      WHERE WS_Account_code = '${esc(ACCOUNT_CODE)}'
        AND Tran_Type IN (${outflowTypesList})
        AND TRANDATE >= '${START_DATE}'
        AND TRANDATE <= '${END_DATE}'
      ORDER BY TRANDATE ASC, executionPriority ASC, ROWID ASC
    `;
        const outflowRows = await fetchBatched(zcql, outflowQueryBase, (t) => ({
          rowId: t.ROWID,
          trandate: t.TRANDATE || t.Setdate || "",
          setdate: t.SETDATE || t.Setdate || t.TRANDATE || "",
          executionPriority: Number(t.executionPriority) ?? 999,
          type: t.Tran_Type || "",
          securityName: t.Security_Name || "",
          netAmount: Number(t.Net_Amount) || 0,
          impactDate: (t.TRANDATE || t.Trandate || t.Setdate || "").toString().slice(0, 10),
          qty: Number(t.QTY) || 0,
          clientCode: String(t.WS_client_id ?? "").trim() || "",
          price: Number(t.NETRATE) || 0,
          isin: String(t.ISIN ?? "").trim() || "",
          stt: Number(t.STT || t.Stt) || 0,
          isInflow: false,
        }));

        // Query 3: Bonus — by WS_Account_code, ExDate in range; quantity = BonusShare
        const bonusQueryBase = `
      SELECT ROWID, SecurityCode, SecurityName, BonusShare, ExDate, ISIN, WS_Account_code
      FROM Bonus
      WHERE WS_Account_code = '${esc(ACCOUNT_CODE)}'
        AND ExDate >= '${START_DATE}'
        AND ExDate <= '${END_DATE}'
      ORDER BY ExDate ASC, ROWID ASC
    `;
        const bonusRows = await fetchBatched(zcql, bonusQueryBase, (b) => {
          const exDateStr = (b.ExDate || "").toString().slice(0, 10);
          return {
            rowId: b.ROWID,
            trandate: exDateStr,
            setdate: exDateStr,
            executionPriority: 500,
            type: "BONUS",
            securityName: b.SecurityName || "",
            netAmount: 0,
            impactDate: exDateStr,
            isInflow: true,
            qty: Number(b.BonusShare) || 0,
            clientCode: "",
            price: 0,
            isin: String(b.ISIN ?? "").trim() || "",
            stt: 0,
          };
        }, "Bonus");

        // Query 4: Split — collect ISINs from all rows, query Split table, then look up holdings
        const uniqueISINs = new Set();
        for (const row of [...inflowRows, ...outflowRows, ...bonusRows]) {
          if (row.isin) uniqueISINs.add(row.isin);
        }

        const splitRows = [];
        if (uniqueISINs.size > 0) {
          const isinList = [...uniqueISINs].map((i) => `'${esc(i)}'`).join(", ");
          const splitQueryBase = `
        SELECT ROWID, Security_Code, Security_Name, Ratio1, Ratio2, Issue_Date, ISIN
        FROM Split
        WHERE ISIN IN (${isinList})
          AND Issue_Date >= '${START_DATE}'
          AND Issue_Date <= '${END_DATE}'
        ORDER BY Issue_Date ASC, ROWID ASC
      `;
          const rawSplits = await fetchBatched(zcql, splitQueryBase, (s) => ({
            rowId: s.ROWID,
            securityName: s.Security_Name || "",
            ratio1: Number(s.Ratio1) || 0,
            ratio2: Number(s.Ratio2) || 0,
            issueDate: (s.Issue_Date || "").toString().slice(0, 10),
            isin: String(s.ISIN ?? "").trim() || "",
          }), "Split");

          for (const sp of rawSplits) {
            if (!sp.ratio1 || !sp.ratio2 || !sp.isin || !sp.issueDate) continue;

            const holdingQuery = `
          SELECT HOLDINGS
          FROM Daily_Holding_Quantity
          WHERE WS_Account_code = '${esc(ACCOUNT_CODE)}'
            AND ISIN = '${esc(sp.isin)}'
            AND TRANDATE <= '${sp.issueDate}'
          ORDER BY TRANDATE DESC
          LIMIT 1
        `;
            const holdingResult = await zcql.executeZCQLQuery(holdingQuery);
            const holdingRow = holdingResult?.[0]?.Daily_Holding_Quantity || holdingResult?.[0];
            const holdings = Number(holdingRow?.HOLDINGS) || 0;

            if (holdings <= 0) continue;

            const qty = Math.round(holdings * (sp.ratio2 / sp.ratio1));

            splitRows.push({
              rowId: sp.rowId,
              trandate: sp.issueDate,
              setdate: sp.issueDate,
              executionPriority: 600,
              type: "SPLIT",
              securityName: sp.securityName,
              netAmount: 0,
              impactDate: sp.issueDate,
              isInflow: true,
              qty,
              clientCode: "",
              price: 0,
              isin: sp.isin,
              stt: 0,
            });
          }
        }

        // Query 5: Dividend — verify holdings on ExDate, use PaymentDate as impact date, cash inflow = HOLDINGS × Rate
        const dividendRows = [];
        if (uniqueISINs.size > 0) {
          const isinList = [...uniqueISINs].map((i) => `'${esc(i)}'`).join(", ");
          const dividendQueryBase = `
        SELECT ROWID, SecurityCode, Security_Name, ISIN, Rate, ExDate, PaymentDate, Dividend_Type
        FROM Dividend
        WHERE ISIN IN (${isinList})
          AND PaymentDate >= '${START_DATE}'
          AND PaymentDate <= '${END_DATE}'
        ORDER BY PaymentDate ASC, ROWID ASC
      `;
          const rawDividends = await fetchBatched(zcql, dividendQueryBase, (d) => ({
            rowId: d.ROWID,
            securityName: d.Security_Name || "",
            rate: Number(d.Rate) || 0,
            exDate: (d.ExDate || "").toString().slice(0, 10),
            paymentDate: (d.PaymentDate || "").toString().slice(0, 10),
            isin: String(d.ISIN ?? "").trim() || "",
            dividendType: d.Dividend_Type || "",
          }), "Dividend");

          for (const div of rawDividends) {
            if (!div.rate || !div.isin || !div.exDate || !div.paymentDate) continue;

            const holdingQuery = `
          SELECT HOLDINGS
          FROM Daily_Holding_Quantity
          WHERE WS_Account_code = '${esc(ACCOUNT_CODE)}'
            AND ISIN = '${esc(div.isin)}'
            AND TRANDATE <= '${div.exDate}'
          ORDER BY TRANDATE DESC
          LIMIT 1
        `;
            const holdingResult = await zcql.executeZCQLQuery(holdingQuery);
            const holdingRow = holdingResult?.[0]?.Daily_Holding_Quantity || holdingResult?.[0];
            const holdings = Number(holdingRow?.HOLDINGS) || 0;

            if (holdings <= 0) continue;

            const totalAmount = Math.round(holdings * div.rate * 100) / 100;

            dividendRows.push({
              rowId: div.rowId,
              trandate: div.paymentDate,
              setdate: div.paymentDate,
              executionPriority: 550,
              type: "DIVIDEND",
              securityName: div.securityName,
              netAmount: totalAmount,
              impactDate: div.paymentDate,
              isInflow: true,
              qty: holdings,
              clientCode: "",
              price: div.rate,
              isin: div.isin,
              stt: 0,
            });
          }
        }

        // Merge and sort by impact date; same date: inflows first, then outflows; bonus/split/dividend included
        const allEvents = [...inflowRows, ...outflowRows, ...bonusRows, ...splitRows, ...dividendRows];
        console.log(`Period ${START_DATE}..${END_DATE}: ${allEvents.length} events (in:${inflowRows.length} out:${outflowRows.length} bonus:${bonusRows.length} split:${splitRows.length} div:${dividendRows.length})`);
        allEvents.sort((a, b) => {
          const dA = new Date(a.impactDate).getTime();
          const dB = new Date(b.impactDate).getTime();
          if (dA !== dB) return dA - dB;
          if (a.isInflow !== b.isInflow) return a.isInflow ? -1 : 1;
          if (a.executionPriority !== b.executionPriority) return a.executionPriority - b.executionPriority;
          return String(a.rowId).localeCompare(String(b.rowId));
        });

        // Starting balance: first impact date — if CS+ on that day, use its Net_Amount
        let startingBalance = 0;
        const firstImpactDate = allEvents.length ? allEvents[0].impactDate : null;
        if (firstImpactDate) {
          const firstDayCsPlus = allEvents.find((e) => e.impactDate === firstImpactDate && e.type === "CS+");
          if (firstDayCsPlus) startingBalance = firstDayCsPlus.netAmount;
        }

        let balance = startingBalance;
        const passbookRows = [];
        let usedOpeningCsPlus = false;

        for (const row of allEvents) {
          const { impactDate, trandate, setdate, type, securityName, netAmount, isInflow, qty, clientCode, price, isin, stt } = row;
          let debit = 0;
          let credit = 0;

          if (firstImpactDate && impactDate === firstImpactDate && type === "CS+" && !usedOpeningCsPlus) {
            usedOpeningCsPlus = true;
            credit = netAmount;
            balance = startingBalance;
          } else if (isInflow) {
            credit = netAmount;
            balance += netAmount;
          } else {
            debit = netAmount;
            balance -= netAmount;
          }

          const description = `${type} ${securityName}`.trim() || type;
          passbookRows.push({
            date: impactDate,
            trandate,
            setdate,
            description,
            transactionType: type,
            qty: qty ?? 0,
            debit: debit || null,
            credit: credit || null,
            balance,
            securityName,
            totalAmount: netAmount,
            clientCode: clientCode ?? "",
            price: price ?? 0,
            isin: isin ?? "",
            stt: stt ?? 0,
          });
        }

        // Console: passbook format
        console.log("Date       | Description                    | Debit        | Credit       | Balance");
        console.log("-----------|--------------------------------|--------------|--------------|---------------");

        for (const row of passbookRows) {
          const date = String(row.date).slice(0, 10);
          const desc = String(row.description).slice(0, 30).padEnd(30);
          const debitStr = row.debit != null ? row.debit.toFixed(2) : "";
          const creditStr = row.credit != null ? row.credit.toFixed(2) : "";
          const balanceStr = row.balance.toFixed(2);
          console.log(
            `${date} | ${desc} | ${debitStr.padStart(12)} | ${creditStr.padStart(12)} | ${balanceStr.padStart(13)}`
          );
        }

        console.log("-----------|--------------------------------|--------------|--------------|---------------");
        console.log(`Final cash balance for ${START_DATE}..${END_DATE}: ${balance.toFixed(2)}`);

        // Append this account/period to CSV and collect for DB insert later
        for (const row of passbookRows) {
          const date = String(row.date).slice(0, 10);
          const debitStr = row.debit != null ? row.debit.toFixed(2) : "";
          const creditStr = row.credit != null ? row.credit.toFixed(2) : "";
          const balanceStr = row.balance.toFixed(2);
          csvRows.push([
            csvEscape(ACCOUNT_CODE),
            csvEscape(START_DATE),
            csvEscape(END_DATE),
            csvEscape(date),
            csvEscape(row.description),
            csvEscape(row.transactionType),
            csvEscape(debitStr),
            csvEscape(creditStr),
            csvEscape(balanceStr),
          ].join(","));
          allPassbookRowsForDb.push({ ...row, accountCode: ACCOUNT_CODE });
        }
      }
    }

    // Build CSV and upload to Stratus
    const fileName = "All_Accounts_Passbook.csv";
    const csvContent = csvRows.join("\n");
    const readableStream = Readable.from(csvContent);
    console.log(`Uploading CSV to passbook-files (~${Math.round(Buffer.byteLength(csvContent, "utf8") / 1024)} KB)`);
    await bucket.putObject(fileName, readableStream, {
      overwrite: true,
      contentType: "text/csv",
    });
    console.log(`Uploaded ${fileName} to bucket passbook-files`);

    // Insert passbook data into Cash_Balance_Per_Transaction (Holding not inserted)
    const tableName = "Cash_Balance_Per_Transaction";
    if (allPassbookRowsForDb.length === 0) {
      console.log("No passbook rows to insert into database.");
    } else {
      let sequence = 1;
      let inserted = 0;
      for (const row of allPassbookRowsForDb) {
        try {
          const txDate = String(row.trandate).slice(0, 10);
          const setDateStr = String(row.setdate).slice(0, 10);
          const insertQuery = `
        INSERT INTO ${tableName} (Account_Code, Client_Code, Transaction_Date, Settlement_Date, Price, Cash_Balance, Security_Name, ISIN, Quantity, Total_Amount, STT, Sequence, Transaction_Type)
        VALUES (
          '${esc(row.accountCode)}',
          '${esc(row.clientCode)}',
          '${txDate}',
          '${setDateStr}',
          ${Number(row.price)},
          ${Number(row.balance)},
          '${esc(row.securityName)}',
          '${esc(row.isin)}',
          ${Math.round(Number(row.qty ?? 0) || 0)},
          ${Number(row.totalAmount)},
          ${Number(row.stt ?? 0)},
          ${sequence},
          '${esc(row.transactionType)}'
        )
      `;
          await zcql.executeZCQLQuery(insertQuery);
          inserted += 1;
          sequence += 1;
        } catch (insertErr) {
          console.error(`Insert failed at sequence ${sequence}:`, insertErr?.message || insertErr);
          throw insertErr;
        }
      }
      console.log(`Inserted ${inserted} row(s) into ${tableName}`);
    }

    context.closeWithSuccess();
  } catch (error) {
    console.error("Cal_CB_Per_TNX failed:", error?.message || error);
    if (error?.stack) console.error(error.stack);
    context.closeWithFailure();
  }
};