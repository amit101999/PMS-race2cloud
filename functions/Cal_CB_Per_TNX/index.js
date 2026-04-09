/**
 * Cal_CB_Per_TNX: Passbook-style cash balance from Transaction + Dividend tables.
 * Inflows use SETDATE, outflows use TRANDATE. Writes to Cash_Balance_Per_Transaction.
 *
 * Job params:
 *   accountCodes  – array of account codes to process
 *   startDate     – yyyy-mm-dd (optional, defaults to all-time)
 *   endDate       – yyyy-mm-dd (optional, defaults to today)
 *
 * @param {import("./types/job").JobRequest} jobRequest
 * @param {import("./types/job").Context} context
 */

const catalyst = require("zcatalyst-sdk-node");

const BATCH_SIZE = 300;

const CASH_ADD = ["CS+", "SL+", "CSI", "IN1", "IN+", "DIO", "DI1", "OI1", "DIS", "SQS"];
const CASH_SUBTRACT = ["BY-", "CS-", "MGF", "E22", "E01", "CUS", "E23", "MGE", "E10", "PRF", "NF-", "SQB"];

const esc = (s) => String(s ?? "").replace(/'/g, "''");

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

async function processAccount(zcql, accountCode, startDate, endDate) {
  console.log(`Processing account ${accountCode} from ${startDate} to ${endDate}`);

  const inflowTypesList = CASH_ADD.map((t) => `'${esc(t)}'`).join(", ");
  const outflowTypesList = CASH_SUBTRACT.map((t) => `'${esc(t)}'`).join(", ");

  const dateFilterInflow = startDate && endDate
    ? `AND SETDATE >= '${startDate}' AND SETDATE <= '${endDate}'`
    : startDate ? `AND SETDATE >= '${startDate}'`
    : endDate ? `AND SETDATE <= '${endDate}'`
    : "";

  const dateFilterOutflow = startDate && endDate
    ? `AND TRANDATE >= '${startDate}' AND TRANDATE <= '${endDate}'`
    : startDate ? `AND TRANDATE >= '${startDate}'`
    : endDate ? `AND TRANDATE <= '${endDate}'`
    : "";

  /* ================= FETCH INFLOWS (by SETDATE) ================= */
  const inflowQueryBase = `
    SELECT ROWID, TRANDATE, SETDATE, executionPriority, Tran_Type, Security_Name, Net_Amount, QTY, WS_client_id, NETRATE, ISIN, STT
    FROM Transaction
    WHERE WS_Account_code = '${esc(accountCode)}'
      AND Tran_Type IN (${inflowTypesList})
      ${dateFilterInflow}
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

  /* ================= FETCH OUTFLOWS (by TRANDATE) ================= */
  const outflowQueryBase = `
    SELECT ROWID, TRANDATE, SETDATE, executionPriority, Tran_Type, Security_Name, Net_Amount, QTY, WS_client_id, NETRATE, ISIN, STT
    FROM Transaction
    WHERE WS_Account_code = '${esc(accountCode)}'
      AND Tran_Type IN (${outflowTypesList})
      ${dateFilterOutflow}
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
    isInflow: false,
    qty: Number(t.QTY) || 0,
    clientCode: String(t.WS_client_id ?? "").trim() || "",
    price: Number(t.NETRATE) || 0,
    isin: String(t.ISIN ?? "").trim() || "",
    stt: Number(t.STT || t.Stt) || 0,
  }));

  /* ================= FETCH DIVIDENDS ================= */
  const uniqueISINs = new Set();
  for (const row of [...inflowRows, ...outflowRows]) {
    if (row.isin) uniqueISINs.add(row.isin);
  }

  const dividendRows = [];
  if (uniqueISINs.size > 0) {
    const isinList = [...uniqueISINs].map((i) => `'${esc(i)}'`).join(", ");

    const dateFilterDividend = startDate && endDate
      ? `AND PaymentDate >= '${startDate}' AND PaymentDate <= '${endDate}'`
      : startDate ? `AND PaymentDate >= '${startDate}'`
      : endDate ? `AND PaymentDate <= '${endDate}'`
      : "";

    const dividendQueryBase = `
      SELECT ROWID, SecurityCode, Security_Name, ISIN, Rate, ExDate, PaymentDate, Dividend_Type
      FROM Dividend
      WHERE ISIN IN (${isinList})
        ${dateFilterDividend}
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
        WHERE WS_Account_code = '${esc(accountCode)}'
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

  /* ================= MERGE & SORT ================= */
  const allEvents = [...inflowRows, ...outflowRows, ...dividendRows];
  allEvents.sort((a, b) => {
    const dA = new Date(a.impactDate).getTime();
    const dB = new Date(b.impactDate).getTime();
    if (dA !== dB) return dA - dB;
    if (a.isInflow !== b.isInflow) return a.isInflow ? -1 : 1;
    if (a.executionPriority !== b.executionPriority) return a.executionPriority - b.executionPriority;
    return String(a.rowId).localeCompare(String(b.rowId));
  });

  /* ================= COMPUTE RUNNING BALANCE ================= */
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

    passbookRows.push({
      date: impactDate,
      trandate,
      setdate,
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

  /* ================= INSERT INTO DB ================= */
  const tableName = "Cash_Balance_Per_Transaction";
  let sequence = 1;
  for (const row of passbookRows) {
    const txDate = String(row.trandate).slice(0, 10);
    const setDateStr = String(row.setdate).slice(0, 10);
    const insertQuery = `
      INSERT INTO ${tableName} (Account_Code, Client_Code, Transaction_Date, Settlement_Date, Transaction_Type, Price, Cash_Balance, Holding, Security_Name, ISIN, Quantity, Total_Amount, STT, Sequence)
      VALUES (
        '${esc(accountCode)}',
        '${esc(row.clientCode)}',
        '${txDate}',
        '${setDateStr}',
        '${esc(row.transactionType)}',
        ${Number(row.price)},
        ${Number(row.balance)},
        0,
        '${esc(row.securityName)}',
        '${esc(row.isin)}',
        ${Math.round(Number(row.qty ?? 0) || 0)},
        ${Number(row.totalAmount)},
        ${Number(row.stt ?? 0)},
        ${sequence}
      )
    `;
    await zcql.executeZCQLQuery(insertQuery);
    sequence += 1;
  }

  console.log(`Account ${accountCode}: inserted ${passbookRows.length} row(s), final balance: ${balance.toFixed(2)}`);
}

module.exports = async (jobRequest, context) => {
  const catalystApp = catalyst.initialize(context);
  const zcql = catalystApp.zcql();

  try {
    const params = jobRequest.getAllJobParams();

    let accountCodes = params.accountCodes || params.accountCode || [];
    if (typeof accountCodes === "string") {
      try { accountCodes = JSON.parse(accountCodes); } catch (_) { accountCodes = [accountCodes]; }
    }
    if (!Array.isArray(accountCodes)) accountCodes = [accountCodes];
    accountCodes = accountCodes.filter(Boolean);

    const startDate = params.startDate || null;
    const endDate = params.endDate || new Date().toISOString().split("T")[0];

    if (!accountCodes.length) {
      console.error("No account codes provided");
      context.closeWithFailure();
      return;
    }

    console.log(`Cal_CB_Per_TNX: ${accountCodes.length} account(s), range ${startDate || "all"} to ${endDate}`);

    for (const acc of accountCodes) {
      try {
        await processAccount(zcql, acc, startDate, endDate);
      } catch (accErr) {
        console.error(`Failed for account ${acc}:`, accErr);
      }
    }

    console.log("Cal_CB_Per_TNX completed");
    context.closeWithSuccess();
  } catch (error) {
    console.error("Cal_CB_Per_TNX failed:", error);
    context.closeWithFailure();
  }
};
