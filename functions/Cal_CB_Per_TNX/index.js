/**
 * Cal_CB_Per_TNX: Passbook-style cash balance from Transaction table.
 * Two queries: inflow by SETDATE, outflow by TRANDATE. Writes to Cash_Balance_Per_Transaction.
 *
 * @param {import("./types/job").JobRequest} jobRequest
 * @param {import("./types/job").Context} context
 */

const catalyst = require("zcatalyst-sdk-node");
console.log("Cal_CB_Per_TNX started");
const ACCOUNT_CODE = "AYAN002";
const START_DATE = "2022-12-12";
const END_DATE = "2022-12-31";
const BATCH_SIZE = 300;

// Cash inflow types (money in on settlement date)
const CASH_ADD = ["CS+", "SL+", "CSI", "IN1", "IN+", "DIO", "DI1", "OI1", "DIS", "SQS"];
// Cash outflow types (money out on transaction date)
const CASH_SUBTRACT = ["BY-", "CS-", "MGF", "E22", "E01", "CUS", "E23", "MGE", "E10", "PRF", "NF-", "SQB"];

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

  try {
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

    // Merge and sort by impact date; same date: inflows first, then outflows; bonus included
    const allEvents = [...inflowRows, ...outflowRows, ...bonusRows];
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

    // Insert first 5 records into Cash_Balance_Per_Transaction (Sequence 1, 2, 3...; cash on top)
    const tableName = "Cash_Balance_Per_Transaction";
    const rowsToInsert = passbookRows;
    let sequence = 1;
    for (const row of rowsToInsert) {
      const txDate = String(row.trandate).slice(0, 10);
      const setDateStr = String(row.setdate).slice(0, 10);
      const insertQuery = `
        INSERT INTO ${tableName} (Account_Code, Client_Code, Transaction_Date, Settlement_Date, Transaction_Type, Price, Cash_Balance, Holding, Security_Name, ISIN, Quantity, Total_Amount, STT, Sequence)
        VALUES (
          '${esc(ACCOUNT_CODE)}',
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
    console.log(`Inserted ${rowsToInsert.length} row(s) into ${tableName}`);

    // Console: passbook format
    console.log("Date       | Description                    | Debit        | Credit       | Balance");
    console.log("-----------|--------------------------------|--------------|---------------|---------------");

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

    console.log("-----------|--------------------------------|--------------|---------------|---------------");
    console.log(`Final cash balance: ${balance.toFixed(2)}`);

    context.closeWithSuccess();
  } catch (error) {
    console.error("Cal_CB_Per_TNX failed:", error);
    context.closeWithFailure();
  }
};