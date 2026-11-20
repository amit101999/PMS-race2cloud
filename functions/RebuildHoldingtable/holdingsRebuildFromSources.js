"use strict";

/**
 * FIFO + Holdings writer for Catalyst job `RebuildHoldingtable`.
 * Single source — do not duplicate under other functions.
 */

const BATCH = 250;
const ISINS_FILTER = [];
const MAX_PAIRS = 0;
const DRY_RUN = false;

const esc = (s) => String(s ?? "").replace(/'/g, "''");

/**
 * Quantity epsilon for normal (non-corporate-action) FIFO math. Snaps tiny
 * floating-point residue (e.g. 0.533 - 0.533 = 1e-16) to exactly zero so
 * fully-sold positions end at 0 and consumed lots leave the queue. Genuine
 * fractional quantities (e.g. 5.533) are preserved. Corporate-action rounding
 * (Math.floor / ratio logic) is intentional and unaffected. This is the
 * canonical rebuild path used to repair existing wrong Holdings rows.
 */
const QTY_EPS = 1e-6;
const snapQty = (n) => {
  const v = Number(n) || 0;
  return Math.abs(v) < QTY_EPS ? 0 : v;
};

/**
 * Decimals are stored in binary, so 190.88 - 190 gives 0.8799999999999955 and
 * 0.88 x 1000 gives 879.9999999999955. Rounding every quantity (6 decimals) and
 * amount (4 decimals) as it is calculated keeps the saved figures equal to a hand
 * calculation, so all screens and exports agree. Same rule as the AppSail FIFO
 * engine (util/analytics/transactionHistory/fifo.js). `=== 0` turns -0 into 0.
 */
const roundQty = (n) => {
  const v = Math.round((Number(n) || 0) * 1e6) / 1e6;
  return v === 0 ? 0 : v;
};
const roundAmt = (n) => {
  const v = Math.round((Number(n) || 0) * 1e4) / 1e4;
  return v === 0 ? 0 : v;
};

const sqlDate = (v) => {
  const s = String(v ?? "").trim().slice(0, 10);
  return s && /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : "";
};

const isBuyType = (type) => /^BY-|SQB|OPI/i.test(String(type || ""));

/* ===== TRADE_DATE_MIGRATION: OLD (settlement basis for ALL buys) — retained, do not delete =====
const getEffectiveDate = (r) => {
  const setDate = r.SETDATE || r.setdate;
  const tradeDate = r.TRANDATE || r.trandate;
  return isBuyType(r.Tran_Type || r.tranType)
    ? setDate || tradeDate
    : tradeDate || setDate;
};
   ============================================================================================= */

/* ===== TRADE_DATE_MIGRATION: NEW (trade basis; OPI keeps settlement) =====
   OPI rows are inception/opening positions: TRANDATE is the original acquisition
   date, SETDATE is the PMS onboarding date. Those are different events, so OPI
   stays settlement-dated. BY-/SQB move to TRANDATE. Sells unchanged. */
const isOpiType = (type) => /^OPI/i.test(String(type || ""));

const getEffectiveDate = (r) => {
  const setDate = r.SETDATE || r.setdate;
  const tradeDate = r.TRANDATE || r.trandate;
  return isOpiType(r.Tran_Type || r.tranType)
    ? setDate || tradeDate
    : tradeDate || setDate;
};

/* ===== MUTUAL FUND BUY: settlement-dated =====
   A mutual fund buy (Security_Type 'm', BY- / SQB) adds units on its settlement
   date, not its trade date. The Holdings table has no mutual fund marker, so the
   date is fixed here, as the row is read: TRANDATE takes the SETDATE value in
   memory. Every date rule after this point, and the Holdings row written, then
   use the settlement date. The Transaction table keeps the real trade date.
   Same rule in functions/CalculateHoldingWorkers and RebuildHoldingtable. */
const settleMutualFundBuy = (r) => {
  const isMutualFund = String(r.Security_Type ?? "").trim().toLowerCase() === "m";
  const isMfBuy = isMutualFund && /^BY-|SQB/i.test(String(r.Tran_Type || ""));
  return isMfBuy && r.SETDATE ? { ...r, TRANDATE: r.SETDATE } : r;
};

const nextDayCutoff = (asOnDate) => {
  if (!asOnDate || !/^\d{4}-\d{2}-\d{2}$/.test(asOnDate)) return null;
  const nextDay = new Date(asOnDate);
  nextDay.setDate(nextDay.getDate() + 1);
  return nextDay.toISOString().split("T")[0];
};

async function fetchAccountTransactions(zcql, accountCode, asOnDate) {
  const cutoff = nextDayCutoff(asOnDate);
  const dateClause = cutoff
    ? ` AND (TRANDATE < '${cutoff}' OR SETDATE < '${cutoff}')`
    : "";

  const rows = [];
  const seen = new Set();
  let offset = 0;
  while (true) {
    try {
      const batch = await zcql.executeZCQLQuery(`
        SELECT SETDATE, TRANDATE, Tran_Type, Security_Name, Security_code, QTY, NETRATE, Net_Amount, ISIN, ROWID, Security_Type
        FROM Transaction
        WHERE WS_Account_code = '${esc(accountCode)}'
        ${dateClause}
        ORDER BY SETDATE ASC, ROWID ASC
        LIMIT ${BATCH} OFFSET ${offset}
      `);
      if (!batch || batch.length === 0) break;
      for (const row of batch) {
        const r = row.Transaction || row;
        if (r.ROWID && seen.has(r.ROWID)) continue;
        if (r.ROWID) seen.add(r.ROWID);
        rows.push(settleMutualFundBuy(r));
      }
      if (batch.length < BATCH) break;
      offset += BATCH;
    } catch (err) {
      // Throw instead of returning a partial list: the caller deletes and
      // re-inserts Holdings from what we return, so missing pages would
      // overwrite good rows with wrong ones. Throwing skips the account.
      throw new Error(`fetchAccountTransactions[${accountCode}] offset=${offset}: ${err.message}`);
    }
  }

  return cutoff
    ? rows.filter((r) => {
        const d = getEffectiveDate(r);
        return !d || d < cutoff;
      })
    : rows;
}

async function fetchAccountBonuses(zcql, accountCode, asOnDate) {
  const cutoff = nextDayCutoff(asOnDate);
  // Record_Date <= ExDate, so this is "record date < cutoff"; the ExDate half
  // covers bonuses saved before Record_Date existed.
  const dateClause = cutoff
    ? ` AND (Record_Date < '${cutoff}' OR ExDate < '${cutoff}')`
    : "";

  const rows = [];
  const seen = new Set();
  let offset = 0;
  while (true) {
    try {
      const batch = await zcql.executeZCQLQuery(`
        SELECT SecurityCode, SecurityName, Record_Date, ExDate, BonusShare, ISIN, ROWID
        FROM Bonus
        WHERE WS_Account_code = '${esc(accountCode)}'
        ${dateClause}
        ORDER BY ExDate ASC, ROWID ASC
        LIMIT ${BATCH} OFFSET ${offset}
      `);
      if (!batch || batch.length === 0) break;
      for (const row of batch) {
        const b = row.Bonus || row;
        if (b.ROWID && seen.has(b.ROWID)) continue;
        if (b.ROWID) seen.add(b.ROWID);
        rows.push(b);
      }
      if (batch.length < BATCH) break;
      offset += BATCH;
    } catch (err) {
      throw new Error(`fetchAccountBonuses[${accountCode}] offset=${offset}: ${err.message}`);
    }
  }
  return rows;
}

async function fetchAccountDemergers(zcql, accountCode, asOnDate) {
  const cutoff = nextDayCutoff(asOnDate);
  const dateClause = cutoff
    ? ` AND (TRANDATE < '${cutoff}' OR SETDATE < '${cutoff}')`
    : "";

  const rows = [];
  const seen = new Set();
  let offset = 0;
  while (true) {
    try {
      const batch = await zcql.executeZCQLQuery(`
        SELECT *
        FROM Demerger_Record
        WHERE WS_Account_code = '${esc(accountCode)}'
        ${dateClause}
        ORDER BY TRANDATE ASC, ROWID ASC
        LIMIT ${BATCH} OFFSET ${offset}
      `);
      if (!batch || batch.length === 0) break;
      for (const row of batch) {
        const d = row.Demerger_Record || row;
        if (d.ROWID != null && seen.has(d.ROWID)) continue;
        if (d.ROWID != null) seen.add(d.ROWID);
        rows.push(d);
      }
      if (batch.length < BATCH) break;
      offset += BATCH;
    } catch (err) {
      throw new Error(`fetchAccountDemergers[${accountCode}] offset=${offset}: ${err.message}`);
    }
  }
  return rows.filter(
    (d) => String(d.Tran_Type || d.tran_type || "").toUpperCase() === "DEMERGER",
  );
}

async function fetchAccountMergers(zcql, accountCode, asOnDate) {
  const cutoff = nextDayCutoff(asOnDate);
  const dateClause = cutoff
    ? ` AND (TRANDATE < '${cutoff}' OR SETDATE < '${cutoff}')`
    : "";

  const rows = [];
  const seen = new Set();
  let offset = 0;
  while (true) {
    try {
      const batch = await zcql.executeZCQLQuery(`
        SELECT *
        FROM Merger
        WHERE WS_Account_code = '${esc(accountCode)}'
        ${dateClause}
        ORDER BY TRANDATE ASC, ROWID ASC
        LIMIT ${BATCH} OFFSET ${offset}
      `);
      if (!batch || batch.length === 0) break;
      for (const row of batch) {
        const m = row.Merger || row;
        if (m.ROWID != null && seen.has(m.ROWID)) continue;
        if (m.ROWID != null) seen.add(m.ROWID);
        rows.push(m);
      }
      if (batch.length < BATCH) break;
      offset += BATCH;
    } catch (err) {
      throw new Error(`fetchAccountMergers[${accountCode}] offset=${offset}: ${err.message}`);
    }
  }
  return rows.filter(
    (m) => String(m.Tran_Type || m.tran_type || "").toUpperCase() === "MERGER",
  );
}

async function fetchSplitsForIsins(zcql, isins, asOnDate) {
  if (!isins || !isins.length) return [];
  const cutoff = nextDayCutoff(asOnDate);
  // Record_Date <= Issue_Date, so this is "record date < cutoff"; the
  // Issue_Date half covers splits saved before Record_Date existed.
  const dateClause = cutoff
    ? ` AND (Record_Date < '${cutoff}' OR Issue_Date < '${cutoff}')`
    : "";
  const inClause = isins.map((i) => `'${esc(i)}'`).join(",");

  const rows = [];
  const seen = new Set();
  let offset = 0;
  while (true) {
    try {
      const batch = await zcql.executeZCQLQuery(`
        SELECT Security_Code, Security_Name, Record_Date, Issue_Date, Ratio1, Ratio2, ISIN, ROWID
        FROM Split
        WHERE ISIN IN (${inClause})
        ${dateClause}
        ORDER BY Issue_Date ASC, ROWID ASC
        LIMIT ${BATCH} OFFSET ${offset}
      `);
      if (!batch || batch.length === 0) break;
      for (const row of batch) {
        const s = row.Split || row;
        if (s.ROWID && seen.has(s.ROWID)) continue;
        if (s.ROWID) seen.add(s.ROWID);
        rows.push(s);
      }
      if (batch.length < BATCH) break;
      offset += BATCH;
    } catch (err) {
      throw new Error(`fetchSplitsForIsins offset=${offset}: ${err.message}`);
    }
  }

  return rows.map((s) => ({
    ratio1: Number(s.Ratio1) || 0,
    ratio2: Number(s.Ratio2) || 0,
    // The split takes effect in holdings on its record date. Splits saved
    // before Record_Date existed only have Issue_Date, which served that role.
    issueDate: s.Record_Date || s.Issue_Date,
    effectiveDate: s.Issue_Date || "",
    securityCode: s.Security_Code || "",
    securityName: s.Security_Name || "",
    isin: s.ISIN || "",
  }));
}

/* ============================== FIFO ENGINE (mirror of analytics fifo.js) ============================== */

/**
 * Tie-breaker for events that share the same date.
 * SPLIT must be processed BEFORE BONUS so that bonus shares (stored in the DB
 * as the post-split count) are not multiplied a second time by the split.
 */
const EVENT_TYPE_PRIORITY = {
  TXN: 0,
  SPLIT: 1,
  BONUS: 2,
  DEMERGER: 3,
  MERGER: 4,
};

function runFifoEngine(
  transactions = [],
  bonuses = [],
  splits = [],
  card = false,
  demergers = [],
  mergers = [],
  splitLog = null, // optional: receives one entry per split applied (for Split_Account)
) {
  const activeIsin =
    transactions[0]?.ISIN ||
    transactions[0]?.isin ||
    bonuses[0]?.ISIN ||
    bonuses[0]?.isin ||
    splits[0]?.isin ||
    demergers[0]?.ISIN ||
    demergers[0]?.isin ||
    mergers[0]?.ISIN ||
    mergers[0]?.isin ||
    null;

  let holdings = 0;
  let lotCounter = 0;
  const buyQueue = [];
  const output = [];
  let lastDemergerEventKey = null;

  const normalizeDate = (rawDate) => {
    if (!rawDate) return null;
    const [y, m, d] = String(rawDate).split("-").map(Number);
    const fullYear = y < 100 ? 2000 + y : y;
    return `${fullYear}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
  };

  const isBuy = (type) => /^BY-|SQB|OPI/.test(String(type || "").toUpperCase());

  /* ===== TRADE_DATE_MIGRATION: OLD — retained, do not delete =====
  const getTxnEventDate = (t) => {
    const setDate = t.SETDATE || t.setdate;
    const tradeDate = t.TRANDATE || t.trandate;
    return isBuy(t.Tran_Type || t.tranType) ? setDate || tradeDate : tradeDate || setDate;
  };
     ============================================================== */

  /* ===== TRADE_DATE_MIGRATION: NEW (OPI keeps settlement) ===== */
  const getTxnEventDate = (t) => {
    const setDate = t.SETDATE || t.setdate;
    const tradeDate = t.TRANDATE || t.trandate;
    return isOpiType(t.Tran_Type || t.tranType)
      ? setDate || tradeDate
      : tradeDate || setDate;
  };

  const events = [
    ...transactions
      .filter((t) => (t.ISIN || t.isin) === activeIsin)
      .map((t) => {
        const eventDate = getTxnEventDate(t);
        return {
          type: "TXN",
          date: normalizeDate(eventDate),
          data: {
            tranType: t.Tran_Type || t.tranType,
            qty: t.QTY || t.qty,
            netrate: t.NETRATE || t.netrate,
            netAmount: t.NETAMOUNT || t.netAmount || t.Net_Amount || 0,
            trandate: eventDate,
            originalTrandate: t.TRANDATE || t.trandate || null,
            setdate: t.SETDATE || t.setdate || null,
            isin: t.ISIN || t.isin,
          },
        };
      }),
    ...bonuses
      .filter((b) => (b.ISIN || b.isin) === activeIsin)
      .map((b) => ({
        type: "BONUS",
        // A bonus lands in holdings on its record date; bonuses saved before
        // Record_Date existed only have ExDate, which served that role.
        date: normalizeDate(b.Record_Date || b.ExDate || b.exDate),
        data: {
          bonusShare: b.BonusShare || b.bonusShare,
          exDate: b.Record_Date || b.ExDate || b.exDate,
          isin: b.ISIN || b.isin,
        },
      })),
    ...splits
      .filter((s) => s.isin === activeIsin)
      .map((s) => ({
        type: "SPLIT",
        date: normalizeDate(s.issueDate),
        data: {
          ratio1: s.ratio1,
          ratio2: s.ratio2,
          issueDate: s.issueDate,
          effectiveDate: s.effectiveDate,
          securityCode: s.securityCode,
          securityName: s.securityName,
          isin: s.isin,
        },
      })),
    ...demergers
      .filter((d) => (d.ISIN || d.isin) === activeIsin)
      .map((d) => {
        const td = d.TRANDATE || d.trandate;
        // Column is `Record_date` in Demerger_Record — reads are case-sensitive,
        // so check every casing. May also come back as a DATETIME
        // ("2026-08-27 00:00:00"); keep only yyyy-mm-dd so normalizeDate/sort work.
        const recDate = String(
          d.Record_Date || d.Record_date || d.record_date || "",
        ).slice(0, 10);
        return {
          type: "DEMERGER",
          // Group/sort the demerger event by its record date, not the per-lot
          // trade date (TRANDATE now carries the original lot's trade date).
          date: normalizeDate(recDate || td),
          data: {
            qty: d.QTY ?? d.qty,
            price: d.PRICE ?? d.price,
            totalAmount:
              Number(d.TOTAL_AMOUNT ?? d.total_amount ?? d.HOLDING_VALUE ?? 0) || 0,
            trandate: td,
            setdate: d.SETDATE || d.setdate || td,
            isin: d.ISIN || d.isin,
            // Corporate-action (demerger) record date → Holdings.CA_DATE.
            recordDate: recDate || "",
          },
        };
      }),
    ...mergers
      .filter((m) => (m.ISIN || m.isin) === activeIsin)
      .map((m) => {
        const td = m.TRANDATE || m.trandate;
        // Check every casing of the record-date column (reads are case-sensitive).
        // May also come back as a DATETIME; keep only yyyy-mm-dd.
        const recDate = String(
          m.Record_Date || m.Record_date || m.record_date || "",
        ).slice(0, 10);
        return {
          type: "MERGER",
          // Group/sort the merger event by its record date, not the per-lot
          // trade date (TRANDATE now carries the original lot's trade date).
          date: normalizeDate(recDate || td),
          data: {
            qty: Number(m.Quantity ?? m.quantity ?? m.Holding ?? 0) || 0,
            price: Number(m.WAP ?? m.wap ?? 0) || 0,
            totalAmount: Number(m.Total_Amount ?? m.total_amount ?? m.HoldingValue ?? 0) || 0,
            trandate: td,
            setdate: m.SETDATE || m.setdate || td,
            isin: m.ISIN || m.isin,
            oldIsin: m.OldISIN || m.oldIsin || "",
            // Corporate-action (merger) record date → Holdings.CA_DATE.
            recordDate: recDate || "",
          },
        };
      }),
  ].sort((a, b) => {
    const dateDiff = new Date(a.date) - new Date(b.date);
    if (dateDiff !== 0) return dateDiff;
    return (EVENT_TYPE_PRIORITY[a.type] ?? 99) - (EVENT_TYPE_PRIORITY[b.type] ?? 99);
  });

  const isSell = (t) => /^SL\+|SQS|OPO|NF-/.test(String(t).toUpperCase());

  const getCostOfHoldings = () =>
    roundAmt(buyQueue.reduce((sum, lot) => sum + lot.qty * lot.price, 0));

  const getWAP = () => (holdings > 0 ? roundAmt(getCostOfHoldings() / holdings) : 0);

  for (const e of events) {
    const t = e.data;

    if (e.type === "TXN") {
      const qty = Math.abs(Number(t.qty) || 0);
      if (!qty) continue;

      const price =
        Number(t.netrate) || (t.netAmount && qty ? t.netAmount / qty : 0);

      if (
        String(t.tranType).toUpperCase() === "OPI" &&
        qty == 1 &&
        Number(price) === 0 &&
        Number(t.netAmount) === 0
      ) {
        continue;
      }

      if (isBuy(t.tranType)) {
        const lotId = ++lotCounter;
        buyQueue.push({
          lotId,
          originalQty: qty,
          qty,
          price,
          buyDate: normalizeDate(t.trandate),
          isActive: true,
        });
        holdings = roundQty(holdings + qty);
        output.push({
          lotId,
          trandate: t.trandate,
          originalTrandate: t.originalTrandate,
          setdate: t.setdate,
          tranType: t.tranType,
          qty,
          price,
          netAmount: t.netAmount,
          holdings,
          costOfHoldings: getCostOfHoldings(),
          averageCostOfHoldings: getWAP(),
          profitLoss: null,
          isActive: true,
          isin: t.ISIN || t.isin,
        });
      }

      if (isSell(t.tranType)) {
        const sellQty = Math.min(qty, holdings);
        let remaining = sellQty;
        let fifoCost = 0;

        while (remaining > 0 && buyQueue.length) {
          const lot = buyQueue[0];
          const used = Math.min(lot.qty, remaining);
          fifoCost = roundAmt(fifoCost + used * lot.price);
          lot.qty = roundQty(lot.qty - used);
          remaining = roundQty(remaining - used);
          if (snapQty(lot.qty) === 0) {
            lot.isActive = false;
            // Fully consumed by this sell → flag the buy's output row inactive
            // too (Holdings.STATUS=false), so the UI shows it as "Inactive".
            const soldRow = output.find(
              (o) => o.lotId === lot.lotId && o.isActive,
            );
            if (soldRow) soldRow.isActive = false;
            buyQueue.shift();
          }
        }

        holdings = roundQty(holdings - sellQty);

        output.push({
          trandate: t.trandate,
          originalTrandate: t.originalTrandate,
          setdate: t.setdate,
          tranType: t.tranType,
          qty,
          price,
          netAmount: t.netAmount,
          holdings,
          costOfHoldings: getCostOfHoldings(),
          averageCostOfHoldings: getWAP(),
          profitLoss: roundAmt(sellQty * price - fifoCost),
          isActive: false,
          isin: t.ISIN || t.isin,
        });
      }
    }

    if (e.type === "BONUS") {
      const qty = Number(e.data.bonusShare) || 0;
      if (!qty) continue;

      const lotId = ++lotCounter;
      buyQueue.push({
        lotId,
        originalQty: qty,
        qty,
        price: 0,
        buyDate: normalizeDate(e.data.exDate),
        isActive: true,
      });
      holdings = roundQty(holdings + qty);
      output.push({
        lotId,
        trandate: e.data.exDate,
        tranType: "BONUS",
        qty,
        price: 0,
        netAmount: 0,
        holdings,
        costOfHoldings: getCostOfHoldings(),
        averageCostOfHoldings: getWAP(),
        profitLoss: null,
        isActive: true,
        isin: e.data.isin,
      });
    }

    if (e.type === "SPLIT") {
      if (!buyQueue.length) continue;
      const ratio1 = Number(e.data.ratio1);
      const ratio2 = Number(e.data.ratio2);
      if (!ratio1 || !ratio2) continue;

      const multiplier = ratio2 / ratio1;
      const splitDate = normalizeDate(e.data.issueDate);
      const activeLots = buyQueue.filter((l) => l.isActive);
      if (!activeLots.length) continue;

      for (const oldLot of activeLots) {
        oldLot.isActive = false;
        const oldRow = output.find((r) => r.lotId === oldLot.lotId && r.isActive);
        if (oldRow) oldRow.isActive = false;
      }
      buyQueue.length = 0;

      let runningHoldings = 0;
      let runningCost = 0;

      for (let i = 0; i < activeLots.length; i++) {
        const oldLot = activeLots[i];
        const newQty = roundQty(oldLot.qty * multiplier);
        const newPrice = oldLot.price / multiplier;
        const newLotId = ++lotCounter;

        buyQueue.push({
          lotId: newLotId,
          originalQty: newQty,
          qty: newQty,
          price: newPrice,
          buyDate: splitDate,
          isActive: true,
        });

        runningHoldings = roundQty(runningHoldings + newQty);
        runningCost = roundAmt(runningCost + newQty * newPrice);
        const runningWAP = runningHoldings > 0 ? roundAmt(runningCost / runningHoldings) : 0;

        const buyRowIndex = output.findIndex((r) => r.lotId === oldLot.lotId);
        let insertIndex = output.length;
        for (let j = buyRowIndex + 1; j < output.length; j++) {
          if (new Date(output[j].trandate) > new Date(splitDate)) {
            insertIndex = j;
            break;
          }
        }

        output.splice(insertIndex, 0, {
          lotId: newLotId,
          trandate: splitDate,
          tranType: "SPLIT",
          qty: newQty,
          price: newPrice,
          netAmount: Number((newQty * newPrice).toFixed(2)),
          holdings: runningHoldings,
          costOfHoldings: runningCost,
          averageCostOfHoldings: runningWAP,
          profitLoss: null,
          isActive: true,
          isin: e.data.isin,
        });
      }

      if (splitLog) {
        splitLog.push({
          ratio1,
          ratio2,
          recordDate: splitDate,
          effectiveDate: e.data.effectiveDate || splitDate,
          securityCode: e.data.securityCode,
          securityName: e.data.securityName,
          oldHolding: holdings,
          newHolding: runningHoldings,
        });
      }

      holdings = runningHoldings;
    }

    if (e.type === "DEMERGER") {
      const qty = Math.abs(Number(e.data.qty) || 0);
      if (!qty) continue;

      let price = Number(e.data.price) || 0;
      let totalAmount = Number(e.data.totalAmount) || 0;
      if (!price && totalAmount && qty) price = totalAmount / qty;
      if (!totalAmount && price && qty) totalAmount = qty * price;

      // Group all rows of one demerger event by record date (the per-lot
      // trandate now varies, so it can't be the grouping key). Close prior lots
      // once per event, then accumulate each surviving lot so every lot's
      // original trade date is preserved (matches the MERGER handler).
      const demergerDate = normalizeDate(e.data.recordDate || e.data.trandate);
      const eventKey = `${demergerDate}|${e.data.isin || ""}`;

      if (eventKey !== lastDemergerEventKey) {
        const activeLots = buyQueue.filter((l) => l.isActive);
        for (const oldLot of activeLots) {
          oldLot.isActive = false;
          const oldRow = output.find((r) => r.lotId === oldLot.lotId && r.isActive);
          if (oldRow) oldRow.isActive = false;
        }
        buyQueue.length = 0;
        holdings = 0;
        lastDemergerEventKey = eventKey;
      }

      const lotId = ++lotCounter;

      buyQueue.push({
        lotId,
        originalQty: qty,
        qty,
        price,
        buyDate: demergerDate,
        isActive: true,
      });

      holdings = roundQty(holdings + qty);

      output.push({
        lotId,
        trandate: e.data.trandate,
        originalTrandate: e.data.trandate,
        setdate: e.data.setdate,
        tranType: "DEMERGER",
        qty,
        price,
        netAmount: totalAmount,
        holdings,
        costOfHoldings: getCostOfHoldings(),
        averageCostOfHoldings: getWAP(),
        profitLoss: null,
        isActive: true,
        isin: e.data.isin,
        caDate: e.data.recordDate || "",
      });
    }

    if (e.type === "MERGER") {
      const qty = Math.abs(Number(e.data.qty) || 0);
      if (!qty) continue;

      let price = Number(e.data.price) || 0;
      let totalAmount = Number(e.data.totalAmount) || 0;
      if (!price && totalAmount && qty) price = totalAmount / qty;
      if (!totalAmount && price && qty) totalAmount = qty * price;

      // The received lots are dated by the merger record date.
      const mergerDate = normalizeDate(e.data.recordDate || e.data.trandate);
      // A merger ADDS the received shares to whatever this account already
      // holds in the surviving security (e.g. 50 OPI + 660 merger = 710).
      // With no prior holding this simply starts from the merger quantity.

      const lotId = ++lotCounter;

      buyQueue.push({
        lotId,
        originalQty: qty,
        qty,
        price,
        buyDate: mergerDate,
        isActive: true,
      });

      holdings = roundQty(holdings + qty);

      output.push({
        lotId,
        trandate: e.data.trandate,
        originalTrandate: e.data.trandate,
        setdate: e.data.setdate,
        tranType: "MERGER",
        qty,
        price,
        netAmount: totalAmount,
        holdings,
        costOfHoldings: getCostOfHoldings(),
        averageCostOfHoldings: getWAP(),
        profitLoss: null,
        isActive: true,
        isin: e.data.isin,
        caDate: e.data.recordDate || "",
      });
    }
  }

  if (card) {
    if (!output.length) {
      return {
        isin: "",
        holdings: 0,
        holdingValue: 0,
        averageCostOfHoldings: 0,
      };
    }
    const last = output[output.length - 1];
    return {
      isin: last.isin || "",
      holdings: last.holdings || 0,
      holdingValue: last.costOfHoldings || 0,
      averageCostOfHoldings: last.averageCostOfHoldings || 0,
    };
  }

  return output;
}

/* ============================== HOLDINGS WRITER ============================== */

/** ZCQL removes at most this many rows per DELETE statement. */
const ZCQL_DELETE_LIMIT = 300;

async function deleteHoldingsForPair(zcql, accountCode, isin) {
  // A pair can hold more than 300 rows. One DELETE would leave the rest behind
  // and the rebuild would then insert on top of them, duplicating the pair, so
  // repeat until a pass removes fewer rows than the limit.
  for (;;) {
    const result = await zcql.executeZCQLQuery(`
      DELETE FROM Holdings
      WHERE WS_Account_code = '${esc(accountCode)}' AND ISIN = '${esc(isin)}'
    `);
    const deleted = Number(result?.[0]?.Holdings?.DELETED_ROWS_COUNT) || 0;
    if (deleted < ZCQL_DELETE_LIMIT) return;
  }
}

async function insertHoldingsRow(zcql, accountCode, row, displayIsin) {
  const txD = sqlDate(row.originalTrandate || row.trandate);
  const setD = sqlDate(row.setdate || row.trandate);
  const typ = String(row.tranType ?? "").trim();
  const qty = Number(row.qty) || 0;
  const price = Number(row.price) || 0;
  const totalAmt = Number(row.netAmount) || 0;
  const holding = Number(row.holdings) || 0;
  const wap = Number(row.averageCostOfHoldings) || 0;
  const hv = Number(row.costOfHoldings) || 0;
  const pl =
    row.profitLoss === null || row.profitLoss === undefined
      ? "NULL"
      : Number(row.profitLoss);
  const status = row.isActive ? "true" : "false";

  // Corporate-action date (e.g. merger record date). Only set on rows that
  // carry one (MERGER); NULL otherwise so it's a true date column.
  const caDateRaw = row.caDate ? sqlDate(row.caDate) : "";
  const caDateSql = caDateRaw ? `'${esc(caDateRaw)}'` : "NULL";

  await zcql.executeZCQLQuery(`
    INSERT INTO Holdings (
      WS_Account_code, TRANSACTION_DATE, SETTLEMENT_DATE, TYPE, ISIN,
      QUANTITY, PRICE, TOTAL_AMOUNT, HOLDING, WAP, HOLDING_VALUE, P_L, STATUS, CA_DATE
    ) VALUES (
      '${esc(accountCode)}',
      '${esc(txD)}',
      '${esc(setD)}',
      '${esc(typ)}',
      '${esc(displayIsin)}',
      ${qty},
      ${price},
      ${totalAmt},
      ${holding},
      ${wap},
      ${hv},
      ${pl},
      ${status},
      ${caDateSql}
    )
  `);
}

/* ============================== SPLIT_ACCOUNT WRITER ==============================
   Split_Account = one row per (account, ISIN, split applied): holding just before
   and just after the split. It is kept in step with the Holdings rows, so it
   always matches Holdings (a back-dated trade re-sizes it automatically).
   The split impact report reads it instead of summing Holdings SPLIT rows.

   The Split page's custodian reconciliation stores Status / Received_Date on
   the same rows. Each side owns its own columns:
     - this writer owns the holding columns and never writes Status /
       Received_Date. Existing rows are UPDATEd in place (not deleted and
       re-inserted), so a status can never be lost half-way through a rebuild.
     - a row is deleted only when the account no longer has that split (or it
       is a duplicate). RECEIVABLE rows of a split not applied yet (not in
       splitLog) are left alone — the Split page is waiting to confirm them. */

const round4 = (n) => Math.round((Number(n) || 0) * 10000) / 10000;

/** One split event of one account: record date + ratio. */
const splitEventKey = (recordDate, ratio1, ratio2) =>
  `${sqlDate(recordDate)}|${Number(ratio1) || 0}|${Number(ratio2) || 0}`;

async function writeSplitAccountRows(zcql, accountCode, isin, splitLog) {
  // An account holds very few splits of one ISIN, so one page is enough.
  const existing = (
    (await zcql.executeZCQLQuery(`
      SELECT ROWID, Record_Date, Ratio1, Ratio2, Status
      FROM Split_Account
      WHERE WS_Account_code = '${esc(accountCode)}' AND ISIN = '${esc(isin)}'
    `)) || []
  ).map((row) => row.Split_Account || row);
  const statusOf = (row) => String(row.Status || "").trim().toUpperCase();
  // If an event somehow has two rows, keep the one that carries a status.
  existing.sort((a, b) => (statusOf(b) ? 1 : 0) - (statusOf(a) ? 1 : 0));

  const loggedKeys = new Set(splitLog.map((s) => splitEventKey(s.recordDate, s.ratio1, s.ratio2)));
  const rowIdByKey = new Map(); // event key → the row to update
  const rowIdsToDelete = [];
  for (const row of existing) {
    const key = splitEventKey(row.Record_Date, row.Ratio1, row.Ratio2);
    if (loggedKeys.has(key)) {
      if (rowIdByKey.has(key)) rowIdsToDelete.push(row.ROWID); // duplicate
      else rowIdByKey.set(key, row.ROWID);
      continue;
    }
    if (statusOf(row) === "RECEIVABLE") continue; // split not applied yet
    rowIdsToDelete.push(row.ROWID); // the account no longer has this split
  }

  if (rowIdsToDelete.length > 0) {
    await zcql.executeZCQLQuery(`
      DELETE FROM Split_Account WHERE ROWID IN (${rowIdsToDelete.join(", ")})
    `);
  }

  for (const s of splitLog) {
    const recordDate = sqlDate(s.recordDate);
    const effectiveDate = sqlDate(s.effectiveDate) || recordDate;
    const rowId = rowIdByKey.get(splitEventKey(s.recordDate, s.ratio1, s.ratio2));
    if (rowId) {
      await zcql.executeZCQLQuery(`
        UPDATE Split_Account SET
          Security_Code = '${esc(s.securityCode)}',
          Security_Name = '${esc(s.securityName)}',
          Issue_Date = ${effectiveDate ? `'${effectiveDate}'` : "NULL"},
          Old_Holding = ${round4(s.oldHolding)},
          New_Holding = ${round4(s.newHolding)}
        WHERE ROWID = ${rowId}
      `);
      continue;
    }
    await zcql.executeZCQLQuery(`
      INSERT INTO Split_Account (
        ISIN, WS_Account_code, Security_Code, Security_Name, Ratio1, Ratio2,
        Record_Date, Issue_Date, Old_Holding, New_Holding
      ) VALUES (
        '${esc(isin)}',
        '${esc(accountCode)}',
        '${esc(s.securityCode)}',
        '${esc(s.securityName)}',
        ${Number(s.ratio1) || 0},
        ${Number(s.ratio2) || 0},
        ${recordDate ? `'${recordDate}'` : "NULL"},
        ${effectiveDate ? `'${effectiveDate}'` : "NULL"},
        ${round4(s.oldHolding)},
        ${round4(s.newHolding)}
      )
    `);
  }
}

/* ============================== PER-ACCOUNT REBUILD ============================== */

async function rebuildHoldingsForAccount(
  zcql,
  accountCode,
  asOnDate,
  counters,
  scopedIsinsSet = null,
  keepCaRecords = false,
) {
  const t0 = Date.now();
  const scoped = scopedIsinsSet && scopedIsinsSet.size > 0;

  const transactions = await fetchAccountTransactions(zcql, accountCode, asOnDate);
  const bonuses = await fetchAccountBonuses(zcql, accountCode, asOnDate);
  const demergers = await fetchAccountDemergers(zcql, accountCode, asOnDate);
  const mergers = await fetchAccountMergers(zcql, accountCode, asOnDate);

  const isins = new Set();
  for (const t of transactions) if (t.ISIN) isins.add(t.ISIN);
  for (const b of bonuses) if (b.ISIN) isins.add(b.ISIN);
  for (const d of demergers) if (d.ISIN) isins.add(d.ISIN);
  for (const m of mergers) if (m.ISIN) isins.add(m.ISIN);

  let isinList = [...isins];
  // The hardcoded ISINS_FILTER is a dev/debug knob; an explicit job scope
  // (scopedIsinsSet) takes precedence over it.
  if (!scoped && ISINS_FILTER.length > 0) {
    const allow = new Set(ISINS_FILTER.map(String));
    isinList = isinList.filter((i) => allow.has(i));
  }

  const splitQueryIsins = scoped ? [...scopedIsinsSet].filter(Boolean) : isinList;
  const splits = await fetchSplitsForIsins(zcql, splitQueryIsins, asOnDate);

  // Match dashboard behavior: ISINs that appear as Merger.OldISIN are merged
  // away — the surviving (new) ISIN is used instead.
  const mergedAwayIsins = new Set();
  for (const m of mergers) {
    const oldIsin = String(m.OldISIN || m.oldIsin || "").trim();
    if (oldIsin) mergedAwayIsins.add(oldIsin);
  }

  // Group inputs by ISIN.
  const txByIsin = {};
  const bonusByIsin = {};
  const splitByIsin = {};
  const demergerByIsin = {};
  const mergerByIsin = {};

  for (const t of transactions) {
    if (!t.ISIN) continue;
    (txByIsin[t.ISIN] = txByIsin[t.ISIN] || []).push(t);
  }
  for (const b of bonuses) {
    if (!b.ISIN) continue;
    (bonusByIsin[b.ISIN] = bonusByIsin[b.ISIN] || []).push(b);
  }
  for (const s of splits) {
    if (!s.isin) continue;
    (splitByIsin[s.isin] = splitByIsin[s.isin] || []).push(s);
  }
  for (const d of demergers) {
    if (!d.ISIN) continue;
    (demergerByIsin[d.ISIN] = demergerByIsin[d.ISIN] || []).push(d);
  }
  for (const m of mergers) {
    if (!m.ISIN) continue;
    (mergerByIsin[m.ISIN] = mergerByIsin[m.ISIN] || []).push(m);
  }

  let targetIsins;
  if (scoped) {
    // Scoped rebuild: only the ISIN(s) the corporate action touched, for this
    // account. ISINs the account doesn't hold yield 0 FIFO rows (harmless).
    targetIsins = [...scopedIsinsSet]
      .filter((i) => i && !mergedAwayIsins.has(i))
      .sort((a, b) => a.localeCompare(b));
  } else {
    targetIsins = isinList.filter((i) => !mergedAwayIsins.has(i));
  }

  console.log(
    `[${accountCode}] tx=${transactions.length} bon=${bonuses.length} ` +
    `spl=${splits.length} dmg=${demergers.length} mrg=${mergers.length} ` +
    `isins=${isinList.length} mergedAway=${mergedAwayIsins.size} ` +
    `scoped=${scoped ? scopedIsinsSet.size : "off"} ` +
    `pairs=${targetIsins.length} (fetched in ${Date.now() - t0}ms)`,
  );

  for (const isin of targetIsins) {
    if (MAX_PAIRS > 0 && counters.pairs >= MAX_PAIRS) {
      console.log(`MAX_PAIRS=${MAX_PAIRS} reached, stopping early`);
      return;
    }

    counters.pairs++;

    let fifoRows;
    const splitLog = [];
    try {
      fifoRows = runFifoEngine(
        txByIsin[isin] || [],
        bonusByIsin[isin] || [],
        splitByIsin[isin] || [],
        false,
        demergerByIsin[isin] || [],
        mergerByIsin[isin] || [],
        splitLog,
      );
    } catch (err) {
      console.error(`[${accountCode}/${isin}] FIFO failed:`, err.message);
      counters.errors++;
      continue;
    }

    const rowCount = Array.isArray(fifoRows) ? fifoRows.length : 0;

    if (DRY_RUN) {
      console.log(
        `[DRY_RUN][${accountCode}/${isin}] would write ${rowCount} row(s)`,
      );
      counters.rows += rowCount;
      continue;
    }

    if (!rowCount) {
      try {
        await deleteHoldingsForPair(zcql, accountCode, isin);
      } catch (err) {
        console.error(`[${accountCode}/${isin}] delete (empty) failed:`, err.message);
        counters.errors++;
      }
    } else {
      try {
        await deleteHoldingsForPair(zcql, accountCode, isin);
        for (const r of fifoRows) {
          await insertHoldingsRow(zcql, accountCode, r, isin);
        }
        counters.rows += rowCount;
      } catch (err) {
        console.error(`[${accountCode}/${isin}] rebuild failed:`, err.message);
        counters.errors++;
      }
    }

    // A transaction deletion must leave corporate action records exactly as
    // saved, so it skips the per-account split rows (see keepCaRecords in index.js).
    if (keepCaRecords) continue;

    // Separate try: a Split_Account failure must not undo a good Holdings
    // rebuild, but it still counts as an error so the job reports it.
    try {
      await writeSplitAccountRows(zcql, accountCode, isin, splitLog);
    } catch (err) {
      console.error(`[${accountCode}/${isin}] Split_Account write failed:`, err.message);
      counters.errors++;
    }
  }
}

async function rebuildHoldingsForAccountList(
  zcql,
  accountCodes,
  asOnDate,
  scopedIsinsSet = null,
  keepCaRecords = false,
) {
  const counters = { pairs: 0, rows: 0, errors: 0 };
  const cutoff = asOnDate !== undefined && asOnDate !== null ? asOnDate : null;
  const list = Array.isArray(accountCodes) ? accountCodes : [];
  for (const accountCode of list) {
    try {
      await rebuildHoldingsForAccount(
        zcql,
        accountCode,
        cutoff,
        counters,
        scopedIsinsSet,
        keepCaRecords,
      );
    } catch (err) {
      console.error(`[RebuildHoldingtable holdings][${accountCode}]`, err.message);
      counters.errors++;
    }
  }
  const scopeStr =
    scopedIsinsSet && scopedIsinsSet.size > 0
      ? `, scopedIsins=${scopedIsinsSet.size}`
      : "";
  console.log(
    `[RebuildHoldingtable] holdings rebuild: ${list.length} accounts${scopeStr}, pairs=${counters.pairs}, rows=${counters.rows}, errors=${counters.errors}`
  );
  return counters;
}

module.exports = { rebuildHoldingsForAccountList };
