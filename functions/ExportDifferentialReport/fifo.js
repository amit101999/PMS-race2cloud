"use strict";

function runFifoEngine(transactions, bonuses, splits, card) {
  if (!transactions) transactions = [];
  if (!bonuses) bonuses = [];
  if (!splits) splits = [];
  if (card === undefined) card = false;

  const activeIsin =
    transactions[0]?.ISIN ||
    transactions[0]?.isin ||
    bonuses[0]?.ISIN ||
    bonuses[0]?.isin ||
    splits[0]?.isin ||
    null;

  let holdings = 0;
  let lotCounter = 0;
  const buyQueue = [];
  const output = [];

  const normalizeDate = (rawDate) => {
    if (!rawDate) return null;
    const [y, m, d] = rawDate.split("-").map(Number);
    const fullYear = y < 100 ? 2000 + y : y;
    return `${fullYear}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
  };

  const events = [
    ...transactions
      .filter((t) => (t.ISIN || t.isin) === activeIsin)
      .map((t) => ({
        type: "TXN",
        date: normalizeDate(t.TRANDATE || t.trandate),
        data: {
          tranType: t.Tran_Type || t.tranType,
          qty: t.QTY || t.qty,
          netrate: t.NETRATE || t.netrate,
          netAmount: t.NETAMOUNT || t.netAmount || t.Net_Amount || 0,
          trandate: t.TRANDATE || t.trandate,
          isin: t.ISIN || t.isin,
        },
      })),
    ...bonuses
      .filter((b) => (b.ISIN || b.isin) === activeIsin)
      .map((b) => ({
        type: "BONUS",
        date: normalizeDate(b.ExDate || b.exDate),
        data: {
          bonusShare: b.BonusShare || b.bonusShare,
          exDate: b.ExDate || b.exDate,
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
          isin: s.isin,
        },
      })),
  ].sort((a, b) => new Date(a.date) - new Date(b.date));

  const isBuy = (t) => /^BY-|SQB|OPI/.test(String(t).toUpperCase());
  const isSell = (t) => /^SL\+|SQS|OPO|NF-/.test(String(t).toUpperCase());
  const getCostOfHoldings = () =>
    buyQueue.reduce((sum, lot) => sum + lot.qty * lot.price, 0);
  const getWAP = () => (holdings > 0 ? getCostOfHoldings() / holdings : 0);

  for (const e of events) {
    const t = e.data;
    if (e.type === "TXN") {
      const qty = Math.abs(Number(t.qty) || 0);
      if (!qty) continue;
      const price =
        Number(t.netrate) || (t.netAmount && qty ? t.netAmount / qty : 0);

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
        holdings += qty;
        output.push({
          lotId,
          trandate: t.trandate,
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
          fifoCost += used * lot.price;
          lot.qty -= used;
          remaining -= used;
          if (lot.qty === 0) {
            lot.isActive = false;
            buyQueue.shift();
          }
        }
        holdings -= sellQty;
        output.push({
          trandate: t.trandate,
          tranType: t.tranType,
          qty,
          price,
          netAmount: t.netAmount,
          holdings,
          costOfHoldings: getCostOfHoldings(),
          averageCostOfHoldings: getWAP(),
          profitLoss: sellQty * price - fifoCost,
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
      holdings += qty;
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

      for (const oldLot of activeLots) {
        const newQty = Math.floor(oldLot.qty * multiplier);
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
        runningHoldings += newQty;
        runningCost += newQty * newPrice;
        const runningWAP = runningHoldings > 0 ? runningCost / runningHoldings : 0;
        const buyRowIndex = output.findIndex((r) => r.lotId === oldLot.lotId);
        let insertIndex = output.length;
        for (let i = buyRowIndex + 1; i < output.length; i++) {
          if (new Date(output[i].trandate) > new Date(splitDate)) {
            insertIndex = i;
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
      holdings = runningHoldings;
    }
  }

  if (card) {
    if (!output.length) {
      return { isin: "", holdings: 0, holdingValue: 0, averageCostOfHoldings: 0 };
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

module.exports = { runFifoEngine };
