export const runFifoCardEngine = (transactions, bonuses, card = false) => {
    let holdings = 0;
    const buyQueue = [];
    const output = [];
  
    const normalizeDate = (rawDate) => {
      if (!rawDate) return null;
      const [y, m, d] = rawDate.split("-").map(Number);
      const fullYear = y < 100 ? 2000 + y : y;
      return `${fullYear}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
    };
  
    const events = [
      ...transactions.map((t) => ({
        type: "TXN",
        date: normalizeDate(t.TRANDATE || t.trandate),
        data: {
          tranType: t.Tran_Type || t.tranType,
          qty: t.QTY || t.qty,
          netrate: t.NETRATE || t.netrate,
          netAmount: t.NETAMOUNT || t.netAmount,
          trandate: t.TRANDATE || t.trandate,
        },
      })),
      ...bonuses.map((b) => ({
        type: "BONUS",
        date: normalizeDate(b.ExDate || b.exDate),
        data: {
          bonusShare: b.BonusShare || b.bonusShare,
          exDate: b.ExDate || b.exDate,
        },
      })),
    ].sort((a, b) => new Date(a.date) - new Date(b.date));
  
    const isBuy = (t) => /^BY-|SQB|OPI/.test(String(t).toUpperCase());
    const isSell = (t) => /^SL\+|SQS|OPO|NF-/.test(String(t).toUpperCase());
  
    const getCostOfHoldings = () =>
      buyQueue.reduce((sum, lot) => sum + lot.qty * lot.price, 0);
  
    const getWAP = () => (holdings > 0 ? getCostOfHoldings() / holdings : 0);
  
    for (const e of events) {
      if (e.type === "TXN") {
        const t = e.data;
        const qty = Math.abs(Number(t.qty) || 0);
        if (!qty) continue;
  
        const price =
          Number(t.netrate) ||
          (t.netAmount && qty ? t.netAmount / qty : 0);
  
        let profitLoss = null;
  
        if (isBuy(t.tranType)) {
          buyQueue.push({ qty, price });
          holdings += qty;
        }
  
        if (isSell(t.tranType)) {
          let remaining = qty;
          let fifoCost = 0;
  
          while (remaining > 0 && buyQueue.length) {
            const lot = buyQueue[0];
            const used = Math.min(lot.qty, remaining);
  
            fifoCost += used * lot.price;
            lot.qty -= used;
            remaining -= used;
  
            if (lot.qty === 0) buyQueue.shift();
          }
  
          profitLoss = qty * price - fifoCost;
          holdings -= qty;
        }
  
        output.push({
          trandate: t.trandate,
          holdings,
          costOfHoldings: getCostOfHoldings(),
          averageCostOfHoldings: getWAP(),
          profitLoss,
        });
      }
  
      if (e.type === "BONUS") {
        const qty = Number(e.data.bonusShare) || 0;
        if (!qty) continue;
  
        buyQueue.push({ qty, price: 0 });
        holdings += qty;
  
        output.push({
          trandate: e.data.exDate,
          holdings,
          costOfHoldings: getCostOfHoldings(),
          averageCostOfHoldings: getWAP(),
          profitLoss: null,
        });
      }
    }
  
    if (card) {
      const last = output[output.length - 1] || {};
      return {
        holdingValue: last.costOfHoldings || 0,
        averageCostOfHoldings: last.averageCostOfHoldings || 0,
      };
    }
  
    return output;
  };
  