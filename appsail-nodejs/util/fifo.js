export const runFifoEngine = (
  transactions = [],
  bonuses = [],
  splits = [],
  card = false
) => {
  let holdings = 0;
  let lotCounter = 0;

  const buyQueue = [];
  const output = [];

  /* ---------------- DATE NORMALIZATION ---------------- */
  const normalizeDate = (rawDate) => {
    if (!rawDate) return null;
    const [y, m, d] = rawDate.split("-").map(Number);
    const fullYear = y < 100 ? 2000 + y : y;
    return `${fullYear}-${String(m).padStart(2, "0")}-${String(d).padStart(
      2,
      "0"
    )}`;
  };

  /* ---------------- MERGE EVENTS ---------------- */
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
    ...splits.map((s) => ({
      type: "SPLIT",
      date: normalizeDate(s.issueDate),
      data: {
        ratio1: s.ratio1,
        ratio2: s.ratio2,
        issueDate: s.issueDate,
      },
    })),
  ].sort((a, b) => new Date(a.date) - new Date(b.date));

  /* ---------------- HELPERS ---------------- */
  const isBuy = (t) => /^BY-|SQB|OPI/.test(String(t).toUpperCase());
  const isSell = (t) => /^SL\+|SQS|OPO|NF-/.test(String(t).toUpperCase());

  const getCostOfHoldings = () =>
    buyQueue.reduce((sum, lot) => sum + lot.qty * lot.price, 0);

  const getWAP = () => (holdings > 0 ? getCostOfHoldings() / holdings : 0);

  /* ---------------- PROCESS EVENTS ---------------- */
  for (const e of events) {
    /* ========== BUY / SELL ========== */
    if (e.type === "TXN") {
      const t = e.data;
      const qty = Math.abs(Number(t.qty) || 0);
      if (!qty) continue;

      const price =
        Number(t.netrate) || (t.netAmount && qty ? t.netAmount / qty : 0);

      /* ---- BUY ---- */
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
        });
      }

      /* ---- SELL ---- */
      if (isSell(t.tranType)) {
        let remaining = qty;
        let fifoCost = 0;

        while (remaining > 0 && buyQueue.length) {
          const lot = buyQueue[0];
          const used = Math.min(lot.qty, remaining);

          fifoCost += used * lot.price;
          lot.qty -= used;
          remaining -= used;

          if (lot.qty === 0) {
            lot.isActive = false; // 🔥 mark inactive
            buyQueue.shift();
          }
        }

        holdings -= qty;

        output.push({
          trandate: t.trandate,
          tranType: t.tranType,
          qty,
          price,
          netAmount: t.netAmount,
          holdings,
          costOfHoldings: getCostOfHoldings(),
          averageCostOfHoldings: getWAP(),
          profitLoss: qty * price - fifoCost,
          isActive: false, // 🔥 mark inactive
        });
      }
    }

    /* ========== BONUS ========== */
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
      });
    }

    /* ========== SPLIT (REPLACEMENT MODEL – FINAL & CORRECT) ========== */
    if (e.type === "SPLIT") {
      if (!buyQueue.length) continue;

      const ratio1 = Number(e.data.ratio1);
      const ratio2 = Number(e.data.ratio2);
      if (!ratio1 || !ratio2) continue;

      const multiplier = ratio2 / ratio1;
      const splitDate = normalizeDate(e.data.issueDate);

      // 1️⃣ Get active lots before split
      const activeLots = buyQueue.filter((l) => l.isActive);

      if (!activeLots.length) continue;

      // 2️⃣ Mark old lots & their BUY rows inactive
      for (const oldLot of activeLots) {
        oldLot.isActive = false;

        const oldRow = output.find(
          (r) => r.lotId === oldLot.lotId && r.isActive
        );
        if (oldRow) oldRow.isActive = false;
      }

      // 3️⃣ Reset FIFO
      buyQueue.length = 0;

      // 4️⃣ Running snapshot for ledger rows
      let runningHoldings = 0;
      let runningCost = 0;

      // 5️⃣ Create split lots one by one (ledger-correct)
      for (const oldLot of activeLots) {
        const newQty = oldLot.qty * multiplier;
        const newPrice = oldLot.price / multiplier;
        const newLotId = ++lotCounter;

        // push new active lot
        buyQueue.push({
          lotId: newLotId,
          originalQty: newQty,
          qty: newQty,
          price: newPrice,
          buyDate: splitDate,
          isActive: true,
        });

        // update running snapshot
        runningHoldings += newQty;
        runningCost += newQty * newPrice;

        const runningWAP =
          runningHoldings > 0 ? runningCost / runningHoldings : 0;

        // 6️⃣ Find correct insert position
        const buyRowIndex = output.findIndex((r) => r.lotId === oldLot.lotId);

        let insertIndex = output.length;
        for (let i = buyRowIndex + 1; i < output.length; i++) {
          if (new Date(output[i].trandate) > new Date(splitDate)) {
            insertIndex = i;
            break;
          }
        }

        // 7️⃣ Insert SPLIT ledger row
        output.splice(insertIndex, 0, {
          lotId: newLotId,
          trandate: splitDate, // ✅ record / issue date
          tranType: "SPLIT",
          qty: newQty,
          price: newPrice,
          netAmount: Number((newQty * newPrice).toFixed(2)), // ✅ qty × price
          holdings: runningHoldings,
          costOfHoldings: runningCost,
          averageCostOfHoldings: runningWAP,
          profitLoss: null,
          isActive: true,
        });
      }

      // 8️⃣ Final authoritative holdings
      holdings = runningHoldings;
    }
  }

  /* ---------------- CARD MODE ---------------- */
  if (card) {
    const last = [...output].reverse().find((r) => r.costOfHoldings !== null);
    return {
      holdingValue: last?.costOfHoldings || 0,
      averageCostOfHoldings: last?.averageCostOfHoldings || 0,
    };
  }

  return output;
};
