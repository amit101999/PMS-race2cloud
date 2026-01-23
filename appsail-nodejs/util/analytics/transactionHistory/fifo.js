export const runFifoEngine = (
  transactions = [],
  bonuses = [],
  splits = [],
  card = false
) => {
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
  let Balance = 0;

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
  // const events = [
  //   ...transactions.map((t) => ({
  //     type: "TXN",
  //     date: normalizeDate(t.TRANDATE || t.trandate),
  //     data: {
  //       tranType: t.Tran_Type || t.tranType,
  //       qty: t.QTY || t.qty,
  //       netrate: t.NETRATE || t.netrate,
  //       netAmount: t.NETAMOUNT || t.netAmount,
  //       trandate: t.TRANDATE || t.trandate,
  //       isin: t.ISIN || t.isin,
  //     },
  //   })),
  //   ...bonuses.map((b) => ({
  //     type: "BONUS",
  //     date: normalizeDate(b.ExDate || b.exDate),
  //     data: {
  //       bonusShare: b.BonusShare || b.bonusShare,
  //       exDate: b.ExDate || b.exDate,
  //       isin: b.ISIN || b.isin,
  //     },
  //   })),
  //   ...splits.map((s) => ({
  //     type: "SPLIT",
  //     date: normalizeDate(s.issueDate),
  //     data: {
  //       ratio1: s.ratio1,
  //       ratio2: s.ratio2,
  //       issueDate: s.issueDate,
  //       isin: s.isin,
  //     },
  //   })),
  // ].sort((a, b) => new Date(a.date) - new Date(b.date));

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

  /* ---------------- HELPERS ---------------- */
  const isBuy = (t) => /^BY-|SQB|OPI/.test(String(t).toUpperCase());
  const isSell = (t) => /^SL\+|SQS|OPO|NF-/.test(String(t).toUpperCase());

  const getCostOfHoldings = () =>
    buyQueue.reduce((sum, lot) => sum + lot.qty * lot.price, 0);

  const getWAP = () => (holdings > 0 ? getCostOfHoldings() / holdings : 0);

  // const calculateCashBalance = (currentBalance, tranType, amount) => {
  //   let cashBalance = currentBalance;

  //   if (
  //     tranType === "CS+" || // Fund Deposit
  //     tranType === "SL+" || // Sell
  //     tranType === "DIO" || // Other Income
  //     tranType === "DI0" || // Dividend
  //     tranType === "CSI" || // Cash Interest
  //     tranType === "DIS" || // Gain Distribution
  //     tranType === "IN1" || // Interest Received
  //     tranType === "OI1" || // Other Income
  //     tranType === "SQS" || // Square up Sell
  //     tranType === "DI1" || // Dividend Received
  //     tranType === "IN+" // Interest
  //   ) {
  //     cashBalance += amount;
  //   }

  //   // -------- CASH SUBTRACT --------
  //   else if (
  //     tranType === "BY-" || // Buy
  //     tranType === "MGF" || // Management Fees
  //     tranType === "TDO" || // TDS Trf to Capital A/c
  //     tranType === "TDI" || // Trf to TDS A/c
  //     tranType === "E22" || // Account Opening Charges
  //     tranType === "E01" || // Other Expenses
  //     tranType === "CUS" || // Custody Charges
  //     tranType === "E23" || // Stamp Duty
  //     tranType === "CS-" || // Fund Withdrawal
  //     tranType === "MGE" || // Fund Accountant Fees
  //     tranType === "E10" || // Audit Fee
  //     tranType === "PRF" || // Performance Fees
  //     tranType === "NF-" || // MF Applications
  //     tranType === "SQB" // Square up Buy
  //   ) {
  //     cashBalance -= amount;
  //   }

  //   // -------- NO CASH IMPACT --------
  //   else if (
  //     tranType === "OPI" || // Security in
  //     tranType === "OPO" || // Security out
  //     tranType === "RD0" // Dividend Reinvest
  //   ) {
  //     // No change to cash balance
  //     cashBalance = cashBalance;
  //   }

  //   // -------- UNKNOWN TYPE --------
  //   else {
  //     console.warn("Unknown Tran Type:", tranType);
  //   }

  //   Balance = cashBalance;
  //   return cashBalance;
  // };

  /* ---------------- PROCESS EVENTS ---------------- */
  for (const e of events) {
    /* ========== BUY / SELL ========== */
    const t = e.data;
    if (e.type === "TXN") {
      // Balance = calculateCashBalance(
      //   Balance,
      //   t.tranType,
      //   Number(t.netAmount || 0)
      // );
      const qty = Math.abs(Number(t.qty) || 0);
      // const qty = Math.min(Math.abs(Number(t.qty) || 0), holdings);

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
          isin: t.ISIN || t.isin,
          // cashBalance: Balance,
        });
        // console.log("Output Queue:", output);
      }

      /* ---- SELL ---- */
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
            lot.isActive = false; // 🔥 mark inactive
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
          isActive: false, // 🔥 mark inactive
          isin: t.ISIN || t.isin,
          // cashBalance: Balance,
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
        isin: e.data.isin,
        // cashBalance: 0,
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
          isin: e.data.isin,
          // cashBalance: 0,
        });
      }

      // 8️⃣ Final authoritative holdings
      holdings = runningHoldings;
    }
  }

  /* ---------------- CARD MODE ---------------- */
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
    // return {
    //   isin: key || "",
    //   holdings,
    //   holdingValue: getCostOfHoldings(),
    //   averageCostOfHoldings: getWAP(),
    // };
  }

  return output;
};
