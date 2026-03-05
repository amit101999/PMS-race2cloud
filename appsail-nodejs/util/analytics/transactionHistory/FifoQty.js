export const runFifoEngine = (
    transactions = [],
    bonuses = [],
    splits = []
  ) => {
    if (!transactions.length && !bonuses.length && !splits.length) {
      return { isin: "", holdings: 0 };
    }
  
    const activeIsin =
      transactions[0]?.ISIN ||
      bonuses[0]?.ISIN ||
      splits[0]?.isin ||
      null;
  
    if (!activeIsin) return { isin: "", holdings: 0 };
  
    let holdings = 0;
  
    /* ---------------- HELPERS ---------------- */
    const isBuy = (t) => /^BY-|SQB|OPI/.test(String(t).toUpperCase());
    const isSell = (t) => /^SL\+|SQS|OPO|NF-/.test(String(t).toUpperCase());
  
    /* ---------------- PROCESS TRANSACTIONS ---------------- */
    for (const txn of transactions) {
      if (txn.ISIN !== activeIsin) continue;
  
      const qty = Math.abs(Number(txn.QTY) || 0);
      if (!qty) continue;
  
      const type = txn.Tran_Type;
  
      if (isBuy(type)) {
        holdings += qty;
      }
  
      if (isSell(type)) {
        holdings -= qty;
      }
    }
  
    /* ---------------- PROCESS BONUS ---------------- */
    for (const bonus of bonuses) {
      if (bonus.ISIN !== activeIsin) continue;
  
      const qty = Number(bonus.BonusShare) || 0;
      holdings += qty;
    }
  
    /* ---------------- PROCESS SPLITS ---------------- */
    for (const split of splits) {
      if (split.isin !== activeIsin) continue;
  
      const ratio1 = Number(split.ratio1);
      const ratio2 = Number(split.ratio2);
  
      if (!ratio1 || !ratio2) continue;
  
      const multiplier = ratio2 / ratio1;
      holdings = holdings * multiplier;
    }
  
    /* Safety */
    if (holdings < 0) holdings = 0;
  
    return {
      isin: activeIsin,
      holdings
    };
  };
  