export const runHoldingQuantityEngine = (
    transactions = [],
    bonuses = [],
    splits = []
  ) => {
    let holdings = 0;
    const output = [];
  
    const normalizeDate = (d) => {
      if (!d) return null;
      const [y, m, day] = d.split("-").map(Number);
      const fullYear = y < 100 ? 2000 + y : y;
      return `${fullYear}-${String(m).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
    };
  
    const isBuy = (t) => /^BY-|SQB|OPI/.test(String(t).toUpperCase());
    const isSell = (t) => /^SL\+|SQS|OPO|NF-/.test(String(t).toUpperCase());
  
    /* -------- Merge & Sort Events -------- */
    const events = [
      ...transactions.map((t) => ({
        type: "TXN",
        date: normalizeDate(t.setdate || t.SETDATE || t.trandate || t.TRANDATE),
        data: t,
      })),
      ...bonuses.map((b) => ({
        type: "BONUS",
        date: normalizeDate(b.exDate || b.ExDate),
        data: b,
      })),
      ...splits.map((s) => ({
        type: "SPLIT",
        date: normalizeDate(s.issueDate),
        data: s,
      })),
    ].sort((a, b) => new Date(a.date) - new Date(b.date));
  
    /* -------- Process Events -------- */
    for (const e of events) {
      if (e.type === "TXN") {
        const t = e.data;
        const qty = Math.abs(Number(t.qty || t.QTY) || 0);
        if (!qty) continue;
  
        const tranType = t.tranType || t.Tran_Type;
  
        if (isBuy(tranType)) {
          holdings += qty;
        }
  
        if (isSell(tranType)) {
          holdings -= qty;
          if (holdings < 0) holdings = 0; // safety
        }
  
        output.push({
          date: e.date,
          tranType,
          qty,
          holdings,
        });
      }
  
      /* -------- BONUS -------- */
      if (e.type === "BONUS") {
        const qty = Number(e.data.bonusShare || e.data.BonusShare) || 0;
        if (!qty) continue;
  
        holdings += qty;
  
        output.push({
          date: e.date,
          tranType: "BONUS",
          qty,
          holdings,
        });
      }
  
      /* -------- SPLIT -------- */
      if (e.type === "SPLIT") {
        const r1 = Number(e.data.ratio1);
        const r2 = Number(e.data.ratio2);
        if (!r1 || !r2) continue;
  
        const multiplier = r2 / r1;
        holdings = holdings * multiplier;
  
        output.push({
          date: e.date,
          tranType: "SPLIT",
          qty: 0,
          holdings,
        });
      }
    }
  
    return output;
  };
  