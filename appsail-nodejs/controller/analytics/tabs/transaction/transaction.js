import {
  fetchHoldingsRowsPaged,
  fetchSecurityListByIsins,
  holdingsTypePriority,
} from "../../../../util/analytics/holdingsFromTable.js";

/** Cash credits add (|amount| − STT); debits subtract (|amount| + STT). */
export const applyCashEffect = (balance, tranType, amount, stt = 0) => {
  const amt = Math.abs(Number(amount)) || 0;
  const sttVal = Math.abs(Number(stt)) || 0;

  if (
    tranType === "CS+" ||
    tranType === "SL+" ||
    tranType === "DIO" ||
    tranType === "DI0" ||
    tranType === "CSI" ||
    tranType === "DIS" ||
    tranType === "IN1" ||
    tranType === "OI1" ||
    tranType === "SQS" ||
    tranType === "DI1" ||
    tranType === "IN+" ||
    tranType === "DIV+"
  ) {
    return balance + amt - sttVal;
  }

  if (
    tranType === "BY-" ||
    tranType === "MGF" ||
    tranType === "TDO" ||
    tranType === "TDI" ||
    tranType === "E22" ||
    tranType === "E01" ||
    tranType === "CUS" ||
    tranType === "E23" ||
    tranType === "CS-" ||
    tranType === "MGE" ||
    tranType === "E10" ||
    tranType === "PRF" ||
    tranType === "NF-" ||
    tranType === "SQB"
  ) {
    return balance - amt - sttVal;
  }

  return balance;
};

export const getPaginatedTransactions = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app missing" });
    }

    const zcql = app.zcql();
    const limit = Math.max(parseInt(req.query.limit, 10) || 20, 1);

    const lastDate = (req.query.lastDate || "").trim() || null;
    const lastPriority =
      req.query.lastPriority != null
        ? parseInt(String(req.query.lastPriority), 10)
        : null;
    const lastRowId =
      req.query.lastRowId != null ? String(req.query.lastRowId) : null;

    const direction = req.query.direction === "prev" ? "prev" : "next";

    const hasCursor =
      lastDate &&
      lastPriority != null &&
      !Number.isNaN(lastPriority) &&
      lastRowId != null;

    const accountCode = (req.query.accountCode || "").trim();
    const rawAsOnDate = (req.query.asOnDate || "").trim();
    const securityNameFilter = (req.query.securityName || "").trim();

    if (!accountCode) {
      return res.status(200).json({
        data: [],
        nextCursor: null,
        prevCursor: null,
        hasNext: false,
        hasPrev: false,
      });
    }

    const normalizeDate = (d) => {
      if (!d) return null;
      if (/^\d{4}-\d{2}-\d{2}$/.test(d)) return d;
      if (/^\d{2}\/\d{2}\/\d{4}$/.test(d)) {
        const [dd, mm, yyyy] = d.split("/");
        return `${yyyy}-${mm}-${dd}`;
      }
      return null;
    };

    const asOnDate = normalizeDate(rawAsOnDate);

    let rawHoldings = await fetchHoldingsRowsPaged(
      zcql,
      accountCode,
      asOnDate,
      "",
    );
    const isinSetFromRows = new Set();
    for (const h of rawHoldings) {
      const isin = String(h.ISIN || "").trim();
      if (isin) isinSetFromRows.add(isin);
    }
    const metaByIsin = await fetchSecurityListByIsins(
      zcql,
      [...isinSetFromRows],
    );

    let filtered = rawHoldings;
    if (securityNameFilter) {
      filtered = rawHoldings.filter((h) => {
        const isin = String(h.ISIN || "").trim();
        const nm = (
          metaByIsin[isin]?.securityName ||
          ""
        ).trim();
        return nm === securityNameFilter;
      });
    }

    const transactions = filtered.map((h) => {
      const isin = String(h.ISIN || "").trim();
      const meta = metaByIsin[isin] || {};
      const trd = String(h.TRANSACTION_DATE || "").trim().slice(0, 10);
      const setD = String(h.SETTLEMENT_DATE || "").trim().slice(0, 10);
      const primaryDate = trd || setD || "";
      return {
        rowId: `H-${h.ROWID}`,
        date: primaryDate,
        trandate: trd || primaryDate || null,
        setdate: setD || trd || null,
        executionPriority: holdingsTypePriority(h.TYPE),
        type: String(h.TYPE || "").trim(),
        securityName: meta.securityName || "—",
        securityCode: meta.securityCode || "",
        isin: isin || null,
        quantity: Number(h.QUANTITY) || 0,
        price: Number(h.PRICE) || 0,
        totalAmount: Number(h.TOTAL_AMOUNT) || 0,
        stt: 0,
      };
    });

    let startIdx = 0;

    if (hasCursor) {
      const cursorIdx = transactions.findIndex(
        (t) =>
          t.date === lastDate &&
          t.executionPriority === lastPriority &&
          t.rowId === lastRowId,
      );

      if (cursorIdx !== -1) {
        if (direction === "next") {
          startIdx = cursorIdx + 1;
        } else {
          startIdx = Math.max(0, cursorIdx - limit);
        }
      }
    }

    const paginated = transactions.slice(startIdx, startIdx + limit);

    const hasNextPage = startIdx + limit < transactions.length;
    const hasPrevPage = startIdx > 0;

    const firstRow = paginated[0];
    const lastRow = paginated[paginated.length - 1];

    const prevCursorObj = firstRow
      ? {
          lastDate: firstRow.date,
          lastPriority: firstRow.executionPriority,
          lastRowId: firstRow.rowId,
        }
      : null;

    const nextCursorObj = lastRow
      ? {
          lastDate: lastRow.date,
          lastPriority: lastRow.executionPriority,
          lastRowId: lastRow.rowId,
        }
      : null;

    return res.status(200).json({
      data: paginated,
      nextCursor: nextCursorObj,
      prevCursor: prevCursorObj,
      hasNext: hasNextPage,
      hasPrev: hasPrevPage,
    });
  } catch (err) {
    console.error("[getPaginatedTransactions]", err);
    return res.status(500).json({
      message: "Failed to fetch transactions",
      error: err.message,
    });
  }
};

/** Security names appearing in Holdings for this account (optional `asOnDate` cutoff). */
export const getSecurityNameOptions = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app missing" });
    }

    const zcql = app.zcql();

    const rawSearch = (req.query.search || "").trim();
    const accountCode = (req.query.accountCode || "").trim();
    const rawAsOn = (req.query.asOnDate || "").trim();
    const asOnDate =
      /^\d{4}-\d{2}-\d{2}$/.test(rawAsOn)
        ? rawAsOn
        : /^\d{2}\/\d{2}\/\d{4}$/.test(rawAsOn)
          ? (() => {
              const [dd, mm, yyyy] = rawAsOn.split("/");
              return `${yyyy}-${mm}-${dd}`;
            })()
          : null;

    if (!accountCode) {
      return res.status(200).json({ data: [] });
    }

    const rawHoldings = await fetchHoldingsRowsPaged(
      zcql,
      accountCode,
      asOnDate,
      "",
    );
    const isinSet = new Set();
    for (const h of rawHoldings) {
      const isin = String(h.ISIN || "").trim();
      if (isin) isinSet.add(isin);
    }
    const metaByIsin = await fetchSecurityListByIsins(zcql, [...isinSet]);

    const securityNameSet = new Set();
    for (const isin of isinSet) {
      const nm = (metaByIsin[isin]?.securityName || "").trim();
      if (nm) securityNameSet.add(nm);
    }

    let names = Array.from(securityNameSet).sort((a, b) =>
      a.localeCompare(b),
    );
    const q = rawSearch.toLowerCase();
    if (q) {
      names = names.filter((n) => n.toLowerCase().includes(q));
    }

    return res.status(200).json({
      data: names,
    });
  } catch (err) {
    console.error("[getSecurityNameOptions]", err);
    return res.status(500).json({
      message: "Failed to fetch security names",
      error: err.message,
    });
  }
};
