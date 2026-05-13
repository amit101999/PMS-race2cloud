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

    /* Page (1-indexed) takes precedence over cursor when present. */
    const pageParam =
      req.query.page != null
        ? parseInt(String(req.query.page), 10)
        : null;
    const hasPage =
      pageParam != null && !Number.isNaN(pageParam) && pageParam >= 1;

    /* Cursor kept as a legacy fallback for callers without `page`. */
    const lastDate = (req.query.lastDate || "").trim() || null;
    const lastPriority =
      req.query.lastPriority != null
        ? parseInt(String(req.query.lastPriority), 10)
        : null;
    const lastRowId =
      req.query.lastRowId != null ? String(req.query.lastRowId) : null;

    const direction = req.query.direction === "prev" ? "prev" : "next";

    const hasCursor =
      !hasPage &&
      lastDate &&
      lastPriority != null &&
      !Number.isNaN(lastPriority) &&
      lastRowId != null;

    const accountCode = (req.query.accountCode || "").trim();
    const rawAsOnDate = (req.query.asOnDate || "").trim();
    const securityNameFilter = (req.query.securityName || "").trim();
    const isinFilter = (req.query.isin || "").trim();

    if (!accountCode) {
      return res.status(200).json({
        data: [],
        nextCursor: null,
        prevCursor: null,
        hasNext: false,
        hasPrev: false,
        totalCount: 0,
        totalPages: 1,
        page: 1,
        pageSize: limit,
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
    /*
     * ISIN filter is preferred (uniquely identifies the security); when sent we also
     * skip the slower name-based filter. `securityName` is kept for back-compat with
     * any caller that might still pass it.
     */
    if (isinFilter) {
      filtered = rawHoldings.filter(
        (h) => String(h.ISIN || "").trim() === isinFilter,
      );
    } else if (securityNameFilter) {
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

    /*
     * Display order: newest TRANDATE first.
     * Tie-breakers (deterministic, page-stable):
     *   1. date DESC (primary)
     *   2. executionPriority ASC (same day: corp-actions like SPLIT/BONUS/DIV+ before regular trades)
     *   3. isin ASC (group same-day rows of the same security together)
     *   4. rowId ASC (final tiebreaker so two requests return the same order)
     */
    transactions.sort((a, b) => {
      const dA = a.date || "";
      const dB = b.date || "";
      if (dA !== dB) return dA < dB ? 1 : -1;
      if (a.executionPriority !== b.executionPriority) {
        return a.executionPriority - b.executionPriority;
      }
      const isinA = a.isin || "";
      const isinB = b.isin || "";
      if (isinA !== isinB) return isinA.localeCompare(isinB);
      return String(a.rowId).localeCompare(String(b.rowId));
    });

    const totalCount = transactions.length;
    const totalPages = Math.max(1, Math.ceil(totalCount / limit));

    let startIdx = 0;
    let pageNumber = 1;

    if (hasPage) {
      pageNumber = Math.min(Math.max(1, pageParam), totalPages);
      startIdx = (pageNumber - 1) * limit;
    } else if (hasCursor) {
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
      pageNumber = Math.floor(startIdx / limit) + 1;
    }

    const paginated = transactions.slice(startIdx, startIdx + limit);

    const hasNextPage = startIdx + limit < totalCount;
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
      totalCount,
      totalPages,
      page: pageNumber,
      pageSize: limit,
    });
  } catch (err) {
    console.error("[getPaginatedTransactions]", err);
    return res.status(500).json({
      message: "Failed to fetch transactions",
      error: err.message,
    });
  }
};

/**
 * Distinct securities (ISIN + name) for an account, optionally cut off by `asOnDate`.
 * Response shape: `{ data: Array<{ isin: string, securityName: string }> }`
 *
 * Search query (`?search=...`) matches against EITHER ISIN or Security_Name (case-insensitive
 * substring) so users can find a security by typing the company name OR the ISIN.
 */
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

    const items = [];
    for (const isin of isinSet) {
      const securityName = (metaByIsin[isin]?.securityName || "").trim();
      items.push({ isin, securityName });
    }

    items.sort((a, b) => {
      const nA = a.securityName || "";
      const nB = b.securityName || "";
      if (nA && nB) return nA.localeCompare(nB);
      if (nA) return -1;
      if (nB) return 1;
      return a.isin.localeCompare(b.isin);
    });

    const q = rawSearch.toLowerCase();
    const filtered = q
      ? items.filter(
          (it) =>
            (it.securityName || "").toLowerCase().includes(q) ||
            it.isin.toLowerCase().includes(q),
        )
      : items;

    return res.status(200).json({ data: filtered });
  } catch (err) {
    console.error("[getSecurityNameOptions]", err);
    return res.status(500).json({
      message: "Failed to fetch security names",
      error: err.message,
    });
  }
};
