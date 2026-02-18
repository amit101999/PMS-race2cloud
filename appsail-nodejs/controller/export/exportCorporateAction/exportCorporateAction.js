// appsail-nodejs/controller/export/exportCorporateAction/exportCorporateAction.js

const BATCH_SIZE = 200;
const HISTORY_LIMIT = 10;

const exportHistory = [];

export const getCorporateActionHistory = async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit, 10) || 10, 20);
    const list = exportHistory.slice(0, limit);
    return res.json(list);
  } catch (err) {
    console.error("CORPORATE ACTION HISTORY ERROR:", err);
    return res.status(500).json([]);
  }
};

export const exportCorporateAction = async (req, res) => {
  try {
    if (!req.catalystApp) {
      return res.status(500).json({ success: false, message: "Catalyst not initialized" });
    }

    const { fromDate, toDate } = req.query;

    if (!fromDate || !toDate) {
      return res.status(400).json({
        success: false,
        message: "fromDate and toDate are required",
      });
    }

    const fromISO = new Date(fromDate).toISOString().split("T")[0];
    const toISO = new Date(toDate).toISOString().split("T")[0];

    if (fromISO > toISO) {
      return res.status(400).json({
        success: false,
        message: "fromDate must be less than or equal to toDate",
      });
    }

    const catalystApp = req.catalystApp;
    const zcql = catalystApp.zcql();

    /* ---------- BONUS ROWS from Bonus_Record (ExDate between fromDate and toDate) ---------- */
    const bonusRows = [];
    let offset = 0;

    while (true) {
      const rows = await zcql.executeZCQLQuery(`
        SELECT SecurityCode, SecurityName, Ratio1, Ratio2, ExDate, ISIN, EXCHG, SCHEMENAME
        FROM Bonus_Record
        WHERE ExDate >= '${fromISO}' AND ExDate <= '${toISO}'
        ORDER BY ExDate ASC, ROWID ASC
        LIMIT ${BATCH_SIZE} OFFSET ${offset}
      `);

      if (!rows.length) break;
      bonusRows.push(...rows);
      if (rows.length < BATCH_SIZE) break;
      offset += BATCH_SIZE;
    }

    /* ---------- SPLIT ROWS (Issue_Date between fromDate and toDate) ---------- */
    const splitRows = [];
    offset = 0;

    while (true) {
      const rows = await zcql.executeZCQLQuery(`
        SELECT ISIN, Issue_Date, Ratio1, Ratio2, Security_Code, Security_Name
        FROM Split
        WHERE Issue_Date >= '${fromISO}' AND Issue_Date <= '${toISO}'
        ORDER BY Issue_Date ASC, ROWID ASC
        LIMIT ${BATCH_SIZE} OFFSET ${offset}
      `);

      if (!rows.length) break;
      splitRows.push(...rows);
      if (rows.length < BATCH_SIZE) break;
      offset += BATCH_SIZE;
    }
     /* ---------- DIVIDEND ROWS (ExDate between fromDate and toDate) ---------- */
     const dividendRows = [];
     offset = 0;
 
     while (true) {
       const rows = await zcql.executeZCQLQuery(`
         SELECT SecurityCode, Security_Name, ISIN, Rate, ExDate, RecordDate, PaymentDate, Dividend_Type
         FROM Dividend
         WHERE RecordDate>= '${fromISO}' AND RecordDate <= '${toISO}'
         ORDER BY RecordDate ASC, ROWID ASC
         LIMIT ${BATCH_SIZE} OFFSET ${offset}
       `);
 
       if (!rows.length) break;
       dividendRows.push(...rows);
       if (rows.length < BATCH_SIZE) break;
       offset += BATCH_SIZE;
     }

    /* ---------- DEDUPE BONUS BY (ExDate, SecurityCode, SecurityName, ISIN) ---------- */
    const bonusByEvent = {};
    for (const r of bonusRows) {
      const b = r.Bonus_Record || r;
      const exDate = b.ExDate ?? "";
      const securityCode = b.SecurityCode ?? "";
      const securityName = b.SecurityName ?? "";
      const isin = b.ISIN ?? "";
      const key = `${exDate}|${securityCode}|${securityName}|${isin}`;
      if (!bonusByEvent[key]) {
        bonusByEvent[key] = {
          date: exDate,
          isin,
          ratio1: b.Ratio1 ?? "",
          ratio2: b.Ratio2 ?? "",
          type: "bonus",
          securityCode: securityCode || "-",
          securityName: securityName || "-",
        };
      }
    }
    const bonusEvents = Object.values(bonusByEvent);

    /* ---------- NORMALIZE SPLIT ROWS ---------- */
    const splitEvents = [];
    for (const r of splitRows) {
      const s = r.Split || r;
      const securityCode = s.Security_Code ?? "";
      const securityName = s.Security_Name ?? "";
      splitEvents.push({
        date: s.Issue_Date ?? "",
        isin: s.ISIN ?? "-",
        ratio1: s.Ratio1 ?? "",
        ratio2: s.Ratio2 ?? "",
        type: "split",
        securityCode: securityCode || "-",
        securityName: securityName || "-",
      });
    }
    /* ---------- NORMALIZE DIVIDEND ROWS ---------- */    /* ---------- NORMALIZE DIVIDEND ROWS ---------- */
    const dividendEvents = [];
    for (const r of dividendRows) {
      const d = r.Dividend || r;
      dividendEvents.push({
        date: d.RecordDate ?? "",
        isin: d.ISIN ?? "-",
        ratio1: "",
        ratio2: "",
        type: "dividend",
        securityCode: d.SecurityCode ?? "-",
        securityName: d.Security_Name ?? "-",
        rate: d.Rate ?? "",
        paymentDate: d.PaymentDate ?? "",
        dividendType: d.Dividend_Type ?? "",
      });
    }

    /* ---------- SINGLE TABLE: MERGE AND SORT BY DATE ---------- */
    const allRows = [...bonusEvents, ...splitEvents, ...dividendEvents].sort(
      (a, b) => (a.date || "").localeCompare(b.date || "")
    );

    /* ---------- BUILD CSV (single table, headers: Date, ISIN, RATIO1, RATIO2, TYPE, Security_Code, Security_Name) ---------- */
    const csvHeader = "DATE,ISIN,RATIO1,RATIO2,TYPE,SECURITY_CODE,SECURITY_NAME,Rate,PaymentDate,DividendType\n";
    let csv = csvHeader;
    for (const row of allRows) {
      csv += [
        row.date ? row.date : "",
        row.isin ? row.isin : "",
        row.ratio1 ? row.ratio1 : "",
        row.ratio2 ? row.ratio2 : "",
        row.type ? row.type : "",
        row.securityCode ? row.securityCode : "",
        row.securityName ? row.securityName : "",
        row.rate ? row.rate : "",
        row.paymentDate ? row.paymentDate : "",
        row.dividendType ? row.dividendType : "",
      ]
        .map((v) => `"${String(v).replace(/"/g, '""')}"`)
        .join(",") + "\n";
    }

    /* ---------- STORE IN HISTORY (last 10) ---------- */
    exportHistory.unshift({
      fromDate: fromISO,
      toDate: toISO,
      requestedAt: new Date().toISOString(),
    });
    if (exportHistory.length > HISTORY_LIMIT) exportHistory.pop();

    /* ---------- RETURN CSV DIRECTLY (same-origin download, no signed URL) ---------- */
    const fileName = `corporate-action-export-${fromISO}-${toISO}.csv`;
    res.setHeader("Content-Type", "text/csv");
    res.setHeader("Content-Disposition", `attachment; filename="${fileName}"`);
    return res.send(csv);
  } catch (err) {
    console.error("EXPORT CORPORATE ACTION ERROR:", err);
    return res.status(500).json({
      success: false,
      message: err.message || "Internal server error",
    });
  }
};