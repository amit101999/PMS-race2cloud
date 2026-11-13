import { useMemo, useState } from "react";
import TransactionPage from "./TransactionPage";

const API_URL = process.env.REACT_APP_API_URL;
const WORKERS = 6;
const REVIEW_STORAGE_KEY = "negativeHoldingsReview";

// Only shares still below zero on the as-on date are listed. "Covered later"
// shares are positive again, and "Check by hand" shares have no reliable
// current value, so both are left out.
const STILL_NEGATIVE = "Still negative";

// Shortfalls smaller than this are storage noise, not trades: the database keeps
// 4 decimals and cut 40,191.97999… down to 40,191.9799, so a full sell of
// 40,191.98 looked 0.0001 short. Funds record units to 3 decimals.
const MIN_SHORTFALL = 0.001;

function todayLocal() {
  const now = new Date();
  const month = String(now.getMonth() + 1).padStart(2, "0");
  const day = String(now.getDate()).padStart(2, "0");
  return `${now.getFullYear()}-${month}-${day}`;
}

function formatQuantity(value) {
  if (value === null || value === undefined) return "–";
  const snapped = Math.abs(value) < 1e-6 ? 0 : value;
  return snapped.toLocaleString("en-IN", { maximumFractionDigits: 6 });
}

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`The server replied with ${res.status}`);
  return res.json();
}

function loadReviews() {
  try {
    return JSON.parse(localStorage.getItem(REVIEW_STORAGE_KEY)) || {};
  } catch {
    return {};
  }
}

function saveReviews(reviews) {
  try {
    localStorage.setItem(REVIEW_STORAGE_KEY, JSON.stringify(reviews));
  } catch {
    // Storage can be blocked. The notes still work until the page is closed.
  }
}

// A text cell starting with one of these runs as a formula in Excel.
function csvCell(value) {
  let text = String(value ?? "");
  if (typeof value === "string" && /^[=+\-@\t\r]/.test(text)) text = `'${text}`;
  return `"${text.replace(/"/g, '""')}"`;
}

function downloadCsv(fileName, rows) {
  const csv = rows.map((row) => row.map(csvCell).join(",")).join("\n");
  const link = document.createElement("a");
  link.href = URL.createObjectURL(new Blob([csv], { type: "text/csv" }));
  link.download = fileName;
  link.click();
  URL.revokeObjectURL(link.href);
}

const SHARE_CSV_HEADER = [
  "Account",
  "Security Name",
  "ISIN",
  "Negative holding",
  "Review status",
  "Note",
];

function shareCsvRow(accountCode, share, review = {}) {
  return [
    accountCode,
    share.securityName,
    share.isin,
    share.realHolding,
    review.status,
    review.note,
  ];
}

function NegativeHoldingsPage() {
  const [asOnDate, setAsOnDate] = useState(todayLocal);
  const [scannedAsOn, setScannedAsOn] = useState("");
  // Account code → its shares that are negative on the as-on date. An account with none has [].
  const [results, setResults] = useState({});
  const [failedAccounts, setFailedAccounts] = useState([]);
  const [scanning, setScanning] = useState(false);
  const [progress, setProgress] = useState({ done: 0, total: 0 });
  const [scanError, setScanError] = useState("");

  const [search, setSearch] = useState("");
  const [show, setShow] = useState("all");
  const [sortBy, setSortBy] = useState("negative");

  const [selectedAccount, setSelectedAccount] = useState("");
  const [openShare, setOpenShare] = useState(null);
  const [reviews, setReviews] = useState(loadReviews);

  /** Checks the accounts, 6 at a time. Every failure is kept, so it can't look clean. */
  const scanAccounts = async (accountCodes, date) => {
    const queue = [...accountCodes];
    const found = {};
    const failed = [];
    let done = 0;
    setProgress({ done: 0, total: accountCodes.length });

    const worker = async () => {
      while (queue.length > 0) {
        const accountCode = queue.shift();
        const params = new URLSearchParams({ accountCode, asOnDate: date });
        try {
          const json = await fetchJson(`${API_URL}/negative-holdings?${params}`);
          found[accountCode] = json.shares.filter(
            (share) => share.status === STILL_NEGATIVE && share.realHolding <= -MIN_SHORTFALL,
          );
        } catch {
          failed.push(accountCode);
        }
        done += 1;
        setProgress({ done, total: accountCodes.length });
      }
    };
    await Promise.all(Array.from({ length: WORKERS }, worker));

    setResults((previous) => ({ ...previous, ...found }));
    setFailedAccounts(failed.sort());
  };

  const handleScan = async () => {
    if (!API_URL) {
      setScanError("The function URL is not set. Set REACT_APP_API_URL and build the page again.");
      return;
    }
    setScanning(true);
    setScanError("");
    setResults({});
    setFailedAccounts([]);
    setSelectedAccount("");
    setScannedAsOn(asOnDate);
    try {
      const { data } = await fetchJson(`${API_URL}/accounts`);
      // With text in the search box, only the accounts whose code contains it are checked.
      const query = search.trim().toLowerCase();
      const accountCodes = query ? data.filter((code) => code.toLowerCase().includes(query)) : data;
      if (accountCodes.length === 0) {
        setScanError(`No account matches "${search.trim()}".`);
        return;
      }
      await scanAccounts(accountCodes, asOnDate);
    } catch (err) {
      setScanError(`Could not load the account list. ${err.message}.`);
    } finally {
      setScanning(false);
    }
  };

  const handleRetry = async () => {
    setScanning(true);
    await scanAccounts(failedAccounts, scannedAsOn);
    setScanning(false);
  };

  const updateReview = (accountCode, change) => {
    const updated = { ...reviews, [accountCode]: { ...reviews[accountCode], ...change } };
    setReviews(updated);
    saveReviews(updated);
  };

  const flaggedAccounts = useMemo(() => {
    return Object.entries(results)
      .filter(([, shares]) => shares.length > 0)
      .map(([accountCode, shares]) => ({ accountCode, shares }));
  }, [results]);

  // The main table shows one row per share.
  const visibleShares = useMemo(() => {
    const query = search.trim().toLowerCase();
    return flaggedAccounts
      .filter((a) => !query || a.accountCode.toLowerCase().includes(query))
      .filter((a) => show !== "todo" || !reviews[a.accountCode]?.status)
      .flatMap((account) =>
        account.shares.map((share) => ({ accountCode: account.accountCode, ...share })),
      )
      .sort((a, b) =>
        sortBy === "account"
          ? a.accountCode.localeCompare(b.accountCode) ||
            (a.securityName || a.isin).localeCompare(b.securityName || b.isin)
          : a.realHolding - b.realHolding,
      );
  }, [flaggedAccounts, search, show, sortBy, reviews]);

  const scannedCount = Object.keys(results).length;
  const hasScanned = scannedCount > 0 || failedAccounts.length > 0;
  const sharesAffected = flaggedAccounts.reduce((sum, a) => sum + a.shares.length, 0);
  const reviewedCount = flaggedAccounts.filter((a) => reviews[a.accountCode]?.status).length;
  const selected = flaggedAccounts.find((a) => a.accountCode === selectedAccount);
  const selectedReview = reviews[selectedAccount] || {};

  // Both exports follow the table: same rows, filters and order.
  const exportClientList = () => {
    downloadCsv("negative_holdings_clients.csv", [
      ["Account", "Security Name", "ISIN", "Negative holding"],
      ...visibleShares.map((share) => [
        share.accountCode,
        share.securityName,
        share.isin,
        share.realHolding,
      ]),
    ]);
  };

  const exportAllShortfalls = () => {
    downloadCsv("negative_holdings_all.csv", [
      SHARE_CSV_HEADER,
      ...visibleShares.map((share) =>
        shareCsvRow(share.accountCode, share, reviews[share.accountCode]),
      ),
    ]);
  };

  const exportThisClient = () => {
    downloadCsv(`negative_holdings_${selected.accountCode}.csv`, [
      SHARE_CSV_HEADER,
      ...selected.shares.map((share) => shareCsvRow(selected.accountCode, share, selectedReview)),
    ]);
  };

  const scanLabel = search.trim() ? "Scan matching accounts" : "Scan all accounts";

  return (
    <>
      <header className="nh-header">
        <h1>Negative Holdings</h1>
        <p>Clients who sold more of a share than they held.</p>
      </header>

      <main className="nh-main">
        <div className="nh-card nh-bar">
          <div>
            <label htmlFor="asOn">As on</label>
            <input
              id="asOn"
              type="date"
              value={asOnDate}
              onChange={(e) => setAsOnDate(e.target.value)}
            />
          </div>
          <button className="nh-primary" onClick={handleScan} disabled={scanning || !asOnDate}>
            {scanning ? "Scanning..." : scanLabel}
          </button>
          <div>
            <label htmlFor="search">Search account</label>
            <input
              id="search"
              value={search}
              placeholder="e.g. HCAYAN"
              onChange={(e) => setSearch(e.target.value)}
            />
          </div>
          <div>
            <label htmlFor="show">Show</label>
            <select id="show" value={show} onChange={(e) => setShow(e.target.value)}>
              <option value="all">All negative holdings</option>
              <option value="todo">Not yet reviewed</option>
            </select>
          </div>
          <div>
            <label htmlFor="sort">Sort by</label>
            <select id="sort" value={sortBy} onChange={(e) => setSortBy(e.target.value)}>
              <option value="negative">Negative holding</option>
              <option value="account">Account</option>
            </select>
          </div>
          <button onClick={exportClientList} disabled={visibleShares.length === 0}>
            Export client list (CSV)
          </button>
          <button onClick={exportAllShortfalls} disabled={visibleShares.length === 0}>
            Export all shortfalls (CSV)
          </button>
        </div>

        {scanError && <div className="nh-note nh-note--bad">{scanError}</div>}

        {scanning && progress.total > 0 && (
          <>
            <p className="nh-progress-text">
              Checked {progress.done} of {progress.total} accounts
            </p>
            <div className="nh-progress">
              <i style={{ width: `${(progress.done / progress.total) * 100}%` }} />
            </div>
          </>
        )}

        {failedAccounts.length > 0 && !scanning && (
          <div className="nh-note nh-note--bad nh-note--row">
            <span>
              Accounts that failed and were not checked ({failedAccounts.length}):{" "}
              {failedAccounts.slice(0, 10).join(", ")}
              {failedAccounts.length > 10 ? " …" : ""}
            </span>
            <button onClick={handleRetry}>Retry</button>
          </div>
        )}

        <div className="nh-kpis">
          <div className="nh-card nh-kpi">
            <small>Accounts scanned</small>
            <b>{scannedCount}</b>
            <span>{scannedAsOn ? `as on ${scannedAsOn}` : ""}</span>
          </div>
          <div className="nh-card nh-kpi">
            <small>With a negative holding</small>
            <b>{flaggedAccounts.length}</b>
            <span>accounts</span>
          </div>
          <div className="nh-card nh-kpi">
            <small>Shares affected</small>
            <b>{sharesAffected}</b>
          </div>
          <div className="nh-card nh-kpi">
            <small>Reviewed</small>
            <b>{reviewedCount}</b>
            <span>of {flaggedAccounts.length} accounts</span>
          </div>
          <div className="nh-card nh-kpi">
            <small>Failed</small>
            <b className={failedAccounts.length > 0 ? "nh-negative" : ""}>{failedAccounts.length}</b>
            <span>accounts</span>
          </div>
        </div>

        <div className="nh-card">
          <div className="data-table-wrap nh-table-scroll">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Account</th>
                  <th className="wrap">Security Name</th>
                  <th>ISIN</th>
                  <th className="num">Negative holding</th>
                </tr>
              </thead>
              <tbody>
                {visibleShares.map((share) => (
                  <tr
                    key={`${share.accountCode}|${share.isin}`}
                    className={`is-clickable ${selectedAccount === share.accountCode ? "is-active" : ""}`}
                    onClick={() => setSelectedAccount(share.accountCode)}
                  >
                    <td>{share.accountCode}</td>
                    <td className="wrap">{share.securityName || "–"}</td>
                    <td>{share.isin || "–"}</td>
                    <td className="num nh-negative">{formatQuantity(share.realHolding)}</td>
                  </tr>
                ))}
                {visibleShares.length === 0 && (
                  <tr>
                    <td colSpan={4} className="nh-empty">
                      {scanning
                        ? "Scanning..."
                        : !hasScanned
                          ? `Click "${scanLabel}" to start.`
                          : flaggedAccounts.length === 0 && failedAccounts.length === 0
                            ? "No negative holdings found."
                            : "Nothing matches these filters."}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>

        {selected && (
          <div className="nh-card">
            <div className="nh-detail-head">
              <h3>
                {selected.accountCode} – {selected.shares.length} shares
              </h3>
              <select
                aria-label="Review status"
                value={selectedReview.status || ""}
                onChange={(e) => updateReview(selected.accountCode, { status: e.target.value })}
              >
                <option value="">Not reviewed</option>
                <option>In progress</option>
                <option>Fixed</option>
                <option>Not an issue</option>
              </select>
              <button onClick={exportThisClient}>Export this client (CSV)</button>
            </div>

            <textarea
              rows={2}
              placeholder="Notes on this client… (saved in this browser only)"
              value={selectedReview.note || ""}
              onChange={(e) => updateReview(selected.accountCode, { note: e.target.value })}
            />

            <div className="data-table-wrap nh-detail-table">
              <table className="data-table">
                <thead>
                  <tr>
                    <th className="wrap">Security</th>
                    <th>ISIN</th>
                    <th className="num">Negative holding</th>
                  </tr>
                </thead>
                <tbody>
                  {selected.shares.map((share) => (
                    <tr
                      key={share.isin}
                      className="is-clickable"
                      onClick={() =>
                        setOpenShare({
                          isin: share.isin,
                          stockName: share.securityName || share.isin,
                        })
                      }
                    >
                      <td className="wrap">{share.securityName || "–"}</td>
                      <td>{share.isin || "–"}</td>
                      <td className="num nh-negative">{formatQuantity(share.realHolding)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </main>

      {openShare && selected && (
        <TransactionPage
          stock={openShare}
          accountCode={selected.accountCode}
          asOnDate={scannedAsOn}
          onClose={() => setOpenShare(null)}
        />
      )}
    </>
  );
}

export default NegativeHoldingsPage;
