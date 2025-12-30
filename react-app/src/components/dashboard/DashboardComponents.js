import React, { useState, useEffect } from "react";
import {
  Button,
  Card,
  StatCard,
  Badge,
  TextInput,
  SelectInput,
  Table,
  Sidebar,
  Topbar,
  PageLayout,
  Pagination,
  TransactionTypeCard,
  ExchangeCard,
} from "../common/CommonComponents";
import {BASE_URL} from "../../constant";
/* ===========================
   SUMMARY CARDS ROW
   =========================== */

export function SummaryCardsRow() {
  const rowStyle = {
    display: "grid",
    gridTemplateColumns: "repeat(4, minmax(0, 1fr))",
    gap: 16,
    marginTop: 16,
  };

  return (
    <div style={rowStyle}>
      <StatCard
        label="Total Trades"
        value="2,65,814"
        subLabel="All time trades"
        style={{ background: "linear-gradient(90deg,#5b3fe2,#8a5cff)" }}
      />
      <StatCard
        label="Total Value"
        value="₹0"
        subLabel="Avg. PnL"
        style={{ background: "linear-gradient(90deg,#ff4b7d,#ff8a9f)" }}
      />
      <StatCard
        label="Buy Trades"
        value="1,250"
        subLabel="Qty of all buys"
        style={{ background: "linear-gradient(90deg,#00c6ff,#0072ff)" }}
      />
      <StatCard
        label="Sell Trades"
        value="880"
        subLabel="Qty of all sells"
        style={{ background: "linear-gradient(90deg,#ffb347,#ffcc33)" }}
      />
    </div>
  );
}

/* ===========================
   CHARTS ROW (STATIC SAMPLE CHARTS)
   =========================== */

export function ChartsRow() {
  const rowStyle = {
    display: "grid",
    gridTemplateColumns: "2fr 2fr 1.5fr",
    gap: 16,
    marginTop: 24,
  };

  const titleStyle = {
    fontSize: 13,
    fontWeight: 600,
    color: "#111827",
    marginBottom: 8,
  };

  const chartWrapper = {
    height: 200,
  };

  const legendItem = (color) => ({
    display: "flex",
    alignItems: "center",
    gap: 6,
    fontSize: 11,
    color: "#4b5563",
    marginTop: 4,
  });

  const colorBox = (color) => ({
    width: 10,
    height: 10,
    borderRadius: 2,
    backgroundColor: color,
  });

  return (
    <div style={rowStyle}>
      {/* Line chart */}
      <Card>
        <div style={titleStyle}>Daily Trade Volume (Last 30 Days)</div>
        <div style={chartWrapper}>
          <svg viewBox="0 0 100 40" style={{ width: "100%", height: "70%" }}>
            <defs>
              <linearGradient id="lineFill" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#c7d2fe" stopOpacity="0.7" />
                <stop offset="100%" stopColor="#e5e7eb" stopOpacity="0" />
              </linearGradient>
            </defs>
            <polyline
              fill="none"
              stroke="#e5e7eb"
              strokeWidth="0.5"
              points="0,35 100,35"
            />
            <polyline
              fill="none"
              stroke="#e5e7eb"
              strokeWidth="0.5"
              points="0,25 100,25"
            />
            <polyline
              fill="none"
              stroke="#4f46e5"
              strokeWidth="1.5"
              strokeLinejoin="round"
              strokeLinecap="round"
              points="0,30 8,18 16,22 24,12 32,20 40,10 48,15 56,9 64,18 72,14 80,20 88,12 96,16"
            />
            <polyline
              fill="url(#lineFill)"
              stroke="none"
              points="0,40 0,30 8,18 16,22 24,12 32,20 40,10 48,15 56,9 64,18 72,14 80,20 88,12 96,16 100,20 100,40"
            />
          </svg>
          <div style={{ marginTop: 4, fontSize: 11, color: "#6b7280" }}>
            Sample volume trend (BUY vs SELL)
          </div>
        </div>
      </Card>

      {/* Bar chart */}
      <Card>
        <div style={titleStyle}>Top 10 Stocks by Trade Value</div>
        <div style={chartWrapper}>
          <svg viewBox="0 0 100 40" style={{ width: "100%", height: "70%" }}>
            <line
              x1="0"
              y1="36"
              x2="100"
              y2="36"
              stroke="#e5e7eb"
              strokeWidth="0.5"
            />
            {[
              { x: 6, h: 26 },
              { x: 16, h: 32 },
              { x: 26, h: 20 },
              { x: 36, h: 30 },
              { x: 46, h: 18 },
              { x: 56, h: 24 },
              { x: 66, h: 16 },
              { x: 76, h: 22 },
              { x: 86, h: 14 },
              { x: 96, h: 19 },
            ].map((bar, idx) => (
              <rect
                key={idx}
                x={bar.x - 2}
                y={36 - bar.h}
                width="4"
                height={bar.h}
                fill="#6366f1"
                rx="1"
              />
            ))}
          </svg>
          <div style={legendItem("#6366f1")}>
            <span style={colorBox("#6366f1")} />
            <span>Trade value (₹) per stock</span>
          </div>
        </div>
      </Card>

      {/* Pie chart */}
      <Card>
        <div style={titleStyle}>Exchange Distribution</div>
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            marginTop: 8,
          }}
        >
          <svg
            viewBox="0 0 32 32"
            style={{ width: 120, height: 120, transform: "rotate(-90deg)" }}
          >
            <circle
              r="16"
              cx="16"
              cy="16"
              fill="transparent"
              stroke="#e5e7eb"
              strokeWidth="8"
            />
            <circle
              r="16"
              cx="16"
              cy="16"
              fill="transparent"
              stroke="#4f46e5"
              strokeWidth="8"
              strokeDasharray="60 40"
            />
            <circle
              r="16"
              cx="16"
              cy="16"
              fill="transparent"
              stroke="#f97316"
              strokeWidth="8"
              strokeDasharray="25 75"
              strokeDashoffset="-60"
            />
            <circle
              r="16"
              cx="16"
              cy="16"
              fill="transparent"
              stroke="#8b5cf6"
              strokeWidth="8"
              strokeDasharray="15 85"
              strokeDashoffset="-85"
            />
          </svg>

          <div style={{ fontSize: 11, color: "#4b5563" }}>
            <div style={legendItem("#4f46e5")}>
              <span style={colorBox("#4f46e5")} />
              <span>NSE · 53%</span>
            </div>
            <div style={legendItem("#f97316")}>
              <span style={colorBox("#f97316")} />
              <span>BSE · 37%</span>
            </div>
            <div style={legendItem("#8b5cf6")}>
              <span style={colorBox("#8b5cf6")} />
              <span>MCX · 10%</span>
            </div>
          </div>
        </div>
      </Card>
    </div>
  );
}

/* ===========================
   FILTER BAR
   =========================== */

export function FiltersBar() {
  const securityOptions = [
    { value: "all", label: "All Securities" },
    { value: "eq", label: "Equity" },
    { value: "fo", label: "F&O" },
  ];

  const exchangeOptions = [
    { value: "all", label: "All Exchanges" },
    { value: "nse", label: "NSE" },
    { value: "bse", label: "BSE" },
  ];

  const typeOptions = [
    { value: "all", label: "All Types" },
    { value: "buy", label: "Buy" },
    { value: "sell", label: "Sell" },
  ];

  const titleRow = {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    marginBottom: 12,
  };

  const grid = {
    display: "grid",
    gridTemplateColumns: "repeat(5, minmax(0, 1fr))",
    gap: 16,
    fontSize: 12,
  };

  const twoCol = {
    display: "grid",
    gridTemplateColumns: "repeat(2, minmax(0, 1fr))",
    gap: 12,
  };

  return (
    <Card style={{ marginTop: 24 }}>
      <div style={titleRow}>
        <h2 style={{ fontSize: 14, fontWeight: 600, color: "#111827" }}>
          Filters
        </h2>
      </div>
      <div style={grid}>
        <SelectInput
          label="Search Security"
          options={securityOptions}
          defaultValue="all"
        />
        <SelectInput
          label="Exchange"
          options={exchangeOptions}
          defaultValue="all"
        />
        <SelectInput
          label="Transaction Type"
          options={typeOptions}
          defaultValue="all"
        />
        <TextInput label="Client ID" placeholder="All Clients" />
        <div style={twoCol}>
          <TextInput label="Start Date" placeholder="dd/mm/yyyy" />
          <TextInput label="End Date" placeholder="dd/mm/yyyy" />
        </div>
      </div>
    </Card>
  );
}

/* ===========================
   RECENT TRADES TABLE
   =========================== */

const tradesColumns = [
  { key: "ROWID", header: "Row ID" },
  { key: "CREATORID", header: "Creator ID" },
  { key: "CREATEDTIME", header: "Created Time" },
  { key: "MODIFIEDTIME", header: "Modified Time" },
  { key: "WS_client_id", header: "WS Client ID" },
  { key: "WS_Account_code", header: "WS Account Code" },
  { key: "TRANDATE", header: "Transaction Date" },
  { key: "SETDATE", header: "Set Date" },
  {
    key: "Tran_Type",
    header: "Transaction Type",
    render: (value) => {
      if (!value) return <TransactionTypeCard>-</TransactionTypeCard>;
      const upperValue = value.toUpperCase();
      const displayValue = 
        upperValue === "BY-" || upperValue === "BUY" || upperValue.includes("BUY") 
          ? "BY-" 
          : upperValue === "SL+" || upperValue === "SELL" || upperValue.includes("SELL") 
          ? "SL+" 
          : value;
      return <TransactionTypeCard>{displayValue}</TransactionTypeCard>;
    },
  },
  { key: "Tran_Desc", header: "Transaction Description" },
  { key: "Security_Type", header: "Security Type" },
  { key: "Security_Type_Description", header: "Security Type Description" },
  { key: "DETAILTYPENAME", header: "Detail Type Name" },
  { key: "ISIN", header: "ISIN" },
  { key: "Security_code", header: "Security Code" },
  { key: "Security_Name", header: "Security Name" },
  {
    key: "EXCHG",
    header: "Exchange",
    render: (value) => <ExchangeCard exchange={value} />,
  },
  { key: "BROKERCODE", header: "Broker Code" },
  { key: "Depositoy_Registrar", header: "Depository Registrar" },
  { key: "DPID_AMC", header: "DP ID AMC" },
  { key: "Dp_Client_id_Folio", header: "DP Client ID Folio" },
  { key: "BANKCODE", header: "Bank Code" },
  { key: "BANKACID", header: "Bank Account ID" },
  { key: "QTY", header: "Quantity" },
  { key: "RATE", header: "Rate" },
  { key: "BROKERAGE", header: "Brokerage" },
  { key: "SERVICETAX", header: "Service Tax" },
  { key: "NETRATE", header: "Net Rate" },
  { key: "Net_Amount", header: "Net Amount" },
  { key: "STT", header: "STT" },
  { key: "TRFDATE", header: "Transfer Date" },
  { key: "TRFRATE", header: "Transfer Rate" },
  { key: "TRFAMT", header: "Transfer Amount" },
  { key: "TOTAL_TRXNFEE", header: "Total Transaction Fee" },
  { key: "TOTAL_TRXNFEE_STAX", header: "Total Transaction Fee STAX" },
  { key: "Txn_Ref_No", header: "Transaction Reference No" },
  { key: "DESCMEMO", header: "Description Memo" },
  { key: "CHEQUENO", header: "Cheque No" },
  { key: "CHEQUEDTL", header: "Cheque Details" },
  { key: "PORTFOLIOID", header: "Portfolio ID" },
  { key: "DELIVERYDATE", header: "Delivery Date" },
  { key: "PAYMENTDATE", header: "Payment Date" },
  { key: "ACCRUEDINTEREST", header: "Accrued Interest" },
  { key: "ISSUER", header: "Issuer" },
  { key: "ISSUERNAME", header: "Issuer Name" },
  { key: "TDSAMOUNT", header: "TDS Amount" },
  { key: "STAMPDUTY", header: "Stamp Duty" },
  { key: "TPMSGAIN", header: "TPMS Gain" },
  { key: "RMID", header: "RM ID" },
  { key: "RMNAME", header: "RM Name" },
  { key: "ADVISORID", header: "Advisor ID" },
  { key: "ADVISORNAME", header: "Advisor Name" },
  { key: "BRANCHID", header: "Branch ID" },
  { key: "BRANCHNAME", header: "Branch Name" },
  { key: "GROUPID", header: "Group ID" },
  { key: "GROUPNAME", header: "Group Name" },
  { key: "OWNERID", header: "Owner ID" },
  { key: "OWNERNAME", header: "Owner Name" },
  { key: "WEALTHADVISOR_NAME", header: "Wealth Advisor Name" },
  { key: "SCHEMEID", header: "Scheme ID" },
  { key: "SCHEMENAME", header: "Scheme Name" },
];

export function RecentTradesSection() {
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  const [transactions, setTransactions] = useState([]);
  const [loading, setLoading] = useState(true);
  const [totalRows, setTotalRows] = useState(0);
  const [error, setError] = useState(null);
  

  useEffect(() => {
    const fetchTransactions = async (retryCount = 0) => {
      try {
        setLoading(true);
        setError(null);
        const response = await fetch(
          `${BASE_URL}/dashboard/getAllTransactions?page=${currentPage}&limit=${pageSize}`
        );
  
        // Check if response is OK and is JSON
        if (!response.ok) {
          // If 503 (Service Unavailable) and we haven't retried too many times, retry
          if (response.status === 503 && retryCount < 3) {
            await new Promise(resolve => setTimeout(resolve, 1000 * (retryCount + 1))); // Exponential backoff
            return fetchTransactions(retryCount + 1);
          }
          const text = await response.text();
          throw new Error(`HTTP ${response.status}: ${text.substring(0, 200)}`);
        }
  
        const contentType = response.headers.get("content-type");
        if (!contentType || !contentType.includes("application/json")) {
          const text = await response.text();
          throw new Error(`Expected JSON but got ${contentType}`);
        }
  
        const result = await response.json();
        
        if (result.data && Array.isArray(result.data)) {
          setTransactions(result.data);
          setTotalRows(result.pagination?.total || 0);
        } else {
          setTransactions([]);
          setTotalRows(0);
        }
      } catch (error) {
        setError(error.message || 'Failed to fetch transactions');
        setTransactions([]);
        setTotalRows(0);
      } finally {
        setLoading(false);
      }
    };
  
    fetchTransactions();
  }, [currentPage, pageSize]);

  const headerRow = {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    marginBottom: 12,
  };

  const subtitle = {
    fontSize: 11,
    color: "#6b7280",
  };

  const footerDivider = {
    borderTop: "1px solid #e5e7eb",
  };

  return (
    <Card style={{ marginTop: 24 }}>
      <div style={headerRow}>
        <h2 style={{ fontSize: 14, fontWeight: 600, color: "#111827" }}>
          Recent Trades
        </h2>
        {!loading && (
          <span style={subtitle}>
            Showing {transactions.length} of {totalRows} trades
          </span>
        )}
      </div>
      {loading ? (
        <div style={{ padding: "40px", textAlign: "center", color: "#6b7280" }}>
          Loading...
        </div>
      ) : error ? (
        <div style={{ padding: "40px", textAlign: "center", color: "#dc2626" }}>
          Error: {error}
        </div>
      ) : (
        <>
          <Table columns={tradesColumns} data={transactions} />
          <div style={footerDivider}>
            <Pagination
              currentPage={currentPage}
              pageSize={pageSize}
              totalRows={totalRows}
              onPageChange={(page) => setCurrentPage(page)}
              onPageSizeChange={(size) => {
                setPageSize(size);
                setCurrentPage(1);
              }}
            />
          </div>
        </>
      )}
    </Card>
  );
}

/* ===========================
   DASHBOARD PAGE SHELL
   =========================== */

export function DashboardSidebar() {
  const items = [
    { key: "dashboard", label: "Dashboard" },
    { key: "analytics", label: "Analytics" },
  ];
  return <Sidebar items={items} activeKey="dashboard" />;
}

export function DashboardTopbar() {
  return (
    <Topbar
      title="Stock Portfolio Dashboard"
      rightContent={
        <Button variant="secondary">Import File (Excel/CSV) </Button>
      }
    />
  );
}

export function DashboardPage() {
  const content = {
    padding: "24px 24px 32px 24px",
  };

  return (
    <PageLayout sidebar={<DashboardSidebar />}>
      <DashboardTopbar />
      <div style={content}>
        <SummaryCardsRow />
        <ChartsRow />
        <FiltersBar />
        <RecentTradesSection />
      </div>
    </PageLayout>
  );
}
