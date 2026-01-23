// import React, { useState, useEffect, useRef } from "react";
// import MainLayout from "../../layouts/MainLayout";
// import { Card } from "../../components/common/CommonComponents";
// import "./SplitPage.css";
// import { BASE_URL } from "../../constant";

// function SplitPage() {
//   /* ===========================
//      SPLIT FORM STATE
//      =========================== */
//   const [securityCode, setSecurityCode] = useState("");
//   const [securityName, setSecurityName] = useState("");
//   const [ratio1, setRatio1] = useState("");
//   const [ratio2, setRatio2] = useState("");
//   const [date, setDate] = useState("");
//   const [isin, setIsin] = useState("");

//   const [securityOptions, setSecurityOptions] = useState([]);
//   const [searchQuery, setSearchQuery] = useState("");
//   const [showDropdown, setShowDropdown] = useState(false);
//   const [loading, setLoading] = useState(false);
//   const [error, setError] = useState(null);
//   const [success, setSuccess] = useState(false);
//   const dropdownRef = useRef(null);

//   /* ===========================
//      FETCH ALL SECURITIES ON MOUNT
//      =========================== */
//   useEffect(() => {
//     fetchAllSecurities();
//   }, []);

//   /* ===========================
//      HANDLE CLICK OUTSIDE - CLOSE DROPDOWN
//      =========================== */
//   useEffect(() => {
//     const handleClickOutside = (e) => {
//       if (dropdownRef.current && !dropdownRef.current.contains(e.target)) {
//         setShowDropdown(false);
//       }
//     };
//     document.addEventListener("mousedown", handleClickOutside);
//     return () => document.removeEventListener("mousedown", handleClickOutside);
//   }, []);

//   /* ===========================
//      FETCH ALL SECURITIES
//      =========================== */
//   const fetchAllSecurities = async () => {
//     try {
//       const res = await fetch(`${BASE_URL}/split/getAllSecurityCodes`);
//       const data = await res.json();
//       if (data.data && Array.isArray(data.data)) {
//         const options = data.data.map((sec) => ({
//           value: sec.securityCode,
//           label: sec.securityCode,
//           name: sec.securityName || sec.securityCode,
//           isin: sec.ISIN || "",
//         }));
//         setSecurityOptions(options);
//       }
//     } catch (err) {
//       console.error("Failed to fetch securities:", err);
//     }
//   };

//   /* ===========================
//      FILTER SECURITIES BASED ON SEARCH
//      =========================== */
//   const filteredOptions = securityOptions.filter(
//     (opt) =>
//       opt.label.toLowerCase().includes(searchQuery.toLowerCase()) ||
//       opt.name.toLowerCase().includes(searchQuery.toLowerCase())
//   );

//   /* ===========================
//      HANDLE INPUT CHANGE
//      =========================== */
//   const handleInputChange = (e) => {
//     setSearchQuery(e.target.value);
//     setShowDropdown(true);
//   };

//   /* ===========================
//      HANDLE SECURITY SELECTION
//      =========================== */
//   const handleSelect = (option) => {
//     setSearchQuery(option.label);
//     setSecurityCode(option.value);
//     setSecurityName(option.name);
//     setIsin(option.isin || "");
//     setShowDropdown(false);
//   };

//   /* ===========================
//      SUBMIT SPLIT
//      =========================== */
//   const handleSubmit = async () => {
//     if (Number(ratio1) === Number(ratio2)) {
//       setError("Split ratio cannot be the same");
//       return;
//     }

//     try {
//       setLoading(true);
//       setError(null);
//       setSuccess(false);

//       const payload = {
//         securityCode,
//         securityName,
//         ratio1: Number(ratio1),
//         ratio2: Number(ratio2),
//         issueDate: date,
//       };

//       const res = await fetch(`${BASE_URL}/split/add`, {
//         method: "POST",
//         headers: { "Content-Type": "application/json" },
//         body: JSON.stringify(payload),
//       });

//       if (!res.ok) {
//         const errData = await res.json().catch(() => ({}));
//         throw new Error(errData.message || "Failed to apply split");
//       }

//       setSuccess(true);

//       // Reset form
//       setSecurityCode("");
//       setSecurityName("");
//       setSearchQuery("");
//       setRatio1("");
//       setRatio2("");
//       setIsin("");
//       setDate("");

//       setTimeout(() => setSuccess(false), 3000);
//     } catch (err) {
//       setError(err.message);
//     } finally {
//       setLoading(false);
//     }
//   };

//   return (
//     <MainLayout title="Stock Split">
//       <Card style={{ marginTop: 24 }}>
//         <h2 style={{ fontSize: 18, fontWeight: 600, marginBottom: 20 }}>
//           Add Stock Split
//         </h2>

//         {success && (
//           <div className="alert success">Split saved successfully!</div>
//         )}
//         {error && <div className="alert error">{error}</div>}

//         <div className="split-card">
//           <div className="split-field">
//             <label>ISIN</label>
//             <input
//               type="text"
//               value={isin}
//               placeholder="Enter ISIN"
//               onChange={(e) => setIsin(e.target.value)}
//             />
//           </div>
//           <div className="account-code-search" ref={dropdownRef}>
//             <label className="search-label">Security Code</label>
//             <div className="search-input-wrapper">
//               <span className="search-icon">🔍</span>
//               <input
//                 type="text"
//                 className="search-input"
//                 placeholder="Search Security Code..."
//                 value={searchQuery}
//                 onChange={handleInputChange}
//                 onFocus={() => setShowDropdown(true)}
//               />
//               {searchQuery && (
//                 <span
//                   className="clear-icon"
//                   onClick={() => {
//                     setSearchQuery("");
//                     setSecurityCode("");
//                     setSecurityName("");
//                   }}
//                 >
//                   ✕
//                 </span>
//               )}
//               <span className="arrow-icon">▾</span>
//             </div>
//             {showDropdown && filteredOptions.length > 0 && (
//               <div className="search-dropdown">
//                 <div className="dropdown-header">Search Security Code...</div>
//                 <div className="dropdown-options">
//                   {filteredOptions.map((opt) => (
//                     <div
//                       key={opt.value}
//                       className="dropdown-option"
//                       onClick={() => handleSelect(opt)}
//                     >
//                       {opt.label}
//                       {opt.name && opt.name !== opt.label && (
//                         <span style={{ color: "#6b7280", marginLeft: 8 }}>
//                           - {opt.name}
//                         </span>
//                       )}
//                     </div>
//                   ))}
//                 </div>
//                 <div className="dropdown-footer">
//                   {filteredOptions.length} of {securityOptions.length} options
//                 </div>
//               </div>
//             )}
//           </div>

//           <div className="split-field">
//             <label>Security Name</label>
//             <input type="text" value={securityName} disabled />
//           </div>

//           <div className="split-field">
//             <label>Ratio 1</label>
//             <input
//               type="number"
//               value={ratio1}
//               placeholder="e.g. 1"
//               onChange={(e) => setRatio1(e.target.value)}
//             />
//           </div>

//           <div className="split-field">
//             <label>Ratio 2</label>
//             <input
//               type="number"
//               value={ratio2}
//               placeholder="e.g. 2"
//               onChange={(e) => setRatio2(e.target.value)}
//             />
//           </div>

//           <div className="split-field">
//             <label>Effective Date</label>
//             <input
//               type="date"
//               value={date}
//               onChange={(e) => setDate(e.target.value)}
//             />
//           </div>

//           <div className="split-actions">
//             <button
//               className="split-submit"
//               disabled={
//                 !securityCode ||
//                 !ratio1 ||
//                 !ratio2 ||
//                 !date ||
//                 Number(ratio1) <= 0 ||
//                 Number(ratio2) <= 0 ||
//                 loading
//               }
//               onClick={handleSubmit}
//             >
//               {loading ? "Saving..." : "Apply Split"}
//             </button>
//           </div>
//         </div>
//       </Card>
//     </MainLayout>
//   );
// }

// export default SplitPage;

import React, { useState, useEffect, useRef } from "react";
import MainLayout from "../../layouts/MainLayout";
import { Card } from "../../components/common/CommonComponents";
import "./SplitPage.css";
import { BASE_URL } from "../../constant";

function SplitPage() {
  /* ===========================
     FORM STATE
     =========================== */
  const [isin, setIsin] = useState("");
  const [securityCode, setSecurityCode] = useState("");
  const [securityName, setSecurityName] = useState("");
  const [ratio1, setRatio1] = useState("");
  const [ratio2, setRatio2] = useState("");
  const [date, setDate] = useState("");

  const [securities, setSecurities] = useState([]);
  const [searchQuery, setSearchQuery] = useState("");
  const [showDropdown, setShowDropdown] = useState(false);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(false);

  const dropdownRef = useRef(null);

  /* ===========================
     FETCH ISIN + CODE + NAME
     =========================== */
  useEffect(() => {
    fetchAllSecurities();
  }, []);

  const fetchAllSecurities = async () => {
    try {
      const res = await fetch(`${BASE_URL}/split/getAllSecuritiesList`);
      const data = await res.json();
      if (data.success) {
        setSecurities(data.data);
      }
    } catch (err) {
      console.error("Failed to fetch securities", err);
    }
  };

  /* ===========================
     CLOSE DROPDOWN ON OUTSIDE CLICK
     =========================== */
  useEffect(() => {
    const handleClickOutside = (e) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target)) {
        setShowDropdown(false);
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  /* ===========================
     FILTER ISIN SEARCH
     =========================== */
  const filteredSecurities = securities.filter(
    (sec) =>
      sec.isin.toLowerCase().includes(searchQuery.toLowerCase()) ||
      sec.securityCode.toLowerCase().includes(searchQuery.toLowerCase()) ||
      sec.securityName.toLowerCase().includes(searchQuery.toLowerCase()),
  );

  /* ===========================
     HANDLE ISIN SELECTION
     =========================== */
  const handleSelectISIN = (sec) => {
    setIsin(sec.isin);
    setSearchQuery(sec.isin);
    setSecurityCode(sec.securityCode);
    setSecurityName(sec.securityName);
    setShowDropdown(false);
  };

  /* ===========================
     SUBMIT SPLIT
     =========================== */
  const handleSubmit = async () => {
    if (Number(ratio1) === Number(ratio2)) {
      setError("Split ratio cannot be the same");
      return;
    }

    try {
      setLoading(true);
      setError(null);
      setSuccess(false);

      const payload = {
        isin,
        securityCode,
        securityName,
        ratio1: Number(ratio1),
        ratio2: Number(ratio2),
        issueDate: date,
      };

      const res = await fetch(`${BASE_URL}/split/add`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.message || "Failed to apply split");
      }

      setSuccess(true);

      // Reset form
      setIsin("");
      setSearchQuery("");
      setSecurityCode("");
      setSecurityName("");
      setRatio1("");
      setRatio2("");
      setDate("");

      setTimeout(() => setSuccess(false), 3000);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <MainLayout title="Stock Split">
      <Card style={{ marginTop: 24 }}>
        <h2 style={{ fontSize: 18, fontWeight: 600, marginBottom: 20 }}>
          Add Stock Split
        </h2>

        {success && (
          <div className="alert success">Split saved successfully!</div>
        )}
        {error && <div className="alert error">{error}</div>}

        <div className="split-card">
          {/* ISIN SEARCH */}
          <div className="account-code-search" ref={dropdownRef}>
            <label className="search-label">ISIN</label>

            <div className="search-input-wrapper">
              <span className="search-icon">🔍</span>

              <input
                type="text"
                className="search-input"
                placeholder="Search ISIN..."
                value={searchQuery}
                onChange={(e) => {
                  setSearchQuery(e.target.value);
                  setShowDropdown(true);
                }}
                onFocus={() => setShowDropdown(true)}
              />

              {searchQuery && (
                <span
                  className="clear-icon"
                  onClick={() => {
                    setSearchQuery("");
                    setIsin("");
                    setSecurityCode("");
                    setSecurityName("");
                  }}
                >
                  ✕
                </span>
              )}

              <span className="arrow-icon">▾</span>
            </div>

            {showDropdown && filteredSecurities.length > 0 && (
              <div className="search-dropdown">
                <div className="dropdown-header">Search ISIN</div>

                <div className="dropdown-options">
                  {filteredSecurities.map((sec) => (
                    <div
                      key={sec.isin}
                      className="dropdown-option"
                      onClick={() => handleSelectISIN(sec)}
                    >
                      <strong>{sec.isin}</strong>
                      <div style={{ fontSize: 12, color: "#6b7280" }}>
                        {sec.securityCode} – {sec.securityName}
                      </div>
                    </div>
                  ))}
                </div>

                <div className="dropdown-footer">
                  {filteredSecurities.length} of {securities.length} ISINs
                </div>
              </div>
            )}
          </div>

          {/* AUTO FILLED */}
          <div className="split-field">
            <label>Security Code</label>
            <input value={securityCode} disabled />
          </div>

          <div className="split-field">
            <label>Security Name</label>
            <input value={securityName} disabled />
          </div>

          {/* RATIOS */}
          <div className="split-field">
            <label>Ratio 1</label>
            <input
              type="number"
              value={ratio1}
              onChange={(e) => setRatio1(e.target.value)}
            />
          </div>

          <div className="split-field">
            <label>Ratio 2</label>
            <input
              type="number"
              value={ratio2}
              onChange={(e) => setRatio2(e.target.value)}
            />
          </div>

          {/* DATE */}
          <div className="split-field">
            <label>Effective Date</label>
            <input
              type="date"
              value={date}
              onChange={(e) => setDate(e.target.value)}
            />
          </div>

          {/* ACTION */}
          <div className="split-actions">
            <button
              className="split-submit"
              disabled={
                !isin ||
                !ratio1 ||
                !ratio2 ||
                !date ||
                Number(ratio1) <= 0 ||
                Number(ratio2) <= 0 ||
                loading
              }
              onClick={handleSubmit}
            >
              {loading ? "Saving..." : "Apply Split"}
            </button>
          </div>
        </div>
      </Card>
    </MainLayout>
  );
}

export default SplitPage;
