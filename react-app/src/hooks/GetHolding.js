import { useState, useRef } from "react";
import { BASE_URL } from "../constant.js";

export function useHoldings() {
  const [holdings, setHoldings] = useState([]);
  const [loadingHoldings, setLoadingHoldings] = useState(false);
  const requestIdRef = useRef(0);

  const fetchHoldings = async (accountCode, asOnDate) => {
    if (!accountCode) return;

    const currentRequestId = ++requestIdRef.current;

    setLoadingHoldings(true);
    setHoldings([]);

    try {
      const params = new URLSearchParams({ accountCode });
      if (asOnDate) params.set("asOnDate", asOnDate);

      const res = await fetch(
        `${BASE_URL}/analytics/getHoldingsSummarySimple?${params.toString()}`
      );
      const data = await res.json();

      if (currentRequestId !== requestIdRef.current) return;

      if (Array.isArray(data)) {
        setHoldings(data);
      } else {
        console.error("Unexpected holdings response:", data);
        setHoldings([]);
      }
    } catch (err) {
      if (currentRequestId !== requestIdRef.current) return;
      console.error("Failed to fetch holdings:", err);
    } finally {
      if (currentRequestId === requestIdRef.current) {
        setLoadingHoldings(false);
      }
    }
  };

  return {
    holdings,
    setHoldings,
    loadingHoldings,
    fetchHoldings,
  };
}
