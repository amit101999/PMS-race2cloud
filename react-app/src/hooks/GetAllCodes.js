import { useEffect, useState } from "react";
import { BASE_URL } from "../constant.js";

export function useAccountCodes() {
  const [clientOptions, setClientOptions] = useState([]);

  useEffect(() => {
    fetchClientIds();
  }, []);

  const fetchClientIds = async () => {
    try {
      const res = await fetch(`${BASE_URL}/analytics/getAllAccountCodes`);
      const data = await res.json();

      const seen = new Set();
      const options = (data.data || [])
        .map((row) => {
          const code = (row.clientIds?.WS_Account_code ?? row.WS_Account_code ?? "").toString().trim();
          return { value: code, label: code };
        })
        .filter((opt) => {
          if (!opt.value || seen.has(opt.value)) return false;
          seen.add(opt.value);
          return true;
        })
        .sort((a, b) => (a.label || "").localeCompare(b.label || ""));

      setClientOptions(options);
    } catch (err) {
      console.error("Failed to fetch account codes:", err);
    }
  };

  return {
    clientOptions,
  };
}
