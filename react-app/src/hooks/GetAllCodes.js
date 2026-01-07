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

      const options = data.data.map((row) => ({
        value: row.clientIds.WS_Account_code,
        label: row.clientIds.WS_Account_code,
      }));

      setClientOptions(options);
    } catch (err) {
      console.error("Failed to fetch account codes:", err);
    }
  };

  return {
    clientOptions,
  };
}
