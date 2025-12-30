export const normalizeName = (name = "") => {
    return String(name)
      .trim()
      .toUpperCase()
      .replace(/\s+/g, " ")
      .replace(/[^\w\s]/g, "")
      .replace(/\bLIMITED\b/g, "LTD")
      .replace(/\bINCORPORATED\b/g, "INC")
      .replace(/\bCORPORATION\b/g, "CORP");
  };