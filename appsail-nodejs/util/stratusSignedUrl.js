/**
 * Normalizes Catalyst Stratus generatePreSignedUrl() result to a single URL string.
 * @param {unknown} res
 * @returns {string}
 */
export function stratusSignedUrlToString(res) {
  if (res == null) return "";
  if (typeof res === "string") return res;
  if (typeof res.accessUrl === "string") return res.accessUrl;
  if (typeof res.signature === "string") return res.signature;
  if (res.signature && typeof res.signature === "object") {
    return stratusSignedUrlToString(res.signature);
  }
  return "";
}

/** Options key per zcatalyst-sdk-node Stratus bucket (expiryIn, not expiresIn). */
export const STRATUS_GET_URL_OPTS = { expiryIn: "3600" };
