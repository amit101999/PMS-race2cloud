// Set REACT_APP_API_BASE_URL at build time to point the app elsewhere
// (Docker builds default it to "/api", proxied by nginx to the backend).
// Deployed AppSail: https://backend-50039746698.development.catalystappsail.in/api
export const BASE_URL =
  process.env.REACT_APP_API_BASE_URL || "http://localhost:3001/api";
