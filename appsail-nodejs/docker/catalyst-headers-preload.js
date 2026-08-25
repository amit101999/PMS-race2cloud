/*
 * Optional preload for running the AppSail server outside Catalyst (i.e. in Docker).
 *
 * Why it exists: index.js calls `catalyst.initialize(req)` per request, and the
 * SDK builds the app purely from `x-zc-*` request headers that the Catalyst
 * AppSail gateway injects. A bare container never gets those headers, so
 * `req.catalystApp` ends up null and every data route returns 500.
 *
 * This module prepends a `request` listener that fills those headers in from
 * env vars. It is a NO-OP unless CATALYST_INJECT_HEADERS=true, so the normal
 * AppSail deployment (which does not use this entrypoint anyway) is unaffected.
 *
 * Required when enabled:
 *   CATALYST_PROJECT_ID     - numeric project id (see .catalystrc -> projects[].id)
 *   CATALYST_PROJECT_KEY    - project key from the Catalyst console
 *   CATALYST_ENVIRONMENT    - "Development" or "Production"
 *   CATALYST_ACCESS_TOKEN   - OAuth access token used as both admin and user cred
 * Optional:
 *   CATALYST_PROJECT_DOMAIN - defaults to the SDK's built-in Catalyst origin
 *
 * Caveat: OAuth access tokens expire (typically 1h). This is a development
 * convenience, not a production auth story.
 */
import http from "node:http";

const enabled = process.env.CATALYST_INJECT_HEADERS === "true";

if (enabled) {
  const projectId = process.env.CATALYST_PROJECT_ID;
  const projectKey = process.env.CATALYST_PROJECT_KEY;
  const environment = process.env.CATALYST_ENVIRONMENT;
  const accessToken = process.env.CATALYST_ACCESS_TOKEN;
  const projectDomain = process.env.CATALYST_PROJECT_DOMAIN;

  const missing = Object.entries({
    CATALYST_PROJECT_ID: projectId,
    CATALYST_PROJECT_KEY: projectKey,
    CATALYST_ENVIRONMENT: environment,
    CATALYST_ACCESS_TOKEN: accessToken,
  })
    .filter(([, v]) => !v)
    .map(([k]) => k);

  if (missing.length) {
    console.warn(
      `[catalyst-headers] CATALYST_INJECT_HEADERS=true but missing: ${missing.join(", ")}. ` +
        "Catalyst-backed routes will fail.",
    );
  }

  const injected = {
    "x-zc-projectid": projectId,
    "x-zc-project-key": projectKey,
    "x-zc-environment": environment,
    "x-zc-admin-cred-type": "token",
    "x-zc-admin-cred-token": accessToken,
    "x-zc-user-cred-type": "token",
    "x-zc-user-cred-token": accessToken,
    "x-zc-user-type": "admin",
  };
  if (projectDomain) {
    injected["x-zc-project-domain"] = projectDomain;
  }

  const createServer = http.createServer;
  // Express's app.listen() delegates to http.createServer(app), which registers
  // the app as a `request` listener. prependListener puts us ahead of it.
  http.createServer = function patchedCreateServer(...args) {
    const server = createServer.apply(this, args);
    server.prependListener("request", (req) => {
      for (const [key, value] of Object.entries(injected)) {
        // Never clobber a real header if one somehow arrives.
        if (value !== undefined && req.headers[key] === undefined) {
          req.headers[key] = value;
        }
      }
    });
    return server;
  };

  console.log("[catalyst-headers] injecting x-zc-* headers from environment");
}
