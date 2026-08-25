# Docker

Containerises the two runnable pieces of PMS:

| Service    | Source          | Base image        | Container port | Default host port |
| ---------- | --------------- | ----------------- | -------------- | ----------------- |
| `backend`  | `appsail-nodejs`| `node:20-alpine`  | 9000           | 3001              |
| `frontend` | `react-app`     | `nginx:1.27-alpine` (built with `node:20-alpine`) | 80 | 3000 |

`functions/` is **not** containerised. Those 30 folders are Catalyst
Functions — event/job targets invoked by the Catalyst platform, not HTTP
servers. They have no standalone runtime and are deployed with
`catalyst deploy --only functions`.

## Quick start

```bash
docker compose up --build
```

- Frontend: http://localhost:3000
- Backend:  http://localhost:3001/ (health JSON)

The backend stays on host port **3001** on purpose: that is what
`react-app/src/constant.js` falls back to, so a plain `npm start` dev frontend
keeps working against the container.

To change anything, `cp .env.docker.example .env` and edit.

## Building images individually

```bash
# backend
docker build -t pms-backend:local ./appsail-nodejs
docker run --rm -p 3001:9000 pms-backend:local

# frontend (API URL is baked in at build time)
docker build -t pms-frontend:local \
  --build-arg REACT_APP_API_BASE_URL=http://localhost:3001/api ./react-app
docker run --rm -p 3000:80 -e API_UPSTREAM=http://host.docker.internal:9000 pms-frontend:local
```

## How the frontend reaches the API

CRA inlines `REACT_APP_*` at **build** time, so the API URL is fixed when the
image is built — it cannot be changed with a runtime `-e` flag.

Compose builds it with `REACT_APP_API_BASE_URL=/api`, i.e. the browser calls the
same origin it loaded from, and nginx reverse-proxies `/api/` to
`${API_UPSTREAM}` (`http://backend:9000`). Two consequences:

- **CORS never applies.** `index.js` hard-codes `origin: "http://localhost:3000"`;
  with the proxy the request is same-origin, so that restriction is bypassed
  rather than fought.
- Pointing at a different backend means a **rebuild** with a different
  `--build-arg`, or overriding `API_UPSTREAM` to re-target the proxy.

`nginx.conf.template` also sets `client_max_body_size 50m` (transaction/bhav CSV
uploads) and 600s proxy timeouts (holding rebuilds, export polling).

## Catalyst credentials — read this before expecting data

`index.js` calls `catalyst.initialize(req)` on every request. The SDK builds the
app **entirely from `x-zc-*` request headers** that the Catalyst AppSail gateway
injects (project id, project key, environment, admin + user credential tokens).
A bare container never receives them, the `try/catch` in `index.js` sets
`req.catalystApp = null`, and every Data Store / Stratus / job route returns
`500 "Catalyst app not initialized"`.

So out of the box the container **builds, boots, and serves `/`** — but no
business route returns data.

`appsail-nodejs/docker/catalyst-headers-preload.js` closes that gap. It is
loaded via `node --import` and is a no-op unless `CATALYST_INJECT_HEADERS=true`.
When enabled it prepends a `request` listener that fills the headers in from
env vars:

| Env var | Header | Required |
| --- | --- | --- |
| `CATALYST_PROJECT_ID` | `x-zc-projectid` | yes |
| `CATALYST_PROJECT_KEY` | `x-zc-project-key` | yes |
| `CATALYST_ENVIRONMENT` | `x-zc-environment` | yes |
| `CATALYST_ACCESS_TOKEN` | `x-zc-admin-cred-token` + `x-zc-user-cred-token` | yes |
| `CATALYST_PROJECT_DOMAIN` | `x-zc-project-domain` | no (SDK default) |

Caveat: an OAuth access token expires (~1 hour), so this is a development
convenience, not a production auth story. Existing headers are never
overwritten, and nothing about the AppSail deployment path changes — Catalyst
runs `node index.js` from `app-config.json`, not this entrypoint.

## Notes

- AppSail's declared stack is **node16** (`app-config.json`). node16 is EOL with
  no maintained base image, so the containers run **node20**. All five runtime
  deps declare `node >=10`; nothing in `index.js` is node16-specific.
- Both `package-lock.json` files were verified in sync with their
  `package.json`, so `npm ci` is reproducible in both images.
- The backend image installs production deps only (`npm ci --omit=dev`) and runs
  as the non-root `node` user.
- The frontend build sets `CI=false` so CRA lint warnings don't fail the image
  build, and `GENERATE_SOURCEMAP=false` to keep the layer small.
- Uploads are held in memory (`express-fileupload` without `useTempFiles`), so
  no writable volume is needed.
