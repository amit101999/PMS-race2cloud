# PMS frontend (CRA 5 / React 19) built once and served by nginx.
#
# The API base URL is baked in at BUILD time (CRA inlines REACT_APP_* into the
# bundle). The default "/api" makes the browser call the same origin, and nginx
# reverse-proxies /api to the backend container — so no CORS is involved.

# ---------- build ----------
FROM node:20-alpine AS build
WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY public ./public
COPY src ./src

ARG REACT_APP_API_BASE_URL=/api
ENV REACT_APP_API_BASE_URL=$REACT_APP_API_BASE_URL
# react-scripts treats lint warnings as errors when CI is set (Docker/BuildKit
# does not set it, but some CI runners inherit it) — keep the build tolerant.
ENV CI=false
ENV GENERATE_SOURCEMAP=false

RUN npm run build

# ---------- serve ----------
FROM nginx:1.27-alpine AS runtime

# Rendered by the nginx image's entrypoint; ${API_UPSTREAM} comes from env.
ENV API_UPSTREAM=http://backend:9000
COPY nginx.conf.template /etc/nginx/templates/default.conf.template
COPY --from=build /app/build /usr/share/nginx/html

EXPOSE 80

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD wget -qO- http://127.0.0.1/ >/dev/null 2>&1 || exit 1
