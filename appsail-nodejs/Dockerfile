# PMS backend (Catalyst AppSail Express server)
#
# AppSail runs this on the node16 stack (app-config.json). node16 is EOL and has
# no maintained base image, so the container runs node20 instead. All five
# runtime deps (express 4, cors, csv-parser, express-fileupload,
# zcatalyst-sdk-node) declare node >=10 and run clean on 20.

# ---------- deps ----------
FROM node:20-alpine AS deps
WORKDIR /app

# package-lock.json is in sync with package.json, so `npm ci` is reproducible.
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

# ---------- runtime ----------
FROM node:20-alpine AS runtime
WORKDIR /app

ENV NODE_ENV=production \
    X_ZOHO_CATALYST_LISTEN_PORT=9000

COPY --from=deps /app/node_modules ./node_modules
COPY package.json package-lock.json ./
COPY index.js ./
COPY controller ./controller
COPY router ./router
COPY util ./util
COPY docker ./docker

USER node
EXPOSE 9000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- "http://127.0.0.1:${X_ZOHO_CATALYST_LISTEN_PORT}/" >/dev/null 2>&1 || exit 1

# The preload is a no-op unless CATALYST_INJECT_HEADERS=true (see docs/DOCKER.md).
CMD ["node", "--import", "./docker/catalyst-headers-preload.js", "index.js"]
