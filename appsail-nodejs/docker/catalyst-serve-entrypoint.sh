#!/bin/sh
# Starts `catalyst serve` and exposes it outside the container.
#
# The CLI's proxy — the part that injects the x-zc-* Catalyst headers — binds
# 127.0.0.1 only, so a published port cannot reach it. (The Express process it
# spawns listens on all interfaces at 4600, but bypasses the proxy and so gets
# no headers: hitting it directly returns 500.) socat bridges 0.0.0.0:9000 to
# the loopback proxy.
set -e

SERVE_PORT="${CATALYST_SERVE_PORT:-3001}"
LISTEN_PORT="${X_ZOHO_CATALYST_LISTEN_PORT:-9000}"

catalyst serve --only appsail --no-open --ignore-scripts &
SERVE_PID=$!

# Bail out if the CLI dies during startup rather than hanging forever.
echo "[entrypoint] waiting for catalyst serve on 127.0.0.1:${SERVE_PORT}..."
while ! nc -z 127.0.0.1 "${SERVE_PORT}"; do
  if ! kill -0 "${SERVE_PID}" 2>/dev/null; then
    echo "[entrypoint] catalyst serve exited before binding ${SERVE_PORT}" >&2
    exit 1
  fi
  sleep 1
done

echo "[entrypoint] forwarding 0.0.0.0:${LISTEN_PORT} -> 127.0.0.1:${SERVE_PORT}"
exec socat "TCP-LISTEN:${LISTEN_PORT},fork,reuseaddr" "TCP:127.0.0.1:${SERVE_PORT}"
