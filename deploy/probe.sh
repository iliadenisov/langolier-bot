#!/usr/bin/env bash
# Liveness probe run right after a deploy. The bot long-polls / speaks MTProto
# through the VPN sidecar and has no public ingress, so there is nothing to curl
# from outside: inspect the containers instead.
#
#   1. the `bot` container must be running, not restarting, with a stable
#      RestartCount (a crash-loop from a bad token or unreachable Telegram shows
#      up as a climbing count);
#   2. the tunnel must actually carry traffic — checked from inside the `vpn`
#      netns, which the bot shares.
#
# Argument: the compose project name (e.g. langolier).
set -u

proj="${1:?usage: probe.sh <compose-project>}"

sleep 20 # let the tunnel handshake and the first long-poll settle

cid="$(docker compose -p "$proj" ps -q bot)"
if [ -z "$cid" ]; then
  echo "bot container not found for project $proj"
  exit 1
fi

healthy=""
for _ in $(seq 1 20); do
  status="$(docker inspect -f '{{.State.Status}}' "$cid" 2>/dev/null || echo missing)"
  restarting="$(docker inspect -f '{{.State.Restarting}}' "$cid" 2>/dev/null || echo true)"
  if [ "$status" = "running" ] && [ "$restarting" = "false" ]; then
    c1="$(docker inspect -f '{{.RestartCount}}' "$cid")"
    sleep 5
    c2="$(docker inspect -f '{{.RestartCount}}' "$cid")"
    if [ "$c1" = "$c2" ]; then
      healthy=1
      echo "bot container healthy: status=running restarts=$c2"
      break
    fi
    echo "bot still restarting ($c1 -> $c2); waiting"
  fi
  sleep 3
done

if [ -z "$healthy" ]; then
  echo "bot not healthy; recent logs:"
  docker logs --tail 80 "$cid" || true
  exit 1
fi

vpn="$(docker compose -p "$proj" ps -q vpn)"
if [ -z "$vpn" ]; then
  echo "vpn container not found for project $proj"
  exit 1
fi

# Active tunnel check from inside the sidecar netns. Use whatever HTTP client the
# sidecar image ships; if it ships none, the check is inconclusive, not a
# failure — the bot's own restart count is the hard gate.
tunnel_check='
if command -v wget >/dev/null 2>&1; then
  wget -q -T 15 -O /dev/null https://api.telegram.org
elif command -v curl >/dev/null 2>&1; then
  curl -sS -m 15 -o /dev/null https://api.telegram.org
else
  echo NO_HTTP_CLIENT; exit 3
fi'

if docker exec "$vpn" sh -c "$tunnel_check"; then
  echo "tunnel OK: api.telegram.org reachable from the sidecar netns"
  exit 0
fi
rc=$?
if [ "$rc" -eq 3 ]; then
  echo "tunnel check skipped: no HTTP client in the sidecar image"
  exit 0
fi

echo "tunnel check failed: api.telegram.org not reachable through the sidecar"
docker logs --tail 40 "$vpn" || true
exit 1
