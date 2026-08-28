# Deploy

The bot runs as a container next to an AmneziaWG egress sidecar, brought up as a
single compose project. The bot shares the sidecar's network namespace, so **all**
of its traffic — MTProto and the Bot API — leaves through the tunnel.

CI does the deploy: on a push to `master` the `deploy` job (gated by the
`production` environment approval) builds the image on the runner and runs
`docker compose -p langolier up -d --force-recreate`, then `probe.sh`.

## Configuration

Everything is supplied via the environment; nothing host-specific is in the repo.

**Secrets**

| Name             | Meaning                                                            |
|------------------|-------------------------------------------------------------------|
| `BOT_TOKEN`      | Service-bot token from @BotFather.                                |
| `API_ID`         | Telegram app id (my.telegram.org).                                |
| `API_HASH`       | Telegram app hash.                                                |
| `AWG_CONF`       | The AmneziaWG sidecar config (multi-line). See below.             |
| `REGISTRY_USER`  | Optional — only if `SIDECAR_IMAGE` is in a private registry.      |
| `REGISTRY_TOKEN` | Optional — pairs with `REGISTRY_USER`.                            |

**Variables**

| Name             | Meaning                                                            |
|------------------|-------------------------------------------------------------------|
| `SIDECAR_IMAGE`  | Image reference for the AmneziaWG sidecar.                        |
| `BOT_OWNER_ID`   | Telegram numeric user id allowed to command the service bot.      |
| `LOG_LEVEL`      | `debug` / `info` / `warn` / `error` (default `info`).             |
| `REGISTRY_HOST`  | Optional — set only to enable a `docker login` before the deploy. |

## `AWG_CONF`

A standard WireGuard / AmneziaWG `.conf`:

- **Full tunnel:** `AllowedIPs = 0.0.0.0/0` (add `::/0` if the peer has IPv6).
- **No `DNS =` line.** In the shared netns it hijacks resolution and breaks
  `api.telegram.org`; without it the bot uses the container runtime's resolver.
  MTProto itself dials data-center IPs and needs no DNS.
- Its **own** WireGuard peer — distinct keys and address, not shared with another
  tunnel (a shared public key evicts the other endpoint).

## Data volume

The bot's state — MTProto session, peer access hashes, updates state and the
per-chat config — lives in the named volume `langolier_data` (compose volume
`data`). It must survive redeploys. Back it up with:

```sh
docker run --rm -v langolier_data:/d -v "$PWD":/b alpine \
  tar czf /b/langolier-data.tgz -C /d .
```

## First run

After the first successful deploy the account is not yet authorized. DM the
service bot:

```
/start
<phone number, international format>
<login code>
<2FA password>
```

The log then prints a `self` line. `/config` opens the chat picker; per group you
set a message TTL (minutes) and instant-delete patterns. `/status` shows counters.

## Manual operations

```sh
# from this directory, with the same env vars exported
docker compose -p langolier up -d --force-recreate
docker compose -p langolier logs -f bot
docker compose -p langolier down

# verify the tunnel
docker exec "$(docker compose -p langolier ps -q vpn)" \
  sh -c 'wget -qO- https://api.telegram.org >/dev/null && echo OK'
```

A redeploy can also be triggered from the Actions tab (`workflow_dispatch` on
`master`), e.g. after restoring the data volume.
