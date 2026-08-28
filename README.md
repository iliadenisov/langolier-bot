# langolier-bot

A Telegram **user-account** client that keeps group chats tidy: it deletes the
account's own messages after a per-chat time-to-live and removes throwaway
command messages the moment they are sent. Everything is configured at runtime
from a small **service bot** — no redeploy to change a rule.

Built on [`gotd/td`](https://github.com/gotd/td) (pure-Go MTProto, no CGO). The
release image is `FROM scratch`, ~22 MB.

## What it does

- **TTL cleanup.** For each configured group, own messages older than the
  configured age (in minutes) are deleted. `0` = disabled; nothing runs until you
  opt a chat in.
- **Instant-delete patterns.** Per chat, a list of patterns (each `exact` or
  `prefix`); a matching own text message is deleted right after it is sent.
- **Groups only.** Supergroups (megagroups) and basic groups. Broadcast channels
  and private chats are never touched.
- **Anti-flood.** All MTProto calls go through a rate limiter (~1 rps) and a
  `FLOOD_WAIT`-aware waiter; deletions are batched (≤100 ids) and serialized, so a
  large cleanup will not get the account limited.
- **Gap recovery.** `updates.Manager` replays updates missed during downtime, so
  messages sent while the bot was offline still age out and get deleted.
- **Persistence.** One bbolt file under `DATA_DIR`: MTProto session, peer access
  hashes, updates state and the per-chat config. The own-message index is
  in-memory and rebuilt by a paced history scan on start.

## Service bot commands (owner only)

| Command   | Effect                                                                       |
|-----------|-----------------------------------------------------------------------------|
| `/start`  | Launch the user client. Prompts for phone → code → 2FA password on first run.|
| `/stop`   | Stop the user client.                                                       |
| `/config` | Inline chat picker → per-chat menu: set/clear TTL, manage patterns, purge now, disable. |
| `/status` | Configured chats with TTL, pattern count, indexed messages and delete count.|

Only `BOT_OWNER_ID` may talk to the service bot.

## Configuration

| Variable       | Required | Meaning                                                     |
|----------------|----------|------------------------------------------------------------|
| `DATA_DIR`     | yes      | Directory for `langolier.db` (the bbolt state file).       |
| `BOT_TOKEN`    | yes      | Service-bot token from @BotFather.                         |
| `BOT_OWNER_ID` | yes      | Telegram numeric user id allowed to command the bot.       |
| `API_ID`       | yes      | Telegram app id from <https://my.telegram.org>.            |
| `API_HASH`     | yes      | Telegram app hash.                                         |
| `LOG_LEVEL`    | no       | `debug` / `info` / `warn` / `error` (default `info`).      |

## Build and run

```sh
# build
docker build -t langolier-bot .

# run (the volume keeps the login session across restarts)
docker run --rm \
  -e DATA_DIR=/data \
  -e BOT_TOKEN=123456:AA... \
  -e BOT_OWNER_ID=100200300 \
  -e API_ID=00000 \
  -e API_HASH=0123456789abcdef0123456789abcdef \
  -v langolier_data:/data \
  langolier-bot
```

Then DM the service bot `/start` and complete the login. From source:
`go build ./cmd/langolier`.

## Development

```sh
go test ./...
go vet ./...
gofmt -l .
```

Layout: `cmd/langolier` (wiring), `internal/tgclient` (gotd wrapper + auth relay),
`internal/cleaner` (index + TTL sweep + patterns), `internal/chatcfg` (bbolt
config store), `internal/bot` (service bot UI).
