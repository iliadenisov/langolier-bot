// Command langolier runs a Telegram user account that deletes the account's own
// messages in group chats once they exceed a per-chat TTL, plus instant-delete
// patterns, all configured at runtime through a service bot.
package main

import (
	"context"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
	_ "time/tzdata" // embed the time-zone database for the scratch image

	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"langolier-bot/internal/bot"
	"langolier-bot/internal/chatcfg"
	"langolier-bot/internal/cleaner"
	"langolier-bot/internal/config"
	"langolier-bot/internal/tgclient"
)

// version is set at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dataDir := config.MustEnvString("DATA_DIR")
	botToken := config.MustEnvString("BOT_TOKEN")
	ownerID := config.MustEnvInt64("BOT_OWNER_ID")
	apiID := config.MustEnvInt("API_ID")
	apiHash := config.MustEnvString("API_HASH")
	logLevel := config.EnvStringDefault("LOG_LEVEL", "info")

	logger := buildLogger(logLevel)
	defer func() { _ = logger.Sync() }()
	logger.Info("starting langolier", zap.String("version", version))

	if err := os.MkdirAll(dataDir, 0o750); err != nil {
		config.ExitWithError(err)
	}
	db, err := bolt.Open(filepath.Join(dataDir, "langolier.db"), 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		config.ExitWithError(err)
	}
	defer func() { _ = db.Close() }()

	cfgStore, err := chatcfg.New(db)
	if err != nil {
		config.ExitWithError(err)
	}

	svc, err := bot.New(botToken, ownerID, logger.Named("bot"))
	if err != nil {
		config.ExitWithError(err)
	}

	tgc, err := tgclient.New(tgclient.Options{
		DB:         db,
		AppID:      apiID,
		AppHash:    apiHash,
		Logger:     logger.Named("tg"),
		Relay:      svc,
		AppVersion: version,
	})
	if err != nil {
		config.ExitWithError(err)
	}

	cl := cleaner.New(tgc, cfgStore, logger.Named("cleaner"))
	tgc.OnOwnMessage(cl.OnOwnMessage)
	tgc.OnDeletedMessages(cl.OnDeleted)

	svc.Attach(tgc, cfgStore, cl)
	svc.Start(ctx)

	<-ctx.Done()
	logger.Info("shutting down")
	svc.Stop()
}

func buildLogger(level string) *zap.Logger {
	lvl := zapcore.InfoLevel
	_ = lvl.UnmarshalText([]byte(level))
	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(lvl)
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	l, err := cfg.Build()
	if err != nil {
		l = zap.NewNop()
	}
	return l
}
