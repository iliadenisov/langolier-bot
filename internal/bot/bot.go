// Package bot implements the operator-facing service bot: it relays the MTProto
// authorization prompts to the owner and exposes an inline-keyboard UI for
// configuring per-chat message TTL and instant-delete patterns.
package bot

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
	"go.uber.org/zap"

	"langolier-bot/internal/chatcfg"
	"langolier-bot/internal/cleaner"
	"langolier-bot/internal/tgclient"
)

// groupsPerPage is the inline-keyboard page size for the chat picker.
const groupsPerPage = 8

type inputKind int

const (
	inputNone inputKind = iota
	inputPhone
	inputCode
	inputPassword
	inputTTL
	inputPattern
)

// Bot is the service bot.
type Bot struct {
	b       *bot.Bot
	log     *zap.Logger
	ownerID int64

	tgc *tgclient.Client
	cfg *chatcfg.Store
	cl  *cleaner.Cleaner

	baseCtx context.Context

	mu        sync.Mutex
	running   bool
	pending   inputKind
	convoChat int64
	stageText string
	authReply chan string
}

// New creates the service bot. Call Attach then Start.
func New(token string, ownerID int64, log *zap.Logger) (*Bot, error) {
	if log == nil {
		log = zap.NewNop()
	}
	bt := &Bot{log: log, ownerID: ownerID}
	b, err := bot.New(token, bot.WithDefaultHandler(bt.onUpdate))
	if err != nil {
		return nil, err
	}
	bt.b = b
	return bt, nil
}

// Attach wires the MTProto client, config store and cleaner into the bot.
func (bt *Bot) Attach(tgc *tgclient.Client, cfg *chatcfg.Store, cl *cleaner.Cleaner) {
	bt.tgc = tgc
	bt.cfg = cfg
	bt.cl = cl
}

// Start runs the bot until ctx is cancelled.
func (bt *Bot) Start(ctx context.Context) {
	bt.baseCtx = ctx
	go bt.b.Start(ctx)
	bt.send("Service bot ready. Send /start to launch the user client.")
}

// Stop stops the user client if it is running.
func (bt *Bot) Stop() {
	if bt.tgc != nil {
		_ = bt.tgc.Stop()
	}
}

// --- tgclient.Relay -------------------------------------------------------------

// AskPhone requests the account phone number from the owner.
func (bt *Bot) AskPhone(ctx context.Context) (string, error) {
	return bt.ask(ctx, inputPhone,
		"Enter the account phone number (international format, e.g. +15551234567):",
		"+15551234567")
}

// AskCode requests the login code from the owner. Telegram invalidates a code
// as soon as it sees the plain number in a message, so the owner is asked to
// break it up (spaces, dashes, words — anything); every non-digit is stripped
// before the code is used.
func (bt *Bot) AskCode(ctx context.Context) (string, error) {
	code, err := bt.ask(ctx, inputCode,
		"Enter the login code with the digits broken up so Telegram does not void it — "+
			"e.g. `1 2 3 4 5` or `1-2-3-4-5`. Everything except the digits is ignored.",
		"1 2 3 4 5")
	if err != nil {
		return "", err
	}
	return digitsOnly(code), nil
}

// digitsOnly returns s with every non-ASCII-digit rune removed.
func digitsOnly(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return r
		}
		return -1
	}, s)
}

// AskPassword requests the 2FA password from the owner. A wrong password is not
// fatal: the client simply asks again with this same prompt, so the operator
// keeps replying until it is accepted (no need to restart with /start).
func (bt *Bot) AskPassword(ctx context.Context, hint string) (string, error) {
	q := "Enter the 2FA password."
	if hint != "" {
		q += " Hint: " + hint + "."
	}
	q += " If it is wrong I will just ask again — no need to /start over."
	return bt.ask(ctx, inputPassword, q, "password")
}

// ask sets the pending-input state, prompts the owner and blocks until the reply
// arrives or ctx is cancelled. A non-empty placeholder makes the prompt a
// ForceReply so the client focuses the input box.
func (bt *Bot) ask(ctx context.Context, kind inputKind, prompt, placeholder string) (string, error) {
	bt.mu.Lock()
	bt.pending = kind
	bt.authReply = make(chan string, 1)
	ch := bt.authReply
	bt.mu.Unlock()

	if placeholder != "" {
		bt.sendForceReply(prompt, placeholder)
	} else {
		bt.send(prompt)
	}

	select {
	case <-ctx.Done():
		bt.clearPending()
		return "", ctx.Err()
	case v := <-ch:
		bt.clearPending()
		return v, nil
	}
}

func (bt *Bot) clearPending() {
	bt.mu.Lock()
	bt.pending = inputNone
	bt.convoChat = 0
	bt.stageText = ""
	bt.authReply = nil
	bt.mu.Unlock()
}

// --- update routing ----------------------------------------------------------

func (bt *Bot) onUpdate(ctx context.Context, _ *bot.Bot, update *models.Update) {
	switch {
	case update.CallbackQuery != nil:
		if update.CallbackQuery.From.ID != bt.ownerID {
			return
		}
		bt.onCallback(ctx, update.CallbackQuery)
	case update.Message != nil && update.Message.From != nil:
		if update.Message.From.ID != bt.ownerID {
			return
		}
		bt.onMessage(ctx, update.Message)
	}
}

func (bt *Bot) onMessage(ctx context.Context, msg *models.Message) {
	text := strings.TrimSpace(msg.Text)

	bt.mu.Lock()
	pending := bt.pending
	convo := bt.convoChat
	ch := bt.authReply
	bt.mu.Unlock()

	switch pending {
	case inputPhone, inputCode, inputPassword:
		if ch != nil {
			ch <- text
		}
		// Keep the login code and 2FA password out of the chat history.
		if pending == inputCode || pending == inputPassword {
			_, _ = bt.b.DeleteMessage(ctx, &bot.DeleteMessageParams{
				ChatID:    msg.Chat.ID,
				MessageID: msg.ID,
			})
		}
		return
	case inputTTL:
		n, err := strconv.Atoi(text)
		if err != nil || n < 0 {
			bt.send("Send a non-negative integer number of minutes (0 disables TTL).")
			return
		}
		bt.clearPending()
		bt.applyTTL(convo, n)
		return
	case inputPattern:
		if text == "" {
			bt.send("Pattern must not be empty.")
			return
		}
		bt.mu.Lock()
		bt.stageText = text
		bt.mu.Unlock()
		bt.sendMarkup("Match type for "+strconv.Quote(text)+"?", kbd(
			row(btn("Exact", "patkind:exact"), btn("Prefix", "patkind:prefix")),
			row(btn("Cancel", "chat:"+strconv.FormatInt(convo, 10))),
		))
		return
	}

	switch text {
	case "/start":
		bt.cmdStart()
	case "/stop":
		bt.cmdStop()
	case "/config":
		bt.cmdConfig(ctx, 0, editTarget{})
	case "/status":
		bt.cmdStatus()
	default:
		bt.send("Commands: /start /stop /config /status")
	}
}

// callback is a parsed inline-keyboard callback payload.
type callback struct {
	kind   string // page | chat | ttl | ttlclear | pat | patadd | patkind | patdel | purge | off
	marked int64  // target chat (marked id) for most kinds
	idx    int    // pattern index for patdel
	page   int    // page number for page
	arg    string // "exact" / "prefix" for patkind
}

// parseCallback decodes a callback data string. It returns ok=false for any
// payload that is unknown or malformed.
func parseCallback(data string) (callback, bool) {
	prefix, rest, ok := strings.Cut(data, ":")
	if !ok {
		return callback{}, false
	}
	switch prefix {
	case "cfg":
		n, ok := strings.CutPrefix(rest, "page:")
		if !ok {
			return callback{}, false
		}
		p, err := strconv.Atoi(n)
		if err != nil {
			return callback{}, false
		}
		return callback{kind: "page", page: p}, true
	case "chat", "ttl", "ttlclear", "pat", "patadd", "purge", "off":
		m, err := strconv.ParseInt(rest, 10, 64)
		if err != nil {
			return callback{}, false
		}
		return callback{kind: prefix, marked: m}, true
	case "patkind":
		if rest != "exact" && rest != "prefix" {
			return callback{}, false
		}
		return callback{kind: "patkind", arg: rest}, true
	case "patdel":
		a, b, ok := strings.Cut(rest, ":")
		if !ok {
			return callback{}, false
		}
		m, err1 := strconv.ParseInt(a, 10, 64)
		idx, err2 := strconv.Atoi(b)
		if err1 != nil || err2 != nil {
			return callback{}, false
		}
		return callback{kind: "patdel", marked: m, idx: idx}, true
	default:
		return callback{}, false
	}
}

func (bt *Bot) onCallback(ctx context.Context, cq *models.CallbackQuery) {
	_, _ = bt.b.AnswerCallbackQuery(ctx, &bot.AnswerCallbackQueryParams{CallbackQueryID: cq.ID})

	var tgt editTarget
	if cq.Message.Message != nil {
		tgt = editTarget{cq.Message.Message.Chat.ID, cq.Message.Message.ID}
	}

	a, ok := parseCallback(cq.Data)
	if !ok {
		return
	}
	switch a.kind {
	case "page":
		bt.cmdConfig(ctx, a.page, tgt)
	case "chat":
		bt.showChatMenu(a.marked, tgt)
	case "ttl":
		bt.mu.Lock()
		bt.pending = inputTTL
		bt.convoChat = a.marked
		bt.mu.Unlock()
		bt.send("Send the message TTL in minutes for this chat (0 disables it):")
	case "ttlclear":
		bt.applyTTL(a.marked, 0)
	case "pat":
		bt.showPatternMenu(a.marked, tgt)
	case "patadd":
		bt.mu.Lock()
		bt.pending = inputPattern
		bt.convoChat = a.marked
		bt.mu.Unlock()
		bt.send("Send the pattern text:")
	case "patkind":
		bt.finishPattern(a.arg)
	case "patdel":
		if err := bt.cfg.RemovePattern(a.marked, a.idx); err != nil {
			bt.send("Remove failed: " + err.Error())
			return
		}
		bt.showPatternMenu(a.marked, tgt)
	case "purge":
		bt.runPurge(a.marked)
	case "off":
		if err := bt.cfg.Disable(a.marked); err != nil {
			bt.send("Disable failed: " + err.Error())
			return
		}
		bt.cl.DisableChat(a.marked)
		bt.send("Chat cleanup disabled.")
	}
}

// --- commands --------------------------------------------------------------

func (bt *Bot) cmdStart() {
	bt.mu.Lock()
	if bt.running {
		bt.mu.Unlock()
		bt.send("User client is already running.")
		return
	}
	bt.mu.Unlock()

	bt.send("Starting the user client. You may be asked for the phone number, login code and 2FA password.")
	go func() {
		if err := bt.tgc.Start(bt.baseCtx, bt.onReady); err != nil {
			bt.send("Start failed: " + err.Error() + "\nSend /start to try again.")
			return
		}
		bt.mu.Lock()
		bt.running = true
		bt.mu.Unlock()
		bt.send("User client authorized and running.")
	}()
}

func (bt *Bot) onReady(ctx context.Context) {
	bt.cl.Run(ctx)
	if _, err := bt.tgc.ResolveGroups(ctx); err != nil {
		bt.log.Warn("resolve groups on ready", zap.Error(err))
	}
	for marked, cfg := range bt.cfg.List() {
		if cfg.TTLMinutes <= 0 {
			continue
		}
		m := marked
		go func() {
			if err := bt.cl.EnableChat(ctx, m); err != nil {
				bt.log.Warn("enable chat on ready", zap.Int64("chat", m), zap.Error(err))
			}
		}()
	}
}

func (bt *Bot) cmdStop() {
	bt.mu.Lock()
	running := bt.running
	bt.mu.Unlock()
	if !running {
		bt.send("User client is not running.")
		return
	}
	if err := bt.tgc.Stop(); err != nil {
		bt.send("Stop error: " + err.Error())
	}
	bt.cl.Reset()
	bt.mu.Lock()
	bt.running = false
	bt.mu.Unlock()
	bt.send("User client stopped. Send /start to launch again.")
}

func (bt *Bot) cmdConfig(ctx context.Context, page int, target editTarget) {
	bt.mu.Lock()
	running := bt.running
	bt.mu.Unlock()
	if !running {
		bt.send("Run /start first.")
		return
	}

	groups, err := bt.tgc.ResolveGroups(ctx)
	if err != nil {
		bt.send("Failed to list chats: " + err.Error())
		return
	}
	if len(groups) == 0 {
		bt.send("No eligible group chats found.")
		return
	}

	pages := (len(groups) + groupsPerPage - 1) / groupsPerPage
	if page < 0 {
		page = 0
	}
	if page >= pages {
		page = pages - 1
	}
	start := page * groupsPerPage
	end := min(start+groupsPerPage, len(groups))

	cfgs := bt.cfg.List()
	var rows [][]models.InlineKeyboardButton
	for _, g := range groups[start:end] {
		label := g.Title
		if cfgs[g.MarkedID].Configured() {
			label = "• " + label
		}
		rows = append(rows, row(btn(label, "chat:"+strconv.FormatInt(g.MarkedID, 10))))
	}
	var nav []models.InlineKeyboardButton
	if page > 0 {
		nav = append(nav, btn("‹ Prev", "cfg:page:"+strconv.Itoa(page-1)))
	}
	if page < pages-1 {
		nav = append(nav, btn("Next ›", "cfg:page:"+strconv.Itoa(page+1)))
	}
	if len(nav) > 0 {
		rows = append(rows, nav)
	}

	text := fmt.Sprintf("Select a group to configure (page %d/%d):", page+1, pages)
	bt.render(target, text, kbd(rows...))
}

func (bt *Bot) showChatMenu(marked int64, target editTarget) {
	cfg := bt.cfg.Get(marked)
	title := bt.groupTitle(marked)
	text := fmt.Sprintf("%s\nTTL: %s\nInstant-delete patterns: %d",
		title, ttlText(cfg.TTLMinutes), len(cfg.Patterns))
	bt.render(target, text, kbd(
		row(btn("Set TTL", "ttl:"+strconv.FormatInt(marked, 10)), btn("Clear TTL", "ttlclear:"+strconv.FormatInt(marked, 10))),
		row(btn("Patterns", "pat:"+strconv.FormatInt(marked, 10))),
		row(btn("Purge now", "purge:"+strconv.FormatInt(marked, 10))),
		row(btn("Disable chat", "off:"+strconv.FormatInt(marked, 10))),
		row(btn("‹ Back", "cfg:page:0")),
	))
}

func (bt *Bot) showPatternMenu(marked int64, target editTarget) {
	cfg := bt.cfg.Get(marked)
	var rows [][]models.InlineKeyboardButton
	for i, p := range cfg.Patterns {
		kind := "prefix"
		if p.Exact {
			kind = "exact"
		}
		rows = append(rows, row(btn(
			fmt.Sprintf("❌ [%s] %s", kind, p.Value),
			fmt.Sprintf("patdel:%d:%d", marked, i),
		)))
	}
	rows = append(rows, row(btn("Add pattern", "patadd:"+strconv.FormatInt(marked, 10))))
	rows = append(rows, row(btn("‹ Back", "chat:"+strconv.FormatInt(marked, 10))))
	bt.render(target, bt.groupTitle(marked)+"\nInstant-delete patterns:", kbd(rows...))
}

func (bt *Bot) finishPattern(kind string) {
	bt.mu.Lock()
	marked := bt.convoChat
	value := bt.stageText
	bt.mu.Unlock()
	if marked == 0 || value == "" {
		return
	}
	bt.clearPending()
	if err := bt.cfg.AddPattern(marked, chatcfg.Pattern{Value: value, Exact: kind == "exact"}); err != nil {
		bt.send("Add pattern failed: " + err.Error())
		return
	}
	bt.send(fmt.Sprintf("Pattern added (%s): %s", kind, value))
}

func (bt *Bot) applyTTL(marked int64, minutes int) {
	// A first activation (no TTL before) drains the whole reachable history in
	// passes; changing an already-positive TTL only re-seeds the index.
	firstEnable := minutes > 0 && bt.cfg.Get(marked).TTLMinutes <= 0
	if err := bt.cfg.SetTTL(marked, minutes); err != nil {
		bt.send("Set TTL failed: " + err.Error())
		return
	}
	if minutes > 0 {
		m := marked
		if firstEnable {
			bt.send(fmt.Sprintf("TTL set to %d min for %s. Cleaning history in passes, this can take a while…", minutes, bt.groupTitle(marked)))
			go func() {
				rep, err := bt.cl.PurgeNow(bt.baseCtx, m)
				if err != nil {
					bt.send("History cleanup failed: " + err.Error())
					return
				}
				bt.send(formatReport("History cleanup", rep))
			}()
			return
		}
		bt.send(fmt.Sprintf("TTL set to %d min for %s. Scanning history…", minutes, bt.groupTitle(marked)))
		go func() {
			if err := bt.cl.EnableChat(bt.baseCtx, m); err != nil {
				bt.send("History scan failed: " + err.Error())
				return
			}
			bt.send("History scan complete for " + bt.groupTitle(m) + ".")
		}()
		return
	}
	bt.cl.DisableChat(marked)
	bt.send("TTL cleared for " + bt.groupTitle(marked) + ".")
}

func (bt *Bot) runPurge(marked int64) {
	bt.send("Purging " + bt.groupTitle(marked) + "…")
	go func() {
		rep, err := bt.cl.PurgeNow(bt.baseCtx, marked)
		if err != nil {
			bt.send("Purge error: " + err.Error())
			return
		}
		bt.send(formatReport("Purge", rep))
	}()
}

func (bt *Bot) cmdStatus() {
	stats := bt.cl.Stats()
	if len(stats) == 0 {
		bt.send("No configured chats.")
		return
	}
	sort.Slice(stats, func(i, j int) bool { return stats[i].Title < stats[j].Title })
	var b strings.Builder
	b.WriteString("Status:\n")
	for _, s := range stats {
		title := s.Title
		if title == "" {
			title = strconv.FormatInt(s.MarkedID, 10)
		}
		fmt.Fprintf(&b, "• %s — TTL %s, patterns %d, indexed %d, deleted %d\n",
			title, ttlText(s.TTLMinutes), s.Patterns, s.Indexed, s.Deleted)
	}
	bt.send(b.String())
}

// --- helpers ---------------------------------------------------------------

type editTarget struct {
	chatID int64
	msgID  int
}

func (bt *Bot) render(t editTarget, text string, markup *models.InlineKeyboardMarkup) {
	if t.msgID != 0 {
		_, err := bt.b.EditMessageText(bt.baseCtx, &bot.EditMessageTextParams{
			ChatID:      t.chatID,
			MessageID:   t.msgID,
			Text:        text,
			ReplyMarkup: markup,
		})
		if err == nil {
			return
		}
	}
	bt.sendMarkup(text, markup)
}

func (bt *Bot) send(text string) {
	if _, err := bt.b.SendMessage(bt.baseCtx, &bot.SendMessageParams{ChatID: bt.ownerID, Text: text}); err != nil {
		bt.log.Warn("send message", zap.Error(err))
	}
}

func (bt *Bot) sendMarkup(text string, markup *models.InlineKeyboardMarkup) {
	if _, err := bt.b.SendMessage(bt.baseCtx, &bot.SendMessageParams{
		ChatID:      bt.ownerID,
		Text:        text,
		ReplyMarkup: markup,
	}); err != nil {
		bt.log.Warn("send markup", zap.Error(err))
	}
}

// sendForceReply prompts the owner with a ForceReply keyboard so the client
// focuses the input box.
func (bt *Bot) sendForceReply(text, placeholder string) {
	if _, err := bt.b.SendMessage(bt.baseCtx, &bot.SendMessageParams{
		ChatID: bt.ownerID,
		Text:   text,
		ReplyMarkup: &models.ForceReply{
			ForceReply:            true,
			InputFieldPlaceholder: placeholder,
		},
	}); err != nil {
		bt.log.Warn("send force-reply", zap.Error(err))
	}
}

func (bt *Bot) groupTitle(marked int64) string {
	if g, ok := bt.tgc.Group(marked); ok && g.Title != "" {
		return g.Title
	}
	return strconv.FormatInt(marked, 10)
}

func ttlText(minutes int) string {
	if minutes <= 0 {
		return "off"
	}
	return strconv.Itoa(minutes) + " min"
}

func formatReport(kind string, rep cleaner.Report) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s of %s: deleted %d, failed %d", kind, rep.Title, rep.Deleted, rep.Failed)
	for _, l := range rep.Links {
		b.WriteString("\n" + l)
	}
	return b.String()
}

func kbd(rows ...[]models.InlineKeyboardButton) *models.InlineKeyboardMarkup {
	return &models.InlineKeyboardMarkup{InlineKeyboard: rows}
}

func row(b ...models.InlineKeyboardButton) []models.InlineKeyboardButton { return b }

func btn(text, data string) models.InlineKeyboardButton {
	return models.InlineKeyboardButton{Text: text, CallbackData: data}
}
