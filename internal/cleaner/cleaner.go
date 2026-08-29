// Package cleaner keeps an in-memory index of the account's own messages per
// group chat and deletes them once they exceed the chat's configured TTL. It
// also performs instant deletion of messages matching a chat's patterns and
// on-demand purges triggered from the service bot.
package cleaner

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"langolier-bot/internal/chatcfg"
	"langolier-bot/internal/tgclient"
)

// errUnknownGroup means the marked chat id is not a group the account belongs to.
var errUnknownGroup = errors.New("cleaner: unknown group")

// sweepInterval is how often the index is checked for expired messages. One
// second gives near-minute TTL precision without any network traffic per tick.
const sweepInterval = time.Second

// purgeLinkLimit caps how many failed-message deeplinks a purge report carries.
const purgeLinkLimit = 10

// maxDrainPasses is the absolute cap on drainChat iterations for one chat, a
// backstop so the loop can never run forever even if every other termination
// check fails to fire.
const maxDrainPasses = 50

// drainPassDelay is the pause between drainChat passes, on top of the per-page
// pacing inside ScanOwn, so the RPC burst stays gentle on the account.
const drainPassDelay = 5 * time.Second

// TG is the subset of the MTProto client the cleaner needs.
type TG interface {
	ResolveGroups(ctx context.Context) ([]tgclient.Group, error)
	Group(markedID int64) (tgclient.Group, bool)
	ScanOwn(ctx context.Context, g tgclient.Group, cb func(msgID int, date time.Time)) error
	Delete(ctx context.Context, g tgclient.Group, ids []int) (failed []int, err error)
	MessageLink(g tgclient.Group, msgID int) string
}

// Report summarises a purge or a sweep batch for the operator.
type Report struct {
	Title   string
	Deleted int
	Failed  int
	Links   []string
}

// ChatStat is a per-chat status line for the /status command.
type ChatStat struct {
	MarkedID   int64
	Title      string
	TTLMinutes int
	Patterns   int
	Indexed    int
	Deleted    int
}

// Cleaner owns the message index and the sweep loop.
type Cleaner struct {
	tg  TG
	cfg *chatcfg.Store
	log *zap.Logger

	mu      sync.Mutex
	index   map[int64]map[int]time.Time // markedID -> msgID -> send time
	deleted map[int64]int               // markedID -> messages deleted this session

	delMu sync.Mutex // serialises delete RPC bursts

	loopMu  sync.Mutex
	looping bool

	// drainPause is the wait between drainChat passes; a field so tests can
	// zero it. New sets it to drainPassDelay.
	drainPause time.Duration
}

// New creates a Cleaner.
func New(tg TG, cfg *chatcfg.Store, log *zap.Logger) *Cleaner {
	if log == nil {
		log = zap.NewNop()
	}
	return &Cleaner{
		tg:         tg,
		cfg:        cfg,
		log:        log,
		index:      make(map[int64]map[int]time.Time),
		deleted:    make(map[int64]int),
		drainPause: drainPassDelay,
	}
}

// Run starts the sweep loop for ctx. It is a no-op if a loop is already running;
// the loop exits when its ctx is cancelled, after which Run may be called again
// with a fresh ctx.
func (c *Cleaner) Run(ctx context.Context) {
	c.loopMu.Lock()
	if c.looping {
		c.loopMu.Unlock()
		return
	}
	c.looping = true
	c.loopMu.Unlock()
	go c.loop(ctx)
}

func (c *Cleaner) loop(ctx context.Context) {
	defer func() {
		c.loopMu.Lock()
		c.looping = false
		c.loopMu.Unlock()
	}()
	t := time.NewTicker(sweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.sweep(ctx)
		}
	}
}

func (c *Cleaner) sweep(ctx context.Context) {
	now := time.Now()
	type job struct {
		marked int64
		ids    []int
	}
	var jobs []job

	c.mu.Lock()
	for marked, msgs := range c.index {
		ttl := c.cfg.Get(marked).TTL()
		if ttl <= 0 {
			continue
		}
		var expired []int
		for id, sent := range msgs {
			if now.Sub(sent) >= ttl {
				expired = append(expired, id)
			}
		}
		if len(expired) > 0 {
			jobs = append(jobs, job{marked: marked, ids: expired})
		}
	}
	c.mu.Unlock()

	for _, j := range jobs {
		if ctx.Err() != nil {
			return
		}
		c.deleteIDs(ctx, j.marked, j.ids)
	}
}

// OnOwnMessage records a new outgoing message, or deletes it immediately when it
// matches an instant-delete pattern of the chat.
func (c *Cleaner) OnOwnMessage(markedID int64, msgID int, date time.Time, text string) {
	cfg := c.cfg.Get(markedID)
	if cfg.Matches(text) {
		go c.deleteIDs(context.Background(), markedID, []int{msgID})
		return
	}
	if cfg.TTLMinutes <= 0 {
		return
	}
	c.indexMessage(markedID, msgID, date)
}

// indexMessage records msgID under markedID with its send time, creating the
// per-chat map on first use.
func (c *Cleaner) indexMessage(markedID int64, msgID int, date time.Time) {
	c.mu.Lock()
	m := c.index[markedID]
	if m == nil {
		m = make(map[int]time.Time)
		c.index[markedID] = m
	}
	m[msgID] = date
	c.mu.Unlock()
}

// OnDeleted drops messages that were deleted elsewhere from the index. A zero
// markedID means the common message box, whose ids are account-global, so every
// non-channel chat is scanned.
func (c *Cleaner) OnDeleted(markedID int64, ids []int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if markedID != 0 {
		c.removeLocked(markedID, ids)
		return
	}
	for marked := range c.index {
		if marked < -tgChannelShift {
			continue // supergroup/channel ids are chat-scoped
		}
		c.removeLocked(marked, ids)
	}
}

// tgChannelShift mirrors tgclient.channelIDShift for the marked-id range check.
const tgChannelShift = 1_000_000_000_000

func (c *Cleaner) removeLocked(markedID int64, ids []int) {
	m := c.index[markedID]
	if m == nil {
		return
	}
	for _, id := range ids {
		delete(m, id)
	}
	if len(m) == 0 {
		delete(c.index, markedID)
	}
}

// resolveGroup returns the cached group for markedID, refreshing the group
// cache once if it is not yet known.
func (c *Cleaner) resolveGroup(ctx context.Context, markedID int64) (tgclient.Group, error) {
	if g, ok := c.tg.Group(markedID); ok {
		return g, nil
	}
	if _, err := c.tg.ResolveGroups(ctx); err != nil {
		return tgclient.Group{}, err
	}
	if g, ok := c.tg.Group(markedID); ok {
		return g, nil
	}
	return tgclient.Group{}, errUnknownGroup
}

// EnableChat performs the paced startup scan for a chat with TTL>0, filling the
// index with the account's existing messages.
func (c *Cleaner) EnableChat(ctx context.Context, markedID int64) error {
	g, err := c.resolveGroup(ctx, markedID)
	if err != nil {
		return err
	}
	c.log.Info("scanning own messages", zap.Int64("chat", markedID), zap.String("title", g.Title))
	return c.tg.ScanOwn(ctx, g, func(msgID int, date time.Time) {
		c.indexMessage(markedID, msgID, date)
	})
}

// DisableChat forgets a chat's index.
func (c *Cleaner) DisableChat(markedID int64) {
	c.mu.Lock()
	delete(c.index, markedID)
	c.mu.Unlock()
}

// Reset drops the whole in-memory index and per-session counters. Call it when
// the user client stops so a later start re-scans from a clean slate.
func (c *Cleaner) Reset() {
	c.mu.Lock()
	c.index = make(map[int64]map[int]time.Time)
	c.deleted = make(map[int64]int)
	c.mu.Unlock()
}

// PurgeNow deletes every reachable own message in the chat older than its TTL,
// walking the whole history in paced passes, and returns a combined summary.
func (c *Cleaner) PurgeNow(ctx context.Context, markedID int64) (Report, error) {
	return c.drainChat(ctx, markedID)
}

// drainChat repeatedly scans the chat's own messages and deletes those older
// than its TTL, one messages.search window at a time.
//
// Telegram's from-self search only exposes the newest ~10k messages that
// currently exist, so a single pass can delete at most that layer; each further
// pass then sees the layer underneath. The loop stops when a pass finds nothing
// left to delete, when the search window stops receding (nothing older is
// reachable), when a pass deletes nothing despite finding candidates
// (persistent delete failures), or after maxDrainPasses as a hard backstop.
//
// Every message seen is added to the in-memory index so ongoing TTL sweeping
// keeps working afterwards. The returned Report aggregates all passes.
func (c *Cleaner) drainChat(ctx context.Context, markedID int64) (Report, error) {
	g, err := c.resolveGroup(ctx, markedID)
	if err != nil {
		return Report{}, err
	}
	rep := Report{Title: g.Title}
	ttl := c.cfg.Get(markedID).TTL()
	if ttl <= 0 {
		return rep, nil
	}

	prevOldest := 0
	for pass := 1; pass <= maxDrainPasses; pass++ {
		if err := ctx.Err(); err != nil {
			return rep, err
		}

		now := time.Now()
		var (
			expired []int
			oldest  int
		)
		if err := c.tg.ScanOwn(ctx, g, func(msgID int, date time.Time) {
			c.indexMessage(markedID, msgID, date)
			if oldest == 0 || msgID < oldest {
				oldest = msgID
			}
			if now.Sub(date) >= ttl {
				expired = append(expired, msgID)
			}
		}); err != nil {
			return rep, err
		}

		if len(expired) == 0 {
			c.log.Info("history drain complete",
				zap.Int64("chat", markedID), zap.String("title", g.Title),
				zap.Int("passes", pass-1), zap.Int("deleted_total", rep.Deleted))
			return rep, nil
		}

		pr := c.deleteIDs(ctx, markedID, expired)
		mergeReport(&rep, pr)
		c.log.Info("history drain pass",
			zap.Int64("chat", markedID), zap.String("title", g.Title),
			zap.Int("pass", pass), zap.Int("expired_found", len(expired)),
			zap.Int("deleted", pr.Deleted), zap.Int("failed", pr.Failed),
			zap.Int("oldest_id", oldest), zap.Int("deleted_total", rep.Deleted))

		if pr.Deleted == 0 {
			c.log.Warn("history drain stopped: pass deleted nothing",
				zap.Int64("chat", markedID), zap.String("title", g.Title),
				zap.Int("pass", pass), zap.Int("failed", pr.Failed))
			return rep, nil
		}
		if prevOldest != 0 && oldest >= prevOldest {
			c.log.Warn("history drain stopped: search window not receding",
				zap.Int64("chat", markedID), zap.String("title", g.Title),
				zap.Int("pass", pass), zap.Int("oldest_id", oldest),
				zap.Int("prev_oldest_id", prevOldest))
			return rep, nil
		}
		prevOldest = oldest

		if c.drainPause > 0 {
			select {
			case <-ctx.Done():
				return rep, ctx.Err()
			case <-time.After(c.drainPause):
			}
		}
	}

	c.log.Warn("history drain stopped: pass ceiling reached",
		zap.Int64("chat", markedID), zap.String("title", g.Title),
		zap.Int("passes", maxDrainPasses), zap.Int("deleted_total", rep.Deleted))
	return rep, nil
}

// mergeReport folds src into dst, keeping at most purgeLinkLimit links.
func mergeReport(dst *Report, src Report) {
	dst.Deleted += src.Deleted
	dst.Failed += src.Failed
	for _, l := range src.Links {
		if len(dst.Links) >= purgeLinkLimit {
			break
		}
		dst.Links = append(dst.Links, l)
	}
}

// deleteIDs deletes ids from the chat, updates counters and the index, and
// returns a Report.
func (c *Cleaner) deleteIDs(ctx context.Context, markedID int64, ids []int) Report {
	g, ok := c.tg.Group(markedID)
	rep := Report{}
	if ok {
		rep.Title = g.Title
	}
	if len(ids) == 0 || !ok {
		return rep
	}

	c.delMu.Lock()
	failed, err := c.tg.Delete(ctx, g, ids)
	c.delMu.Unlock()
	if err != nil {
		c.log.Warn("delete error", zap.Int64("chat", markedID), zap.Error(err))
	}

	failedSet := make(map[int]struct{}, len(failed))
	for _, id := range failed {
		failedSet[id] = struct{}{}
	}

	var okIDs []int
	for _, id := range ids {
		if _, bad := failedSet[id]; !bad {
			okIDs = append(okIDs, id)
		}
	}

	c.mu.Lock()
	c.removeLocked(markedID, okIDs)
	c.deleted[markedID] += len(okIDs)
	c.mu.Unlock()

	rep.Deleted = len(okIDs)
	rep.Failed = len(failed)
	sort.Ints(failed)
	for _, id := range failed {
		if len(rep.Links) >= purgeLinkLimit {
			break
		}
		if link := c.tg.MessageLink(g, id); link != "" {
			rep.Links = append(rep.Links, link)
		}
	}
	return rep
}

// Stats returns a status line for every chat that is either configured or
// currently indexed.
func (c *Cleaner) Stats() []ChatStat {
	cfgs := c.cfg.List()

	c.mu.Lock()
	seen := make(map[int64]struct{})
	var out []ChatStat
	add := func(marked int64) {
		if _, done := seen[marked]; done {
			return
		}
		seen[marked] = struct{}{}
		cfg := cfgs[marked]
		st := ChatStat{
			MarkedID:   marked,
			TTLMinutes: cfg.TTLMinutes,
			Patterns:   len(cfg.Patterns),
			Indexed:    len(c.index[marked]),
			Deleted:    c.deleted[marked],
		}
		if g, ok := c.tg.Group(marked); ok {
			st.Title = g.Title
		}
		out = append(out, st)
	}
	for marked := range cfgs {
		add(marked)
	}
	for marked := range c.index {
		add(marked)
	}
	c.mu.Unlock()

	sort.Slice(out, func(i, j int) bool { return out[i].Title < out[j].Title })
	return out
}
