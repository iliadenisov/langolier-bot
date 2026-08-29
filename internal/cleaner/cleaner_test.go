package cleaner

import (
	"context"
	"errors"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"langolier-bot/internal/chatcfg"
	"langolier-bot/internal/tgclient"
)

type scanMsg struct {
	id   int
	date time.Time
}

type fakeTG struct {
	mu         sync.Mutex
	groups     map[int64]tgclient.Group
	scan       map[int64][]scanMsg
	deleted    map[int64][]int
	failIDs    map[int]bool
	pending    map[int64]tgclient.Group // revealed by ResolveGroups
	absent     map[int64]bool           // in groups, but ResolveGroups hides it (account left)
	resolveErr error                    // when set, ResolveGroups fails with it
	resolves   int
	link       bool // when true, MessageLink returns a non-empty string

	// scanWindow, when > 0, caps how many not-yet-deleted messages ScanOwn
	// hands back per call, mimicking Telegram's ~10k from-self search ceiling:
	// deleting the visible layer reveals the one beneath it on the next call.
	scanWindow int
	scanCalls  int
	// scanGen, when set, synthesises the messages for a group on demand from
	// the call count, for histories too deep to enumerate up front. A scanGen
	// group is a raw script: the already-deleted filter and scanWindow do not
	// apply to it, so it can model a server that keeps re-serving the same ids.
	scanGen map[int64]func(call int) []scanMsg
}

func newFakeTG() *fakeTG {
	return &fakeTG{
		groups:  map[int64]tgclient.Group{},
		scan:    map[int64][]scanMsg{},
		deleted: map[int64][]int{},
		failIDs: map[int]bool{},
		pending: map[int64]tgclient.Group{},
		absent:  map[int64]bool{},
		scanGen: map[int64]func(int) []scanMsg{},
	}
}

// newCleaner builds a Cleaner for tests with the inter-pass drain delay
// disabled so drainChat runs without real sleeps.
func newCleaner(tg TG, store *chatcfg.Store) *Cleaner {
	c := New(tg, store, nil)
	c.drainPause = 0
	return c
}

func (f *fakeTG) ResolveGroups(context.Context) ([]tgclient.Group, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resolves++
	if f.resolveErr != nil {
		return nil, f.resolveErr
	}
	for k, g := range f.pending {
		f.groups[k] = g
		delete(f.pending, k)
	}
	out := make([]tgclient.Group, 0, len(f.groups))
	for _, g := range f.groups {
		if f.absent[g.MarkedID] {
			continue
		}
		out = append(out, g)
	}
	return out, nil
}

func (f *fakeTG) Group(marked int64) (tgclient.Group, bool) {
	g, ok := f.groups[marked]
	return g, ok
}

func (f *fakeTG) ScanOwn(_ context.Context, g tgclient.Group, cb func(int, time.Time)) error {
	f.mu.Lock()
	f.scanCalls++
	call := f.scanCalls
	gone := make(map[int]bool, len(f.deleted[g.MarkedID]))
	for _, id := range f.deleted[g.MarkedID] {
		gone[id] = true
	}
	msgs := f.scan[g.MarkedID]
	window := f.scanWindow
	raw := false
	if gen := f.scanGen[g.MarkedID]; gen != nil {
		msgs, window, raw = gen(call), 0, true
	}
	f.mu.Unlock()

	n := 0
	for _, m := range msgs {
		if !raw && gone[m.id] {
			continue
		}
		cb(m.id, m.date)
		if n++; window > 0 && n >= window {
			break
		}
	}
	return nil
}

func (f *fakeTG) Delete(_ context.Context, g tgclient.Group, ids []int) ([]int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var failed []int
	for _, id := range ids {
		if f.failIDs[id] {
			failed = append(failed, id)
			continue
		}
		f.deleted[g.MarkedID] = append(f.deleted[g.MarkedID], id)
	}
	return failed, nil
}

func (f *fakeTG) MessageLink(g tgclient.Group, id int) string {
	if !f.link {
		return ""
	}
	return "https://t.me/c/" + strconv.FormatInt(-g.MarkedID, 10) + "/" + strconv.Itoa(id)
}

func (f *fakeTG) deletedIDs(marked int64) []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int(nil), f.deleted[marked]...)
}

func newStore(t *testing.T) *chatcfg.Store {
	t.Helper()
	db, err := bolt.Open(filepath.Join(t.TempDir(), "t.db"), 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s, err := chatcfg.New(db)
	if err != nil {
		t.Fatalf("chatcfg.New: %v", err)
	}
	return s
}

func TestSweepDeletesExpired(t *testing.T) {
	const chat int64 = -100111

	f := newFakeTG()
	f.groups[chat] = tgclient.Group{MarkedID: chat, Title: "G", IsChannel: true}
	store := newStore(t)
	if err := store.SetTTL(chat, 1); err != nil {
		t.Fatal(err)
	}
	c := newCleaner(f, store)

	now := time.Now()
	c.OnOwnMessage(chat, 10, now.Add(-2*time.Minute), "old")
	c.OnOwnMessage(chat, 11, now, "fresh")

	c.sweep(context.Background())

	got := f.deletedIDs(chat)
	if len(got) != 1 || got[0] != 10 {
		t.Fatalf("deleted = %v, want [10]", got)
	}
	c.mu.Lock()
	_, has10 := c.index[chat][10]
	_, has11 := c.index[chat][11]
	c.mu.Unlock()
	if has10 {
		t.Error("expired message still indexed")
	}
	if !has11 {
		t.Error("fresh message dropped from index")
	}
}

func TestOnOwnMessageIgnoredWhenTTLZero(t *testing.T) {
	const chat int64 = -100222
	f := newFakeTG()
	f.groups[chat] = tgclient.Group{MarkedID: chat}
	c := newCleaner(f, newStore(t))

	c.OnOwnMessage(chat, 1, time.Now(), "hi")

	c.mu.Lock()
	n := len(c.index[chat])
	c.mu.Unlock()
	if n != 0 {
		t.Fatalf("indexed %d messages with TTL=0", n)
	}
}

func TestOnOwnMessageInstantPatternNotIndexed(t *testing.T) {
	const chat int64 = -100333
	f := newFakeTG()
	f.groups[chat] = tgclient.Group{MarkedID: chat}
	store := newStore(t)
	_ = store.SetTTL(chat, 60)
	_ = store.AddPattern(chat, chatcfg.Pattern{Value: "/q", Exact: true})
	c := newCleaner(f, store)

	c.OnOwnMessage(chat, 7, time.Now(), "/q")

	c.mu.Lock()
	_, indexed := c.index[chat][7]
	c.mu.Unlock()
	if indexed {
		t.Fatal("pattern-matched message was indexed instead of deleted")
	}
}

func TestOnDeletedRemovesFromIndex(t *testing.T) {
	const chat int64 = -100444
	f := newFakeTG()
	f.groups[chat] = tgclient.Group{MarkedID: chat}
	store := newStore(t)
	_ = store.SetTTL(chat, 60)
	c := newCleaner(f, store)

	c.OnOwnMessage(chat, 5, time.Now(), "keep")
	c.OnDeleted(chat, []int{5})

	c.mu.Lock()
	_, still := c.index[chat][5]
	c.mu.Unlock()
	if still {
		t.Fatal("message not removed from index on external delete")
	}
}

func TestEnableChatFillsIndex(t *testing.T) {
	const chat int64 = -100555
	f := newFakeTG()
	f.groups[chat] = tgclient.Group{MarkedID: chat, IsChannel: true}
	f.scan[chat] = []scanMsg{
		{id: 1, date: time.Now().Add(-time.Hour)},
		{id: 2, date: time.Now()},
	}
	store := newStore(t)
	_ = store.SetTTL(chat, 30)
	c := newCleaner(f, store)

	if err := c.EnableChat(context.Background(), chat); err != nil {
		t.Fatalf("EnableChat: %v", err)
	}
	c.mu.Lock()
	n := len(c.index[chat])
	c.mu.Unlock()
	if n != 2 {
		t.Fatalf("indexed %d messages, want 2", n)
	}
}

func TestPurgeNowDeletesOldOnly(t *testing.T) {
	const chat int64 = -100666
	f := newFakeTG()
	f.groups[chat] = tgclient.Group{MarkedID: chat, Title: "G", IsChannel: true}
	f.scan[chat] = []scanMsg{
		{id: 1, date: time.Now().Add(-48 * time.Hour)},
		{id: 2, date: time.Now()},
	}
	store := newStore(t)
	_ = store.SetTTL(chat, 60)
	c := newCleaner(f, store)

	rep, err := c.PurgeNow(context.Background(), chat)
	if err != nil {
		t.Fatalf("PurgeNow: %v", err)
	}
	if rep.Deleted != 1 || rep.Failed != 0 {
		t.Fatalf("report = %+v, want Deleted=1 Failed=0", rep)
	}
	if got := f.deletedIDs(chat); len(got) != 1 || got[0] != 1 {
		t.Fatalf("deleted = %v, want [1]", got)
	}
}

// descendingExpired builds n messages with ids n..1, all sent well before any
// sane TTL, newest (largest id) first — the order messages.search returns.
func descendingExpired(n int) []scanMsg {
	old := time.Now().Add(-48 * time.Hour)
	msgs := make([]scanMsg, 0, n)
	for id := n; id >= 1; id-- {
		msgs = append(msgs, scanMsg{id: id, date: old})
	}
	return msgs
}

func TestPurgeNowDrainsLayeredHistory(t *testing.T) {
	const chat int64 = -101001
	f := newFakeTG()
	f.groups[chat] = tgclient.Group{MarkedID: chat, Title: "G", IsChannel: true}
	f.scan[chat] = descendingExpired(250) // three search windows: 100 + 100 + 50
	f.scanWindow = 100
	f.link = true
	f.failIDs[1], f.failIDs[2], f.failIDs[3] = true, true, true // stuck in the last window

	store := newStore(t)
	_ = store.SetTTL(chat, 60)
	c := newCleaner(f, store)

	rep, err := c.PurgeNow(context.Background(), chat)
	if err != nil {
		t.Fatalf("PurgeNow: %v", err)
	}
	// 247 deletable messages cleared across three delete passes; the fourth
	// pass sees only the three stuck ids, deletes nothing, and stops the loop.
	if rep.Deleted != 247 {
		t.Errorf("rep.Deleted = %d, want 247", rep.Deleted)
	}
	if rep.Failed == 0 {
		t.Errorf("rep.Failed = 0, want the stuck ids counted")
	}
	if len(rep.Links) > purgeLinkLimit {
		t.Errorf("rep.Links = %d, want <= %d", len(rep.Links), purgeLinkLimit)
	}
	if got := len(f.deletedIDs(chat)); got != 247 {
		t.Errorf("actually deleted %d, want 247", got)
	}
	if f.scanCalls != 4 {
		t.Errorf("ScanOwn called %d times, want 4 (3 draining + 1 empty)", f.scanCalls)
	}
	// The three ids that could not be deleted stay indexed for the live sweep
	// to retry; everything else is gone.
	c.mu.Lock()
	left := len(c.index[chat])
	c.mu.Unlock()
	if left != 3 {
		t.Errorf("index holds %d messages, want 3 (the stuck ids)", left)
	}
}

func TestPurgeNowStopsAtPassCeiling(t *testing.T) {
	const chat int64 = -101002
	f := newFakeTG()
	f.groups[chat] = tgclient.Group{MarkedID: chat, Title: "G", IsChannel: true}
	// A bottomless history: every scan yields one fresh, ever-older message, so
	// the loop always makes progress and can only be stopped by the ceiling.
	old := time.Now().Add(-48 * time.Hour)
	f.scanGen[chat] = func(call int) []scanMsg {
		return []scanMsg{{id: 1_000_000 - call, date: old}}
	}
	store := newStore(t)
	_ = store.SetTTL(chat, 60)
	c := newCleaner(f, store)

	rep, err := c.PurgeNow(context.Background(), chat)
	if err != nil {
		t.Fatalf("PurgeNow: %v", err)
	}
	if rep.Deleted != maxDrainPasses {
		t.Errorf("rep.Deleted = %d, want %d (one per pass)", rep.Deleted, maxDrainPasses)
	}
	if f.scanCalls != maxDrainPasses {
		t.Errorf("ScanOwn called %d times, want %d", f.scanCalls, maxDrainPasses)
	}
}

func TestPurgeNowStopsWhenWindowNotReceding(t *testing.T) {
	const chat int64 = -101003
	f := newFakeTG()
	f.groups[chat] = tgclient.Group{MarkedID: chat, Title: "G", IsChannel: true}
	old := time.Now().Add(-48 * time.Hour)
	// The search floor never drops: call 1 reaches id 7, every later call stops
	// at id 8. Passes keep deleting (ids are re-served), so the loop can only
	// end via the "window not receding" guard.
	f.scanGen[chat] = func(call int) []scanMsg {
		if call == 1 {
			return []scanMsg{{id: 10, date: old}, {id: 9, date: old}, {id: 8, date: old}, {id: 7, date: old}}
		}
		return []scanMsg{{id: 10, date: old}, {id: 9, date: old}, {id: 8, date: old}}
	}
	store := newStore(t)
	_ = store.SetTTL(chat, 60)
	c := newCleaner(f, store)

	rep, err := c.PurgeNow(context.Background(), chat)
	if err != nil {
		t.Fatalf("PurgeNow: %v", err)
	}
	if f.scanCalls != 2 {
		t.Errorf("ScanOwn called %d times, want 2 (guard trips on the second pass)", f.scanCalls)
	}
	if rep.Deleted != 7 { // 4 on pass one, 3 on pass two
		t.Errorf("rep.Deleted = %d, want 7", rep.Deleted)
	}
}

func TestPurgeNowNoTTLIsNoop(t *testing.T) {
	const chat int64 = -101004
	f := newFakeTG()
	f.groups[chat] = tgclient.Group{MarkedID: chat, Title: "G", IsChannel: true}
	f.scan[chat] = descendingExpired(10)
	c := newCleaner(f, newStore(t)) // TTL unset

	rep, err := c.PurgeNow(context.Background(), chat)
	if err != nil {
		t.Fatalf("PurgeNow: %v", err)
	}
	if rep.Deleted != 0 || f.scanCalls != 0 {
		t.Fatalf("rep = %+v, scanCalls = %d, want no work", rep, f.scanCalls)
	}
	if rep.Title != "G" {
		t.Errorf("rep.Title = %q, want the group title", rep.Title)
	}
}

func TestOnDeletedCommonBox(t *testing.T) {
	basic := tgclient.MarkChat(500)      // > -channelIDShift
	channel := tgclient.MarkChannel(500) // < -channelIDShift

	f := newFakeTG()
	f.groups[basic] = tgclient.Group{MarkedID: basic}
	f.groups[channel] = tgclient.Group{MarkedID: channel, IsChannel: true}
	store := newStore(t)
	_ = store.SetTTL(basic, 60)
	_ = store.SetTTL(channel, 60)
	c := newCleaner(f, store)

	now := time.Now()
	c.OnOwnMessage(basic, 7, now, "a")
	c.OnOwnMessage(channel, 7, now, "b")

	// Common-box delete update carries no chat id; ids there are account-global
	// across non-channel chats only.
	c.OnDeleted(0, []int{7})

	c.mu.Lock()
	_, inBasic := c.index[basic][7]
	_, inChannel := c.index[channel][7]
	c.mu.Unlock()
	if inBasic {
		t.Error("basic-group message not dropped on common-box delete")
	}
	if !inChannel {
		t.Error("channel message wrongly dropped on common-box delete")
	}
}

func TestDeleteReportLinkCap(t *testing.T) {
	const chat int64 = -100777
	f := newFakeTG()
	f.link = true
	f.groups[chat] = tgclient.Group{MarkedID: chat, Title: "G", IsChannel: true}
	for i := 1; i <= 15; i++ {
		f.failIDs[i] = true
	}
	c := newCleaner(f, newStore(t))

	ids := make([]int, 15)
	for i := range ids {
		ids[i] = i + 1
	}
	rep := c.deleteIDs(context.Background(), chat, ids)
	if rep.Deleted != 0 || rep.Failed != 15 {
		t.Fatalf("report = %+v", rep)
	}
	if len(rep.Links) != purgeLinkLimit {
		t.Fatalf("links = %d, want capped at %d", len(rep.Links), purgeLinkLimit)
	}
}

func TestDeleteReportPartialFailure(t *testing.T) {
	const chat int64 = -100888
	f := newFakeTG()
	f.link = true
	f.groups[chat] = tgclient.Group{MarkedID: chat, Title: "G", IsChannel: true}
	f.failIDs[2] = true
	f.failIDs[4] = true
	c := newCleaner(f, newStore(t))

	rep := c.deleteIDs(context.Background(), chat, []int{1, 2, 3, 4, 5})
	if rep.Deleted != 3 || rep.Failed != 2 || len(rep.Links) != 2 {
		t.Fatalf("report = %+v", rep)
	}
	if got := f.deletedIDs(chat); len(got) != 3 {
		t.Fatalf("actually deleted = %v", got)
	}
}

func TestStats(t *testing.T) {
	a := int64(-100001)
	b := int64(-100002)
	f := newFakeTG()
	f.groups[a] = tgclient.Group{MarkedID: a, Title: "Alpha", IsChannel: true}
	f.groups[b] = tgclient.Group{MarkedID: b, Title: "Bravo", IsChannel: true}
	store := newStore(t)
	_ = store.SetTTL(a, 60)
	_ = store.AddPattern(a, chatcfg.Pattern{Value: "/q", Exact: true})
	_ = store.SetTTL(b, 30)
	c := newCleaner(f, store)

	now := time.Now()
	c.OnOwnMessage(a, 1, now, "x")
	c.OnOwnMessage(a, 2, now, "y")

	stats := c.Stats()
	if len(stats) != 2 {
		t.Fatalf("stats len = %d", len(stats))
	}
	if stats[0].Title != "Alpha" || stats[1].Title != "Bravo" {
		t.Fatalf("not sorted by title: %+v", stats)
	}
	if stats[0].TTLMinutes != 60 || stats[0].Patterns != 1 || stats[0].Indexed != 2 {
		t.Errorf("Alpha stat = %+v", stats[0])
	}
	if stats[1].TTLMinutes != 30 || stats[1].Patterns != 0 || stats[1].Indexed != 0 {
		t.Errorf("Bravo stat = %+v", stats[1])
	}
}

func TestReset(t *testing.T) {
	const chat int64 = -100999
	f := newFakeTG()
	f.groups[chat] = tgclient.Group{MarkedID: chat, Title: "G", IsChannel: true}
	store := newStore(t)
	_ = store.SetTTL(chat, 60)
	c := newCleaner(f, store)

	c.OnOwnMessage(chat, 1, time.Now(), "x")
	c.deleteIDs(context.Background(), chat, []int{1})

	c.Reset()

	c.mu.Lock()
	nIdx := len(c.index)
	nDel := c.deleted[chat]
	c.mu.Unlock()
	if nIdx != 0 || nDel != 0 {
		t.Fatalf("after Reset: index=%d deleted=%d", nIdx, nDel)
	}
}

func TestSweepMultiChatDifferentTTL(t *testing.T) {
	fast := int64(-100010)
	slow := int64(-100011)
	f := newFakeTG()
	f.groups[fast] = tgclient.Group{MarkedID: fast, IsChannel: true}
	f.groups[slow] = tgclient.Group{MarkedID: slow, IsChannel: true}
	store := newStore(t)
	_ = store.SetTTL(fast, 1)
	_ = store.SetTTL(slow, 10)
	c := newCleaner(f, store)

	sent := time.Now().Add(-5 * time.Minute)
	c.OnOwnMessage(fast, 1, sent, "x")
	c.OnOwnMessage(slow, 1, sent, "y")

	c.sweep(context.Background())

	if got := f.deletedIDs(fast); len(got) != 1 {
		t.Errorf("fast chat: expected msg deleted, got %v", got)
	}
	if got := f.deletedIDs(slow); len(got) != 0 {
		t.Errorf("slow chat: message deleted too early, got %v", got)
	}
}

func TestEnableChatResolvesWhenMissing(t *testing.T) {
	const chat int64 = -100012
	f := newFakeTG()
	// not in f.groups yet; ResolveGroups will reveal it
	f.pending[chat] = tgclient.Group{MarkedID: chat, IsChannel: true}
	f.scan[chat] = []scanMsg{{id: 1, date: time.Now()}, {id: 2, date: time.Now()}}
	store := newStore(t)
	_ = store.SetTTL(chat, 30)
	c := newCleaner(f, store)

	if err := c.EnableChat(context.Background(), chat); err != nil {
		t.Fatalf("EnableChat: %v", err)
	}
	if f.resolves != 1 {
		t.Errorf("ResolveGroups called %d times, want 1", f.resolves)
	}
	c.mu.Lock()
	n := len(c.index[chat])
	c.mu.Unlock()
	if n != 2 {
		t.Errorf("indexed %d, want 2", n)
	}
}

func TestReconcileStopsCleanupOnLeave(t *testing.T) {
	const chat int64 = -102001
	f := newFakeTG()
	f.groups[chat] = tgclient.Group{MarkedID: chat, Title: "Gone", IsChannel: true}
	f.scan[chat] = []scanMsg{{id: 1, date: time.Now()}}
	store := newStore(t)
	_ = store.SetTTL(chat, 60)
	c := newCleaner(f, store)
	if err := c.EnableChat(context.Background(), chat); err != nil { // now maintained
		t.Fatalf("EnableChat: %v", err)
	}

	f.absent[chat] = true // the account has left / been removed

	res, err := c.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(res.Lost) != 1 || res.Lost[0] != "Gone" {
		t.Fatalf("Lost = %v, want [Gone]", res.Lost)
	}
	if len(res.Rejoined) != 0 {
		t.Fatalf("Rejoined = %v, want none", res.Rejoined)
	}
	c.mu.Lock()
	_, indexed := c.index[chat]
	c.mu.Unlock()
	if indexed {
		t.Error("index not dropped for the left chat")
	}
	if store.Get(chat).TTLMinutes != 60 {
		t.Error("TTL config wrongly cleared on leave")
	}
}

func TestReconcileDrainsOnRejoin(t *testing.T) {
	const chat int64 = -102002
	f := newFakeTG()
	f.groups[chat] = tgclient.Group{MarkedID: chat, Title: "Back", IsChannel: true}
	store := newStore(t)
	_ = store.SetTTL(chat, 60)
	c := newCleaner(f, store)

	var drained []int64
	c.startDrain = func(m int64) { drained = append(drained, m) }

	res, err := c.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(res.Rejoined) != 1 || res.Rejoined[0] != "Back" {
		t.Fatalf("Rejoined = %v, want [Back]", res.Rejoined)
	}
	if len(res.Lost) != 0 {
		t.Fatalf("Lost = %v, want none", res.Lost)
	}
	if len(drained) != 1 || drained[0] != chat {
		t.Fatalf("startDrain calls = %v, want [%d]", drained, chat)
	}
}

func TestReconcileNoopWhenActiveOrUnconfigured(t *testing.T) {
	active := int64(-102003)
	unconfigured := int64(-102004)
	f := newFakeTG()
	f.groups[active] = tgclient.Group{MarkedID: active, Title: "Active", IsChannel: true}
	f.groups[unconfigured] = tgclient.Group{MarkedID: unconfigured, Title: "Off", IsChannel: true}
	f.scan[active] = []scanMsg{{id: 1, date: time.Now()}}
	store := newStore(t)
	_ = store.SetTTL(active, 60)
	c := newCleaner(f, store)
	c.startDrain = func(int64) { t.Fatal("startDrain called for an already-active chat") }
	if err := c.EnableChat(context.Background(), active); err != nil { // active is maintained
		t.Fatalf("EnableChat: %v", err)
	}

	res, err := c.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(res.Lost) != 0 || len(res.Rejoined) != 0 {
		t.Fatalf("res = %+v, want no transitions", res)
	}
}

func TestReconcilePropagatesResolveError(t *testing.T) {
	f := newFakeTG()
	f.resolveErr = errors.New("boom")
	c := newCleaner(f, newStore(t))

	if _, err := c.Reconcile(context.Background()); err == nil {
		t.Fatal("Reconcile returned nil, want the ResolveGroups error")
	}
}

func TestReconcileNoRejoinForIdleActiveChat(t *testing.T) {
	const chat int64 = -102005
	f := newFakeTG()
	f.groups[chat] = tgclient.Group{MarkedID: chat, Title: "Idle", IsChannel: true}
	f.scan[chat] = nil // the account is a member but has no own messages
	store := newStore(t)
	_ = store.SetTTL(chat, 60)
	c := newCleaner(f, store)
	c.startDrain = func(int64) { t.Fatal("startDrain called for a chat we never left") }

	// Simulate the steady state: startup scan ran, found nothing to index.
	if err := c.EnableChat(context.Background(), chat); err != nil {
		t.Fatalf("EnableChat: %v", err)
	}

	res, err := c.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(res.Lost) != 0 || len(res.Rejoined) != 0 {
		t.Fatalf("res = %+v, want no transitions for an idle chat we are still in", res)
	}
}

func TestReconcileDetectsLeaveOfIdleChat(t *testing.T) {
	const chat int64 = -102006
	f := newFakeTG()
	f.groups[chat] = tgclient.Group{MarkedID: chat, Title: "IdleGone", IsChannel: true}
	f.scan[chat] = nil // maintained but nothing indexed
	store := newStore(t)
	_ = store.SetTTL(chat, 60)
	c := newCleaner(f, store)
	if err := c.EnableChat(context.Background(), chat); err != nil {
		t.Fatalf("EnableChat: %v", err)
	}

	f.absent[chat] = true
	res, err := c.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(res.Lost) != 1 || res.Lost[0] != "IdleGone" {
		t.Fatalf("Lost = %v, want [IdleGone] (an idle chat leaving still counts)", res.Lost)
	}
	if c.isActive(chat) {
		t.Error("chat still marked active after leave")
	}
}

func TestReconcileRejoinAfterLoss(t *testing.T) {
	const chat int64 = -102007
	f := newFakeTG()
	f.groups[chat] = tgclient.Group{MarkedID: chat, Title: "Cycle", IsChannel: true}
	f.scan[chat] = []scanMsg{{id: 1, date: time.Now()}}
	store := newStore(t)
	_ = store.SetTTL(chat, 60)
	c := newCleaner(f, store)
	var drained []int64
	c.startDrain = func(m int64) { drained = append(drained, m) }

	if err := c.EnableChat(context.Background(), chat); err != nil {
		t.Fatalf("EnableChat: %v", err)
	}

	// Leave.
	f.absent[chat] = true
	res, err := c.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile (leave): %v", err)
	}
	if len(res.Lost) != 1 || len(res.Rejoined) != 0 {
		t.Fatalf("leave: res = %+v", res)
	}

	// Rejoin.
	f.absent[chat] = false
	res, err = c.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile (rejoin): %v", err)
	}
	if len(res.Rejoined) != 1 || res.Rejoined[0] != "Cycle" || len(res.Lost) != 0 {
		t.Fatalf("rejoin: res = %+v", res)
	}
	if len(drained) != 1 || drained[0] != chat {
		t.Fatalf("startDrain calls = %v, want [%d]", drained, chat)
	}
	if !c.isActive(chat) {
		t.Error("chat not re-marked active after rejoin")
	}
}
