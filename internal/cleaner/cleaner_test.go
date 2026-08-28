package cleaner

import (
	"context"
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
	mu       sync.Mutex
	groups   map[int64]tgclient.Group
	scan     map[int64][]scanMsg
	deleted  map[int64][]int
	failIDs  map[int]bool
	pending  map[int64]tgclient.Group // revealed by ResolveGroups
	resolves int
	link     bool // when true, MessageLink returns a non-empty string
}

func newFakeTG() *fakeTG {
	return &fakeTG{
		groups:  map[int64]tgclient.Group{},
		scan:    map[int64][]scanMsg{},
		deleted: map[int64][]int{},
		failIDs: map[int]bool{},
		pending: map[int64]tgclient.Group{},
	}
}

func (f *fakeTG) ResolveGroups(context.Context) ([]tgclient.Group, error) {
	f.mu.Lock()
	f.resolves++
	for k, g := range f.pending {
		f.groups[k] = g
		delete(f.pending, k)
	}
	f.mu.Unlock()
	out := make([]tgclient.Group, 0, len(f.groups))
	for _, g := range f.groups {
		out = append(out, g)
	}
	return out, nil
}

func (f *fakeTG) Group(marked int64) (tgclient.Group, bool) {
	g, ok := f.groups[marked]
	return g, ok
}

func (f *fakeTG) ScanOwn(_ context.Context, g tgclient.Group, cb func(int, time.Time)) error {
	for _, m := range f.scan[g.MarkedID] {
		cb(m.id, m.date)
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
	c := New(f, store, nil)

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
	c := New(f, newStore(t), nil)

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
	c := New(f, store, nil)

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
	c := New(f, store, nil)

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
	c := New(f, store, nil)

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
	c := New(f, store, nil)

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

func TestOnDeletedCommonBox(t *testing.T) {
	basic := tgclient.MarkChat(500)      // > -channelIDShift
	channel := tgclient.MarkChannel(500) // < -channelIDShift

	f := newFakeTG()
	f.groups[basic] = tgclient.Group{MarkedID: basic}
	f.groups[channel] = tgclient.Group{MarkedID: channel, IsChannel: true}
	store := newStore(t)
	_ = store.SetTTL(basic, 60)
	_ = store.SetTTL(channel, 60)
	c := New(f, store, nil)

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
	c := New(f, newStore(t), nil)

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
	c := New(f, newStore(t), nil)

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
	c := New(f, store, nil)

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
	c := New(f, store, nil)

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
	c := New(f, store, nil)

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
	c := New(f, store, nil)

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
