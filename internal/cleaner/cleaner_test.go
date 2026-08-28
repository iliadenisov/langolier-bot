package cleaner

import (
	"context"
	"path/filepath"
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
	mu      sync.Mutex
	groups  map[int64]tgclient.Group
	scan    map[int64][]scanMsg
	deleted map[int64][]int
	failIDs map[int]bool
}

func newFakeTG() *fakeTG {
	return &fakeTG{
		groups:  map[int64]tgclient.Group{},
		scan:    map[int64][]scanMsg{},
		deleted: map[int64][]int{},
		failIDs: map[int]bool{},
	}
}

func (f *fakeTG) ResolveGroups(context.Context) ([]tgclient.Group, error) {
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

func (f *fakeTG) MessageLink(tgclient.Group, int) string { return "" }

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
