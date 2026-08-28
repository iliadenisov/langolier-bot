package chatcfg

import (
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func TestConfigMatches(t *testing.T) {
	cfg := Config{Patterns: []Pattern{
		{Value: "+++", Exact: true},
		{Value: "/q", Exact: false},
	}}

	cases := []struct {
		text string
		want bool
	}{
		{"+++", true},
		{" +++ ", true},   // trimmed
		{"++++", false},   // exact, no match
		{"/q", true},      // prefix
		{"/q rest", true}, // prefix
		{"q", false},
		{"", false},
		{"   ", false},
	}
	for _, c := range cases {
		if got := cfg.Matches(c.text); got != c.want {
			t.Errorf("Matches(%q) = %v, want %v", c.text, got, c.want)
		}
	}
}

func TestConfigMatchesEmptyPatternIgnored(t *testing.T) {
	cfg := Config{Patterns: []Pattern{{Value: "", Exact: false}}}
	if cfg.Matches("anything") {
		t.Fatal("empty pattern must never match")
	}
}

func openStore(t *testing.T) (*Store, *bolt.DB) {
	t.Helper()
	db, err := bolt.Open(filepath.Join(t.TempDir(), "t.db"), 0o600, nil)
	if err != nil {
		t.Fatalf("open bbolt: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s, err := New(db)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s, db
}

func TestStoreRoundTrip(t *testing.T) {
	s, db := openStore(t)

	const chat int64 = -1001234567890

	if err := s.SetTTL(chat, 60); err != nil {
		t.Fatalf("SetTTL: %v", err)
	}
	if err := s.AddPattern(chat, Pattern{Value: " /q ", Exact: true}); err != nil {
		t.Fatalf("AddPattern: %v", err)
	}
	if err := s.AddPattern(chat, Pattern{Value: "spam", Exact: false}); err != nil {
		t.Fatalf("AddPattern: %v", err)
	}

	got := s.Get(chat)
	if got.TTLMinutes != 60 {
		t.Errorf("TTLMinutes = %d, want 60", got.TTLMinutes)
	}
	if len(got.Patterns) != 2 || got.Patterns[0].Value != "/q" || !got.Patterns[0].Exact {
		t.Fatalf("patterns = %+v", got.Patterns)
	}
	if !got.Configured() {
		t.Error("Configured() = false")
	}

	// Mutating the returned copy must not affect the store.
	got.Patterns[0].Value = "mutated"
	if s.Get(chat).Patterns[0].Value != "/q" {
		t.Error("Get returned a shared slice")
	}

	if err := s.RemovePattern(chat, 0); err != nil {
		t.Fatalf("RemovePattern: %v", err)
	}
	if p := s.Get(chat).Patterns; len(p) != 1 || p[0].Value != "spam" {
		t.Fatalf("after remove: %+v", p)
	}

	// Reload from disk.
	s2, err := New(db)
	if err != nil {
		t.Fatalf("reload New: %v", err)
	}
	if r := s2.Get(chat); r.TTLMinutes != 60 || len(r.Patterns) != 1 {
		t.Fatalf("reloaded config = %+v", r)
	}

	if err := s.Disable(chat); err != nil {
		t.Fatalf("Disable: %v", err)
	}
	if s.Get(chat).Configured() {
		t.Error("chat still configured after Disable")
	}
	if len(s.List()) != 0 {
		t.Errorf("List not empty after Disable: %v", s.List())
	}
}

func TestStoreEmptyPatternRejected(t *testing.T) {
	s, _ := openStore(t)
	if err := s.AddPattern(1, Pattern{Value: "   "}); err == nil {
		t.Fatal("expected error for empty pattern")
	}
}
