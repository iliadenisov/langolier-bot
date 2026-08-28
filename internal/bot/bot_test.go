package bot

import (
	"testing"

	"langolier-bot/internal/cleaner"
)

func TestDigitsOnly(t *testing.T) {
	cases := map[string]string{
		"12345":       "12345",
		"1 2 3 4 5":   "12345",
		"1-2-3-4-5":   "12345",
		"code: 12 34": "1234",
		" 5 5 5 ":     "555",
		"":            "",
		"no digits":   "",
		"０１2":         "2", // full-width digits are not ASCII, dropped
	}
	for in, want := range cases {
		if got := digitsOnly(in); got != want {
			t.Errorf("digitsOnly(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestTTLText(t *testing.T) {
	for in, want := range map[int]string{-5: "off", 0: "off", 1: "1 min", 720: "720 min"} {
		if got := ttlText(in); got != want {
			t.Errorf("ttlText(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestFormatReport(t *testing.T) {
	got := formatReport("Purge", cleaner.Report{Title: "Group", Deleted: 3, Failed: 0})
	if got != "Purge of Group: deleted 3, failed 0" {
		t.Errorf("no links: %q", got)
	}

	got = formatReport("Sweep", cleaner.Report{
		Title:   "G",
		Deleted: 1,
		Failed:  2,
		Links:   []string{"https://t.me/c/1/2", "https://t.me/c/1/3"},
	})
	want := "Sweep of G: deleted 1, failed 2\nhttps://t.me/c/1/2\nhttps://t.me/c/1/3"
	if got != want {
		t.Errorf("with links:\n got %q\nwant %q", got, want)
	}
}

func TestParseCallback(t *testing.T) {
	ok := func(data, kind string) callback {
		t.Helper()
		c, valid := parseCallback(data)
		if !valid {
			t.Fatalf("parseCallback(%q) not ok", data)
		}
		if c.kind != kind {
			t.Fatalf("parseCallback(%q).kind = %q, want %q", data, c.kind, kind)
		}
		return c
	}

	if c := ok("cfg:page:3", "page"); c.page != 3 {
		t.Errorf("page = %d", c.page)
	}
	if c := ok("chat:-1001234567890", "chat"); c.marked != -1001234567890 {
		t.Errorf("marked = %d", c.marked)
	}
	for _, k := range []string{"ttl", "ttlclear", "pat", "patadd", "purge", "off"} {
		if c := ok(k+":-42", k); c.marked != -42 {
			t.Errorf("%s marked = %d", k, c.marked)
		}
	}
	if c := ok("patkind:exact", "patkind"); c.arg != "exact" {
		t.Errorf("patkind arg = %q", c.arg)
	}
	ok("patkind:prefix", "patkind")
	if c := ok("patdel:-7:2", "patdel"); c.marked != -7 || c.idx != 2 {
		t.Errorf("patdel = %+v", c)
	}

	for _, bad := range []string{
		"", "noselon", "cfg:page:x", "cfg:other", "chat:notint",
		"patkind:weird", "patdel:-7", "patdel:x:2", "patdel:-7:y", "unknown:1",
	} {
		if _, valid := parseCallback(bad); valid {
			t.Errorf("parseCallback(%q) unexpectedly ok", bad)
		}
	}
}
