package main

import "testing"

func TestShortVersion(t *testing.T) {
	cases := map[string]string{
		"":         "",
		"dev":      "dev",
		"abcdefg":  "abcdefg", // exactly 7
		"abcdefgh": "abcdefg", // 8 -> 7
		"d71b43edac7af9b5ee3e2e181a3d9098ef141b34": "d71b43e",
	}
	for in, want := range cases {
		if got := shortVersion(in); got != want {
			t.Errorf("shortVersion(%q) = %q, want %q", in, got, want)
		}
	}
}
