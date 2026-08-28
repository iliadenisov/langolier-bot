package config

import "testing"

func TestEnvString(t *testing.T) {
	if _, err := EnvString("LANGOLIER_TEST_UNSET"); err == nil {
		t.Fatal("expected error for unset variable")
	}
	t.Setenv("LANGOLIER_TEST_X", "value")
	v, err := EnvString("LANGOLIER_TEST_X")
	if err != nil || v != "value" {
		t.Fatalf("EnvString = %q, %v", v, err)
	}
	t.Setenv("LANGOLIER_TEST_X", "")
	if _, err := EnvString("LANGOLIER_TEST_X"); err == nil {
		t.Fatal("expected error for empty variable")
	}
}

func TestEnvStringDefault(t *testing.T) {
	if got := EnvStringDefault("LANGOLIER_TEST_UNSET", "def"); got != "def" {
		t.Errorf("unset: got %q, want def", got)
	}
	t.Setenv("LANGOLIER_TEST_Y", "set")
	if got := EnvStringDefault("LANGOLIER_TEST_Y", "def"); got != "set" {
		t.Errorf("set: got %q, want set", got)
	}
	t.Setenv("LANGOLIER_TEST_Y", "")
	if got := EnvStringDefault("LANGOLIER_TEST_Y", "def"); got != "def" {
		t.Errorf("empty: got %q, want def", got)
	}
}

func TestEnvInt(t *testing.T) {
	if _, err := EnvInt("LANGOLIER_TEST_UNSET"); err == nil {
		t.Fatal("expected error for unset")
	}
	t.Setenv("LANGOLIER_TEST_N", "42")
	if n, err := EnvInt("LANGOLIER_TEST_N"); err != nil || n != 42 {
		t.Fatalf("EnvInt = %d, %v", n, err)
	}
	t.Setenv("LANGOLIER_TEST_N", "notanint")
	if _, err := EnvInt("LANGOLIER_TEST_N"); err == nil {
		t.Fatal("expected error for non-integer")
	}
}

func TestEnvInt64(t *testing.T) {
	t.Setenv("LANGOLIER_TEST_B", "9000000000")
	if n, err := EnvInt64("LANGOLIER_TEST_B"); err != nil || n != 9_000_000_000 {
		t.Fatalf("EnvInt64 = %d, %v", n, err)
	}
	t.Setenv("LANGOLIER_TEST_B", "x")
	if _, err := EnvInt64("LANGOLIER_TEST_B"); err == nil {
		t.Fatal("expected error for non-integer")
	}
	if _, err := EnvInt64("LANGOLIER_TEST_UNSET"); err == nil {
		t.Fatal("expected error for unset")
	}
}
