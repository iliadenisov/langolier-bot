// Package config reads runtime configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
)

// EnvString returns the value of the environment variable named key. It returns
// an error if the variable is unset or empty.
func EnvString(key string) (string, error) {
	v, ok := os.LookupEnv(key)
	if !ok || len(v) == 0 {
		return "", fmt.Errorf("variable %q not set or empty", key)
	}
	return v, nil
}

// EnvStringDefault returns the value of the environment variable named key, or
// def when the variable is unset or empty.
func EnvStringDefault(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && len(v) > 0 {
		return v
	}
	return def
}

// MustEnvString returns the value of the environment variable named key and
// terminates the process when it is unset or empty.
func MustEnvString(key string) string {
	v, err := EnvString(key)
	if err != nil {
		ExitWithError(err)
	}
	return v
}

// EnvInt returns the value of the environment variable named key parsed as a
// base-10 integer.
func EnvInt(key string) (int, error) {
	v, err := EnvString(key)
	if err != nil {
		return 0, err
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("variable %q is not an integer: %q", key, v)
	}
	return i, nil
}

// MustEnvInt returns the value of the environment variable named key parsed as a
// base-10 integer and terminates the process on any error.
func MustEnvInt(key string) int {
	v, err := EnvInt(key)
	if err != nil {
		ExitWithError(err)
	}
	return v
}

// EnvInt64 returns the value of the environment variable named key parsed as a
// base-10 64-bit integer.
func EnvInt64(key string) (int64, error) {
	v, err := EnvString(key)
	if err != nil {
		return 0, err
	}
	i, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("variable %q is not an integer: %q", key, v)
	}
	return i, nil
}

// MustEnvInt64 returns the value of the environment variable named key parsed as
// a base-10 64-bit integer and terminates the process on any error.
func MustEnvInt64(key string) int64 {
	v, err := EnvInt64(key)
	if err != nil {
		ExitWithError(err)
	}
	return v
}

// ExitWithError prints err to standard error and terminates the process with a
// non-zero status.
func ExitWithError(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
