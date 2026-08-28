// Package chatcfg stores the per-chat cleanup configuration (message TTL and
// instant-delete patterns) in a bbolt bucket and keeps an in-memory copy for
// lock-free reads on the hot path.
package chatcfg

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// bucketName is the bbolt bucket holding one JSON-encoded Config per chat,
// keyed by the decimal marked chat id.
var bucketName = []byte("chatcfg")

// Pattern is a single instant-delete rule for a chat. When Exact is true the
// message text must equal Value; otherwise the text must start with Value.
type Pattern struct {
	Value string `json:"value"`
	Exact bool   `json:"exact"`
}

// Config is the cleanup configuration for one chat.
type Config struct {
	// TTLMinutes is the age, in minutes, after which the account's own messages
	// are deleted. Zero disables TTL cleanup.
	TTLMinutes int `json:"ttl_minutes"`
	// Patterns lists instant-delete rules applied to the account's own text
	// messages as they arrive.
	Patterns []Pattern `json:"patterns"`
}

// TTL returns the configured message age threshold as a duration.
func (c Config) TTL() time.Duration {
	return time.Duration(c.TTLMinutes) * time.Minute
}

// Configured reports whether the chat has any active cleanup behaviour.
func (c Config) Configured() bool {
	return c.TTLMinutes > 0 || len(c.Patterns) > 0
}

// Matches reports whether text (after trimming surrounding whitespace) satisfies
// any instant-delete pattern of the chat.
func (c Config) Matches(text string) bool {
	t := strings.TrimSpace(text)
	if t == "" {
		return false
	}
	for _, p := range c.Patterns {
		if p.Value == "" {
			continue
		}
		if p.Exact {
			if t == p.Value {
				return true
			}
			continue
		}
		if strings.HasPrefix(t, p.Value) {
			return true
		}
	}
	return false
}

// Store is a bbolt-backed collection of per-chat Config values. It is safe for
// concurrent use.
type Store struct {
	db    *bolt.DB
	mu    sync.RWMutex
	cache map[int64]Config
}

// New opens the chat-config store over db, creating its bucket and loading the
// existing entries into memory.
func New(db *bolt.DB) (*Store, error) {
	s := &Store{db: db, cache: make(map[int64]Config)}
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		return b.ForEach(func(k, v []byte) error {
			id, err := strconv.ParseInt(string(k), 10, 64)
			if err != nil {
				return fmt.Errorf("chatcfg: bad key %q: %w", k, err)
			}
			var cfg Config
			if err := json.Unmarshal(v, &cfg); err != nil {
				return fmt.Errorf("chatcfg: bad value for %d: %w", id, err)
			}
			s.cache[id] = cfg
			return nil
		})
	}); err != nil {
		return nil, err
	}
	return s, nil
}

// Get returns the configuration for chatID, or the zero Config when the chat is
// not configured.
func (s *Store) Get(chatID int64) Config {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cache[chatID].clone()
}

// List returns a copy of every configured chat keyed by marked chat id.
func (s *Store) List() map[int64]Config {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[int64]Config, len(s.cache))
	for id, cfg := range s.cache {
		out[id] = cfg.clone()
	}
	return out
}

// SetTTL sets the TTL threshold for chatID to minutes. A value of zero clears
// the TTL but keeps any instant-delete patterns.
func (s *Store) SetTTL(chatID int64, minutes int) error {
	if minutes < 0 {
		minutes = 0
	}
	return s.mutate(chatID, func(c *Config) { c.TTLMinutes = minutes })
}

// AddPattern appends p to the instant-delete patterns of chatID.
func (s *Store) AddPattern(chatID int64, p Pattern) error {
	p.Value = strings.TrimSpace(p.Value)
	if p.Value == "" {
		return fmt.Errorf("chatcfg: empty pattern")
	}
	return s.mutate(chatID, func(c *Config) { c.Patterns = append(c.Patterns, p) })
}

// RemovePattern deletes the instant-delete pattern at index i for chatID.
func (s *Store) RemovePattern(chatID int64, i int) error {
	return s.mutate(chatID, func(c *Config) {
		if i >= 0 && i < len(c.Patterns) {
			c.Patterns = append(c.Patterns[:i], c.Patterns[i+1:]...)
		}
	})
}

// Disable removes all configuration for chatID.
func (s *Store) Disable(chatID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Delete(key(chatID))
	}); err != nil {
		return err
	}
	delete(s.cache, chatID)
	return nil
}

func (s *Store) mutate(chatID int64, fn func(*Config)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg := s.cache[chatID].clone()
	fn(&cfg)
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Put(key(chatID), data)
	}); err != nil {
		return err
	}
	s.cache[chatID] = cfg
	return nil
}

func (c Config) clone() Config {
	if len(c.Patterns) == 0 {
		return Config{TTLMinutes: c.TTLMinutes}
	}
	p := make([]Pattern, len(c.Patterns))
	copy(p, c.Patterns)
	return Config{TTLMinutes: c.TTLMinutes, Patterns: p}
}

func key(chatID int64) []byte {
	return []byte(strconv.FormatInt(chatID, 10))
}
