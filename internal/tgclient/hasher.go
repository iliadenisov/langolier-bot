package tgclient

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/gotd/td/telegram/updates"
	bolt "go.etcd.io/bbolt"
)

// hashBucket stores channel and user access hashes so updates.Manager can
// recover gaps for peers seen in previous runs.
var hashBucket = []byte("access_hashes")

// hashStore is a bbolt-backed implementation of both updates.ChannelAccessHasher
// and updates.UserAccessHasher.
type hashStore struct {
	db *bolt.DB
}

var (
	_ updates.ChannelAccessHasher = (*hashStore)(nil)
	_ updates.UserAccessHasher    = (*hashStore)(nil)
)

func newHashStore(db *bolt.DB) (*hashStore, error) {
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(hashBucket)
		return err
	}); err != nil {
		return nil, err
	}
	return &hashStore{db: db}, nil
}

func (s *hashStore) get(key string) (int64, bool, error) {
	var (
		hash  int64
		found bool
	)
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(hashBucket).Get([]byte(key))
		if v == nil {
			return nil
		}
		found = true
		hash = int64(binary.LittleEndian.Uint64(v))
		return nil
	})
	return hash, found, err
}

func (s *hashStore) set(key string, hash int64) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(hash))
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(hashBucket).Put([]byte(key), buf)
	})
}

func (s *hashStore) SetChannelAccessHash(_ context.Context, userID, channelID, accessHash int64) error {
	return s.set(fmt.Sprintf("c:%d:%d", userID, channelID), accessHash)
}

func (s *hashStore) GetChannelAccessHash(_ context.Context, userID, channelID int64) (int64, bool, error) {
	return s.get(fmt.Sprintf("c:%d:%d", userID, channelID))
}

func (s *hashStore) SetUserAccessHash(_ context.Context, userID, targetUserID, accessHash int64) error {
	return s.set(fmt.Sprintf("u:%d:%d", userID, targetUserID), accessHash)
}

func (s *hashStore) GetUserAccessHash(_ context.Context, userID, targetUserID int64) (int64, bool, error) {
	return s.get(fmt.Sprintf("u:%d:%d", userID, targetUserID))
}
