package onebot

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/JFSAS/wechat-runtime/internal/store"
)

// This writer is test-only: it produces the previous released snapshot format
// so migration tests cannot accidentally validate a new-format source instead.
type legacyFixture struct {
	directory string
	data      durableState
	aead      cipher.AEAD
}

func openLegacyFixture(t *testing.T, directory string) *legacyFixture {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	if err := store.Atomic(filepath.Join(directory, "onebot.key"), key); err != nil {
		t.Fatal(err)
	}
	block, _ := aes.NewCipher(key)
	aead, _ := cipher.NewGCM(block)
	return &legacyFixture{directory: directory, aead: aead, data: durableState{Version: 1, Next: 10000, IDs: map[string]int64{}, Refs: map[string]reference{}, Cursors: map[string]uint64{}}}
}
func (s *legacyFixture) saveLocked() error {
	raw, err := json.Marshal(s.data)
	if err != nil {
		return err
	}
	if len(raw)+s.aead.NonceSize()+s.aead.Overhead() > maxStateBytes {
		return errors.New("legacy_fixture_too_large")
	}
	nonce := make([]byte, s.aead.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return err
	}
	return store.Atomic(filepath.Join(s.directory, "onebot.state"), append(nonce, s.aead.Seal(nil, nonce, raw, []byte("wechat-runtime/onebot/v1"))...))
}
func (s *legacyFixture) ID(account, kind, key string) (int64, error) {
	lookup := identityKey(account, kind, key)
	if id, ok := s.data.IDs[lookup]; ok {
		return id, nil
	}
	id := s.data.Next
	s.data.Next++
	s.data.IDs[lookup] = id
	s.data.Refs[strconv.FormatInt(id, 10)] = reference{account, kind, key}
	return id, s.saveLocked()
}
func (s *legacyFixture) setCursor(account, consumer string, cursor uint64) error {
	s.data.Cursors[identityKey(account, "cursor", consumer)] = cursor
	return s.saveLocked()
}
