package onebot

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/JFSAS/wechat-runtime/internal/store"
)

type reference struct {
	Account string `json:"account"`
	Kind    string `json:"kind"`
	Key     string `json:"key"`
}

const maxStateBytes = 32 << 20

type durableState struct {
	Version int                  `json:"version"`
	Next    int64                `json:"next"`
	IDs     map[string]int64     `json:"ids"`
	Refs    map[string]reference `json:"refs"`
	Cursors map[string]uint64    `json:"cursors"`
	Imports map[string]string    `json:"imports,omitempty"`
}

// State persists account-scoped, collision-free v11 IDs and per-consumer cursors.
// ID allocation is committed before an ID is exposed on any transport.
type State struct {
	mu   sync.Mutex
	path string
	aead cipher.AEAD
	data durableState
}

func OpenState(directory string) (*State, error) {
	if err := store.Directory(directory); err != nil {
		return nil, err
	}
	keyPath := filepath.Join(directory, "onebot.key")
	key, err := store.ReadPrivate(keyPath, 32)
	if errors.Is(err, os.ErrNotExist) {
		if _, stateErr := os.Lstat(filepath.Join(directory, "onebot.state")); !errors.Is(stateErr, os.ErrNotExist) {
			return nil, errors.New("onebot_key_missing_for_existing_state")
		}
		key = make([]byte, 32)
		if _, err = rand.Read(key); err != nil {
			return nil, err
		}
		if err = store.Atomic(keyPath, key); err != nil {
			return nil, err
		}
	}
	if err != nil || len(key) != 32 {
		return nil, errors.New("onebot_key_invalid")
	}
	block, _ := aes.NewCipher(key)
	aead, _ := cipher.NewGCM(block)
	s := &State{path: filepath.Join(directory, "onebot.state"), aead: aead, data: durableState{Version: 1, Next: 10000, IDs: map[string]int64{}, Refs: map[string]reference{}, Cursors: map[string]uint64{}}}
	raw, err := store.ReadPrivate(s.path, maxStateBytes)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	if len(raw) < aead.NonceSize() {
		return nil, errors.New("onebot_state_invalid")
	}
	plain, err := aead.Open(nil, raw[:aead.NonceSize()], raw[aead.NonceSize():], []byte("wechat-runtime/onebot/v1"))
	if err != nil {
		return nil, errors.New("onebot_state_authentication_failed")
	}
	if json.Unmarshal(plain, &s.data) != nil || s.data.Version != 1 || s.data.Next < 10000 || s.data.Next > 2147483647 || s.data.IDs == nil || s.data.Refs == nil || s.data.Cursors == nil || len(s.data.IDs) > 100000 {
		return nil, errors.New("onebot_state_invalid")
	}
	for key, id := range s.data.IDs {
		ref, ok := s.data.Refs[strconv.FormatInt(id, 10)]
		if !ok || id <= 0 || id >= s.data.Next || key != identityKey(ref.Account, ref.Kind, ref.Key) {
			return nil, errors.New("onebot_mapping_invalid")
		}
	}
	if len(s.data.IDs) != len(s.data.Refs) {
		return nil, errors.New("onebot_mapping_invalid")
	}
	return s, nil
}
func identityKey(account, kind, key string) string { return account + "\x00" + kind + "\x00" + key }
func (s *State) saveLocked() error {
	plain, err := json.Marshal(s.data)
	if err != nil {
		return err
	}
	if len(plain)+s.aead.NonceSize()+s.aead.Overhead() > maxStateBytes {
		return errors.New("onebot_state_size_limit")
	}
	nonce := make([]byte, s.aead.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return err
	}
	return store.Atomic(s.path, append(nonce, s.aead.Seal(nil, nonce, plain, []byte("wechat-runtime/onebot/v1"))...))
}
func (s *State) ID(account, kind, key string) (int64, error) {
	if account == "" || key == "" || len(key) > 8192 {
		return 0, errors.New("identity_unavailable")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := identityKey(account, kind, key)
	if id, ok := s.data.IDs[k]; ok {
		return id, nil
	}
	if len(s.data.IDs) >= 100000 || s.data.Next >= 2147483647 {
		return 0, errors.New("onebot_mapping_full")
	}
	id := s.data.Next
	s.data.Next++
	s.data.IDs[k] = id
	s.data.Refs[strconv.FormatInt(id, 10)] = reference{account, kind, key}
	if err := s.saveLocked(); err != nil {
		delete(s.data.IDs, k)
		delete(s.data.Refs, strconv.FormatInt(id, 10))
		s.data.Next = id
		return 0, err
	}
	return id, nil
}
func (s *State) resolve(account, kind string, id int64) (reference, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ref, ok := s.data.Refs[strconv.FormatInt(id, 10)]
	return ref, ok && ref.Account == account && ref.Kind == kind
}
func (s *State) cursor(account, consumer string, fallback uint64) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data.Cursors[identityKey(account, "cursor", consumer)]
	if !ok {
		return fallback
	}
	return v
}
func (s *State) setCursor(account, consumer string, cursor uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := identityKey(account, "cursor", consumer)
	old, exists := s.data.Cursors[key]
	if exists && old == cursor {
		return nil
	}
	if !exists && len(s.data.Cursors) >= 1024 {
		return errors.New("consumer_limit")
	}
	s.data.Cursors[key] = cursor
	if err := s.saveLocked(); err != nil {
		if exists {
			s.data.Cursors[key] = old
		} else {
			delete(s.data.Cursors, key)
		}
		return err
	}
	return nil
}
