package onebot

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/JFSAS/wispcat/internal/securedb"
)

type reference struct {
	Account string `json:"account"`
	Kind    string `json:"kind"`
	Key     string `json:"key"`
}

const maxStateBytes = 32 << 20 // Read limit of the legacy snapshot only.

var errStateUnavailable = errors.New("state_unavailable")

// durableState is the read-only v1 migration format. New writes use the v2
// index and never rewrite, truncate or delete this legacy snapshot.
type durableState struct {
	Version int                  `json:"version"`
	Next    int64                `json:"next"`
	IDs     map[string]int64     `json:"ids"`
	Refs    map[string]reference `json:"refs"`
	Cursors map[string]uint64    `json:"cursors"`
	Imports map[string]string    `json:"imports,omitempty"`
}
type indexMeta struct {
	Version int    `json:"version"`
	Next    int64  `json:"next"`
	IDs     uint64 `json:"ids"`
	Cursors uint64 `json:"cursors"`
	Imports uint64 `json:"imports"`
}

// State persists account-scoped public identities and independent cursors.
// The owner must Close it after all runtime/transport users have stopped.
type State struct{ db *securedb.DB }

type Identity struct{ Kind, Key string }
type StateStats struct {
	Format     string `json:"format"`
	Identities uint64 `json:"identities"`
	Cursors    uint64 `json:"cursors"`
	Imports    uint64 `json:"legacy_imports"`
	NextID     int64  `json:"next_id"`
	FileBytes  int64  `json:"file_bytes"`
}

func OpenState(directory string) (*State, error) {
	return openState(directory, nil)
}

func openState(directory string, legacyDirectories []string) (*State, error) {
	db, err := securedb.Open(directory, "onebot")
	if err != nil {
		return nil, err
	}
	s := &State{db: db}
	err = db.Update(func(tx *securedb.Tx) error {
		raw, e := tx.Get("meta", []byte("state"))
		if e != nil {
			return e
		}
		meta := indexMeta{Version: 2, Next: 10000}
		if raw != nil {
			meta, e = decodeMeta(raw)
			if e != nil {
				return e
			}
		} else {
			for _, bucket := range []string{"identities", "references", "cursors", "imports"} {
				if tx.HasRecords(bucket) {
					return errors.New("onebot_index_metadata_missing")
				}
			}
		}
		if e = s.importLegacy(tx, &meta, directory, true); e != nil {
			return e
		}
		for _, source := range legacyDirectories {
			if e = s.importLegacy(tx, &meta, source, false); e != nil {
				return e
			}
		}
		return putMeta(tx, meta)
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *State) Close() error                      { return s.db.Close() }
func identityKey(account, kind, key string) string { return account + "\x00" + kind + "\x00" + key }
func validMeta(meta indexMeta) bool {
	return meta.Version == 2 && meta.Next >= 10000 && meta.Next <= 2147483647 && meta.IDs < uint64(meta.Next) && meta.Cursors <= 1024
}
func readMeta(tx *securedb.Tx) (indexMeta, error) {
	raw, err := tx.Get("meta", []byte("state"))
	if err != nil {
		return indexMeta{}, err
	}
	return decodeMeta(raw)
}
func decodeMeta(raw []byte) (indexMeta, error) {
	var meta indexMeta
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return meta, errors.New("onebot_index_metadata_invalid")
	}
	for _, name := range []string{"version", "next", "ids", "cursors", "imports"} {
		if fields[name] == nil || string(fields[name]) == "null" {
			return meta, errors.New("onebot_index_metadata_invalid")
		}
	}
	if json.Unmarshal(raw, &meta) != nil || !validMeta(meta) {
		return meta, errors.New("onebot_index_metadata_invalid")
	}
	return meta, nil
}
func putMeta(tx *securedb.Tx, meta indexMeta) error {
	raw, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return tx.Put("meta", []byte("state"), raw)
}
func (s *State) ID(account, kind, key string) (int64, error) {
	ids, err := s.IDBatch(account, []Identity{{kind, key}})
	if err != nil {
		return 0, err
	}
	return ids[0], nil
}

// IDBatch commits all new numbers together, before exposing any of them. Reads
// of existing identities do not start a write transaction or rewrite old rows.
func (s *State) IDBatch(account string, identities []Identity) ([]int64, error) {
	if account == "" || len(account) > 256 || strings.IndexByte(account, 0) >= 0 || !utf8.ValidString(account) || len(identities) == 0 || len(identities) > 1024 {
		return nil, errors.New("identity_unavailable")
	}
	for _, ref := range identities {
		if ref.Kind == "" || len(ref.Kind) > 32 || strings.IndexByte(ref.Kind, 0) >= 0 || ref.Key == "" || len(ref.Key) > 8192 || !utf8.ValidString(ref.Kind) || !utf8.ValidString(ref.Key) {
			return nil, errors.New("identity_unavailable")
		}
	}
	ids := make([]int64, len(identities))
	complete := true
	err := s.db.View(func(tx *securedb.Tx) error {
		for i, ref := range identities {
			id, e := s.lookup(tx, reference{account, ref.Kind, ref.Key})
			if e != nil {
				return e
			}
			ids[i] = id
			complete = complete && id != 0
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if complete {
		return ids, nil
	}
	err = s.db.Update(func(tx *securedb.Tx) error {
		meta, e := readMeta(tx)
		if e != nil {
			return e
		}
		for i, ref := range identities {
			item := reference{account, ref.Kind, ref.Key}
			id, e := s.lookup(tx, item)
			if e != nil {
				return e
			}
			if id == 0 {
				if meta.Next >= 2147483647 {
					return errors.New("onebot_numeric_id_space_full")
				}
				id = meta.Next
				if e = s.putIdentity(tx, item, id); e != nil {
					return e
				}
				meta.Next++
				meta.IDs++
			}
			ids[i] = id
		}
		return putMeta(tx, meta)
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

func (s *State) lookup(tx *securedb.Tx, ref reference) (int64, error) {
	raw, err := tx.Get("identities", s.db.Key("identity", ref.Account, ref.Kind, ref.Key))
	if err != nil || raw == nil {
		return 0, err
	}
	var id int64
	if json.Unmarshal(raw, &id) != nil || id <= 0 || id >= 2147483647 {
		return 0, errors.New("onebot_mapping_invalid")
	}
	reverse, err := tx.Get("references", []byte(strconv.FormatInt(id, 10)))
	if err != nil {
		return 0, err
	}
	var found reference
	if json.Unmarshal(reverse, &found) != nil || found != ref {
		return 0, errors.New("onebot_mapping_invalid")
	}
	return id, nil
}
func (s *State) putIdentity(tx *securedb.Tx, ref reference, id int64) error {
	other, err := tx.Get("references", []byte(strconv.FormatInt(id, 10)))
	if err != nil {
		return err
	}
	if other != nil {
		return errors.New("onebot_numeric_id_already_reserved")
	}
	rawID, _ := json.Marshal(id)
	rawRef, err := json.Marshal(ref)
	if err != nil {
		return err
	}
	if err = tx.Put("identities", s.db.Key("identity", ref.Account, ref.Kind, ref.Key), rawID); err != nil {
		return err
	}
	return tx.Put("references", []byte(strconv.FormatInt(id, 10)), rawRef)
}
func (s *State) resolveChecked(account, kind string, id int64) (reference, bool, error) {
	var ref reference
	err := s.db.View(func(tx *securedb.Tx) error {
		raw, e := tx.Get("references", []byte(strconv.FormatInt(id, 10)))
		if e != nil || raw == nil {
			return e
		}
		if json.Unmarshal(raw, &ref) != nil {
			return errors.New("onebot_mapping_invalid")
		}
		return nil
	})
	return ref, err == nil && ref.Account == account && ref.Kind == kind, err
}
func (s *State) cursor(account, consumer string, fallback uint64) (uint64, error) {
	value := fallback
	err := s.db.View(func(tx *securedb.Tx) error {
		raw, e := tx.Get("cursors", s.db.Key("cursor", account, consumer))
		if e != nil || raw == nil {
			return e
		}
		if json.Unmarshal(raw, &value) != nil {
			return errors.New("onebot_cursor_invalid")
		}
		return nil
	})
	return value, err
}
func (s *State) setCursor(account, consumer string, cursor uint64) error {
	if account == "" || consumer == "" || len(account) > 256 || len(consumer) > 128 {
		return errors.New("invalid_consumer")
	}
	return s.db.Update(func(tx *securedb.Tx) error {
		meta, e := readMeta(tx)
		if e != nil {
			return e
		}
		key := s.db.Key("cursor", account, consumer)
		raw, e := tx.Get("cursors", key)
		if e != nil {
			return e
		}
		if raw == nil {
			if meta.Cursors >= 1024 {
				return errors.New("consumer_limit")
			}
			meta.Cursors++
		} else {
			var old uint64
			if json.Unmarshal(raw, &old) != nil {
				return errors.New("onebot_cursor_invalid")
			}
			if old == cursor {
				return nil
			}
		}
		raw, _ = json.Marshal(cursor)
		if e = tx.Put("cursors", key, raw); e != nil {
			return e
		}
		return putMeta(tx, meta)
	})
}
func (s *State) Stats() (StateStats, error) {
	result := StateStats{Format: "encrypted_bbolt_v2"}
	err := s.db.View(func(tx *securedb.Tx) error {
		meta, e := readMeta(tx)
		if e != nil {
			return e
		}
		result.Identities, result.Cursors, result.Imports, result.NextID = meta.IDs, meta.Cursors, meta.Imports, meta.Next
		return nil
	})
	if err != nil {
		return result, err
	}
	result.FileBytes, err = s.db.Size()
	return result, err
}
