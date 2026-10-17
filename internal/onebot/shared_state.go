package onebot

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/JFSAS/wispcat/internal/securedb"
	"github.com/JFSAS/wispcat/internal/store"
)

// OpenSharedState imports every legacy mapping in one index transaction, without
// renumbering or deleting source snapshots. A conflict publishes none of the batch.
func OpenSharedState(directory string, legacyDirectories []string) (*State, error) {
	absolute, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	var sources []string
	for _, legacy := range legacyDirectories {
		source, e := filepath.Abs(legacy)
		if e != nil {
			return nil, e
		}
		if source != absolute {
			sources = append(sources, source)
		}
	}
	return openState(directory, sources)
}

func legacyBytes(directory string) ([]byte, string, error) {
	raw, err := store.ReadPrivate(filepath.Join(directory, "onebot.state"), maxStateBytes)
	if errors.Is(err, os.ErrNotExist) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	digest := sha256.Sum256(raw)
	return raw, hex.EncodeToString(digest[:]), nil
}

func readLegacy(directory string) (*durableState, string, error) {
	raw, fingerprint, err := legacyBytes(directory)
	if err != nil || raw == nil {
		return nil, fingerprint, err
	}
	data, err := decodeLegacy(directory, raw)
	return data, fingerprint, err
}

func decodeLegacy(directory string, raw []byte) (*durableState, error) {
	key, err := store.ReadPrivate(filepath.Join(directory, "onebot.key"), 32)
	if err != nil || len(key) != 32 {
		return nil, errors.New("legacy_onebot_key_invalid")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil || len(raw) < aead.NonceSize()+aead.Overhead() {
		return nil, errors.New("legacy_onebot_state_invalid")
	}
	plain, err := aead.Open(nil, raw[:aead.NonceSize()], raw[aead.NonceSize():], []byte("wechat-runtime/onebot/v1"))
	if err != nil {
		return nil, errors.New("legacy_onebot_state_authentication_failed")
	}
	var data durableState
	if json.Unmarshal(plain, &data) != nil || data.Version != 1 || data.Next < 10000 || data.Next > 2147483647 || data.IDs == nil || data.Refs == nil || data.Cursors == nil || len(data.IDs) > 100000 || len(data.IDs) != len(data.Refs) {
		return nil, errors.New("legacy_onebot_state_invalid")
	}
	for key, id := range data.IDs {
		ref, ok := data.Refs[strconv.FormatInt(id, 10)]
		if !ok || id <= 0 || id >= data.Next || key != identityKey(ref.Account, ref.Kind, ref.Key) {
			return nil, errors.New("legacy_onebot_mapping_invalid")
		}
	}
	return &data, nil
}

func (s *State) importLegacy(tx *securedb.Tx, meta *indexMeta, directory string, own bool) error {
	raw, fingerprint, err := legacyBytes(directory)
	if err != nil || raw == nil {
		return err
	}
	absolute, err := filepath.Abs(directory)
	if err != nil {
		return err
	}
	digest := sha256.Sum256([]byte(absolute))
	source := hex.EncodeToString(digest[:])
	if own {
		source = "root-v1-snapshot"
	}
	imported, err := tx.Get("imports", s.db.Key("legacy", source))
	if err != nil {
		return err
	}
	if imported != nil {
		if string(imported) != fingerprint {
			return errors.New("legacy_onebot_state_changed_after_migration")
		}
		return nil
	}
	data, err := decodeLegacy(directory, raw)
	if err != nil {
		return err
	}
	for _, id := range data.IDs {
		ref := data.Refs[strconv.FormatInt(id, 10)]
		old, e := s.lookup(tx, ref)
		if e != nil {
			return e
		}
		if old != 0 {
			if old != id {
				return errors.New("legacy_onebot_id_conflict")
			}
			continue
		}
		other, e := tx.Get("references", []byte(strconv.FormatInt(id, 10)))
		if e != nil {
			return e
		}
		if other != nil {
			return errors.New("legacy_onebot_id_collision")
		}
		if e = s.putIdentity(tx, ref, id); e != nil {
			return e
		}
		meta.IDs++
	}
	for key, cursor := range data.Cursors {
		parts := strings.SplitN(key, "\x00", 3)
		if len(parts) != 3 || parts[0] == "" || parts[1] != "cursor" || parts[2] == "" {
			return errors.New("legacy_onebot_cursor_invalid")
		}
		lookup := s.db.Key("cursor", parts[0], parts[2])
		raw, e := tx.Get("cursors", lookup)
		if e != nil {
			return e
		}
		if raw != nil {
			var old uint64
			if json.Unmarshal(raw, &old) != nil {
				return errors.New("onebot_cursor_invalid")
			}
			if old >= cursor {
				continue
			}
		} else {
			if meta.Cursors >= 1024 {
				return errors.New("consumer_limit")
			}
			meta.Cursors++
		}
		raw, _ = json.Marshal(cursor)
		if e = tx.Put("cursors", lookup, raw); e != nil {
			return e
		}
	}
	for previous, fingerprint := range data.Imports {
		if err = s.rememberImport(tx, meta, previous, fingerprint); err != nil {
			return err
		}
	}
	if data.Next > meta.Next {
		meta.Next = data.Next
	}
	return s.rememberImport(tx, meta, source, fingerprint)
}

func (s *State) rememberImport(tx *securedb.Tx, meta *indexMeta, source, fingerprint string) error {
	key := s.db.Key("legacy", source)
	old, err := tx.Get("imports", key)
	if err != nil {
		return err
	}
	if old != nil {
		if string(old) != fingerprint {
			return errors.New("legacy_onebot_state_changed_after_migration")
		}
		return nil
	}
	if err = tx.Put("imports", key, []byte(fingerprint)); err != nil {
		return err
	}
	meta.Imports++
	return nil
}
