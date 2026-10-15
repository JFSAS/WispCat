package onebot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"

	"github.com/JFSAS/wechat-runtime/internal/store"
)

// OpenSharedState allocates public IDs once per application root. Keys remain
// account-scoped, but two accounts cannot advertise the same OneBot self_id.
// Compatible legacy mappings are imported without renumbering or deleting them.
func OpenSharedState(directory string, legacyDirectories []string) (*State, error) {
	sharedDirectory, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	s, err := OpenState(directory)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, _ := json.Marshal(s.data)
	var candidate durableState
	if err = json.Unmarshal(raw, &candidate); err != nil {
		return nil, err
	}
	if candidate.Imports == nil {
		candidate.Imports = map[string]string{}
	}
	for _, legacyDirectory := range legacyDirectories {
		absolute, err := filepath.Abs(legacyDirectory)
		if err != nil {
			return nil, err
		}
		if absolute == sharedDirectory {
			continue
		}
		path := filepath.Join(absolute, "onebot.state")
		encrypted, err := store.ReadPrivate(path, maxStateBytes)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		identity := sha256.Sum256([]byte(absolute))
		source := hex.EncodeToString(identity[:])
		digest := sha256.Sum256(encrypted)
		fingerprint := hex.EncodeToString(digest[:])
		if imported, ok := candidate.Imports[source]; ok {
			if imported != fingerprint {
				return nil, errors.New("legacy_onebot_state_changed_after_migration")
			}
			continue
		}
		legacy, err := OpenState(absolute)
		if err != nil {
			return nil, err
		}
		for key, id := range legacy.data.IDs {
			ref := legacy.data.Refs[strconv.FormatInt(id, 10)]
			if old, exists := candidate.IDs[key]; exists {
				if old != id {
					return nil, errors.New("legacy_onebot_id_conflict")
				}
				continue
			}
			if old, exists := candidate.Refs[strconv.FormatInt(id, 10)]; exists && old != ref {
				return nil, errors.New("legacy_onebot_id_collision")
			}
			candidate.IDs[key] = id
			candidate.Refs[strconv.FormatInt(id, 10)] = ref
		}
		for key, cursor := range legacy.data.Cursors {
			if old, exists := candidate.Cursors[key]; !exists || cursor > old {
				candidate.Cursors[key] = cursor
			}
		}
		if legacy.data.Next > candidate.Next {
			candidate.Next = legacy.data.Next
		}
		candidate.Imports[source] = fingerprint
	}
	if len(candidate.IDs) > 100000 || len(candidate.Cursors) > 1024 {
		return nil, errors.New("shared_onebot_state_full")
	}
	old := s.data
	s.data = candidate
	if err = s.saveLocked(); err != nil {
		s.data = old
		return nil, err
	}
	return s, nil
}
