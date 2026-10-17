package service

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"sort"

	"github.com/JFSAS/wispcat/internal/protocol"
	"github.com/JFSAS/wispcat/internal/securedb"
	"github.com/JFSAS/wispcat/internal/store"
)

// Public consumer names are at most64 bytes in the unchanged native writer.
// A longer reserved key cannot collide with any valid existing consumer name.
const contactCheckpointKey = "wechat-runtime/internal/contact-checkpoint/directory-dependency/v1/reserved"
const contactInlineBytes = 1 << 20
const contactCheckpointSentinel = "\x00wechat-runtime/contact-checkpoint/v1"

// This small authenticated checkpoint record makes the directory dependency
// explicit. The original store format and protocol contacts remain unchanged.
type contactCheckpoint struct {
	Schema    int    `json:"schema"`
	Account   string `json:"account"`
	Namespace string `json:"namespace"`
	Identity  string `json:"identity"`
	Revision  uint64 `json:"revision"`
	Count     uint64 `json:"count"`
	Digest    string `json:"digest"`
}

func readContactCheckpoint(data *store.Data) (*contactCheckpoint, error) {
	if data == nil {
		return nil, nil
	}
	raw, exists := data.Consumers[contactCheckpointKey]
	if !exists {
		for _, contact := range data.Contacts {
			if string(contact.Username) == contactCheckpointSentinel {
				return nil, ErrEventHistory
			}
		}
		return nil, nil
	}
	var value contactCheckpoint
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return nil, ErrEventHistory
	}
	for _, key := range []string{"schema", "account", "namespace", "identity", "revision", "count", "digest"} {
		if fields[key] == nil || bytes.Equal(fields[key], []byte("null")) {
			return nil, ErrEventHistory
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&value) != nil || decoder.Decode(new(any)) != io.EOF || value.Schema != 1 || value.Account != data.Binding || value.Namespace != historyNamespace(data) || !mediaID(value.Identity, 64) || value.Revision == 0 || !mediaID(value.Digest, 64) {
		return nil, ErrEventHistory
	}
	return &value, nil
}

// The second reference makes separate loss of either serialized field explicit.
// It is private local metadata and is never admitted to a live protocol roster.
func serializedContactCheckpoint(data *store.Data) error {
	value, err := readContactCheckpoint(data)
	if err != nil || value == nil {
		return err
	}
	if len(data.Contacts) != 1 {
		return ErrEventHistory
	}
	contact := data.Contacts[0]
	if string(contact.Username) != contactCheckpointSentinel || contact.Nickname != nil || contact.Remark != nil || contact.Deleted {
		return ErrEventHistory
	}
	return nil
}

func contactCheckpointDependency(data *store.Data, h *EventHistory) error {
	value, err := readContactCheckpoint(data)
	if err != nil {
		return err
	}
	if value != nil && (h == nil || value.Identity != h.identity) {
		return ErrEventHistory
	}
	return nil
}

// Hash one record at a time, rather than allocating a second whole-roster JSON
// document. Sorting the shallow copy matches native updateContacts ordering.
func contactFingerprint(contacts []protocol.Contact) (string, int, error) {
	ordered := append([]protocol.Contact(nil), contacts...)
	sort.Slice(ordered, func(i, j int) bool { return bytes.Compare(ordered[i].Username, ordered[j].Username) < 0 })
	hash := sha256.New()
	size := 2 // enclosing JSON array
	for i, contact := range ordered {
		if len(contact.Username) == 0 || string(contact.Username) == contactCheckpointSentinel || contact.Deleted || (i > 0 && bytes.Equal(ordered[i-1].Username, contact.Username)) {
			return "", 0, ErrEventHistory
		}
		raw, err := json.Marshal(contact)
		if err != nil {
			return "", 0, ErrEventHistory
		}
		_, _ = hash.Write(raw)
		_, _ = hash.Write([]byte{'\n'})
		size += len(raw) + 1
	}
	return hex.EncodeToString(hash.Sum(nil)), size, nil
}

// Called after history and its outside guard commit, under Service.mu. Only
// the synchronous store.Save sees the omitted contacts; every exit restores
// the live roster, including a checkpoint failure. Other consumer data stays.
func (h *EventHistory) prepareContactCheckpoint(data *store.Data) (func(), error) {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	if h.closed || contactCheckpointDependency(data, h) != nil {
		return nil, ErrEventHistory
	}
	digest, size, err := contactFingerprint(data.Contacts)
	if err != nil {
		return nil, err
	}
	_, external := data.Consumers[contactCheckpointKey]
	if !external && size <= contactInlineBytes {
		return func() {}, nil
	}
	if h.unavailable || h.stageErr != nil {
		return nil, ErrEventHistory
	}
	if h.published.Account != data.Binding || h.published.Namespace != historyNamespace(data) || h.publishedDirectory == 0 {
		return nil, ErrEventHistory
	}
	value := contactCheckpoint{Schema: 1, Account: data.Binding, Namespace: historyNamespace(data), Identity: h.identity, Revision: h.publishedDirectory, Count: uint64(len(data.Contacts)), Digest: digest}
	raw, _ := json.Marshal(value)
	if data.Consumers == nil {
		data.Consumers = map[string]json.RawMessage{}
	}
	data.Consumers[contactCheckpointKey] = raw
	contacts := data.Contacts
	data.Contacts = []protocol.Contact{{Username: []byte(contactCheckpointSentinel)}}
	return func() { data.Contacts = contacts }, nil
}

// QR retry may follow a failed history transaction or outside-guard write. The
// external roster must first reconcile that durable state; no protocol cursor
// is advanced and staged replay facts remain pending. Other readers stay fenced
// if either the guard or complete roster cannot be authenticated.
func (h *EventHistory) restoreContactAnchor(data *store.Data) error {
	marker, err := readContactCheckpoint(data)
	if err != nil {
		return err
	}
	h.writeMu.Lock()
	if marker == nil && h.published.Account != data.Binding {
		h.writeMu.Unlock()
		return nil
	}
	if h.closed || h.stageErr != nil || h.ensureGuard(data.Binding) != nil {
		h.writeMu.Unlock()
		return ErrEventHistory
	}
	h.unavailable = false
	h.writeMu.Unlock()
	if err := h.restoreCheckpointContacts(data); err != nil {
		h.writeMu.Lock()
		h.unavailable = true
		h.writeMu.Unlock()
		return err
	}
	return nil
}

// A newer committed directory may precede a failed protocol checkpoint. Its
// latest contact facts win, just as the existing availability recovery does.
// At the exact checkpoint revision the complete roster checksum must match.
func (h *EventHistory) restoreCheckpointContacts(data *store.Data) error {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	return h.restoreCheckpointContactsLocked(data, true)
}

// Also hydrate inline checkpoints: a first external save may have failed after
// contact facts committed. History is authoritative before and after migration.
func (h *EventHistory) restoreCheckpointContactsLocked(data *store.Data, verifyMarker bool) error {
	marker, err := readContactCheckpoint(data)
	if err != nil {
		return err
	}
	if contactCheckpointDependency(data, h) != nil {
		return ErrEventHistory
	}
	var contacts []protocol.Contact
	var unavailable [][]byte
	var revision uint64
	err = h.directoryReadLocked(data.Binding, func(tx *securedb.Tx) error {
		meta, ready, err := h.directoryMeta(tx, data.Binding, historyNamespace(data))
		if err != nil || !ready || (marker != nil && meta.Revision < marker.Revision) {
			return ErrEventHistory
		}
		revision = meta.Revision
		visited := uint64(0)
		err = scanDirectory(tx, "conversations", h.db.Key("conversations", data.Binding), func(key []byte) error {
			visited++
			if visited > meta.Conversations {
				return ErrEventHistory
			}
			entry, err := h.directoryEntry(tx, "conversations", data.Binding, string(key[32:]))
			if err != nil || entry == nil {
				return ErrEventHistory
			}
			if !entry.Available {
				unavailable = append(unavailable, bytes.Clone(entry.Raw))
				return nil
			}
			if entry.Contact != nil {
				contact := *entry.Contact
				if contact.Deleted || !bytes.Equal(contact.Username, entry.Raw) {
					return ErrEventHistory
				}
				contacts = append(contacts, contact)
			}
			return nil
		})
		if err != nil || visited != meta.Conversations {
			return ErrEventHistory
		}
		return nil
	})
	if err != nil {
		return err
	}
	digest, _, err := contactFingerprint(contacts)
	if err != nil || (verifyMarker && marker != nil && revision == marker.Revision && (uint64(len(contacts)) != marker.Count || digest != marker.Digest)) {
		return ErrEventHistory
	}
	sort.Slice(contacts, func(i, j int) bool { return bytes.Compare(contacts[i].Username, contacts[j].Username) < 0 })
	// Preserve unknown checkpoint-only fences, but a current available Contact
	// explicitly clears an old deletion, including during QR-anchor recovery.
	available := map[string]bool{}
	for _, contact := range contacts {
		available[string(contact.Username)] = true
	}
	seen := map[string]bool{}
	retained := [][]byte{}
	for _, raw := range data.Unavailable {
		if !available[string(raw)] && !seen[string(raw)] {
			retained = append(retained, raw)
			seen[string(raw)] = true
		}
	}
	data.Unavailable = retained
	for _, raw := range unavailable {
		if !seen[string(raw)] {
			data.Unavailable = append(data.Unavailable, raw)
			seen[string(raw)] = true
		}
	}
	data.Contacts = contacts
	return nil
}
