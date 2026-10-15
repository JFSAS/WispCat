package service

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/JFSAS/wechat-runtime/internal/messaging"
	"github.com/JFSAS/wechat-runtime/internal/protocol"
	"github.com/JFSAS/wechat-runtime/internal/securedb"
	"github.com/JFSAS/wechat-runtime/internal/store"
	"github.com/JFSAS/wechat-runtime/pkg/bot"
)

var ErrEventHistory = fmt.Errorf("event_history_unavailable: %w", bot.ErrStorage)

type historyMeta struct {
	Schema    int    `json:"schema"`
	Account   string `json:"account"`
	Namespace string `json:"namespace"`
	First     uint64 `json:"first"`
	Last      uint64 `json:"last"`
	Messages  uint64 `json:"messages"`
}
type historyRecord struct {
	Schema int              `json:"schema"`
	Event  bot.Event        `json:"event"`
	Raw    protocol.Message `json:"raw"`
}

func (historyRecord) String() string   { return "historyRecord(redacted)" }
func (historyRecord) GoString() string { return "historyRecord(redacted)" }

type historyInput struct {
	account   string
	namespace string
	raw       protocol.Message
	event     bot.Event
}

// EventHistory owns encrypted durable normalized events and their native media
// references. The service lock owns staging; writeMu serializes commits/Close.
// History commits precede native checkpoint progress. A replay from an older
// protocol cursor deduplicates by canonical message ID, independently of native
// event sequence reuse. The original native cache keeps its existing bounds.
type EventHistory struct {
	db           *securedb.DB
	directory    string
	writeMu      sync.Mutex
	closed       bool
	pending      []historyInput
	pendingBytes int
	stageErr     error
	write        func(func(*securedb.Tx) error) error
	identity     string
	unavailable  bool
	guardWrite   func([]byte) error
	published    historyMeta
}

type HistoryStats struct {
	Format      string `json:"format"`
	Messages    uint64 `json:"messages"`
	FirstCursor uint64 `json:"first_cursor"`
	Cursor      uint64 `json:"cursor"`
	FileBytes   int64  `json:"file_bytes"`
}

func historyNamespace(data *store.Data) string {
	sum := sha256.Sum256(data.APIKey)
	return hex.EncodeToString(sum[:])
}

type historyGuard struct {
	Schema   int         `json:"schema"`
	Identity string      `json:"identity"`
	Meta     historyMeta `json:"meta"`
}

func OpenEventHistory(directory string) (*EventHistory, error) {
	required := filepath.Join(directory, "event-history.required")
	raw, guardErr := store.ReadPrivate(required, 2048)
	var guard *historyGuard
	if guardErr == nil {
		var value historyGuard
		var fields map[string]json.RawMessage
		if json.Unmarshal(raw, &fields) != nil || fields["schema"] == nil || fields["identity"] == nil || fields["meta"] == nil {
			return nil, ErrEventHistory
		}
		if _, err := decodeHistoryMeta(fields["meta"]); err != nil {
			return nil, ErrEventHistory
		}
		if json.Unmarshal(raw, &value) != nil || value.Schema != 1 || !mediaID(value.Identity, 64) || !validHistoryMeta(value.Meta) {
			return nil, ErrEventHistory
		}
		guard = &value
		if _, err := os.Lstat(filepath.Join(directory, "event-history", "events.db")); err != nil {
			return nil, ErrEventHistory
		}
	} else if !errors.Is(guardErr, os.ErrNotExist) {
		return nil, ErrEventHistory
	}
	db, err := securedb.Open(filepath.Join(directory, "event-history"), "events")
	if err != nil {
		return nil, ErrEventHistory
	}
	h := &EventHistory{db: db, directory: directory}
	err = db.Update(func(tx *securedb.Tx) error {
		id, e := tx.Get("meta", []byte("identity"))
		if e != nil {
			return e
		}
		if id == nil {
			if guard != nil || tx.HasRecords("messages") || tx.HasRecords("events") || tx.HasRecords("meta") {
				return ErrEventHistory
			}
			token := make([]byte, 32)
			if _, e = rand.Read(token); e != nil {
				return e
			}
			id = []byte(hex.EncodeToString(token))
			if e = tx.Put("meta", []byte("identity"), id); e != nil {
				return e
			}
		}
		if !mediaID(string(id), 64) || (guard != nil && guard.Identity != string(id)) {
			return ErrEventHistory
		}
		h.identity = string(id)
		if guard != nil {
			m, exists, e := h.meta(tx, guard.Meta.Account, guard.Meta.Namespace)
			if e != nil || !exists || m.First != guard.Meta.First || m.Last < guard.Meta.Last || m.Messages < guard.Meta.Messages {
				return ErrEventHistory
			}
		}
		return nil
	})
	var data *store.Data
	if err == nil {
		var exists bool
		var cp store.Checkpoint
		cp, exists, err = store.Unique(filepath.Join(directory, "checkpoints"))
		if err == nil && exists {
			data, err = cp.Load()
			if err == nil {
				err = h.importCheckpoint(data)
			}
			if err == nil {
				err = h.ensureGuard(data.Binding)
			}
		} else if err == nil {
			err = db.View(func(tx *securedb.Tx) error {
				owner, e := tx.Get("meta", []byte("owner"))
				if e != nil || owner != nil || guard != nil {
					return ErrEventHistory
				}
				return nil
			})
		}
	}
	if err != nil {
		_ = db.Close()
		return nil, ErrEventHistory
	}
	return h, nil
}
func validHistoryMeta(m historyMeta) bool {
	return m.Schema == 1 && mediaID(m.Account, 64) && mediaID(m.Namespace, 64) && m.First <= m.Last && m.Last-m.First <= m.Messages
}
func (h *EventHistory) ensureGuard(account string) error {
	var meta historyMeta
	err := h.db.View(func(tx *securedb.Tx) error { var e error; meta, _, e = h.meta(tx, account, ""); return e })
	if err != nil || !validHistoryMeta(meta) {
		return ErrEventHistory
	}
	if h.published == meta {
		return nil
	}
	raw, _ := json.Marshal(historyGuard{Schema: 1, Identity: h.identity, Meta: meta})
	if h.guardWrite != nil {
		err = h.guardWrite(raw)
	} else {
		err = store.Atomic(filepath.Join(h.directory, "event-history.required"), raw)
	}
	if err != nil {
		return ErrEventHistory
	}
	h.published = meta
	return nil
}
func (h *EventHistory) Close() error {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	if h.closed {
		return nil
	}
	h.closed = true
	return h.db.Close()
}
func (a BotAdapter) EnableHistory(h *EventHistory) error {
	s := a.Service
	s.mu.Lock()
	defer s.mu.Unlock()
	if h == nil || s.history != nil {
		return ErrEventHistory
	}
	if s.data != nil {
		h.writeMu.Lock()
		err := h.importCheckpoint(s.data)
		if err == nil {
			err = h.ensureGuard(s.data.Binding)
		}
		h.writeMu.Unlock()
		if err != nil {
			return ErrEventHistory
		}
	}
	s.history = h
	return nil
}
func (h *EventHistory) stageLocked(s *Service, m protocol.Message, e messaging.Event) {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	if h.closed {
		h.stageErr = ErrEventHistory
	}
	if h.stageErr != nil {
		return
	}
	// One protocol response is bounded to 4MiB. These independent staging bounds
	// also prevent repeated failed commits from growing an unbounded memory queue.
	size := len(m.Sender) + len(m.Recipient) + len(m.Content) + len(m.Source)
	if len(h.pending) >= 250000 || h.pendingBytes+size > 32<<20 {
		h.stageErr = ErrEventHistory
		return
	}
	m.Sender = bytes.Clone(m.Sender)
	m.Recipient = bytes.Clone(m.Recipient)
	m.Content = bytes.Clone(m.Content)
	m.Source = bytes.Clone(m.Source)
	event := (BotAdapter{Service: s}).eventLocked(m, e.Sequence)
	event.LocalEcho = e.LocalEcho
	h.pending = append(h.pending, historyInput{account: s.data.Binding, namespace: historyNamespace(s.data), raw: m, event: event})
	h.pendingBytes += size
}
func historyEventKey(db *securedb.DB, account string, sequence uint64) []byte {
	prefix := db.Key("stream", account)
	var suffix [8]byte
	binary.BigEndian.PutUint64(suffix[:], sequence)
	return append(prefix, suffix[:]...)
}
func (h *EventHistory) meta(tx *securedb.Tx, account, namespace string) (historyMeta, bool, error) {
	raw, err := tx.Get("meta", h.db.Key("account", account))
	if err != nil {
		return historyMeta{}, false, err
	}
	if raw == nil {
		owner, err := tx.Get("meta", []byte("owner"))
		if err != nil || string(owner) == account {
			return historyMeta{}, false, ErrEventHistory
		}
		return historyMeta{Schema: 1, Account: account, Namespace: namespace}, false, nil
	}
	m, err := decodeHistoryMeta(raw)
	if err != nil || m.Account != account || (namespace != "" && m.Namespace != namespace) {
		return m, false, ErrEventHistory
	}
	return m, true, nil
}
func decodeHistoryMeta(raw []byte) (historyMeta, error) {
	var m historyMeta
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return m, ErrEventHistory
	}
	for _, name := range []string{"schema", "account", "namespace", "first", "last", "messages"} {
		if fields[name] == nil || string(fields[name]) == "null" {
			return m, ErrEventHistory
		}
	}
	if json.Unmarshal(raw, &m) != nil || !validHistoryMeta(m) {
		return m, ErrEventHistory
	}
	return m, nil
}
func (h *EventHistory) putMeta(tx *securedb.Tx, m historyMeta) error {
	owner, e := tx.Get("meta", []byte("owner"))
	if e != nil || (owner != nil && string(owner) != m.Account) {
		return ErrEventHistory
	}
	if e = tx.Put("meta", []byte("owner"), []byte(m.Account)); e != nil {
		return e
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return tx.Put("meta", h.db.Key("account", m.Account), raw)
}
func (h *EventHistory) record(tx *securedb.Tx, account, id string) (*historyRecord, error) {
	raw, err := h.readBody(tx, account, id)
	if err != nil || raw == nil {
		return nil, err
	}
	var row historyRecord
	if json.Unmarshal(raw, &row) != nil || row.Schema != 1 || row.Event.Account != account || row.Event.ID != id || !mediaID(id, 64) || !mediaID(row.Event.Conversation.ID, 32) || !mediaID(row.Event.Sender.ID, 32) {
		return nil, ErrEventHistory
	}
	return &row, nil
}
func (h *EventHistory) put(tx *securedb.Tx, row historyRecord) (bool, error) {
	old, err := h.record(tx, row.Event.Account, row.Event.ID)
	if err != nil {
		return false, err
	}
	if old != nil {
		return false, nil
	}
	raw, err := json.Marshal(row)
	if err != nil {
		return false, err
	}
	if err = h.writeBody(tx, row.Event.Account, row.Event.ID, raw); err != nil {
		return false, err
	}
	if row.Event.Sequence > 0 {
		key := historyEventKey(h.db, row.Event.Account, row.Event.Sequence)
		existing, err := tx.Get("events", key)
		if err != nil || existing != nil {
			return false, ErrEventHistory
		}
		value, _ := json.Marshal(row.Event.ID)
		if err = tx.Put("events", key, value); err != nil {
			return false, err
		}
	}
	return true, nil
}
func (h *EventHistory) importCheckpoint(data *store.Data) error {
	if !data.ValidBinding() || len(data.APIKey) != 32 {
		return ErrEventHistory
	}
	s := &Service{data: data, state: "offline"}
	adapter := BotAdapter{Service: s}
	return h.db.Update(func(tx *securedb.Tx) error {
		meta, exists, err := h.meta(tx, data.Binding, historyNamespace(data))
		if err != nil {
			return err
		}
		if exists {
			return nil
		}
		// Missing legacy bodies cannot be reconstructed. Preserve the contiguous
		// available suffix and its original numbers; earlier cursors fail explicitly.
		byID := map[string]protocol.Message{}
		for _, m := range data.Messages {
			byID[messageKey(m, data.Session.Bootstrap.Username)] = m
		}
		meta.First, meta.Last = data.EventSequence, data.EventSequence
		start := len(data.Events)
		expected := data.EventSequence
		seen := map[string]bool{}
		for i := len(data.Events) - 1; i >= 0; i-- {
			e := data.Events[i]
			_, ok := byID[e.Message]
			if !ok || seen[e.Message] || e.Account != data.Binding || e.Sequence != expected || expected == 0 {
				break
			}
			start = i
			seen[e.Message] = true
			meta.First = e.Sequence - 1
			expected--
		}
		sequences := map[string]uint64{}
		for _, e := range data.Events[start:] {
			sequences[e.Message] = e.Sequence
		}
		for id, m := range byID {
			e := adapter.eventLocked(m, sequences[id])
			e.LocalEcho = false
			if !mediaID(e.Conversation.ID, 32) || !mediaID(e.Sender.ID, 32) {
				continue
			}
			if added, err := h.put(tx, historyRecord{Schema: 1, Raw: m, Event: e}); err != nil {
				return err
			} else if added {
				meta.Messages++
			}
		}
		if err = h.putMeta(tx, meta); err != nil {
			return err
		}
		return tx.Put("meta", []byte("initialized"), []byte("1"))
	})
}
func (h *EventHistory) persistLocked(s *Service) error {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	if h.closed || h.stageErr != nil {
		h.unavailable = true
		return ErrEventHistory
	}
	write := h.write
	if write == nil {
		write = h.db.Update
	}
	err := write(func(tx *securedb.Tx) error {
		meta, exists, err := h.meta(tx, s.data.Binding, historyNamespace(s.data))
		if err != nil {
			return err
		}
		if !exists {
			meta.First, meta.Last = s.data.EventSequence, s.data.EventSequence
			if len(h.pending) > 0 {
				meta.First = h.pending[0].event.Sequence - 1
				meta.Last = meta.First
			}
		}
		for _, input := range h.pending {
			if input.account != s.data.Binding || input.namespace != meta.Namespace {
				return ErrEventHistory
			}
			old, err := h.record(tx, input.account, input.event.ID)
			if err != nil {
				return err
			}
			if old != nil {
				continue
			}
			if meta.Last == math.MaxUint64 {
				return ErrEventHistory
			}
			meta.Last++
			event := input.event
			event.Sequence = meta.Last
			if _, err = h.put(tx, historyRecord{Schema: 1, Event: event, Raw: input.raw}); err != nil {
				return err
			}
			meta.Messages++
		}
		return h.putMeta(tx, meta)
	})
	if err != nil {
		h.unavailable = true
		return ErrEventHistory
	}
	// Retain staged facts until the outside marker is durable. Retrying after an
	// ambiguous transaction or marker failure observes existing canonical IDs.
	if err = h.ensureGuard(s.data.Binding); err != nil {
		h.unavailable = true
		return ErrEventHistory
	}
	h.unavailable = false
	h.pending = nil
	h.pendingBytes = 0
	return nil
}
func (h *EventHistory) Stats(account string) (HistoryStats, error) {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	if h.closed || h.unavailable || h.stageErr != nil {
		return HistoryStats{}, ErrEventHistory
	}
	result := HistoryStats{Format: "encrypted_event_history_v1"}
	err := h.db.View(func(tx *securedb.Tx) error {
		if account == "" {
			return nil
		}
		meta, _, err := h.meta(tx, account, "")
		if err == nil && h.published.Account == account && meta != h.published {
			return ErrEventHistory
		}
		result.Messages, result.FirstCursor, result.Cursor = meta.Messages, meta.First, meta.Last
		return err
	})
	if err != nil {
		return result, ErrEventHistory
	}
	result.FileBytes, err = h.db.Size()
	if err != nil {
		return result, ErrEventHistory
	}
	return result, nil
}
func (h *EventHistory) events(account string, after uint64, limit int) ([]historyRecord, error) {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	if h.closed || h.unavailable || h.stageErr != nil {
		return nil, ErrEventHistory
	}
	result := []historyRecord{}
	if limit < 1 || limit > 100 {
		limit = 100
	}
	err := h.db.View(func(tx *securedb.Tx) error {
		meta, _, err := h.meta(tx, account, "")
		if err == nil && h.published.Account == account && meta != h.published {
			return ErrEventHistory
		}
		if err != nil {
			return err
		}
		if after < meta.First || after > meta.Last {
			return bot.ErrCursor
		}
		if after == meta.Last {
			return nil
		}
		prefix := h.db.Key("stream", account)
		expected := after + 1
		err = tx.Scan("events", prefix, historyEventKey(h.db, account, expected), limit+1, func(key, value []byte) (bool, error) {
			if len(key) != len(prefix)+8 || binary.BigEndian.Uint64(key[len(prefix):]) != expected {
				return false, ErrEventHistory
			}
			var id string
			if json.Unmarshal(value, &id) != nil {
				return false, ErrEventHistory
			}
			row, err := h.record(tx, account, id)
			if err != nil || row == nil || row.Event.Sequence != expected {
				return false, ErrEventHistory
			}
			result = append(result, *row)
			expected++
			return len(result) < limit && expected <= meta.Last, nil
		})
		if err == nil && len(result) < limit && expected <= meta.Last {
			return ErrEventHistory
		}
		return err
	})
	if errors.Is(err, bot.ErrCursor) {
		return nil, bot.ErrCursor
	}
	if err != nil {
		return nil, ErrEventHistory
	}
	return result, nil
}
func (h *EventHistory) message(account, id string) (*historyRecord, error) {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	if h.closed || h.unavailable || h.stageErr != nil {
		return nil, ErrEventHistory
	}
	var row *historyRecord
	err := h.db.View(func(tx *securedb.Tx) error {
		meta, _, err := h.meta(tx, account, "")
		if err != nil || (h.published.Account == account && meta != h.published) {
			return ErrEventHistory
		}
		row, err = h.record(tx, account, id)
		if row != nil && row.Event.Sequence > meta.Last {
			return ErrEventHistory
		}
		return err
	})
	if err != nil {
		return nil, ErrEventHistory
	}
	return row, nil
}
