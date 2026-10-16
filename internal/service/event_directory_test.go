package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/JFSAS/wechat-runtime/internal/protocol"
	"github.com/JFSAS/wechat-runtime/internal/securedb"
	"github.com/JFSAS/wechat-runtime/pkg/bot"
)

func TestDirectoryRetainsOldPrivateGroupMemberAndMentionAfterCompaction(t *testing.T) {
	a, h := historyFixture(t)
	s := a.Service
	nick := "Historical Friend"
	roomName := "Historical Room"
	s.ingestLocked(protocol.SyncResult{Contacts: []protocol.Contact{{Username: []byte("old-friend"), Nickname: &nick}, {Username: []byte("old@chatroom"), Nickname: &roomName}}, Messages: []protocol.Message{
		{Sender: []byte("old-friend"), Recipient: s.data.Session.Bootstrap.Username, Content: []byte("private"), Type: 1, Timestamp: 1700000000, ServerID: 1},
		{Sender: []byte("old@chatroom"), Recipient: s.data.Session.Bootstrap.Username, Content: []byte("old-friend:\ngroup"), Type: 1, Timestamp: 1700000001, ServerID: 2},
	}})
	if err := s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	extra := historyMessages(s, 600)
	for i := range extra {
		extra[i].ServerID += 10000
	}
	s.ingestLocked(protocol.SyncResult{Messages: extra})
	if err := s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	if len(s.data.Messages) > historyWorkingEntries || len(s.data.Events) > historyWorkingEntries || h.workingBytes > historyWorkingBytes {
		t.Fatal("native working set did not compact")
	}
	friend, room := s.sessionID([]byte("old-friend")), s.sessionID([]byte("old@chatroom"))
	user, err := a.User(s.data.Binding, friend)
	if err != nil || user.Name != nick {
		t.Fatal(user, err)
	}
	conversation, err := a.Conversation(s.data.Binding, room)
	if err != nil || conversation.Name != roomName {
		t.Fatal(conversation, err)
	}
	roster, err := a.Members(s.data.Binding, room)
	if err != nil || roster.Complete {
		t.Fatal(roster, err)
	}
	found := false
	for _, member := range roster.Users {
		if member.ID == friend && member.Name == nick {
			found = true
		}
	}
	if !found {
		t.Fatal("observed group member disappeared")
	}
	request := bot.Send{Account: s.data.Binding, Epoch: s.modeEpoch, Conversation: room, Key: "old-room-mention", Text: "reply", Mentions: []string{friend}}
	sent, err := a.Submit(context.Background(), request)
	if err != nil {
		t.Fatal("old group or member could not be used", err)
	}
	job := s.data.Jobs[sent.ID]
	mentions, err := protocol.ParseMentions([]byte(job.Source))
	if err != nil || len(mentions) != 1 || mentions[0] != "old-friend" || job.Content != "@"+nick+"\u2005reply" {
		t.Fatal("native mention encoding changed", job.Source, err)
	}
	request.Key = "foreign-member"
	request.Mentions = []string{s.sessionID([]byte("synthetic-friend"))}
	if _, err = a.Submit(context.Background(), request); !errors.Is(err, bot.ErrNotFound) {
		t.Fatal("unobserved member accepted", err)
	}
	if _, err = a.User("foreign", friend); !errors.Is(err, bot.ErrScope) {
		t.Fatal(err)
	}
	if err = h.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenEventHistory(s.directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restarted := New(s.directory, s.interval, s.source.App)
	if err = restarted.prepare(false, false); err != nil {
		t.Fatal(err)
	}
	next := BotAdapter{Service: restarted}
	if err = next.EnableHistory(reopened); err != nil {
		t.Fatal(err)
	}
	got, err := next.User(restarted.data.Binding, friend)
	if err != nil || got.ID != friend || got.Name != nick {
		t.Fatal("directory restart lost identity", got, err)
	}
}
func TestDirectoryDeletionAndRenameAreDurableFactsNotMessageCacheHints(t *testing.T) {
	a, h := historyFixture(t)
	s := a.Service
	first := "First Name"
	second := "Second Name"
	s.ingestLocked(protocol.SyncResult{Contacts: []protocol.Contact{{Username: []byte("friend"), Nickname: &first}}})
	if err := s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	old, err := os.ReadFile(filepath.Join(s.checkpoint.Directory, s.data.Binding+".checkpoint"))
	if err != nil {
		t.Fatal(err)
	}
	s.ingestLocked(protocol.SyncResult{Contacts: []protocol.Contact{{Username: []byte("friend"), Nickname: &second}}})
	if err = s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	s.ingestLocked(protocol.SyncResult{Contacts: []protocol.Contact{{Username: []byte("friend"), Deleted: true}}})
	if err = s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	id := s.sessionID([]byte("friend"))
	user, err := a.User(s.data.Binding, id)
	if err != nil || user.Name != second {
		t.Fatal(user, err)
	}
	conversation, err := a.Conversation(s.data.Binding, id)
	if err != nil || !conversation.Unavailable {
		t.Fatal(conversation, err)
	}
	if _, err = a.Submit(context.Background(), bot.Send{Account: s.data.Binding, Epoch: s.modeEpoch, Conversation: id, Key: "deleted", Text: "must fail"}); !errors.Is(err, bot.ErrNotFound) {
		t.Fatal(err)
	}
	if err = h.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(s.checkpoint.Directory, s.data.Binding+".checkpoint"), old, 0600); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenEventHistory(s.directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restarted := New(s.directory, s.interval, s.source.App)
	if err = restarted.prepare(false, false); err != nil {
		t.Fatal(err)
	}
	next := BotAdapter{Service: restarted}
	if err = next.EnableHistory(reopened); err != nil {
		t.Fatal(err)
	}
	user, err = next.User(restarted.data.Binding, id)
	if err != nil || user.Name != second {
		t.Fatal("old checkpoint reverted directory name", user, err)
	}
	conversation, err = next.Conversation(restarted.data.Binding, id)
	if err != nil || !conversation.Unavailable {
		t.Fatal("old checkpoint resurrected deleted contact", conversation, err)
	}
}
func TestDirectoryCorruptionDoesNotBecomeMissingOrWrongRecipient(t *testing.T) {
	a, h := historyFixture(t)
	s := a.Service
	name := "Friend"
	s.ingestLocked(protocol.SyncResult{Contacts: []protocol.Contact{{Username: []byte("friend"), Nickname: &name}}})
	if err := s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	id := s.sessionID([]byte("friend"))
	err := h.db.Update(func(tx *securedb.Tx) error {
		entry, err := h.directoryEntry(tx, "conversations", s.data.Binding, id)
		if err != nil {
			return err
		}
		entry.Raw = []byte("wrong-recipient")
		raw, _ := json.Marshal(entry)
		return h.directoryLocation("conversations", s.data.Binding, id).write(tx, raw)
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.Conversation(s.data.Binding, id); !errors.Is(err, bot.ErrStorage) {
		t.Fatal(err)
	}
	if _, err = a.Submit(context.Background(), bot.Send{Account: s.data.Binding, Epoch: s.modeEpoch, Conversation: id, Key: "wrong", Text: "must fail"}); !errors.Is(err, bot.ErrStorage) {
		t.Fatal(err)
	}
	if len(s.data.Jobs) != 0 {
		t.Fatal("corrupt directory queued a send")
	}
}
func TestWorkingSetRetainsUnsupportedRawVariantsAndLegacyFragments(t *testing.T) {
	a, h := historyFixture(t)
	s := a.Service
	messages := historyMessages(s, 700)
	messages[0].Type = 987
	s.ingestLocked(protocol.SyncResult{Messages: messages})
	if err := s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	id := messageKey(messages[0], s.data.Session.Bootstrap.Username)
	stored, err := h.message(s.data.Binding, id)
	if err != nil || stored == nil || stored.Raw.Type != 987 || stored.Event.Sequence != 0 {
		t.Fatal("unsupported raw fact lost or fabricated event", err)
	}
	changed := messages[0]
	changed.Content = []byte("updated raw variant")
	s.ingestLocked(protocol.SyncResult{Messages: []protocol.Message{changed}})
	if err = s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	var variant []byte
	err = h.db.View(func(tx *securedb.Tx) error {
		var err error
		variant, err = h.privateFactLocation("raw-variants", s.data.Binding, id+"/"+rawDigest(changed)).read(tx)
		return err
	})
	expected, _ := json.Marshal(changed)
	if err != nil || !bytes.Equal(variant, expected) {
		t.Fatal("raw variant not retained", err)
	}
	if h.workingBytes > historyWorkingBytes || len(s.data.Messages) > 512 || len(s.data.Events) > 512 {
		t.Fatal("cache bounds exceeded")
	}
}
func TestDirectoryFailureCannotPruneNativeWorkingSet(t *testing.T) {
	a, h := historyFixture(t)
	s := a.Service
	s.ingestLocked(protocol.SyncResult{Messages: historyMessages(s, 600)})
	before := len(s.data.Messages)
	h.guardWrite = func([]byte) error { return errors.New("synthetic outside manifest failure") }
	if err := s.saveLocked(); !errors.Is(err, bot.ErrStorage) {
		t.Fatal(err)
	}
	if len(s.data.Messages) != before {
		t.Fatal("unanchored facts were pruned")
	}
	h.guardWrite = nil
	if err := s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	if len(s.data.Messages) > 512 {
		t.Fatal("retry did not compact")
	}
}
func TestWorkingSetByteBudgetRetainsLargeNativeBodiesInHistory(t *testing.T) {
	a, h := historyFixture(t)
	s := a.Service
	messages := historyMessages(s, 4)
	for i := range messages {
		messages[i].Content = bytes.Repeat([]byte("<"), 600<<10)
	}
	s.ingestLocked(protocol.SyncResult{Messages: messages})
	if err := s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(struct {
		Messages any
		Events   any
	}{s.data.Messages, s.data.Events})
	if len(raw) > historyWorkingBytes || h.workingBytes > historyWorkingBytes {
		t.Fatal("serialized native cache exceeds budget", len(raw))
	}
	events, err := a.Events(s.data.Binding, 0, 100)
	if err != nil || len(events) != 4 {
		t.Fatal("large native messages were lost", len(events), err)
	}
	for _, event := range events {
		if len(event.Message[0].Data["text"]) != 600<<10 {
			t.Fatal("large body changed")
		}
	}
	t.Logf("WORKING_SET_LOAD_REPORT={\"input_messages\":4,\"native_messages\":%d,\"native_events\":%d,\"serialized_cache_bytes\":%d,\"budget_bytes\":%d,\"history_events\":%d,\"live_wechat_requests\":0}", len(s.data.Messages), len(s.data.Events), len(raw), historyWorkingBytes, len(events))
	t.Run("encoded-boundary", func(t *testing.T) {
		a, h := historyFixture(t)
		s := a.Service
		message := historyMessages(s, 1)[0]
		message.Content = []byte{}
		base, _ := json.Marshal(message)
		message.Content = bytes.Repeat([]byte("x"), (historyWorkingBytes-len(base)-8)/4*3)
		encoded, _ := json.Marshal(message)
		if len(encoded) >= historyWorkingBytes || len(encoded) < historyWorkingBytes-16 {
			t.Fatal("fixture must place one native record near the serialized boundary", len(encoded))
		}
		s.ingestLocked(protocol.SyncResult{Messages: []protocol.Message{message}})
		if err := s.saveLocked(); err != nil {
			t.Fatal(err)
		}
		cache, _ := json.Marshal(struct {
			Messages any
			Events   any
		}{s.data.Messages, s.data.Events})
		if len(cache) > historyWorkingBytes || h.workingBytes > historyWorkingBytes {
			t.Fatal("JSON framing exceeded native working-set budget", len(cache), h.workingBytes)
		}
		events, err := a.Events(s.data.Binding, 0, 10)
		if err != nil || len(events) != 1 || events[0].Message[0].Data["text"] != string(message.Content) {
			t.Fatal("boundary message was not retained in history", err)
		}
	})
}

func TestDirectoryMigratesVersionOneHistoryBeyondRetainedCheckpoint(t *testing.T) {
	s := recoveryFixture(t)
	nick := "Captured Friendly Name"
	messages := []protocol.Message{
		{Sender: []byte("old-friend"), Recipient: s.data.Session.Bootstrap.Username, Content: []byte("private"), Type: 1, Timestamp: 1700000000, ServerID: 1},
		{Sender: []byte("one@chatroom"), Recipient: s.data.Session.Bootstrap.Username, Content: []byte("old-friend:\none"), Type: 1, Timestamp: 1700000001, ServerID: 2},
		{Sender: []byte("two@chatroom"), Recipient: s.data.Session.Bootstrap.Username, Content: []byte("old-friend:\ntwo"), Type: 1, Timestamp: 1700000002, ServerID: 3},
	}
	s.ingestLocked(protocol.SyncResult{Contacts: []protocol.Contact{{Username: []byte("old-friend"), Nickname: &nick}}, Messages: messages})
	events := []bot.Event{}
	adapter := BotAdapter{Service: s}
	for i, message := range messages {
		events = append(events, adapter.eventLocked(message, uint64(i+1)))
	}
	// Build the actual v1 shape: no directory or reversible-message-ID bucket,
	// all normalized history present, checkpoint retains only its newest body.
	s.data.Messages = s.data.Messages[2:]
	s.data.Events = s.data.Events[2:]
	s.data.Contacts = nil
	if err := s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	db, err := securedb.Open(filepath.Join(s.directory, "event-history"), "events")
	if err != nil {
		t.Fatal(err)
	}
	legacy := &EventHistory{db: db, directory: s.directory, identity: string(bytes.Repeat([]byte("a"), 64))}
	meta := historyMeta{Schema: 1, Account: s.data.Binding, Namespace: historyNamespace(s.data), First: 0, Last: 3, Messages: 3}
	err = db.Update(func(tx *securedb.Tx) error {
		if err := tx.Put("meta", []byte("identity"), []byte(legacy.identity)); err != nil {
			return err
		}
		for i, message := range messages {
			row := historyRecord{Schema: 1, Raw: message, Event: events[i]}
			raw, _ := json.Marshal(row)
			if err := legacy.writeBody(tx, s.data.Binding, row.Event.ID, raw); err != nil {
				return err
			}
			value, _ := json.Marshal(row.Event.ID)
			if err := tx.Put("events", historyEventKey(db, s.data.Binding, uint64(i+1)), value); err != nil {
				return err
			}
		}
		return legacy.putMeta(tx, meta)
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(struct {
		Schema   int         `json:"schema"`
		Identity string      `json:"identity"`
		Meta     historyMeta `json:"meta"`
	}{1, legacy.identity, meta})
	if err = os.WriteFile(filepath.Join(s.directory, "event-history.required"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	migrated, err := OpenEventHistory(s.directory)
	if err != nil {
		t.Fatal("v1 history upgrade failed", err)
	}
	defer migrated.Close()
	if err = adapter.EnableHistory(migrated); err != nil {
		t.Fatal(err)
	}
	friend := s.sessionID([]byte("old-friend"))
	user, err := adapter.User(s.data.Binding, friend)
	if err != nil || user.Name != nick {
		t.Fatal("old history name was not migrated", user, err)
	}
	for _, room := range []string{"one@chatroom", "two@chatroom"} {
		members, err := adapter.Members(s.data.Binding, s.sessionID([]byte(room)))
		if err != nil {
			t.Fatal(room, err)
		}
		found := false
		for _, member := range members.Users {
			if member.ID == friend {
				found = true
			}
		}
		if !found {
			t.Fatal("old observed member absent after migration", room)
		}
	}
	restored, err := adapter.Events(s.data.Binding, 0, 100)
	if err != nil || len(restored) != 3 {
		t.Fatal(restored, err)
	}
	for i, event := range restored {
		if event.ID != events[i].ID || event.Sequence != events[i].Sequence {
			t.Fatal("migration renumbered history")
		}
	}
	persisted, err := os.ReadFile(filepath.Join(s.directory, "event-history.required"))
	if err != nil {
		t.Fatal(err)
	}
	var guard historyGuard
	if json.Unmarshal(persisted, &guard) != nil || guard.Meta.Schema != 2 || guard.DirectoryRevision == 0 {
		t.Fatal("new guard missing format/revision")
	}
}
func TestDirectoryCommittedDeletionFencesQueuedWorkFromOlderCheckpoint(t *testing.T) {
	a, h := historyFixture(t)
	s := a.Service
	nick := "Friend"
	s.ingestLocked(protocol.SyncResult{Contacts: []protocol.Contact{{Username: []byte("friend"), Nickname: &nick}}})
	if err := s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	delivery, err := a.Submit(context.Background(), bot.Send{Account: s.data.Binding, Epoch: s.modeEpoch, Conversation: s.sessionID([]byte("friend")), Key: "queued-before-delete", Text: "must not dispatch"})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(s.checkpoint.Directory, s.data.Binding+".checkpoint")
	old, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s.ingestLocked(protocol.SyncResult{Contacts: []protocol.Contact{{Username: []byte("friend"), Deleted: true}}})
	if err = s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	if err = h.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, old, 0600); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenEventHistory(s.directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restarted := New(s.directory, s.interval, s.source.App)
	next := BotAdapter{Service: restarted}
	if err = next.EnableHistory(reopened); err != nil {
		t.Fatal(err)
	}
	if err = restarted.prepare(false, false); err != nil {
		t.Fatal(err)
	}
	unavailable := false
	for _, peer := range restarted.data.Unavailable {
		if string(peer) == "friend" {
			unavailable = true
		}
	}
	if !unavailable {
		t.Fatal("queued recipient was not fenced before worker start")
	}
	restarted.state, restarted.mode = "connected", "online"
	if err = restarted.sendQueued(context.Background()); err != nil {
		t.Fatal(err)
	}
	if restarted.data.Jobs[delivery.ID].Status != "failed" || restarted.Status()["send_calls"] != uint64(0) {
		t.Fatal("deleted recipient reached protocol transport")
	}
}
func TestDirectoryPublishedRevisionRejectsAnOlderDatabaseWithoutNewMessages(t *testing.T) {
	a, h := historyFixture(t)
	s := a.Service
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(s.directory, "event-history", "events.db")
	old, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	h, err = OpenEventHistory(s.directory)
	if err != nil {
		t.Fatal(err)
	}
	s.history = h
	nick := "New contact without any new event"
	s.ingestLocked(protocol.SyncResult{Contacts: []protocol.Contact{{Username: []byte("friend"), Nickname: &nick}}})
	if err = s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	if a.Snapshot().Cursor != 0 {
		t.Fatal("contact update invented an event")
	}
	if err = h.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, old, 0600); err != nil {
		t.Fatal(err)
	}
	if reopened, err := OpenEventHistory(s.directory); err == nil {
		reopened.Close()
		t.Fatal("old directory copy silently accepted with matching event counters")
	}
}

func TestDirectoryMessageObservationPreservesPartialContactMerge(t *testing.T) {
	a, _ := historyFixture(t)
	s := a.Service
	nick, remark, renamed := "Alice", "Boss", "Alicia"
	s.ingestLocked(protocol.SyncResult{Contacts: []protocol.Contact{{Username: []byte("friend"), Nickname: &nick, Remark: &remark}}})
	if err := s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	s.ingestLocked(protocol.SyncResult{Messages: []protocol.Message{{Sender: []byte("friend"), Recipient: s.data.Session.Bootstrap.Username, Content: []byte("hello"), Type: 1, ServerID: 1}}})
	if err := s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	s.ingestLocked(protocol.SyncResult{Contacts: []protocol.Contact{{Username: []byte("friend"), Nickname: &renamed}}})
	if err := s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	user, err := a.User(s.data.Binding, s.sessionID([]byte("friend")))
	if err != nil || user.Name != remark {
		t.Fatal("message observation discarded native contact merge facts", user, err)
	}
}
func TestWorkingSetRetainsConflictingRawVariantsWithinSingleSync(t *testing.T) {
	a, h := historyFixture(t)
	s := a.Service
	first := historyMessages(s, 1)[0]
	second := first
	second.Content = []byte("different body in same sync")
	s.ingestLocked(protocol.SyncResult{Messages: []protocol.Message{first, second}})
	if err := s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	events, err := a.Events(s.data.Binding, 0, 100)
	if err != nil || len(events) != 1 || events[0].Message[0].Data["text"] != string(first.Content) {
		t.Fatal("public event duplicated or paired with wrong raw", events, err)
	}
	id := messageKey(first, s.data.Session.Bootstrap.Username)
	var variant []byte
	err = h.db.View(func(tx *securedb.Tx) error {
		var e error
		variant, e = h.privateFactLocation("raw-variants", s.data.Binding, id+"/"+rawDigest(second)).read(tx)
		return e
	})
	expected, _ := json.Marshal(second)
	if err != nil || !bytes.Equal(variant, expected) {
		t.Fatal("same-sync raw variant lost", err)
	}
	var indexed string
	err = h.db.View(func(tx *securedb.Tx) error {
		idref := id + "/" + rawDigest(second)
		raw, e := tx.Get("native-fact-ids", append(h.db.Key("native-fact-ids", "raw-variants", s.data.Binding), h.db.Key("id", idref)...))
		if e != nil {
			return e
		}
		return json.Unmarshal(raw, &indexed)
	})
	if err != nil || indexed != id+"/"+rawDigest(second) {
		t.Fatal("retained native variant cannot be enumerated after restart", err)
	}
}
func TestDirectoryEnumerationBoundsDeletedRowsBeforeFiltering(t *testing.T) {
	a, h := historyFixture(t)
	s := a.Service
	h.writeMu.Lock()
	err := h.db.Update(func(tx *securedb.Tx) error {
		meta, _, err := h.directoryMeta(tx, s.data.Binding, historyNamespace(s.data))
		if err != nil {
			return err
		}
		for i := 0; i < directoryListLimit; i++ {
			raw := []byte(fmt.Sprintf("deleted-%d", i))
			entry := directoryEntry{Schema: 1, Account: s.data.Binding, ID: s.sessionID(raw), Raw: raw, Name: "deleted", Kind: "private", Available: false}
			if err = h.putDirectoryEntry(tx, &meta, "conversations", entry); err != nil {
				return err
			}
		}
		return h.putDirectoryMeta(tx, meta)
	})
	if err == nil {
		err = h.ensureGuard(s.data.Binding)
	}
	h.writeMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	rows, err := a.Conversations()
	if !errors.Is(err, bot.ErrLimit) || rows != nil {
		t.Fatal("deleted rows bypassed traversal budget", len(rows), err)
	}
}

func TestDirectoryReaddedContactClearsOldCheckpointAvailabilityFence(t *testing.T) {
	a, h := historyFixture(t)
	s := a.Service
	nick := "Friend"
	s.ingestLocked(protocol.SyncResult{Contacts: []protocol.Contact{{Username: []byte("friend"), Nickname: &nick}}})
	if err := s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	delivery, err := a.Submit(context.Background(), bot.Send{Account: s.data.Binding, Epoch: s.modeEpoch, Conversation: s.sessionID([]byte("friend")), Key: "queued-before-remove", Text: "still queued"})
	if err != nil {
		t.Fatal(err)
	}
	s.ingestLocked(protocol.SyncResult{Contacts: []protocol.Contact{{Username: []byte("friend"), Deleted: true}}})
	if err = s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(s.checkpoint.Directory, s.data.Binding+".checkpoint")
	removed, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s.ingestLocked(protocol.SyncResult{Contacts: []protocol.Contact{{Username: []byte("friend"), Nickname: &nick}}})
	if err = s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	if err = h.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, removed, 0600); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenEventHistory(s.directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restarted := New(s.directory, s.interval, s.source.App)
	next := BotAdapter{Service: restarted}
	if err = next.EnableHistory(reopened); err != nil {
		t.Fatal(err)
	}
	if err = restarted.prepare(false, false); err != nil {
		t.Fatal(err)
	}
	for _, peer := range restarted.data.Unavailable {
		if string(peer) == "friend" {
			t.Fatal("explicit re-add left stale unavailable fence")
		}
	}
	if restarted.data.Jobs[delivery.ID].Status != "queued" {
		t.Fatal("availability reconciliation changed queued outcome")
	}
	// Do not run a protocol worker; the corrected projection is what native
	// sendQueued consults before choosing whether to reject a queued recipient.
}
func TestDirectoryMemberEnumerationBoundsEscapedNamesAndPointLookupIsIndependent(t *testing.T) {
	a, _ := historyFixture(t)
	s := a.Service
	huge := string(bytes.Repeat([]byte("<"), 3<<20))
	small := "Small"
	s.ingestLocked(protocol.SyncResult{Contacts: []protocol.Contact{{Username: []byte("huge"), Nickname: &huge}, {Username: []byte("small"), Nickname: &small}}, Messages: []protocol.Message{
		{Sender: []byte("room@chatroom"), Recipient: s.data.Session.Bootstrap.Username, Content: []byte("huge:\nhello"), Type: 1, ServerID: 1},
		{Sender: []byte("room@chatroom"), Recipient: s.data.Session.Bootstrap.Username, Content: []byte("small:\nhello"), Type: 1, ServerID: 2},
	}})
	if err := s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	room := s.sessionID([]byte("room@chatroom"))
	roster, err := a.Members(s.data.Binding, room)
	if !errors.Is(err, bot.ErrLimit) || len(roster.Users) != 0 {
		t.Fatal("escaped member name exceeded enumeration budget", err)
	}
	member, err := a.Member(s.data.Binding, room, s.sessionID([]byte("small")))
	if err != nil || member.User.Name != small || member.Complete {
		t.Fatal("single-member lookup depended on oversized group enumeration", member, err)
	}
}
