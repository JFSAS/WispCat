package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JFSAS/wechat-runtime/internal/protocol"
	"github.com/JFSAS/wechat-runtime/internal/securedb"
	"github.com/JFSAS/wechat-runtime/internal/store"
	"github.com/JFSAS/wechat-runtime/pkg/bot"
)

func checkpointContacts(s *Service, start, count, nameBytes int) []protocol.Contact {
	contacts := make([]protocol.Contact, count)
	for i := range contacts {
		name := fmt.Sprintf("name-%05d-", start+i) + strings.Repeat("n", nameBytes)
		remark := fmt.Sprintf("remark-%05d", start+i)
		contacts[i] = protocol.Contact{Username: []byte(fmt.Sprintf("contact-%05d", start+i)), Nickname: &name, Remark: &remark}
	}
	return contacts
}

func externalContactFixture(t *testing.T) (BotAdapter, *EventHistory) {
	t.Helper()
	a, h := historyFixture(t)
	a.Service.ingestLocked(protocol.SyncResult{Contacts: checkpointContacts(a.Service, 0, 300, 4096)})
	if err := a.Service.saveLocked(); err != nil {
		t.Fatal(err)
	}
	return a, h
}

func reopenContactService(t *testing.T, old *Service, h *EventHistory) *Service {
	t.Helper()
	next := New(old.directory, old.interval, old.source.App)
	if err := (BotAdapter{Service: next}).EnableHistory(h); err != nil {
		t.Fatal(err)
	}
	if err := next.prepare(false, false); err != nil {
		t.Fatal(err)
	}
	return next
}

func TestContactCheckpointCrossesNativeSizeLimitAndReopensCompleteRoster(t *testing.T) {
	a, h := historyFixture(t)
	s := a.Service
	started := time.Now()
	const count = 10050
	for at := 0; at < count; at += 1000 {
		n := min(1000, count-at)
		s.ingestLocked(protocol.SyncResult{Contacts: checkpointContacts(s, at, n, 3400)})
		if err := s.saveLocked(); err != nil {
			t.Fatal(at, err)
		}
	}
	full, err := json.Marshal(s.data.Contacts)
	if err != nil || len(full) <= 32<<20 || len(s.data.Contacts) != count {
		t.Fatal("fixture did not exceed inherited contact checkpoint capacity", len(full), len(s.data.Contacts), err)
	}
	legacy := store.Checkpoint{Directory: filepath.Join(t.TempDir(), "legacy"), Binding: s.data.Binding}
	if err := legacy.Save(s.data); err == nil || !strings.Contains(err.Error(), "size bound") {
		t.Fatal("unmodified native serializer accepted oversized whole state", err)
	}
	durable, err := s.checkpoint.Load()
	if err != nil || serializedContactCheckpoint(durable) != nil || len(durable.Contacts) != 1 {
		t.Fatal("native checkpoint retained full roster", err)
	}
	marker, err := readContactCheckpoint(durable)
	if err != nil || marker == nil || marker.Count != count {
		t.Fatal(marker, err)
	}
	info, err := os.Stat(filepath.Join(s.checkpoint.Directory, s.data.Binding+".checkpoint"))
	if err != nil || info.Size() >= 64<<10 {
		t.Fatal("contact bytes still rewritten in native checkpoint", info, err)
	}
	before, _, _ := contactFingerprint(s.data.Contacts)
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenEventHistory(s.directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	next := reopenContactService(t, s, reopened)
	after, _, err := contactFingerprint(next.data.Contacts)
	if err != nil || len(next.data.Contacts) != count || before != after {
		t.Fatal("full native roster changed across restart", len(next.data.Contacts), err)
	}
	response, err := next.Contacts("contact-00000")
	if err != nil || response["total"] != 1 {
		t.Fatal("inherited contact search lost old row", response["total"], err)
	}
	user, err := (BotAdapter{Service: next}).User(next.data.Binding, next.sessionID([]byte("contact-00000")))
	if err != nil || user.Name != "remark-00000" {
		t.Fatal("durable normalized ID/label lost", err)
	}
	if _, err = (BotAdapter{Service: next}).User("foreign", user.ID); !errors.Is(err, bot.ErrScope) {
		t.Fatal(err)
	}
	t.Logf("CONTACT_CHECKPOINT_LOAD_REPORT={\"contacts\":%d,\"encoded_contact_bytes\":%d,\"native_checkpoint_bytes\":%d,\"complete_roster_reopened\":true,\"inline_bytes_limit\":%d,\"elapsed_millis\":%d,\"live_wechat_requests\":0}", count, len(full), info.Size(), contactInlineBytes, time.Since(started).Milliseconds())
}

func TestContactCheckpointFailureRestoresLiveRosterAndReopensNewerFacts(t *testing.T) {
	a, h := externalContactFixture(t)
	s := a.Service
	path := filepath.Join(s.checkpoint.Directory, s.data.Binding+".checkpoint")
	if err := os.Rename(path, path+".fixture-backup"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	name := "new nickname"
	s.ingestLocked(protocol.SyncResult{Contacts: []protocol.Contact{{Username: []byte("contact-00000"), Nickname: &name}, {Username: []byte("contact-00001"), Deleted: true}}})
	if err := s.saveLocked(); err == nil || len(s.data.Contacts) != 299 || *s.data.Contacts[0].Nickname != name || *s.data.Contacts[0].Remark != "remark-00000" {
		t.Fatal("checkpoint error hid or modified the live native merge", err, len(s.data.Contacts))
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".fixture-backup", path); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenEventHistory(s.directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	next := reopenContactService(t, s, reopened)
	if len(next.data.Contacts) != 299 || *next.data.Contacts[0].Nickname != name || *next.data.Contacts[0].Remark != "remark-00000" {
		t.Fatal("older checkpoint overwrote newer committed contacts")
	}
	room, err := (BotAdapter{Service: next}).Conversation(next.data.Binding, next.sessionID([]byte("contact-00001")))
	if err != nil || !room.Unavailable {
		t.Fatal("deleted contact resurrected", room, err)
	}
	seen := false
	for _, peer := range next.data.Unavailable {
		seen = seen || string(peer) == "contact-00001"
	}
	if !seen {
		t.Fatal("native send fence not restored")
	}
	next.client = &protocol.Client{Bootstrap: next.data.Session.Bootstrap, State: cloneSession(next.data.Session)}
	next.ingestLocked(protocol.SyncResult{Contacts: []protocol.Contact{{Username: []byte("contact-00001")}}})
	if err = next.saveLocked(); err != nil || len(next.data.Contacts) != 300 {
		t.Fatal("re-addition failed", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	third, err := OpenEventHistory(next.directory)
	if err != nil {
		t.Fatal("delete/re-add produced an unreopenable roster", err)
	}
	defer third.Close()
	last := reopenContactService(t, next, third)
	if len(last.data.Contacts) != 300 || string(last.data.Contacts[1].Username) != "contact-00001" || last.data.Contacts[1].Nickname != nil || last.data.Contacts[1].Remark != nil {
		t.Fatal("re-added contact inherited fields from its deleted native record")
	}
}

func TestContactCheckpointMarkerAndMissingRowsFailBeforeWorkers(t *testing.T) {
	for _, kind := range []string{"null", "missing_marker", "missing_sentinel", "malformed_sentinel", "missing_field", "unknown_field", "identity", "namespace", "count", "digest", "missing_row"} {
		t.Run(kind, func(t *testing.T) {
			a, h := externalContactFixture(t)
			s := a.Service
			data, err := s.checkpoint.Load()
			if err != nil {
				t.Fatal(err)
			}
			var marker map[string]any
			if err := json.Unmarshal(data.Consumers[contactCheckpointKey], &marker); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "null":
				data.Consumers[contactCheckpointKey] = json.RawMessage("null")
			case "missing_marker":
				delete(data.Consumers, contactCheckpointKey)
			case "missing_sentinel":
				data.Contacts = nil
			case "malformed_sentinel":
				name := "not local metadata"
				data.Contacts[0].Nickname = &name
			case "missing_field":
				delete(marker, "count")
			case "unknown_field":
				marker["unexpected"] = true
			case "identity", "namespace", "digest":
				marker[kind] = strings.Repeat("a", 64)
			case "count":
				marker["count"] = 1
			case "missing_row":
				err = h.db.Update(func(tx *securedb.Tx) error {
					return tx.Delete("conversations", h.directoryLocation("conversations", s.data.Binding, s.sessionID([]byte("contact-00000"))).key)
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			if kind != "null" && kind != "missing_marker" {
				data.Consumers[contactCheckpointKey], _ = json.Marshal(marker)
			}
			if err = s.checkpoint.Save(data); err != nil {
				t.Fatal(err)
			}
			if err = h.Close(); err != nil {
				t.Fatal(err)
			}
			bad, err := OpenEventHistory(s.directory)
			if err == nil {
				_ = bad.Close()
				t.Fatal("invalid dependency silently became an empty roster")
			}
		})
	}
}

func TestContactCheckpointDependencyAndOtherConsumerState(t *testing.T) {
	a, _ := historyFixture(t)
	s := a.Service
	s.data.Consumers = map[string]json.RawMessage{"external-example": json.RawMessage(`{"revision":7}`)}
	// This legal old name was used by an initial candidate; it remains a normal
	// consumer slot. The actual private marker exceeds the native writer's bound.
	if err := s.WriteState(s.data.Binding, "wechat-runtime/contact-checkpoint", json.RawMessage(`{"legacy":true}`)); err != nil {
		t.Fatal("valid legacy consumer name became reserved", err)
	}
	before := len(s.data.Consumers)
	if len(contactCheckpointKey) <= 64 || s.WriteState(s.data.Binding, contactCheckpointKey, json.RawMessage(`{}`)) == nil || len(s.data.Consumers) != before {
		t.Fatal("private marker namespace admitted a public writer")
	}
	s.ingestLocked(protocol.SyncResult{Contacts: checkpointContacts(s, 0, 300, 4096)})
	if err := s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	data, err := s.checkpoint.Load()
	if err != nil || !bytes.Equal(data.Consumers["external-example"], s.data.Consumers["external-example"]) {
		t.Fatal("unrelated consumer state changed", err)
	}
	if !bytes.Equal(data.Consumers["wechat-runtime/contact-checkpoint"], json.RawMessage(`{"legacy":true}`)) {
		t.Fatal("legal old consumer state was changed by externalization")
	}
	if s.WriteState(s.data.Binding, contactCheckpointKey, json.RawMessage(`{}`)) == nil {
		t.Fatal("public writer replaced existing directory dependency")
	}
	if err := s.saveLocked(); err != nil {
		t.Fatal("rejected public write poisoned future saves", err)
	}
	nativeOnly := New(s.directory, s.interval, s.source.App)
	if err := nativeOnly.prepare(false, false); !errors.Is(err, ErrEventHistory) {
		t.Fatal("external roster opened without its owner", err)
	}
}

func TestContactCheckpointInlinePathPreservesNativeCheckpoint(t *testing.T) {
	a, _ := historyFixture(t)
	s := a.Service
	s.ingestLocked(protocol.SyncResult{Contacts: checkpointContacts(s, 0, 3, 10)})
	if err := s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	data, err := s.checkpoint.Load()
	if err != nil || len(data.Contacts) != 3 || data.Consumers[contactCheckpointKey] != nil {
		t.Fatal("small legacy-compatible roster was externalized", err)
	}
	nativeOnly := New(s.directory, s.interval, s.source.App)
	if err := nativeOnly.prepare(false, false); err != nil || len(nativeOnly.data.Contacts) != 3 {
		t.Fatal("inline native restore changed", err)
	}
}

func TestContactCheckpointHistoryFailureCannotOmitUncommittedRoster(t *testing.T) {
	a, h := historyFixture(t)
	s := a.Service
	path := filepath.Join(s.checkpoint.Directory, s.data.Binding+".checkpoint")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s.ingestLocked(protocol.SyncResult{Contacts: checkpointContacts(s, 0, 300, 4096)})
	h.write = func(func(*securedb.Tx) error) error { return errors.New("synthetic contact transaction failure") }
	if err := s.saveLocked(); !errors.Is(err, ErrEventHistory) || len(s.data.Contacts) != 300 || s.data.Consumers[contactCheckpointKey] != nil {
		t.Fatal("uncommitted contact data was omitted", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("failed history write changed native checkpoint", err)
	}
	h.write = nil
	if err := s.saveLocked(); err != nil {
		t.Fatal("contact transaction retry failed", err)
	}
}

func TestContactCheckpointQRAnchorKeepsRosterBeforeFirstSync(t *testing.T) {
	a, _ := externalContactFixture(t)
	s := a.Service
	candidate := *s.data
	candidate.Session = cloneSession(s.data.Session)
	client := &protocol.Client{Bootstrap: candidate.Session.Bootstrap, State: cloneSession(candidate.Session)}
	name := "after first sync"
	result := protocol.SyncResult{Contacts: []protocol.Contact{{Username: []byte("contact-00000"), Nickname: &name}}}
	if err := s.adoptHistoryLoginLocked(client, result, &candidate, s.checkpoint); err != nil {
		t.Fatal(err)
	}
	if len(s.data.Contacts) != 300 || *s.data.Contacts[0].Nickname != name || *s.data.Contacts[0].Remark != "remark-00000" {
		t.Fatal("QR anchor replaced a full external roster with a partial Sync")
	}
}

func TestContactCheckpointQRRetryReconcilesHistoryTransactionAndGuardFailures(t *testing.T) {
	for _, failure := range []string{"transaction", "guard"} {
		t.Run(failure, func(t *testing.T) {
			a, h := externalContactFixture(t)
			s := a.Service
			candidate := *s.data
			candidate.Session = cloneSession(s.data.Session)
			client := &protocol.Client{Bootstrap: candidate.Session.Bootstrap, State: cloneSession(candidate.Session)}
			name := "replayed first sync"
			result := protocol.SyncResult{Contacts: []protocol.Contact{{Username: []byte("contact-00000"), Nickname: &name}}}
			if failure == "transaction" {
				h.write = func(func(*securedb.Tx) error) error { return errors.New("synthetic transaction failure") }
			} else {
				h.guardWrite = func([]byte) error { return errors.New("synthetic outside guard failure") }
			}
			if err := s.adoptHistoryLoginLocked(client, result, &candidate, s.checkpoint); !errors.Is(err, ErrEventHistory) {
				t.Fatal("expected failed first-Sync commit", err)
			}
			h.write, h.guardWrite = nil, nil
			// The staged first Sync remains durable input even if the next
			// authorization does not repeat its contact delta.
			if err := s.adoptHistoryLoginLocked(client, protocol.SyncResult{}, &candidate, s.checkpoint); err != nil {
				t.Fatal("external contact dependency prevented QR retry", err)
			}
			if len(s.data.Contacts) != 300 || *s.data.Contacts[0].Nickname != name || *s.data.Contacts[0].Remark != "remark-00000" {
				t.Fatal("QR retry lost contact merge or complete roster")
			}
		})
	}
}

func TestContactCheckpointFirstTransitionAndLegacyInlineRecoverNewerDirectory(t *testing.T) {
	for _, layout := range []string{"first_transition", "legacy_large_inline"} {
		t.Run(layout, func(t *testing.T) {
			a, h := historyFixture(t)
			s := a.Service
			if layout == "legacy_large_inline" {
				s.ingestLocked(protocol.SyncResult{Contacts: checkpointContacts(s, 0, 300, 4096)})
				if err := s.saveLocked(); err != nil {
					t.Fatal(err)
				}
				// Recreate a valid checkpoint produced by the old unmodified
				// serializer, retaining the same already-initialized directory.
				legacy := *s.data
				legacy.Consumers = nil
				if err := s.checkpoint.Save(&legacy); err != nil {
					t.Fatal(err)
				}
			}
			path := filepath.Join(s.checkpoint.Directory, s.data.Binding+".checkpoint")
			if err := os.Rename(path, path+".fixture-backup"); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(path, 0700); err != nil {
				t.Fatal(err)
			}
			name := "latest committed contact"
			changes := checkpointContacts(s, 0, 300, 4096)
			if layout == "legacy_large_inline" {
				changes = []protocol.Contact{{Username: []byte("contact-00000"), Nickname: &name}, {Username: []byte("contact-00001"), Deleted: true}}
			}
			s.ingestLocked(protocol.SyncResult{Contacts: changes})
			if err := s.saveLocked(); err == nil {
				t.Fatal("fixture failed to interrupt first external checkpoint")
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(path+".fixture-backup", path); err != nil {
				t.Fatal(err)
			}
			if err := h.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenEventHistory(s.directory)
			if err != nil {
				t.Fatal(err)
			}
			next := reopenContactService(t, s, reopened)
			want := 300
			if layout == "legacy_large_inline" {
				want--
				if *next.data.Contacts[0].Nickname != name {
					t.Fatal("legacy inline checkpoint replaced a newer name")
				}
			}
			if len(next.data.Contacts) != want {
				t.Fatal("inline fallback lost a committed roster", len(next.data.Contacts), want)
			}
			if err := next.saveLocked(); err != nil {
				t.Fatal(err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
			last, err := OpenEventHistory(s.directory)
			if err != nil {
				t.Fatal("migration bound an old roster digest to a newer directory", err)
			}
			defer last.Close()
			if len(reopenContactService(t, next, last).data.Contacts) != want {
				t.Fatal("second restart lost migrated contacts")
			}
		})
	}
}

func TestContactCheckpointQRRecoveryClearsReaddedContactFence(t *testing.T) {
	a, _ := externalContactFixture(t)
	s := a.Service
	s.ingestLocked(protocol.SyncResult{Contacts: []protocol.Contact{{Username: []byte("contact-00000"), Deleted: true}}})
	if err := s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(s.checkpoint.Directory, s.data.Binding+".checkpoint")
	if err := os.Rename(path, path+".fixture-backup"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	s.ingestLocked(protocol.SyncResult{Contacts: []protocol.Contact{{Username: []byte("contact-00000")}}})
	if err := s.saveLocked(); err == nil {
		t.Fatal("fixture did not retain an older deleted-contact checkpoint")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".fixture-backup", path); err != nil {
		t.Fatal(err)
	}
	candidate := *s.data
	candidate.Session = cloneSession(s.data.Session)
	client := &protocol.Client{Bootstrap: candidate.Session.Bootstrap, State: cloneSession(candidate.Session)}
	if err := s.adoptHistoryLoginLocked(client, protocol.SyncResult{}, &candidate, s.checkpoint); err != nil {
		t.Fatal(err)
	}
	if len(s.data.Contacts) != 300 {
		t.Fatal("re-added roster missing after QR recovery")
	}
	for _, raw := range s.data.Unavailable {
		if string(raw) == "contact-00000" {
			t.Fatal("older native checkpoint retained a stale deletion fence")
		}
	}
}
