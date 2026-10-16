package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/JFSAS/wechat-runtime/internal/cdn"
	"github.com/JFSAS/wechat-runtime/internal/onebot"
	"github.com/JFSAS/wechat-runtime/internal/protocol"
	"github.com/JFSAS/wechat-runtime/internal/securedb"
	"github.com/JFSAS/wechat-runtime/internal/store"
	"github.com/JFSAS/wechat-runtime/pkg/bot"
)

func historyFixture(t *testing.T) (BotAdapter, *EventHistory) {
	t.Helper()
	s := recoveryFixture(t)
	if err := s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	h, err := OpenEventHistory(s.directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	a := BotAdapter{Service: s}
	if err = a.EnableHistory(h); err != nil {
		t.Fatal(err)
	}
	return a, h
}
func historyMessages(s *Service, count int) []protocol.Message {
	messages := make([]protocol.Message, count)
	for i := range messages {
		messages[i] = protocol.Message{Sender: []byte("synthetic-friend"), Recipient: s.data.Session.Bootstrap.Username, Content: []byte(fmt.Sprintf("synthetic event %d", i)), Type: 1, ServerID: uint64(i + 1), Timestamp: uint32(1700000000 + i)}
	}
	return messages
}
func TestHistoryCapturesWholeSyncBeyondNativeWindow(t *testing.T) {
	a, h := historyFixture(t)
	s := a.Service
	const count = 10050
	messages := historyMessages(s, count)
	started := time.Now()
	s.ingestLocked(protocol.SyncResult{Messages: messages})
	if len(s.data.Events) != 10000 || len(s.data.Messages) != 10000 {
		t.Fatal("native cache bounds changed")
	}
	if err := s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	if len(s.data.Messages) > historyWorkingEntries || len(s.data.Events) > historyWorkingEntries || h.workingBytes > historyWorkingBytes {
		t.Fatal("retained native cache did not compact")
	}
	snap := a.Snapshot()
	if snap.Cursor != count || snap.FirstCursor != 0 || snap.Error != "" {
		t.Fatal(snap)
	}
	cursor := uint64(0)
	for cursor < count {
		events, err := a.Events(snap.Account, cursor, 100)
		if err != nil || len(events) == 0 {
			t.Fatal(cursor, len(events), err)
		}
		for _, e := range events {
			cursor++
			if e.Sequence != cursor || e.ID != messageKey(messages[cursor-1], s.data.Session.Bootstrap.Username) || e.Message[0].Data["text"] != fmt.Sprintf("synthetic event %d", cursor-1) {
				t.Fatal("history order/content changed", cursor)
			}
		}
	}
	first, err := a.Message(snap.Account, messageKey(messages[0], s.data.Session.Bootstrap.Username))
	if err != nil || first.Sequence != 1 {
		t.Fatal(first, err)
	}
	if _, err = a.Events("other-account", 0, 1); !errors.Is(err, bot.ErrScope) {
		t.Fatal("history crossed account scope", err)
	}
	stats, err := h.Stats(snap.Account)
	if err != nil || stats.Messages != count {
		t.Fatal(stats, err)
	}
	t.Logf("EVENT_HISTORY_LOAD_REPORT={\"events\":%d,\"native_cache_events\":%d,\"archive_file_bytes\":%d,\"elapsed_millis\":%d,\"system_scope\":\"synthetic single sync and paged replay; no protocol worker or live platform\",\"live_wechat_requests\":0}", count, len(s.data.Events), stats.FileBytes, time.Since(started).Milliseconds())
}
func TestHistoryOldCheckpointReplayKeepsStableSequenceAndMessageIDs(t *testing.T) {
	a, h := historyFixture(t)
	s := a.Service
	cpPath := filepath.Join(s.checkpoint.Directory, s.data.Binding+".checkpoint")
	old, err := os.ReadFile(cpPath)
	if err != nil {
		t.Fatal(err)
	}
	messages := historyMessages(s, 3)
	s.ingestLocked(protocol.SyncResult{Messages: messages})
	if err = s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	first, err := a.Events(s.data.Binding, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if err = h.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(cpPath, old, 0600); err != nil {
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
	restarted.client = &protocol.Client{Bootstrap: restarted.data.Session.Bootstrap, State: cloneSession(restarted.data.Session)}
	next := BotAdapter{Service: restarted}
	if err = next.EnableHistory(reopened); err != nil {
		t.Fatal(err)
	}
	messages = historyMessages(restarted, 4)
	restarted.ingestLocked(protocol.SyncResult{Messages: messages})
	if err = restarted.saveLocked(); err != nil {
		t.Fatal(err)
	}
	got, err := next.Events(restarted.data.Binding, 0, 100)
	if err != nil || len(got) != 4 || !reflect.DeepEqual(got[:3], first) || got[3].Sequence != 4 {
		t.Fatal("replayed checkpoint duplicated or renumbered events", got, err)
	}
}
func TestHistoryCommitFailuresCannotAdvanceCheckpointOrExposeUnanchoredEvents(t *testing.T) {
	for _, kind := range []string{"transaction", "ambiguous_transaction", "required_manifest"} {
		t.Run(kind, func(t *testing.T) {
			a, h := historyFixture(t)
			s := a.Service
			before, _ := s.checkpoint.Load()
			switch kind {
			case "transaction":
				h.write = func(func(*securedb.Tx) error) error { return errors.New("synthetic commit failure") }
			case "ambiguous_transaction":
				h.write = func(fn func(*securedb.Tx) error) error {
					if err := h.db.Update(fn); err != nil {
						return err
					}
					return errors.New("synthetic commit uncertainty")
				}
			case "required_manifest":
				h.guardWrite = func([]byte) error { return errors.New("synthetic manifest failure") }
			}
			s.ingestLocked(protocol.SyncResult{Messages: historyMessages(s, 1)})
			if err := s.saveLocked(); !errors.Is(err, bot.ErrStorage) {
				t.Fatal(err)
			}
			saved, err := s.checkpoint.Load()
			if err != nil || saved.EventSequence != before.EventSequence {
				t.Fatal("history failure advanced native checkpoint", err)
			}
			if _, err = a.Events(s.data.Binding, 0, 100); !errors.Is(err, bot.ErrStorage) {
				t.Fatal("unanchored events exposed", err)
			}
			if snap := a.Snapshot(); snap.Error == "" || snap.Online {
				t.Fatal(snap)
			}
			h.write, h.guardWrite = nil, nil
			if err = s.saveLocked(); err != nil {
				t.Fatal(err)
			}
			events, err := a.Events(s.data.Binding, 0, 100)
			if err != nil || len(events) != 1 || events[0].Sequence != 1 {
				t.Fatal(events, err)
			}
		})
	}
}
func TestHistoryDurableFactsSurviveLaterCheckpointFailure(t *testing.T) {
	a, h := historyFixture(t)
	s := a.Service
	directory := s.checkpoint.Directory
	bad := filepath.Join(s.directory, "blocked-checkpoint")
	if err := os.WriteFile(bad, []byte("synthetic"), 0600); err != nil {
		t.Fatal(err)
	}
	s.checkpoint.Directory = bad
	s.ingestLocked(protocol.SyncResult{Messages: historyMessages(s, 1)})
	if err := s.saveLocked(); err == nil {
		t.Fatal("checkpoint fault did not trigger")
	}
	if s.persistedSequence != 0 {
		t.Fatal("native checkpoint progress advanced")
	}
	events, err := a.Events(s.data.Binding, 0, 100)
	if err != nil || len(events) != 1 || a.Snapshot().Cursor != 1 {
		t.Fatal("durable authenticated facts disappeared", events, err)
	}
	s.checkpoint.Directory = directory
	if err = s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	stats, err := h.Stats(s.data.Binding)
	if err != nil || stats.Cursor != 1 {
		t.Fatal(stats, err)
	}
}
func TestHistoryRetainedLegacyWindowPreservesNumbersAndExplicitGap(t *testing.T) {
	s := recoveryFixture(t)
	s.ingestLocked(protocol.SyncResult{Messages: historyMessages(s, 4)})
	s.data.Messages = s.data.Messages[2:]
	if err := s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	h, err := OpenEventHistory(s.directory)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	a := BotAdapter{Service: s}
	if err = a.EnableHistory(h); err != nil {
		t.Fatal(err)
	}
	if snap := a.Snapshot(); snap.FirstCursor != 2 || snap.Cursor != 4 {
		t.Fatal(snap)
	}
	if _, err = a.Events(s.data.Binding, 0, 100); !errors.Is(err, bot.ErrCursor) {
		t.Fatal("missing legacy history silently skipped", err)
	}
	events, err := a.Events(s.data.Binding, 2, 100)
	if err != nil || len(events) != 2 || events[0].Sequence != 3 || events[1].Sequence != 4 {
		t.Fatal(events, err)
	}
}
func TestHistoryDefaultNativeBehaviorMatchesWithoutIntegration(t *testing.T) {
	plain := recoveryFixture(t)
	a, _ := historyFixture(t)
	extended := a.Service
	// Both fixtures use the same synthetic namespace and native session keys.
	messages := historyMessages(plain, 5)
	result := protocol.SyncResult{Messages: messages}
	plain.ingestLocked(result)
	extended.ingestLocked(result)
	if err := plain.saveLocked(); err != nil {
		t.Fatal(err)
	}
	if err := extended.saveLocked(); err != nil {
		t.Fatal(err)
	}
	left, _ := json.Marshal(plain.data)
	right, _ := json.Marshal(extended.data)
	if !bytes.Equal(left, right) {
		t.Fatal("optional history changed native ingestion/checkpoint facts")
	}
}
func TestHistoryMissingOrOlderDatabaseAndNamespaceChangesAreExplicit(t *testing.T) {
	for _, kind := range []string{"missing", "older", "namespace"} {
		t.Run(kind, func(t *testing.T) {
			a, h := historyFixture(t)
			s := a.Service
			path := filepath.Join(s.directory, "event-history", "events.db")
			if err := h.Close(); err != nil {
				t.Fatal(err)
			}
			old, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			h, err = OpenEventHistory(s.directory)
			if err != nil {
				t.Fatal(err)
			}
			s.history = h
			s.ingestLocked(protocol.SyncResult{Messages: historyMessages(s, 1)})
			if err = s.saveLocked(); err != nil {
				t.Fatal(err)
			}
			if err = h.Close(); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "missing":
				err = os.RemoveAll(filepath.Dir(path))
			case "older":
				err = os.WriteFile(path, old, 0600)
			case "namespace":
				s.data.APIKey = bytes.Repeat([]byte{9}, 32)
				err = s.checkpoint.Save(s.data)
			}
			if err != nil {
				t.Fatal(err)
			}
			if opened, err := OpenEventHistory(s.directory); err == nil {
				opened.Close()
				t.Fatal("history silently reset", kind)
			}
		})
	}
}
func TestHistoryCloseRejectsReadsAndReleasesOwnership(t *testing.T) {
	a, h := historyFixture(t)
	if err := a.CloseHistory(); err != nil {
		t.Fatal(err)
	}
	if _, err := h.events(a.Service.data.Binding, 0, 1); !errors.Is(err, bot.ErrStorage) {
		t.Fatal(err)
	}
	next, err := OpenEventHistory(a.Service.directory)
	if err != nil {
		t.Fatal(err)
	}
	_ = next.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = a.Download(ctx, a.Service.data.Binding, "x", "x"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestHistoryMediaDescriptorSurvivesNativeEvictionWithoutExposingKeys(t *testing.T) {
	body := mediaTestPNG(t)
	s, conversation, message := mediaFixture(t, body, "image", "")
	if err := s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	h, err := OpenEventHistory(s.directory)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	a := BotAdapter{Service: s}
	if err = a.EnableHistory(h); err != nil {
		t.Fatal(err)
	}
	s.data.Messages = nil
	event, err := a.Message(s.data.Binding, message)
	if err != nil || len(event.Message) != 1 {
		t.Fatal(event, err)
	}
	public, _ := json.Marshal(event)
	if bytes.Contains(public, []byte("00112233445566778899aabbccddeeff")) || bytes.Contains(public, []byte("synthetic-private-CDN-id")) {
		t.Fatal("private media reference leaked")
	}
	s.SetMediaDownloader(func(context.Context, MediaDownloadRequest) (*cdn.Result, error) { return verifiedMedia(body), nil })
	id := mediaQueueID(t, s, conversation, message, "regular")
	if err = s.downloadQueued(context.Background()); err != nil {
		t.Fatal(err)
	}
	content, err := a.Download(context.Background(), s.data.Binding, conversation, message+"/regular")
	if err != nil || !bytes.Equal(content.Data, body) || s.data.MediaJobs[id].Status != "downloaded" {
		t.Fatal("history media descriptor unusable", err)
	}
	if _, err = s.QueueMediaDownload(s.sessionID([]byte("other")), message, "regular"); err == nil {
		t.Fatal("history media crossed conversation")
	}
}

func TestHistoryLargeNativeMessageUsesAuthenticatedBoundedChunks(t *testing.T) {
	a, h := historyFixture(t)
	s := a.Service
	message := historyMessages(s, 1)[0]
	// The frozen protocol accepts this body. JSON escaping plus the raw copy
	// exceeds securedb's 1MiB per-record limit; history must not stall its sync.
	message.Content = bytes.Repeat([]byte("<"), 600<<10)
	s.ingestLocked(protocol.SyncResult{Messages: []protocol.Message{message}})
	if err := s.saveLocked(); err != nil {
		t.Fatal("valid native text stalled history", err)
	}
	key := messageKey(message, s.data.Session.Bootstrap.Username)
	got, err := a.Message(s.data.Binding, key)
	if err != nil || got.Message[0].Data["text"] != string(message.Content) {
		t.Fatal("large body changed", err)
	}
	err = h.db.View(func(tx *securedb.Tx) error {
		raw, err := tx.Get("messages", h.db.Key("message", s.data.Binding, key))
		if err != nil {
			return err
		}
		var ref historyBody
		if json.Unmarshal(raw, &ref) != nil || ref.Chunks < 2 || ref.Bytes < 1<<20 {
			return errors.New("large body was not chunked")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = h.db.Update(func(tx *securedb.Tx) error {
		return tx.Put("bodies", h.db.Key("body", s.data.Binding, key, "1"), []byte("synthetic invalid chunk"))
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.Message(s.data.Binding, key); !errors.Is(err, bot.ErrStorage) {
		t.Fatal("corrupt chunk appeared as missing message", err)
	}
}

func TestHistoryDuplicateLegacyIDsKeepReadableUniqueSuffix(t *testing.T) {
	s := recoveryFixture(t)
	messages := historyMessages(s, 2)
	s.ingestLocked(protocol.SyncResult{Messages: messages})
	last := s.data.Events[0]
	last.Sequence = 3
	s.data.Events = append(s.data.Events, last)
	s.data.EventSequence = 3
	if err := s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	h, err := OpenEventHistory(s.directory)
	if err != nil {
		t.Fatal("legitimate duplicate legacy IDs prevented upgrade", err)
	}
	defer h.Close()
	a := BotAdapter{Service: s}
	if err = a.EnableHistory(h); err != nil {
		t.Fatal(err)
	}
	snap := a.Snapshot()
	if snap.FirstCursor != 1 || snap.Cursor != 3 {
		t.Fatal(snap)
	}
	if _, err = a.Events(s.data.Binding, 0, 100); !errors.Is(err, bot.ErrCursor) {
		t.Fatal("ambiguous old duplicate was silently skipped", err)
	}
	events, err := a.Events(s.data.Binding, 1, 100)
	if err != nil || len(events) != 2 || events[0].Sequence != 2 || events[1].Sequence != 3 {
		t.Fatal(events, err)
	}
}
func TestHistoryStagingLimitIsAnObservableStorageFailure(t *testing.T) {
	a, h := historyFixture(t)
	s := a.Service
	h.pendingBytes = 32 << 20
	s.ingestLocked(protocol.SyncResult{Messages: historyMessages(s, 1)})
	if err := s.saveLocked(); !errors.Is(err, bot.ErrStorage) {
		t.Fatal(err)
	}
	if _, err := h.Stats(s.data.Binding); !errors.Is(err, bot.ErrStorage) {
		t.Fatal("staging failure reported healthy storage", err)
	}
	if _, err := a.Events(s.data.Binding, 0, 100); !errors.Is(err, bot.ErrStorage) {
		t.Fatal(err)
	}
	saved, err := s.checkpoint.Load()
	if err != nil || saved.EventSequence != 0 {
		t.Fatal("staging bound advanced native cursor", err)
	}
}
func TestHistoryQRFirstSyncCannotAdvancePastFailedHistory(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprintf("existing=%v", existing), func(t *testing.T) {
			s, login := loginFixture(t)
			beforeCursor := protocol.Join(protocol.Uint(1, 1), protocol.Blob(2, protocol.Join(protocol.Uint(1, 1), protocol.Uint(2, 7))))
			afterCursor := protocol.Join(protocol.Uint(1, 1), protocol.Blob(2, protocol.Join(protocol.Uint(1, 1), protocol.Uint(2, 8))))
			login.candidate.Cursor = beforeCursor
			if existing {
				s.data = &store.Data{Schema: 1, Binding: login.candidate.Bootstrap.Binding(), SourceApp: s.source.App, APIKey: bytes.Repeat([]byte{7}, 32), Session: cloneSession(login.candidate)}
				s.checkpoint = store.Checkpoint{Directory: filepath.Join(s.directory, "checkpoints"), Binding: s.data.Binding}
				if err := s.saveLocked(); err != nil {
					t.Fatal(err)
				}
			}
			h, err := OpenEventHistory(s.directory)
			if err != nil {
				t.Fatal(err)
			}
			defer h.Close()
			a := BotAdapter{Service: s}
			if err = a.EnableHistory(h); err != nil {
				t.Fatal(err)
			}
			h.write = func(func(*securedb.Tx) error) error { return errors.New("synthetic first-sync history failure") }
			s.loginValidate = func(_ context.Context, c *protocol.Client) (protocol.SyncResult, error) {
				c.State.Confirmed = true
				c.State.Cursor = afterCursor
				return protocol.SyncResult{Messages: []protocol.Message{{Sender: []byte("synthetic-friend"), Recipient: c.Bootstrap.Username, Content: []byte("first sync"), Type: 1, Timestamp: 1700000000, ServerID: 1}}}, nil
			}
			if _, err = s.QueueLogin(); err != nil {
				t.Fatal(err)
			}
			s.workLogin(context.Background())
			s.workLogin(context.Background())
			cp, exists, err := store.Unique(filepath.Join(s.directory, "checkpoints"))
			if err != nil || !exists {
				t.Fatal("no recoverable QR session anchor", err)
			}
			saved, err := cp.Load()
			if err != nil || !bytes.Equal(saved.Session.Cursor, beforeCursor) || saved.EventSequence != 0 || len(saved.APIKey) != 32 {
				t.Fatal("QR checkpoint advanced past uncommitted messages", err)
			}
			h.write = nil
			if err = s.saveLocked(); err != nil {
				t.Fatal(err)
			}
			saved, err = cp.Load()
			if err != nil || !bytes.Equal(saved.Session.Cursor, afterCursor) || saved.EventSequence != 1 {
				t.Fatal("QR recovery did not finish", err)
			}
			events, err := a.Events(saved.Binding, 0, 100)
			if err != nil || len(events) != 1 || events[0].Message[0].Data["text"] != "first sync" {
				t.Fatal(events, err)
			}
		})
	}
}

func TestHistoryQRRetryAnchorCannotHideUncommittedFirstSyncAfterRestart(t *testing.T) {
	s, login := loginFixture(t)
	beforeCursor := protocol.Join(protocol.Uint(1, 1), protocol.Blob(2, protocol.Join(protocol.Uint(1, 1), protocol.Uint(2, 7))))
	afterCursor := protocol.Join(protocol.Uint(1, 1), protocol.Blob(2, protocol.Join(protocol.Uint(1, 1), protocol.Uint(2, 8))))
	h, err := OpenEventHistory(s.directory)
	if err != nil {
		t.Fatal(err)
	}
	a := BotAdapter{Service: s}
	if err = a.EnableHistory(h); err != nil {
		t.Fatal(err)
	}
	h.write = func(func(*securedb.Tx) error) error {
		return errors.New("synthetic history unavailable across QR retries")
	}
	var first protocol.Message
	s.loginValidate = func(_ context.Context, c *protocol.Client) (protocol.SyncResult, error) {
		c.State.Confirmed = true
		c.State.Cursor = afterCursor
		first = protocol.Message{Sender: []byte("synthetic-friend"), Recipient: c.Bootstrap.Username, Content: []byte("first sync"), Type: 1, Timestamp: 1700000000, ServerID: 1}
		return protocol.SyncResult{Messages: []protocol.Message{first}}, nil
	}
	for range 2 {
		login.candidate = loginCandidate()
		login.candidate.Cursor = beforeCursor
		if _, err = s.QueueLogin(); err != nil {
			t.Fatal(err)
		}
		s.workLogin(context.Background())
		s.workLogin(context.Background())
	}
	cp, exists, err := store.Unique(filepath.Join(s.directory, "checkpoints"))
	if err != nil || !exists {
		t.Fatal(err)
	}
	anchor, err := cp.Load()
	if err != nil || len(anchor.Messages) != 0 || len(anchor.Events) != 0 || anchor.EventSequence != 0 || !bytes.Equal(anchor.Session.Cursor, beforeCursor) {
		t.Fatal("retry anchor swallowed uncommitted event", err)
	}
	if err = h.Close(); err != nil {
		t.Fatal(err)
	}
	// Discard staged memory, keeping exactly the durable state of a crash after
	// anchor save and before the final history/checkpoint transaction completes.
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
	restarted.client.State.Cursor = afterCursor
	restarted.snapshotLocked()
	restarted.ingestLocked(protocol.SyncResult{Messages: []protocol.Message{first}})
	if err = restarted.saveLocked(); err != nil {
		t.Fatal(err)
	}
	events, err := next.Events(restarted.data.Binding, 0, 100)
	if err != nil || len(events) != 1 || events[0].Message[0].Data["text"] != "first sync" {
		t.Fatal("QR retry lost its first sync", events, err)
	}
}

func TestHistoryHTTPConsumerResumesBeyondNativeRetainedWindow(t *testing.T) {
	a, _ := historyFixture(t)
	s := a.Service
	state, err := onebot.OpenState(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	runtime := onebot.New(a, state)
	enabled := make(chan struct{}, 2)
	delivered := make(chan uint64, 201)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var event struct {
			PostType string `json:"post_type"`
			Meta     string `json:"meta_event_type"`
			Sequence uint64 `json:"wechat_sequence"`
		}
		if json.NewDecoder(r.Body).Decode(&event) != nil {
			w.WriteHeader(400)
			return
		}
		if event.Meta == "lifecycle" {
			enabled <- struct{}{}
		}
		if event.PostType == "message" {
			delivered <- event.Sequence
		}
	}))
	defer peer.Close()
	config := onebot.PostConfig{Name: "durable-history-consumer", URL: peer.URL}
	first := onebot.NewServer(runtime, "synthetic-transport-token", "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- first.RunPost(ctx, config) }()
	select {
	case <-enabled:
	case <-time.After(3 * time.Second):
		t.Fatal("consumer did not initialize its durable cursor")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("consumer did not stop")
	}
	if err = first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.ingestLocked(protocol.SyncResult{Messages: historyMessages(s, 201)})
	if err = s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	s.data.Messages = s.data.Messages[len(s.data.Messages)-10:]
	s.data.Events = s.data.Events[len(s.data.Events)-10:]
	if err = s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	second := onebot.NewServer(runtime, "synthetic-transport-token", "")
	defer second.Close(context.Background())
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go func() { done <- second.RunPost(ctx, config) }()
	deadline := time.After(8 * time.Second)
	for sequence := uint64(1); sequence <= 201; sequence++ {
		select {
		case got := <-delivered:
			if got != sequence {
				t.Fatal("consumer missed/reordered history", got, sequence)
			}
		case <-deadline:
			t.Fatal("consumer could not resume beyond native cache", sequence)
		}
	}
	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("resumed consumer did not stop")
	}
}

func TestHistoryAndDeliveryArchivePreserveSharedCheckpointOutcomes(t *testing.T) {
	a, request := archiveFixture(t)
	s := a.Service
	h, err := OpenEventHistory(s.directory)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if err = a.EnableHistory(h); err != nil {
		t.Fatal(err)
	}
	delivery := acknowledgeArchiveFixture(t, a, request)
	if _, err = a.ArchiveCompleted(context.Background()); err != nil {
		t.Fatal(err)
	}
	again, err := a.Submit(context.Background(), request)
	if err != nil || again.ID != delivery.ID || again.State != "accepted" {
		t.Fatal("history changed archived delivery outcome", err)
	}
	row, err := a.Archive.indexed(request.Account, "jobs", delivery.ID)
	if err != nil || row == nil {
		t.Fatal(err)
	}
	job := row.Jobs[0]
	echo := protocol.Message{Sender: s.data.Session.Bootstrap.Username, Recipient: []byte(job.Recipient), Content: []byte(job.Content), Type: 1, Timestamp: job.CreateTime, ServerID: job.ServerID}
	s.ingestLocked(protocol.SyncResult{Messages: []protocol.Message{echo}})
	if err = s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	s.data.Messages = nil
	s.data.Events = nil
	events, err := a.Events(request.Account, 0, 100)
	if err != nil || len(events) != 1 || !events[0].LocalEcho {
		t.Fatal("archived own echo escaped history filtering", events, err)
	}
	again, err = a.Submit(context.Background(), request)
	if err != nil || again.ID != delivery.ID {
		t.Fatal("shared checkpoint lost archived key", err)
	}
}
