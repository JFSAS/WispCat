package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JFSAS/wechat-runtime/internal/cdn"
	"github.com/JFSAS/wechat-runtime/internal/messaging"
	"github.com/JFSAS/wechat-runtime/internal/onebot"
	"github.com/JFSAS/wechat-runtime/internal/protocol"
	"github.com/JFSAS/wechat-runtime/internal/securedb"
	"github.com/JFSAS/wechat-runtime/internal/store"
	"github.com/JFSAS/wechat-runtime/pkg/bot"
	bolt "go.etcd.io/bbolt"
)

func incomingFixture(t *testing.T) (BotAdapter, *EventHistory, string, string, []byte, *int) {
	t.Helper()
	body := mediaTestPNG(t)
	s, session, message := mediaFixture(t, body, "image", "")
	if err := s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	archive, err := OpenDeliveryArchive(s.directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = archive.Close() })
	h, err := OpenEventHistory(s.directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	a := BotAdapter{Service: s, Archive: archive}
	if err = a.EnableHistory(h); err != nil {
		t.Fatal(err)
	}
	calls := new(int)
	s.SetMediaDownloader(func(context.Context, MediaDownloadRequest) (*cdn.Result, error) {
		*calls++
		return verifiedMedia(body), nil
	})
	a.wrapArchiveMediaDownloader()
	return a, h, session, message, body, calls
}
func incomingComplete(t *testing.T, a BotAdapter, session, message string) string {
	t.Helper()
	id, err := a.Service.Download(a.Service.data.Binding, session, message, "regular")
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Service.downloadQueued(context.Background()); err != nil {
		t.Fatal(err)
	}
	return id
}
func incomingRecord(t *testing.T, h *EventHistory, account, id string) incomingMediaRecord {
	t.Helper()
	var row *incomingMediaRecord
	err := h.db.View(func(tx *securedb.Tx) error { var e error; row, e = h.mediaRecord(tx, account, id); return e })
	if err != nil || row == nil {
		t.Fatal("incoming facts unavailable", err)
	}
	return *row
}
func incomingRestart(t *testing.T, a BotAdapter, h *EventHistory) (BotAdapter, *EventHistory) {
	t.Helper()
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := OpenEventHistory(a.Service.directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = next.Close() })
	s := New(a.Service.directory, time.Second, "/synthetic/WeChat.app")
	b := BotAdapter{Service: s, Archive: a.Archive}
	if err = b.EnableHistory(next); err != nil {
		t.Fatal(err)
	}
	if err = s.prepare(false, false); err != nil {
		t.Fatal(err)
	}
	return b, next
}

func TestIncomingMediaHistoryCrossesNativeTaskLimitAndReclaimsCache(t *testing.T) {
	a, h, session, first, body, calls := incomingFixture(t)
	s := a.Service
	account := s.data.Binding
	const count = 550
	started := time.Now()
	firstTask := ""
	last := first
	template := s.data.Messages[0]
	for i := 0; i < count; i++ {
		message := template
		message.ServerID += uint64(i)
		message.Timestamp += uint32(i)
		last = messageKey(message, s.data.Session.Bootstrap.Username)
		if i > 0 {
			s.ingestLocked(protocol.SyncResult{Messages: []protocol.Message{message}})
		}
		id := incomingComplete(t, a, session, last)
		if i == 0 {
			firstTask = id
		}
		if len(s.data.MediaJobs) != 0 {
			t.Fatal("terminal native tasks accumulated", i, len(s.data.MediaJobs))
		}
	}
	stats, err := h.Stats(account)
	if err != nil || stats.IncomingMediaTasks != count || stats.IncomingResidentFiles > incomingMediaFiles || *calls != count {
		t.Fatal(stats, *calls, err)
	}
	old := incomingRecord(t, h, account, firstTask)
	if old.Phase != "evicted" || old.Job.Status != "downloaded" {
		t.Fatal("oldest fact lost or cache not reclaimed", old.Phase)
	}
	if _, err = os.Lstat(filepath.Join(s.directory, "media", firstTask+".enc")); !os.IsNotExist(err) {
		t.Fatal("evicted file still present", err)
	}
	content, err := a.Download(context.Background(), account, session, last+"/regular")
	if err != nil || !bytes.Equal(content.Data, body) || *calls != count {
		t.Fatal("cache read fetched again", err, *calls)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		content, err := a.Download(ctx, account, session, first+"/regular")
		if err == nil && !bytes.Equal(content.Data, body) {
			err = errors.New("redownload body changed")
		}
		result <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.mu.RLock()
		queued := len(s.data.MediaJobs) > 0
		s.mu.RUnlock()
		if queued {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("explicit retry did not queue")
		}
		time.Sleep(time.Millisecond)
	}
	if err = s.downloadQueued(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-result; err != nil {
		t.Fatal(err)
	}
	stats, err = h.Stats(account)
	if err != nil || stats.IncomingMediaTasks != count+1 || *calls != count+1 {
		t.Fatal(stats, *calls, err)
	}
	entries, err := os.ReadDir(filepath.Join(s.directory, "media"))
	if err != nil {
		t.Fatal(err)
	}
	files := 0
	var fileBytes int64
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".enc") {
			info, e := entry.Info()
			if e != nil {
				t.Fatal(e)
			}
			files++
			fileBytes += info.Size()
		}
	}
	if files != int(stats.IncomingResidentFiles) {
		t.Fatal("cache accounting differs from actual files", files, stats.IncomingResidentFiles)
	}
	t.Logf("INCOMING_MEDIA_LOAD_REPORT={\"downloads\":%d,\"active_native_tasks\":%d,\"accounted_files\":%d,\"accounted_payload_bytes\":%d,\"cache_file_count\":%d,\"cache_ciphertext_file_bytes\":%d,\"released_payload_bytes\":%d,\"elapsed_millis\":%d,\"explicit_redownload_verified\":true,\"system_scope\":\"synthetic CDN result through native serial download worker and persistent cache lifecycle\",\"live_wechat_requests\":0}", *calls, len(s.data.MediaJobs), stats.IncomingResidentFiles, stats.IncomingResidentBytes, files, fileBytes, stats.IncomingReclaimedBytes, time.Since(started).Milliseconds())
}

func TestIncomingMediaCleanupProtectsWaitersArtifactsAndOutgoingArchive(t *testing.T) {
	a, h, session, message, body, _ := incomingFixture(t)
	s := a.Service
	id := incomingComplete(t, a, session, message)
	account := s.data.Binding
	s.mu.Lock()
	h.mediaWaiters = map[string]int{account + "/" + id: 1}
	s.mu.Unlock()
	result, err := a.ReclaimIncomingMedia(context.Background())
	if err != nil || result.ReclaimedFiles != 0 {
		t.Fatal("active reader lost cache", result, err)
	}
	s.mu.Lock()
	delete(h.mediaWaiters, account+"/"+id)
	s.data.Artifacts = map[string]messaging.Artifact{"pinned": {CacheID: id, Size: int64(len(body))}}
	s.mu.Unlock()
	result, err = a.ReclaimIncomingMedia(context.Background())
	if err != nil || result.ReclaimedFiles != 0 {
		t.Fatal("artifact-shared file removed", result, err)
	}
	s.mu.Lock()
	s.data.Artifacts = nil
	s.mu.Unlock()
	request := bot.Send{Account: account, Epoch: a.Snapshot().Epoch, Conversation: session, Key: "pinned-outgoing", Upload: &bot.Upload{Kind: "file", Name: "outgoing.bin", Data: []byte("unknown outgoing payload")}}
	delivery, err := a.Submit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.data.Jobs[delivery.ID].Status = "unknown"
	s.mu.Unlock()
	if _, err = a.ArchiveCompleted(context.Background()); err != nil {
		t.Fatal(err)
	}
	result, err = a.ReclaimIncomingMedia(context.Background())
	if err != nil || result.ReclaimedFiles != 1 {
		t.Fatal(result, err)
	}
	stored, err := a.Delivery(account, delivery.ID)
	if err != nil || stored.State != "unknown" {
		t.Fatal(stored, err)
	}
	content, err := a.Download(context.Background(), account, session, stored.Message[0].Data["file"])
	if err != nil || string(content.Data) != "unknown outgoing payload" {
		t.Fatal("cleanup erased unknown send", err)
	}
}

func TestIncomingMediaDeleteFailureRetainsFactsAndAccounting(t *testing.T) {
	for _, point := range []string{"unlink", "directory-sync"} {
		t.Run(point, func(t *testing.T) {
			a, h, session, message, body, _ := incomingFixture(t)
			id := incomingComplete(t, a, session, message)
			account := a.Service.data.Binding
			ciphertext, e := os.ReadFile(filepath.Join(a.Service.directory, "media", id+".enc"))
			if e != nil {
				t.Fatal(e)
			}
			if point == "unlink" {
				h.mediaRemove = func(string) error { return errors.New("synthetic unlink failure") }
			} else {
				h.mediaSync = func() error { return errors.New("synthetic directory sync failure") }
			}
			if _, err := a.ReclaimIncomingMedia(context.Background()); !errors.Is(err, bot.ErrStorage) {
				t.Fatal(err)
			}
			var meta incomingMediaMeta
			if err := h.db.View(func(tx *securedb.Tx) error { var e error; meta, _, e = h.mediaMeta(tx, account, ""); return e }); err != nil || meta.ResidentBytes != int64(len(body)) || meta.ResidentFiles != 1 {
				t.Fatal("deletion freed quota before sync", meta, err)
			}
			row := incomingRecord(t, h, account, id)
			if row.Phase != "pending_delete" || row.Job.Status != "downloaded" {
				t.Fatal(row.Phase)
			}
			atomicPath := filepath.Join(a.Service.directory, "media", ".write-12345")
			if err := os.WriteFile(atomicPath, ciphertext[:len(ciphertext)/2], 0600); err != nil {
				t.Fatal(err)
			}
			b, next := incomingRestart(t, a, h)
			stats, err := next.Stats(account)
			if err != nil || stats.IncomingResidentFiles != 0 || stats.IncomingMediaTasks != 1 || stats.IncomingReclaimedBytes != int64(len(body)) || len(b.Service.data.MediaJobs) != 0 {
				t.Fatal(stats, err)
			}
			if _, err = os.Lstat(filepath.Join(b.Service.directory, "media", id+".enc")); !os.IsNotExist(err) {
				t.Fatal(err)
			}
			if _, err = os.Lstat(atomicPath); !os.IsNotExist(err) {
				t.Fatal("native Atomic orphan survived recovery", err)
			}
			if _, err = os.Lstat(filepath.Join(b.Service.directory, "media", account+".key")); err != nil {
				t.Fatal("recovery removed native cache key", err)
			}
		})
	}
}

func TestIncomingMediaOldCheckpointCannotReplayCompletedDownload(t *testing.T) {
	a, h, session, message, body, _ := incomingFixture(t)
	s := a.Service
	account := s.data.Binding
	id, err := s.Download(account, session, message, "regular")
	if err != nil {
		t.Fatal(err)
	}
	cpPath := filepath.Join(s.checkpoint.Directory, account+".checkpoint")
	older, err := os.ReadFile(cpPath)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.downloadQueued(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(cpPath, older, 0600); err != nil {
		t.Fatal(err)
	}
	b, next := incomingRestart(t, a, h)
	if len(b.Service.data.MediaJobs) != 0 {
		t.Fatal("older queued task survived reconciliation")
	}
	content, err := b.Download(context.Background(), account, session, message+"/regular")
	if err != nil || !bytes.Equal(content.Data, body) {
		t.Fatal(err)
	}
	if row := incomingRecord(t, next, account, id); row.Job.Status != "downloaded" {
		t.Fatal(row.Job.Status)
	}
}

func TestIncomingMediaPublishedRevisionRejectsOldDatabase(t *testing.T) {
	a, h, session, message, _, _ := incomingFixture(t)
	account := a.Service.data.Binding
	incomingComplete(t, a, session, message)
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(a.Service.directory, "event-history", "events.db")
	older, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	next, err := OpenEventHistory(a.Service.directory)
	if err != nil {
		t.Fatal(err)
	}
	a.Service.history = next
	next.deliveryArchive = a.Archive
	if _, err = a.ReclaimIncomingMedia(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = next.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, older, 0600); err != nil {
		t.Fatal(err)
	}
	if restored, err := OpenEventHistory(a.Service.directory); err == nil {
		_ = restored.Close()
		t.Fatal("old media state bypassed outside revision", account)
	}
}

func TestIncomingMediaReservationSurvivesFailedGuardAndRestart(t *testing.T) {
	a, h, session, message, body, _ := incomingFixture(t)
	s := a.Service
	account := s.data.Binding
	if _, err := s.Download(account, session, message, "regular"); err != nil {
		t.Fatal(err)
	}
	h.guardWrite = func([]byte) error { return errors.New("synthetic media guard failure") }
	if err := s.downloadQueued(context.Background()); !errors.Is(err, bot.ErrStorage) {
		t.Fatal(err)
	}
	var meta incomingMediaMeta
	if err := h.db.View(func(tx *securedb.Tx) error { var e error; meta, _, e = h.mediaMeta(tx, account, ""); return e }); err != nil || meta.ResidentBytes != int64(len(body)) {
		t.Fatal("uncertain reservation lost", meta, err)
	}
	_, next := incomingRestart(t, a, h)
	stats, err := next.Stats(account)
	if err != nil || stats.IncomingResidentBytes != 0 || stats.IncomingMediaTasks != 1 {
		t.Fatal(stats, err)
	}
}

func TestIncomingMediaGuardMigrationAndMissingMetadataFailClosed(t *testing.T) {
	a, h, _, _, _, _ := incomingFixture(t)
	account := a.Service.data.Binding
	if err := h.db.Update(func(tx *securedb.Tx) error {
		return tx.Delete("incoming-media-meta", h.db.Key("incoming-media", account))
	}); err != nil {
		t.Fatal(err)
	}
	legacy, _ := json.Marshal(map[string]any{"schema": 1, "identity": h.identity, "meta": h.published, "directory_revision": h.publishedDirectory})
	if err := os.WriteFile(filepath.Join(a.Service.directory, "event-history.required"), legacy, 0600); err != nil {
		t.Fatal(err)
	}
	_, next := incomingRestart(t, a, h)
	raw, err := os.ReadFile(filepath.Join(a.Service.directory, "event-history.required"))
	if err != nil {
		t.Fatal(err)
	}
	var guard historyGuard
	if json.Unmarshal(raw, &guard) != nil || guard.Schema != 2 || guard.Meta.Account != account {
		t.Fatal("legacy guard did not upgrade")
	}
	if err = next.db.Update(func(tx *securedb.Tx) error {
		return tx.Delete("incoming-media-meta", next.db.Key("incoming-media", account))
	}); err != nil {
		t.Fatal(err)
	}
	if err = next.Close(); err != nil {
		t.Fatal(err)
	}
	if broken, err := OpenEventHistory(a.Service.directory); err == nil {
		_ = broken.Close()
		t.Fatal("required media metadata silently reset")
	}
}

func TestIncomingMediaCorruptionStaysStorageErrorWithoutRedownload(t *testing.T) {
	a, _, session, message, _, calls := incomingFixture(t)
	id := incomingComplete(t, a, session, message)
	if err := os.WriteFile(filepath.Join(a.Service.directory, "media", id+".enc"), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := a.Download(context.Background(), a.Service.data.Binding, session, message+"/regular")
	if !errors.Is(err, bot.ErrStorage) || *calls != 1 {
		t.Fatal("corruption became a cache miss", err, *calls)
	}
	if _, err = a.Download(context.Background(), strings.Repeat("f", 64), session, message+"/regular"); !errors.Is(err, bot.ErrScope) {
		t.Fatal(err)
	}
	if result, e := a.ReclaimIncomingMedia(context.Background()); e != nil || result.ReclaimedFiles != 1 {
		t.Fatal("explicit cleanup could not discard owned corrupt cache", result, e)
	}
}

func TestIncomingMediaCanceledConsumerReleasesWaiter(t *testing.T) {
	a, h, session, message, _, _ := incomingFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := a.Download(ctx, a.Service.data.Binding, session, message+"/regular"); result <- err }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		a.Service.mu.RLock()
		pinned := len(h.mediaWaiters) > 0
		a.Service.mu.RUnlock()
		if pinned {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("request did not register a waiter")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if len(h.mediaWaiters) != 0 {
		t.Fatal("canceled request leaked a pin")
	}
	status, err := a.IncomingMediaStatus()
	if err != nil || strings.Contains(fmt.Sprint(status), "synthetic-private") {
		t.Fatal(status, err)
	}
}

func TestIncomingMediaHistoryWithoutDeliveryArchiveUsesReservationsAndQuota(t *testing.T) {
	body := mediaTestPNG(t)
	s, session, message := mediaFixture(t, body, "image", "")
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
	s.SetMediaDownloader(func(context.Context, MediaDownloadRequest) (*cdn.Result, error) { return verifiedMedia(body), nil })
	a.wrapArchiveMediaDownloader()
	id, err := s.Download(s.data.Binding, session, message, "regular")
	if err != nil {
		t.Fatal(err)
	}
	reservedBeforeFile := false
	h.guardWrite = func(raw []byte) error {
		row := incomingRecord(t, h, s.data.Binding, id)
		if row.Phase == "reserved" {
			if _, err := os.Lstat(filepath.Join(s.directory, "media", id+".enc")); os.IsNotExist(err) {
				reservedBeforeFile = true
			}
		}
		return store.Atomic(filepath.Join(s.directory, "event-history.required"), raw)
	}
	if err = s.downloadQueued(context.Background()); err != nil || !reservedBeforeFile {
		t.Fatal("history-only download skipped durable reservation", reservedBeforeFile, err)
	}
	request := bot.Send{Account: s.data.Binding, Epoch: a.Snapshot().Epoch, Conversation: session, Key: "history-only-upload", Upload: &bot.Upload{Kind: "file", Name: "fixture.bin", Data: []byte("file")}}
	if _, err = a.Submit(context.Background(), request); err != nil {
		t.Fatal("history-only upload failed", err)
	}
	if stats, err := h.Stats(s.data.Binding); err != nil || stats.IncomingResidentBytes != int64(len(body)) {
		t.Fatal(stats, err)
	}
}

func TestIncomingMediaIndexCiphertextFailureRemainsTypedThroughOneBot(t *testing.T) {
	for _, bucket := range []string{"incoming-media", "incoming-media-latest"} {
		t.Run(bucket, func(t *testing.T) {
			a, h, session, message, _, _ := incomingFixture(t)
			id := incomingComplete(t, a, session, message)
			account := a.Service.data.Binding
			key := h.mediaKey(account, id)
			if bucket == "incoming-media-latest" {
				row := incomingRecord(t, h, account, id)
				key = h.mediaLatestKey(account, row.Job)
			}
			if err := h.Close(); err != nil {
				t.Fatal(err)
			}
			db, err := bolt.Open(filepath.Join(a.Service.directory, "event-history", "events.db"), 0600, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err = db.Update(func(tx *bolt.Tx) error {
				return tx.Bucket([]byte(bucket)).Put(key, []byte("unauthenticated synthetic ciphertext"))
			}); err != nil {
				_ = db.Close()
				t.Fatal(err)
			}
			_ = db.Close()
			b, _ := incomingRestart(t, a, h)
			state, err := onebot.OpenState(filepath.Join(t.TempDir(), "onebot"))
			if err != nil {
				t.Fatal(err)
			}
			defer state.Close()
			runtime := onebot.New(b, state)
			fileID, err := state.ID(account, "file", session+"\x00"+message+"/regular")
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(fmt.Sprint(fileID))
			response := runtime.Invoke(context.Background(), onebot.Request{Action: "get_image", Params: map[string]json.RawMessage{"file": raw}})
			if response.Retcode != 1500 || response.Message != "runtime_storage_unavailable" {
				t.Fatal("ciphertext failure downgraded", response)
			}
		})
	}
}

func TestIncomingMediaSharedByteQuotaReclaimsOnlyIncoming(t *testing.T) {
	a, h, session, message, body, _ := incomingFixture(t)
	s := a.Service
	old := incomingComplete(t, a, session, message)
	s.data.Artifacts = map[string]messaging.Artifact{"synthetic-budget": {Size: MaxMediaCacheBytes - int64(len(body))}}
	next := s.data.Messages[0]
	next.ServerID++
	s.ingestLocked(protocol.SyncResult{Messages: []protocol.Message{next}})
	incomingComplete(t, a, session, messageKey(next, s.data.Session.Bootstrap.Username))
	if row := incomingRecord(t, h, s.data.Binding, old); row.Phase != "evicted" {
		t.Fatal("shared byte pressure did not reclaim incoming", row.Phase)
	}
	request := bot.Send{Account: s.data.Binding, Epoch: a.Snapshot().Epoch, Conversation: session, Key: "over-budget", Upload: &bot.Upload{Kind: "file", Name: "one.bin", Data: []byte{1}}}
	if _, err := a.Submit(context.Background(), request); !errors.Is(err, ErrMediaQueueFull) || len(s.data.Jobs) != 0 {
		t.Fatal("pinned byte budget ignored", err)
	}
}

func TestIncomingMediaReclamationRefusesSubstitutedCacheDirectory(t *testing.T) {
	a, _, session, message, _, _ := incomingFixture(t)
	id := incomingComplete(t, a, session, message)
	root := filepath.Join(a.Service.directory, "media")
	original := filepath.Join(a.Service.directory, "media-owned")
	if err := os.Rename(root, original); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	target := filepath.Join(outside, id+".enc")
	body := []byte("unrelated private fixture")
	if err := os.WriteFile(target, body, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, root); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ReclaimIncomingMedia(context.Background()); !errors.Is(err, bot.ErrStorage) {
		t.Fatal("substituted cache root accepted", err)
	}
	actual, err := os.ReadFile(target)
	if err != nil || !bytes.Equal(actual, body) {
		t.Fatal("reclamation escaped its cache directory", err)
	}
	if err = os.Remove(root); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(original, root); err != nil {
		t.Fatal(err)
	}
	if _, err = a.ReclaimIncomingMedia(context.Background()); err != nil {
		t.Fatal("repaired cache root could not reconcile", err)
	}
}
