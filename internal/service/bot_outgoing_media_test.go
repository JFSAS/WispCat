package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/JFSAS/wechat-runtime/internal/messaging"
	"github.com/JFSAS/wechat-runtime/internal/onebot"
	"github.com/JFSAS/wechat-runtime/internal/protocol"
	"github.com/JFSAS/wechat-runtime/internal/securedb"
	"github.com/JFSAS/wechat-runtime/internal/store"
	"github.com/JFSAS/wechat-runtime/pkg/bot"
	bolt "go.etcd.io/bbolt"
)

func ageOutgoingArchive(a *BotAdapter) {
	a.archiveWrite = func(rows []archivedBatch) error {
		for i := range rows {
			rows[i].ArchivedAt = time.Now().Add(-48 * time.Hour).Unix()
		}
		return a.Archive.store(rows, a.Service.data.Session.Bootstrap.Username)
	}
}

func reopenOutgoingArchive(t *testing.T, a BotAdapter) BotAdapter {
	t.Helper()
	if err := a.Archive.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := OpenDeliveryArchive(a.Service.directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = next.Close() })
	a.Archive = next
	if a.Service.history != nil {
		a.Service.history.deliveryArchive = next
	}
	if err = next.Recover(a.Service.directory); err != nil {
		t.Fatal(err)
	}
	if err = a.RecoverOutgoingMedia(); err != nil {
		t.Fatal(err)
	}
	return a
}

func TestOutgoingMediaRetentionPreservesOutcomesAndUnknownPayloads(t *testing.T) {
	a, request := archiveFixture(t)
	d := acknowledgeArchiveFixture(t, a, request)
	ageOutgoingArchive(&a)
	a.Service.data.Jobs[d.Parts[2].ID].Status, a.Service.data.Jobs[d.Parts[2].ID].ServerID = "unknown", 0
	before, err := a.Delivery(request.Account, d.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.ArchiveCompleted(context.Background()); err != nil {
		t.Fatal(err)
	}
	row, _ := a.Archive.batch(request.Account, "bot/"+request.Key)
	original, _ := json.Marshal(row)
	result, err := a.ReclaimOutgoingMedia(context.Background())
	if err != nil || result.ReleasedFiles != 1 || result.ReleasedBytes != int64(len(request.Outputs[1].Upload.Data)) {
		t.Fatal(result, err)
	}
	row, _ = a.Archive.batch(request.Account, "bot/"+request.Key)
	after, _ := json.Marshal(row)
	if !bytes.Equal(original, after) {
		t.Fatal("payload cleanup rewrote immutable outcome facts")
	}
	current, err := a.Delivery(request.Account, d.ID)
	if err != nil || !reflect.DeepEqual(current, before) {
		t.Fatal("delivery or public reference changed", err)
	}
	if _, err = a.Download(context.Background(), request.Account, before.Conversation, "outgoing:"+d.Parts[1].ID+"/cache"); !errors.Is(err, bot.ErrNotFound) {
		t.Fatal("evicted bytes not reported missing", err)
	}
	unknown, err := a.Download(context.Background(), request.Account, before.Conversation, "outgoing:"+d.Parts[2].ID+"/cache")
	if err != nil || !bytes.Equal(unknown.Data, request.Outputs[2].Upload.Data) {
		t.Fatal("unknown media was reclaimed", err)
	}
	message, err := a.Message(request.Account, before.Parts[1].MessageID)
	if err != nil || message.ID != before.Parts[1].MessageID {
		t.Fatal("receipt lookup disappeared", err)
	}
	echo, err := a.Archive.localEcho(request.Account, protocol.Message{Sender: a.Service.data.Session.Bootstrap.Username, Recipient: []byte(row.Jobs[1].Recipient), ServerID: row.Jobs[1].ServerID}, a.Service.data.Session.Bootstrap.Username)
	if err != nil || !echo {
		t.Fatal("cleanup lost echo suppression", err)
	}
	a = reopenOutgoingArchive(t, a)
	a.Service.mode = "offline"
	request.Epoch++
	again, err := a.Submit(context.Background(), request)
	if err != nil || !reflect.DeepEqual(again, before) || len(a.Service.data.Jobs) != 0 {
		t.Fatal("cleanup/restart replayed a request", err)
	}
	request.Outputs[0].Text = "conflict"
	if _, err = a.Submit(context.Background(), request); !errors.Is(err, bot.ErrConflict) {
		t.Fatal("old fingerprint changed", err)
	}
	stats, err := a.Archive.Stats(request.Account)
	if err != nil || stats.ReleasedMediaFiles != 1 || stats.RetainedMediaBytes != int64(len(request.Outputs[2].Upload.Data)) || stats.ArchivedJobs != 4 {
		t.Fatal(stats, err)
	}
}

func TestOutgoingMediaRetentionKeepsRecentAndUnverifiedFacts(t *testing.T) {
	for _, kind := range []string{"recent", "missing-receipt", "unknown", "failed", "cancelled"} {
		t.Run(kind, func(t *testing.T) {
			a, request := archiveFixture(t)
			d := acknowledgeArchiveFixture(t, a, request)
			if kind != "recent" {
				ageOutgoingArchive(&a)
				for _, part := range d.Parts {
					job := a.Service.data.Jobs[part.ID]
					if kind == "missing-receipt" {
						job.ServerID = 0
					} else {
						job.Status = kind
					}
				}
			}
			if _, err := a.ArchiveCompleted(context.Background()); err != nil {
				t.Fatal(err)
			}
			result, err := a.ReclaimOutgoingMedia(context.Background())
			if err != nil || result.ReleasedFiles != 0 {
				t.Fatal(kind, result, err)
			}
			cache := store.MediaCache{Directory: filepath.Join(a.Service.directory, "media"), Binding: request.Account}
			for _, index := range []int{1, 2} {
				if data, err := cache.Load(d.Parts[index].ID); err != nil || !bytes.Equal(data, request.Outputs[index].Upload.Data) {
					t.Fatal("protected payload lost", kind, err)
				}
			}
		})
	}
}

func TestOutgoingMediaPressureReleasesOnlyConfirmedOldCopies(t *testing.T) {
	a, request := archiveFixture(t)
	d := acknowledgeArchiveFixture(t, a, request)
	ageOutgoingArchive(&a)
	a.Service.data.Jobs[d.Parts[2].ID].Status = "unknown"
	a.Service.data.Jobs[d.Parts[2].ID].ServerID = 0
	if _, err := a.ArchiveCompleted(context.Background()); err != nil {
		t.Fatal(err)
	}
	stats, _ := a.Archive.Stats(request.Account)
	a.Service.data.Artifacts = map[string]messaging.Artifact{"synthetic-byte-pressure": {Size: MaxMediaCacheBytes - stats.RetainedMediaBytes}}
	next := bot.Send{Account: request.Account, Epoch: a.Snapshot().Epoch, Conversation: request.Conversation, Key: "next", Upload: &bot.Upload{Kind: "file", Name: "next.txt", Data: []byte("next")}}
	if _, err := a.Submit(context.Background(), next); err != nil {
		t.Fatal("eligible bytes did not make room", err)
	}
	stats, err := a.Archive.Stats(request.Account)
	if err != nil || stats.ReleasedMediaFiles != 1 || stats.RetainedMediaBytes != int64(len(request.Outputs[2].Upload.Data)) {
		t.Fatal(stats, err)
	}
	next.Key, next.Upload.Data = "too-large-for-protected-budget", make([]byte, len(request.Outputs[1].Upload.Data))
	if _, err = a.Submit(context.Background(), next); !errors.Is(err, ErrMediaQueueFull) {
		t.Fatal("protected bytes were ignored", err)
	}
}

func TestOutgoingMediaFaultsRetainAccountingAndRecoverBeforeWorkers(t *testing.T) {
	for _, kind := range []string{"guard", "unlink", "directory-sync", "final-guard"} {
		t.Run(kind, func(t *testing.T) {
			a, request := archiveFixture(t)
			d := acknowledgeArchiveFixture(t, a, request)
			ageOutgoingArchive(&a)
			if _, err := a.ArchiveCompleted(context.Background()); err != nil {
				t.Fatal(err)
			}
			archive := a.Archive
			injected := errors.New("synthetic outgoing deletion fault")
			if kind == "guard" || kind == "final-guard" {
				archive.guardWrite = func(raw []byte) error {
					stats, err := archive.Stats(request.Account)
					if err != nil {
						return err
					}
					if (kind == "guard" && stats.MediaPending > 0) || (kind == "final-guard" && stats.ReleasedMediaFiles > 0) {
						return injected
					}
					return store.Atomic(filepath.Join(archive.directory, "delivery-archive.required"), raw)
				}
			} else if kind == "unlink" {
				archive.mediaRemove = func(string) error { return injected }
			} else {
				archive.mediaSync = func() error { return injected }
			}
			if _, err := a.ReclaimOutgoingMedia(context.Background()); !errors.Is(err, bot.ErrStorage) {
				t.Fatal("fault returned success", kind, err)
			}
			if _, err := archive.Stats(request.Account); !errors.Is(err, bot.ErrStorage) {
				t.Fatal("ambiguous storage remained available", err)
			}
			if err := archive.db.View(func(tx *securedb.Tx) error {
				meta, err := archive.mediaMeta(tx)
				if err == nil && kind != "final-guard" && (meta.ReleasedBytes != 0 || meta.Pending != 2) {
					t.Fatal("accounting released before durable deletion", meta)
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if kind == "guard" || kind == "unlink" {
				if _, err := os.Lstat(filepath.Join(archive.directory, "media", d.Parts[1].ID+".enc")); err != nil {
					t.Fatal("payload removed before successful guard/unlink", err)
				}
			}
			a = reopenOutgoingArchive(t, a)
			stats, err := a.Archive.Stats(request.Account)
			if err != nil || stats.ReleasedMediaFiles != 2 || stats.RetainedMediaBytes != 0 || stats.MediaPending != 0 {
				t.Fatal("pending deletion did not reconcile", stats, err)
			}
			request.Epoch++
			if result, err := a.Submit(context.Background(), request); err != nil || result.ID != d.ID || len(a.Service.data.Jobs) != 0 {
				t.Fatal("recovery replayed a completed request", err)
			}
		})
	}
}

func TestOutgoingMediaRevisionRejectsOldDatabaseWithSameArchiveCounts(t *testing.T) {
	a, request := archiveFixture(t)
	acknowledgeArchiveFixture(t, a, request)
	ageOutgoingArchive(&a)
	if _, err := a.ArchiveCompleted(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := a.Archive.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(a.Service.directory, "delivery-archive", "deliveries.db")
	old, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	a = reopenOutgoingArchive(t, a)
	if _, err = a.ReclaimOutgoingMedia(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = a.Archive.Close()
	if err = os.WriteFile(path, old, 0600); err != nil {
		t.Fatal(err)
	}
	if stale, err := OpenDeliveryArchive(a.Service.directory); err == nil {
		_ = stale.Close()
		t.Fatal("same-count database rollback lost payload tombstones")
	}
}

func TestOutgoingMediaDirectorySubstitutionCannotDeleteExternalFile(t *testing.T) {
	a, request := archiveFixture(t)
	d := acknowledgeArchiveFixture(t, a, request)
	ageOutgoingArchive(&a)
	if _, err := a.ArchiveCompleted(context.Background()); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(a.Service.directory, "media")
	owned := filepath.Join(a.Service.directory, "media-owned")
	if err := os.Rename(root, owned); err != nil {
		t.Fatal(err)
	}
	external := t.TempDir()
	target := filepath.Join(external, d.Parts[1].ID+".enc")
	data := []byte("unrelated private file")
	if err := os.WriteFile(target, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, root); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ReclaimOutgoingMedia(context.Background()); !errors.Is(err, bot.ErrStorage) {
		t.Fatal("substituted root accepted", err)
	}
	actual, err := os.ReadFile(target)
	if err != nil || !bytes.Equal(actual, data) {
		t.Fatal("cleanup escaped owned directory", err)
	}
	if err = os.Remove(root); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(owned, root); err != nil {
		t.Fatal(err)
	}
	a = reopenOutgoingArchive(t, a)
	if stats, err := a.Archive.Stats(request.Account); err != nil || stats.ReleasedMediaFiles != 2 {
		t.Fatal("repaired root did not reconcile", stats, err)
	}
}

func TestOutgoingMediaCiphertextFailureRemainsOneBotStorageError(t *testing.T) {
	a, request := archiveFixture(t)
	d := acknowledgeArchiveFixture(t, a, request)
	ageOutgoingArchive(&a)
	if _, err := a.ReclaimOutgoingMedia(context.Background()); err != nil {
		t.Fatal(err)
	}
	key := a.Archive.db.Key("release", request.Account, d.Parts[1].ID)
	_ = a.Archive.Close()
	db, err := bolt.Open(filepath.Join(a.Service.directory, "delivery-archive", "deliveries.db"), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("outgoing-media-releases")).Put(key, []byte("invalid encrypted metadata"))
	}); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	a = reopenOutgoingArchive(t, a)
	state, err := onebot.OpenState(filepath.Join(t.TempDir(), "onebot"))
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	id, err := state.ID(request.Account, "file", request.Conversation+"\x00outgoing:"+d.Parts[1].ID+"/cache")
	if err != nil {
		t.Fatal(err)
	}
	file, _ := json.Marshal(fmt.Sprint(id))
	response := onebot.New(a, state).Invoke(context.Background(), onebot.Request{Action: "get_image", Params: map[string]json.RawMessage{"file": file}})
	if response.Retcode != 1500 || response.Message != "runtime_storage_unavailable" {
		t.Fatal("corrupt tombstone downgraded to missing media", response)
	}
}

func addOutgoingArchiveRows(t *testing.T, a BotAdapter, count int) {
	t.Helper()
	digest := sha256.Sum256([]byte{1})
	cache := store.MediaCache{Directory: filepath.Join(a.Service.directory, "media"), Binding: a.Service.data.Binding}
	for start := 0; start < count; start += 64 {
		var rows []archivedBatch
		for i := start; i < min(start+64, count); i++ {
			id := fmt.Sprintf("%032x", i+1)
			if err := cache.Save(id, []byte{1}); err != nil {
				t.Fatal(err)
			}
			job := store.Job{Origin: "onebot", Recipient: "filehelper", Status: "accepted", ServerID: uint64(9000 + i), Kind: "file", Media: &store.OutgoingMedia{Binding: a.Service.data.Binding, Name: "fixture.bin", Size: 1, MIME: "application/octet-stream", Digest: hex.EncodeToString(digest[:]), Stage: "completed"}}
			rows = append(rows, archivedBatch{Schema: 1, Account: a.Service.data.Binding, Key: fmt.Sprintf("bot/synthetic-%d", i), Fingerprint: hex.EncodeToString(digest[:]), IDs: []string{id}, Jobs: []store.Job{job}, NativeNext: 1, ArchivedAt: time.Now().Add(-48 * time.Hour).Unix()})
		}
		if err := a.Archive.store(rows, a.Service.data.Session.Bootstrap.Username); err != nil {
			t.Fatal(err)
		}
	}
}

func TestOutgoingMediaLegacyMigrationAndProtectedPrefixAreBounded(t *testing.T) {
	started := time.Now()
	a, _ := archiveFixture(t)
	addOutgoingArchiveRows(t, a, 600)
	files, err := filepath.Glob(filepath.Join(a.Service.directory, "media", "*.enc"))
	if err != nil || len(files) != 600 {
		t.Fatal("fixture did not create actual encrypted payload files", len(files), err)
	}
	var ciphertextBytes int64
	for _, path := range files {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		ciphertextBytes += info.Size()
	}
	if err := a.Service.saveLocked(); err != nil {
		t.Fatal(err)
	}
	_ = a.Archive.Close()
	path := filepath.Join(a.Service.directory, "delivery-archive", "deliveries.db")
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket([]byte("outgoing-media-candidates")); err != nil {
			return err
		}
		return tx.Bucket([]byte("meta")).Delete([]byte("media-lifecycle"))
	}); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	guardPath := filepath.Join(a.Service.directory, "delivery-archive.required")
	raw, _ := os.ReadFile(guardPath)
	var guard map[string]any
	_ = json.Unmarshal(raw, &guard)
	guard["schema"] = 1
	delete(guard, "minimum_media_revision")
	raw, _ = json.Marshal(guard)
	if err = store.Atomic(guardPath, raw); err != nil {
		t.Fatal(err)
	}
	a = reopenOutgoingArchive(t, a)
	a.Archive.writeMu.Lock()
	err = a.Archive.migrateOutgoingMediaLocked()
	a.Archive.writeMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Archive.db.View(func(tx *securedb.Tx) error {
		meta, err := a.Archive.mediaMeta(tx)
		if err == nil && (meta.Migrated || len(meta.After) != 64) {
			t.Fatal("legacy archive loaded as an unbounded migration", meta)
		}
		count := 0
		if err == nil {
			err = tx.ScanAll("outgoing-media-candidates", nil, 300, func(_, _ []byte) (bool, error) { count++; return true, nil })
		}
		if count != 256 {
			t.Fatal("migration page bound", count)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	a = reopenOutgoingArchive(t, a)
	a.Service.data.Artifacts = map[string]messaging.Artifact{}
	for i := 1; i <= 300; i++ {
		a.Service.data.Artifacts[fmt.Sprint(i)] = messaging.Artifact{CacheID: fmt.Sprintf("%032x", i), Size: 1}
	}
	result, err := a.ReclaimOutgoingMedia(context.Background())
	if err != nil || result.ReleasedFiles != 300 || result.Scanned < 600 || !result.IndexReady {
		t.Fatal("protected prefix blocked later eligible rows", result, err)
	}
	for i := 1; i <= 300; i++ {
		if _, err = os.Lstat(filepath.Join(a.Service.directory, "media", fmt.Sprintf("%032x.enc", i))); err != nil {
			t.Fatal("artifact payload lost", i, err)
		}
	}
	a.Service.data.Artifacts = map[string]messaging.Artifact{}
	result, err = a.ReclaimOutgoingMedia(context.Background())
	if err != nil || result.ReleasedFiles != 300 {
		t.Fatal("released protection could not reclaim old rows", result, err)
	}
	stats, err := a.Archive.Stats(a.Service.data.Binding)
	if err != nil || stats.ReleasedMediaFiles != 600 || stats.RetainedMediaBytes != 0 || stats.ArchivedJobs != 600 {
		t.Fatal(stats, err)
	}
	files, err = filepath.Glob(filepath.Join(a.Service.directory, "media", "*.enc"))
	if err != nil || len(files) != 0 {
		t.Fatal("released files remained on disk", len(files), err)
	}
	report := map[string]any{"archived_jobs": 600, "initial_cache_files": 600, "initial_ciphertext_file_bytes": ciphertextBytes, "protected_files_during_first_reclaim": 300, "released_files": stats.ReleasedMediaFiles, "released_payload_bytes": stats.ReleasedMediaBytes, "final_cache_files": len(files), "migration_page_rows": 256, "elapsed_millis": time.Since(started).Milliseconds(), "system_scope": "synthetic immutable archive rows and actual encrypted one-byte cache files; not native send/upload, throughput or sustained load", "live_wechat_requests": 0}
	rawReport, _ := json.Marshal(report)
	t.Log("OUTGOING_MEDIA_LOAD_REPORT=" + string(rawReport))
}

func TestOutgoingMediaGuardRequiresLifecycleMetadata(t *testing.T) {
	for _, kind := range []string{"missing-meta", "missing-guard-revision", "null-guard-revision"} {
		t.Run(kind, func(t *testing.T) {
			a, _ := archiveFixture(t)
			_ = a.Archive.Close()
			if kind == "missing-meta" {
				db, err := bolt.Open(filepath.Join(a.Service.directory, "delivery-archive", "deliveries.db"), 0600, nil)
				if err != nil {
					t.Fatal(err)
				}
				if err = db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("meta")).Delete([]byte("media-lifecycle")) }); err != nil {
					t.Fatal(err)
				}
				_ = db.Close()
			} else {
				path := filepath.Join(a.Service.directory, "delivery-archive.required")
				raw, _ := os.ReadFile(path)
				var fields map[string]any
				_ = json.Unmarshal(raw, &fields)
				if kind == "missing-guard-revision" {
					delete(fields, "minimum_media_revision")
				} else {
					fields["minimum_media_revision"] = nil
				}
				raw, _ = json.Marshal(fields)
				if err := store.Atomic(path, raw); err != nil {
					t.Fatal(err)
				}
			}
			if archive, err := OpenDeliveryArchive(a.Service.directory); err == nil {
				_ = archive.Close()
				t.Fatal("required lifecycle metadata silently reset", kind)
			}
		})
	}
}

func TestOutgoingMediaPressureAlsoProtectsNativeIncomingWrites(t *testing.T) {
	a, h, session, message, body, calls := incomingFixture(t)
	request := bot.Send{Account: a.Service.data.Binding, Epoch: a.Snapshot().Epoch, Conversation: session, Key: "outgoing-before-download", Upload: &bot.Upload{Kind: "file", Name: "old.bin", Data: body}}
	d := acknowledgeArchiveFixture(t, a, request)
	ageOutgoingArchive(&a)
	if _, err := a.ArchiveCompleted(context.Background()); err != nil {
		t.Fatal(err)
	}
	a.Service.data.Artifacts = map[string]messaging.Artifact{"synthetic-byte-pressure": {Size: MaxMediaCacheBytes - int64(len(body))}}
	id := incomingComplete(t, a, session, message)
	if *calls != 1 || incomingRecord(t, h, request.Account, id).Phase != "resident" {
		t.Fatal("incoming write could not reclaim confirmed outgoing bytes")
	}
	stats, err := a.Archive.Stats(request.Account)
	if err != nil || stats.ReleasedMediaFiles != 1 || stats.RetainedMediaBytes != 0 {
		t.Fatal(stats, err)
	}
	if _, err = os.Lstat(filepath.Join(a.Service.directory, "media", d.ID+".enc")); !os.IsNotExist(err) {
		t.Fatal("outgoing copy retained despite released accounting", err)
	}
}

func TestOutgoingMediaMissingUnreleasedCopyIsStorageFailure(t *testing.T) {
	a, request := archiveFixture(t)
	d := acknowledgeArchiveFixture(t, a, request)
	if _, err := a.ArchiveCompleted(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(a.Service.directory, "media", d.Parts[1].ID+".enc")); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Download(context.Background(), request.Account, request.Conversation, "outgoing:"+d.Parts[1].ID+"/cache"); !errors.Is(err, bot.ErrStorage) {
		t.Fatal("unrecorded payload loss was presented as intentional eviction", err)
	}
}

func TestOutgoingMediaRespectsIncomingAndArtifactOwnership(t *testing.T) {
	t.Run("incoming-history", func(t *testing.T) {
		a, h, session, message, body, _ := incomingFixture(t)
		id := incomingComplete(t, a, session, message)
		digest := sha256.Sum256(body)
		row := archivedBatch{Schema: 1, Account: a.Service.data.Binding, Key: "bot/shared-role-fixture", Fingerprint: hex.EncodeToString(digest[:]), IDs: []string{id}, NativeNext: 1, ArchivedAt: time.Now().Add(-48 * time.Hour).Unix(), Jobs: []store.Job{{Origin: "onebot", Recipient: "filehelper", Status: "accepted", ServerID: 9000, Kind: "image", Media: &store.OutgoingMedia{Binding: a.Service.data.Binding, Name: "image.png", MIME: "image/png", Size: int64(len(body)), Digest: hex.EncodeToString(digest[:]), Stage: "completed"}}}}
		if err := a.Archive.store([]archivedBatch{row}, a.Service.data.Session.Bootstrap.Username); err != nil {
			t.Fatal(err)
		}
		result, err := a.ReclaimOutgoingMedia(context.Background())
		if err != nil || result.ReleasedFiles != 0 || incomingRecord(t, h, row.Account, id).Phase != "resident" {
			t.Fatal("incoming ownership ignored", result, err)
		}
		if content, err := a.Download(context.Background(), row.Account, session, message+"/regular"); err != nil || !bytes.Equal(content.Data, body) {
			t.Fatal("incoming content lost", err)
		}
	})
	t.Run("artifact-and-active-job", func(t *testing.T) {
		a, request := archiveFixture(t)
		d := acknowledgeArchiveFixture(t, a, request)
		ageOutgoingArchive(&a)
		if _, err := a.ArchiveCompleted(context.Background()); err != nil {
			t.Fatal(err)
		}
		a.Service.data.Artifacts = map[string]messaging.Artifact{"protected": {CacheID: d.Parts[1].ID, Size: 1}}
		a.Service.data.Jobs[d.Parts[2].ID] = &store.Job{Origin: "foreign", Status: "queued"}
		result, err := a.ReclaimOutgoingMedia(context.Background())
		if err != nil || result.ReleasedFiles != 0 {
			t.Fatal("live role ownership ignored", result, err)
		}
		delete(a.Service.data.Jobs, d.Parts[2].ID)
		delete(a.Service.data.Artifacts, "protected")
		result, err = a.ReclaimOutgoingMedia(context.Background())
		if err != nil || result.ReleasedFiles != 2 {
			t.Fatal("unprotected copies could not be reclaimed", result, err)
		}
		if _, err = os.Lstat(filepath.Join(a.Service.directory, "media", request.Account+".key")); err != nil {
			t.Fatal("native media key deleted", err)
		}
	})
}
