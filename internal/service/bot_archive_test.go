package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/JFSAS/wechat-runtime/internal/cdn"
	"github.com/JFSAS/wechat-runtime/internal/messaging"
	"github.com/JFSAS/wechat-runtime/internal/protocol"
	"github.com/JFSAS/wechat-runtime/internal/store"
	"github.com/JFSAS/wechat-runtime/pkg/bot"
)

func archiveFixture(t *testing.T) (BotAdapter, bot.Send) {
	t.Helper()
	a, request := mixedFixture(t)
	archive, err := OpenDeliveryArchive(a.Service.directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = archive.Close() })
	a.Archive = archive
	return a, request
}

func acknowledgeArchiveFixture(t *testing.T, a BotAdapter, request bot.Send) bot.Delivery {
	t.Helper()
	delivery, err := a.Submit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{delivery.ID}
	if len(delivery.Parts) > 0 {
		ids = nil
		for _, part := range delivery.Parts {
			ids = append(ids, part.ID)
		}
	}
	for index, id := range ids {
		job := a.Service.data.Jobs[id]
		job.Status, job.ServerID = "accepted", uint64(7000+index)
		if job.Media != nil {
			job.Media.Stage = "completed"
		}
	}
	return delivery
}

func TestArchivePreservesMixedOutcomesMediaQueriesAndIdempotency(t *testing.T) {
	a, request := archiveFixture(t)
	delivery := acknowledgeArchiveFixture(t, a, request)
	s := a.Service
	reason := "send_outcome_unknown"
	s.data.Jobs[delivery.Parts[1].ID].Status, s.data.Jobs[delivery.Parts[1].ID].ServerID = "unknown", 0
	s.data.Jobs[delivery.Parts[1].ID].ErrorCategory = &reason
	s.data.Jobs[delivery.Parts[2].ID].Status, s.data.Jobs[delivery.Parts[2].ID].ServerID = "failed", 0
	s.data.Jobs[delivery.Parts[3].ID].Status, s.data.Jobs[delivery.Parts[3].ID].ServerID = "cancelled", 0
	before, err := a.Delivery(request.Account, delivery.ID)
	if err != nil || before.State != "unknown" {
		t.Fatal(before.State, err)
	}
	report, err := a.ArchiveCompleted(context.Background())
	if err != nil || report.ArchivedJobs != 4 || report.ArchivedBatches != 1 || len(s.data.Jobs) != 0 || len(s.data.DeliveryBatches) != 0 {
		t.Fatal(report, err)
	}
	after, err := a.Delivery(request.Account, delivery.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("archival changed the normalized outcome", err)
	}
	message, err := a.Message(request.Account, before.Parts[0].MessageID)
	if err != nil || message.ID != before.Parts[0].MessageID || !message.LocalEcho {
		t.Fatal("canonical receipt cannot be queried after pruning", err)
	}
	for _, index := range []int{1, 2} {
		part, err := a.Delivery(request.Account, delivery.Parts[index].ID)
		if err != nil || part.State != before.Parts[index].State {
			t.Fatal("native part lookup changed", index, err)
		}
		content, err := a.Download(context.Background(), request.Account, part.Conversation, part.Message[0].Data["file"])
		if err != nil || !bytes.Equal(content.Data, request.Outputs[index].Upload.Data) {
			t.Fatal("archival lost retained encrypted media", index, err)
		}
	}
	stats, err := a.Archive.Stats(request.Account)
	if err != nil || stats.ArchivedJobs != 4 || stats.RetainedMediaBytes != int64(len(request.Outputs[1].Upload.Data)+len(request.Outputs[2].Upload.Data)) {
		t.Fatal(stats, err)
	}
	s.mode = "offline"
	request.Epoch++
	again, err := a.Submit(context.Background(), request)
	if err != nil || !reflect.DeepEqual(before, again) || len(s.data.Jobs) != 0 {
		t.Fatal("archived request was requeued or changed", err)
	}
	request.Outputs[0].Text = "conflicting-content"
	if _, err = a.Submit(context.Background(), request); !errors.Is(err, bot.ErrConflict) {
		t.Fatal("archived fingerprint conflict accepted", err)
	}
	if _, err = a.Delivery(strings.Repeat("f", 64), delivery.ID); !errors.Is(err, bot.ErrScope) {
		t.Fatal("archived delivery crossed account scope", err)
	}
	if err = a.Archive.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenDeliveryArchive(s.directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err = reopened.Recover(s.directory); err != nil {
		t.Fatal(err)
	}
	data, err := s.checkpoint.Load()
	if err != nil || len(data.Jobs) != 0 {
		t.Fatal("pruned checkpoint did not reopen", err)
	}
	s.data = data
	a.Archive = reopened
	request.Outputs[0].Text = "before"
	again, err = a.Submit(context.Background(), request)
	if err != nil || !reflect.DeepEqual(before, again) {
		t.Fatal("restart lost archived idempotency or unknown outcome", err)
	}
}

func TestArchiveNeverPrunesInFlightSharedOrGuardedBatches(t *testing.T) {
	a, template := archiveFixture(t)
	request := bot.Send{Account: template.Account, Epoch: template.Epoch, Conversation: template.Conversation, Text: "synthetic"}
	for _, status := range []string{"queued", "sending", "uploading", "submitting", "unknown"} {
		request.Key = status
		delivery, err := a.Submit(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		a.Service.data.Jobs[delivery.ID].Status = status
		if status == "unknown" {
			batch := a.Service.data.DeliveryBatches["bot/"+status]
			if _, err = writeBotIntent(a.Service.directory, request.Account, "bot/"+status, batch.Fingerprint, batch.IDs); err != nil {
				t.Fatal(err)
			}
		}
	}
	request.Key = "shared"
	delivery := acknowledgeArchiveFixture(t, a, request)
	a.Service.data.DeliveryBatches["other-producer"] = messaging.DeliveryBatch{IDs: []string{delivery.ID}}
	report, err := a.ArchiveCompleted(context.Background())
	if err != nil || report.ArchivedJobs != 0 || len(a.Service.data.Jobs) != 6 {
		t.Fatal("active or guarded jobs were pruned", report, err)
	}
}

func TestArchivePruneFailureLeavesTerminalCheckpointAndCanRetry(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-prune-rename", true: "after-prune-rename"}[ambiguous], func(t *testing.T) {
			a, request := archiveFixture(t)
			delivery := acknowledgeArchiveFixture(t, a, request)
			writes := 0
			a.persist = func() error {
				writes++
				if writes == 1 || ambiguous {
					if err := a.Service.saveLocked(); err != nil {
						return err
					}
				}
				if writes == 2 {
					return errors.New("synthetic prune persistence failure")
				}
				return nil
			}
			if _, err := a.ArchiveCompleted(context.Background()); err == nil || len(a.Service.data.Jobs) != 4 {
				t.Fatal("failed prune lost active entries", err)
			}
			disk, err := a.Service.checkpoint.Load()
			if err != nil {
				t.Fatal(err)
			}
			for _, job := range disk.Jobs {
				if job.Status != "accepted" {
					t.Fatal("native checkpoint reload could replay an archived operation")
				}
			}
			a.persist = nil
			a.Service.mu.Lock()
			report, err := a.compactLocked(context.Background(), archiveKeepActive)
			a.Service.mu.Unlock()
			if err != nil || report.ArchivedJobs != 4 || len(a.Service.data.Jobs) != 0 {
				t.Fatal("committed cleanup did not retry below high water", report, err)
			}
			stats, err := a.Archive.Stats(request.Account)
			if err != nil || stats.ArchivedBatches != 1 || stats.ArchivedJobs != 4 {
				t.Fatal("prune retry double counted archive rows", stats, err)
			}
			result, err := a.Delivery(request.Account, delivery.ID)
			if err != nil || result.State != "accepted" {
				t.Fatal(result.State, err)
			}
		})
	}
}

func TestArchiveWriteFailureCannotRemoveActiveOrFreezeQueuedSnapshot(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "not-committed", true: "committed-before-error"}[committed], func(t *testing.T) {
			a, request := archiveFixture(t)
			acknowledgeArchiveFixture(t, a, request)
			a.archiveWrite = func(rows []archivedBatch) error {
				if committed {
					if err := a.Archive.store(rows, a.Service.data.Session.Bootstrap.Username); err != nil {
						return err
					}
				}
				return errors.New("synthetic archive commit uncertainty")
			}
			if _, err := a.ArchiveCompleted(context.Background()); err == nil || len(a.Service.data.Jobs) != 4 {
				t.Fatal("archive failure pruned active outcomes", err)
			}
			disk, err := a.Service.checkpoint.Load()
			if err != nil || len(disk.Jobs) != 4 {
				t.Fatal(err)
			}
			for _, job := range disk.Jobs {
				if job.Status != "accepted" {
					t.Fatal("terminal status was not durable before archive attempt")
				}
			}
			if err = a.Archive.Recover(a.Service.directory); err != nil {
				t.Fatal(err)
			}
			disk, err = a.Service.checkpoint.Load()
			if err != nil || (committed && len(disk.Jobs) != 0) || (!committed && len(disk.Jobs) != 4) {
				t.Fatal("startup did not resolve archive uncertainty", err)
			}
		})
	}
}

func TestArchiveReconcilesOldQueuedCheckpointBeforeWorkerStart(t *testing.T) {
	a, request := archiveFixture(t)
	delivery, err := a.Submit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	queued, err := a.Service.checkpoint.Load()
	if err != nil {
		t.Fatal(err)
	}
	for i, part := range delivery.Parts {
		job := a.Service.data.Jobs[part.ID]
		job.Status, job.ServerID = "accepted", uint64(9000+i)
		if job.Media != nil {
			job.Media.Stage = "completed"
		}
	}
	if _, err = a.ArchiveCompleted(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = a.Service.checkpoint.Save(queued); err != nil {
		t.Fatal(err)
	}
	if err = a.Archive.Recover(a.Service.directory); err != nil {
		t.Fatal(err)
	}
	disk, err := a.Service.checkpoint.Load()
	if err != nil || len(disk.Jobs) != 0 || len(disk.DeliveryBatches) != 0 {
		t.Fatal("restored queued checkpoint can replay archived sends", err)
	}
}

func TestArchiveReappearedIntentCannotDeleteRetainedMedia(t *testing.T) {
	a, request := archiveFixture(t)
	delivery := acknowledgeArchiveFixture(t, a, request)
	batch := a.Service.data.DeliveryBatches["bot/"+request.Key]
	if _, err := a.ArchiveCompleted(context.Background()); err != nil {
		t.Fatal(err)
	}
	path, err := writeBotIntent(a.Service.directory, request.Account, "bot/"+request.Key, batch.Fingerprint, batch.IDs)
	if err != nil {
		t.Fatal(err)
	}
	if err = RecoverBotIntents(a.Service.directory, a.Archive); err != nil {
		t.Fatal("archived intent could not reconcile", err)
	}
	if _, err = os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("archived intent was not cleared", err)
	}
	cache := store.MediaCache{Directory: filepath.Join(a.Service.directory, "media"), Binding: request.Account}
	if _, err = cache.Load(delivery.Parts[1].ID); err != nil {
		t.Fatal("intent recovery removed an archived media reference", err)
	}
}

func TestArchivedFilehelperEchoAndUnknownOutcomeRemainSuppressed(t *testing.T) {
	a, template := archiveFixture(t)
	request := bot.Send{Account: template.Account, Epoch: template.Epoch, Conversation: template.Conversation, Key: "echo", Text: "synthetic echo"}
	delivery := acknowledgeArchiveFixture(t, a, request)
	when := a.Service.data.Jobs[delivery.ID].CreateTime
	if _, err := a.ArchiveCompleted(context.Background()); err != nil {
		t.Fatal(err)
	}
	m := protocol.Message{Sender: []byte("filehelper"), Recipient: a.Service.data.Session.Bootstrap.Username, Content: []byte(request.Text), Type: 1, Timestamp: when + 86400, ServerID: 7000}
	if echo, err := a.Archive.localEcho(request.Account, m, a.Service.data.Session.Bootstrap.Username); err != nil || !echo {
		t.Fatal("receipt echo lost after active pruning", echo, err)
	}
	request.Key, request.Text = "unknown-echo", "unknown submission"
	delivery, err := a.Submit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	job := a.Service.data.Jobs[delivery.ID]
	job.Status = "unknown"
	when = job.CreateTime
	if _, err = a.ArchiveCompleted(context.Background()); err != nil {
		t.Fatal(err)
	}
	m.Content, m.Timestamp, m.ServerID = []byte(request.Text), when, 8000
	a.Service.ingestLocked(protocol.SyncResult{Messages: []protocol.Message{m}})
	if err = a.Service.saveLocked(); err != nil {
		t.Fatal(err)
	}
	events, err := a.Events(request.Account, 0, 100)
	if err != nil || len(events) != 1 || !events[0].LocalEcho {
		t.Fatal("archived unknown filehelper echo would reach the consumer", err)
	}
	m.Timestamp = when + 301
	if echo, err := a.Archive.localEcho(request.Account, m, a.Service.data.Session.Bootstrap.Username); err != nil || echo {
		t.Fatal("time fence suppressed a later unrelated message", echo, err)
	}
	m.Timestamp, m.Content = when, []byte("different content")
	if echo, err := a.Archive.localEcho(request.Account, m, a.Service.data.Session.Bootstrap.Username); err != nil || echo {
		t.Fatal("archive suppressed unrelated content", echo, err)
	}
}

func TestArchiveRetainedMediaAndDownloadReservationStayInQuota(t *testing.T) {
	a, request := archiveFixture(t)
	acknowledgeArchiveFixture(t, a, request)
	if _, err := a.ArchiveCompleted(context.Background()); err != nil {
		t.Fatal(err)
	}
	stats, err := a.Archive.Stats(request.Account)
	if err != nil {
		t.Fatal(err)
	}
	s := a.Service
	// Reserve the rest of the budget using synthetic artifact metadata; no
	// hundreds of MiB of real payload are needed to test accounting/interleaving.
	s.data.Artifacts = map[string]messaging.Artifact{"synthetic-budget": {Size: MaxMediaCacheBytes - stats.RetainedMediaBytes - 4}}
	id := strings.Repeat("1", 32)
	s.data.MediaJobs = map[string]*store.MediaJob{id: {Binding: request.Account, Status: "downloading", Attempts: 1}}
	s.SetMediaDownloader(func(context.Context, MediaDownloadRequest) (*cdn.Result, error) {
		return &cdn.Result{Data: []byte("four")}, nil
	})
	a.wrapArchiveMediaDownloader()
	if _, err = s.mediaDownload(context.Background(), MediaDownloadRequest{ID: id}); err != nil {
		t.Fatal(err)
	}
	newRequest := bot.Send{Account: request.Account, Epoch: request.Epoch, Conversation: request.Conversation, Key: "quota", Upload: &bot.Upload{Kind: "file", Name: "one.bin", Data: []byte{1}}}
	if _, err = a.Submit(context.Background(), newRequest); !errors.Is(err, ErrMediaQueueFull) || len(s.data.Jobs) != 0 {
		t.Fatal("download/cache gap allowed an upload beyond shared quota", err)
	}
	s.data.MediaJobs[id].Status = "failed"
	if _, err = a.Submit(context.Background(), newRequest); err != nil {
		t.Fatal("finished download reservation was not released", err)
	}
}

func TestArchiveUnavailableCannotRequeueAnOldRequest(t *testing.T) {
	a, request := archiveFixture(t)
	acknowledgeArchiveFixture(t, a, request)
	if _, err := a.ArchiveCompleted(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := a.Archive.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Submit(context.Background(), request); !errors.Is(err, bot.ErrStorage) || len(a.Service.data.Jobs) != 0 {
		t.Fatal("archive read failure became a new send", err)
	}
}

func TestArchiveRecoveryRejectsChangedRequestBeforePruning(t *testing.T) {
	a, request := archiveFixture(t)
	delivery, err := a.Submit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	old, err := a.Service.checkpoint.Load()
	if err != nil {
		t.Fatal(err)
	}
	for i, part := range delivery.Parts {
		job := a.Service.data.Jobs[part.ID]
		job.Status, job.ServerID = "accepted", uint64(10000+i)
		if job.Media != nil {
			job.Media.Stage = "completed"
		}
	}
	if _, err = a.ArchiveCompleted(context.Background()); err != nil {
		t.Fatal(err)
	}
	batch := old.DeliveryBatches["bot/"+request.Key]
	batch.Fingerprint = strings.Repeat("f", 64)
	old.DeliveryBatches["bot/"+request.Key] = batch
	if err = a.Service.checkpoint.Save(old); err != nil {
		t.Fatal(err)
	}
	if err = a.Archive.Recover(a.Service.directory); err == nil {
		t.Fatal("changed checkpoint request was silently erased")
	}
	disk, err := a.Service.checkpoint.Load()
	if err != nil || len(disk.Jobs) != 4 {
		t.Fatal("failed reconciliation partly pruned the checkpoint", err)
	}
}

func TestAutomaticArchiveValidatesFirstAndReleasesJournalCapacity(t *testing.T) {
	a, template := archiveFixture(t)
	s := a.Service
	s.data.Jobs = map[string]*store.Job{}
	s.data.DeliveryBatches = map[string]messaging.DeliveryBatch{}
	s.data.NextClientID = archiveHighWater + 1
	for index := 0; index < archiveHighWater; index++ {
		id := fmt.Sprintf("%032x", index+1)
		key := fmt.Sprintf("bot/old-%d", index)
		s.data.Jobs[id] = &store.Job{Origin: "onebot", Recipient: "filehelper", Content: "old", ClientID: uint32(index + 1), CreateTime: 1700000000, Status: "accepted", ServerID: uint64(index + 1)}
		s.data.DeliveryBatches[key] = messaging.DeliveryBatch{Fingerprint: strings.Repeat("a", 64), IDs: []string{id}}
	}
	request := bot.Send{Account: template.Account, Epoch: template.Epoch, Conversation: template.Conversation, Key: "new", Text: "new", Mentions: []string{"invalid-user"}}
	if _, err := a.Submit(context.Background(), request); err == nil {
		t.Fatal("invalid mention was accepted")
	}
	stats, err := a.Archive.Stats(template.Account)
	if err != nil || stats.ArchivedJobs != 0 || len(s.data.Jobs) != archiveHighWater {
		t.Fatal("semantic validation ran after archive effects", stats, err)
	}
	request.Mentions = nil
	if _, err = a.Submit(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	stats, err = a.Archive.Stats(template.Account)
	if err != nil || stats.ArchivedJobs != 64 || len(s.data.Jobs) != archiveHighWater-64+1 {
		t.Fatal("automatic bounded pass did not release active journal capacity", stats, err)
	}
}

func TestArchiveDirectoryLossOrOlderCopyCannotResetHistory(t *testing.T) {
	for _, oldCopy := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing-directory", true: "older-copy"}[oldCopy], func(t *testing.T) {
			a, request := archiveFixture(t)
			archiveDir := filepath.Join(a.Service.directory, "delivery-archive")
			if err := a.Archive.Close(); err != nil {
				t.Fatal(err)
			}
			before := map[string][]byte{}
			for _, name := range []string{"deliveries.db", "deliveries.key", "deliveries.db.ready"} {
				value, err := os.ReadFile(filepath.Join(archiveDir, name))
				if err != nil {
					t.Fatal(err)
				}
				before[name] = value
			}
			reopened, err := OpenDeliveryArchive(a.Service.directory)
			if err != nil {
				t.Fatal(err)
			}
			a.Archive = reopened
			acknowledgeArchiveFixture(t, a, request)
			if _, err = a.ArchiveCompleted(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err = reopened.Close(); err != nil {
				t.Fatal(err)
			}
			if oldCopy {
				for name, value := range before {
					if err = os.WriteFile(filepath.Join(archiveDir, name), value, 0600); err != nil {
						t.Fatal(err)
					}
				}
			} else if err = os.RemoveAll(archiveDir); err != nil {
				t.Fatal(err)
			}
			if broken, err := OpenDeliveryArchive(a.Service.directory); err == nil {
				_ = broken.Close()
				t.Fatal("lost/rolled-back archive silently reset idempotency history")
			}
			if !oldCopy {
				if _, err = os.Lstat(archiveDir); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("missing required archive was recreated", err)
				}
			}
		})
	}
}

func TestArchiveCounterReservationSurvivesOlderCheckpointAndWrap(t *testing.T) {
	a, request := archiveFixture(t)
	a.Service.data.NextClientID = 0x7fffffff
	first := acknowledgeArchiveFixture(t, a, request)
	for i, part := range first.Parts {
		if a.Service.data.Jobs[part.ID].ClientID != uint32(i+1) {
			t.Fatal("durable counter changed native nonwrapping batch shape")
		}
	}
	old, err := a.Service.checkpoint.Load()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.ArchiveCompleted(context.Background()); err != nil {
		t.Fatal(err)
	}
	request.Key = "second-batch"
	second, err := a.Submit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	for i, part := range second.Parts {
		job := a.Service.data.Jobs[part.ID]
		if job.ClientID != uint32(i+5) {
			t.Fatal("durable counter did not continue after wrap")
		}
		job.Status, job.ServerID = "accepted", uint64(9000+i)
		if job.Media != nil {
			job.Media.Stage = "completed"
		}
	}
	if _, err = a.ArchiveCompleted(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = a.Service.checkpoint.Save(old); err != nil {
		t.Fatal(err)
	}
	if err = a.Archive.Recover(a.Service.directory); err != nil {
		t.Fatal(err)
	}
	a.Service.data, err = a.Service.checkpoint.Load()
	if err != nil {
		t.Fatal(err)
	}
	request.Key = "after-restore"
	third, err := a.Submit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	for i, part := range third.Parts {
		if a.Service.data.Jobs[part.ID].ClientID != uint32(i+9) {
			t.Fatal("old checkpoint reused previously reserved native IDs")
		}
	}
}
