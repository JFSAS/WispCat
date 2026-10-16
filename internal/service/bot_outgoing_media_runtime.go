package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/JFSAS/wechat-runtime/internal/securedb"
	"github.com/JFSAS/wechat-runtime/internal/store"
	"github.com/JFSAS/wechat-runtime/pkg/bot"
)

const outgoingMediaRetention = 24 * time.Hour

// ensurePayloadBudgetLocked reclaims disposable copies only under byte pressure.
// Active/unknown outgoing data, artifacts and unrecoverable incoming data remain
// charged; callers still reject writes when protected data exhausts the budget.
func (a BotAdapter) ensurePayloadBudgetLocked(ctx context.Context, needed int64) (int64, error) {
	cached, err := a.accountedMediaLocked()
	if err != nil || cached+needed <= MaxMediaCacheBytes {
		return cached, err
	}
	s := a.Service
	if s.history != nil {
		stats, e := s.history.mediaStatsLocked(s.data.Binding)
		if e != nil {
			return 0, e
		}
		target := max(int64(0), stats.ResidentBytes-(cached+needed-MaxMediaCacheBytes))
		if _, _, e = s.history.reclaimMediaLocked(ctx, s, target, incomingMediaFiles); e != nil {
			return 0, e
		}
		cached, err = a.accountedMediaLocked()
		if err != nil || cached+needed <= MaxMediaCacheBytes {
			return cached, err
		}
	}
	if a.Archive != nil {
		// Remove already archived duplicate checkpoint charges and archive one
		// bounded pass of terminal batches before considering their payloads.
		if _, err = a.compactLocked(ctx, 0); err != nil {
			return 0, err
		}
		cached, err = a.accountedMediaLocked()
		if err != nil || cached+needed <= MaxMediaCacheBytes {
			return cached, err
		}
		stats, e := a.Archive.Stats(s.data.Binding)
		if e != nil {
			return 0, e
		}
		target := max(int64(0), stats.RetainedMediaBytes-(cached+needed-MaxMediaCacheBytes))
		if _, e = a.reclaimOutgoingLocked(ctx, target); e != nil {
			return 0, e
		}
		cached, err = a.accountedMediaLocked()
	}
	return cached, err
}

type OutgoingMediaCleanup struct {
	ReleasedFiles int   `json:"released_files"`
	ReleasedBytes int64 `json:"released_payload_bytes"`
	IndexReady    bool  `json:"index_ready"`
	Scanned       int   `json:"scanned_candidates"`
	More          bool  `json:"more"`
}

func (a BotAdapter) OutgoingMediaStatus() (map[string]any, error) {
	if a.Archive == nil {
		return nil, bot.ErrUnsupported
	}
	s := a.Service
	s.mu.RLock()
	defer s.mu.RUnlock()
	account := ""
	if s.mediaScopeLocked() {
		account = s.data.Binding
	}
	stats, err := a.Archive.Stats(account)
	if err != nil {
		return nil, err
	}
	return map[string]any{"format": "encrypted_outgoing_media_v1", "accounted_payload_bytes": stats.RetainedMediaBytes, "released_payload_bytes": stats.ReleasedMediaBytes, "released_files": stats.ReleasedMediaFiles, "pending_deletions": stats.MediaPending, "index_ready": stats.MediaIndexReady, "minimum_retention_seconds": int64(outgoingMediaRetention.Seconds()), "automatic_trigger": "shared decoded-payload byte pressure", "protected": "active jobs, unknown/failed/cancelled or unverified outcomes, incoming references and artifacts", "metadata_policy": "immutable requests, receipts, fingerprints, media facts and public IDs retained"}, nil
}

func (a *DeliveryArchive) mediaMutationLocked(change func(*securedb.Tx) error) error {
	if a.closed || a.mediaUnavailable.Load() {
		return ErrDeliveryArchive
	}
	err := a.db.Update(change)
	if err == nil {
		err = a.ensureGuard()
	}
	if err != nil {
		a.mediaUnavailable.Store(true)
		return ErrDeliveryArchive
	}
	return nil
}

func outgoingMediaProtected(data *store.Data, history *EventHistory, id string) (bool, error) {
	if data.Jobs[id] != nil || data.MediaJobs[id] != nil {
		return true, nil
	}
	for _, artifact := range data.Artifacts {
		if artifact.CacheID == id {
			return true, nil
		}
	}
	if history != nil {
		history.writeMu.Lock()
		defer history.writeMu.Unlock()
		var found bool
		err := history.mediaReadLocked(data.Binding, func(tx *securedb.Tx, _ incomingMediaMeta) error {
			row, err := history.mediaRecord(tx, data.Binding, id)
			found = row != nil
			return err
		})
		return found, err
	}
	return false, nil
}

func (a *DeliveryArchive) finishOutgoingDeletesLocked(data *store.Data, history *EventHistory) error {
	var meta outgoingMediaMeta
	var refs []outgoingMediaRef
	err := a.db.View(func(tx *securedb.Tx) error {
		var err error
		meta, err = a.mediaMeta(tx)
		if err != nil || meta.Pending == 0 {
			return err
		}
		if data == nil || data.Binding != meta.PendingAccount {
			return ErrDeliveryArchive
		}
		prefix := a.db.Key("outgoing-media-pending", data.Binding)
		return tx.Scan("outgoing-media-pending", prefix, prefix, 64, func(key, raw []byte) (bool, error) {
			ref, err := decodeOutgoingMedia(raw)
			if err != nil || ref.Account != data.Binding || ref.Phase != "pending_delete" || !bytes.Equal(key, append(prefix, a.db.Key("id", ref.ID)...)) {
				return false, ErrDeliveryArchive
			}
			current, err := a.mediaRelease(tx, ref.Account, ref.ID)
			if err != nil || current == nil || *current != ref {
				return false, ErrDeliveryArchive
			}
			_, _, err = a.outgoingMediaJob(tx, ref)
			if err != nil {
				return false, err
			}
			refs = append(refs, ref)
			return len(refs) < 64, nil
		})
	})
	if err != nil || uint64(len(refs)) != meta.Pending {
		return ErrDeliveryArchive
	}
	if len(refs) == 0 {
		return nil
	}
	if err = a.ensureGuard(); err != nil {
		return ErrDeliveryArchive
	}
	directory := filepath.Join(a.directory, "media")
	if err = store.Directory(directory); err != nil {
		return ErrDeliveryArchive
	}
	for _, ref := range refs {
		protected, err := outgoingMediaProtected(data, history, ref.ID)
		if err != nil || protected {
			return ErrDeliveryArchive
		}
		path := filepath.Join(directory, ref.ID+".enc")
		if err = privateMediaFile(path); err != nil {
			return ErrDeliveryArchive
		}
		remove := a.mediaRemove
		if remove == nil {
			remove = func(id string) error {
				return (store.MediaCache{Directory: directory, Binding: ref.Account}).Remove(id)
			}
		}
		if err = remove(ref.ID); err != nil {
			return ErrDeliveryArchive
		}
		if _, err = os.Lstat(path); !os.IsNotExist(err) {
			return ErrDeliveryArchive
		}
	}
	if a.mediaSync != nil {
		err = a.mediaSync()
	} else {
		var dir *os.File
		dir, err = os.Open(directory)
		if err == nil {
			err = dir.Sync()
			closeErr := dir.Close()
			if err == nil {
				err = closeErr
			}
		}
	}
	if err != nil {
		return ErrDeliveryArchive
	}
	return a.mediaMutationLocked(func(tx *securedb.Tx) error {
		m, err := a.mediaMeta(tx)
		if err != nil || m.Pending != uint64(len(refs)) || m.PendingAccount != data.Binding {
			return ErrDeliveryArchive
		}
		account, err := a.mediaAccount(tx, data.Binding)
		if err != nil {
			return err
		}
		for _, ref := range refs {
			current, err := a.mediaRelease(tx, ref.Account, ref.ID)
			if err != nil || current == nil || *current != ref {
				return ErrDeliveryArchive
			}
			_, job, err := a.outgoingMediaJob(tx, ref)
			if err != nil || m.ReleasedBytes > math.MaxInt64-job.Media.Size || account.ReleasedBytes > math.MaxInt64-job.Media.Size || m.ReleasedFiles == math.MaxUint64 || account.ReleasedFiles == math.MaxUint64 {
				return ErrDeliveryArchive
			}
			ref.Phase = "evicted"
			raw, _ := json.Marshal(ref)
			if err = tx.Put("outgoing-media-releases", a.db.Key("release", ref.Account, ref.ID), raw); err != nil {
				return err
			}
			if err = tx.Delete("outgoing-media-pending", append(a.db.Key("outgoing-media-pending", ref.Account), a.db.Key("id", ref.ID)...)); err != nil {
				return err
			}
			m.ReleasedBytes += job.Media.Size
			account.ReleasedBytes += job.Media.Size
			m.ReleasedFiles++
			account.ReleasedFiles++
		}
		m.Pending, m.PendingAccount = 0, ""
		if err = advanceOutgoingMedia(&m); err != nil {
			return err
		}
		if err = a.putMediaAccount(tx, data.Binding, account); err != nil {
			return err
		}
		return a.putMediaMeta(tx, m)
	})
}

func (a *DeliveryArchive) recoverOutgoingLocked(data *store.Data, history *EventHistory) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	if a.closed || a.mediaUnavailable.Load() {
		return ErrDeliveryArchive
	}
	if err := a.resolvePendingLocked(); err != nil {
		return err
	}
	if err := a.finishOutgoingDeletesLocked(data, history); err != nil {
		a.mediaUnavailable.Store(true)
		return err
	}
	return nil
}

// RecoverOutgoingMedia finishes an already guarded deletion before workers.
// Host calls this only after archive/intents and event history have opened.
func (a BotAdapter) RecoverOutgoingMedia() error {
	if a.Archive == nil {
		return nil
	}
	s := a.Service
	s.mu.Lock()
	defer s.mu.Unlock()
	data := s.data
	if data == nil {
		cp, exists, err := store.Unique(filepath.Join(s.directory, "checkpoints"))
		if err != nil {
			return ErrDeliveryArchive
		}
		if exists {
			data, err = cp.Load()
			if err != nil {
				return ErrDeliveryArchive
			}
		}
	}
	return a.Archive.recoverOutgoingLocked(data, s.history)
}

func (a BotAdapter) reclaimOutgoingLocked(ctx context.Context, target int64) (OutgoingMediaCleanup, error) {
	var result OutgoingMediaCleanup
	s := a.Service
	archive := a.Archive
	if archive == nil {
		return result, bot.ErrUnsupported
	}
	if !s.mediaScopeLocked() {
		return result, bot.ErrUnavailable
	}
	archive.writeMu.Lock()
	defer archive.writeMu.Unlock()
	if archive.closed || archive.mediaUnavailable.Load() {
		return result, ErrDeliveryArchive
	}
	if err := archive.resolvePendingLocked(); err != nil {
		return result, err
	}
	if err := archive.finishOutgoingDeletesLocked(s.data, s.history); err != nil {
		archive.mediaUnavailable.Store(true)
		return result, err
	}
	for pass := 0; pass < 8; pass++ {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if err := archive.migrateOutgoingMediaLocked(); err != nil {
			return result, err
		}
		stats, err := archive.Stats(s.data.Binding)
		if err != nil {
			return result, err
		}
		result.IndexReady = stats.MediaIndexReady
		if stats.RetainedMediaBytes <= target {
			return result, nil
		}
		var refs []outgoingMediaRef
		var selectedBytes int64
		var after string
		scanned, ended := 0, true
		err = archive.db.View(func(tx *securedb.Tx) error {
			prefix := archive.db.Key("outgoing-media-candidates", s.data.Binding)
			account, err := archive.mediaAccount(tx, s.data.Binding)
			if err != nil {
				return err
			}
			start := prefix
			if account.After != "" {
				start, _ = hex.DecodeString(account.After)
				if !bytes.HasPrefix(start, prefix) {
					return ErrDeliveryArchive
				}
				start = append(start, 0)
			}
			return tx.Scan("outgoing-media-candidates", prefix, start, 256, func(key, raw []byte) (bool, error) {
				ref, err := decodeOutgoingMedia(raw)
				if err != nil || ref.Account != s.data.Binding || ref.Phase != "resident" {
					return false, ErrDeliveryArchive
				}
				row, job, err := archive.outgoingMediaJob(tx, ref)
				if err != nil || !bytes.Equal(key, archive.outgoingMediaKey(row, ref.ID)) {
					return false, ErrDeliveryArchive
				}
				if row.ArchivedAt > time.Now().Add(-outgoingMediaRetention).Unix() {
					after = ""
					return false, nil
				}
				scanned++
				after = hex.EncodeToString(key)
				protected, err := outgoingMediaProtected(s.data, s.history, ref.ID)
				if err != nil {
					return false, err
				}
				if !protected {
					refs = append(refs, ref)
					selectedBytes += job.Media.Size
				}
				ended = len(refs) < 64 && scanned < 256 && stats.RetainedMediaBytes-selectedBytes > target
				return ended, nil
			})
		})
		if err != nil {
			return result, ErrDeliveryArchive
		}
		result.Scanned += scanned
		if ended {
			after = ""
		}
		result.More = !ended || !result.IndexReady
		err = archive.mediaMutationLocked(func(tx *securedb.Tx) error {
			meta, err := archive.mediaMeta(tx)
			if err != nil || meta.Pending != 0 {
				return ErrDeliveryArchive
			}
			account, err := archive.mediaAccount(tx, s.data.Binding)
			if err != nil {
				return err
			}
			account.After = after
			if err = archive.putMediaAccount(tx, s.data.Binding, account); err != nil {
				return err
			}
			for _, ref := range refs {
				row, _, err := archive.outgoingMediaJob(tx, ref)
				if err != nil {
					return err
				}
				ref.Phase = "pending_delete"
				raw, _ := json.Marshal(ref)
				if err = tx.Put("outgoing-media-releases", archive.db.Key("release", ref.Account, ref.ID), raw); err != nil {
					return err
				}
				if err = tx.Put("outgoing-media-pending", append(archive.db.Key("outgoing-media-pending", ref.Account), archive.db.Key("id", ref.ID)...), raw); err != nil {
					return err
				}
				if err = tx.Delete("outgoing-media-candidates", archive.outgoingMediaKey(row, ref.ID)); err != nil {
					return err
				}
			}
			meta.Pending = uint64(len(refs))
			if len(refs) > 0 {
				meta.PendingAccount = s.data.Binding
			}
			if err = advanceOutgoingMedia(&meta); err != nil {
				return err
			}
			return archive.putMediaMeta(tx, meta)
		})
		if err != nil {
			return result, err
		}
		if len(refs) == 0 {
			if !result.More {
				return result, nil
			}
			continue
		}
		if err = archive.finishOutgoingDeletesLocked(s.data, s.history); err != nil {
			archive.mediaUnavailable.Store(true)
			return result, err
		}
		result.ReleasedFiles += len(refs)
		result.ReleasedBytes += selectedBytes
	}
	return result, nil
}

func (a BotAdapter) ReclaimOutgoingMedia(ctx context.Context) (OutgoingMediaCleanup, error) {
	s := a.Service
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return OutgoingMediaCleanup{}, err
	}
	// Persist/reconcile terminal jobs before any payload can be removed.
	report, err := a.compactLocked(ctx, 0)
	if err != nil {
		return OutgoingMediaCleanup{}, err
	}
	result, err := a.reclaimOutgoingLocked(ctx, 0)
	result.More = result.More || report.ArchivedBatches == 64
	return result, err
}

func (a *DeliveryArchive) outgoingEvicted(account, id string) (bool, error) {
	if a.mediaUnavailable.Load() {
		return false, ErrDeliveryArchive
	}
	evicted := false
	err := a.db.View(func(tx *securedb.Tx) error {
		if _, err := a.mediaMeta(tx); err != nil {
			return err
		}
		ref, err := a.mediaRelease(tx, account, id)
		if err != nil {
			return err
		}
		if ref != nil {
			if _, _, err := a.outgoingMediaJob(tx, *ref); err != nil || ref.Phase != "evicted" {
				return ErrDeliveryArchive
			}
			evicted = true
		}
		return nil
	})
	if err != nil {
		return false, ErrDeliveryArchive
	}
	return evicted, nil
}

func (a BotAdapter) outgoingContentLocked(account, id string, metadata store.OutgoingMedia) (bot.Content, error) {
	if a.Archive != nil {
		evicted, err := a.Archive.outgoingEvicted(account, id)
		if err != nil {
			return bot.Content{}, err
		}
		if evicted {
			return bot.Content{}, bot.ErrNotFound
		}
	}
	content, err := (store.MediaCache{Directory: filepath.Join(a.Service.directory, "media"), Binding: account}).Load(id)
	if err != nil {
		return bot.Content{}, ErrDeliveryArchive
	}
	digest := sha256.Sum256(content)
	if int64(len(content)) != metadata.Size || hex.EncodeToString(digest[:]) != metadata.Digest {
		return bot.Content{}, ErrDeliveryArchive
	}
	return bot.Content{Data: content, MIME: metadata.MIME, Name: metadata.Name}, nil
}
