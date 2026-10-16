package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/JFSAS/wechat-runtime/internal/cdn"
	"github.com/JFSAS/wechat-runtime/internal/media"
	"github.com/JFSAS/wechat-runtime/internal/securedb"
	"github.com/JFSAS/wechat-runtime/internal/store"
	"github.com/JFSAS/wechat-runtime/pkg/bot"
)

func (h *EventHistory) mediaWriteLocked(account string, change func(*securedb.Tx) error) error {
	write := h.write
	if write == nil {
		write = h.db.Update
	}
	if err := write(change); err != nil {
		h.unavailable = true
		return ErrEventHistory
	}
	if err := h.ensureGuard(account); err != nil {
		h.unavailable = true
		return ErrEventHistory
	}
	return nil
}
func (h *EventHistory) mediaReadLocked(account string, read func(*securedb.Tx, incomingMediaMeta) error) error {
	if h.closed || h.unavailable || h.stageErr != nil {
		return ErrEventHistory
	}
	err := h.db.View(func(tx *securedb.Tx) error {
		meta, ready, err := h.mediaMeta(tx, account, h.published.Namespace)
		if err != nil || !ready || meta.Revision != h.publishedMedia || h.published.Account != account {
			return ErrEventHistory
		}
		return read(tx, meta)
	})
	if err != nil {
		return ErrEventHistory
	}
	return nil
}

func (h *EventHistory) reserveMediaLocked(s *Service, id string, job *store.MediaJob, result *cdn.Result) error {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	if h.closed || h.unavailable || h.stageErr != nil || !validIncomingMediaJob(s.data.Binding, id, *job) || job.Status != "downloading" {
		return ErrEventHistory
	}
	return h.mediaWriteLocked(s.data.Binding, func(tx *securedb.Tx) error {
		meta, ready, err := h.mediaMeta(tx, s.data.Binding, historyNamespace(s.data))
		if err != nil || !ready {
			return ErrEventHistory
		}
		old, err := h.mediaRecord(tx, s.data.Binding, id)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(result.Data)
		if old != nil {
			if old.Phase != "reserved" || !sameIncomingMediaIdentity(old.Job, *job) || old.Job.Attempts != job.Attempts || old.Bytes != int64(len(result.Data)) || old.Digest != hex.EncodeToString(digest[:]) {
				return ErrEventHistory
			}
			return nil
		}
		row := incomingMediaRecord{Schema: 1, Account: s.data.Binding, ID: id, Job: *job, Phase: "reserved", Bytes: int64(len(result.Data)), Digest: hex.EncodeToString(digest[:]), CapturedAt: job.CreatedAt}
		if err = h.putMediaRecord(tx, &meta, row, nil); err != nil {
			return err
		}
		return h.putMediaMeta(tx, meta)
	})
}

func (h *EventHistory) mediaProtected(s *Service, id string) (bool, error) {
	if s.data.Jobs[id] != nil {
		return true, nil
	}
	for _, artifact := range s.data.Artifacts {
		if artifact.CacheID == id {
			return true, nil
		}
	}
	if h.deliveryArchive != nil {
		row, err := h.deliveryArchive.indexed(s.data.Binding, "jobs", id)
		if err != nil {
			return true, err
		}
		if row != nil {
			return true, nil
		}
	}
	return false, nil
}
func (h *EventHistory) recoverableMedia(tx *securedb.Tx, s *Service, row incomingMediaRecord) (bool, error) {
	message, err := h.record(tx, row.Account, row.Job.MessageID)
	if err != nil {
		return false, err
	}
	if message == nil {
		return false, nil
	}
	peer := message.Raw.Peer(s.data.Session.Bootstrap.Username)
	if s.sessionID(peer) != row.Job.SessionID {
		return false, ErrEventHistory
	}
	description, err := media.Parse(message.Raw.Type, normalizedMessage(message.Raw, s.data.Session.Bootstrap.Username).Content)
	if err != nil {
		return false, nil
	}
	return string(description.Public().Kind) == row.Job.Kind && description.DownloadReference().Has(media.Variant(row.Job.Variant)), nil
}
func (h *EventHistory) markMediaPendingLocked(s *Service, rows []incomingMediaRecord) error {
	return h.mediaWriteLocked(s.data.Binding, func(tx *securedb.Tx) error {
		meta, ready, err := h.mediaMeta(tx, s.data.Binding, historyNamespace(s.data))
		if err != nil || !ready {
			return ErrEventHistory
		}
		for _, row := range rows {
			current, err := h.mediaRecord(tx, row.Account, row.ID)
			if err != nil || current == nil {
				return ErrEventHistory
			}
			if current.Phase == "evicted" {
				continue
			}
			if current.Phase == "reserved" {
				return ErrEventHistory
			}
			updated := *current
			updated.Phase = "pending_delete"
			if err = h.putMediaRecord(tx, &meta, updated, current); err != nil {
				return err
			}
		}
		return h.putMediaMeta(tx, meta)
	})
}

func (h *EventHistory) settleMediaReservationsLocked(s *Service) error {
	var rows []incomingMediaRecord
	err := h.db.View(func(tx *securedb.Tx) error {
		prefix := h.db.Key("incoming-media-reservations", s.data.Binding)
		return tx.Scan("incoming-media-reservations", prefix, prefix, incomingMediaFiles, func(key, value []byte) (bool, error) {
			var id string
			if json.Unmarshal(value, &id) != nil {
				return false, ErrEventHistory
			}
			row, err := h.mediaRecord(tx, s.data.Binding, id)
			if err != nil || row == nil || !bytes.Equal(key, append(prefix, h.db.Key("id", id)...)) {
				return false, ErrEventHistory
			}
			if row.Phase != "reserved" {
				return false, ErrEventHistory
			}
			job := s.data.MediaJobs[id]
			if job != nil && !sameIncomingMediaIdentity(*job, row.Job) {
				return false, ErrEventHistory
			}
			if job != nil && job.Status == "downloading" {
				return true, nil
			}
			rows = append(rows, *row)
			return true, nil
		})
	})
	if err != nil {
		return ErrEventHistory
	}
	if len(rows) == 0 {
		return nil
	}
	return h.mediaWriteLocked(s.data.Binding, func(tx *securedb.Tx) error {
		meta, ready, err := h.mediaMeta(tx, s.data.Binding, historyNamespace(s.data))
		if err != nil || !ready {
			return ErrEventHistory
		}
		for _, row := range rows {
			old := row
			row.Job.Status = "failed"
			row.Job.Error = "media_interrupted"
			row.Phase = "pending_delete"
			if err = h.putMediaRecord(tx, &meta, row, &old); err != nil {
				return err
			}
		}
		return h.putMediaMeta(tx, meta)
	})
}
func (h *EventHistory) syncMediaDirectory() error {
	if h.mediaSync != nil {
		return h.mediaSync()
	}
	path := filepath.Join(h.directory, "media")
	if err := store.Directory(path); err != nil {
		return ErrEventHistory
	}
	directory, err := os.Open(path)
	if err != nil {
		return ErrEventHistory
	}
	defer directory.Close()
	return directory.Sync()
}
func privateMediaFile(path string) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return ErrEventHistory
	}
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != uint32(os.Getuid()) {
		return ErrEventHistory
	}
	return nil
}

func (h *EventHistory) finishMediaDeletesLocked(s *Service) error {
	if err := h.settleMediaReservationsLocked(s); err != nil {
		return err
	}
	for pass := 0; pass < incomingMediaFiles/64; pass++ {
		var rows []incomingMediaRecord
		err := h.db.View(func(tx *securedb.Tx) error {
			prefix := h.db.Key("incoming-media-pending", s.data.Binding)
			return tx.Scan("incoming-media-pending", prefix, prefix, incomingMediaFiles, func(key, value []byte) (bool, error) {
				var id string
				if json.Unmarshal(value, &id) != nil {
					return false, ErrEventHistory
				}
				row, err := h.mediaRecord(tx, s.data.Binding, id)
				if err != nil || row == nil || row.Phase != "pending_delete" || !bytes.Equal(key, append(h.db.Key("incoming-media-pending", row.Account), h.db.Key("id", row.ID)...)) {
					return false, ErrEventHistory
				}
				rows = append(rows, *row)
				return len(rows) < 64, nil
			})
		})
		if err != nil {
			return ErrEventHistory
		}
		if len(rows) == 0 {
			return nil
		}
		if err = h.ensureGuard(s.data.Binding); err != nil {
			return ErrEventHistory
		}
		// Check the cache directory itself before Remove follows its parent path.
		// Validating only the final file would allow a substituted directory link
		// to redirect an otherwise valid opaque filename outside this profile.
		if err = store.Directory(filepath.Join(h.directory, "media")); err != nil {
			return ErrEventHistory
		}
		for _, row := range rows {
			protected, err := h.mediaProtected(s, row.ID)
			if err != nil || protected {
				return ErrEventHistory
			}
			path := filepath.Join(h.directory, "media", row.ID+".enc")
			if err = privateMediaFile(path); err != nil {
				return err
			}
			// Pending metadata and the outside guard prove cache ownership. Cache
			// corruption remains a read error, but does not make discardable owned
			// bytes undeletable; no payload is used as successful content here.
			remove := h.mediaRemove
			if remove == nil {
				remove = func(id string) error {
					return (store.MediaCache{Directory: filepath.Join(h.directory, "media"), Binding: s.data.Binding}).Remove(id)
				}
			}
			if err = remove(row.ID); err != nil {
				return ErrEventHistory
			}
			if _, err = os.Lstat(path); !os.IsNotExist(err) {
				return ErrEventHistory
			}
		}
		if err = h.syncMediaDirectory(); err != nil {
			return ErrEventHistory
		}
		if err = h.mediaWriteLocked(s.data.Binding, func(tx *securedb.Tx) error {
			meta, ready, err := h.mediaMeta(tx, s.data.Binding, historyNamespace(s.data))
			if err != nil || !ready {
				return ErrEventHistory
			}
			for _, row := range rows {
				old, err := h.mediaRecord(tx, row.Account, row.ID)
				if err != nil || old == nil || old.Phase != "pending_delete" {
					return ErrEventHistory
				}
				if meta.ReclaimedBytes > math.MaxInt64-old.Bytes {
					return ErrEventHistory
				}
				updated := *old
				updated.Phase = "evicted"
				meta.ReclaimedBytes += old.Bytes
				if err = h.putMediaRecord(tx, &meta, updated, old); err != nil {
					return err
				}
			}
			return h.putMediaMeta(tx, meta)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (h *EventHistory) reclaimMediaLocked(ctx context.Context, s *Service, bytesWanted int64, filesWanted uint64) (int, int64, error) {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	if h.closed || h.unavailable || h.stageErr != nil {
		return 0, 0, ErrEventHistory
	}
	count := 0
	var reclaimed int64
	for pass := 0; pass < incomingMediaFiles/64; pass++ {
		if err := ctx.Err(); err != nil {
			return count, reclaimed, err
		}
		var selected []incomingMediaRecord
		err := h.mediaReadLocked(s.data.Binding, func(tx *securedb.Tx, meta incomingMediaMeta) error {
			if meta.ResidentBytes <= bytesWanted && meta.ResidentFiles <= filesWanted {
				return nil
			}
			prefix := h.db.Key("incoming-media-live", s.data.Binding)
			remainingBytes, remainingFiles := meta.ResidentBytes, meta.ResidentFiles
			return tx.Scan("incoming-media-live", prefix, prefix, incomingMediaFiles, func(key, value []byte) (bool, error) {
				var id string
				if json.Unmarshal(value, &id) != nil {
					return false, ErrEventHistory
				}
				row, err := h.mediaRecord(tx, s.data.Binding, id)
				if err != nil || row == nil || !bytes.Equal(key, h.mediaLiveKey(*row)) {
					return false, ErrEventHistory
				}
				if row.Phase != "resident" {
					return true, nil
				}
				if h.mediaWaiters[row.Account+"/"+row.ID] > 0 {
					return true, nil
				}
				protected, err := h.mediaProtected(s, row.ID)
				if err != nil {
					return false, err
				}
				if protected {
					return true, nil
				}
				recoverable, err := h.recoverableMedia(tx, s, *row)
				if err != nil {
					return false, err
				}
				if !recoverable {
					return true, nil
				}
				selected = append(selected, *row)
				remainingBytes -= row.Bytes
				remainingFiles--
				return len(selected) < 64 && (remainingBytes > bytesWanted || remainingFiles > filesWanted), nil
			})
		})
		if err != nil {
			return count, reclaimed, err
		}
		if len(selected) == 0 {
			return count, reclaimed, nil
		}
		if err = h.markMediaPendingLocked(s, selected); err != nil {
			return count, reclaimed, err
		}
		if err = h.finishMediaDeletesLocked(s); err != nil {
			h.unavailable = true
			return count, reclaimed, err
		}
		for _, row := range selected {
			count++
			reclaimed += row.Bytes
		}
	}
	return count, reclaimed, nil
}

func (h *EventHistory) cleanupMediaAtomicLocked() error {
	path := filepath.Join(h.directory, "media")
	if _, err := os.Lstat(path); os.IsNotExist(err) {
		return nil
	}
	if err := store.Directory(path); err != nil {
		return ErrEventHistory
	}
	directory, err := os.Open(path)
	if err != nil {
		return ErrEventHistory
	}
	defer directory.Close()
	for {
		entries, err := directory.ReadDir(128)
		for _, entry := range entries {
			n := entry.Name()
			if !strings.HasPrefix(n, ".write-") {
				continue
			}
			if len(n) <= 7 || len(n) > 27 || strings.Trim(n[7:], "0123456789") != "" {
				return ErrEventHistory
			}
			file := filepath.Join(path, n)
			if err := privateMediaFile(file); err != nil {
				return err
			}
			if err := os.Remove(file); err != nil && !os.IsNotExist(err) {
				return ErrEventHistory
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return ErrEventHistory
		}
	}
	return h.syncMediaDirectory()
}

func (h *EventHistory) mediaStatsLocked(account string) (incomingMediaMeta, error) {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	var result incomingMediaMeta
	err := h.mediaReadLocked(account, func(_ *securedb.Tx, meta incomingMediaMeta) error { result = meta; return nil })
	return result, err
}

func (h *EventHistory) restoreMediaLocked(s *Service) error {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	if h.closed || h.unavailable || h.stageErr != nil {
		return ErrEventHistory
	}
	if err := h.cleanupMediaAtomicLocked(); err != nil {
		h.unavailable = true
		return err
	}
	if err := h.finishMediaDeletesLocked(s); err != nil {
		h.unavailable = true
		return err
	}
	return h.pruneMediaLocked(s)
}

func (h *EventHistory) latestMediaLocked(s *Service, session, message, variant string) (*incomingMediaRecord, error) {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	var row *incomingMediaRecord
	err := h.mediaReadLocked(s.data.Binding, func(tx *securedb.Tx, _ incomingMediaMeta) error {
		key := h.mediaLatestKey(s.data.Binding, store.MediaJob{SessionID: session, MessageID: message, Variant: variant})
		raw, err := tx.Get("incoming-media-latest", key)
		if err != nil || raw == nil {
			return err
		}
		var id string
		if json.Unmarshal(raw, &id) != nil {
			return ErrEventHistory
		}
		row, err = h.mediaRecord(tx, s.data.Binding, id)
		if err != nil || row == nil || row.Job.SessionID != session || row.Job.MessageID != message || row.Job.Variant != variant || row.Job.Status == "downloading" {
			return ErrEventHistory
		}
		return nil
	})
	return row, err
}

func (h *EventHistory) contentFromMediaLocked(row incomingMediaRecord) (bot.Content, bool, error) {
	if row.Phase != "resident" || row.Job.Status != "downloaded" {
		return bot.Content{}, false, nil
	}
	data, present, err := h.readMediaPayload(row)
	if err != nil {
		return bot.Content{}, false, err
	}
	if !present {
		return bot.Content{}, false, nil
	}
	return bot.Content{Data: data, MIME: row.Job.MIME, Name: row.Job.Name}, true, nil
}
