package service

import (
	"context"
	"errors"
	"time"

	"github.com/JFSAS/wispcat/internal/media"
	"github.com/JFSAS/wispcat/internal/securedb"
	"github.com/JFSAS/wispcat/pkg/bot"
)

func (a BotAdapter) downloadIncomingMedia(ctx context.Context, account, conversation, message, variant string) (bot.Content, error) {
	s := a.Service
	s.mu.Lock()
	if !s.mediaScopeLocked() || s.data.Binding != account {
		s.mu.Unlock()
		return bot.Content{}, bot.ErrScope
	}
	h := s.history
	row, err := h.latestMediaLocked(s, conversation, message, variant)
	if err != nil {
		s.mu.Unlock()
		return bot.Content{}, err
	}
	if row != nil {
		content, ready, e := h.contentFromMediaLocked(*row)
		if e != nil {
			s.mu.Unlock()
			return bot.Content{}, e
		}
		if ready {
			s.mu.Unlock()
			return content, nil
		}
		if row.Phase == "resident" && row.Job.Status == "downloaded" {
			h.writeMu.Lock()
			e = h.markMediaPendingLocked(s, []incomingMediaRecord{*row})
			if e == nil {
				e = h.finishMediaDeletesLocked(s)
			}
			if e != nil {
				h.unavailable = true
			}
			h.writeMu.Unlock()
			if e != nil {
				s.mu.Unlock()
				return bot.Content{}, e
			}
		}
	}
	if !mediaID(conversation, 32) || !mediaID(message, 64) {
		s.mu.Unlock()
		return bot.Content{}, ErrMediaInvalidRequest
	}
	queued, err := s.queueMediaDownloadLocked(conversation, message, media.Variant(variant))
	if err != nil {
		s.mu.Unlock()
		return bot.Content{}, channelError(err)
	}
	id := queued["id"].(string)
	if h.mediaWaiters == nil {
		h.mediaWaiters = map[string]int{}
	}
	key := account + "/" + id
	h.mediaWaiters[key]++
	if err = s.saveLocked(); err != nil {
		h.mediaWaiters[key]--
		if h.mediaWaiters[key] == 0 {
			delete(h.mediaWaiters, key)
		}
		s.mu.Unlock()
		return bot.Content{}, err
	}
	s.mu.Unlock()
	s.signal()
	defer func() {
		s.mu.Lock()
		h.mediaWaiters[key]--
		if h.mediaWaiters[key] == 0 {
			delete(h.mediaWaiters, key)
		}
		s.mu.Unlock()
	}()
	timer := time.NewTicker(50 * time.Millisecond)
	defer timer.Stop()
	for {
		s.mu.RLock()
		if !s.mediaScopeLocked() || s.data.Binding != account {
			s.mu.RUnlock()
			return bot.Content{}, bot.ErrScope
		}
		var current *incomingMediaRecord
		h.writeMu.Lock()
		err = h.mediaReadLocked(account, func(tx *securedb.Tx, _ incomingMediaMeta) error {
			var e error
			current, e = h.mediaRecord(tx, account, id)
			return e
		})
		h.writeMu.Unlock()
		if err != nil {
			s.mu.RUnlock()
			return bot.Content{}, err
		}
		if current != nil && current.Job.Status != "downloading" {
			if current.Job.Status == "failed" {
				s.mu.RUnlock()
				return bot.Content{}, errors.New(current.Job.Error)
			}
			content, ready, e := h.contentFromMediaLocked(*current)
			s.mu.RUnlock()
			if e != nil {
				return bot.Content{}, e
			}
			if !ready {
				return bot.Content{}, ErrMediaUnavailable
			}
			return content, nil
		}
		s.mu.RUnlock()
		select {
		case <-ctx.Done():
			return bot.Content{}, ctx.Err()
		case <-timer.C:
		}
	}
}

type IncomingMediaCleanup struct {
	ReclaimedFiles int   `json:"released_entries"`
	ReleasedBytes  int64 `json:"released_payload_bytes"`
}

func (a BotAdapter) IncomingMediaStatus() (map[string]any, error) {
	s := a.Service
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.history == nil {
		return nil, bot.ErrUnsupported
	}
	account := ""
	active := 0
	if s.mediaScopeLocked() {
		account = s.data.Binding
		active = len(s.data.MediaJobs)
	}
	stats, err := s.history.Stats(account)
	if err != nil {
		return nil, err
	}
	return map[string]any{"format": "encrypted_incoming_media_v1", "tasks": stats.IncomingMediaTasks, "accounted_files": stats.IncomingResidentFiles, "accounted_payload_bytes": stats.IncomingResidentBytes, "released_payload_bytes": stats.IncomingReclaimedBytes, "active_native_tasks": active, "accounted_file_limit": incomingMediaFiles, "shared_payload_quota_bytes": MaxMediaCacheBytes, "accounting": "decoded bytes, including reserved writes and pending deletions", "outgoing_media_policy": "retained; never reclaimed here"}, nil
}
func (a BotAdapter) ReclaimIncomingMedia(ctx context.Context) (IncomingMediaCleanup, error) {
	s := a.Service
	s.mu.Lock()
	defer s.mu.Unlock()
	var result IncomingMediaCleanup
	if s.history == nil {
		return result, bot.ErrUnsupported
	}
	if !s.mediaScopeLocked() {
		return result, bot.ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	// Reconcile any previous ambiguous history or native-checkpoint commit before
	// allowing a new deletion pass. This does not start login or network work.
	if err := s.saveLocked(); err != nil {
		return result, err
	}
	count, released, err := s.history.reclaimMediaLocked(ctx, s, 0, 0)
	result.ReclaimedFiles, result.ReleasedBytes = count, released
	return result, err
}
