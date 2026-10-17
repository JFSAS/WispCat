package service

import (
	"context"

	"github.com/JFSAS/wispcat/internal/cdn"
)

type archiveMediaReservation struct {
	Account string
	Attempt uint32
	Bytes   int64
}

// EnableNativeMedia reuses the frozen native implementation, adding only a
// runtime quota check for outgoing cache bytes now referenced by the archive.
func (a BotAdapter) EnableNativeMedia() {
	a.Service.EnableNativeMedia()
	a.wrapArchiveMediaDownloader()
}

func (a BotAdapter) accountedMediaLocked() (int64, error) {
	s := a.Service
	active := mediaCachedBytes(s.data)
	if s.history != nil {
		media, err := s.history.mediaStatsLocked(s.data.Binding)
		if err != nil {
			return 0, err
		}
		active += media.ResidentBytes
	}
	if a.Archive == nil {
		return active, nil
	}
	stats, err := a.Archive.Stats(s.data.Binding)
	if err != nil {
		return 0, err
	}
	a.Archive.reservationMu.Lock()
	defer a.Archive.reservationMu.Unlock()
	var pending int64
	for id, reservation := range a.Archive.reservations {
		job := s.data.MediaJobs[id]
		if reservation.Account != s.data.Binding || job == nil || job.Status != "downloading" || reservation.Attempt != job.Attempts {
			delete(a.Archive.reservations, id)
			continue
		}
		pending += reservation.Bytes
	}
	return active + stats.RetainedMediaBytes + pending, nil
}

func (a BotAdapter) wrapArchiveMediaDownloader() {
	if a.Archive == nil && a.Service.history == nil {
		return
	}
	s := a.Service
	s.mu.Lock()
	defer s.mu.Unlock()
	base := s.mediaDownload
	if base == nil {
		return
	}
	s.mediaDownload = func(ctx context.Context, request MediaDownloadRequest) (*cdn.Result, error) {
		s.mu.RLock()
		if !s.mediaScopeLocked() || s.data.MediaJobs[request.ID] == nil {
			s.mu.RUnlock()
			return nil, ErrMediaUnavailable
		}
		binding, original := s.data.Binding, s.data.MediaJobs[request.ID]
		attempt := original.Attempts
		if s.history != nil {
			if _, err := s.history.mediaStatsLocked(binding); err != nil {
				s.mu.RUnlock()
				return nil, err
			}
		}
		s.mu.RUnlock()
		result, err := base(ctx, request)
		if err != nil || result == nil {
			return result, err
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.mediaScopeLocked() || s.data.Binding != binding {
			return nil, ErrMediaUnavailable
		}
		job := s.data.MediaJobs[request.ID]
		if job != original || job.Binding != s.data.Binding || job.Status != "downloading" || job.Attempts != attempt {
			return nil, ErrMediaUnavailable
		}
		if s.history != nil {
			if err = ctx.Err(); err != nil {
				return nil, err
			}
			if err = validateMediaResult(request, result); err != nil {
				return nil, err
			}
		}
		cached, err := a.accountedMediaLocked()
		if err != nil {
			return nil, err
		}
		if s.history != nil {
			stats, e := s.history.mediaStatsLocked(binding)
			if e != nil {
				return nil, e
			}
			other := cached - stats.ResidentBytes
			target := max(int64(0), int64(MaxMediaCacheBytes)-other-int64(len(result.Data)))
			if stats.ResidentBytes > target || stats.ResidentFiles >= incomingMediaFiles {
				if _, _, e = s.history.reclaimMediaLocked(ctx, s, target, incomingMediaFiles-1); e != nil {
					return nil, e
				}
				cached, e = a.accountedMediaLocked()
				if e != nil {
					return nil, e
				}
			}
			stats, e = s.history.mediaStatsLocked(binding)
			if e != nil {
				return nil, e
			}
			if cached+int64(len(result.Data)) > MaxMediaCacheBytes {
				cached, e = a.ensurePayloadBudgetLocked(ctx, int64(len(result.Data)))
				if e != nil {
					return nil, e
				}
			}
			if cached+int64(len(result.Data)) > MaxMediaCacheBytes || stats.ResidentFiles >= incomingMediaFiles {
				return nil, ErrMediaQueueFull
			}
			if e = s.history.reserveMediaLocked(s, request.ID, job, result); e != nil {
				return nil, e
			}
			return result, nil
		}
		if cached+int64(len(result.Data)) > MaxMediaCacheBytes {
			cached, err = a.ensurePayloadBudgetLocked(ctx, int64(len(result.Data)))
			if err != nil {
				return nil, err
			}
		}
		if cached+int64(len(result.Data)) > MaxMediaCacheBytes {
			return nil, ErrMediaQueueFull
		}
		// Keep these bytes reserved between this callback and the frozen worker's
		// cache commit. New submissions observe the reservation under the same
		// service lock; the next quota query removes it after terminal progress.
		a.Archive.reservationMu.Lock()
		a.Archive.reservations[request.ID] = archiveMediaReservation{Account: s.data.Binding, Attempt: job.Attempts, Bytes: int64(len(result.Data))}
		a.Archive.reservationMu.Unlock()
		return result, nil
	}
}
