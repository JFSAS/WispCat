package service

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/JFSAS/wechat-runtime/internal/messaging"
	"github.com/JFSAS/wechat-runtime/internal/store"
	"github.com/JFSAS/wechat-runtime/pkg/bot"
)

const archiveHighWater = 1024
const archiveKeepActive = 512

type ArchiveReport struct {
	ArchivedBatches  int `json:"archived_batches"`
	ArchivedJobs     int `json:"archived_jobs"`
	ActiveBatches    int `json:"active_batches"`
	ActiveJobs       int `json:"active_jobs"`
	ProtectedBatches int `json:"protected_batches"`
}

func (a BotAdapter) ArchiveStatus() (map[string]any, error) {
	if a.Archive == nil {
		return nil, bot.ErrUnsupported
	}
	s := a.Service
	s.mu.RLock()
	defer s.mu.RUnlock()
	account := ""
	jobs, batches := 0, 0
	if s.mediaScopeLocked() {
		account = s.data.Binding
		jobs, batches = len(s.data.Jobs), len(s.data.DeliveryBatches)
	}
	stats, err := a.Archive.Stats(account)
	if err != nil {
		return nil, err
	}
	return map[string]any{"archive": stats, "active_jobs": jobs, "active_batches": batches, "automatic_high_water": archiveHighWater, "automatic_keep_active": archiveKeepActive, "media_retention": "confirmed archived payload copies eligible after 24h; immutable metadata and unknown outcomes retained"}, nil
}

// ArchiveCompleted performs one bounded pass. Only complete terminal OneBot
// batches move; queued, in-flight, foreign-producer and guarded batches remain.
func (a BotAdapter) ArchiveCompleted(ctx context.Context) (ArchiveReport, error) {
	s := a.Service
	s.mu.Lock()
	defer s.mu.Unlock()
	return a.compactLocked(ctx, 0)
}
func (a BotAdapter) RunArchive(ctx context.Context) {
	if a.Archive == nil {
		return
	}
	timer := time.NewTicker(30 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			s := a.Service
			s.mu.Lock()
			if s.mediaScopeLocked() {
				if _, err := a.compactLocked(ctx, archiveKeepActive); err != nil && ctx.Err() == nil {
					s.category = "delivery_archive_pending"
				}
			}
			s.mu.Unlock()
		}
	}
}

func (a BotAdapter) compactLocked(ctx context.Context, keep int) (ArchiveReport, error) {
	s := a.Service
	var report ArchiveReport
	if a.Archive == nil {
		return report, bot.ErrUnsupported
	}
	if err := ctx.Err(); err != nil {
		return report, err
	}
	if !s.mediaScopeLocked() || len(s.data.Session.Original) != 16 || len(s.data.Session.ClientKey) != 24 || len(s.data.Session.ServerKey) != 24 {
		return report, bot.ErrUnavailable
	}
	report.ActiveJobs, report.ActiveBatches = len(s.data.Jobs), len(s.data.DeliveryBatches)
	owners := map[string]int{}
	for _, batch := range s.data.DeliveryBatches {
		for _, id := range batch.IDs {
			owners[id]++
		}
	}
	var rows []archivedBatch
	alreadyStored := map[string]bool{}
	for key, batch := range s.data.DeliveryBatches {
		if !strings.HasPrefix(key, "bot/") {
			continue
		}
		guard := filepath.Join(s.directory, "bot-intents", batchHash(s.data.Binding, key)+".json")
		if _, err := os.Lstat(guard); err == nil {
			report.ProtectedBatches++
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			return report, ErrDeliveryArchive
		}
		row := archivedBatch{Schema: 1, Account: s.data.Binding, Key: key, Fingerprint: batch.Fingerprint, IDs: append([]string(nil), batch.IDs...), ArchivedAt: time.Now().Unix(), NativeNext: s.data.NextClientID}
		complete := len(batch.IDs) > 0 && len(batch.IDs) <= 20
		for _, id := range batch.IDs {
			job := s.data.Jobs[id]
			if !terminalBotJob(job) || owners[id] != 1 {
				complete = false
				break
			}
			row.Jobs = append(row.Jobs, *job)
		}
		if complete {
			old, err := a.Archive.batch(row.Account, row.Key)
			if err != nil {
				return report, err
			}
			alreadyStored[row.Key] = old != nil
			rows = append(rows, row)
		} else {
			report.ProtectedBatches++
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if alreadyStored[rows[i].Key] != alreadyStored[rows[j].Key] {
			return alreadyStored[rows[i].Key]
		}
		if rows[i].Jobs[0].CreateTime != rows[j].Jobs[0].CreateTime {
			return rows[i].Jobs[0].CreateTime < rows[j].Jobs[0].CreateTime
		}
		return rows[i].Key < rows[j].Key
	})
	selected := rows[:0]
	jobs, batches := report.ActiveJobs, report.ActiveBatches
	for _, row := range rows {
		if (jobs <= keep && batches <= keep && !alreadyStored[row.Key]) || len(selected) == 64 {
			break
		}
		selected = append(selected, row)
		jobs -= len(row.IDs)
		batches--
	}
	if len(selected) == 0 {
		return report, nil
	}
	if err := ctx.Err(); err != nil {
		return report, err
	}
	persist := a.persist
	if persist == nil {
		persist = s.saveLocked
	}
	// Persist nonreplayable terminal states first. The frozen service can reload
	// its checkpoint during reconnect without going through host startup recovery.
	// Every checkpoint reachable after archive commit must therefore already be
	// terminal or pruned, even when the later prune commit fails ambiguously.
	if err := persist(); err != nil {
		return report, ErrDeliveryArchive
	}
	write := a.archiveWrite
	if write == nil {
		write = func(rows []archivedBatch) error { return a.Archive.store(rows, s.data.Session.Bootstrap.Username) }
	}
	if err := write(selected); err != nil {
		return report, ErrDeliveryArchive
	}
	// The archive is durable before any active entry disappears. If checkpoint
	// commit is ambiguous, keep the active in-memory entries and let startup
	// reconciliation use the authoritative archive before workers can replay.
	removedJobs := map[string]*store.Job{}
	removedBatches := map[string]messaging.DeliveryBatch{}
	for _, row := range selected {
		removedBatches[row.Key] = s.data.DeliveryBatches[row.Key]
		delete(s.data.DeliveryBatches, row.Key)
		for _, id := range row.IDs {
			removedJobs[id] = s.data.Jobs[id]
			delete(s.data.Jobs, id)
		}
	}
	if err := persist(); err != nil {
		for key, batch := range removedBatches {
			s.data.DeliveryBatches[key] = batch
		}
		for id, job := range removedJobs {
			s.data.Jobs[id] = job
		}
		return report, ErrDeliveryArchive
	}
	report.ArchivedBatches, report.ArchivedJobs = len(selected), len(removedJobs)
	report.ActiveJobs, report.ActiveBatches = len(s.data.Jobs), len(s.data.DeliveryBatches)
	return report, nil
}

func immutableArchiveJobMatches(active, archived *store.Job) bool {
	if active == nil {
		return true
	}
	if active.Origin != archived.Origin || active.Recipient != archived.Recipient || active.Content != archived.Content || active.Source != archived.Source || active.ClientID != archived.ClientID || active.CreateTime != archived.CreateTime || active.Kind != archived.Kind || (active.Media == nil) != (archived.Media == nil) {
		return false
	}
	if active.ServerID != 0 && active.ServerID != archived.ServerID {
		return false
	}
	if active.Media != nil {
		x, y := active.Media, archived.Media
		return x.Binding == y.Binding && x.Name == y.Name && x.MIME == y.MIME && x.Size == y.Size && x.Digest == y.Digest && x.NativeID == y.NativeID && bytes.Equal(x.AESKey, y.AESKey) && bytes.Equal(x.ThumbnailKey, y.ThumbnailKey)
	}
	return true
}

// Recover removes checkpoint entries already durably archived, including stale
// queued/sending snapshots, before the native channel worker can start.
func (a *DeliveryArchive) Recover(directory string) error {
	stats, err := a.Stats("")
	if err != nil || (stats.ArchivedBatches == 0 && stats.NativeReservations == 0) {
		return err
	}
	cp, exists, err := store.Unique(filepath.Join(directory, "checkpoints"))
	if err != nil || !exists {
		return ErrDeliveryArchive
	}
	data, err := cp.Load()
	if err != nil {
		return ErrDeliveryArchive
	}
	removeJobs, removeBatches := map[string]bool{}, map[string]bool{}
	owners := map[string][]string{}
	for key, batch := range data.DeliveryBatches {
		for _, id := range batch.IDs {
			owners[id] = append(owners[id], key)
		}
		if !strings.HasPrefix(key, "bot/") {
			continue
		}
		row, err := a.batch(data.Binding, key)
		if err != nil {
			return err
		}
		if row == nil {
			continue
		}
		if row.Fingerprint != batch.Fingerprint || !reflect.DeepEqual(row.IDs, batch.IDs) {
			return ErrDeliveryArchive
		}
		removeBatches[key] = true
	}
	for id, job := range data.Jobs {
		row, err := a.indexed(data.Binding, "jobs", id)
		if err != nil {
			return err
		}
		if row == nil {
			continue
		}
		for _, key := range owners[id] {
			if key != row.Key {
				return ErrDeliveryArchive
			}
		}
		matched := false
		for i, archivedID := range row.IDs {
			if archivedID == id {
				matched = immutableArchiveJobMatches(job, &row.Jobs[i])
				break
			}
		}
		if !matched {
			return ErrDeliveryArchive
		}
		removeJobs[id] = true
	}
	if len(removeJobs) == 0 && len(removeBatches) == 0 {
		return nil
	}
	for id := range removeJobs {
		delete(data.Jobs, id)
	}
	for key := range removeBatches {
		delete(data.DeliveryBatches, key)
	}
	if err = cp.Save(data); err != nil {
		return ErrDeliveryArchive
	}
	return nil
}

func (a BotAdapter) archivedDeliveryLocked(row *archivedBatch, id string) (bot.Delivery, error) {
	if row == nil {
		return bot.Delivery{}, bot.ErrNotFound
	}
	jobs := map[string]*store.Job{}
	for i, key := range row.IDs {
		jobs[key] = &row.Jobs[i]
	}
	if len(row.IDs) > 1 && row.IDs[0] == id {
		return a.batchFromJobsLocked(row.IDs, jobs)
	}
	job := jobs[id]
	if job == nil {
		return bot.Delivery{}, ErrDeliveryArchive
	}
	return a.deliveryLocked(id, job), nil
}
