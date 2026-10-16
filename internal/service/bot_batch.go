package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/JFSAS/wechat-runtime/internal/media"
	"github.com/JFSAS/wechat-runtime/internal/messaging"
	"github.com/JFSAS/wechat-runtime/internal/protocol"
	"github.com/JFSAS/wechat-runtime/internal/store"
	"github.com/JFSAS/wechat-runtime/pkg/bot"
)

type preparedOutput struct {
	text, source, mime string
	upload             *bot.Upload
}

func (a BotAdapter) batchLocked(ids []string) (bot.Delivery, error) {
	return a.batchFromJobsLocked(ids, a.Service.data.Jobs)
}
func (a BotAdapter) batchFromJobsLocked(ids []string, jobs map[string]*store.Job) (bot.Delivery, error) {
	if len(ids) == 0 {
		return bot.Delivery{}, bot.ErrNotFound
	}
	parts := make([]bot.Delivery, 0, len(ids))
	state := "accepted"
	priority := map[string]int{"accepted": 0, "echo_observed": 0, "queued": 1, "sending": 1, "uploading": 1, "submitting": 1, "cancelled": 2, "failed": 3, "unknown": 4}
	for _, id := range ids {
		job := jobs[id]
		if job == nil {
			return bot.Delivery{}, bot.ErrNotFound
		}
		part := a.deliveryLocked(id, job)
		if _, known := priority[part.State]; !known {
			part.State = "unknown"
			part.Error = "unrecognized_delivery_state"
		}
		parts = append(parts, part)
		if priority[part.State] > priority[state] {
			state = part.State
		}
	}
	result := parts[0]
	result.State = state
	if len(parts) > 1 {
		result.Parts = parts
	}
	return result, nil
}
func normalizeOutputs(r bot.Send) (bot.Send, []bot.Output, error) {
	if len(r.Outputs) > 0 && (r.Text != "" || len(r.Mentions) > 0 || r.Upload != nil) {
		return r, nil, errors.New("ambiguous_outputs")
	}
	outputs := r.Outputs
	if len(outputs) == 0 {
		outputs = []bot.Output{{Text: r.Text, Mentions: r.Mentions, Upload: r.Upload}}
	}
	if len(outputs) > 20 {
		return r, nil, errors.New("too_many_outputs")
	}
	if len(outputs) == 1 {
		r.Text, r.Mentions, r.Upload = outputs[0].Text, outputs[0].Mentions, outputs[0].Upload
		r.Outputs = nil
	}
	return r, outputs, nil
}

// Submit validates all native message parts, stages encrypted media and commits
// the whole queue batch once. Platform submission remains per native message:
// callers receive every part's state, including partial/unknown outcomes.
func (a BotAdapter) Submit(ctx context.Context, r bot.Send) (bot.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return bot.Delivery{}, err
	}
	var err error
	var outputs []bot.Output
	r, outputs, err = normalizeOutputs(r)
	if err != nil {
		return bot.Delivery{}, err
	}
	if r.Key == "" || len(r.Key) > 160 {
		return bot.Delivery{}, errors.New("invalid_request_key")
	}
	var total int64
	for _, out := range outputs {
		if len(out.Mentions) > 20 || !utf8.ValidString(out.Text) || len(out.Text) > 4096 {
			return bot.Delivery{}, errors.New("invalid_message")
		}
		if out.Upload != nil {
			u := out.Upload
			if out.Text != "" || len(out.Mentions) > 0 || !safeUploadName(u.Name) || len(u.Data) == 0 || (u.Kind != "image" && u.Kind != "file") {
				return bot.Delivery{}, errors.New("invalid_upload")
			}
			total += int64(len(u.Data))
			if total > MaxMediaSendBytes {
				return bot.Delivery{}, errors.New("media_batch_too_large")
			}
		} else if strings.TrimSpace(out.Text) == "" && len(out.Mentions) == 0 {
			return bot.Delivery{}, errors.New("empty_message")
		}
	}
	logical := r
	logical.Epoch = 0
	encoded, _ := json.Marshal(logical)
	digest := sha256.Sum256(encoded)
	fingerprint := hex.EncodeToString(digest[:])
	key := "bot/" + r.Key
	s := a.Service
	s.mu.Lock()
	defer s.mu.Unlock()
	if err = ctx.Err(); err != nil {
		return bot.Delivery{}, err
	}
	if !s.mediaScopeLocked() || r.Account != s.data.Binding {
		return bot.Delivery{}, bot.ErrScope
	}
	if batch, ok := s.data.DeliveryBatches[key]; ok {
		if batch.Fingerprint != fingerprint {
			return bot.Delivery{}, bot.ErrConflict
		}
		return a.batchLocked(batch.IDs)
	}
	if a.Archive != nil {
		row, err := a.Archive.batch(r.Account, key)
		if err != nil {
			return bot.Delivery{}, err
		}
		if row != nil {
			if row.Fingerprint != fingerprint {
				return bot.Delivery{}, bot.ErrConflict
			}
			return a.archivedDeliveryLocked(row, row.IDs[0])
		}
	}
	if s.state != "connected" || s.mode != "online" || s.modeEpoch != r.Epoch || s.data.SessionInvalid {
		return bot.Delivery{}, bot.ErrUnavailable
	}
	peer, ok := s.peersLocked()[r.Conversation]
	if s.history != nil {
		conversation, raw, err := s.history.conversation(r.Account, r.Conversation)
		if err != nil {
			return bot.Delivery{}, err
		}
		if conversation.Unavailable {
			return bot.Delivery{}, bot.ErrNotFound
		}
		peer, ok = raw, true
	}
	if !ok {
		return bot.Delivery{}, bot.ErrNotFound
	}
	prepared := make([]preparedOutput, 0, len(outputs))
	// Complete semantic validation before staging any media or queue entry.
	for _, out := range outputs {
		raw, prefix, e := a.resolveBotMentionsLocked(peer, out.Mentions)
		if e != nil {
			return bot.Delivery{}, e
		}
		source, e := protocol.MentionSource(raw)
		if e != nil {
			return bot.Delivery{}, e
		}
		part := preparedOutput{text: prefix + out.Text, source: source, upload: out.Upload}
		if len(part.text) > 4096 {
			return bot.Delivery{}, errors.New("message_too_long")
		}
		if part.upload != nil {
			part.mime = http.DetectContentType(part.upload.Data)
			if part.upload.Kind == "image" {
				info, e := media.ValidateImage(part.upload.Data)
				if e != nil {
					return bot.Delivery{}, e
				}
				part.mime = info.MIME
			}
		}
		prepared = append(prepared, part)
	}
	if a.Archive != nil && (len(s.data.Jobs) >= archiveHighWater || len(s.data.DeliveryBatches) >= archiveHighWater) {
		if _, err := a.compactLocked(ctx, archiveKeepActive); err != nil {
			return bot.Delivery{}, err
		}
	}
	if len(s.data.Jobs)+len(outputs) > 10000 || len(s.data.DeliveryBatches) >= 10000 {
		return bot.Delivery{}, errors.New("send_journal_full")
	}
	if total > 0 {
		cached, err := a.accountedMediaLocked()
		if err != nil {
			return bot.Delivery{}, err
		}
		if cached+total > MaxMediaCacheBytes && a.Archive != nil {
			// Finish any already-committed pruning before conservatively charging
			// the same retained media in both the active checkpoint and archive.
			if _, err = a.compactLocked(ctx, archiveKeepActive); err != nil {
				return bot.Delivery{}, err
			}
			cached, err = a.accountedMediaLocked()
			if err != nil {
				return bot.Delivery{}, err
			}
		}
		if cached+total > MaxMediaCacheBytes {
			return bot.Delivery{}, ErrMediaQueueFull
		}
	}
	oldNext := s.data.NextClientID
	if a.Archive != nil {
		reserved, err := a.Archive.reserveNative(r.Account, s.data.NextClientID, len(prepared))
		if err != nil {
			return bot.Delivery{}, err
		}
		s.data.NextClientID = reserved
	}
	if s.data.NextClientID == 0 {
		s.data.NextClientID = 1
	}
	if uint64(s.data.NextClientID)+uint64(len(prepared))-1 > 0x7fffffff {
		s.data.NextClientID = 1
	}
	staged := map[string]*store.Job{}
	ids := []string{}
	cache := store.MediaCache{Directory: filepath.Join(s.directory, "media"), Binding: r.Account}
	when := uint32(time.Now().Unix())
	rollback := func() {
		s.data.NextClientID = oldNext
		for id, job := range staged {
			delete(s.data.Jobs, id)
			if job.Media != nil {
				_ = cache.Remove(id)
			}
		}
		delete(s.data.DeliveryBatches, key)
	}
	for _, part := range prepared {
		if err = ctx.Err(); err != nil {
			rollback()
			return bot.Delivery{}, err
		}
		var id string
		var job *store.Job
		if part.upload != nil {
			u := part.upload
			id, job, err = s.stageMediaSendLocked(peer, u.Kind, u.Name, part.mime, u.Data)
			if err != nil {
				rollback()
				return bot.Delivery{}, err
			}
			job.CreateTime = when
			job.Media.NativeID = string(peer) + "_" + strconv.FormatUint(uint64(when), 10) + "_" + strconv.FormatUint(uint64(job.ClientID), 10) + "_xwechat_1"
		} else {
			random, e := protocol.Random(16)
			if e != nil {
				rollback()
				return bot.Delivery{}, e
			}
			id = hex.EncodeToString(random)
			n := s.data.NextClientID
			if n == 0 {
				n = 1
			}
			job = &store.Job{Recipient: string(peer), Content: part.text, Source: part.source, ClientID: n, CreateTime: when, Status: "queued"}
			s.data.NextClientID = n%0x7fffffff + 1
		}
		job.Origin = "onebot"
		staged[id] = job
		ids = append(ids, id)
	}
	if s.data.Jobs == nil {
		s.data.Jobs = map[string]*store.Job{}
	}
	if s.data.DeliveryBatches == nil {
		s.data.DeliveryBatches = map[string]messaging.DeliveryBatch{}
	}
	for id, job := range staged {
		s.data.Jobs[id] = job
	}
	s.data.DeliveryBatches[key] = messaging.DeliveryBatch{Fingerprint: fingerprint, IDs: ids}
	intentPath, intentErr := writeBotIntent(s.directory, r.Account, key, fingerprint, ids)
	if intentErr != nil {
		rollback()
		return bot.Delivery{}, intentErr
	}
	persist := a.persist
	if persist == nil {
		persist = s.saveLocked
	}
	if err = persist(); err != nil {
		if checkpointDefinitelyExcludes(s.checkpoint, key, fingerprint) {
			rollback()
			_ = clearBotIntent(intentPath)
			return bot.Delivery{}, err
		}
		// Rename may have committed before directory fsync failed. Preserve all
		// media and the request key; pause dispatch and retain a startup guard.
		for _, job := range staged {
			job.Status = "unknown"
			reason := "queue_commit_uncertain"
			job.ErrorCategory = &reason
		}
		s.setModeLocked("offline", false)
		s.category = "queue_commit_uncertain"
		s.detail = "投递记录的持久化结果不确定，已暂停账号发送；保留记录等待恢复。"
		if retryErr := persist(); retryErr == nil {
			_ = clearBotIntent(intentPath)
		}
		return a.batchLocked(ids)
	}
	if err = clearBotIntent(intentPath); err != nil {
		s.category = "delivery_intent_cleanup_pending"
	}
	s.signal()
	return a.batchLocked(ids)
}
