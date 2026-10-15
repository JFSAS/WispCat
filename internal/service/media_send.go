package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/JFSAS/wechat-runtime/internal/cdn"
	"github.com/JFSAS/wechat-runtime/internal/media"
	"github.com/JFSAS/wechat-runtime/internal/protocol"
	"github.com/JFSAS/wechat-runtime/internal/store"
)

const MaxMediaSendBytes = 16 << 20

// MediaSendRequest is private worker input. SaveProgress must succeed before
// proceeding from upload to message submission. It is never an API response.
type MediaSendRequest struct {
	ID           string                          `json:"id"`
	Kind         string                          `json:"kind"`
	Peer         string                          `json:"-"`
	CreatedAt    uint32                          `json:"-"`
	Metadata     store.OutgoingMedia             `json:"-"`
	Data         []byte                          `json:"-"`
	SaveProgress func(store.OutgoingMedia) error `json:"-"`
}

func (r MediaSendRequest) String() string   { return "service.MediaSendRequest(redacted)" }
func (r MediaSendRequest) GoString() string { return r.String() }

type MediaSendResult struct {
	Receipt           protocol.MediaSubmissionReceipt
	SubmissionStarted bool
	FailureStep       string
}
type MediaSender func(context.Context, MediaSendRequest) (MediaSendResult, error)

func (s *Service) SetMediaSender(sender MediaSender) {
	s.mu.Lock()
	s.mediaSend = sender
	s.mu.Unlock()
	s.signal()
}

func safeUploadName(name string) bool {
	if name == "" || name == "." || name == ".." || len(name) > 255 || !utf8.ValidString(name) || strings.ContainsAny(name, "/\\") {
		return false
	}
	for _, r := range name {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf) {
			return false
		}
	}
	return true
}

// QueueMediaSend accepts supplied bytes only. It never opens a caller's path,
// touches credentials, or starts a second login. One persistent queue entry is
// created after validation and sealing the file into the private media cache.
func (s *Service) QueueMediaSend(session, kind, name string, content []byte) (map[string]any, error) {
	if !mediaID(session, 32) || (kind != "image" && kind != "file") || !safeUploadName(name) || len(content) == 0 || len(content) > MaxMediaSendBytes {
		return nil, ErrMediaInvalidRequest
	}
	mime := http.DetectContentType(content)
	if kind == "image" {
		info, err := media.ValidateImage(content)
		if err != nil {
			return nil, ErrMediaInvalidRequest
		}
		mime = info.MIME
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.mediaScopeLocked() || s.state != "connected" || s.mode != "online" || s.data.SessionInvalid {
		return nil, ErrMediaUnavailable
	}
	peer, ok := s.peersLocked()[session]
	if !ok {
		return nil, ErrMediaUnavailable
	}
	if len(s.data.Jobs) >= 10000 {
		return nil, ErrMediaQueueFull
	}
	id, j, err := s.stageMediaSendLocked(peer, kind, name, mime, content)
	if err != nil {
		return nil, err
	}
	s.data.Jobs[id] = j
	if err := s.saveLocked(); err != nil {
		delete(s.data.Jobs, id)
		_ = (store.MediaCache{Directory: filepath.Join(s.directory, "media"), Binding: s.data.Binding}).Remove(id)
		return nil, ErrMediaUnavailable
	}
	s.signal()
	return sendJobView(id, j), nil
}

// Stage encrypted bytes and allocate a message ID, but do not publish a queue
// entry. Batch callers can commit text and image replies together.
func (s *Service) stageMediaSendLocked(peer []byte, kind, name, mime string, content []byte) (string, *store.Job, error) {
	if mediaCachedBytes(s.data)+int64(len(content)) > MaxMediaCacheBytes {
		return "", nil, ErrMediaQueueFull
	}
	hash := sha256.Sum256(content)
	random := make([]byte, 48)
	if _, err := rand.Read(random); err != nil {
		return "", nil, ErrMediaUnavailable
	}
	id := hex.EncodeToString(random[:16])
	cache := store.MediaCache{Directory: filepath.Join(s.directory, "media"), Binding: s.data.Binding}
	if err := cache.Save(id, content); err != nil {
		return "", nil, ErrMediaUnavailable
	}
	clientID := s.data.NextClientID
	if clientID == 0 {
		clientID = 1
	}
	when := uint32(time.Now().Unix())
	// Follow the native peer_time_uint32_xwechat_counter field shape, with our
	// own durable logical counter. This is not the official database local ID.
	nativeID := string(peer) + "_" + strconv.FormatUint(uint64(when), 10) + "_" + strconv.FormatUint(uint64(clientID), 10) + "_xwechat_1"
	m := &store.OutgoingMedia{Binding: s.data.Binding, Name: name, MIME: mime, Size: int64(len(content)), Digest: hex.EncodeToString(hash[:]), NativeID: nativeID, AESKey: append([]byte(nil), random[16:32]...), Stage: "queued"}
	if kind == "image" {
		m.ThumbnailKey = append([]byte(nil), random[32:48]...)
	}
	j := &store.Job{Kind: kind, Media: m, Recipient: string(peer), ClientID: clientID, CreateTime: when, Status: "queued"}
	s.data.NextClientID = clientID%0x7fffffff + 1
	return id, j, nil
}

func mediaCachedBytes(data *store.Data) int64 {
	var size int64
	for _, j := range data.MediaJobs {
		if j != nil && j.Status == "downloaded" {
			size += j.Size
		}
	}
	for _, j := range data.Jobs {
		if j != nil && j.Media != nil {
			size += j.Media.Size
		}
	}
	for _, a := range data.Artifacts {
		size += a.Size
	}
	return size
}

func sendJobView(id string, j *store.Job) map[string]any {
	v := map[string]any{"id": id, "status": j.Status, "error_category": j.ErrorCategory, "server_id_present": j.ServerID != 0}
	if j.Media != nil {
		v["kind"], v["name"], v["size_bytes"], v["stage"] = j.Kind, j.Media.Name, j.Media.Size, j.Media.Stage
		if j.ErrorCode != 0 {
			v["error_code"] = j.ErrorCode
		}
		if j.Media.FailureStep != "" {
			v["failure_step"] = j.Media.FailureStep
		}
	}
	return v
}

func (s *Service) sendMediaQueued(ctx context.Context, id string) error {
	s.mu.Lock()
	j := s.data.Jobs[id]
	if j == nil || j.Media == nil || j.Status != "queued" || s.state != "connected" || s.mode != "online" || !s.mediaScopeLocked() || s.data.SessionInvalid {
		s.mu.Unlock()
		return nil
	}
	if legacyDelivery(j) {
		s.mu.Unlock()
		return nil
	}
	if s.deliveryCancelledLocked(j) {
		j.Status = "cancelled"
		err := s.saveLocked()
		s.mu.Unlock()
		return err
	}
	binding := s.data.Binding
	sender := s.mediaSend
	for _, u := range s.data.Unavailable {
		if string(u) == j.Recipient {
			reason := "contact_unavailable"
			j.Status = "failed"
			j.ErrorCategory = &reason
			err := s.saveLocked()
			s.mu.Unlock()
			return err
		}
	}
	if sender == nil {
		reason := "media_transport_unconfigured"
		j.Status = "failed"
		j.ErrorCategory = &reason
		err := s.saveLocked()
		s.mu.Unlock()
		return err
	}
	j.Status = "uploading"
	j.Media.Stage = "uploading"
	j.Attempts++
	if err := s.saveLocked(); err != nil {
		s.mu.Unlock()
		return err
	}
	copyMeta := *j.Media
	copyMeta.AESKey = append([]byte(nil), j.Media.AESKey...)
	copyMeta.ThumbnailKey = append([]byte(nil), j.Media.ThumbnailKey...)
	request := MediaSendRequest{ID: id, Kind: j.Kind, Peer: j.Recipient, CreatedAt: j.CreateTime, Metadata: copyMeta}
	cache := store.MediaCache{Directory: filepath.Join(s.directory, "media"), Binding: binding}
	s.mu.Unlock()
	data, err := cache.Load(id)
	if err == nil {
		hash := sha256.Sum256(data)
		if int64(len(data)) != copyMeta.Size || hex.EncodeToString(hash[:]) != copyMeta.Digest {
			err = store.ErrMediaCache
		}
	}
	request.Data = data
	request.SaveProgress = func(progress store.OutgoingMedia) error {
		if e := ctx.Err(); e != nil {
			return e
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.mediaScopeLocked() || s.data.Binding != binding || s.data.Jobs[id] != j || s.mode != "online" || s.state != "connected" || s.data.SessionInvalid {
			return ErrMediaUnavailable
		}
		// Immutable identity and local content must never be overwritten by a
		// transport adapter. Only references/digests and the phase are copied.
		switch progress.Stage {
		case "main_uploaded", "uploaded", "submitting":
		default:
			return ErrMediaInvalidRequest
		}
		j.Media.Stage = progress.Stage
		j.Media.FileID = progress.FileID
		j.Media.ThumbnailID = progress.ThumbnailID
		j.Media.MD5 = progress.MD5
		j.Media.UploadSize = progress.UploadSize
		j.Media.ThumbSize = progress.ThumbSize
		j.Media.ThumbWidth = progress.ThumbWidth
		j.Media.ThumbHeight = progress.ThumbHeight
		if progress.Stage == "submitting" {
			j.Status = "submitting"
		}
		return s.saveLocked()
	}
	var result MediaSendResult
	if err == nil {
		s.count("send")
		result, err = sender(ctx, request)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.mediaScopeLocked() || s.data.Binding != binding || s.data.Jobs[id] != j {
		return nil
	}
	if err != nil || !result.Receipt.ServerAccepted || result.Receipt.ServerID == 0 {
		switch result.FailureStep {
		case "cdn_discovery", "cdn_main_upload", "cdn_thumbnail_upload", "message_encode", "message_exchange", "message_receipt", "message_submit":
			j.Media.FailureStep = result.FailureStep
		}
		j.Status = "failed"
		reason := "media_upload_failed"
		if result.SubmissionStarted || j.Media.Stage == "submitting" {
			j.Status = "unknown"
			reason = "send_outcome_unknown"
		}
		var p *protocol.Error
		if errors.As(err, &p) && p.Category == "media_submit_rejected" {
			j.Status = "failed"
			reason = p.Category
			j.ErrorCode = int64(p.Code)
		}
		var c *cdn.Error
		if errors.As(err, &c) && !result.SubmissionStarted {
			reason = "media_upload_rejected"
			j.ErrorCode = c.Code
		}
		if errors.Is(err, store.ErrMediaCache) {
			reason = "media_cache_unavailable"
		}
		j.ErrorCategory = &reason
	} else {
		j.Status = "accepted"
		j.ServerID = result.Receipt.ServerID
		j.Media.Stage = "completed"
		j.ErrorCategory = nil
	}
	s.snapshotLocked()
	// Failed/unknown jobs are retained for review, never replayed automatically.
	// Media errors don't invalidate the otherwise independent sync session.
	return s.saveLocked()
}
