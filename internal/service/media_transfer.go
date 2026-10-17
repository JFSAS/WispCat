package service

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/JFSAS/wispcat/internal/cdn"
	"github.com/JFSAS/wispcat/internal/media"
	"github.com/JFSAS/wispcat/internal/store"
)

const MaxMediaJobs = 512
const MaxMediaCacheBytes = 256 << 20

var (
	ErrMediaInvalidRequest        = errors.New("invalid_media_request")
	ErrMediaUnavailable           = errors.New("media_unavailable")
	ErrMediaNotReady              = errors.New("media_not_ready")
	ErrMediaQueueFull             = errors.New("media_queue_full")
	ErrMediaTransportUnconfigured = errors.New("media_transport_unconfigured")
)

// MediaDownloadRequest is private transport input. Peer and download references
// are available only by explicit access; ordinary JSON/logging is redacted.
type MediaDownloadRequest struct {
	ID          string           `json:"id"`
	Variant     media.Variant    `json:"variant"`
	Description media.Descriptor `json:"description"`
	Peer        []byte           `json:"-"`
	MaxBytes    int64            `json:"max_bytes"`
}

func (r MediaDownloadRequest) String() string   { return "service.MediaDownloadRequest(redacted)" }
func (r MediaDownloadRequest) GoString() string { return r.String() }

// MediaDownloader must return a decoded, sequence/status/size-validated result.
// It is invoked only by downloadQueued in the existing serial network worker.
type MediaDownloader func(context.Context, MediaDownloadRequest) (*cdn.Result, error)

func (s *Service) SetMediaDownloader(downloader MediaDownloader) {
	s.mu.Lock()
	s.mediaDownload = downloader
	s.mu.Unlock()
	s.signal()
}

type MediaContentData struct {
	Data []byte `json:"-"`
	MIME string `json:"mime"`
	Name string `json:"name"`
	Kind string `json:"kind"`
}

func (c MediaContentData) String() string   { return "service.MediaContentData(redacted)" }
func (c MediaContentData) GoString() string { return c.String() }
func (c MediaContentData) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		MIME string `json:"mime"`
		Name string `json:"name"`
		Kind string `json:"kind"`
		Size int    `json:"size_bytes"`
	}{c.MIME, c.Name, c.Kind, len(c.Data)})
}

func mediaID(value string, size int) bool {
	return len(value) == size && strings.Trim(value, "0123456789abcdef") == ""
}

func mediaVariant(kind media.Kind, value media.Variant) bool {
	if kind == media.File {
		return value == media.Attachment
	}
	return kind == media.Image && (value == media.Regular || value == media.Thumbnail || value == media.Original)
}

// QueueMediaDownload uses only opaque local IDs. Repeated requests for an
// existing message/variant return the same task. An explicit request can retry
// a terminal failure; an in-flight or successful task is never duplicated.
func (s *Service) QueueMediaDownload(session, message, variant string) (map[string]any, error) {
	if !mediaID(session, 32) || !mediaID(message, 64) {
		return nil, ErrMediaInvalidRequest
	}
	s.mu.Lock()
	previousCount := 0
	if s.data != nil {
		previousCount = len(s.data.MediaJobs)
	}
	result, err := s.queueMediaDownloadLocked(session, message, media.Variant(variant))
	var previousJob *store.MediaJob
	if err == nil {
		job := s.data.MediaJobs[result["id"].(string)]
		if job.Status == "failed" {
			copyJob := *job
			previousJob = &copyJob
			job.Status, job.Error, job.ErrorCode = "queued", "", 0
			job.ExpectedCipherBytes, job.ReceivedCipherBytes = 0, 0
			result = mediaJobView(result["id"].(string), job)
		}
	}
	if err == nil && (len(s.data.MediaJobs) > previousCount || previousJob != nil) {
		if saveErr := s.saveLocked(); saveErr != nil {
			if previousJob != nil {
				*s.data.MediaJobs[result["id"].(string)] = *previousJob
			} else {
				delete(s.data.MediaJobs, result["id"].(string))
			}
			result, err = nil, ErrMediaUnavailable
		}
	}
	s.mu.Unlock()
	if err == nil {
		s.signal()
	}
	return result, err
}

func (s *Service) queueMediaDownloadLocked(session, message string, variant media.Variant) (map[string]any, error) {
	if !s.mediaScopeLocked() {
		return nil, ErrMediaUnavailable
	}
	description, _, err := s.mediaDescriptionLocked(session, message)
	if err != nil {
		return nil, err
	}
	if !mediaVariant(description.Public().Kind, variant) || !description.DownloadReference().Has(variant) {
		return nil, ErrMediaInvalidRequest
	}
	if size, ok := description.DownloadReference().SizeBytes(variant); ok && size > uint64(mediaLimit(description.Public().Kind)) {
		return nil, ErrMediaInvalidRequest
	}
	for id, job := range s.data.MediaJobs {
		if job != nil && job.Binding == s.data.Binding && job.SessionID == session && job.MessageID == message && job.Variant == string(variant) {
			return mediaJobView(id, job), nil
		}
	}
	if len(s.data.MediaJobs) >= MaxMediaJobs {
		return nil, ErrMediaQueueFull
	}
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil {
		return nil, ErrMediaUnavailable
	}
	id := hex.EncodeToString(idBytes)
	if s.data.MediaJobs == nil {
		s.data.MediaJobs = map[string]*store.MediaJob{}
	}
	metadata := description.Public()
	name := metadata.Name
	if name == "" {
		name = "image"
	}
	job := &store.MediaJob{Binding: s.data.Binding, SessionID: session, MessageID: message, Variant: string(variant), Kind: string(metadata.Kind), Name: name, Status: "queued", CreatedAt: time.Now().UnixNano()}
	s.data.MediaJobs[id] = job
	return mediaJobView(id, job), nil
}

func (s *Service) mediaDescriptionLocked(session, message string) (media.Descriptor, []byte, error) {
	if !s.mediaScopeLocked() {
		return media.Descriptor{}, nil, ErrMediaUnavailable
	}
	if s.history != nil {
		if description, peer, found, err := s.history.mediaDescriptionLocked(s, session, message); found || err != nil {
			return description, peer, err
		}
	}
	peer, ok := s.peersLocked()[session]
	if !ok {
		return media.Descriptor{}, nil, ErrMediaUnavailable
	}
	owner := s.data.Session.Bootstrap.Username
	for _, entry := range s.data.Messages {
		if messageKey(entry, owner) != message || !bytes.Equal(entry.Peer(owner), peer) {
			continue
		}
		description, err := media.Parse(entry.Type, normalizedMessage(entry, owner).Content)
		if err != nil {
			return media.Descriptor{}, nil, ErrMediaUnavailable
		}
		return description, bytes.Clone(peer), nil
	}
	return media.Descriptor{}, nil, ErrMediaUnavailable
}

func mediaJobView(id string, job *store.MediaJob) map[string]any {
	view := map[string]any{"id": id, "session_id": job.SessionID, "message_id": job.MessageID, "variant": job.Variant, "kind": job.Kind, "name": job.Name, "status": job.Status, "attempts": job.Attempts}
	if job.Status == "downloaded" {
		view["mime"], view["size_bytes"], view["checksum_verified"] = job.MIME, job.Size, job.ChecksumVerified
	}
	if job.Error != "" {
		category, code := safeMediaCategory(job.Error, job.ErrorCode)
		view["error_category"], view["error_code"] = category, code
		if category == "cdn_cipher_size_mismatch" && job.ExpectedCipherBytes <= media.MaxImageBytes+16 && job.ReceivedCipherBytes <= media.MaxImageBytes+16 {
			view["expected_cipher_bytes"], view["received_cipher_bytes"] = job.ExpectedCipherBytes, job.ReceivedCipherBytes
		}
	}
	return view
}

func (s *Service) MediaJobs() []map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := []map[string]any{}
	if !s.mediaScopeLocked() {
		return result
	}
	ids := make([]string, 0, len(s.data.MediaJobs))
	for id, job := range s.data.MediaJobs {
		if job != nil && job.Binding == s.data.Binding {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		left, right := s.data.MediaJobs[ids[i]], s.data.MediaJobs[ids[j]]
		if left.CreatedAt == right.CreatedAt {
			return ids[i] < ids[j]
		}
		return left.CreatedAt < right.CreatedAt
	})
	for _, id := range ids {
		result = append(result, mediaJobView(id, s.data.MediaJobs[id]))
	}
	return result
}

func mediaLimit(kind media.Kind) int64 {
	if kind == media.Image {
		return media.MaxImageBytes
	}
	return store.MaxMediaBytes
}

// downloadQueued is a serial worker hook, not an HTTP handler. A nil adapter is
// an explicit unconfigured transport; it does not fabricate downloaded bytes.
func (s *Service) downloadQueued(ctx context.Context) error {
	s.mu.Lock()
	if s.mode != "online" || s.state != "connected" || !s.mediaScopeLocked() || s.data.SessionInvalid || s.mediaActive != "" {
		s.mu.Unlock()
		return nil
	}
	id := ""
	for candidate, job := range s.data.MediaJobs {
		if job == nil || job.Binding != s.data.Binding || job.Status != "queued" {
			continue
		}
		if id == "" || job.CreatedAt < s.data.MediaJobs[id].CreatedAt || job.CreatedAt == s.data.MediaJobs[id].CreatedAt && candidate < id {
			id = candidate
		}
	}
	if id == "" {
		s.mu.Unlock()
		return nil
	}
	job := s.data.MediaJobs[id]
	description, peer, err := s.mediaDescriptionLocked(job.SessionID, job.MessageID)
	if err != nil || !mediaVariant(description.Public().Kind, media.Variant(job.Variant)) || !description.DownloadReference().Has(media.Variant(job.Variant)) {
		job.Status, job.Error = "failed", "media_unavailable"
		err = s.saveLocked()
		s.mu.Unlock()
		return err
	}
	binding := s.data.Binding
	request := MediaDownloadRequest{ID: id, Variant: media.Variant(job.Variant), Description: description, Peer: peer, MaxBytes: mediaLimit(description.Public().Kind)}
	downloader := s.mediaDownload
	job.Status, job.Error, job.ErrorCode = "downloading", "", 0
	job.Attempts++
	s.mediaActive = id
	if err := s.saveLocked(); err != nil {
		job.Status = "queued"
		s.mediaActive = ""
		s.mu.Unlock()
		return err
	}
	s.calls["media_download"]++
	s.mu.Unlock()
	var decoded *cdn.Result
	if downloader == nil {
		err = ErrMediaTransportUnconfigured
	} else {
		decoded, err = downloader(ctx, request)
	}
	if err == nil {
		err = validateMediaResult(request, decoded)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mediaActive = ""
	if s.data == nil || s.data.Binding != binding || s.data.MediaJobs[id] != job {
		return nil
	}
	s.snapshotLocked()
	if s.mode != "online" {
		job.Status, job.Error, job.ErrorCode = "queued", "media_paused", 0
		return s.saveLocked()
	}
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	if err != nil {
		job.Status = "failed"
		job.Error, job.ErrorCode = mediaFailure(err)
		var sizeError *cdn.Error
		if errors.As(err, &sizeError) && sizeError.Category == "cipher_size_mismatch" {
			job.ExpectedCipherBytes, job.ReceivedCipherBytes = sizeError.ExpectedBytes, sizeError.ReceivedBytes
		}
		return s.saveLocked()
	}
	cached := mediaCachedBytes(s.data)
	if cached+int64(len(decoded.Data)) > MaxMediaCacheBytes {
		job.Status, job.Error = "failed", "media_cache_full"
		return s.saveLocked()
	}
	cache := store.MediaCache{Directory: filepath.Join(s.directory, "media"), Binding: binding}
	if err := cache.Save(id, decoded.Data); err != nil {
		job.Status, job.Error = "failed", "media_cache_unavailable"
		return s.saveLocked()
	}
	digest := sha256.Sum256(decoded.Data)
	job.MIME = mediaMIME(decoded.Data)
	if job.Kind == "image" {
		if job.MIME != "image/png" && job.MIME != "image/jpeg" && job.MIME != "image/gif" {
			job.MIME = "application/octet-stream"
		}
		job.Name = mediaImageName(job.MIME)
	}
	job.Size, job.Digest, job.ChecksumVerified = int64(len(decoded.Data)), hex.EncodeToString(digest[:]), decoded.ChecksumVerified
	job.Status = "downloaded"
	if err := s.saveLocked(); err != nil {
		_ = cache.Remove(id)
		job.Status, job.Error = "failed", "media_cache_unavailable"
		return err
	}
	return nil
}

func validateMediaResult(request MediaDownloadRequest, result *cdn.Result) error {
	if result == nil || result.RawSize != uint64(len(result.Data)) || int64(len(result.Data)) > request.MaxBytes {
		return errors.New("media_decode_unverified")
	}
	if request.Description.Public().Kind == media.Image && len(result.Data) == 0 {
		return errors.New("media_decode_unverified")
	}
	if request.Variant == media.Regular || request.Variant == media.Original || request.Variant == media.Attachment {
		expected := request.Description.DownloadReference().MD5()
		if request.Variant == media.Original {
			expected = request.Description.DownloadReference().OriginMD5()
		}
		if expected != "" {
			actual := md5.Sum(result.Data)
			if !result.ChecksumVerified || hex.EncodeToString(actual[:]) != expected {
				return errors.New("media_checksum_mismatch")
			}
		}
	}
	return nil
}

func mediaMIME(data []byte) string {
	typeValue := http.DetectContentType(data)
	if semicolon := strings.IndexByte(typeValue, ';'); semicolon >= 0 {
		typeValue = typeValue[:semicolon]
	}
	return typeValue
}
func mediaImageName(mime string) string {
	switch mime {
	case "image/png":
		return "image.png"
	case "image/jpeg":
		return "image.jpg"
	case "image/gif":
		return "image.gif"
	default:
		return "image"
	}
}

func safeMediaCategory(category string, code int64) (string, int64) {
	switch category {
	case "media_transport_unconfigured", "media_unavailable", "media_paused", "media_interrupted", "media_timeout", "media_decode_unverified", "media_checksum_mismatch", "media_cache_unavailable", "media_cache_full", "media_download_failed":
		return category, code
	case "server_status", "server_error", "network_error", "invalid_frame", "invalid_response", "invalid_ciphertext", "checksum_mismatch", "decrypt_error", "invalid_profile", "invalid_request", "deadline_exceeded", "cancelled", "unsupported_meta_encryption", "missing_status", "sequence_mismatch", "missing_filedata", "size_limit", "invalid_raw_size", "cipher_size_mismatch", "raw_size_mismatch", "missing_checksum", "invalid_number", "invalid_padding", "invalid_key", "public_key_unavailable", "key_wrap_failed":
		return "cdn_" + category, code
	case "cdn_server_status", "cdn_server_error", "cdn_network_error", "cdn_invalid_frame", "cdn_invalid_response", "cdn_invalid_ciphertext", "cdn_checksum_mismatch", "cdn_decrypt_error", "cdn_invalid_profile", "cdn_invalid_request", "cdn_deadline_exceeded", "cdn_cancelled", "cdn_unsupported_meta_encryption", "cdn_missing_status", "cdn_sequence_mismatch", "cdn_missing_filedata", "cdn_size_limit", "cdn_invalid_raw_size", "cdn_cipher_size_mismatch", "cdn_raw_size_mismatch", "cdn_missing_checksum", "cdn_invalid_number", "cdn_invalid_padding", "cdn_invalid_key", "cdn_public_key_unavailable", "cdn_key_wrap_failed":
		return category, code
	default:
		return "media_download_failed", 0
	}
}
func mediaFailure(err error) (string, int64) {
	if errors.Is(err, context.DeadlineExceeded) {
		return "media_timeout", 0
	}
	if errors.Is(err, context.Canceled) {
		return "media_interrupted", 0
	}
	var cdnError *cdn.Error
	if errors.As(err, &cdnError) {
		return safeMediaCategory(cdnError.Category, cdnError.Code)
	}
	return safeMediaCategory(err.Error(), 0)
}

func (s *Service) MediaContent(id string) (MediaContentData, error) {
	if !mediaID(id, 32) {
		return MediaContentData{}, ErrMediaInvalidRequest
	}
	s.mu.RLock()
	if !s.mediaScopeLocked() {
		s.mu.RUnlock()
		return MediaContentData{}, ErrMediaUnavailable
	}
	job, ok := s.data.MediaJobs[id]
	if !ok || job == nil || job.Binding != s.data.Binding {
		s.mu.RUnlock()
		return MediaContentData{}, ErrMediaUnavailable
	}
	if job.Status != "downloaded" {
		s.mu.RUnlock()
		return MediaContentData{}, ErrMediaNotReady
	}
	metadata := *job
	binding := s.data.Binding
	s.mu.RUnlock()
	data, err := (store.MediaCache{Directory: filepath.Join(s.directory, "media"), Binding: binding}).Load(id)
	if err != nil {
		return MediaContentData{}, ErrMediaUnavailable
	}
	digest := sha256.Sum256(data)
	if int64(len(data)) != metadata.Size || hex.EncodeToString(digest[:]) != metadata.Digest {
		return MediaContentData{}, ErrMediaUnavailable
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.mediaScopeLocked() || s.data.Binding != binding || s.data.MediaJobs[id] != job || job.Status != "downloaded" {
		return MediaContentData{}, ErrMediaUnavailable
	}
	return MediaContentData{Data: data, MIME: metadata.MIME, Name: metadata.Name, Kind: metadata.Kind}, nil
}

func (s *Service) mediaScopeLocked() bool {
	return s.data != nil && s.state != "scope_changed" && s.data.ValidBinding()
}
