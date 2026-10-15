package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/JFSAS/wechat-runtime/internal/media"
	"github.com/JFSAS/wechat-runtime/internal/messaging"
	"github.com/JFSAS/wechat-runtime/internal/protocol"
	"github.com/JFSAS/wechat-runtime/internal/store"
)

func (s *Service) Snapshot() messaging.Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := messaging.Snapshot{Epoch: s.modeEpoch, Online: s.state == "connected" && s.mode == "online"}
	if s.mediaScopeLocked() {
		snap.Account = s.data.Binding
		if s.mode == "online" && !s.data.SessionInvalid {
			switch s.state {
			case "reconnecting", "network_unavailable", "recovering":
				snap.Recovering = true
			}
		}
		snap.Cursor = s.persistedSequence
		if len(s.data.Events) > 0 {
			snap.FirstCursor = s.data.Events[0].Sequence - 1
		}
	}
	return snap
}

func (s *Service) appendEventLocked(key string, m protocol.Message) {
	owner := s.data.Session.Bootstrap.Username
	peer := m.Peer(owner)
	if len(peer) == 0 {
		return
	}
	normalized := normalizedMessage(m, owner)
	event := messaging.Event{ID: key, Message: key, Account: s.data.Binding, Conversation: s.sessionID(peer), Kind: "private", Sender: s.sessionID(m.Sender), Timestamp: m.Timestamp, Own: isOwnMessage(m, owner), Addressed: bytes.Equal(m.Recipient, owner), LocalEcho: s.localEchoLocked(m, peer)}
	if string(peer) == "filehelper" {
		event.Kind = "filehelper"
		event.Addressed = bytes.Equal(m.Sender, owner) || bytes.Equal(m.Recipient, owner)
	}
	if group, ok := m.Group(owner); ok {
		event.Kind = "group"
		event.Sender = ""
		if len(group.Member) > 0 {
			event.Sender = s.groupMemberID(group.Room, group.Member)
			event.SenderLabel = s.groupMemberLabelLocked(group.Room, group.Member)
		}
		for _, id := range group.Mentions {
			if id == string(owner) {
				event.Mentioned = true
			}
		}
	}
	switch m.Type {
	case 1:
		if !utf8.Valid(normalized.Content) || strings.TrimSpace(string(normalized.Content)) == "" {
			return
		}
		event.Text = string(normalized.Content)
	case 3, 49:
		if quoted, err := media.QuotedReplyText(normalized.Content); m.Type == 49 && err == nil {
			event.Text = "[引用回复] " + quoted
			break
		}
		description, err := media.Parse(m.Type, normalized.Content)
		if err != nil {
			return
		}
		metadata := description.Public()
		if metadata.Kind == media.Image {
			event.Text = "[图片]"
			event.MediaVariant = string(metadata.PreferredVariant)
		} else {
			event.Text = "[文件] " + metadata.Name
			event.MediaVariant = string(media.Attachment)
		}
	default:
		return
	}
	s.data.EventSequence++
	event.Sequence = s.data.EventSequence
	s.data.Events = append(s.data.Events, event)
	if len(s.data.Events) > 10000 {
		s.data.Events = s.data.Events[len(s.data.Events)-10000:]
	}
}

func (s *Service) Events(account string, after uint64, limit int) ([]messaging.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.mediaScopeLocked() || account != s.data.Binding {
		return nil, messaging.ErrScopeChanged
	}
	if after > s.persistedSequence {
		return nil, messaging.ErrCursorExpired
	}
	if len(s.data.Events) > 0 && after < s.data.Events[0].Sequence-1 {
		return nil, messaging.ErrCursorExpired
	}
	if limit < 1 || limit > 1000 {
		limit = 100
	}
	result := []messaging.Event{}
	for _, event := range s.data.Events {
		if event.Sequence > after && event.Sequence <= s.persistedSequence {
			result = append(result, event)
			if len(result) == limit {
				break
			}
		}
	}
	return result, nil
}

func (s *Service) Conversations() []messaging.Conversation {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := []messaging.Conversation{}
	if !s.mediaScopeLocked() {
		return result
	}
	for id, peer := range s.peersLocked() {
		item := messaging.Conversation{ID: id, Label: s.labelLocked(peer), Kind: "private"}
		if protocol.IsGroup(peer) {
			item.Kind = "group"
		} else if string(peer) == "filehelper" {
			item.Kind = "filehelper"
		}
		for _, c := range s.data.Contacts {
			if bytes.Equal(c.Username, peer) && c.Nickname != nil {
				item.Nickname = *c.Nickname
			}
		}
		result = append(result, item)
	}
	return result
}

func (s *Service) ReadState(account, consumer string) (json.RawMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.mediaScopeLocked() || account != s.data.Binding {
		return nil, messaging.ErrScopeChanged
	}
	if consumer == "legacy-codex" {
		return bytes.Clone(s.data.LegacyCodex), nil
	}
	return bytes.Clone(s.data.Consumers[consumer]), nil
}
func (s *Service) WriteState(account, consumer string, state json.RawMessage) error {
	if consumer == "" || len(consumer) > 64 || consumer == "legacy-codex" || len(state) > 16<<20 || !json.Valid(state) {
		return errors.New("invalid_consumer_state")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.mediaScopeLocked() || account != s.data.Binding {
		return messaging.ErrScopeChanged
	}
	if s.data.Consumers == nil {
		s.data.Consumers = map[string]json.RawMessage{}
	}
	old := s.data.Consumers[consumer]
	s.data.Consumers[consumer] = bytes.Clone(state)
	if err := s.saveLocked(); err != nil {
		s.data.Consumers[consumer] = old
		return err
	}
	return nil
}
func (s *Service) ResolveLegacyConversation(account, peer string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.mediaScopeLocked() || account != s.data.Binding {
		return "", messaging.ErrScopeChanged
	}
	return s.sessionID([]byte(peer)), nil
}
func (s *Service) Download(account, conversation, message, variant string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.mediaScopeLocked() || s.data.Binding != account {
		return "", messaging.ErrScopeChanged
	}
	result, err := s.queueMediaDownloadLocked(conversation, message, media.Variant(variant))
	if err != nil {
		return "", err
	}
	if err = s.saveLocked(); err != nil {
		return "", err
	}
	s.signal()
	return result["id"].(string), nil
}
func (s *Service) DownloadStatus(account, id string) (string, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.mediaScopeLocked() || s.data.Binding != account {
		return "failed", "account_scope_changed"
	}
	job := s.data.MediaJobs[id]
	if job == nil {
		return "failed", "media_unavailable"
	}
	return job.Status, job.Error
}
func (s *Service) Content(account, id string) (messaging.Content, error) {
	if s.Snapshot().Account != account {
		return messaging.Content{}, messaging.ErrScopeChanged
	}
	content, err := s.MediaContent(id)
	if s.Snapshot().Account != account {
		return messaging.Content{}, messaging.ErrScopeChanged
	}
	return messaging.Content{Data: content.Data, MIME: content.MIME, Name: content.Name, Kind: content.Kind}, err
}
func (s *Service) Deliveries() []map[string]any { return s.Jobs() }

func (s *Service) StoreArtifacts(account, key string, contents []messaging.Content) ([]messaging.Artifact, error) {
	if key == "" || len(contents) > 4 {
		return nil, errors.New("generated_image_invalid")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.mediaScopeLocked() || account != s.data.Binding {
		return nil, messaging.ErrScopeChanged
	}
	if s.data.Artifacts == nil {
		s.data.Artifacts = map[string]messaging.Artifact{}
	}
	cache := store.MediaCache{Directory: filepath.Join(s.directory, "media"), Binding: account}
	var refs []messaging.Artifact
	total := int64(0)
	extra := int64(0)
	for i, c := range contents {
		total += int64(len(c.Data))
		info, err := media.ValidateImage(c.Data)
		if err != nil || info.MIME != c.MIME || total > 16<<20 {
			return nil, errors.New("generated_image_invalid")
		}
		hash := sha256.Sum256([]byte(key + fmt.Sprint("/", i)))
		id := hex.EncodeToString(hash[:16])
		digest := sha256.Sum256(c.Data)
		ext := map[string]string{"image/png": "png", "image/jpeg": "jpg", "image/gif": "gif"}[c.MIME]
		ref := messaging.Artifact{CacheID: id, Name: fmt.Sprintf("reply-image-%d.%s", i+1, ext), MIME: c.MIME, Size: int64(len(c.Data)), Digest: hex.EncodeToString(digest[:])}
		if previous, ok := s.data.Artifacts[id]; ok {
			if previous.Digest != ref.Digest {
				return nil, messaging.ErrConflict
			}
		} else {
			extra += ref.Size
		}
		refs = append(refs, ref)
	}
	if mediaCachedBytes(s.data)+extra > MaxMediaCacheBytes {
		return nil, ErrMediaQueueFull
	}
	var staged []string
	rollback := func() {
		for _, id := range staged {
			delete(s.data.Artifacts, id)
			_ = cache.Remove(id)
		}
	}
	for i, ref := range refs {
		if _, ok := s.data.Artifacts[ref.CacheID]; ok {
			continue
		}
		if err := cache.Save(ref.CacheID, contents[i].Data); err != nil {
			rollback()
			return nil, err
		}
		s.data.Artifacts[ref.CacheID] = ref
		staged = append(staged, ref.CacheID)
	}
	if err := s.saveLocked(); err != nil {
		rollback()
		return nil, err
	}
	return refs, nil
}
func (s *Service) ReleaseArtifacts(account string, refs []messaging.Artifact) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.mediaScopeLocked() || account != s.data.Binding {
		return messaging.ErrScopeChanged
	}
	saved := map[string]messaging.Artifact{}
	for _, ref := range refs {
		if a, ok := s.data.Artifacts[ref.CacheID]; ok {
			saved[ref.CacheID] = a
			delete(s.data.Artifacts, ref.CacheID)
		}
	}
	if err := s.saveLocked(); err != nil {
		for id, a := range saved {
			s.data.Artifacts[id] = a
		}
		return err
	}
	cache := store.MediaCache{Directory: filepath.Join(s.directory, "media"), Binding: account}
	for id := range saved {
		if err := cache.Remove(id); err != nil {
			return err
		}
	}
	return nil
}

func dispatchKey(scope, conversation string) string { return scope + "/" + conversation }
func (s *Service) deliveryCancelledLocked(job *store.Job) bool {
	return job.DispatchScope != "" && job.DispatchGeneration < s.data.DispatchGenerations[dispatchKey(job.DispatchScope, s.sessionID([]byte(job.Recipient)))]
}
func (s *Service) CancelScope(account, scope, conversation string, generation uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.mediaScopeLocked() || account != s.data.Binding {
		return 0, messaging.ErrScopeChanged
	}
	if scope == "" || conversation == "" {
		return 0, errors.New("invalid_dispatch_scope")
	}
	if s.data.DispatchGenerations == nil {
		s.data.DispatchGenerations = map[string]uint64{}
	}
	key := dispatchKey(scope, conversation)
	old := s.data.DispatchGenerations[key]
	if generation < old {
		generation = old
	}
	s.data.DispatchGenerations[key] = generation
	changed := map[string]string{}
	for id, j := range s.data.Jobs {
		if j.DispatchScope == scope && s.sessionID([]byte(j.Recipient)) == conversation && j.DispatchGeneration < generation && j.Status == "queued" {
			changed[id] = j.Status
			j.Status = "cancelled"
		}
	}
	if err := s.saveLocked(); err != nil {
		s.data.DispatchGenerations[key] = old
		for id, status := range changed {
			s.data.Jobs[id].Status = status
		}
		return 0, err
	}
	return generation, nil
}

func (s *Service) Send(ctx context.Context, req messaging.SendRequest) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req.Key == "" || len(req.Key) > 200 || strings.Trim(req.Scope, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_") != "" || len(req.Scope) > 64 || len(req.Outputs) == 0 || len(req.Outputs) > 20 {
		return nil, errors.New("invalid_delivery_request")
	}
	// Epoch is a transient connection fence, not part of the logical request identity.
	identity := req
	identity.Epoch = 0
	encoded, err := json.Marshal(identity)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(encoded)
	fingerprint := hex.EncodeToString(hash[:])
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.mediaScopeLocked() || s.data.Binding != req.Account {
		return nil, messaging.ErrScopeChanged
	}
	if batch, ok := s.data.DeliveryBatches[req.Key]; ok {
		if batch.Fingerprint != fingerprint {
			return nil, messaging.ErrConflict
		}
		return append([]string(nil), batch.IDs...), nil
	}
	if s.state != "connected" || s.mode != "online" || s.modeEpoch != req.Epoch {
		return nil, messaging.ErrUnavailable
	}
	if req.Scope != "" && req.Generation < s.data.DispatchGenerations[dispatchKey(req.Scope, req.Conversation)] {
		return nil, messaging.ErrCancelled
	}
	peer, ok := s.peersLocked()[req.Conversation]
	if !ok {
		return nil, errors.New("contact_unavailable")
	}
	if len(s.data.Jobs)+len(req.Outputs) > 10000 || len(s.data.DeliveryBatches) >= 10000 {
		return nil, errors.New("send_journal_full")
	}
	cache := store.MediaCache{Directory: filepath.Join(s.directory, "media"), Binding: req.Account}
	var content [][]byte
	var extra int64
	for _, out := range req.Outputs {
		if out.Artifact != nil {
			if out.Text != "" {
				return nil, errors.New("invalid_delivery_output")
			}
			a := out.Artifact
			data, e := cache.Load(a.CacheID)
			if e != nil {
				return nil, errors.New("generated_image_cache_failed")
			}
			digest := sha256.Sum256(data)
			info, e := media.ValidateImage(data)
			if e != nil || int64(len(data)) != a.Size || hex.EncodeToString(digest[:]) != a.Digest || info.MIME != a.MIME || !safeUploadName(a.Name) {
				return nil, errors.New("generated_image_invalid")
			}
			extra += a.Size
			content = append(content, data)
		} else {
			if !utf8.ValidString(out.Text) || strings.TrimSpace(out.Text) == "" || len(out.Text) > 4096 {
				return nil, errors.New("invalid_text")
			}
			content = append(content, nil)
		}
	}
	if mediaCachedBytes(s.data)+extra > MaxMediaCacheBytes {
		return nil, ErrMediaQueueFull
	}
	staged := map[string]*store.Job{}
	ids := []string{}
	oldNext := s.data.NextClientID
	rollback := func() {
		s.data.NextClientID = oldNext
		for id, j := range staged {
			delete(s.data.Jobs, id)
			if j.Media != nil {
				_ = cache.Remove(id)
			}
		}
		delete(s.data.DeliveryBatches, req.Key)
	}
	for i, out := range req.Outputs {
		var id string
		var job *store.Job
		if out.Artifact != nil {
			id, job, err = s.stageMediaSendLocked(peer, "image", out.Artifact.Name, out.Artifact.MIME, content[i])
			if err != nil {
				rollback()
				return nil, err
			}
		} else {
			random, e := protocol.Random(16)
			if e != nil {
				rollback()
				return nil, e
			}
			id = hex.EncodeToString(random)
			clientID := s.data.NextClientID
			if clientID == 0 {
				clientID = 1
			}
			s.data.NextClientID = clientID%0x7fffffff + 1
			job = &store.Job{Recipient: string(peer), Content: out.Text, ClientID: clientID, CreateTime: uint32(time.Now().Unix()), Status: "queued"}
		}
		job.Origin = req.Source
		job.DispatchScope = req.Scope
		job.DispatchGeneration = req.Generation
		staged[id] = job
		ids = append(ids, id)
	}
	for id, job := range staged {
		s.data.Jobs[id] = job
	}
	if s.data.DeliveryBatches == nil {
		s.data.DeliveryBatches = map[string]messaging.DeliveryBatch{}
	}
	s.data.DeliveryBatches[req.Key] = messaging.DeliveryBatch{Fingerprint: fingerprint, IDs: ids}
	if err = s.saveLocked(); err != nil {
		rollback()
		return nil, err
	}
	s.signal()
	return append([]string(nil), ids...), nil
}

func (s *Service) localEchoLocked(m protocol.Message, peer []byte) bool {
	owner := s.data.Session.Bootstrap.Username
	if !isOwnMessage(m, owner) && !bytes.Equal(m.Sender, []byte("filehelper")) {
		return false
	}
	mediaKind, mediaDigest := "", ""
	if m.Type == 3 || m.Type == 49 {
		if description, err := media.Parse(m.Type, normalizedMessage(m, owner).Content); err == nil {
			mediaKind, mediaDigest = string(description.Public().Kind), description.DownloadReference().MD5()
		}
	}
	for _, job := range s.data.Jobs {
		if job == nil || job.Recipient != string(peer) {
			continue
		}
		if m.ServerID != 0 && job.ServerID != 0 {
			if m.ServerID == job.ServerID {
				return true
			}
			continue
		}
		// A lost send ACK has no server ID. Conservatively suppress an echo
		// matching our own journal's text and send time instead of looping.
		delta := int64(m.Timestamp) - int64(job.CreateTime)
		if job.Media != nil && mediaKind != "" && job.Kind == mediaKind && mediaDigest != "" && strings.EqualFold(job.Media.MD5, mediaDigest) && delta >= -300 && delta <= 300 {
			switch job.Status {
			case "uploading", "submitting", "accepted", "unknown", "echo_observed":
				return true
			}
		}
		if job.Content == string(normalizedMessage(m, owner).Content) && delta >= -300 && delta <= 300 {
			return true
		}
	}
	return false
}

func (s *Service) CancelDelivery(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.mediaScopeLocked() {
		return messaging.ErrUnavailable
	}
	job := s.data.Jobs[id]
	if job == nil {
		return errors.New("delivery_unavailable")
	}
	if job.Status == "cancelled" {
		return nil
	}
	if job.Status != "queued" {
		return errors.New("delivery_already_started")
	}
	job.Status = "cancelled"
	if err := s.saveLocked(); err != nil {
		job.Status = "queued"
		return err
	}
	return nil
}

// AdoptDeliveries attaches a legacy producer journal to an opaque cancellation
// scope. It never changes a submitted outcome or interprets reply policy.
func (s *Service) AdoptDeliveries(account, scope, conversation string, generation uint64, ids []string, cancelPending bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.mediaScopeLocked() || s.data.Binding != account {
		return messaging.ErrScopeChanged
	}
	for _, id := range ids {
		if j := s.data.Jobs[id]; j != nil && s.sessionID([]byte(j.Recipient)) != conversation {
			return messaging.ErrScopeChanged
		}
	}
	originals := map[string]store.Job{}
	for _, id := range ids {
		j := s.data.Jobs[id]
		if j == nil {
			continue
		}
		if s.sessionID([]byte(j.Recipient)) != conversation {
			return messaging.ErrScopeChanged
		}
		if j.DispatchScope != "" {
			continue
		}
		originals[id] = *j
		if j.Source == "codex" {
			j.Source = ""
			j.Origin = "codex"
		}
		j.DispatchScope = scope
		j.DispatchGeneration = generation
		if j.Status == "queued" && (cancelPending || s.deliveryCancelledLocked(j)) {
			j.Status = "cancelled"
		}
	}
	if err := s.saveLocked(); err != nil {
		for id, j := range originals {
			*s.data.Jobs[id] = j
		}
		return err
	}
	return nil
}

// Old automatic sends wait until a consumer explicitly adopts their journal.
func legacyDelivery(job *store.Job) bool {
	return job.DispatchScope == "" && (job.GroupAuto || job.Source == "codex")
}

func (s *Service) BackupState(account, label string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.mediaScopeLocked() || s.data.Binding != account {
		return messaging.ErrScopeChanged
	}
	return s.checkpoint.Backup(label)
}
