package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/JFSAS/wechat-runtime/internal/media"
	"github.com/JFSAS/wechat-runtime/internal/messaging"
	"github.com/JFSAS/wechat-runtime/internal/protocol"
	"github.com/JFSAS/wechat-runtime/internal/store"
	"github.com/JFSAS/wechat-runtime/pkg/bot"
)

// BotAdapter adds normalized identities without changing the imported wire
// implementation. A group sender and a direct contact share sessionID(username).
type BotAdapter struct{ Service *Service }

func channelError(err error) error {
	switch {
	case errors.Is(err, messaging.ErrScopeChanged):
		return bot.ErrScope
	case errors.Is(err, messaging.ErrCursorExpired):
		return bot.ErrCursor
	default:
		return err
	}
}
func (a BotAdapter) Snapshot() bot.Snapshot {
	s := a.Service
	s.mu.RLock()
	defer s.mu.RUnlock()
	v := bot.Snapshot{Online: s.state == "connected" && s.mode == "online", State: s.state, Epoch: s.modeEpoch, Cursor: s.persistedSequence}
	if !s.mediaScopeLocked() {
		return v
	}
	v.Account = s.data.Binding
	v.Self = bot.User{ID: s.sessionID(s.data.Session.Bootstrap.Username), Name: "WeChat"}
	if len(s.data.Events) > 0 {
		v.FirstCursor = s.data.Events[0].Sequence - 1
	}
	return v
}
func (a BotAdapter) Conversations() []bot.Conversation {
	result := []bot.Conversation{}
	for _, c := range a.Service.Conversations() {
		result = append(result, bot.Conversation{ID: c.ID, Kind: c.Kind, Name: c.Label})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}
func (a BotAdapter) usersLocked() map[string][]byte {
	s := a.Service
	result := map[string][]byte{}
	if !s.mediaScopeLocked() {
		return result
	}
	add := func(u []byte) {
		if len(u) > 0 && !protocol.IsGroup(u) {
			result[s.sessionID(u)] = bytes.Clone(u)
		}
	}
	add(s.data.Session.Bootstrap.Username)
	for _, c := range s.data.Contacts {
		if !c.Deleted {
			add(c.Username)
		}
	}
	for _, m := range s.data.Messages {
		if g, ok := m.Group(s.data.Session.Bootstrap.Username); ok {
			add(g.Member)
		} else {
			add(m.Sender)
			add(m.Recipient)
		}
	}
	add([]byte("filehelper"))
	return result
}
func (a BotAdapter) Users() []bot.User {
	s := a.Service
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := []bot.User{}
	for id, u := range a.usersLocked() {
		result = append(result, bot.User{ID: id, Name: s.labelLocked(u)})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}
func (a BotAdapter) eventLocked(m protocol.Message, sequence uint64) bot.Event {
	s := a.Service
	owner := s.data.Session.Bootstrap.Username
	peer := m.Peer(owner)
	sender := m.Sender
	v := bot.Event{ID: messageKey(m, owner), Sequence: sequence, Account: s.data.Binding, Time: int64(m.Timestamp), Own: isOwnMessage(m, owner), LocalEcho: s.localEchoLocked(m, peer), Conversation: bot.Conversation{ID: s.sessionID(peer), Kind: "private", Name: s.labelLocked(peer)}}
	if string(peer) == "filehelper" {
		v.Conversation.Kind = "filehelper"
	}
	if g, ok := m.Group(owner); ok {
		v.Conversation.Kind = "group"
		sender = g.Member
		for _, u := range g.Mentions {
			if u != "" {
				v.Message = append(v.Message, bot.Segment{Type: "at", Data: map[string]string{"user_id": s.sessionID([]byte(u))}})
			}
		}
	}
	v.Sender = bot.User{ID: s.sessionID(sender), Name: s.labelLocked(sender)}
	n := normalizedMessage(m, owner)
	if m.Type == 1 {
		v.Message = append(v.Message, bot.Segment{Type: "text", Data: map[string]string{"text": string(n.Content)}})
	} else if m.Type == 3 || m.Type == 49 {
		if quoted, err := media.QuotedReplyText(n.Content); m.Type == 49 && err == nil {
			v.Message = append(v.Message, bot.Segment{Type: "text", Data: map[string]string{"text": "[引用回复] " + quoted}})
		} else if d, err := media.Parse(m.Type, n.Content); err == nil {
			p := d.Public()
			kind, variant := "image", "regular"
			if p.Kind != media.Image {
				kind, variant = "file", "attachment"
			}
			v.Message = append(v.Message, bot.Segment{Type: kind, Data: map[string]string{"file": v.ID + "/" + variant, "name": p.Name}})
		}
	}
	return v
}
func (a BotAdapter) Events(account string, after uint64, limit int) ([]bot.Event, error) {
	s := a.Service
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.mediaScopeLocked() || account != s.data.Binding {
		return nil, bot.ErrScope
	}
	if after > s.persistedSequence || len(s.data.Events) > 0 && after < s.data.Events[0].Sequence-1 {
		return nil, bot.ErrCursor
	}
	if limit < 1 || limit > 100 {
		limit = 100
	}
	indices := map[string]protocol.Message{}
	for _, m := range s.data.Messages {
		indices[messageKey(m, s.data.Session.Bootstrap.Username)] = m
	}
	result := []bot.Event{}
	for _, e := range s.data.Events {
		if e.Sequence > after && e.Sequence <= s.persistedSequence {
			m, ok := indices[e.Message]
			if !ok {
				return nil, bot.ErrNotFound
			}
			result = append(result, a.eventLocked(m, e.Sequence))
			if len(result) == limit {
				break
			}
		}
	}
	return result, nil
}
func (a BotAdapter) Message(account, id string) (bot.Event, error) {
	s := a.Service
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.mediaScopeLocked() || account != s.data.Binding {
		return bot.Event{}, bot.ErrScope
	}
	for _, m := range s.data.Messages {
		if messageKey(m, s.data.Session.Bootstrap.Username) == id {
			return a.eventLocked(m, 0), nil
		}
	}
	return bot.Event{}, bot.ErrNotFound
}
func (a BotAdapter) deliveryLocked(id string, j *store.Job) bot.Delivery {
	return bot.Delivery{ID: id, State: j.Status, Error: safeJobError(j), Conversation: a.Service.sessionID([]byte(j.Recipient)), Time: int64(j.CreateTime), Text: j.Content}
}
func (a BotAdapter) Delivery(account, id string) (bot.Delivery, error) {
	s := a.Service
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.mediaScopeLocked() || account != s.data.Binding {
		return bot.Delivery{}, bot.ErrScope
	}
	j := s.data.Jobs[id]
	if j == nil {
		return bot.Delivery{}, bot.ErrNotFound
	}
	return a.deliveryLocked(id, j), nil
}

func (a BotAdapter) Submit(ctx context.Context, r bot.Send) (bot.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return bot.Delivery{}, err
	}
	if r.Key == "" || len(r.Key) > 160 || len(r.Mentions) > 20 || !utf8.ValidString(r.Text) || len(r.Text) > 4096 {
		return bot.Delivery{}, errors.New("invalid_message")
	}
	if r.Upload != nil && (r.Text != "" || len(r.Mentions) > 0 || !safeUploadName(r.Upload.Name) || len(r.Upload.Data) == 0 || len(r.Upload.Data) > MaxMediaSendBytes || (r.Upload.Kind != "image" && r.Upload.Kind != "file")) {
		return bot.Delivery{}, errors.New("invalid_upload")
	}
	if r.Upload == nil && strings.TrimSpace(r.Text) == "" {
		return bot.Delivery{}, errors.New("empty_message")
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
	if err := ctx.Err(); err != nil {
		return bot.Delivery{}, err
	}
	if !s.mediaScopeLocked() || r.Account != s.data.Binding {
		return bot.Delivery{}, bot.ErrScope
	}
	if batch, ok := s.data.DeliveryBatches[key]; ok {
		if batch.Fingerprint != fingerprint || len(batch.IDs) != 1 {
			return bot.Delivery{}, bot.ErrConflict
		}
		j := s.data.Jobs[batch.IDs[0]]
		if j == nil {
			return bot.Delivery{}, bot.ErrNotFound
		}
		return a.deliveryLocked(batch.IDs[0], j), nil
	}
	if s.state != "connected" || s.mode != "online" || s.modeEpoch != r.Epoch || s.data.SessionInvalid {
		return bot.Delivery{}, bot.ErrUnavailable
	}
	if len(s.data.Jobs) >= 10000 || len(s.data.DeliveryBatches) >= 10000 {
		return bot.Delivery{}, errors.New("send_journal_full")
	}
	peer, ok := s.peersLocked()[r.Conversation]
	if !ok {
		return bot.Delivery{}, bot.ErrNotFound
	}
	var mentions []string
	users := a.usersLocked()
	for _, id := range r.Mentions {
		user, ok := users[id]
		if !ok {
			return bot.Delivery{}, bot.ErrNotFound
		}
		mentions = append(mentions, s.groupMemberID(peer, user))
	}
	raw, prefix, err := s.resolveMentionsLocked(peer, mentions)
	if err != nil {
		return bot.Delivery{}, err
	}
	source, err := protocol.MentionSource(raw)
	if err != nil {
		return bot.Delivery{}, err
	}
	text := prefix + r.Text
	if len(text) > 4096 {
		return bot.Delivery{}, errors.New("message_too_long")
	}
	oldNext := s.data.NextClientID
	var id string
	var job *store.Job
	if r.Upload != nil {
		mime := http.DetectContentType(r.Upload.Data)
		if r.Upload.Kind == "image" {
			info, e := media.ValidateImage(r.Upload.Data)
			if e != nil {
				return bot.Delivery{}, e
			}
			mime = info.MIME
		}
		id, job, err = s.stageMediaSendLocked(peer, r.Upload.Kind, r.Upload.Name, mime, r.Upload.Data)
		if err != nil {
			return bot.Delivery{}, err
		}
	} else {
		random, e := protocol.Random(16)
		if e != nil {
			return bot.Delivery{}, e
		}
		id = hex.EncodeToString(random)
		n := s.data.NextClientID
		if n == 0 {
			n = 1
		}
		job = &store.Job{Recipient: string(peer), Content: text, Source: source, ClientID: n, CreateTime: uint32(time.Now().Unix()), Status: "queued"}
		s.data.NextClientID = n%0x7fffffff + 1
	}
	job.Origin = "onebot"
	if s.data.Jobs == nil {
		s.data.Jobs = map[string]*store.Job{}
	}
	if s.data.DeliveryBatches == nil {
		s.data.DeliveryBatches = map[string]messaging.DeliveryBatch{}
	}
	s.data.Jobs[id] = job
	s.data.DeliveryBatches[key] = messaging.DeliveryBatch{Fingerprint: fingerprint, IDs: []string{id}}
	if err = s.saveLocked(); err != nil {
		delete(s.data.Jobs, id)
		delete(s.data.DeliveryBatches, key)
		s.data.NextClientID = oldNext
		if job.Media != nil {
			_ = (store.MediaCache{Directory: filepath.Join(s.directory, "media"), Binding: r.Account}).Remove(id)
		}
		return bot.Delivery{}, err
	}
	s.signal()
	return a.deliveryLocked(id, job), nil
}
func (a BotAdapter) Download(ctx context.Context, account, conversation, file string) (bot.Content, error) {
	parts := strings.Split(file, "/")
	if len(parts) != 2 {
		return bot.Content{}, bot.ErrNotFound
	}
	id, err := a.Service.Download(account, conversation, parts[0], parts[1])
	if err != nil {
		return bot.Content{}, channelError(err)
	}
	timer := time.NewTicker(50 * time.Millisecond)
	defer timer.Stop()
	for {
		state, category := a.Service.DownloadStatus(account, id)
		switch state {
		case "downloaded":
			c, e := a.Service.Content(account, id)
			return bot.Content{Data: c.Data, MIME: c.MIME, Name: c.Name}, channelError(e)
		case "failed":
			return bot.Content{}, errors.New(category)
		}
		select {
		case <-ctx.Done():
			return bot.Content{}, ctx.Err()
		case <-timer.C:
		}
	}
}

var _ bot.Channel = BotAdapter{}

func safeJobError(j *store.Job) string {
	if j.ErrorCategory != nil {
		return *j.ErrorCategory
	}
	return ""
}
