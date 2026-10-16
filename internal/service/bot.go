package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/JFSAS/wechat-runtime/internal/media"
	"github.com/JFSAS/wechat-runtime/internal/messaging"
	"github.com/JFSAS/wechat-runtime/internal/protocol"
	"github.com/JFSAS/wechat-runtime/internal/store"
	"github.com/JFSAS/wechat-runtime/pkg/bot"
)

// BotAdapter adds normalized identities without changing the imported wire
// implementation. A group sender and a direct contact share sessionID(username).
type BotAdapter struct {
	Service      *Service
	Archive      *DeliveryArchive
	persist      func() error
	archiveWrite func([]archivedBatch) error
}

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
	if s.history != nil {
		stats, err := s.history.Stats(s.data.Binding)
		if err != nil {
			v.Online, v.Error = false, "event_history_unavailable"
			return v
		}
		v.Cursor, v.FirstCursor = stats.Cursor, stats.FirstCursor
	}
	return v
}
func (a BotAdapter) Conversations() ([]bot.Conversation, error) {
	s := a.Service
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := []bot.Conversation{}
	if !s.mediaScopeLocked() {
		return result, nil
	}
	if s.history != nil {
		entries, err := s.history.directoryList(s.data.Binding, "conversations", "")
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			result = append(result, bot.Conversation{ID: entry.ID, Kind: entry.Kind, Name: entry.Name})
		}
	} else {
		for id, peer := range s.peersLocked() {
			result = append(result, bot.Conversation{ID: id, Kind: conversationKind(peer), Name: s.labelLocked(peer)})
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}
func (a BotAdapter) Conversation(account, id string) (bot.Conversation, error) {
	s := a.Service
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.mediaScopeLocked() || account != s.data.Binding {
		return bot.Conversation{}, bot.ErrScope
	}
	if s.history != nil {
		row, _, err := s.history.conversation(account, id)
		return row, err
	}
	raw, ok := s.peersLocked()[id]
	if !ok {
		return bot.Conversation{}, bot.ErrNotFound
	}
	return bot.Conversation{ID: id, Kind: conversationKind(raw), Name: s.labelLocked(raw)}, nil
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
func (a BotAdapter) Users() ([]bot.User, error) {
	s := a.Service
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := []bot.User{}
	if !s.mediaScopeLocked() {
		return result, nil
	}
	if s.history != nil {
		entries, err := s.history.directoryList(s.data.Binding, "users", "")
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			result = append(result, bot.User{ID: entry.ID, Name: entry.Name})
		}
	} else {
		for id, raw := range a.usersLocked() {
			result = append(result, bot.User{ID: id, Name: s.labelLocked(raw)})
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}
func (a BotAdapter) User(account, id string) (bot.User, error) {
	s := a.Service
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.mediaScopeLocked() || account != s.data.Binding {
		return bot.User{}, bot.ErrScope
	}
	if s.history != nil {
		return s.history.user(account, id)
	}
	raw, ok := a.usersLocked()[id]
	if !ok {
		return bot.User{}, bot.ErrNotFound
	}
	return bot.User{ID: id, Name: s.labelLocked(raw)}, nil
}
func (a BotAdapter) Members(account, conversation string) (bot.Roster, error) {
	s := a.Service
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := bot.Roster{Users: []bot.User{}, Complete: false, Source: "observed_messages"}
	if !s.mediaScopeLocked() || s.data.Binding != account {
		return result, bot.ErrScope
	}
	if s.history != nil {
		room, _, err := s.history.conversation(account, conversation)
		if err != nil {
			return result, err
		}
		if room.Kind != "group" || room.Unavailable {
			return result, bot.ErrNotFound
		}
		users, err := s.history.members(account, conversation, s.sessionID(s.data.Session.Bootstrap.Username))
		if err != nil {
			return result, err
		}
		own := s.sessionID(s.data.Session.Bootstrap.Username)
		found := false
		for _, u := range users {
			if u.ID == own {
				found = true
				break
			}
		}
		if !found {
			users = append(users, bot.User{ID: own, Name: "我"})
		}
		sort.Slice(users, func(i, j int) bool { return users[i].ID < users[j].ID })
		result.Users, result.Source = users, "observed_history"
		return result, nil
	}
	room, exists := s.groupRoomsLocked()[conversation]
	if !exists {
		return result, bot.ErrNotFound
	}
	for _, raw := range s.groupMembersLocked(room) {
		result.Users = append(result.Users, bot.User{ID: s.sessionID(raw), Name: s.groupMemberLabelLocked(room, raw)})
	}
	sort.Slice(result.Users, func(i, j int) bool { return result.Users[i].ID < result.Users[j].ID })
	return result, nil
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
	if s.history != nil {
		rows, err := s.history.events(account, after, limit)
		if err != nil {
			return nil, err
		}
		events := make([]bot.Event, 0, len(rows))
		for i := range rows {
			if err := a.historyEchoLocked(&rows[i]); err != nil {
				return nil, err
			}
			events = append(events, rows[i].Event)
		}
		return events, nil
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
			value := a.eventLocked(m, e.Sequence)
			if !value.LocalEcho && a.Archive != nil {
				echo, err := a.Archive.localEcho(account, m, s.data.Session.Bootstrap.Username)
				if err != nil {
					return nil, err
				}
				value.LocalEcho = echo
			}
			result = append(result, value)
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
	if s.history != nil {
		row, err := s.history.message(account, id)
		if err != nil {
			return bot.Event{}, err
		}
		if row != nil {
			if err := a.historyEchoLocked(row); err != nil {
				return bot.Event{}, err
			}
			return row.Event, nil
		}
	}
	for _, m := range s.data.Messages {
		if messageKey(m, s.data.Session.Bootstrap.Username) == id {
			return a.eventLocked(m, 0), nil
		}
	}
	for deliveryID, job := range s.data.Jobs {
		if job.ServerID == 0 || a.receiptMessageID(job) != id {
			continue
		}
		delivery := a.deliveryLocked(deliveryID, job)
		if delivery.MessageID == id {
			return bot.Event{ID: id, Account: account, Conversation: bot.Conversation{ID: delivery.Conversation, Kind: conversationKind([]byte(job.Recipient)), Name: s.labelLocked([]byte(job.Recipient))}, Sender: bot.User{ID: s.sessionID(s.data.Session.Bootstrap.Username), Name: "WeChat"}, Time: delivery.Time, Message: delivery.Message, Own: true, LocalEcho: true}, nil
		}
	}
	if a.Archive != nil {
		row, err := a.Archive.indexed(account, "messages", id)
		if err != nil {
			return bot.Event{}, err
		}
		if row != nil {
			for i, job := range row.Jobs {
				if job.ServerID == 0 || a.receiptMessageID(&job) != id {
					continue
				}
				delivery := a.deliveryLocked(row.IDs[i], &job)
				return bot.Event{ID: id, Account: account, Conversation: bot.Conversation{ID: delivery.Conversation, Kind: conversationKind([]byte(job.Recipient)), Name: s.labelLocked([]byte(job.Recipient))}, Sender: bot.User{ID: s.sessionID(s.data.Session.Bootstrap.Username), Name: "WeChat"}, Time: delivery.Time, Message: delivery.Message, Own: true, LocalEcho: true}, nil
			}
			return bot.Event{}, ErrDeliveryArchive
		}
	}
	return bot.Event{}, bot.ErrNotFound
}
func conversationKind(peer []byte) string {
	if protocol.IsGroup(peer) {
		return "group"
	}
	if string(peer) == "filehelper" {
		return "filehelper"
	}
	return "private"
}
func (a BotAdapter) deliveryLocked(id string, j *store.Job) bot.Delivery {
	s := a.Service
	d := bot.Delivery{ID: id, State: j.Status, Error: safeJobError(j), Conversation: s.sessionID([]byte(j.Recipient)), Time: int64(j.CreateTime), Text: j.Content}
	if (j.Status == "accepted" || j.Status == "echo_observed") && j.ServerID == 0 {
		d.State = "unknown"
		d.Error = "message_receipt_missing"
	}
	if j.ServerID != 0 {
		d.MessageID = a.receiptMessageID(j)
	}
	for _, m := range s.data.Messages {
		if j.ServerID != 0 && m.ServerID == j.ServerID && string(m.Peer(s.data.Session.Bootstrap.Username)) == j.Recipient {
			event := a.eventLocked(m, 0)
			d.MessageID = event.ID
			d.Message = event.Message
			return d
		}
	}
	if j.Media != nil {
		d.Message = []bot.Segment{{Type: j.Kind, Data: map[string]string{"file": "outgoing:" + id + "/cache", "name": j.Media.Name}}}
	} else {
		d.Message = []bot.Segment{{Type: "text", Data: map[string]string{"text": j.Content}}}
	}
	return d
}
func (a BotAdapter) receiptMessageID(j *store.Job) string {
	return messageKey(protocol.Message{Sender: a.Service.data.Session.Bootstrap.Username, Recipient: []byte(j.Recipient), ServerID: j.ServerID}, a.Service.data.Session.Bootstrap.Username)
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
		if a.Archive != nil {
			row, err := a.Archive.indexed(account, "jobs", id)
			if err != nil {
				return bot.Delivery{}, err
			}
			return a.archivedDeliveryLocked(row, id)
		}
		return bot.Delivery{}, bot.ErrNotFound
	}
	for _, batch := range s.data.DeliveryBatches {
		if len(batch.IDs) > 1 && batch.IDs[0] == id {
			return a.batchLocked(batch.IDs)
		}
	}
	return a.deliveryLocked(id, j), nil
}

func (a BotAdapter) Download(ctx context.Context, account, conversation, file string) (bot.Content, error) {
	if err := ctx.Err(); err != nil {
		return bot.Content{}, err
	}
	parts := strings.Split(file, "/")
	if len(parts) != 2 {
		return bot.Content{}, bot.ErrNotFound
	}
	if strings.HasPrefix(parts[0], "outgoing:") && parts[1] == "cache" {
		id := strings.TrimPrefix(parts[0], "outgoing:")
		s := a.Service
		s.mu.RLock()
		if !s.mediaScopeLocked() || s.data.Binding != account {
			s.mu.RUnlock()
			return bot.Content{}, bot.ErrScope
		}
		job := s.data.Jobs[id]
		if job == nil && a.Archive != nil {
			row, err := a.Archive.indexed(account, "jobs", id)
			if err != nil {
				s.mu.RUnlock()
				return bot.Content{}, err
			}
			if row != nil {
				for i, archivedID := range row.IDs {
					if archivedID == id {
						job = &row.Jobs[i]
						break
					}
				}
			}
		}
		if job == nil || job.Media == nil || s.sessionID([]byte(job.Recipient)) != conversation {
			s.mu.RUnlock()
			return bot.Content{}, bot.ErrNotFound
		}
		metadata := *job.Media
		s.mu.RUnlock()
		content, err := (store.MediaCache{Directory: filepath.Join(s.directory, "media"), Binding: account}).Load(id)
		if err != nil {
			return bot.Content{}, err
		}
		digest := sha256.Sum256(content)
		if int64(len(content)) != metadata.Size || hex.EncodeToString(digest[:]) != metadata.Digest {
			return bot.Content{}, errors.New("media_integrity_failed")
		}
		if a.Snapshot().Account != account {
			return bot.Content{}, bot.ErrScope
		}
		return bot.Content{Data: content, MIME: metadata.MIME, Name: metadata.Name}, nil
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
