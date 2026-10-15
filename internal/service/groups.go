package service

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"strings"

	"github.com/JFSAS/wechat-runtime/internal/protocol"
)

func normalizedMessage(m protocol.Message, owner []byte) protocol.Message {
	if group, ok := m.Group(owner); ok {
		m.Content = group.Content
	}
	return m
}

func (s *Service) groupMemberID(room, member []byte) string {
	h := hmac.New(sha256.New, s.data.APIKey)
	h.Write([]byte("group-member\x00"))
	h.Write(room)
	h.Write([]byte{0})
	h.Write(member)
	return hex.EncodeToString(h.Sum(nil)[:16])
}

func (s *Service) groupMembersLocked(room []byte) map[string][]byte {
	members := map[string][]byte{}
	owner := s.data.Session.Bootstrap.Username
	members[s.groupMemberID(room, owner)] = bytes.Clone(owner)
	for _, m := range s.data.Messages {
		g, ok := m.Group(owner)
		if ok && bytes.Equal(g.Room, room) && len(g.Member) > 0 {
			members[s.groupMemberID(room, g.Member)] = bytes.Clone(g.Member)
		}
	}
	return members
}

func (s *Service) groupMemberLabelLocked(room, member []byte) string {
	if bytes.Equal(member, s.data.Session.Bootstrap.Username) {
		return "我"
	}
	for _, c := range s.data.Contacts {
		if bytes.Equal(c.Username, member) {
			return c.Label()
		}
	}
	if len(member) == 0 {
		return "未知成员"
	}
	return "成员 " + s.groupMemberID(room, member)[:6]
}

func (s *Service) groupRoomsLocked() map[string][]byte {
	rooms := map[string][]byte{}
	for _, peer := range s.data.Pinned {
		if protocol.IsGroup(peer) {
			rooms[s.sessionID(peer)] = peer
		}
	}
	for _, m := range s.data.Messages {
		peer := m.Peer(s.data.Session.Bootstrap.Username)
		if protocol.IsGroup(peer) {
			rooms[s.sessionID(peer)] = peer
		}
	}
	for _, c := range s.data.Contacts {
		if !c.Deleted && protocol.IsGroup(c.Username) {
			rooms[s.sessionID(c.Username)] = c.Username
		}
	}
	for _, peer := range s.data.Unavailable {
		delete(rooms, s.sessionID(peer))
	}
	return rooms
}

func (s *Service) Groups() []map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := []map[string]any{}
	if !s.mediaScopeLocked() {
		return result
	}
	for id, room := range s.groupRoomsLocked() {
		result = append(result, map[string]any{"id": id, "label": s.labelLocked(room), "kind": "group", "observed_member_count": len(s.groupMembersLocked(room)), "members_complete": false})
	}
	sort.Slice(result, func(i, j int) bool { return result[i]["label"].(string) < result[j]["label"].(string) })
	return result
}

func (s *Service) GroupMembers(session string) (map[string]any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.mediaScopeLocked() {
		return nil, errors.New("group unavailable")
	}
	room, ok := s.groupRoomsLocked()[session]
	if !ok {
		return nil, errors.New("group unavailable")
	}
	items := []map[string]any{}
	for id, member := range s.groupMembersLocked(room) {
		items = append(items, map[string]any{"id": id, "label": s.groupMemberLabelLocked(room, member), "self": bytes.Equal(member, s.data.Session.Bootstrap.Username)})
	}
	sort.Slice(items, func(i, j int) bool { return items[i]["label"].(string) < items[j]["label"].(string) })
	return map[string]any{"session": session, "items": items, "complete": false, "source": "observed_messages"}, nil
}

func (s *Service) resolveMentionsLocked(room []byte, ids []string) ([]string, string, error) {
	if len(ids) == 0 {
		return nil, "", nil
	}
	if !protocol.IsGroup(room) || len(ids) > 20 {
		return nil, "", errors.New("invalid group mentions")
	}
	known := s.groupMembersLocked(room)
	seen := map[string]bool{}
	var users []string
	var labels []string
	for _, id := range ids {
		member, ok := known[id]
		if !mediaID(id, 32) || !ok || seen[id] {
			return nil, "", errors.New("group member unavailable")
		}
		seen[id] = true
		users = append(users, string(member))
		labels = append(labels, "@"+s.groupMemberLabelLocked(room, member)+"\u2005")
	}
	return users, strings.Join(labels, ""), nil
}

func isOwnMessage(m protocol.Message, owner []byte) bool {
	if g, ok := m.Group(owner); ok {
		return bytes.Equal(g.Member, owner)
	}
	return bytes.Equal(m.Sender, owner)
}
