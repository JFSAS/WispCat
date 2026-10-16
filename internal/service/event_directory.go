package service

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/JFSAS/wechat-runtime/internal/protocol"
	"github.com/JFSAS/wechat-runtime/internal/securedb"
	"github.com/JFSAS/wechat-runtime/pkg/bot"
)

type directoryMeta struct {
	Schema        int    `json:"schema"`
	Account       string `json:"account"`
	Namespace     string `json:"namespace"`
	Revision      uint64 `json:"revision"`
	Users         uint64 `json:"users"`
	Conversations uint64 `json:"conversations"`
	Members       uint64 `json:"members"`
	Variants      uint64 `json:"variants"`
	Fragments     uint64 `json:"fragments"`
}
type directoryEntry struct {
	Schema    int               `json:"schema"`
	Account   string            `json:"account"`
	ID        string            `json:"id"`
	Raw       []byte            `json:"raw"`
	Name      string            `json:"name"`
	Named     bool              `json:"named"`
	Kind      string            `json:"kind"`
	Available bool              `json:"available"`
	Room      string            `json:"room,omitempty"`
	Contact   *protocol.Contact `json:"contact,omitempty"`
}

func (directoryEntry) String() string   { return "directoryEntry(redacted)" }
func (directoryEntry) GoString() string { return "directoryEntry(redacted)" }

type historyContact struct {
	account, namespace string
	contact            protocol.Contact
}

const directoryListLimit = 20000
const directoryListBytes = 16 << 20

func (h *EventHistory) directoryLocation(bucket, account, id string) chunkedRecord {
	key := append(h.db.Key(bucket, account), []byte(id)...)
	return chunkedRecord{bucket: bucket, key: key, chunkBucket: "directory-bodies", chunkKey: func(i int) []byte { return h.db.Key("directory-body", bucket, account, id, strconv.Itoa(i)) }}
}
func (h *EventHistory) directoryMeta(tx *securedb.Tx, account, namespace string) (directoryMeta, bool, error) {
	raw, err := tx.Get("directory-meta", h.db.Key("directory", account))
	if err != nil {
		return directoryMeta{}, false, err
	}
	if raw == nil {
		return directoryMeta{Schema: 1, Account: account, Namespace: namespace}, false, nil
	}
	var meta directoryMeta
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return meta, false, ErrEventHistory
	}
	for _, field := range []string{"schema", "account", "namespace", "revision", "users", "conversations", "members", "variants", "fragments"} {
		if fields[field] == nil || string(fields[field]) == "null" {
			return meta, false, ErrEventHistory
		}
	}
	if json.Unmarshal(raw, &meta) != nil || meta.Schema != 1 || meta.Account != account || !mediaID(meta.Namespace, 64) || (namespace != "" && meta.Namespace != namespace) || meta.Users > meta.Revision || meta.Conversations > meta.Revision || meta.Members > meta.Revision || meta.Variants > meta.Revision || meta.Fragments > meta.Revision {
		return meta, false, ErrEventHistory
	}
	return meta, true, nil
}
func (h *EventHistory) putDirectoryMeta(tx *securedb.Tx, meta directoryMeta) error {
	raw, _ := json.Marshal(meta)
	return tx.Put("directory-meta", h.db.Key("directory", meta.Account), raw)
}
func (h *EventHistory) directoryEntry(tx *securedb.Tx, bucket, account, id string) (*directoryEntry, error) {
	return h.directoryEntryLimit(tx, bucket, account, id, historyMaxBodyBytes)
}
func (h *EventHistory) directoryEntryLimit(tx *securedb.Tx, bucket, account, id string, limit int) (*directoryEntry, error) {
	raw, err := h.directoryLocation(bucket, account, id).readLimit(tx, limit)
	if err != nil || raw == nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return nil, ErrEventHistory
	}
	for _, field := range []string{"schema", "account", "id", "raw", "name", "named", "kind", "available"} {
		if fields[field] == nil || string(fields[field]) == "null" {
			return nil, ErrEventHistory
		}
	}
	var entry directoryEntry
	if json.Unmarshal(raw, &entry) != nil || entry.Schema != 1 || entry.Account != account || entry.ID != id || len(entry.Raw) == 0 || len(entry.Raw) > 4<<20 {
		return nil, ErrEventHistory
	}
	if bucket == "members" {
		if !mediaID(entry.Room, 32) || len(id) != 65 || id[:33] != entry.Room+"/" || !mediaID(id[33:], 32) {
			return nil, ErrEventHistory
		}
	} else if !mediaID(id, 32) {
		return nil, ErrEventHistory
	}
	if entry.Kind != "user" && entry.Kind != "group" && entry.Kind != "private" && entry.Kind != "filehelper" {
		return nil, ErrEventHistory
	}
	if len(h.directoryKey) != 32 {
		return nil, ErrEventHistory
	}
	hash := hmac.New(sha256.New, h.directoryKey)
	hash.Write(entry.Raw)
	canonical := hex.EncodeToString(hash.Sum(nil)[:16])
	if (bucket == "members" && id[33:] != canonical) || (bucket != "members" && id != canonical) {
		return nil, ErrEventHistory
	}
	if bucket == "conversations" {
		if entry.Kind != conversationKind(entry.Raw) {
			return nil, ErrEventHistory
		}
	} else if entry.Kind != "user" || protocol.IsGroup(entry.Raw) {
		return nil, ErrEventHistory
	}
	return &entry, nil
}
func (h *EventHistory) putDirectoryEntry(tx *securedb.Tx, meta *directoryMeta, bucket string, entry directoryEntry) error {
	old, err := h.directoryEntry(tx, bucket, entry.Account, entry.ID)
	if err != nil {
		return err
	}
	if old != nil && old.Contact != nil && entry.Contact == nil {
		entry.Contact = old.Contact
	}
	if old != nil && old.Named && !entry.Named {
		entry.Name, entry.Named = old.Name, true
	}
	raw, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if old != nil {
		previous, _ := json.Marshal(old)
		if bytes.Equal(previous, raw) {
			return nil
		}
	}
	if meta.Revision == math.MaxUint64 {
		return ErrEventHistory
	}
	meta.Revision++
	if old == nil {
		switch bucket {
		case "users":
			meta.Users++
		case "conversations":
			meta.Conversations++
		case "members":
			meta.Members++
		}
	}
	return h.directoryLocation(bucket, entry.Account, entry.ID).write(tx, raw)
}
func (h *EventHistory) observeUser(tx *securedb.Tx, s *Service, meta *directoryMeta, raw []byte) error {
	if len(raw) == 0 || protocol.IsGroup(raw) {
		return nil
	}
	id := s.sessionID(raw)
	entry := directoryEntry{Schema: 1, Account: s.data.Binding, ID: id, Raw: raw, Name: s.labelLocked(raw), Kind: "user", Available: true}
	if bytes.Equal(raw, s.data.Session.Bootstrap.Username) {
		entry.Name = "WeChat"
		entry.Named = true
	}
	return h.putDirectoryEntry(tx, meta, "users", entry)
}
func (h *EventHistory) observeConversation(tx *securedb.Tx, s *Service, meta *directoryMeta, raw []byte) error {
	if len(raw) == 0 {
		return nil
	}
	id := s.sessionID(raw)
	old, err := h.directoryEntry(tx, "conversations", s.data.Binding, id)
	if err != nil {
		return err
	}
	entry := directoryEntry{Schema: 1, Account: s.data.Binding, ID: id, Raw: raw, Name: s.labelLocked(raw), Kind: conversationKind(raw), Available: true}
	if old != nil && !old.Available {
		entry.Available = false
	}
	for _, peer := range s.data.Unavailable {
		if bytes.Equal(peer, raw) {
			entry.Available = false
		}
	}
	return h.putDirectoryEntry(tx, meta, "conversations", entry)
}
func (h *EventHistory) observeMessage(tx *securedb.Tx, s *Service, meta *directoryMeta, message protocol.Message) error {
	owner := s.data.Session.Bootstrap.Username
	peer := message.Peer(owner)
	if err := h.observeConversation(tx, s, meta, peer); err != nil {
		return err
	}
	if !protocol.IsGroup(peer) {
		if err := h.observeUser(tx, s, meta, message.Sender); err != nil {
			return err
		}
		return h.observeUser(tx, s, meta, message.Recipient)
	}
	group, ok := message.Group(owner)
	if !ok {
		return nil
	}
	for _, member := range [][]byte{owner, group.Member} {
		if len(member) == 0 {
			continue
		}
		if err := h.observeUser(tx, s, meta, member); err != nil {
			return err
		}
		entry := directoryEntry{Schema: 1, Account: s.data.Binding, ID: s.sessionID(peer) + "/" + s.sessionID(member), Raw: member, Room: s.sessionID(peer), Name: s.groupMemberLabelLocked(peer, member), Kind: "user", Available: true}
		if err := h.putDirectoryEntry(tx, meta, "members", entry); err != nil {
			return err
		}
	}
	return nil
}
func (h *EventHistory) stageContactsLocked(s *Service, changes []protocol.Contact) {
	if len(changes) == 0 {
		return
	}
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	if h.closed || h.stageErr != nil {
		h.stageErr = ErrEventHistory
		return
	}
	for _, contact := range changes {
		size := len(contact.Username)
		if contact.Nickname != nil {
			size += len(*contact.Nickname)
		}
		if contact.Remark != nil {
			size += len(*contact.Remark)
		}
		if len(h.pendingContacts) >= 50000 || h.pendingBytes+size > 32<<20 {
			h.stageErr = ErrEventHistory
			return
		}
		contact.Username = bytes.Clone(contact.Username)
		if contact.Nickname != nil {
			name := *contact.Nickname
			contact.Nickname = &name
		}
		if contact.Remark != nil {
			name := *contact.Remark
			contact.Remark = &name
		}
		h.pendingContacts = append(h.pendingContacts, historyContact{account: s.data.Binding, namespace: historyNamespace(s.data), contact: contact})
		h.pendingBytes += size
	}
}
func (h *EventHistory) observeContact(tx *securedb.Tx, s *Service, meta *directoryMeta, contact protocol.Contact) error {
	if len(contact.Username) == 0 || bytes.Equal(contact.Username, s.data.Session.Bootstrap.Username) {
		return nil
	}
	id := s.sessionID(contact.Username)
	old, err := h.directoryEntry(tx, "conversations", s.data.Binding, id)
	if err != nil {
		return err
	}
	merged := contact
	if old != nil && old.Contact != nil {
		if merged.Nickname == nil {
			merged.Nickname = old.Contact.Nickname
		}
		if merged.Remark == nil {
			merged.Remark = old.Contact.Remark
		}
	}
	deleted := contact.Deleted
	merged.Deleted = false
	entry := directoryEntry{Schema: 1, Account: s.data.Binding, ID: id, Raw: contact.Username, Name: merged.Label(), Named: merged.Nickname != nil || merged.Remark != nil, Kind: conversationKind(contact.Username), Available: !deleted, Contact: &merged}
	if err = h.putDirectoryEntry(tx, meta, "conversations", entry); err != nil {
		return err
	}
	if !protocol.IsGroup(contact.Username) {
		user := entry
		user.Kind = "user"
		user.Available = true
		user.Contact = nil
		return h.putDirectoryEntry(tx, meta, "users", user)
	}
	return nil
}

// Bounded-memory traversal. Every page stops itself before Scan's hard limit.
func scanDirectory(tx *securedb.Tx, bucket string, prefix []byte, visit func([]byte) error) error {
	start := bytes.Clone(prefix)
	for {
		count := 0
		var last []byte
		err := tx.Scan(bucket, prefix, start, 1001, func(key, _ []byte) (bool, error) {
			count++
			last = bytes.Clone(key)
			if err := visit(key); err != nil {
				return false, err
			}
			return count < 1000, nil
		})
		if err != nil {
			return err
		}
		if count < 1000 {
			return nil
		}
		start = append(last, 0)
	}
}

// Version 2 atomically derives the observed directory from old retained history
// and the legacy checkpoint. No public event sequence is renumbered.
func (h *EventHistory) upgradeDirectory(tx *securedb.Tx, s *Service, meta historyMeta, exists bool) (historyMeta, directoryMeta, error) {
	h.directoryKey = bytes.Clone(s.data.APIKey)
	directory, ready, err := h.directoryMeta(tx, s.data.Binding, historyNamespace(s.data))
	if err != nil {
		return meta, directory, err
	}
	if meta.Schema == 2 {
		if !ready {
			return meta, directory, ErrEventHistory
		}
		return meta, directory, nil
	}
	if ready {
		return meta, directory, ErrEventHistory
	}
	if exists {
		prefix := h.db.Key("stream", s.data.Binding)
		err = scanDirectory(tx, "events", prefix, func(key []byte) error {
			raw, e := tx.Get("events", key)
			if e != nil {
				return e
			}
			var id string
			if json.Unmarshal(raw, &id) != nil {
				return ErrEventHistory
			}
			row, e := h.record(tx, s.data.Binding, id)
			if e != nil || row == nil {
				return ErrEventHistory
			}
			if e = h.indexMessageID(tx, s.data.Binding, id); e != nil {
				return e
			}
			return h.observeHistoryRow(tx, s, &directory, row)
		})
		if err != nil {
			return meta, directory, err
		}
		for _, message := range s.data.Messages {
			event := (BotAdapter{Service: s}).eventLocked(message, 0)
			added, e := h.put(tx, historyRecord{Schema: 1, Raw: message, Event: event})
			if e != nil {
				return meta, directory, e
			}
			if added {
				meta.Messages++
			}
		}
	}
	prefix := h.db.Key("message-ids", s.data.Binding)
	err = scanDirectory(tx, "message-ids", prefix, func(key []byte) error {
		raw, e := tx.Get("message-ids", key)
		if e != nil {
			return e
		}
		var id string
		if json.Unmarshal(raw, &id) != nil {
			return ErrEventHistory
		}
		row, e := h.record(tx, s.data.Binding, id)
		if e != nil || row == nil {
			return ErrEventHistory
		}
		return h.observeMessage(tx, s, &directory, row.Raw)
	})
	if err != nil {
		return meta, directory, err
	}
	if err = h.observeUser(tx, s, &directory, s.data.Session.Bootstrap.Username); err != nil {
		return meta, directory, err
	}
	if err = h.observeUser(tx, s, &directory, []byte("filehelper")); err != nil {
		return meta, directory, err
	}
	if err = h.observeConversation(tx, s, &directory, []byte("filehelper")); err != nil {
		return meta, directory, err
	}
	for _, contact := range s.data.Contacts {
		if err = h.observeContact(tx, s, &directory, contact); err != nil {
			return meta, directory, err
		}
	}
	for _, message := range s.data.Messages {
		if err = h.observeMessage(tx, s, &directory, message); err != nil {
			return meta, directory, err
		}
	}
	for _, peer := range s.data.Pinned {
		if err = h.observeConversation(tx, s, &directory, peer); err != nil {
			return meta, directory, err
		}
	}
	for _, peer := range s.data.Unavailable {
		if err = h.observeContact(tx, s, &directory, protocol.Contact{Username: peer, Deleted: true}); err != nil {
			return meta, directory, err
		}
	}
	for _, event := range s.data.Events {
		if err = h.retainFragment(tx, &directory, event); err != nil {
			return meta, directory, err
		}
	}
	meta.Schema = 2
	if directory.Revision == 0 {
		directory.Revision = 1
	}
	if err = h.putDirectoryMeta(tx, directory); err != nil {
		return meta, directory, err
	}
	return meta, directory, nil
}
func (h *EventHistory) directoryRead(account string, read func(*securedb.Tx) error) error {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	if h.closed || h.unavailable || h.stageErr != nil {
		return ErrEventHistory
	}
	err := h.db.View(func(tx *securedb.Tx) error {
		meta, exists, err := h.meta(tx, account, "")
		if err != nil {
			return err
		}
		if !exists {
			return bot.ErrNotFound
		}
		dir, ready, err := h.directoryMeta(tx, account, meta.Namespace)
		if err != nil || !ready || meta != h.published || meta.Schema != 2 || dir.Revision != h.publishedDirectory {
			return ErrEventHistory
		}
		return read(tx)
	})
	if err == nil || err == bot.ErrNotFound || err == bot.ErrLimit {
		return err
	}
	return ErrEventHistory
}
func (h *EventHistory) user(account, id string) (bot.User, error) {
	var result bot.User
	err := h.directoryRead(account, func(tx *securedb.Tx) error {
		entry, err := h.directoryEntry(tx, "users", account, id)
		if err != nil {
			return err
		}
		if entry == nil {
			return bot.ErrNotFound
		}
		if len(entry.Name) > directoryListBytes {
			return bot.ErrLimit
		}
		result = bot.User{ID: id, Name: entry.Name}
		return nil
	})
	return result, err
}
func (h *EventHistory) conversation(account, id string) (bot.Conversation, []byte, error) {
	var result bot.Conversation
	var peer []byte
	err := h.directoryRead(account, func(tx *securedb.Tx) error {
		entry, err := h.directoryEntry(tx, "conversations", account, id)
		if err != nil {
			return err
		}
		if entry == nil {
			return bot.ErrNotFound
		}
		result = bot.Conversation{ID: id, Kind: entry.Kind, Name: entry.Name, Unavailable: !entry.Available}
		peer = bytes.Clone(entry.Raw)
		return nil
	})
	return result, peer, err
}
func (h *EventHistory) directoryList(account, bucket, room string) ([]directoryEntry, error) {
	result := []directoryEntry{}
	size, visited := 0, 0
	err := h.directoryRead(account, func(tx *securedb.Tx) error {
		prefix := h.db.Key(bucket, account)
		if room != "" {
			prefix = append(prefix, []byte(room+"/")...)
		}
		return scanDirectory(tx, bucket, prefix, func(key []byte) error {
			visited++
			if visited > directoryListLimit {
				return bot.ErrLimit
			}
			id := string(key[32:])
			entry, err := h.directoryEntryLimit(tx, bucket, account, id, directoryListBytes-size)
			if err != nil {
				return err
			}
			if entry == nil {
				return ErrEventHistory
			}
			encoded, _ := json.Marshal(entry)
			size += len(encoded)
			if visited > directoryListLimit || size > directoryListBytes {
				return bot.ErrLimit
			}
			if bucket == "conversations" && !entry.Available {
				return nil
			}
			result = append(result, *entry)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (h *EventHistory) indexMessageID(tx *securedb.Tx, account, id string) error {
	if !mediaID(id, 64) {
		return ErrEventHistory
	}
	key := append(h.db.Key("message-ids", account), h.db.Key("id", id)...)
	old, err := tx.Get("message-ids", key)
	if err != nil {
		return err
	}
	raw, _ := json.Marshal(id)
	if old != nil {
		if !bytes.Equal(old, raw) {
			return ErrEventHistory
		}
		return nil
	}
	return tx.Put("message-ids", key, raw)
}
func (h *EventHistory) restoreRuntimeLocked(s *Service) error {
	if err := h.restoreMediaLocked(s); err != nil {
		return err
	}
	if h.deliveryArchive != nil {
		if err := h.deliveryArchive.recoverOutgoingLocked(s.data, h); err != nil {
			return err
		}
	}
	return h.directoryRead(s.data.Binding, func(tx *securedb.Tx) error {
		unavailable := map[string]bool{}
		for _, raw := range s.data.Unavailable {
			entry, err := h.directoryEntry(tx, "conversations", s.data.Binding, s.sessionID(raw))
			if err != nil {
				return err
			}
			if entry == nil || !entry.Available || entry.Contact == nil {
				unavailable[string(raw)] = true
			}
		}
		retained := [][]byte{}
		for _, raw := range s.data.Unavailable {
			if unavailable[string(raw)] {
				retained = append(retained, raw)
			}
		}
		for _, job := range s.data.Jobs {
			entry, err := h.directoryEntry(tx, "conversations", s.data.Binding, s.sessionID([]byte(job.Recipient)))
			if err != nil {
				return err
			}
			if entry != nil && !entry.Available && !unavailable[job.Recipient] {
				retained = append(retained, []byte(job.Recipient))
				unavailable[job.Recipient] = true
			}
		}
		s.data.Unavailable = retained
		return nil
	})
}

func (h *EventHistory) members(account, room, own string) ([]bot.User, error) {
	result := []bot.User{}
	size := 0
	err := h.directoryRead(account, func(tx *securedb.Tx) error {
		prefix := append(h.db.Key("members", account), []byte(room+"/")...)
		return scanDirectory(tx, "members", prefix, func(key []byte) error {
			if len(result) >= directoryListLimit {
				return bot.ErrLimit
			}
			entry, err := h.directoryEntryLimit(tx, "members", account, string(key[32:]), directoryListBytes-size)
			if err != nil {
				return err
			}
			if entry == nil {
				return ErrEventHistory
			}
			id := entry.ID[33:]
			user, err := h.directoryEntryLimit(tx, "users", account, id, directoryListBytes-size)
			if err != nil {
				return err
			}
			if user == nil {
				return ErrEventHistory
			}
			name := entry.Name
			if user.Named && id != own {
				name = user.Name
			}
			encoded, _ := json.Marshal(bot.User{ID: id, Name: name})
			size += len(encoded) + 1
			if len(result) >= directoryListLimit || size > directoryListBytes {
				return bot.ErrLimit
			}
			result = append(result, bot.User{ID: id, Name: name})
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}
func (a BotAdapter) resolveBotMentionsLocked(peer []byte, ids []string) ([]string, string, error) {
	s := a.Service
	if s.history == nil {
		users := a.usersLocked()
		aliases := []string{}
		for _, id := range ids {
			user, ok := users[id]
			if !ok {
				return nil, "", bot.ErrNotFound
			}
			aliases = append(aliases, s.groupMemberID(peer, user))
		}
		return s.resolveMentionsLocked(peer, aliases)
	}
	if len(ids) == 0 {
		return nil, "", nil
	}
	if !protocol.IsGroup(peer) || len(ids) > 20 {
		return nil, "", bot.ErrNotFound
	}
	users := []string{}
	labels := []string{}
	seen := map[string]bool{}
	err := s.history.directoryRead(s.data.Binding, func(tx *securedb.Tx) error {
		room := s.sessionID(peer)
		for _, id := range ids {
			if !mediaID(id, 32) || seen[id] {
				return bot.ErrNotFound
			}
			seen[id] = true
			entry, err := s.history.directoryEntry(tx, "members", s.data.Binding, room+"/"+id)
			if err != nil {
				return err
			}
			if id == s.sessionID(s.data.Session.Bootstrap.Username) {
				entry = &directoryEntry{Raw: s.data.Session.Bootstrap.Username, Name: "我"}
			}
			if entry == nil {
				return bot.ErrNotFound
			}
			user, err := s.history.directoryEntry(tx, "users", s.data.Binding, id)
			if err != nil || user == nil {
				return ErrEventHistory
			}
			name := entry.Name
			if user.Named && id != s.sessionID(s.data.Session.Bootstrap.Username) {
				name = user.Name
			}
			users = append(users, string(entry.Raw))
			labels = append(labels, "@"+name+"\u2005")
		}
		return nil
	})
	return users, strings.Join(labels, ""), err
}

func (h *EventHistory) observeHistoryRow(tx *securedb.Tx, s *Service, meta *directoryMeta, row *historyRecord) error {
	if err := h.observeMessage(tx, s, meta, row.Raw); err != nil {
		return err
	}
	owner := s.data.Session.Bootstrap.Username
	sender := row.Raw.Sender
	if group, ok := row.Raw.Group(owner); ok {
		sender = group.Member
	}
	for _, item := range []struct {
		bucket, id, name string
		raw              []byte
	}{
		{"users", row.Event.Sender.ID, row.Event.Sender.Name, sender},
		{"conversations", row.Event.Conversation.ID, row.Event.Conversation.Name, row.Raw.Peer(owner)},
	} {
		if len(item.raw) == 0 || item.name == "" || item.name == string(item.raw) {
			continue
		}
		entry, err := h.directoryEntry(tx, item.bucket, s.data.Binding, item.id)
		if err != nil {
			return err
		}
		if entry == nil {
			continue
		}
		entry.Name, entry.Named = item.name, true
		if err = h.putDirectoryEntry(tx, meta, item.bucket, *entry); err != nil {
			return err
		}
	}
	return nil
}

func (a BotAdapter) Member(account, conversation, user string) (bot.Membership, error) {
	s := a.Service
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.mediaScopeLocked() || s.data.Binding != account {
		return bot.Membership{}, bot.ErrScope
	}
	if s.history == nil {
		room, ok := s.groupRoomsLocked()[conversation]
		if !ok {
			return bot.Membership{}, bot.ErrNotFound
		}
		for _, raw := range s.groupMembersLocked(room) {
			if s.sessionID(raw) == user {
				return bot.Membership{User: bot.User{ID: user, Name: s.groupMemberLabelLocked(room, raw)}, Source: "observed_messages", Complete: false}, nil
			}
		}
		return bot.Membership{}, bot.ErrNotFound
	}
	own := s.sessionID(s.data.Session.Bootstrap.Username)
	result := bot.Membership{Source: "observed_history", Complete: false}
	err := s.history.directoryRead(account, func(tx *securedb.Tx) error {
		room, err := s.history.directoryEntryLimit(tx, "conversations", account, conversation, directoryListBytes)
		if err != nil {
			return err
		}
		if room == nil || !room.Available || room.Kind != "group" {
			return bot.ErrNotFound
		}
		if user == own {
			result.User = bot.User{ID: own, Name: "我"}
			return nil
		}
		member, err := s.history.directoryEntryLimit(tx, "members", account, conversation+"/"+user, directoryListBytes)
		if err != nil {
			return err
		}
		if member == nil {
			return bot.ErrNotFound
		}
		entry, err := s.history.directoryEntryLimit(tx, "users", account, user, directoryListBytes)
		if err != nil {
			return err
		}
		if entry == nil {
			return ErrEventHistory
		}
		name := member.Name
		if entry.Named {
			name = entry.Name
		}
		result.User = bot.User{ID: user, Name: name}
		raw, _ := json.Marshal(result)
		if len(raw) > directoryListBytes {
			return bot.ErrLimit
		}
		return nil
	})
	return result, err
}
