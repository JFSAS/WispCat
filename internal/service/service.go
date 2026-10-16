// Package service owns the account state and one serial network worker.
package service

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/JFSAS/wechat-runtime/internal/account"
	"github.com/JFSAS/wechat-runtime/internal/assets"
	"github.com/JFSAS/wechat-runtime/internal/protocol"
	"github.com/JFSAS/wechat-runtime/internal/store"
)

type Service struct {
	mu                            sync.RWMutex
	directory                     string
	source                        account.Source
	pendingApp                    string
	interval                      time.Duration
	data                          *store.Data
	checkpoint                    store.Checkpoint
	client                        *protocol.Client // worker-owned; the HTTP layer sees snapshots only
	state, detail, category, mode string
	sessionState                  string
	lastSync                      int64
	connection                    connectionAttempt
	code                          int32
	fresh, refresh, restored      bool
	reload                        bool
	modeEpoch                     uint64
	refreshState                  string
	calls                         map[string]uint64
	wake                          chan struct{}
	done                          chan struct{}
	cancelRPC                     context.CancelFunc
	persistedSequence             uint64
	mediaDownload                 MediaDownloader
	mediaSend                     MediaSender
	mediaActive                   string
	login                         loginAttempt
	loginFactory                  func() (loginEngine, error)
	loginValidate                 func(context.Context, *protocol.Client) (protocol.SyncResult, error)
	history                       *EventHistory // optional independent runtime persistence
	credential                    *credentialLogin
	credentialAuth                func(context.Context, *protocol.Client) error
	credentialClosed              bool
}

// Reserve a distinct epoch range for each runtime, including instances with
// identical account bindings. A delayed external send cannot cross a switch.
var runtimeGeneration atomic.Uint64

func New(directory string, interval time.Duration, app string) *Service {
	return &Service{modeEpoch: runtimeGeneration.Add(1) << 32, directory: directory, source: account.Source{App: app}, interval: interval, state: "starting", sessionState: "unverified", mode: "online", refreshState: "idle", calls: map[string]uint64{}, wake: make(chan struct{}, 1), done: make(chan struct{})}
}
func cloneSession(s *protocol.Session) *protocol.Session {
	if s == nil {
		return nil
	}
	v := *s
	if s.Identity != nil {
		identity := *s.Identity
		v.Identity = &identity
	}
	v.Cursor = bytes.Clone(s.Cursor)
	v.Cookie = bytes.Clone(s.Cookie)
	v.Bootstrap.AAK = bytes.Clone(s.Bootstrap.AAK)
	return &v
}
func (s *Service) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
func (s *Service) SetMode(mode string, fresh bool) error {
	if mode != "online" && mode != "offline" {
		return errors.New("invalid mode")
	}
	s.mu.Lock()
	s.setModeLocked(mode, fresh)
	s.mu.Unlock()
	s.signal()
	return nil
}
func (s *Service) setModeLocked(mode string, fresh bool) {
	if !fresh {
		s.pendingApp = ""
	}
	s.modeEpoch++
	if s.mode == "qr" {
		s.login.epoch++
		s.login.State, s.login.image = "cancelled", nil
	}
	s.mode = mode
	s.fresh = fresh
	s.reload = mode == "online"
	if mode == "online" {
		s.state = "connecting"
		s.detail = "正在从官方微信磁盘读取登录材料"
		if !fresh {
			s.detail = "正在恢复本项目保存的登录状态"
		}
		s.sessionState = "unverified"
		s.category, s.code = "", 0
		s.connection.Stage, s.connection.DiskRead = "queued", "not_attempted"
		s.connection.Source = "cache"
		if fresh {
			s.connection.Source = "official_disk"
		}
		s.connection.AuthReason, s.connection.TicketMatchesCache = "", nil
		s.connection.NextStep = "等待本次登录结果，无需重复操作。"
		if s.cancelRPC != nil {
			s.cancelRPC()
		}
	} else {
		s.state = "offline"
		s.detail = "仅显示本机保存的记录"
		s.connection.NextStep = "当前只查看缓存；正常启动时会自动恢复，也可以选择重新扫码登录或从磁盘导入登录。"
		if s.cancelRPC != nil {
			s.state = "pausing"
			s.detail = "正在结束当前网络任务，随后仅显示缓存"
			s.cancelRPC()
		}
	}
}

// QueueDiskLogin always imports the selected installation's disk material,
// regardless of how the existing account originally logged in.
func (s *Service) QueueDiskLogin() error { return s.SetMode("online", true) }

func (s *Service) Start(ctx context.Context, mode string) {
	s.mu.Lock()
	s.mode = mode
	s.mu.Unlock()
	go s.run(ctx)
}
func (s *Service) Wait()             { <-s.done }
func (s *Service) count(name string) { s.mu.Lock(); s.calls[name]++; s.mu.Unlock() }
func (s *Service) rpc(ctx context.Context) (context.Context, func()) {
	c, cancel := context.WithTimeout(ctx, 50*time.Second)
	s.mu.Lock()
	s.cancelRPC = cancel
	s.mu.Unlock()
	return c, func() { cancel(); s.mu.Lock(); s.cancelRPC = nil; s.mu.Unlock() }
}
func (s *Service) saveLocked() error {
	if s.data == nil || s.data.Session == nil {
		return nil
	}
	if len(s.data.Session.Original) != 16 || len(s.data.Session.ClientKey) != 24 || len(s.data.Session.ServerKey) != 24 {
		// A first login that has not produced keys must not leave an invalid
		// checkpoint that prevents the next official-login import.
		return nil
	}
	if s.history != nil {
		if err := s.history.persistLocked(s); err != nil {
			return err
		}
		restoreContacts, err := s.history.prepareContactCheckpoint(s.data)
		if err != nil {
			return err
		}
		defer restoreContacts()
	}
	err := s.checkpoint.Save(s.data)
	if err == nil {
		s.persistedSequence = s.data.EventSequence
	}
	return err
}
func (s *Service) snapshotLocked() {
	if s.client != nil && s.client.State != nil && s.data != nil {
		s.data.Session = cloneSession(s.client.State)
	}
}

func (s *Service) saveAuthenticationLocked() error {
	s.snapshotLocked()
	if s.connection.Source == "official_disk" {
		s.data.LoginSource = "official_disk"
	}
	s.data.SessionInvalid = false
	s.sessionState = "unverified"
	return s.saveLocked()
}

func (s *Service) prepare(online, fresh bool) error {
	s.mu.Lock()
	from := "cache"
	if fresh {
		from = "official_disk"
	}
	s.beginConnectionLocked(from, "read_cache")
	s.sessionState = "unverified"
	s.mu.Unlock()
	dir := filepath.Join(s.directory, "checkpoints")
	cp, exists, e := store.Unique(dir)
	if e != nil {
		return e
	}
	var data *store.Data
	if exists {
		data, e = cp.Load()
		if e != nil {
			return e
		}

	}
	s.mu.Lock()
	s.data, s.checkpoint, s.restored = data, cp, exists
	if data != nil {
		s.persistedSequence = data.EventSequence
	}
	s.mu.Unlock()
	s.client = nil
	if online && !fresh && data != nil && data.SessionInvalid {
		s.connectionPhase("cache_validation")
		if data.LoginSource == "qr" {
			return &protocol.Error{Category: "login_required"}
		}
		return &protocol.Error{Category: "authentication_rejected"}
	}
	if online && !fresh && data == nil {
		return &protocol.Error{Category: "login_required"}
	}

	if online && (data == nil || fresh) {
		s.mu.Lock()
		s.connection.Source, s.connection.Stage, s.connection.DiskRead = "official_disk", "read_disk", "reading"
		s.mu.Unlock()
		b, e := s.source.Load()
		if e != nil {
			return e
		}
		s.mu.Lock()
		s.connection.DiskRead = "succeeded"
		if data != nil && data.Session != nil {
			matches := bytes.Equal(data.Session.Bootstrap.AAK, b.AAK)
			s.connection.TicketMatchesCache = &matches
		}
		s.mu.Unlock()
		if data != nil {
			data, e = cacheForLogin(data, b)
			if e != nil {
				return e
			}
		}
		if data == nil {
			key, e := protocol.Random(32)
			if e != nil {
				return e
			}
			id, e := protocol.Random(4)
			if e != nil {
				return e
			}
			data = &store.Data{Schema: 1, Binding: b.Binding(), APIKey: key, NextClientID: binary.BigEndian.Uint32(id)%0x7ffffffe + 1, Jobs: map[string]*store.Job{}, Session: &protocol.Session{Bootstrap: b}}
			cp = store.Checkpoint{Directory: dir, Binding: data.Binding}
		}
		data.SourceApp = s.source.App
		savedCursor := bytes.Clone(data.Session.Cursor)
		s.client = &protocol.Client{Bootstrap: b, SavedCursor: savedCursor}
	} else if data != nil && data.Session != nil {
		s.client = &protocol.Client{Bootstrap: data.Session.Bootstrap, State: cloneSession(data.Session)}
	}
	if online && s.client == nil {
		return account.ErrLogin
	}
	roots := filepath.Join(s.directory, "runtime", "production-roots.pem")
	if online {
		b, e := assets.Files.ReadFile("production-roots.pem")
		if e != nil {
			return e
		}
		if e = store.Atomic(roots, b); e != nil {
			return e
		}
		s.client.Roots = roots
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = data
	s.checkpoint = cp
	s.restored = exists
	s.sessionState = "unverified"
	s.connection.Stage = "ready"
	if err := serializedContactCheckpoint(data); err != nil {
		return err
	}
	if err := contactCheckpointDependency(data, s.history); err != nil {
		return err
	}
	if s.history != nil && data != nil {
		if err := s.history.restoreRuntimeLocked(s); err != nil {
			return err
		}
	}
	return nil
}
func (s *Service) block(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blockLocked(err)
}

func (s *Service) blockLocked(err error) {
	s.mode = "blocked"
	s.state = "needs_attention"
	s.detail = "连接未完成，缓存保留；可以选择重新扫码登录、从磁盘导入登录或离线查看"
	s.category = "local_or_protocol_error"
	var p *protocol.Error
	if errors.As(err, &p) {
		s.category = p.Category
		s.code = p.Code
		if p.Category == "session_expired" || p.Category == "authentication_rejected" || p.Category == "session_missing" {
			s.state = "disconnected"
			s.sessionState = "invalid"
			s.detail = "缓存登录状态已失效；请选择重新扫码登录或从磁盘导入登录"
			if s.data != nil {
				s.data.SessionInvalid = true
				if e := s.saveLocked(); e != nil {
					s.detail += "；缓存失效标记未能保存"
				}
			}
		}
		if p.Category == "account_scope_changed" {
			s.state = "scope_changed"
			s.detail = "账号已变化，请选择对应账号的独立缓存"
		}
		if p.Category == "source_scope_changed" {
			s.state = "scope_changed"
			s.detail = "缓存所属微信实例不同；请选择对应缓存目录或从所选实例磁盘导入登录"
		}
	}
	if errors.As(err, &p) && p.Category == "login_required" {
		s.state, s.category = "needs_login", "login_required"
		s.detail = "没有可用的缓存登录状态，请选择重新扫码登录或从磁盘导入登录"
	}
	if errors.Is(err, account.ErrLogin) {
		s.state = "needs_login"
		s.detail = "官方磁盘没有可用登录材料；可重新扫码登录，或登录所选微信后再从磁盘导入登录"
		s.category = "login_required"
	}
	if errors.Is(err, account.ErrVersion) {
		s.state = "needs_setup"
		s.detail = "当前仅支持已确认的微信4.1.13（269602）"
		s.category = "unsupported_version"
	}
	if errors.Is(err, account.ErrDiskAmbiguous) {
		s.category = "ambiguous_disk_source"
		s.detail = "找到多个官方磁盘配置，无法确定凭证来源"
	}
	if errors.Is(err, store.ErrLegacyCheckpoint) {
		s.state = "needs_setup"
		s.detail = "旧缓存依赖钥匙串，已保留；请先备份checkpoints目录或使用新的私有目录"
		s.category = "legacy_checkpoint"
	}
	if errors.Is(err, store.ErrKeyMissing) {
		s.detail = "缓存的.key文件缺失，请恢复原文件；现有缓存不会被覆盖"
		s.category = "checkpoint_key_missing"
	}
	s.connectionFailureLocked(err)
	if s.state == "needs_login" {
		s.connection.NextStep = "选择重新扫码登录，或从磁盘导入登录。缓存会自动检测，无需手动恢复。"
		if runtime.GOOS != "darwin" {
			s.detail = "没有可用的缓存登录状态，请扫码登录或迁移已有项目缓存。"
			s.connection.NextStep = "点击重新扫码登录；也可以停止服务、迁移完整项目缓存后重新启动。"
		}
	}
}
func (s *Service) run(ctx context.Context) {
	defer close(s.done)
	defer s.discardCredentialLogin()
	prepared := false
	recovery := sessionRecovery{}
	scene := uint32(2)
	for ctx.Err() == nil {
		if s.credentialLoginPending() {
			s.workCredentialLogin(ctx)
			prepared = true
			recovery = sessionRecovery{}
			continue
		}
		s.mu.Lock()
		if s.applyPendingAppLocked() {
			prepared = false
		}
		mode, fresh, reload, epoch := s.mode, s.fresh, s.reload, s.modeEpoch
		s.mu.Unlock()
		if mode == "qr" {
			call, done := s.rpc(ctx)
			delay := s.workLogin(call)
			done()
			prepared = s.client != nil
			select {
			case <-ctx.Done():
			case <-s.wake:
			case <-time.After(delay):
			}
			continue
		}
		if !prepared || fresh || reload {
			// An explicit login or startup mode change starts a bounded attempt.
			recovery = sessionRecovery{}
			err := s.prepare(mode == "online", fresh)
			prepared = true
			s.mu.Lock()
			if s.modeEpoch != epoch {
				s.mu.Unlock()
				continue
			}
			s.fresh = false
			s.reload = false
			if err != nil {
				s.blockLocked(err)
			}
			s.mu.Unlock()
			continue
		}
		if mode != "online" {
			if mode == "offline" {
				s.mu.Lock()
				s.state = "offline"
				s.detail = "仅显示本机保存的记录"
				s.mu.Unlock()
			}
			select {
			case <-ctx.Done():
			case <-s.wake:
			}
			continue
		}
		if s.client == nil {
			s.block(account.ErrLogin)
			continue
		}
		call, done := s.rpc(ctx)
		var err error
		if s.client.State == nil || recovery.reauth {
			s.connectionPhase("authenticate")
			s.count("auth")
			err = s.client.Authenticate(call)
			if err == nil {
				recovery.reauth = false
				s.mu.Lock()
				if s.modeEpoch != epoch {
					s.client = nil
					prepared = false
					s.mu.Unlock()
					done()
					continue
				}
				err = s.saveAuthenticationLocked()
				s.mu.Unlock()
				if err != nil {
					err = &protocol.Error{Category: "checkpoint_save_failed"}
				}
			}
		}
		if err == nil {
			err = s.sendQueued(call)
		}
		if err == nil {
			err = s.refreshContacts(call)
		}
		if err == nil {
			err = s.downloadQueued(call)
		}
		var result protocol.SyncResult
		if err == nil {
			s.mu.RLock()
			validating := s.state != "connected"
			s.mu.RUnlock()
			if validating {
				s.connectionPhase("sync")
			}
			s.count("sync")
			result, err = s.client.Sync(call, scene, nil)
		}
		done()
		if ctx.Err() != nil {
			break
		}
		s.mu.RLock()
		stillOnline := s.mode == "online" && !s.fresh && s.modeEpoch == epoch
		s.mu.RUnlock()
		if !stillOnline {
			continue
		}
		if err != nil {
			delay, retry := s.recoverFailure(err, &recovery)
			if !retry {
				continue
			}
			select {
			case <-ctx.Done():
			case <-s.wake:
			case <-time.After(delay):
			}
			continue
		}
		s.mu.Lock()
		if s.modeEpoch != epoch {
			s.mu.Unlock()
			continue
		}
		old := bytes.Clone(s.data.Session.Cursor)
		s.snapshotLocked()
		s.ingestLocked(result)
		err = s.saveLocked()
		if err == nil {
			s.state = "connected"
			s.sessionState = "validated"
			s.lastSync = time.Now().Unix()
			s.connection.Stage, s.connection.NextStep = "connected", ""
			s.detail = ""
			s.category = ""
			s.code = 0
		}
		mergedChanged := !bytes.Equal(old, s.data.Session.Cursor)
		s.mu.Unlock()
		if err != nil {
			s.block(err)
			continue
		}
		recovery = sessionRecovery{}
		scene = 2
		if result.Continue&7 != 0 && mergedChanged {
			scene = 6
		}
		delay := s.interval
		if scene == 6 {
			delay = 500 * time.Millisecond
		}
		select {
		case <-ctx.Done():
		case <-s.wake:
		case <-time.After(delay):
		}
	}
	s.mu.Lock()
	s.snapshotLocked()
	if e := s.saveLocked(); e != nil {
		s.category = "checkpoint_save_failed"
	}
	s.state = "stopped"
	s.mu.Unlock()
}
func messageKey(m protocol.Message, owner []byte) string {
	peer := m.Peer(owner)
	var b []byte
	if m.ServerID != 0 {
		b = protocol.Join(peer, []byte("\x00id\x00"), []byte(strconv.FormatUint(m.ServerID, 10)))
	} else {
		b = protocol.Join(peer, []byte("\x00fallback\x00"), m.Sender, []byte{0}, m.Recipient, []byte{0}, []byte(strconv.FormatUint(uint64(m.Type), 10)), []byte{0}, []byte(strconv.FormatUint(uint64(m.Timestamp), 10)), []byte{0}, m.Content)
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func (s *Service) updateContactsLocked(changes []protocol.Contact) {
	if s.data == nil {
		return
	}
	if s.history != nil {
		s.history.stageContactsLocked(s, changes)
	}
	contacts := map[string]protocol.Contact{}
	for _, c := range s.data.Contacts {
		contacts[string(c.Username)] = c
	}
	unavailable := map[string]bool{}
	for _, u := range s.data.Unavailable {
		unavailable[string(u)] = true
	}
	for _, c := range changes {
		key := string(c.Username)
		if key == string(s.client.Bootstrap.Username) {
			continue
		}
		if c.Deleted {
			delete(contacts, key)
			unavailable[key] = true
			var pinned [][]byte
			for _, p := range s.data.Pinned {
				if string(p) != key {
					pinned = append(pinned, p)
				}
			}
			s.data.Pinned = pinned
			continue
		}
		delete(unavailable, key)
		old := contacts[key]
		if c.Nickname == nil {
			c.Nickname = old.Nickname
		}
		if c.Remark == nil {
			c.Remark = old.Remark
		}
		contacts[key] = c
	}
	s.data.Contacts = nil
	keys := make([]string, 0, len(contacts))
	for key := range contacts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, k := range keys {
		s.data.Contacts = append(s.data.Contacts, contacts[k])
	}
	s.data.Unavailable = nil
	for u := range unavailable {
		s.data.Unavailable = append(s.data.Unavailable, []byte(u))
	}
}
func (s *Service) ingestLocked(r protocol.SyncResult) {
	s.updateContactsLocked(r.Contacts)
	seen := map[string]bool{}
	owner := s.client.Bootstrap.Username
	for _, m := range s.data.Messages {
		seen[messageKey(m, owner)] = true
	}
	for _, m := range r.Messages {
		key := messageKey(m, owner)
		if s.history != nil {
			s.history.stageRawLocked(s, m, !seen[key])
		}
		if !seen[key] {
			s.data.Messages = append(s.data.Messages, m)
			seen[key] = true
			s.appendEventLocked(key, m)
		}
		for _, job := range s.data.Jobs {
			if job.Media != nil && m.ServerID != 0 && job.ServerID == m.ServerID && job.Recipient == string(m.Peer(owner)) {
				job.Status = "echo_observed"
				continue
			}
			if m.ServerID != 0 && isOwnMessage(m, owner) && job.ServerID == m.ServerID && job.Recipient == string(m.Peer(owner)) && job.Content == string(normalizedMessage(m, owner).Content) && m.Type == 1 {
				job.Status = "echo_observed"
			}
		}
	}
	if len(s.data.Messages) > 10000 {
		s.data.Messages = s.data.Messages[len(s.data.Messages)-10000:]
	}
}
func (s *Service) sessionID(peer []byte) string {
	m := hmac.New(sha256.New, s.data.APIKey)
	m.Write(peer)
	return hex.EncodeToString(m.Sum(nil)[:16])
}
func (s *Service) peersLocked() map[string][]byte {
	r := map[string][]byte{}
	if s.data == nil || s.state == "scope_changed" {
		return r
	}
	r[s.sessionID([]byte("filehelper"))] = []byte("filehelper")
	for _, p := range s.data.Pinned {
		r[s.sessionID(p)] = p
	}
	for _, contact := range s.data.Contacts {
		if !contact.Deleted && protocol.IsGroup(contact.Username) {
			r[s.sessionID(contact.Username)] = contact.Username
		}
	}
	owner := s.data.Session
	if owner == nil {
		return r
	}
	for _, m := range s.data.Messages {
		if _, supported := messageView(m); supported {
			peer := m.Peer(owner.Bootstrap.Username)
			r[s.sessionID(peer)] = peer
		}
	}
	for _, u := range s.data.Unavailable {
		delete(r, s.sessionID(u))
	}
	return r
}
func (s *Service) labelLocked(peer []byte) string {
	if string(peer) == "filehelper" {
		return "文件传输助手"
	}
	for _, c := range s.data.Contacts {
		if bytes.Equal(c.Username, peer) {
			return c.Label()
		}
	}
	return string(peer)
}
func (s *Service) Sessions() []map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r := []map[string]any{}
	for id, peer := range s.peersLocked() {
		count := 0
		preview := ""
		for _, m := range s.data.Messages {
			if view, supported := messageView(m); supported && bytes.Equal(m.Peer(s.data.Session.Bootstrap.Username), peer) {
				count++
				text := []rune(view["text"].(string))
				if len(text) > 120 {
					text = text[:120]
				}
				preview = string(text)
			}
		}
		row := map[string]any{"id": id, "label": s.labelLocked(peer), "message_count": count, "preview": preview, "kind": "private"}
		if protocol.IsGroup(peer) {
			row["kind"] = "group"
		}
		r = append(r, row)
	}
	sort.Slice(r, func(i, j int) bool { return r[i]["label"].(string) < r[j]["label"].(string) })
	return r
}
func (s *Service) Messages(id string) ([]map[string]any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	peer, ok := s.peersLocked()[id]
	if !ok {
		return nil, errors.New("session unavailable")
	}
	r := []map[string]any{}
	owner := s.data.Session.Bootstrap.Username
	seenJobs := map[string]bool{}
	for _, m := range s.data.Messages {
		view, supported := messageView(m)
		if !supported || !bytes.Equal(m.Peer(owner), peer) {
			continue
		}
		var jobID, status any
		for key, job := range s.data.Jobs {
			if job.ServerID != 0 && job.ServerID == m.ServerID && job.Recipient == string(peer) {
				jobID = key
				status = job.Status
				seenJobs[key] = true
				break
			}
		}
		view["id"], view["outgoing"], view["job_id"], view["send_status"] = messageKey(m, owner), bytes.Equal(m.Sender, owner), jobID, status
		if group, ok := m.Group(owner); ok {
			view["group"] = true
			view["outgoing"] = bytes.Equal(group.Member, owner)
			view["sender_label"] = s.groupMemberLabelLocked(group.Room, group.Member)
			if len(group.Member) > 0 {
				view["member_id"] = s.groupMemberID(group.Room, group.Member)
			}
			for _, mention := range group.Mentions {
				if mention == string(owner) {
					view["mentioned_self"] = true
				}
			}
		}
		r = append(r, view)
	}
	// Show the encrypted journal after browser reload, including queued,
	// failed and unknown outcomes. These are explicitly local outgoing rows.
	var pending []string
	for id, job := range s.data.Jobs {
		if job.Recipient == string(peer) && !seenJobs[id] {
			pending = append(pending, id)
		}
	}
	sort.Slice(pending, func(i, j int) bool { return s.data.Jobs[pending[i]].CreateTime < s.data.Jobs[pending[j]].CreateTime })
	for _, id := range pending {
		job := s.data.Jobs[id]
		view := map[string]any{"id": "job:" + id, "text": job.Content, "outgoing": true, "job_id": id, "send_status": job.Status}
		if job.Media != nil {
			label := "[文件] "
			if job.Kind == "image" {
				label = "[图片] "
			}
			view["text"], view["kind"] = label+job.Media.Name, job.Kind
			view["media"] = map[string]any{"kind": job.Kind, "name": job.Media.Name, "size_bytes": job.Media.Size, "local_outgoing": true}
		}
		r = append(r, view)
	}
	if len(r) > 1000 {
		r = r[len(r)-1000:]
	}
	return r, nil
}
func (s *Service) Contacts(query string) (map[string]any, error) {
	if len([]rune(query)) > 200 {
		return nil, errors.New("invalid query")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := []map[string]any{}
	q := strings.ToLower(strings.TrimSpace(query))
	if s.data != nil && s.state != "scope_changed" {
		for _, c := range s.data.Contacts {
			nick, remark := "", ""
			if c.Nickname != nil {
				nick = *c.Nickname
			}
			if c.Remark != nil {
				remark = *c.Remark
			}
			if q != "" && !strings.Contains(strings.ToLower(nick), q) && !strings.Contains(strings.ToLower(remark), q) && !strings.Contains(strings.ToLower(string(c.Username)), q) {
				continue
			}
			items = append(items, map[string]any{"id": s.sessionID(c.Username), "label": c.Label(), "nickname": nick, "remark": remark})
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i]["label"].(string) < items[j]["label"].(string) })
	count := len(items)
	if count > 50 {
		items = items[:50]
	}
	return map[string]any{"items": items, "total": count, "truncated": count > 50}, nil
}
func (s *Service) OpenSession(id string) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil || s.state == "scope_changed" {
		return nil, errors.New("account unavailable")
	}
	for _, c := range s.data.Contacts {
		if s.sessionID(c.Username) != id {
			continue
		}
		present := false
		for _, p := range s.data.Pinned {
			if bytes.Equal(p, c.Username) {
				present = true
			}
		}
		if !present {
			s.data.Pinned = append(s.data.Pinned, c.Username)
		}
		if e := s.saveLocked(); e != nil {
			return nil, e
		}
		return map[string]any{"id": id, "label": c.Label()}, nil
	}
	return nil, errors.New("contact unavailable")
}
func (s *Service) Queue(id, text string) (string, error) { return s.QueueWithMentions(id, text, nil) }
func (s *Service) QueueWithMentions(id, text string, mentions []string) (string, error) {
	if strings.TrimSpace(text) == "" || len(text) > 4096 || !utf8.ValidString(text) {
		return "", errors.New("invalid text")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != "connected" || s.mode != "online" || s.data == nil {
		return "", errors.New("backend not connected")
	}
	peer, ok := s.peersLocked()[id]
	if !ok {
		return "", errors.New("session unavailable")
	}
	users, prefix, e := s.resolveMentionsLocked(peer, mentions)
	if e != nil {
		return "", e
	}
	source, e := protocol.MentionSource(users)
	if e != nil {
		return "", e
	}
	text = prefix + text
	if len(text) > 4096 {
		return "", errors.New("正文加@标签后超过4096字节")
	}
	if len(s.data.Jobs) >= 10000 {
		return "", errors.New("send journal exceeds bound")
	}
	random, e := protocol.Random(16)
	if e != nil {
		return "", e
	}
	jobID := hex.EncodeToString(random)
	clientID := s.data.NextClientID
	if clientID == 0 {
		clientID = 1
	}
	s.data.NextClientID = clientID%0x7fffffff + 1
	s.data.Jobs[jobID] = &store.Job{Recipient: string(peer), Content: text, Source: source, ClientID: clientID, CreateTime: uint32(time.Now().Unix()), Status: "queued"}
	if e = s.saveLocked(); e != nil {
		delete(s.data.Jobs, jobID)
		return "", e
	}
	s.signal()
	return jobID, nil
}
func (s *Service) Jobs() []map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r := []map[string]any{}
	if s.data != nil {
		for id, job := range s.data.Jobs {
			r = append(r, sendJobView(id, job))
		}
	}
	return r
}
func (s *Service) Status() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	contacts, messages, cursor := 0, 0, 0
	generation, authenticatedAt := uint64(0), int64(0)
	if s.data != nil && s.state != "scope_changed" {
		contacts = len(s.data.Contacts)
		messages = len(s.data.Messages)
		if s.data.Session != nil {
			cursor = len(s.data.Session.Cursor)
			generation, authenticatedAt = s.data.Session.AuthGeneration, s.data.Session.AuthenticatedAt
		}
	}
	return map[string]any{"auth_generation": generation, "authenticated_at": authenticatedAt, "login_source": s.connection.Source, "state": s.state, "wechat_app": s.source.App, "selected_wechat_app": s.selectedAppLocked(), "detail": s.detail, "mode": s.mode, "error_category": s.category, "error_code": s.code, "session_state": s.sessionState, "last_sync_at": s.lastSync, "connection": s.connection, "restored_from_checkpoint": s.restored, "contact_count": contacts, "auth_calls": s.calls["auth"], "sync_calls": s.calls["sync"], "send_calls": s.calls["send"], "contact_list_calls": s.calls["list"], "contact_brief_calls": s.calls["brief"], "metrics": map[string]any{"unique_messages": messages, "sync_cursor_bytes": cursor}, "contact_refresh": map[string]any{"state": s.refreshState}}
}
func (s *Service) Refresh() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != "connected" || s.mode != "online" {
		return errors.New("backend not connected")
	}
	if s.refreshState == "queued" || s.refreshState == "running" {
		return nil
	}
	s.refresh = true
	s.refreshState = "queued"
	s.signal()
	return nil
}
func (s *Service) sendQueued(ctx context.Context) error {
	s.mu.RLock()
	var ids []string
	for id, job := range s.data.Jobs {
		if job.Status == "queued" && !legacyDelivery(job) {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		a, b := s.data.Jobs[ids[i]], s.data.Jobs[ids[j]]
		if a.CreateTime != b.CreateTime {
			return a.CreateTime < b.CreateTime
		}
		return a.ClientID < b.ClientID
	})
	s.mu.RUnlock()
	for _, id := range ids {
		if e := ctx.Err(); e != nil {
			return e
		}
		s.mu.RLock()
		isMedia := s.data.Jobs[id] != nil && s.data.Jobs[id].Media != nil
		s.mu.RUnlock()
		if isMedia {
			if e := s.sendMediaQueued(ctx, id); e != nil {
				return e
			}
			continue
		}
		s.mu.Lock()
		job := s.data.Jobs[id]
		if job == nil || job.Status != "queued" {
			s.mu.Unlock()
			continue
		}
		if s.deliveryCancelledLocked(job) {
			job.Status = "cancelled"
			err := s.saveLocked()
			s.mu.Unlock()
			if err != nil {
				return err
			}
			continue
		}
		unavailable := false
		for _, u := range s.data.Unavailable {
			if string(u) == job.Recipient {
				unavailable = true
			}
		}
		if unavailable {
			job.Status = "failed"
			reason := "contact_unavailable"
			job.ErrorCategory = &reason
			if e := s.saveLocked(); e != nil {
				s.mu.Unlock()
				return e
			}
			s.mu.Unlock()
			continue
		}
		job.Status = "sending"
		job.Attempts++
		if e := s.saveLocked(); e != nil {
			s.mu.Unlock()
			return e
		}
		recipient, content, when, clientID, source := job.Recipient, job.Content, job.CreateTime, job.ClientID, job.Source
		s.mu.Unlock()
		s.count("send")
		receipt, e := s.client.SendWithSource(ctx, recipient, content, when, clientID, source)
		s.mu.Lock()
		if e != nil {
			job.Status = "unknown"
			reason := "send_outcome_unknown"
			var p *protocol.Error
			if errors.As(e, &p) && p.Category == "send_item_rejected" {
				job.Status = "failed"
				reason = p.Category
			}
			job.ErrorCategory = &reason
		} else {
			job.Status = "accepted"
			job.ServerID = receipt.ServerID
			message := protocol.Message{Sender: s.client.Bootstrap.Username, Recipient: []byte(recipient), Content: []byte(content), Type: 1, Timestamp: when, ServerID: receipt.ServerID, Source: []byte(source)}
			key := messageKey(message, s.client.Bootstrap.Username)
			exists := false
			for _, old := range s.data.Messages {
				if messageKey(old, s.client.Bootstrap.Username) == key {
					exists = true
					break
				}
			}
			if !exists {
				s.data.Messages = append(s.data.Messages, message)
			}

		}
		s.snapshotLocked()
		saveErr := s.saveLocked()
		s.mu.Unlock()
		if saveErr != nil {
			return saveErr
		}
		if e != nil {
			var p *protocol.Error
			if errors.As(e, &p) && p.Category == "send_item_rejected" {
				continue
			}
			return e
		}
	}
	return nil
}
func (s *Service) refreshContacts(ctx context.Context) (err error) {
	s.mu.Lock()
	requested := s.refresh
	s.refresh = false
	s.mu.Unlock()
	if !requested {
		return nil
	}
	s.mu.Lock()
	s.refreshState = "running"
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if err != nil {
			s.refreshState = "failed"
		} else {
			s.refreshState = "completed"
		}
	}()
	a, b := uint32(0), uint32(0)
	names := map[string]bool{}
	complete := false
	for page := 0; page < 16; page++ {
		if e := ctx.Err(); e != nil {
			return e
		}
		s.count("list")
		r, e := s.client.List(ctx, a, b)
		if e != nil {
			return e
		}
		for _, u := range r.Usernames {
			names[string(u)] = true
		}
		if r.Continue == 0 {
			complete = true
			break
		}
		if r.CursorA == a && r.CursorB == b {
			return protocol.ErrWire
		}
		a, b = r.CursorA, r.CursorB
	}
	if !complete {
		return errors.New("contact list exceeds page bound")
	}
	var identities []protocol.Contact
	for name := range names {
		identities = append(identities, protocol.Contact{Username: []byte(name)})
	}
	s.mu.Lock()
	s.updateContactsLocked(identities)
	s.snapshotLocked()
	saveErr := s.saveLocked()
	s.mu.Unlock()
	if saveErr != nil {
		return saveErr
	}
	var ordered []string
	for name := range names {
		if name != string(s.client.Bootstrap.Username) {
			ordered = append(ordered, name)
		}
	}
	sort.Strings(ordered)
	if len(ordered) > 10240 {
		return errors.New("contact list exceeds profile bound")
	}
	for at := 0; at < len(ordered); at += 80 {
		if e := ctx.Err(); e != nil {
			return e
		}
		end := at + 80
		if end > len(ordered) {
			end = len(ordered)
		}
		var batch [][]byte
		for _, name := range ordered[at:end] {
			batch = append(batch, []byte(name))
		}
		s.count("brief")
		rows, e := s.client.Brief(ctx, batch)
		if e != nil {
			return e
		}
		var updates []protocol.Contact
		for _, r := range rows {
			if r.Contact != nil {
				updates = append(updates, *r.Contact)
			}
		}
		s.mu.Lock()
		s.updateContactsLocked(updates)
		s.snapshotLocked()
		e = s.saveLocked()
		s.mu.Unlock()
		if e != nil {
			return e
		}
	}
	return nil
}
func DefaultDirectory() string {
	if v := os.Getenv("REA_STATE_DIR"); v != "" {
		return v
	}
	cwd, e := os.Getwd()
	if e == nil {
		if _, e = os.Stat(filepath.Join(cwd, "go.mod")); e == nil {
			return filepath.Join(cwd, ".private-state")
		}
	}
	home, e := os.UserHomeDir()
	if e != nil {
		return ".private-state"
	}
	if runtime.GOOS == "darwin" {
		return filepath.Join(home, "Library", "Application Support", "WeChatProtocolLab")
	}
	if state := os.Getenv("XDG_STATE_HOME"); filepath.IsAbs(state) {
		return filepath.Join(state, "wechat-protocol-lab")
	}
	return filepath.Join(home, ".local", "state", "wechat-protocol-lab")
}
