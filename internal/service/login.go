package service

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/JFSAS/wechat-runtime/internal/assets"
	"github.com/JFSAS/wechat-runtime/internal/native"
	"github.com/JFSAS/wechat-runtime/internal/protocol"
	"github.com/JFSAS/wechat-runtime/internal/store"
)

type loginEngine interface {
	Start(context.Context, time.Time) error
	Poll(context.Context, time.Time) error
	Authenticate(context.Context) (*protocol.Session, error)
	Status() protocol.LoginStatus
	Image() ([]byte, error)
	Cancel(string)
}

type loginAttempt struct {
	ID string `json:"id,omitempty"`
	protocol.LoginStatus
	Error           string                    `json:"error_category,omitempty"`
	Diagnostics     protocol.LoginDiagnostics `json:"diagnostics"`
	epoch           uint64
	engine          loginEngine
	image           []byte
	networkFailures int
}

func (s *Service) LoginStatus() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return map[string]any{"id": s.login.ID, "state": s.login.State, "created_at": s.login.CreatedAt, "deadline": s.login.Deadline, "image_ready": len(s.login.image) > 0, "error_category": s.login.Error, "diagnostics": s.login.Diagnostics.Clone()}
}

func (s *Service) LoginImage(id string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if id == "" || id != s.login.ID || len(s.login.image) == 0 {
		return nil, errors.New("login_image_unavailable")
	}
	return bytes.Clone(s.login.image), nil
}

func (s *Service) QueueLogin() (map[string]any, error) {
	id, err := protocol.Random(16)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.mode == "qr" && s.login.ID != "" {
		s.mu.Unlock()
		return s.LoginStatus(), nil
	}
	s.login = loginAttempt{ID: hex.EncodeToString(id), LoginStatus: protocol.LoginStatus{State: "requesting"}, epoch: s.login.epoch + 1}
	s.modeEpoch++
	s.mode, s.state, s.detail = "qr", "logging_in", "正在申请独立登录二维码"
	s.category, s.code = "", 0
	s.fresh, s.reload = false, false
	s.pendingApp = ""
	s.sessionState = "unverified"
	s.beginConnectionLocked("qr", "request_qr")
	if s.cancelRPC != nil {
		s.cancelRPC()
	}
	s.mu.Unlock()
	s.signal()
	return s.LoginStatus(), nil
}

func (s *Service) CancelLogin() {
	s.mu.Lock()
	s.login.epoch++
	s.login.State, s.login.image = "cancelled", nil
	if s.mode == "qr" {
		s.modeEpoch++
		s.mode, s.state, s.detail = "offline", "offline", "扫码登录已取消"
	}
	if s.cancelRPC != nil {
		s.cancelRPC()
	}
	s.mu.Unlock()
	s.signal()
}

func (s *Service) prepareLoginIdentity() (protocol.LoginIdentity, error) {
	path := filepath.Join(s.directory, "login-device.id")
	device, err := store.ReadPrivate(path, 128)
	if errors.Is(err, os.ErrNotExist) {
		s.mu.RLock()
		if s.data != nil && s.data.Session != nil && s.data.LoginSource == "qr" && len(s.data.Session.Bootstrap.DeviceID) == 33 {
			device = bytes.Clone(s.data.Session.Bootstrap.DeviceID)
		}
		s.mu.RUnlock()
		if len(device) == 0 {
			device, err = protocol.GenerateLoginDevice()
		} else {
			err = nil
		}
		if err == nil {
			err = store.Atomic(path, device)
		}
	}
	if err != nil {
		return protocol.LoginIdentity{}, err
	}
	identity, err := protocol.LocalLoginIdentity(device)
	if err != nil {
		return protocol.LoginIdentity{}, err
	}
	s.mu.RLock()
	if s.data != nil && s.data.Session != nil && bytes.Equal(device, s.data.Session.Bootstrap.DeviceID) {
		if s.data.Session.Profile.Version != 0 {
			identity.Profile = s.data.Session.Profile
		}
		if s.data.Session.Identity != nil {
			saved := *s.data.Session.Identity
			identity.Runtime = &saved
			identity.Computer, identity.Description = saved.Computer, saved.Description()
		}
	}
	s.mu.RUnlock()
	if err = identity.Runtime.Validate(); err != nil {
		return protocol.LoginIdentity{}, err
	}
	return identity, nil
}
func (s *Service) nativeLogin() (loginEngine, error) {
	identity, err := s.prepareLoginIdentity()
	if err != nil {
		return nil, err
	}
	roots, err := assets.Files.ReadFile("production-roots.pem")
	if err != nil {
		return nil, err
	}
	rootPath := filepath.Join(s.directory, "runtime", "production-roots.pem")
	if err = store.Atomic(rootPath, roots); err != nil {
		return nil, err
	}
	c := &protocol.Client{Roots: rootPath, Bootstrap: protocol.Bootstrap{DeviceID: identity.DeviceID}, Profile: &identity.Profile, Identity: identity.Runtime}
	return protocol.NewQRLogin(identity, c.QRExchange)
}

// workLogin runs on the existing serial network worker. HTTP handlers only
// queue requests or read public snapshots; they never run a second login.
func (s *Service) workLogin(ctx context.Context) time.Duration {
	s.mu.RLock()
	attempt := s.login
	factory := s.loginFactory
	s.mu.RUnlock()
	if attempt.ID == "" {
		if _, err := s.QueueLogin(); err != nil {
			s.block(err)
		}
		return time.Millisecond
	}
	engine := attempt.engine
	var err error
	if engine == nil {
		// A QR-only start restores its own checkpoint for account binding, but
		// never imports a ticket from the official application's disk.
		if s.data == nil {
			cp, exists, readErr := store.Unique(filepath.Join(s.directory, "checkpoints"))
			if readErr != nil {
				err = readErr
			} else if exists {
				data, readErr := cp.Load()
				if readErr != nil {
					err = readErr
				} else {
					s.mu.Lock()
					s.data, s.checkpoint = data, cp
					s.mu.Unlock()
				}
			}
		}
		if factory != nil {
			if err == nil {
				engine, err = factory()
			}
		} else {
			if err == nil {
				engine, err = s.nativeLogin()
			}
		}
		if err == nil {
			err = engine.Start(ctx, time.Now())
		}
	} else if engine.Status().State == "idle" {
		err = engine.Start(ctx, time.Now())
	} else {
		err = engine.Poll(ctx, time.Now())
	}
	var candidate *protocol.Session
	var result protocol.SyncResult
	var client *protocol.Client
	if err == nil && engine.Status().State == "confirmed" {
		s.mu.RLock()
		if s.data != nil && s.data.Session != nil {
			if scoped, ok := engine.(interface{ ExpectAccount(protocol.Bootstrap) }); ok {
				scoped.ExpectAccount(s.data.Session.Bootstrap)
			}
		}
		s.mu.RUnlock()
		candidate, err = engine.Authenticate(ctx)
		if err == nil {
			client = &protocol.Client{Bootstrap: candidate.Bootstrap, State: candidate, Roots: filepath.Join(s.directory, "runtime", "production-roots.pem")}
			if s.history != nil {
				client.SavedCursor = bytes.Clone(client.State.Cursor)
			}
			if s.loginValidate != nil {
				result, err = s.loginValidate(ctx, client)
			} else {
				result, err = client.Sync(ctx, 2, nil)
			}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.login.epoch != attempt.epoch || s.mode != "qr" {
		if engine != nil {
			engine.Cancel("cancelled")
		}
		return time.Second
	}
	s.login.engine = engine
	if engine != nil {
		s.login.LoginStatus = engine.Status()
		if diagnosed, ok := engine.(interface {
			Diagnostics() protocol.LoginDiagnostics
		}); ok {
			s.login.Diagnostics = diagnosed.Diagnostics()
		}
		if engine.Status().ImageReady {
			if pixels, imageErr := engine.Image(); imageErr == nil {
				s.login.image = pixels
			}
		} else {
			s.login.image = nil
		}
	}
	if err != nil {
		if engine != nil && loginNetworkRetryable(err) && attempt.networkFailures < 4 {
			state := engine.Status().State
			if state == "idle" || state == "waiting_scan" || state == "waiting_confirmation" {
				s.login.networkFailures = attempt.networkFailures + 1
				s.login.Error, s.category = "login_network_retry", "login_network_retry"
				s.detail = "二维码查询连接中断，保留本次二维码并自动重试"
				return time.Duration(1<<s.login.networkFailures) * time.Second
			}
		}
		s.login.State, s.login.Error = "failed", loginErrorCategory(err)
		if engine != nil {
			engine.Cancel("failed")
		}
		s.login.image = nil
		s.mode, s.state, s.category = "blocked", "needs_attention", s.login.Error
		s.detail = "独立扫码登录未完成；查看登录状态后重新申请二维码"
		return time.Second
	}
	s.login.networkFailures = 0
	s.login.Error, s.category = "", ""
	if candidate != nil {
		if err = s.adoptLoginLocked(client, result); err != nil {
			s.login.State, s.login.Error = "failed", loginErrorCategory(err)
			s.mode, s.state, s.category = "blocked", "needs_attention", s.login.Error
			s.detail = "登录完成，但本机保存或账号绑定检查未通过；未开始消息收发"
			return time.Second
		}
		s.login.State, s.login.image = "connected", nil
		s.mode, s.state, s.sessionState = "online", "connected", "validated"
		s.category, s.detail = "", ""
		s.lastSync = time.Now().Unix()
		s.connection.Source, s.connection.Stage = "qr", "connected"
		return time.Millisecond
	}
	state := engine.Status().State
	switch state {
	case "waiting_scan":
		s.detail = "二维码已生成，等待手机扫码"
	case "waiting_confirmation":
		s.detail = "已扫描，等待手机确认登录"
	}
	if state == "cancelled" || state == "expired" || state == "additional_verification" {
		s.mode, s.state = "blocked", "needs_attention"
		s.login.image = nil
		if state == "expired" {
			s.category, s.login.Error = "login_qr_expired", "login_qr_expired"
			s.detail = "二维码已失效，请重新申请后扫描最新二维码"
		}
	}
	if paced, ok := engine.(interface{ PollInterval() time.Duration }); ok && paced.PollInterval() > 0 {
		return paced.PollInterval()
	}
	return 2 * time.Second
}

func loginNetworkRetryable(err error) bool {
	var transport *native.ExchangeError
	if errors.As(err, &transport) {
		return transport.Retryable
	}
	var network net.Error
	if errors.As(err, &network) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var p *protocol.Error
	return errors.As(err, &p) && p.Category == "http_response_failed" && (p.Code == 408 || p.Code == 429 || p.Code >= 500 && p.Code <= 599)
}

func loginErrorCategory(err error) string {
	var p *protocol.Error
	if errors.As(err, &p) {
		return p.Category
	}
	if errors.Is(err, context.Canceled) {
		return "login_cancelled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "login_network_timeout"
	}
	if errors.Is(err, protocol.ErrWire) {
		return "login_protocol_error"
	}
	return "login_not_completed"
}

// Keep the account's cache namespace when an explicit login changes devices.
// A different account must use a separate state directory.
func cacheForLogin(data *store.Data, b protocol.Bootstrap) (*store.Data, error) {
	if !data.ValidBinding() || data.Session.Bootstrap.UIN != b.UIN ||
		!bytes.Equal(data.Session.Bootstrap.Username, b.Username) {
		return nil, &protocol.Error{Category: "account_scope_changed"}
	}
	copyData := *data
	if data.Binding != b.Binding() && len(copyData.BindingDeviceID) == 0 {
		device := data.Session.Bootstrap.DeviceID
		if len(device) < 16 {
			return nil, &protocol.Error{Category: "account_scope_changed"}
		}
		copyData.BindingDeviceID = bytes.Clone(device[:16])
	}
	return &copyData, nil
}

func (s *Service) adoptLoginLocked(client *protocol.Client, result protocol.SyncResult) error {
	if client == nil || client.State == nil || !client.State.Confirmed {
		return protocol.ErrWire
	}
	binding := client.Bootstrap.Binding()
	data := s.data
	if s.data != nil {
		var err error
		data, err = cacheForLogin(s.data, client.Bootstrap)
		if err != nil {
			return err
		}
		// The account is unchanged; a QR device is independent from the
		// official client's device. Preserve the local cache namespace.
		binding = s.data.Binding
	}
	if data == nil {
		key, err := protocol.Random(32)
		if err != nil {
			return err
		}
		id, err := protocol.Random(4)
		if err != nil {
			return err
		}
		data = &store.Data{Schema: 1, Binding: binding, SourceApp: s.source.App, APIKey: key, NextClientID: binary.BigEndian.Uint32(id)%0x7ffffffe + 1, Jobs: map[string]*store.Job{}}
	}
	copyData := *data
	copyData.LoginSource = "qr"
	copyData.SourceApp = s.source.App
	copyData.Session = cloneSession(client.State)
	copyData.SessionInvalid = false
	cp := store.Checkpoint{Directory: filepath.Join(s.directory, "checkpoints"), Binding: binding}
	if s.history != nil {
		return s.adoptHistoryLoginLocked(client, result, &copyData, cp)
	}
	if err := cp.Save(&copyData); err != nil {
		return &protocol.Error{Category: "checkpoint_save_failed"}
	}
	s.data, s.client, s.checkpoint = &copyData, client, cp
	s.ingestLocked(result)
	return s.saveLocked()
}
