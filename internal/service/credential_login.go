package service

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"path/filepath"
	"time"

	"github.com/JFSAS/wechat-runtime/internal/assets"
	"github.com/JFSAS/wechat-runtime/internal/credentials"
	"github.com/JFSAS/wechat-runtime/internal/protocol"
	"github.com/JFSAS/wechat-runtime/internal/store"
)

type credentialLogin struct{ bootstrap protocol.Bootstrap }

func cloneBootstrap(b protocol.Bootstrap) protocol.Bootstrap {
	b.Username, b.DeviceID = bytes.Clone(b.Username), bytes.Clone(b.DeviceID)
	b.AAK, b.First, b.Second = bytes.Clone(b.AAK), bytes.Clone(b.First), bytes.Clone(b.Second)
	b.Cookie, b.KeyInfo = bytes.Clone(b.Cookie), bytes.Clone(b.KeyInfo)
	return b
}

// QueueCredentialLogin admits one import. Only the existing serial worker may
// authenticate, validate the first Sync and replace the private checkpoint.
func (s *Service) QueueCredentialLogin(b protocol.Bootstrap) error {
	if err := credentials.Validate(b); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.credentialClosed {
		return errors.New("runtime_stopping")
	}
	select {
	case <-s.done:
		return errors.New("runtime_stopping")
	default:
	}
	if s.mode == "credential" {
		return errors.New("credential_login_in_progress")
	}
	s.credential = &credentialLogin{bootstrap: cloneBootstrap(b)}
	s.login.epoch++
	s.login.State, s.login.image = "cancelled", nil
	s.modeEpoch++
	s.mode, s.state, s.detail = "credential", "logging_in", "凭证文件已接收，等待服务器鉴权"
	s.sessionState, s.category, s.code = "unverified", "", 0
	s.pendingApp, s.fresh, s.reload = "", false, false
	s.beginConnectionLocked("credential_file", "queued")
	s.connection.NextStep = "等待鉴权与首次同步；上传成功尚不代表已登录。"
	if s.cancelRPC != nil {
		s.cancelRPC()
	}
	s.signal()
	return nil
}

func (s *Service) credentialLoginPending() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mode != "credential" {
		s.credential = nil
	}
	return s.credential != nil
}

func (s *Service) discardCredentialLogin() {
	s.mu.Lock()
	s.credential = nil
	s.credentialClosed = true
	s.mu.Unlock()
}

func (s *Service) credentialCurrent(epoch uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mode == "credential" && s.modeEpoch == epoch
}

func (s *Service) workCredentialLogin(ctx context.Context) {
	s.mu.Lock()
	request, epoch := s.credential, s.modeEpoch
	s.credential = nil
	if request == nil || s.mode != "credential" {
		s.mu.Unlock()
		return
	}
	// Capture the request and install its cancellation handle together. An
	// earlier mode switch must not cancel the next request's fresh RPC context.
	ctx, cancel := context.WithTimeout(ctx, 50*time.Second)
	s.cancelRPC = cancel
	defer func() {
		cancel()
		s.mu.Lock()
		s.cancelRPC = nil
		s.mu.Unlock()
	}()
	loadCache := s.data == nil
	s.mu.Unlock()
	var err error
	if loadCache {
		err = s.prepare(false, false)
	}
	if !s.credentialCurrent(epoch) {
		return
	}
	s.mu.Lock()
	if s.mode != "credential" || s.modeEpoch != epoch {
		s.mu.Unlock()
		return
	}
	s.beginConnectionLocked("credential_file", "authenticate")
	s.detail = "凭证文件已解密，正在请求服务器鉴权"
	var cursor []byte
	if err == nil && s.data != nil {
		_, err = cacheForLogin(s.data, request.bootstrap)
		if err == nil {
			cursor = bytes.Clone(s.data.Session.Cursor)
		}
	}
	s.mu.Unlock()
	client := &protocol.Client{Bootstrap: request.bootstrap, SavedCursor: cursor, Roots: filepath.Join(s.directory, "runtime", "production-roots.pem")}
	if err == nil {
		var roots []byte
		roots, err = assets.Files.ReadFile("production-roots.pem")
		if err == nil {
			err = store.Atomic(client.Roots, roots)
		}
	}
	if err == nil {
		err = ctx.Err()
	}
	if err == nil && s.credentialCurrent(epoch) {
		s.count("auth")
		if s.credentialAuth != nil {
			err = s.credentialAuth(ctx, client)
		} else {
			err = client.Authenticate(ctx)
		}
	}
	if !s.credentialCurrent(epoch) {
		return
	}
	var result protocol.SyncResult
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		if client.State == nil {
			err = protocol.ErrWire
		} else {
			s.mu.Lock()
			if s.mode != "credential" || s.modeEpoch != epoch {
				s.mu.Unlock()
				return
			}
			s.connection.Stage, s.detail = "sync", "服务器鉴权已完成，正在验证首次同步"
			s.mu.Unlock()
			client.SavedCursor = bytes.Clone(client.State.Cursor)
			s.count("sync")
			if s.loginValidate != nil {
				result, err = s.loginValidate(ctx, client)
			} else {
				result, err = client.Sync(ctx, 2, nil)
			}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mode != "credential" || s.modeEpoch != epoch {
		return
	}
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		err = s.adoptCredentialLocked(client, result)
	}
	if err != nil {
		s.mode, s.state, s.category = "blocked", "needs_attention", loginErrorCategory(err)
		s.connection.Stage = "failed"
		s.detail = "凭证文件登录未完成；尚未建立可用的独立会话"
		s.connection.NextStep = "查看错误类别；确认 Mac 微信可用后重新提取，或使用二维码登录。"
		if s.category == "account_scope_changed" {
			s.detail = "文件中的账号与当前实例缓存不同，本次未请求服务器鉴权"
			s.connection.NextStep = "请为文件中的账号创建独立账号实例，再上传文件。"
		}
		return
	}
	s.mode, s.state, s.sessionState = "online", "connected", "validated"
	s.detail, s.category, s.code = "", "", 0
	s.lastSync = time.Now().Unix()
	s.connection.Stage, s.connection.NextStep = "connected", ""
}

func (s *Service) adoptCredentialLocked(client *protocol.Client, result protocol.SyncResult) error {
	if client == nil || client.State == nil || !client.State.Confirmed {
		return protocol.ErrWire
	}
	data, binding := s.data, client.Bootstrap.Binding()
	if data != nil {
		var err error
		data, err = cacheForLogin(data, client.Bootstrap)
		if err != nil {
			return err
		}
		binding = data.Binding
	} else {
		key, err := protocol.Random(32)
		if err != nil {
			return err
		}
		id, err := protocol.Random(4)
		if err != nil {
			return err
		}
		data = &store.Data{Schema: 1, Binding: binding, APIKey: key, NextClientID: binary.BigEndian.Uint32(id)%0x7ffffffe + 1, Jobs: map[string]*store.Job{}}
	}
	candidate := *data
	candidate.SourceApp, candidate.LoginSource = s.source.App, "credential_file"
	candidate.Session, candidate.SessionInvalid = cloneSession(client.State), false
	cp := store.Checkpoint{Directory: filepath.Join(s.directory, "checkpoints"), Binding: binding}
	if s.history != nil {
		return s.adoptHistoryLoginLocked(client, result, &candidate, cp)
	}
	if err := cp.Save(&candidate); err != nil {
		return &protocol.Error{Category: "checkpoint_save_failed"}
	}
	s.data, s.client, s.checkpoint = &candidate, client, cp
	s.ingestLocked(result)
	return s.saveLocked()
}
