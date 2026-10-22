package service

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"syscall"
	"time"

	"github.com/JFSAS/wispcat/internal/native"
	"github.com/JFSAS/wispcat/internal/protocol"
)

const networkRetryLimit = 6
const protocolRetryLimit = 2
const saveRetryLimit = 3
const authenticationAttemptLimit = 3
const storageProbeInterval = time.Minute

var errRecoveryInterrupted = errors.New("recovery interrupted by shutdown or mode change")

// Fields are generated locally from allowlisted metadata. No error text, paths,
// credentials, peers, HTTP headers or message bodies enter logs or status.
type failureReport struct {
	Class       string `json:"class"`
	Operation   string `json:"operation"`
	Stage       string `json:"stage"`
	Reason      string `json:"reason"`
	Code        int64  `json:"code,omitempty"`
	Retries     int    `json:"retries"`
	Limit       int    `json:"retry_limit"`
	Decision    string `json:"decision"`
	ObservedAt  int64  `json:"observed_at"`
	NextRetryAt int64  `json:"next_retry_at,omitempty"`
}

type runtimeFailure struct {
	class, stage, reason string
	code                 int64
	retries, limit       int
	cause                error
}

func (e *runtimeFailure) Error() string { return e.class + ": " + e.reason }
func (e *runtimeFailure) Unwrap() error { return e.cause }

func storageFailure(stage string, err error) error {
	if err == nil {
		return nil
	}
	reason := "storage_operation_failed"
	switch {
	case errors.Is(err, syscall.ENOSPC):
		reason = "storage_full"
	case errors.Is(err, os.ErrPermission):
		reason = "storage_permission_denied"
	case errors.Is(err, syscall.EROFS):
		reason = "storage_read_only"
	case errors.Is(err, syscall.EIO):
		reason = "storage_io_error"
	}
	var errno syscall.Errno
	_ = errors.As(err, &errno)
	return &runtimeFailure{class: "storage", stage: stage, reason: reason, code: int64(errno), cause: err}
}

func classifyFailure(err error, operation string) runtimeFailure {
	var failure *runtimeFailure
	if errors.As(err, &failure) {
		return *failure
	}
	f := runtimeFailure{class: "local", stage: operation, reason: "unexpected_runtime_error", cause: err}
	var response *protocol.ResponseError
	var exchange *native.ExchangeError
	var p *protocol.Error
	var network net.Error
	switch {
	case errors.As(err, &response):
		f.class, f.stage, f.reason = "protocol", response.Stage, response.Reason
		if response.Transient {
			f.class = "network"
		}
	case errors.As(err, &exchange):
		f.class, f.stage, f.reason, f.code = "network", exchange.Stage, "tls_exchange_failed", int64(exchange.Code)
		if exchange.Stage == "verify_certificate" {
			f.reason, f.code = "certificate_verification_failed", exchange.VerifyResult
		}
	case errors.As(err, &p):
		f.class, f.reason, f.code = "protocol", p.Category, int64(p.Code)
		switch p.Category {
		case "session_expired", "authentication_rejected", "session_missing", "authentication_backoff":
			f.class = "authentication"
		case "http_response_failed":
			if p.Code == 408 || p.Code == 429 || (p.Code >= 500 && p.Code <= 599) {
				f.class = "network"
			}
		case "checkpoint_save_failed":
			f.class, f.stage = "storage", "checkpoint"
		}
	case errors.Is(err, protocol.ErrWire):
		f.class, f.reason = "protocol", "invalid_protocol_encoding"
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.As(err, &network):
		f.class, f.reason = "network", "transport_interrupted"
	}
	if f.class == "local" && (err.Error() == "authenticated payload rejected" || err.Error() == "server signature rejected") {
		f.class, f.reason = "protocol", "payload_integrity_failed"
	}
	return f
}

func (s *Service) reportFailureLocked(f runtimeFailure, decision string, delay time.Duration) {
	report := failureReport{Class: f.class, Operation: s.operation, Stage: f.stage, Reason: f.reason,
		Code: f.code, Retries: f.retries, Limit: f.limit, Decision: decision, ObservedAt: time.Now().Unix()}
	if delay > 0 {
		report.NextRetryAt = time.Now().Add(delay).Unix()
	}
	s.connection.Failure = &report
	if s.failureSink != nil {
		s.failureSink(report)
		return
	}
	level := slog.LevelWarn
	if decision == "blocked" {
		level = slog.LevelError
	} else if decision == "recovered" || decision == "save_recovered" {
		level = slog.LevelInfo
	}
	slog.Log(context.Background(), level, "wechat_runtime_failure", "runtime_id", s.modeEpoch>>32, "class", report.Class,
		"operation", report.Operation, "stage", report.Stage, "reason", report.Reason, "code", report.Code,
		"retries", report.Retries, "retry_limit", report.Limit, "decision", report.Decision, "next_retry_at", report.NextRetryAt)
}

// Called after the inherited block handling so specific account/login states
// remain intact, while generic failures acquire useful, safe diagnostics.
func (s *Service) describeBlockedLocked(err error) {
	f := classifyFailure(err, s.operation)
	s.reportFailureLocked(f, "blocked", 0)
	if s.state != "needs_attention" || (f.class == "local" && s.category != "local_or_protocol_error") {
		return
	}
	s.category = f.class + "_error"
	if f.limit > 0 {
		s.category = f.reason
	}
	switch f.class {
	case "storage":
		s.detail = "状态保存失败，已暂停网络收发；已有缓存保留"
		s.connection.NextStep = "查看错误日志中的保存阶段，检查磁盘空间、权限和存储状态；修复后恢复上线。"
	case "network":
		s.detail = "网络连接失败，自动恢复已停止；缓存登录材料保留"
		s.connection.NextStep = "查看网络或 TLS 错误日志，修复后恢复上线；无需直接重新导入凭证。"
	case "protocol":
		s.detail = "协议响应处理失败，自动重试已停止；缓存登录材料保留"
		s.connection.NextStep = "查看错误日志中的失败阶段和重试次数，检查服务端响应或协议兼容性后恢复上线。"
	case "authentication":
		s.detail = "自动鉴权已达到次数上限；缓存保留，等待明确操作"
		s.connection.NextStep = "检查鉴权错误日志和网络状态；不会继续自动反复登录。"
	}
}

func (s *Service) authenticationAllowed(recovery *sessionRecovery) bool {
	if recovery.authAttempts >= authenticationAttemptLimit {
		s.block(&runtimeFailure{class: "authentication", stage: "authenticate", reason: "authentication_retry_exhausted", retries: recovery.authAttempts, limit: authenticationAttemptLimit})
		return false
	}
	recovery.authAttempts++
	return true
}

func (s *Service) authenticate(ctx context.Context) error {
	if s.authenticateRPC != nil {
		return s.authenticateRPC(ctx)
	}
	return s.client.Authenticate(ctx)
}

func (s *Service) sync(ctx context.Context, scene uint32) (result protocol.SyncResult, err error) {
	state := s.client.State
	cursor := bytes.Clone(state.Cursor)
	defer func() {
		// MergeCursor can return nil with an error. A failed response must not
		// erase our position before the next bounded Sync retry.
		if err != nil {
			state.Cursor = cursor
		}
	}()
	if s.syncRPC != nil {
		return s.syncRPC(ctx, scene)
	}
	return s.client.Sync(ctx, scene, nil)
}

func (s *Service) setOperation(operation string) {
	s.mu.Lock()
	s.operation = operation
	s.mu.Unlock()
}

func (s *Service) activeEpochLocked(epoch uint64) bool {
	return s.mode == "online" && s.modeEpoch == epoch
}

func (s *Service) waitRecovery(ctx context.Context, delay time.Duration, epoch uint64) error {
	if s.recoveryWait != nil {
		return s.recoveryWait(ctx, delay)
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return errRecoveryInterrupted
		case <-timer.C:
			return nil
		case <-s.wake:
			s.mu.RLock()
			active := s.activeEpochLocked(epoch)
			s.mu.RUnlock()
			if !active {
				return errRecoveryInterrupted
			}
		}
	}
}

// Retry the already ingested state, never a new Sync, send, or login. A mode
// change or shutdown interrupts the wait, and no lock is held while sleeping.
func (s *Service) retrySave(ctx context.Context, epoch uint64, err error) error {
	for retry := 1; retry <= saveRetryLimit; retry++ {
		s.mu.Lock()
		if ctx.Err() != nil || !s.activeEpochLocked(epoch) {
			s.mu.Unlock()
			return errRecoveryInterrupted
		}
		f := classifyFailure(err, s.operation)
		f.retries, f.limit = retry, saveRetryLimit
		delay := time.Second * time.Duration(1<<(retry-1))
		s.state, s.category = "saving_retry", "storage_error"
		s.detail = "状态保存失败，正在有限重试当前批次；暂停新的网络收发"
		s.connection.NextStep = "等待保存重试结果；检查磁盘空间和权限。"
		s.reportFailureLocked(f, "retry_save", delay)
		s.mu.Unlock()
		if s.waitRecovery(ctx, delay, epoch) != nil {
			return errRecoveryInterrupted
		}
		s.mu.Lock()
		if ctx.Err() != nil || !s.activeEpochLocked(epoch) {
			s.mu.Unlock()
			return errRecoveryInterrupted
		}
		err = s.saveLocked()
		if err == nil {
			s.reportFailureLocked(f, "save_recovered", 0)
			s.mu.Unlock()
			return nil
		}
		s.mu.Unlock()
	}
	return s.probeStorage(ctx, epoch, err)
}

// Exhausting the short retry burst pauses network work, not the local worker.
// Keep the uncommitted batch and the recovery/login budget in place. A timer
// retries only its durable save, so freeing disk space can resume the existing
// session without an operator reconnect or another authentication attempt.
func (s *Service) probeStorage(ctx context.Context, epoch uint64, err error) error {
	for {
		s.mu.Lock()
		if ctx.Err() != nil || !s.activeEpochLocked(epoch) {
			s.mu.Unlock()
			return errRecoveryInterrupted
		}
		f := classifyFailure(err, s.operation)
		f.retries, f.limit = saveRetryLimit, saveRetryLimit
		s.state, s.category = "storage_paused", "storage_unavailable"
		s.detail = "状态尚未保存，已暂停网络收发；每 60 秒自动重试保存当前批次"
		s.connection.NextStep = "检查磁盘空间、权限和存储状态；保存成功后自动继续原会话，无需重新登录。"
		s.reportFailureLocked(f, "storage_paused", storageProbeInterval)
		s.mu.Unlock()
		if s.waitRecovery(ctx, storageProbeInterval, epoch) != nil {
			return errRecoveryInterrupted
		}
		s.mu.Lock()
		if ctx.Err() != nil || !s.activeEpochLocked(epoch) {
			s.mu.Unlock()
			return errRecoveryInterrupted
		}
		err = s.saveLocked()
		if err == nil {
			s.reportFailureLocked(f, "save_recovered", 0)
			s.mu.Unlock()
			return nil
		}
		s.mu.Unlock()
	}
}

func (s *Service) commitCheckpoint() error {
	if s.checkpointWrite != nil {
		return s.checkpointWrite(s.data)
	}
	return s.checkpoint.Save(s.data)
}

func (s *Service) markSyncSuccessLocked() {
	if previous := s.connection.Failure; previous != nil && (previous.Decision == "retry_network" || previous.Decision == "retry_sync" || previous.Decision == "save_recovered") {
		f := runtimeFailure{class: previous.Class, stage: previous.Stage, reason: previous.Reason, code: previous.Code, retries: previous.Retries, limit: previous.Limit}
		s.reportFailureLocked(f, "recovered", 0)
	}
	s.state, s.sessionState, s.lastSync = "connected", "validated", time.Now().Unix()
	s.connection.Stage, s.connection.NextStep = "connected", ""
	s.detail, s.category, s.code = "", "", 0
}
