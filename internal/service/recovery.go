package service

import (
	"errors"
	"time"

	"github.com/JFSAS/wispcat/internal/native"
	"github.com/JFSAS/wispcat/internal/protocol"
)

// Recovery uses only the existing session and its saved automatic-login ticket.
// Only an explicit disk-login operation imports a replacement from official disk.
type sessionRecovery struct {
	reauth          bool
	refreshed       bool
	retries         int
	protocolRetries int
	authAttempts    int
}

func (s *Service) recoverFailure(err error, recovery *sessionRecovery) (time.Duration, bool) {
	s.mu.RLock()
	operation, sessionState := s.operation, s.sessionState
	if operation == "" {
		operation = s.connection.Stage
	}
	s.mu.RUnlock()
	f := classifyFailure(err, operation)
	var p *protocol.Error
	if errors.As(err, &p) {
		if p.Category == "authentication_backoff" {
			// The protocol limiter made no request; do not consume a login attempt.
			if recovery.authAttempts > 0 {
				recovery.authAttempts--
			}
			s.mu.Lock()
			s.state, s.detail = "recovering", "正在等待鉴权间隔，随后继续恢复登录状态"
			s.mu.Unlock()
			return 10 * time.Second, true
		}
		if p.Category == "session_expired" && !recovery.refreshed {
			recovery.reauth, recovery.refreshed = true, true
			s.mu.Lock()
			s.beginConnectionLocked("cache", "authenticate")
			s.connection.NextStep = "正在自动使用缓存票据恢复，无需手动操作。"
			s.state, s.sessionState = "recovering", "refreshing"
			s.detail = "正在使用缓存票据自动恢复登录状态"
			s.category, s.code = "session_refreshing", 0
			f.retries, f.limit = recovery.authAttempts, authenticationAttemptLimit
			s.reportFailureLocked(f, "refresh_cached_session", 0)
			s.mu.Unlock()
			return 0, true
		}
	}
	var exchange *native.ExchangeError
	if errors.As(err, &exchange) && !exchange.Retryable {
		s.block(err)
		return 0, false
	}
	var response *protocol.ResponseError
	retryableProtocol := errors.Is(err, protocol.ErrWire) || (errors.As(err, &response) && response.Reason != "http_body_limit")
	if f.class == "protocol" && retryableProtocol && operation == "sync" && sessionState == "validated" && !recovery.reauth {
		if recovery.protocolRetries >= protocolRetryLimit {
			f.reason, f.retries, f.limit = "protocol_retry_exhausted", recovery.protocolRetries, protocolRetryLimit
			s.block(&f)
			return 0, false
		}
		recovery.protocolRetries++
		f.retries, f.limit = recovery.protocolRetries, protocolRetryLimit
		delay := time.Second * time.Duration(1<<(recovery.protocolRetries-1))
		s.mu.Lock()
		s.state, s.category = "reconnecting", "protocol_error"
		s.detail = "同步响应异常，正在有限重试；保留缓存会话和已保存的同步位置"
		s.connection.NextStep = "等待本次同步重试结果；不会因此反复重新登录。"
		s.reportFailureLocked(f, "retry_sync", delay)
		s.mu.Unlock()
		return delay, true
	}
	if f.class != "network" {
		s.block(err)
		return 0, false
	}
	if operation == "authenticate" && recovery.authAttempts >= authenticationAttemptLimit {
		f.reason, f.retries, f.limit = "authentication_retry_exhausted", recovery.authAttempts, authenticationAttemptLimit
		s.block(&f)
		return 0, false
	}
	if recovery.retries >= networkRetryLimit {
		f.reason, f.retries, f.limit = "network_retry_exhausted", recovery.retries, networkRetryLimit
		s.block(&f)
		return 0, false
	}
	recovery.retries++
	f.retries, f.limit = recovery.retries, networkRetryLimit
	delay := max(s.interval, time.Second) * time.Duration(1<<recovery.retries)
	if delay > 30*time.Second {
		delay = 30 * time.Second
	}
	s.mu.Lock()
	s.state = "reconnecting"
	s.detail = "网络暂不可用，正在自动重试；保留缓存登录状态"
	if recovery.reauth {
		s.state = "recovering"
		s.detail = "网络暂不可用，恢复后继续用缓存票据鉴权"
	} else if recovery.retries >= 3 {
		s.state = "network_unavailable"
	}
	s.category, s.code = "network_unavailable", 0
	s.connection.NextStep = "网络恢复后会自动重试，暂时不需要重新登录。"
	s.reportFailureLocked(f, "retry_network", delay)
	s.mu.Unlock()
	return delay, true
}
