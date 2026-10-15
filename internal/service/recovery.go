package service

import (
	"errors"
	"time"

	"github.com/JFSAS/wechat-runtime/internal/protocol"
)

// Recovery uses only the existing session and its saved automatic-login ticket.
// Only an explicit disk-login operation imports a replacement from official disk.
type sessionRecovery struct {
	reauth    bool
	refreshed bool
	retries   int
}

func (s *Service) recoverFailure(err error, recovery *sessionRecovery) (time.Duration, bool) {
	var p *protocol.Error
	if errors.As(err, &p) {
		if p.Category == "authentication_backoff" {
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
			s.mu.Unlock()
			return 0, true
		}
		// HTTP outages do not prove that a credential has expired.
		if p.Category != "http_response_failed" || (p.Code != 408 && p.Code != 429 && (p.Code < 500 || p.Code > 599)) {
			s.block(err)
			return 0, false
		}
	} else if errors.Is(err, protocol.ErrWire) {
		s.block(err)
		return 0, false
	}
	if recovery.retries < 6 {
		recovery.retries++
	}
	delay := s.interval * time.Duration(1<<recovery.retries)
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
	s.mu.Unlock()
	return delay, true
}
