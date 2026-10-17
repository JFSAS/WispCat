package service

import (
	"errors"

	"github.com/JFSAS/wispcat/internal/account"
)

var ErrLoginSuperseded = errors.New("login_request_superseded")

// QueueDiskLoginFrom chooses an installation; the serial worker owns the
// actual source replacement, verification and credential import.
func (s *Service) QueueDiskLoginFrom(path string) error {
	return s.queueDiskLoginFrom(path, account.ResolveApp)
}
func (s *Service) queueDiskLoginFrom(path string, resolve func(string) (account.Installation, error)) error {
	s.mu.RLock()
	epoch := s.modeEpoch
	s.mu.RUnlock()
	selected, err := resolve(path)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.modeEpoch != epoch {
		s.mu.Unlock()
		return ErrLoginSuperseded
	}
	s.pendingApp = selected.Path
	s.setModeLocked("online", true)
	s.mu.Unlock()
	s.signal()
	return nil
}
func (s *Service) selectedAppLocked() string {
	if s.pendingApp != "" {
		return s.pendingApp
	}
	return s.source.App
}
func (s *Service) applyPendingAppLocked() bool {
	if s.pendingApp == "" {
		return false
	}
	selected := s.pendingApp
	s.pendingApp = ""
	if selected == s.source.App {
		return false
	}
	s.source = account.Source{App: selected}
	s.client = nil
	return true
}
func (s *Service) DiskLoginApps() map[string]any {
	s.mu.RLock()
	selected, active := s.selectedAppLocked(), s.source.App
	cached := ""
	if s.data != nil {
		cached = s.data.SourceApp
	}
	s.mu.RUnlock()
	return map[string]any{"selected_app": selected, "active_app": active, "cached_app": cached, "apps": account.DiscoverApps(selected)}
}
