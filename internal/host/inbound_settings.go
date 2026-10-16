package host

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/JFSAS/wechat-runtime/internal/instances"
	"github.com/JFSAS/wechat-runtime/internal/store"
)

type inboundPlan struct {
	Listen   string `json:"listen"`
	TokenEnv string `json:"token_env"`
}

func effectiveInboundConfig(c Config) (Config, error) {
	c.Accounts = append([]Account{}, c.Accounts...)
	for i, account := range c.Accounts {
		profile, err := instances.Preview(c.StateDir, account.Instance)
		if err != nil {
			return c, err
		}
		raw, err := store.ReadPrivate(filepath.Join(profile.Directory, "listener.json"), 8192)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return c, err
		}
		var plan inboundPlan
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&plan) != nil || decoder.Decode(new(any)) != io.EOF {
			return c, errors.New("listener_config_invalid")
		}
		c.Accounts[i].Listen, c.Accounts[i].TokenEnv = plan.Listen, plan.TokenEnv
	}
	return c, nil
}

type inboundSettings struct {
	mu                          sync.Mutex
	path                        string
	saved                       inboundPlan
	revision, active, lastError string
	manager                     *inboundServer
	base                        Account
	top                         Config
	configSource                func() Config
}

func newInboundSettings(directory string, top Config, account Account) *inboundSettings {
	plan := inboundPlan{Listen: account.Listen, TokenEnv: account.TokenEnv}
	return &inboundSettings{path: filepath.Join(directory, "listener.json"), saved: plan,
		revision: inboundRevision(plan), base: account, top: top}
}

func inboundRevision(plan inboundPlan) string {
	// Revision hashes contain environment names only, never resolved tokens.
	return configRevision(plan)
}

func (s *inboundSettings) save(revision string, plan inboundPlan) error {
	account := s.base
	account.Listen, account.TokenEnv = plan.Listen, plan.TokenEnv
	c := s.top
	if s.configSource != nil {
		c = s.configSource()
	}
	c.Accounts = []Account{account}
	if err := c.validate(false); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if revision != s.revision {
		return errors.New("listener_revision_conflict")
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		return err
	}
	if len(raw) > 8192 {
		return errors.New("listener_config_too_large")
	}
	if err = store.Atomic(s.path, raw); err != nil {
		return err
	}
	s.saved, s.revision = plan, inboundRevision(plan)
	return nil
}

func (s *inboundSettings) view() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	view := map[string]any{"saved": s.saved, "revision": s.revision,
		"active_revision": s.active, "applied": false, "error": s.lastError}
	if s.manager != nil {
		secret, err := token(s.saved.TokenEnv)
		status, match := s.manager.snapshot(s.saved.Listen, secret)
		view["listener"] = status
		view["applied"] = err == nil && match && !status.Retiring
		if err != nil {
			view["error"] = "listener_token_unavailable"
		}
	}
	return view
}

func (s *inboundSettings) apply(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.manager == nil {
		return errors.New("runtime_not_started")
	}
	secret, err := token(s.saved.TokenEnv)
	if err == nil {
		err = s.manager.apply(ctx, s.saved.Listen, secret)
	}
	_, match := s.manager.snapshot(s.saved.Listen, secret)
	if match {
		s.active = s.revision
	}
	if err != nil {
		s.lastError = "listener_apply_failed"
		return err
	}
	s.lastError = ""
	return nil
}
