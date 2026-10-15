package host

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/JFSAS/wechat-runtime/internal/instances"
	"github.com/JFSAS/wechat-runtime/internal/onebot"
	"github.com/JFSAS/wechat-runtime/internal/store"
)

func effectiveConfig(c Config) (Config, error) {
	if err := c.validate(false); err != nil {
		return c, err
	}
	c.Accounts = append([]Account(nil), c.Accounts...)
	for i, a := range c.Accounts {
		p, err := instances.Preview(c.StateDir, a.Instance)
		if err != nil {
			return c, err
		}
		raw, err := store.ReadPrivate(filepath.Join(p.Directory, "connections.json"), 64<<10)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return c, err
		}
		var plan connectionPlan
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&plan) != nil || decoder.Decode(new(any)) != io.EOF {
			return c, errors.New("connections_config_invalid")
		}
		c.Accounts[i].Reverse, c.Accounts[i].HTTPPost = plan.Reverse, plan.HTTPPost
	}
	return c, c.Validate()
}

type connectionPlan struct {
	Reverse  []Reverse `json:"reverse"`
	HTTPPost []Post    `json:"http_post"`
}
type connectionSettings struct {
	mu                          sync.Mutex
	path                        string
	saved                       connectionPlan
	revision, active, lastError string
	manager                     *onebot.NetworkManager
	base                        Account
	top                         Config
}

func planRevision(plan connectionPlan) string {
	raw, _ := json.Marshal(plan)
	hash := sha256.Sum256(raw)
	return hex.EncodeToString(hash[:16])
}
func openConnectionSettings(directory string, top Config, a Account) (*connectionSettings, error) {
	s := &connectionSettings{path: filepath.Join(directory, "connections.json"), saved: connectionPlan{Reverse: a.Reverse, HTTPPost: a.HTTPPost}, base: a, top: top}
	raw, err := store.ReadPrivate(s.path, 64<<10)
	if err == nil {
		if json.Unmarshal(raw, &s.saved) != nil {
			return nil, errors.New("connections_config_invalid")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err = s.validate(s.saved); err != nil {
		return nil, err
	}
	s.revision = planRevision(s.saved)
	return s, nil
}
func (s *connectionSettings) validate(plan connectionPlan) error {
	a := s.base
	a.Reverse, a.HTTPPost = plan.Reverse, plan.HTTPPost
	c := s.top
	c.Accounts = []Account{a}
	return c.Validate()
}
func (s *connectionSettings) specs(plan connectionPlan) ([]onebot.NetworkSpec, error) {
	result := []onebot.NetworkSpec{}
	for _, r := range plan.Reverse {
		secret, err := token(r.TokenEnv)
		if err != nil {
			return nil, err
		}
		result = append(result, onebot.NetworkSpec{Name: r.Name, Kind: "wsClient", URL: r.URL, Role: r.Role, Secret: secret})
	}
	for _, p := range plan.HTTPPost {
		secret, err := token(p.SecretEnv)
		if err != nil {
			return nil, err
		}
		result = append(result, onebot.NetworkSpec{Name: p.Name, Kind: "httpClient", URL: p.URL, Secret: secret})
	}
	return result, nil
}
func (s *connectionSettings) view() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]any{"saved": s.saved, "revision": s.revision, "active_revision": s.active, "applied": s.active == s.revision, "error": s.lastError}
}
func (s *connectionSettings) save(revision string, plan connectionPlan) error {
	if err := s.validate(plan); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if revision != s.revision {
		return errors.New("connections_revision_conflict")
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		return err
	}
	if len(raw) > 64<<10 {
		return errors.New("connections_config_too_large")
	}
	if err = store.Atomic(s.path, raw); err != nil {
		return err
	}
	s.saved = plan
	s.revision = planRevision(plan)
	return nil
}
func (s *connectionSettings) apply(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.manager == nil {
		return errors.New("runtime_not_started")
	}
	specs, err := s.specs(s.saved)
	if err == nil {
		err = s.manager.Apply(ctx, specs)
	}
	if err != nil {
		s.lastError = "connections_apply_failed"
		return err
	}
	s.active = s.revision
	s.lastError = ""
	return nil
}
