package host

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"

	"github.com/JFSAS/wechat-runtime/internal/instances"
	"github.com/JFSAS/wechat-runtime/internal/native"
	"github.com/JFSAS/wechat-runtime/internal/onebot"
	"github.com/JFSAS/wechat-runtime/internal/store"
)

const maxTopologyBytes = 256 << 10
const topologyGuard = "wechat-runtime/topology/v1"

type topologyPlan struct {
	Version  int       `json:"version"`
	Accounts []Account `json:"accounts"`
}

func cloneTopology(plan topologyPlan) topologyPlan {
	raw, _ := json.Marshal(plan)
	var cloned topologyPlan
	_ = json.Unmarshal(raw, &cloned)
	if cloned.Accounts == nil {
		cloned.Accounts = []Account{}
	}
	return cloned
}

func readTopology(root string, bootstrap []Account) (topologyPlan, error) {
	plan := cloneTopology(topologyPlan{Version: 1, Accounts: bootstrap})
	guard, guardErr := store.ReadPrivate(filepath.Join(root, "topology.required"), 128)
	if guardErr != nil && !errors.Is(guardErr, os.ErrNotExist) {
		return plan, guardErr
	}
	if guardErr == nil && string(guard) != topologyGuard {
		return plan, errors.New("topology_guard_invalid")
	}
	raw, err := store.ReadPrivate(filepath.Join(root, "topology.json"), maxTopologyBytes)
	if errors.Is(err, os.ErrNotExist) {
		if guardErr == nil {
			return plan, errors.New("topology_missing_preserve_state")
		}
		if bootstrap == nil {
			return plan, errors.New("invalid_runtime_topology")
		}
		return plan, nil
	}
	if err != nil {
		return plan, err
	}
	if guardErr != nil {
		return plan, errors.New("topology_guard_missing")
	}
	plan = topologyPlan{} // Existing files must never inherit bootstrap fields.
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&plan) != nil || decoder.Decode(new(any)) != io.EOF || plan.Version != 1 || plan.Accounts == nil || len(plan.Accounts) > 16 {
		return plan, errors.New("topology_config_invalid")
	}
	return plan, nil
}

func effectiveTopology(c Config) (Config, error) {
	if c.StateDir == "" {
		return c, errors.New("invalid_runtime_topology")
	}
	plan, err := readTopology(c.StateDir, c.Accounts)
	if err != nil {
		return c, err
	}
	c.Accounts = plan.Accounts
	return c, nil
}

type topologySettings struct {
	mu                          sync.Mutex
	host                        *Host
	path                        string
	saved                       topologyPlan
	revision, active, lastError string
	closed                      bool
}

func newTopologySettings(h *Host, bootstrap []Account) (*topologySettings, error) {
	plan, err := readTopology(h.config.StateDir, bootstrap)
	if err != nil {
		return nil, err
	}
	return &topologySettings{host: h, path: filepath.Join(h.config.StateDir, "topology.json"), saved: plan, revision: configRevision(plan)}, nil
}

func instanceKey(account Account) string {
	key, _ := instances.NormalizeKey(account.Instance)
	return key
}

func (s *topologySettings) save(revision string, plan topologyPlan) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.host
	h.mu.Lock()
	top, closed := h.config, h.closed
	owners := append([]*accountRuntime(nil), h.accounts...)
	h.mu.Unlock()
	currentManagement := h.currentConfig()
	top.Listen, top.TokenEnv = currentManagement.Listen, currentManagement.TokenEnv
	if s.closed || closed {
		return errors.New("runtime_stopping")
	}
	if revision != s.revision {
		return errors.New("topology_revision_conflict")
	}
	if plan.Version != 1 || plan.Accounts == nil || len(plan.Accounts) > 16 {
		return errors.New("topology_config_invalid")
	}
	plan = cloneTopology(plan)
	top.Accounts = plan.Accounts
	if _, err := effectiveAccounts(top); err != nil {
		return err
	}
	for _, desired := range plan.Accounts {
		for _, owner := range owners {
			if owner.config.Name == desired.Name && configRevision(owner.bootstrap) != configRevision(desired) {
				return errors.New("active_account_bootstrap_immutable")
			}
			if instanceKey(owner.config) == instanceKey(desired) && owner.config.Name != desired.Name {
				return errors.New("stop_instance_before_rename")
			}
		}
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		return err
	}
	if len(raw) > maxTopologyBytes {
		return errors.New("topology_config_too_large")
	}
	// Publish the required marker first. An interrupted first save must fail
	// closed on reopen rather than silently resurrect bootstrap accounts.
	if err = store.Atomic(filepath.Join(filepath.Dir(s.path), "topology.required"), []byte(topologyGuard)); err != nil {
		return err
	}
	if err = store.Atomic(s.path, raw); err != nil {
		return err
	}
	s.saved, s.revision = plan, configRevision(plan)
	return nil
}

func (s *topologySettings) view() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.host
	h.mu.Lock()
	owners := append([]*accountRuntime(nil), h.accounts...)
	retiring := append([]*accountRuntime(nil), h.retiring...)
	started, closed := h.started, h.closed
	h.mu.Unlock()
	active := []any{}
	match := len(owners) == len(s.saved.Accounts)
	for i, a := range owners {
		address := ""
		if a.inbound.manager != nil {
			status, _ := a.inbound.manager.snapshot("", "")
			address = status.Address
		}
		active = append(active, map[string]any{"name": a.config.Name, "instance": a.config.Instance, "onebot_address": address})
		if i >= len(s.saved.Accounts) || a.config.Name != s.saved.Accounts[i].Name || instanceKey(a.config) != instanceKey(s.saved.Accounts[i]) {
			match = false
		}
	}
	pending := []any{}
	for _, a := range retiring {
		state, category := "retiring", ""
		select {
		case <-a.done:
			state = "closed"
			if a.closeError != nil {
				state = "cleanup_failed"
				category = "account_cleanup_failed"
			}
		default:
		}
		pending = append(pending, map[string]string{"name": a.config.Name, "instance": a.config.Instance, "state": state, "error": category})
	}
	return map[string]any{"saved": s.saved, "revision": s.revision, "active_revision": s.active,
		"active": active, "retiring": pending, "applied": started && !closed && match && len(retiring) == 0 && s.active == s.revision, "error": s.lastError}
}

func (s *topologySettings) joinRetiring(ctx context.Context) error {
	h := s.host
	h.mu.Lock()
	owners := append([]*accountRuntime(nil), h.retiring...)
	h.mu.Unlock()
	for _, a := range owners {
		if err := waitInbound(ctx, a.done); err != nil {
			s.lastError = "account_retirement_pending"
			return err
		}
		if a.closeError != nil {
			s.lastError = "account_cleanup_failed"
			return errors.New(s.lastError)
		}
	}
	h.mu.Lock()
	h.retiring = nil
	h.mu.Unlock()
	return nil
}

func (s *topologySettings) retireCandidates(candidates []*accountRuntime) {
	if len(candidates) == 0 {
		return
	}
	h := s.host
	h.mu.Lock()
	h.retiring = append(h.retiring, candidates...)
	h.mu.Unlock()
	for _, a := range candidates {
		a.retire()
	}
}

func (s *topologySettings) apply(ctx context.Context) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.host
	h.mu.Lock()
	top, parent := h.config, h.parent
	stopped := h.closed || !h.started
	owners := append([]*accountRuntime(nil), h.accounts...)
	h.mu.Unlock()
	currentManagement := h.currentConfig()
	top.Listen, top.TokenEnv = currentManagement.Listen, currentManagement.TokenEnv
	if s.closed || stopped || parent == nil || parent.Err() != nil {
		return errors.New("runtime_not_running")
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = s.joinRetiring(ctx); err != nil {
		return err
	}
	// A retiring account may still own an admitted management request that
	// needs this gate. Join previous retirements before acquiring it, and
	// release it after publication before joining newly removed accounts.
	h.listenerChanges.Lock()
	gateHeld := true
	defer func() {
		if gateHeld {
			h.listenerChanges.Unlock()
		}
	}()
	top.Accounts = cloneTopology(s.saved).Accounts
	top, err = effectiveAccounts(top)
	if err != nil {
		s.lastError = "topology_preflight_failed"
		return err
	}
	for _, a := range top.Accounts {
		if a.Mode == "online" && !native.Supported() {
			s.lastError = "native_transport_unavailable"
			return errors.New(s.lastError)
		}
	}
	current := map[string]*accountRuntime{}
	for _, a := range owners {
		current[a.config.Name] = a
	}
	absolute, err := filepath.Abs(top.StateDir)
	if err != nil {
		return err
	}
	catalog := &instances.Catalog{Root: absolute}
	profiles := map[string]instances.Profile{}
	sources := []string{}
	for _, a := range top.Accounts {
		if old := current[a.Name]; old != nil {
			if instanceKey(old.config) != instanceKey(a) {
				return errors.New("active_account_identity_immutable")
			}
			continue
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		var profile instances.Profile
		profile, err = catalog.Ensure(a.Instance)
		if err != nil {
			s.lastError = "topology_preflight_failed"
			return err
		}
		profiles[a.Name] = profile
		sources = append(sources, filepath.Join(profile.Directory, "onebot"))
	}
	if len(sources) > 0 {
		err = h.state.ImportLegacyDirectories(sources)
	}
	if err != nil {
		s.lastError = "topology_legacy_import_failed"
		return err
	}
	candidates := []*accountRuntime{}
	defer func() {
		if err != nil {
			s.retireCandidates(candidates)
		}
	}()
	next := []*accountRuntime{}
	initial := map[*accountRuntime][]onebot.NetworkSpec{}
	for _, config := range top.Accounts {
		if old := current[config.Name]; old != nil {
			next = append(next, old)
			continue
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		var owner *accountRuntime
		owner, err = openAccount(top, config, profiles[config.Name], h.state, h.workerFactory)
		if err != nil {
			s.lastError = "topology_preflight_failed"
			return err
		}
		for _, original := range s.saved.Accounts {
			if original.Name == config.Name {
				owner.bootstrap = original
				break
			}
		}
		candidates = append(candidates, owner)
		owner.connections.configSource = h.currentConfig
		owner.inbound.configSource = h.currentConfig
		owner.inbound.changes = &h.listenerChanges
		initial[owner], err = owner.connections.specs(owner.connections.saved)
		if err != nil {
			s.lastError = "topology_preflight_failed"
			return err
		}
		owner.listener, err = net.Listen("tcp", owner.config.Listen)
		if err != nil {
			s.lastError = "topology_bind_failed"
			return err
		}
		next = append(next, owner)
	}
	h.mu.Lock()
	if err = ctx.Err(); err != nil {
		h.mu.Unlock()
		return err
	}
	if h.closed || parent.Err() != nil {
		h.mu.Unlock()
		return errors.New("runtime_stopping")
	}
	for _, owner := range candidates {
		if err = owner.start(parent, initial[owner]); err != nil {
			h.mu.Unlock()
			s.lastError = "topology_start_failed"
			return err
		}
	}
	retained := map[*accountRuntime]bool{}
	for _, a := range next {
		retained[a] = true
	}
	removed := []*accountRuntime{}
	for _, a := range owners {
		if !retained[a] {
			removed = append(removed, a)
		}
	}
	h.accounts = next
	h.config.Accounts = top.Accounts
	h.retiring = append(h.retiring, removed...)
	h.mu.Unlock()
	candidates = nil // ownership transferred to the active host slice
	for _, a := range removed {
		a.retire()
	}
	h.listenerChanges.Unlock()
	gateHeld = false
	if err = s.joinRetiring(ctx); err != nil {
		return err
	}
	s.active = s.revision
	s.lastError = ""
	return nil
}
