package host

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JFSAS/wispcat/internal/store"
)

const managementGuard = "wechat-runtime/management/v1"

type managementPlan struct {
	Version  int    `json:"version"`
	Listen   string `json:"listen"`
	TokenEnv string `json:"token_env"`
}
type managementActive struct {
	Plan    managementPlan `json:"plan"`
	Address string         `json:"address"`
	secret  string
}
type managementEndpointKey struct{}
type managementEndpoint struct{ address, secret string }

func readManagement(root string, bootstrap managementPlan) (managementPlan, error) {
	guard, guardErr := store.ReadPrivate(filepath.Join(root, "management.required"), 128)
	if guardErr != nil && !errors.Is(guardErr, os.ErrNotExist) {
		return managementPlan{}, guardErr
	}
	if guardErr == nil && string(guard) != managementGuard {
		return managementPlan{}, errors.New("management_guard_invalid")
	}
	raw, err := store.ReadPrivate(filepath.Join(root, "management.json"), 8192)
	if errors.Is(err, os.ErrNotExist) {
		if guardErr == nil {
			return managementPlan{}, errors.New("management_config_missing")
		}
		return bootstrap, nil
	}
	if err != nil {
		return managementPlan{}, err
	}
	if guardErr != nil {
		return managementPlan{}, errors.New("management_guard_missing")
	}
	var plan managementPlan
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&plan) != nil || decoder.Decode(new(any)) != io.EOF || plan.Version != 1 || !loopbackListen(plan.Listen) || plan.TokenEnv == "" {
		return plan, errors.New("management_config_invalid")
	}
	return plan, nil
}
func effectiveManagement(c Config) (Config, error) {
	if c.StateDir == "" {
		return c, errors.New("invalid_runtime_topology")
	}
	plan, err := readManagement(c.StateDir, managementPlan{Version: 1, Listen: c.Listen, TokenEnv: c.TokenEnv})
	if err != nil {
		return c, err
	}
	c.Listen, c.TokenEnv = plan.Listen, plan.TokenEnv
	return c, nil
}

type managementOperation struct {
	ID       string `json:"id"`
	Revision string `json:"revision"`
	Address  string `json:"address"`
	State    string `json:"state"`
	Error    string `json:"error,omitempty"`
	plan     managementPlan
	change   *listenerChange
	done     chan struct{}
}

type managementSettings struct {
	mu         sync.Mutex
	host       *Host
	path       string
	saved      managementPlan
	revision   string
	lastError  string
	manager    *inboundServer
	current    atomic.Pointer[managementActive]
	publishing atomic.Pointer[managementPlan]
	operation  *managementOperation
	history    []*managementOperation
	closed     bool
}

func newManagementSettings(h *Host, bootstrap Config) (*managementSettings, error) {
	plan, err := readManagement(h.config.StateDir, managementPlan{Version: 1, Listen: bootstrap.Listen, TokenEnv: bootstrap.TokenEnv})
	if err != nil {
		return nil, err
	}
	return &managementSettings{host: h, path: filepath.Join(h.config.StateDir, "management.json"), saved: plan, revision: configRevision(plan)}, nil
}

// publish is called inside the listener admission lock and never acquires the
// settings/Host mutex. Address, credential and public plan change as one tuple.
func (s *managementSettings) publish(address, secret string) {
	plan := s.publishing.Load()
	s.current.Store(&managementActive{Plan: *plan, Address: address, secret: secret})
}
func (s *managementSettings) startLocked(parent context.Context, listener net.Listener, secret string) {
	plan := s.saved
	s.publishing.Store(&plan)
	s.manager = newListenerServer(listenerTarget{serve: s.host.ServeEndpoint, publish: s.publish}, parent, listener, s.saved.Listen, secret)
}
func (s *managementSettings) runningOperation() bool {
	if s.operation == nil {
		return false
	}
	select {
	case <-s.operation.done:
		return false
	default:
		return true
	}
}
func operationView(op *managementOperation) map[string]string {
	if op == nil {
		return nil
	}
	return map[string]string{"id": op.ID, "revision": op.Revision, "address": op.Address, "state": op.State, "error": op.Error}
}
func (s *managementSettings) view() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	view := map[string]any{"saved": s.saved, "revision": s.revision, "applied": false, "error": s.lastError, "operation": operationView(s.operation)}
	if active := s.current.Load(); active != nil {
		view["active"] = active
	}
	if s.manager != nil {
		secret, err := token(s.saved.TokenEnv)
		status, match := s.manager.snapshot(s.saved.Listen, secret)
		active := s.current.Load()
		view["listener"] = status
		view["applied"] = err == nil && match && !status.Retiring && active != nil && active.Plan == s.saved
		if err != nil {
			view["error"] = "management_token_unavailable"
		}
	}
	return view
}
func (s *managementSettings) operationView(id string) (map[string]string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.operation != nil && s.operation.ID == id {
		return operationView(s.operation), true
	}
	for _, op := range s.history {
		if op.ID == id {
			return operationView(op), true
		}
	}
	return nil, false
}
func (s *managementSettings) save(revision string, plan managementPlan) error {
	s.host.listenerChanges.Lock()
	defer s.host.listenerChanges.Unlock()
	if err := s.host.configurationReady(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("runtime_stopping")
	}
	if s.manager == nil {
		return errors.New("runtime_not_started")
	}
	if s.runningOperation() {
		return errors.New("management_operation_pending")
	}
	if revision != s.revision {
		return errors.New("management_revision_conflict")
	}
	if plan.Version != 1 || !loopbackListen(plan.Listen) || plan.TokenEnv == "" {
		return errors.New("management_config_invalid")
	}
	c := s.host.currentConfig()
	c.Listen, c.TokenEnv = plan.Listen, plan.TokenEnv
	if _, err := effectiveAccounts(c); err != nil {
		return err
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		return err
	}
	if len(raw) > 8192 {
		return errors.New("management_config_too_large")
	}
	if err = store.Atomic(filepath.Join(filepath.Dir(s.path), "management.required"), []byte(managementGuard)); err != nil {
		return err
	}
	if err = store.Atomic(s.path, raw); err != nil {
		return err
	}
	s.saved, s.revision = plan, configRevision(plan)
	return nil
}
func (s *managementSettings) begin(ctx context.Context) (*managementOperation, error) {
	s.host.listenerChanges.Lock()
	defer s.host.listenerChanges.Unlock()
	if err := s.host.configurationReady(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.manager == nil {
		return nil, errors.New("runtime_not_running")
	}
	if s.runningOperation() {
		return nil, errors.New("management_operation_pending")
	}
	secret, err := token(s.saved.TokenEnv)
	if err != nil {
		return nil, err
	}
	var nonce [16]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	change, err := s.manager.prepare(ctx, s.saved.Listen, secret)
	if err != nil {
		s.lastError = "management_prepare_failed"
		return nil, err
	}
	if s.operation != nil {
		s.history = append(s.history, s.operation)
		if len(s.history) > 16 {
			s.history = s.history[len(s.history)-16:]
		}
	}
	op := &managementOperation{ID: hex.EncodeToString(nonce[:]), Revision: s.revision, Address: change.address, State: "queued", plan: s.saved, change: change, done: make(chan struct{})}
	s.operation = op
	s.lastError = ""
	return op, nil
}
func (s *managementSettings) abort(op *managementOperation, category string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if op.State != "queued" {
		return
	}
	s.manager.discard(op.change)
	op.State, op.Error = "failed", category
	s.lastError = category
	close(op.done)
	s.trimCompleted()
}

// The most recent terminal operation counts toward the sixteen-result bound.
// While it is unfinished, sixteen prior terminal results may still be kept.
func (s *managementSettings) trimCompleted() {
	if len(s.history) > 15 {
		s.history = s.history[len(s.history)-15:]
	}
}
func (s *managementSettings) dispatch(op *managementOperation) {
	s.mu.Lock()
	if s.closed || op.State != "queued" {
		s.mu.Unlock()
		s.abort(op, "runtime_stopping")
		return
	}
	op.State = "applying"
	s.publishing.Store(&op.plan)
	s.mu.Unlock()
	go func() {
		// The HTTP response is already flushed. Bound the initial join so
		// status becomes responsive even when an old handler ignores cancel.
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		err := s.manager.commit(ctx, op.change)
		cancel()
		status, match := s.manager.snapshot(op.plan.Listen, op.change.secret)
		active := s.current.Load()
		committed := match && active != nil && active.Plan == op.plan
		if errors.Is(err, context.DeadlineExceeded) && committed {
			s.mu.Lock()
			op.State = "retiring"
			s.mu.Unlock()
			s.manager.finishRetirement()
			err = nil
		}
		status, match = s.manager.snapshot(op.plan.Listen, op.change.secret)
		active = s.current.Load()
		if err == nil && (!match || status.Retiring || active == nil || active.Plan != op.plan) {
			err = errors.New("management_not_applied")
		}
		s.mu.Lock()
		if s.closed || s.manager.parent.Err() != nil {
			err = errors.New("runtime_stopping")
		}
		if err != nil {
			op.State, op.Error = "failed", "management_apply_failed"
			s.lastError = op.Error
		} else {
			op.State = "applied"
			s.lastError = ""
		}
		close(op.done)
		s.trimCompleted()
		s.mu.Unlock()
	}()
}
func (s *managementSettings) close(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	op, manager := s.operation, s.manager
	s.mu.Unlock()
	if op != nil {
		s.abort(op, "runtime_stopping")
	}
	if manager != nil {
		if err := manager.close(ctx); err != nil {
			return err
		}
	}
	if op != nil {
		return waitInbound(ctx, op.done)
	}
	return nil
}

func (h *Host) currentConfig() Config {
	h.mu.Lock()
	c := h.config
	h.mu.Unlock()
	if h.management != nil {
		if active := h.management.current.Load(); active != nil {
			c.Listen, c.TokenEnv = active.Plan.Listen, active.Plan.TokenEnv
		}
	}
	return c
}
func (h *Host) ServeEndpoint(w http.ResponseWriter, r *http.Request, address, secret string) {
	ctx := context.WithValue(r.Context(), managementEndpointKey{}, managementEndpoint{address, secret})
	h.ServeHTTP(w, r.WithContext(ctx))
}
