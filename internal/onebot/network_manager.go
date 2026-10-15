package onebot

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/url"
	"sort"
	"sync"
)

// NetworkSpec is an in-memory plan. Credentials are never serialized.
type NetworkSpec struct {
	Name   string `json:"name"`
	Kind   string `json:"kind"`
	URL    string `json:"url"`
	Role   string `json:"role,omitempty"`
	Secret string `json:"-"`
}
type networkWorker struct {
	signature string
	cancel    context.CancelFunc
	done      chan struct{}
}
type NetworkManager struct {
	server  *Server
	parent  context.Context
	mu      sync.Mutex
	workers map[string]*networkWorker
	closed  bool
}

func NewNetworkManager(server *Server, parent context.Context) *NetworkManager {
	return &NetworkManager{server: server, parent: parent, workers: map[string]*networkWorker{}}
}
func networkSignature(s NetworkSpec) string {
	hash := sha256.Sum256([]byte(s.Name + "\x00" + s.Kind + "\x00" + s.URL + "\x00" + s.Role + "\x00" + s.Secret))
	return hex.EncodeToString(hash[:])
}
func validateNetworks(plan []NetworkSpec) error {
	if len(plan) > 32 {
		return errors.New("connection_limit")
	}
	names := map[string]bool{}
	for _, spec := range plan {
		u, err := url.Parse(spec.URL)
		if spec.Name == "" || len(spec.Name) > 64 || names[spec.Name] || err != nil || u.Host == "" || u.User != nil || u.Fragment != "" {
			return errors.New("invalid_network_plan")
		}
		names[spec.Name] = true
		switch spec.Kind {
		case "wsClient":
			if (u.Scheme != "ws" && u.Scheme != "wss") || (spec.Role != "" && spec.Role != "Universal" && spec.Role != "API" && spec.Role != "Event") {
				return errors.New("invalid_network_plan")
			}
		case "httpClient":
			if u.Scheme != "http" && u.Scheme != "https" {
				return errors.New("invalid_network_plan")
			}
		default:
			return errors.New("invalid_network_kind")
		}
	}
	return nil
}

// Apply serializes topology changes. Existing workers are stopped and joined
// before names can be reused; unchanged connections keep running.
func (m *NetworkManager) Apply(ctx context.Context, plan []NetworkSpec) error {
	if err := validateNetworks(plan); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errors.New("network_manager_closed")
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	desired := map[string]NetworkSpec{}
	for _, spec := range plan {
		desired[spec.Name] = spec
	}
	for name, w := range m.workers {
		spec, wanted := desired[name]
		same := wanted && w.signature == networkSignature(spec)
		select {
		case <-w.done:
			same = false
		default:
		}
		if same {
			continue
		}
		w.cancel()
		select {
		case <-w.done:
			delete(m.workers, name)
			m.server.mu.Lock()
			delete(m.server.statuses, name)
			m.server.mu.Unlock()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for _, spec := range plan {
		if _, exists := m.workers[spec.Name]; exists {
			continue
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		child, cancel := context.WithCancel(m.parent)
		w := &networkWorker{signature: networkSignature(spec), cancel: cancel, done: make(chan struct{})}
		m.workers[spec.Name] = w
		go func(spec NetworkSpec, w *networkWorker) {
			defer close(w.done)
			var err error
			switch spec.Kind {
			case "wsClient":
				err = m.server.RunReverse(child, ReverseConfig{Name: spec.Name, URL: spec.URL, Token: spec.Secret, Role: spec.Role})
			case "httpClient":
				err = m.server.RunPost(child, PostConfig{Name: spec.Name, URL: spec.URL, Secret: spec.Secret})
			}
			if err != nil {
				m.server.finish(spec.Name, spec.Kind, err)
			}
		}(spec, w)
	}
	return nil
}
func (m *NetworkManager) Names() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	names := []string{}
	for name := range m.workers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
func (m *NetworkManager) Close(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	for _, w := range m.workers {
		w.cancel()
	}
	for name, w := range m.workers {
		select {
		case <-w.done:
			delete(m.workers, name)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
