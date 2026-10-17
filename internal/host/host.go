// Package host composes protocol channels and robot interfaces, without any
// model, provider, prompt or automatic reply dependency.
package host

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/JFSAS/wispcat/internal/instances"
	"github.com/JFSAS/wispcat/internal/native"
	"github.com/JFSAS/wispcat/internal/onebot"
	"github.com/JFSAS/wispcat/internal/service"
	"github.com/JFSAS/wispcat/internal/store"
)

type accountRuntime struct {
	accountLifecycle
	config      Account
	bootstrap   Account
	connections *connectionSettings
	inbound     *inboundSettings
	channel     *service.Service
	worker      accountWorker
	bot         service.BotAdapter
	adapter     *onebot.Server
	listener    net.Listener
}
type Host struct {
	config               Config
	lock                 *os.File
	state                *onebot.State
	accounts             []*accountRuntime
	retiring             []*accountRuntime
	topology             *topologySettings
	management           *managementSettings
	parent               context.Context
	listener             net.Listener
	admin                *http.Server
	token                string
	cancel               context.CancelFunc
	wg                   sync.WaitGroup
	mu                   sync.Mutex
	started              bool
	closed               bool
	requests             chan struct{}
	requestWork          sync.WaitGroup
	shutdownDone         chan struct{}
	shutdownError        error
	workerFactory        *accountWorkerFactory
	listenerChanges      sync.Mutex
	listenerBatchError   string
	configurationPending bool
	configurationError   string
	configurationSync    func(string) error
	configurationWrite   func(string, []byte) error
}

func New(c Config) (*Host, error) {
	return newHost(c, nil)
}

func newHost(c Config, factory *accountWorkerFactory) (*Host, error) {
	bootstrap := c
	var err error
	c, err = effectiveConfig(c)
	if err != nil {
		return nil, err
	}
	if err := store.Directory(c.StateDir); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(c.StateDir, "backend.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		return nil, errors.New("state_root_in_use")
	}
	h := &Host{config: c, lock: lock, requests: make(chan struct{}, 16), workerFactory: factory}
	h.token, _ = token(c.TokenEnv)
	cleanup := func(err error) (*Host, error) {
		for _, a := range h.accounts {
			_ = a.closeResources()
		}
		if h.state != nil {
			_ = h.state.Close()
		}
		_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
		_ = lock.Close()
		return nil, err
	}
	// Reload persisted topology/settings after acquiring exclusive ownership.
	// A previous process may have completed a save while this one waited.
	if err = recoverConfiguration(bootstrap.StateDir); err != nil {
		return cleanup(err)
	}
	c, err = effectiveConfig(bootstrap)
	if err != nil {
		return cleanup(err)
	}
	if factory != nil {
		if err := factory.checkRoot(c); err != nil {
			return cleanup(err)
		}
	}
	h.config = c
	h.token, err = token(c.TokenEnv)
	if err != nil {
		return cleanup(err)
	}
	h.management, err = newManagementSettings(h, bootstrap)
	if err != nil {
		return cleanup(err)
	}
	h.topology, err = newTopologySettings(h, bootstrap.Accounts)
	if err != nil {
		return cleanup(err)
	}
	fallback := ""
	if len(c.Accounts) > 0 {
		fallback = c.Accounts[0].Instance
	}
	catalog, err := instances.Open(c.StateDir, fallback)
	if err != nil {
		return cleanup(err)
	}
	profiles := map[string]instances.Profile{}
	for _, a := range c.Accounts {
		p, e := catalog.Ensure(a.Instance)
		if e != nil {
			return cleanup(e)
		}
		profiles[a.Name] = p
	}
	// Include temporarily disabled instances before allocating any new public
	// IDs, so a later re-enable cannot lose its legacy numbers to a new account.
	registered, err := catalog.List()
	if err != nil {
		return cleanup(err)
	}
	legacy := make([]string, 0, len(registered))
	for _, p := range registered {
		legacy = append(legacy, filepath.Join(p.Directory, "onebot"))
	}
	shared, e := onebot.OpenSharedState(filepath.Join(c.StateDir, "onebot"), legacy)
	if e != nil {
		return cleanup(e)
	}
	h.state = shared
	for _, config := range c.Accounts {
		owner, err := openAccount(c, config, profiles[config.Name], shared, factory)
		if err != nil {
			return cleanup(err)
		}
		owner.connections.configSource = h.currentConfig
		owner.inbound.configSource = h.currentConfig
		owner.inbound.changes = &h.listenerChanges
		owner.inbound.ready = h.configurationReady
		owner.connections.changes = &h.listenerChanges
		owner.connections.ready = h.configurationReady
		for _, original := range h.topology.saved.Accounts {
			if original.Name == config.Name {
				owner.bootstrap = original
				break
			}
		}
		h.accounts = append(h.accounts, owner)
	}
	return h, nil
}
func (h *Host) Start(parent context.Context) error {
	h.topology.mu.Lock()
	defer h.topology.mu.Unlock()
	h.listenerChanges.Lock()
	defer h.listenerChanges.Unlock()
	h.management.mu.Lock()
	defer h.management.mu.Unlock()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.started || h.closed {
		return errors.New("runtime_already_started_or_closed")
	}
	if parent.Err() != nil {
		return parent.Err()
	}
	for _, a := range h.accounts {
		if a.config.Mode == "online" && !native.Supported() {
			return errors.New("native_transport_unavailable")
		}
	}
	initial := map[*accountRuntime][]onebot.NetworkSpec{}
	for _, a := range h.accounts {
		plan, err := a.connections.specs(a.connections.saved)
		if err != nil {
			return err
		}
		initial[a] = plan
	}
	// Bind all listeners before starting any account. A bad topology cannot
	// leave a partial set of live sessions behind.
	listener, err := net.Listen("tcp", h.config.Listen)
	if err != nil {
		return err
	}
	h.listener = listener
	for _, a := range h.accounts {
		a.listener, err = net.Listen("tcp", a.config.Listen)
		if err != nil {
			_ = listener.Close()
			for _, v := range h.accounts {
				if v.listener != nil {
					_ = v.listener.Close()
				}
			}
			return err
		}
		a.adapter.Host = a.listener.Addr().String()
	}
	ctx, cancel := context.WithCancel(parent)
	h.cancel = cancel
	h.started = true
	h.parent = ctx
	for _, a := range h.accounts {
		if err := a.start(ctx, initial[a]); err != nil {
			cancel()
			return err
		}
	}
	h.topology.active = h.topology.revision
	h.management.startLocked(ctx, listener, h.token)
	h.admin = h.management.manager.active.http
	return nil
}
func (h *Host) Address() string {
	if h.management != nil {
		if active := h.management.current.Load(); active != nil {
			return active.Address
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.listener == nil {
		return ""
	}
	return h.listener.Addr().String()
}
func (h *Host) Close(ctx context.Context) error {
	h.mu.Lock()
	if h.shutdownDone == nil {
		h.shutdownDone = make(chan struct{})
		h.closed = true
		go h.shutdown()
	}
	done := h.shutdownDone
	h.mu.Unlock()
	select {
	case <-done:
		h.mu.Lock()
		err := h.shutdownError
		h.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (h *Host) shutdown() {
	h.mu.Lock()
	started, cancel := h.started, h.cancel
	h.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	ctx, stop := context.WithTimeout(context.Background(), 60*time.Second)
	defer stop()
	var failures []error
	if h.management != nil {
		if err := h.management.close(ctx); err != nil {
			failures = append(failures, err)
		}
	}
	if started && h.admin != nil {
		if err := h.admin.Shutdown(ctx); err != nil {
			failures = append(failures, err)
		}
	}
	if h.listener != nil {
		_ = h.listener.Close()
	}
	h.topology.mu.Lock()
	h.topology.closed = true
	h.mu.Lock()
	owners := append(append([]*accountRuntime(nil), h.accounts...), h.retiring...)
	h.mu.Unlock()
	h.topology.mu.Unlock()
	for _, a := range owners {
		a.retire()
	}
	for _, a := range owners {
		if err := waitInbound(ctx, a.done); err != nil {
			failures = append(failures, err)
		}
	}
	h.wg.Wait()
	// No new admissions are possible after Close marks the host closed under
	// the same mutex used by request registration. Keep the root/index alive
	// even after HTTP/caller deadlines while admitted handlers still use them.
	h.requestWork.Wait()
	if h.management != nil {
		_ = h.management.close(context.Background())
	}
	for _, a := range owners {
		<-a.done
		if a.closeError != nil {
			failures = append(failures, a.closeError)
		}
	}
	if h.state != nil {
		if err := h.state.Close(); err != nil {
			failures = append(failures, err)
		}
	}
	if h.lock != nil {
		_ = syscall.Flock(int(h.lock.Fd()), syscall.LOCK_UN)
		_ = h.lock.Close()
	}
	h.mu.Lock()
	h.shutdownError = errors.Join(failures...)
	close(h.shutdownDone)
	h.mu.Unlock()
}
func (h *Host) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	select {
	case h.requests <- struct{}{}:
		defer func() { <-h.requests }()
	default:
		http.Error(w, "management_concurrency_limit", 503)
		return
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		http.Error(w, "runtime_stopping", 503)
		return
	}
	h.requestWork.Add(1)
	h.mu.Unlock()
	defer h.requestWork.Done()
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	endpoint, pinned := r.Context().Value(managementEndpointKey{}).(managementEndpoint)
	if !pinned {
		if active := h.management.current.Load(); active != nil {
			endpoint = managementEndpoint{active.Address, active.secret}
		} else {
			endpoint = managementEndpoint{h.Address(), h.token}
		}
	}
	if r.Host != endpoint.address {
		http.Error(w, "host_not_allowed", 403)
		return
	}
	if origin := r.Header.Get("Origin"); origin != "" && origin != "http://"+r.Host {
		http.Error(w, "origin_not_allowed", 403)
		return
	}
	if serveManagementWeb(w, r) {
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+endpoint.secret)) != 1 {
		http.Error(w, "authentication_required", 401)
		return
	}
	reply := func(code int, value any) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(value)
	}
	if r.URL.Path == "/configuration" || r.URL.Path == "/configuration/recover" {
		h.serveConfiguration(w, r, reply)
		return
	}
	if r.URL.Path == "/management" || r.URL.Path == "/management/apply" || strings.HasPrefix(r.URL.Path, "/management/operations/") {
		h.serveManagement(w, r, reply)
		return
	}
	if r.URL.Path == "/topology" || r.URL.Path == "/topology/apply" {
		h.serveTopology(w, r, reply)
		return
	}
	if r.URL.Path == "/accounts" && r.Method == "GET" {
		h.mu.Lock()
		if h.closed {
			h.mu.Unlock()
			reply(503, map[string]string{"error": "runtime_stopping"})
			return
		}
		owners := append([]*accountRuntime(nil), h.accounts...)
		for _, a := range owners {
			a.requests.Add(1)
		}
		h.mu.Unlock()
		defer func() {
			for _, a := range owners {
				a.requests.Done()
			}
		}()
		result := []any{}
		for _, a := range owners {
			address := ""
			if a.inbound.manager != nil {
				state, _ := a.inbound.manager.snapshot("", "")
				address = state.Address
			} else {
				h.mu.Lock()
				if a.listener != nil {
					address = a.listener.Addr().String()
				}
				h.mu.Unlock()
			}
			result = append(result, map[string]any{"name": a.config.Name, "status": a.channel.Status(), "connections": a.adapter.Statuses(), "onebot_address": address})
		}
		reply(200, result)
		return
	}
	if r.URL.Path == "/storage" && r.Method == "GET" {
		stats, err := h.state.Stats()
		if err != nil {
			reply(503, map[string]string{"error": "state_storage_unavailable"})
			return
		}
		reply(200, stats)
		return
	}
	if r.URL.Path == "/listeners" || r.URL.Path == "/listeners/apply" {
		h.serveListeners(w, r, reply)
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 3 || parts[0] != "accounts" {
		reply(404, map[string]string{"error": "not_found"})
		return
	}
	var a *accountRuntime
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		reply(503, map[string]string{"error": "runtime_stopping"})
		return
	}
	for _, v := range h.accounts {
		if v.config.Name == parts[1] {
			a = v
			break
		}
	}
	if a != nil {
		a.requests.Add(1)
	}
	h.mu.Unlock()
	if a == nil {
		reply(404, map[string]string{"error": "account_not_configured"})
		return
	}
	defer a.requests.Done()
	action := strings.Join(parts[2:], "/")
	if r.Method == "GET" {
		switch action {
		case "status":
			reply(200, a.channel.Status())
		case "login":
			reply(200, a.channel.LoginStatus())
		case "login/image":
			data, err := a.channel.LoginImage(r.URL.Query().Get("id"))
			if err != nil {
				reply(404, map[string]string{"error": "login_image_unavailable"})
				return
			}
			w.Header().Set("Content-Type", "image/png")
			_, _ = w.Write(data)
		case "connections":
			reply(200, a.connections.view())
		case "listener":
			reply(200, a.inbound.view())
		case "actions":
			reply(200, a.adapter.Runtime.Actions())
		case "deliveries":
			reply(200, a.channel.Deliveries())
		case "archive":
			status, err := a.bot.ArchiveStatus()
			if err != nil {
				reply(503, map[string]string{"error": "delivery_archive_unavailable"})
				return
			}
			reply(200, status)
		case "history":
			status, err := a.bot.HistoryStatus()
			if err != nil {
				reply(503, map[string]string{"error": "event_history_unavailable"})
				return
			}
			reply(200, status)
		case "streams":
			status, err := a.adapter.Runtime.Streams.Snapshot("")
			if err != nil {
				reply(503, map[string]string{"error": "stream_storage_unavailable"})
				return
			}
			reply(200, status)
		case "media-cache":
			status, err := a.bot.IncomingMediaStatus()
			if err != nil {
				reply(503, map[string]string{"error": "incoming_media_storage_unavailable"})
				return
			}
			reply(200, status)
		case "outgoing-media":
			status, err := a.bot.OutgoingMediaStatus()
			if err != nil {
				reply(503, map[string]string{"error": "outgoing_media_storage_unavailable"})
				return
			}
			reply(200, status)
		default:
			reply(404, map[string]string{"error": "not_found"})
		}
		return
	}
	if r.Method != "POST" {
		reply(405, map[string]string{"error": "method_not_allowed"})
		return
	}
	if action == "login/import" {
		h.serveCredentialLogin(w, r, a, reply)
		return
	}
	limit := int64(8192)
	if action == "connections" {
		limit = 64 << 10
	}
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	defer r.Body.Close()
	if action == "listener" {
		var input struct {
			Revision string      `json:"revision"`
			Plan     inboundPlan `json:"plan"`
		}
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if dec.Decode(&input) != nil || dec.Decode(new(any)) != io.EOF {
			reply(400, map[string]string{"error": "invalid_listener_config"})
			return
		}
		if err := a.inbound.save(input.Revision, input.Plan); err != nil {
			reply(409, map[string]string{"error": "listener_save_failed"})
			return
		}
		reply(200, a.inbound.view())
		return
	}
	if action == "connections" {
		var input struct {
			Revision string         `json:"revision"`
			Plan     connectionPlan `json:"plan"`
		}
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if dec.Decode(&input) != nil || dec.Decode(new(any)) != io.EOF {
			reply(400, map[string]string{"error": "invalid_connections_config"})
			return
		}
		if err := a.connections.save(input.Revision, input.Plan); err != nil {
			reply(409, map[string]string{"error": "connections_save_failed"})
			return
		}
		reply(200, a.connections.view())
		return
	}
	var body struct{}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if dec.Decode(&body) != nil || dec.Decode(new(any)) != io.EOF {
		reply(400, map[string]string{"error": "expected_empty_json_object"})
		return
	}
	if (action == "online" || action == "login/start" || action == "login/disk") && !native.Supported() {
		reply(409, map[string]string{"error": "native_transport_unavailable"})
		return
	}
	var err error
	switch action {
	case "outgoing-media/compact":
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		result, e := a.bot.ReclaimOutgoingMedia(ctx)
		if e != nil {
			reply(409, map[string]string{"error": "outgoing_media_reclamation_pending"})
			return
		}
		reply(200, result)
		return
	case "media-cache/compact":
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		result, e := a.bot.ReclaimIncomingMedia(ctx)
		if e != nil {
			reply(409, map[string]string{"error": "incoming_media_reclamation_pending"})
			return
		}
		reply(200, result)
		return
	case "streams/clean":
		result, e := a.adapter.Runtime.Streams.Clean("")
		if e != nil {
			reply(503, map[string]string{"error": "stream_storage_unavailable"})
			return
		}
		reply(200, result)
		return
	case "archive/compact":
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		report, e := a.bot.ArchiveCompleted(ctx)
		if e != nil {
			reply(409, map[string]string{"error": "delivery_archive_pending"})
			return
		}
		reply(200, report)
		return
	case "connections/apply":
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		err = a.connections.apply(ctx)
	case "listener/apply":
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		err = a.inbound.apply(ctx)
	case "online":
		err = a.channel.SetMode("online", false)
	case "offline":
		err = a.channel.SetMode("offline", false)
	case "login/start":
		_, err = a.channel.QueueLogin()
	case "login/cancel":
		a.channel.CancelLogin()
	case "login/disk":
		err = a.channel.QueueDiskLogin()
	default:
		reply(404, map[string]string{"error": "not_found"})
		return
	}
	if err != nil {
		reply(409, map[string]string{"error": "account_operation_failed"})
		return
	}
	reply(202, map[string]string{"status": "queued"})
}
