package onebot

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JFSAS/wechat-runtime/pkg/bot"
	"github.com/gorilla/websocket"
)

const maxFrameBytes = 24 << 20

type ConnectionStatus struct {
	Name       string `json:"name"`
	Kind       string `json:"kind"`
	State      string `json:"state"`
	Error      string `json:"error,omitempty"`
	Reconnects uint64 `json:"reconnects"`
	Since      int64  `json:"since"`
	Cursor     uint64 `json:"cursor"`
}
type ReverseConfig struct {
	Name  string `json:"name"`
	URL   string `json:"url"`
	Token string `json:"token,omitempty"`
	Role  string `json:"role,omitempty"`
}
type Server struct {
	Runtime     *Runtime
	Token       string
	Host        string
	mu          sync.Mutex
	connections map[*websocket.Conn]struct{}
	statuses    map[string]ConnectionStatus
	closing     bool
	wg          sync.WaitGroup
	shutdown    context.Context
	cancel      context.CancelFunc
	httpSlots   chan struct{}
}

func NewServer(runtime *Runtime, token, host string) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	server := &Server{Runtime: runtime, Token: token, Host: host, connections: map[*websocket.Conn]struct{}{}, statuses: map[string]ConnectionStatus{}, shutdown: ctx, cancel: cancel, httpSlots: make(chan struct{}, 16)}
	runtime.MediaURL = server.mediaURL
	return server
}
func (s *Server) Statuses() []ConnectionStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := []ConnectionStatus{}
	for _, v := range s.statuses {
		r = append(r, v)
	}
	return r
}
func (s *Server) status(name, kind, state, category string, reconnect bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.statuses[name]
	v.Name, v.Kind, v.State, v.Error = name, kind, state, category
	v.Since = time.Now().Unix()
	if reconnect {
		v.Reconnects++
	}
	s.statuses[name] = v
}
func (s *Server) progress(name string, cursor uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := s.statuses[name]
	status.Cursor = cursor
	s.statuses[name] = status
}
func (s *Server) Close(ctx context.Context) error {
	s.mu.Lock()
	s.closing = true
	s.cancel()
	for c := range s.connections {
		_ = c.Close()
	}
	s.mu.Unlock()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s *Server) authorize(w http.ResponseWriter, r *http.Request) bool {
	if s.Host != "" && r.Host != s.Host {
		http.Error(w, "host_not_allowed", 403)
		return false
	}
	if origin := r.Header.Get("Origin"); origin != "" {
		u, e := url.Parse(origin)
		if e != nil || u.Host != r.Host || (u.Scheme != "http" && u.Scheme != "https") {
			http.Error(w, "origin_not_allowed", 403)
			return false
		}
	}
	auth := r.Header.Get("Authorization")
	if auth == "" && r.URL.Query().Get("access_token") != "" {
		auth = "Bearer " + r.URL.Query().Get("access_token")
	}
	if s.Token == "" {
		http.Error(w, "server_token_not_configured", 503)
		return false
	}
	if auth == "" {
		http.Error(w, "authentication_required", 401)
		return false
	}
	if subtle.ConstantTimeCompare([]byte(auth), []byte("Bearer "+s.Token)) != 1 {
		http.Error(w, "authentication_failed", 403)
		return false
	}
	return true
}
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		http.Error(w, "runtime_stopping", 503)
		return
	}
	s.wg.Add(1)
	s.mu.Unlock()
	defer s.wg.Done()
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.URL.Path == "/_media" {
		s.serveMedia(w, r)
		return
	}
	if !s.authorize(w, r) {
		return
	}
	if websocket.IsWebSocketUpgrade(r) {
		role := "Universal"
		switch strings.TrimSuffix(r.URL.Path, "/") {
		case "", "/ws":
		case "/api":
			role = "API"
		case "/event":
			role = "Event"
		default:
			http.NotFound(w, r)
			return
		}
		snapshot := s.Runtime.Channel.Snapshot()
		if snapshot.Error != "" {
			http.Error(w, "runtime_storage_unavailable", 503)
			return
		}
		if snapshot.Account == "" {
			http.Error(w, "account_unavailable", 409)
			return
		}
		s.mu.Lock()
		full := s.closing || len(s.connections) >= 64
		s.mu.Unlock()
		if full {
			http.Error(w, "connection_limit", 503)
			return
		}
		conn, err := (&websocket.Upgrader{HandshakeTimeout: 5 * time.Second}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		s.serveConn(r.Context(), conn, role, "")
		return
	}
	respond := func(status int, value any) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(value)
	}
	select {
	case s.httpSlots <- struct{}{}:
		defer func() { <-s.httpSlots }()
	default:
		respond(503, fail(1500, "action_concurrency_limit"))
		return
	}
	if r.URL.Path == "/_actions" && r.Method == "GET" {
		respond(200, s.Runtime.Actions())
		return
	}
	if r.URL.Path == "/_connections" && r.Method == "GET" {
		respond(200, s.Statuses())
		return
	}
	action := strings.Trim(r.URL.Path, "/")
	if action == "" || strings.Contains(action, "/") || !s.Runtime.known(action) {
		respond(404, fail(1404, "action_not_supported"))
		return
	}
	q := Request{Action: action, Params: map[string]json.RawMessage{}}
	switch r.Method {
	case http.MethodGet:
		for k, values := range r.URL.Query() {
			if k == "access_token" {
				continue
			}
			if len(values) != 1 {
				respond(400, fail(1400, "duplicate_parameter"))
				return
			}
			raw, _ := json.Marshal(values[0])
			q.Params[k] = raw
		}
	case http.MethodPost:
		r.Body = http.MaxBytesReader(w, r.Body, maxFrameBytes)
		defer r.Body.Close()
		content, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
		switch content {
		case "application/json":
			dec := json.NewDecoder(r.Body)
			if dec.Decode(&q.Params) != nil || dec.Decode(new(any)) != io.EOF || q.Params == nil {
				respond(400, fail(1400, "invalid_json"))
				return
			}
		case "application/x-www-form-urlencoded":
			if r.ParseForm() != nil {
				respond(400, fail(1400, "invalid_form"))
				return
			}
			for k, values := range r.PostForm {
				if len(values) != 1 {
					respond(400, fail(1400, "duplicate_parameter"))
					return
				}
				raw, _ := json.Marshal(values[0])
				q.Params[k] = raw
			}
		default:
			respond(406, fail(1400, "unsupported_content_type"))
			return
		}
	default:
		respond(405, fail(1400, "method_not_allowed"))
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stop := context.AfterFunc(s.shutdown, cancel)
	defer stop()
	if streamAction(q.Action) {
		controller := http.NewResponseController(w)
		defer func() { _ = controller.SetWriteDeadline(time.Time{}) }()
		w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
		w.WriteHeader(200)
		_ = s.Runtime.InvokeFrames(ctx, q, func(out Response) error {
			_ = controller.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := json.NewEncoder(w).Encode(out); err != nil {
				return err
			}
			return controller.Flush()
		})
		return
	}
	respond(200, s.Runtime.Invoke(ctx, q))
}

type socketWriter struct {
	mu   sync.Mutex
	conn *websocket.Conn
}

func (w *socketWriter) json(value any) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_ = w.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return w.conn.WriteJSON(value)
}
func (s *Server) serveConn(parent context.Context, c *websocket.Conn, role, consumer string) {
	s.mu.Lock()
	if s.closing || len(s.connections) >= 64 {
		s.mu.Unlock()
		_ = c.Close()
		return
	}
	s.connections[c] = struct{}{}
	s.wg.Add(1)
	s.mu.Unlock()
	defer func() { _ = c.Close(); s.mu.Lock(); delete(s.connections, c); s.mu.Unlock(); s.wg.Done() }()
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	writer := &socketWriter{conn: c}
	snap := s.Runtime.Channel.Snapshot()
	if snap.Error != "" {
		if consumer != "" {
			s.status(consumer, "wsClient", "degraded", "runtime_storage_unavailable", false)
		}
		return
	}
	if snap.Account == "" {
		return
	}
	self, err := s.Runtime.State.ID(snap.Account, "user", snap.Self.ID)
	if err != nil {
		return
	}
	cursor := snap.Cursor
	if consumer != "" && role != "API" {
		cursor, err = s.Runtime.State.cursor(snap.Account, consumer, snap.Cursor)
		if err != nil {
			s.status(consumer, "wsClient", "degraded", "state_cursor_unavailable", false)
			return
		}
		if err = s.Runtime.State.setCursor(snap.Account, consumer, cursor); err != nil {
			return
		}
		s.progress(consumer, cursor)
	}
	if role != "API" {
		if writer.json(map[string]any{"time": time.Now().Unix(), "self_id": self, "post_type": "meta_event", "meta_event_type": "lifecycle", "sub_type": "connect"}) != nil {
			return
		}
	}
	c.SetReadLimit(maxFrameBytes)
	_ = c.SetReadDeadline(time.Now().Add(90 * time.Second))
	c.SetPongHandler(func(string) error { return c.SetReadDeadline(time.Now().Add(90 * time.Second)) })
	var actions sync.WaitGroup
	slots := make(chan struct{}, 8)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		defer cancel()
		for {
			kind, raw, e := c.ReadMessage()
			if e != nil {
				return
			}
			if kind != websocket.TextMessage {
				_ = writer.json(fail(1400, "json_text_frame_required"))
				continue
			}
			if role == "Event" {
				continue
			}
			var q Request
			if json.Unmarshal(raw, &q) != nil || q.Action == "" {
				if writer.json(fail(1400, "invalid_action_request")) != nil {
					return
				}
				continue
			}
			select {
			case slots <- struct{}{}:
			default:
				out := fail(1500, "action_concurrency_limit")
				out.Echo = q.Echo
				if writer.json(out) != nil {
					return
				}
				continue
			}
			actions.Add(1)
			go func(q Request) {
				defer actions.Done()
				defer func() { <-slots }()
				current := s.Runtime.Channel.Snapshot()
				if current.Account != snap.Account {
					cancel()
					return
				}
				if s.Runtime.InvokeFrames(ctx, q, func(out Response) error {
					if s.Runtime.Channel.Snapshot().Account != snap.Account {
						return bot.ErrScope
					}
					return writer.json(out)
				}) != nil {
					cancel()
				}
			}(q)
		}
	}()
	defer func() { cancel(); _ = c.Close(); <-readerDone; actions.Wait() }()
	poll := time.NewTicker(100 * time.Millisecond)
	defer poll.Stop()
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	ping := time.NewTicker(30 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ping.C:
			if c.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)) != nil {
				return
			}
		case <-heartbeat.C:
			if role != "API" {
				current := s.Runtime.Channel.Snapshot()
				if writer.json(map[string]any{"time": time.Now().Unix(), "self_id": self, "post_type": "meta_event", "meta_event_type": "heartbeat", "interval": 15000, "status": map[string]bool{"online": current.Online, "good": current.Online}}) != nil {
					return
				}
			}
		case <-poll.C:
			current := s.Runtime.Channel.Snapshot()
			if current.Account != snap.Account {
				return
			}
			if role == "API" {
				continue
			}
			events, e := s.Runtime.Channel.Events(snap.Account, cursor, 100)
			if e != nil {
				if consumer != "" {
					s.status(consumer, "wsClient", "degraded", "event_cursor_unavailable", false)
				}
				return
			}
			for _, ev := range events {
				if !ev.LocalEcho && (!ev.Own || ev.Conversation.Kind == "filehelper") {
					v, e := s.Runtime.Convert(ev)
					if e != nil {
						return
					}
					if writer.json(v) != nil {
						return
					}
				}
				cursor = ev.Sequence
				if consumer != "" {
					if s.Runtime.State.setCursor(snap.Account, consumer, cursor) != nil {
						return
					}
					s.progress(consumer, cursor)
				}
			}
		}
	}
}
func (s *Server) RunReverse(ctx context.Context, cfg ReverseConfig) (runErr error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(s.shutdown, cancel)
	defer stop()
	u, err := url.Parse(cfg.URL)
	if err != nil || u.Host == "" || (u.Scheme != "ws" && u.Scheme != "wss") || u.User != nil || u.Fragment != "" || cfg.Name == "" || len(cfg.Name) > 64 {
		return errors.New("invalid_reverse_configuration")
	}
	if cfg.Role == "" {
		cfg.Role = "Universal"
	}
	if cfg.Role != "Universal" && cfg.Role != "Event" && cfg.Role != "API" {
		return errors.New("invalid_reverse_role")
	}
	s.mu.Lock()
	if _, exists := s.statuses[cfg.Name]; exists || s.closing {
		s.mu.Unlock()
		return errors.New("duplicate_or_closed_connection")
	}
	s.statuses[cfg.Name] = ConnectionStatus{Name: cfg.Name}
	s.wg.Add(1)
	s.mu.Unlock()
	defer s.wg.Done()
	defer func() { s.finish(cfg.Name, "wsClient", runErr) }()
	delay := time.Second
	for {
		if ctx.Err() != nil {
			s.status(cfg.Name, "wsClient", "stopped", "", false)
			return nil
		}
		s.mu.Lock()
		closing := s.closing
		s.mu.Unlock()
		if closing {
			return nil
		}
		snap := s.Runtime.Channel.Snapshot()
		if snap.Error != "" {
			return bot.ErrStorage
		}
		if snap.Account == "" {
			s.status(cfg.Name, "wsClient", "waiting_account", "", false)
			if !waitContext(ctx, time.Second) {
				return nil
			}
			continue
		}
		self, e := s.Runtime.State.ID(snap.Account, "user", snap.Self.ID)
		if e != nil {
			return e
		}
		header := http.Header{"X-Self-ID": []string{strconv.FormatInt(self, 10)}, "X-Client-Role": []string{cfg.Role}, "User-Agent": []string{"OneBot/11"}}
		if cfg.Token != "" {
			header.Set("Authorization", "Bearer "+cfg.Token)
		}
		s.status(cfg.Name, "wsClient", "connecting", "", false)
		dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
		conn, response, e := dialer.DialContext(ctx, cfg.URL, header)
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		if e == nil {
			s.status(cfg.Name, "wsClient", "connected", "", false)
			delay = time.Second
			s.serveConn(ctx, conn, cfg.Role, cfg.Name)
		}
		if ctx.Err() != nil {
			return nil
		}
		s.status(cfg.Name, "wsClient", "reconnecting", "connection_closed_or_failed", true)
		if !waitContext(ctx, delay) {
			return nil
		}
		delay *= 2
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}
	}
}
func waitContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
