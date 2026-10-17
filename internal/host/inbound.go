package host

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/JFSAS/wispcat/internal/onebot"
)

type inboundEpoch struct {
	secret   string
	ctx      context.Context
	cancel   context.CancelFunc
	requests sync.WaitGroup
}

type inboundConnKey struct{}

// A binding owns one socket; epochs replace its authentication without changing
// an ephemeral port. Admission and request registration share the same lock.
type inboundBinding struct {
	mu          sync.Mutex
	listener    net.Listener
	http        *http.Server
	epoch       *inboundEpoch
	target      listenerTarget
	paused      bool
	connections map[*inboundConnection]struct{}
	closed      bool
	done        chan struct{}
}

func newInboundEpoch(parent context.Context, secret string) *inboundEpoch {
	ctx, cancel := context.WithCancel(parent)
	return &inboundEpoch{secret: secret, ctx: ctx, cancel: cancel}
}

func newInboundBinding(target listenerTarget, listener net.Listener, epoch *inboundEpoch) *inboundBinding {
	b := &inboundBinding{listener: listener, epoch: epoch, target: target, connections: map[*inboundConnection]struct{}{}, done: make(chan struct{})}
	b.http = &http.Server{ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		IdleTimeout: 90 * time.Second, MaxHeaderBytes: 16384,
		ConnContext: func(ctx context.Context, conn net.Conn) context.Context {
			return context.WithValue(ctx, inboundConnKey{}, conn)
		}}
	b.http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.mu.Lock()
		epoch := b.epoch
		target := b.target
		conn, owned := r.Context().Value(inboundConnKey{}).(*inboundConnection)
		_, tracked := b.connections[conn]
		if b.closed || b.paused || epoch.ctx.Err() != nil || !owned || !tracked || conn.closed.Load() || conn.epoch != epoch {
			b.mu.Unlock()
			http.Error(w, "listener_stopping", 503)
			return
		}
		epoch.requests.Add(1)
		b.mu.Unlock()
		defer epoch.requests.Done()
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		// Closing the connection also interrupts a partial HTTP request body.
		// http.Server.Shutdown alone would leave hijacked WebSockets alive.
		stop := context.AfterFunc(epoch.ctx, func() {
			cancel()
			if conn, ok := r.Context().Value(inboundConnKey{}).(net.Conn); ok {
				_ = conn.Close()
			}
		})
		defer stop()
		target.serve(w, r.WithContext(ctx), listener.Addr().String(), epoch.secret)
	})
	return b
}

func (b *inboundBinding) start() {
	go func() { defer close(b.done); _ = b.http.Serve(inboundListener{Listener: b.listener, binding: b}) }()
}
func (b *inboundBinding) stop() {
	b.mu.Lock()
	b.closed = true
	b.epoch.cancel()
	b.mu.Unlock()
	_ = b.http.Close()
	_ = b.listener.Close()
}

type inboundStatus struct {
	Listen   string `json:"listen"`
	Address  string `json:"address"`
	State    string `json:"state"`
	Retiring bool   `json:"retiring"`
	Error    string `json:"error,omitempty"`
}

// inboundServer serializes replacement and teardown. A new port is reserved
// before the old binding is touched. At most one old epoch may be retiring;
// another apply must join it before allocating another binding.
type inboundServer struct {
	operations sync.Mutex
	target     listenerTarget
	prepared   *listenerChange
	mu         sync.Mutex
	parent     context.Context
	active     *inboundBinding
	listen     string
	retiring   <-chan struct{}
	lastError  string
	closed     bool
	done       chan struct{}
}

type listenerTarget struct {
	serve   func(http.ResponseWriter, *http.Request, string, string)
	publish func(string, string)
}

type listenerChange struct {
	listen, secret, address string
	listener                net.Listener
	reuse, noop             bool
}

func newInboundServer(adapter *onebot.Server, parent context.Context, listener net.Listener, listen, secret string) *inboundServer {
	return newListenerServer(listenerTarget{serve: adapter.ServeEndpoint, publish: adapter.SetMediaEndpoint}, parent, listener, listen, secret)
}
func newListenerServer(target listenerTarget, parent context.Context, listener net.Listener, listen, secret string) *inboundServer {
	s := &inboundServer{target: target, parent: parent, listen: listen}
	s.active = newInboundBinding(target, listener, newInboundEpoch(parent, secret))
	target.publish(listener.Addr().String(), secret)
	s.active.start()
	return s
}

func waitInbound(ctx context.Context, done <-chan struct{}) error {
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *inboundServer) snapshot(listen, secret string) (inboundStatus, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := inboundStatus{Listen: s.listen, Address: s.active.listener.Addr().String(), State: "listening", Error: s.lastError}
	if s.retiring != nil {
		select {
		case <-s.retiring:
		default:
			status.Retiring = true
		}
	}
	select {
	case <-s.active.done:
		status.State = "stopped"
	default:
	}
	if s.closed || s.parent.Err() != nil {
		status.State = "stopping"
	}
	match := status.State == "listening" && s.listen == listen && s.active.epoch.secret == secret
	return status, match
}

func (s *inboundServer) prepare(ctx context.Context, listen, secret string) (*listenerChange, error) {
	if !loopbackListen(listen) || len(secret) < 16 || len(secret) > 4096 || strings.ContainsAny(secret, "\r\n") {
		return nil, errors.New("invalid_listener_configuration")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.parent.Err() != nil {
		return nil, errors.New("listener_closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.prepared != nil {
		return nil, errors.New("listener_change_pending")
	}
	if err := waitInbound(ctx, s.retiring); err != nil {
		return nil, err
	}
	s.retiring = nil
	old := s.active
	change := &listenerChange{listen: listen, secret: secret, address: old.listener.Addr().String(), reuse: s.listen == listen}
	select {
	case <-old.done:
		change.reuse = false
	default:
	}
	change.noop = change.reuse && old.epoch.secret == secret
	if !change.reuse {
		var err error
		change.listener, err = net.Listen("tcp", listen)
		if err != nil {
			s.lastError = "listener_bind_failed"
			return nil, errors.New(s.lastError)
		}
		change.address = change.listener.Addr().String()
		if err = ctx.Err(); err != nil {
			_ = change.listener.Close()
			return nil, err
		}
	}
	s.prepared = change
	return change, nil
}
func (s *inboundServer) discard(change *listenerChange) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.prepared != change {
		return
	}
	s.prepared = nil
	if change.listener != nil {
		_ = change.listener.Close()
	}
}
func (s *inboundServer) commit(ctx context.Context, change *listenerChange) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.prepared != change {
		return errors.New("listener_change_cancelled")
	}
	if s.closed || s.parent.Err() != nil {
		if change.listener != nil {
			_ = change.listener.Close()
		}
		if s.prepared == change {
			s.prepared = nil
		}
		return errors.New("listener_change_cancelled")
	}
	if err := ctx.Err(); err != nil {
		s.prepared = nil
		if change.listener != nil {
			_ = change.listener.Close()
		}
		return err
	}
	s.prepared = nil
	old := s.active
	if change.noop {
		// Metadata such as an environment reference may change while the
		// resolved credentials stay identical. Publish without retiring peers.
		s.target.publish(old.listener.Addr().String(), change.secret)
		s.lastError = ""
		return nil
	}
	epoch := newInboundEpoch(s.parent, change.secret)
	previous := old.epoch
	if change.reuse {
		// Retire idle connections too. A parsed request may enter the handler
		// after cancellation; its immutable connection epoch must stay old.
		old.pause()
		old.mu.Lock()
		s.target.publish(old.listener.Addr().String(), change.secret)
		old.epoch = epoch
		old.paused = false
		old.mu.Unlock()
	} else {
		s.active = newInboundBinding(s.target, change.listener, epoch)
		s.listen = change.listen
		s.target.publish(change.address, change.secret)
		s.active.start()
		old.stop()
	}
	previous.cancel()
	done := make(chan struct{})
	s.retiring = done
	go func() {
		previous.requests.Wait()
		if !change.reuse {
			<-old.done
		}
		close(done)
	}()
	if err := waitInbound(ctx, done); err != nil {
		s.lastError = "listener_retirement_pending"
		return err
	}
	s.retiring = nil
	s.lastError = ""
	return nil
}
func (s *inboundServer) apply(ctx context.Context, listen, secret string) error {
	s.operations.Lock()
	defer s.operations.Unlock()
	change, err := s.prepare(ctx, listen, secret)
	if err != nil {
		return err
	}
	return s.commit(ctx, change)
}
func (s *inboundServer) finishRetirement() {
	s.mu.Lock()
	done := s.retiring
	s.mu.Unlock()
	_ = waitInbound(context.Background(), done)
	s.mu.Lock()
	if s.retiring == done {
		s.retiring = nil
		s.lastError = ""
	}
	s.mu.Unlock()
}

func (s *inboundServer) close(ctx context.Context) error {
	s.mu.Lock()
	if s.done == nil {
		s.closed = true
		if s.prepared != nil {
			if s.prepared.listener != nil {
				_ = s.prepared.listener.Close()
			}
			s.prepared = nil
		}
		s.done = make(chan struct{})
		s.active.stop()
		active, retiring, done := s.active, s.retiring, s.done
		go func() {
			_ = waitInbound(context.Background(), retiring)
			active.epoch.requests.Wait()
			<-active.done
			close(done)
		}()
	}
	done := s.done
	s.mu.Unlock()
	return waitInbound(ctx, done)
}
