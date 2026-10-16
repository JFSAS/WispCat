package host

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/JFSAS/wechat-runtime/internal/onebot"
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
	mu       sync.Mutex
	listener net.Listener
	http     *http.Server
	epoch    *inboundEpoch
	closed   bool
	done     chan struct{}
}

func newInboundEpoch(parent context.Context, secret string) *inboundEpoch {
	ctx, cancel := context.WithCancel(parent)
	return &inboundEpoch{secret: secret, ctx: ctx, cancel: cancel}
}

func newInboundBinding(adapter *onebot.Server, listener net.Listener, epoch *inboundEpoch) *inboundBinding {
	b := &inboundBinding{listener: listener, epoch: epoch, done: make(chan struct{})}
	b.http = &http.Server{ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		IdleTimeout: 90 * time.Second, MaxHeaderBytes: 16384,
		ConnContext: func(ctx context.Context, conn net.Conn) context.Context {
			return context.WithValue(ctx, inboundConnKey{}, conn)
		}}
	b.http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.mu.Lock()
		epoch := b.epoch
		if b.closed || epoch.ctx.Err() != nil {
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
		adapter.ServeEndpoint(w, r.WithContext(ctx), listener.Addr().String(), epoch.secret)
	})
	return b
}

func (b *inboundBinding) start() { go func() { defer close(b.done); _ = b.http.Serve(b.listener) }() }
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
	mu        sync.Mutex
	adapter   *onebot.Server
	parent    context.Context
	active    *inboundBinding
	listen    string
	retiring  <-chan struct{}
	lastError string
	closed    bool
	done      chan struct{}
}

func newInboundServer(adapter *onebot.Server, parent context.Context, listener net.Listener, listen, secret string) *inboundServer {
	s := &inboundServer{adapter: adapter, parent: parent, listen: listen}
	s.active = newInboundBinding(adapter, listener, newInboundEpoch(parent, secret))
	adapter.SetMediaEndpoint(listener.Addr().String(), secret)
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

func (s *inboundServer) apply(ctx context.Context, listen, secret string) error {
	if !loopbackListen(listen) || len(secret) < 16 || len(secret) > 4096 || strings.ContainsAny(secret, "\r\n") {
		return errors.New("invalid_listener_configuration")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.parent.Err() != nil {
		return errors.New("listener_closed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := waitInbound(ctx, s.retiring); err != nil {
		return err
	}
	s.retiring = nil
	old := s.active
	reuse := s.listen == listen
	select {
	case <-old.done:
		reuse = false
	default:
	}
	if reuse && old.epoch.secret == secret {
		s.lastError = ""
		return nil
	}
	var listener net.Listener
	if !reuse {
		var err error
		listener, err = net.Listen("tcp", listen)
		if err != nil {
			s.lastError = "listener_bind_failed"
			return errors.New("listener_bind_failed")
		}
		if err = ctx.Err(); err != nil {
			_ = listener.Close()
			return err
		}
	}
	epoch := newInboundEpoch(s.parent, secret)
	previous := old.epoch
	if reuse {
		old.mu.Lock()
		// Publish links before admitting any request with the new credentials.
		// The admission lock prevents a new epoch from returning an old-token
		// media URL in the interval between these two configuration writes.
		s.adapter.SetMediaEndpoint(old.listener.Addr().String(), secret)
		old.epoch = epoch
		old.mu.Unlock()
	} else {
		s.active = newInboundBinding(s.adapter, listener, epoch)
		s.listen = listen
		s.adapter.SetMediaEndpoint(listener.Addr().String(), secret)
	}
	if !reuse {
		s.active.start()
		old.stop()
	}
	previous.cancel()
	done := make(chan struct{})
	s.retiring = done
	go func() {
		previous.requests.Wait()
		if !reuse {
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

func (s *inboundServer) close(ctx context.Context) error {
	s.mu.Lock()
	if s.done == nil {
		s.closed = true
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
