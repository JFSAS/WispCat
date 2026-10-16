package onebot

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"
)

const responseWriteTimeout = 5 * time.Second

// beginHTTPWrite starts the write budget after action/download work. Cancellation
// interrupts blocked network writes, and the cleanup joins both callbacks before
// returning the ResponseWriter to net/http. A non-network writer without deadline
// support remains its caller's responsibility.
func (s *Server) beginHTTPWrite(parent context.Context, w http.ResponseWriter) (*httpWriteBudget, error) {
	budget := &httpWriteBudget{request: parent, shutdown: s.shutdown, controller: http.NewResponseController(w)}
	if err := budget.renew(); err != nil {
		return nil, err
	}
	budget.stopRequest = afterHTTPJoin(parent, budget.expire)
	budget.stopServer = afterHTTPJoin(s.shutdown, budget.expire)
	return budget, nil
}

// Cancellation is irreversible for this response: a later stream frame must
// never renew a deadline that the cancellation callback has already expired.
type httpWriteBudget struct {
	mu                      sync.Mutex
	request, shutdown       context.Context
	controller              *http.ResponseController
	expired                 bool
	stopRequest, stopServer func()
}

func (b *httpWriteBudget) renew() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	err := b.request.Err()
	if err == nil {
		err = b.shutdown.Err()
	}
	if b.expired || err != nil {
		b.expired = true
		_ = b.controller.SetWriteDeadline(time.Now())
		if err != nil {
			return err
		}
		return context.Canceled
	}
	if err := b.controller.SetWriteDeadline(time.Now().Add(responseWriteTimeout)); !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return nil
}

func (b *httpWriteBudget) expire() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.expired = true
	_ = b.controller.SetWriteDeadline(time.Now())
}

func (b *httpWriteBudget) close() {
	b.stopRequest()
	b.stopServer()
	// net/http must finish buffered headers/chunks under the same deadline.
	// Its HTTP/1 owner clears it after finishRequest, before keep-alive reuse.
}

func afterHTTPJoin(ctx context.Context, callback func()) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(done); callback() })
	return func() {
		if !stop() {
			<-done
		}
	}
}
