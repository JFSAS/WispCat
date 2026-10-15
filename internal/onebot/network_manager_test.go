package onebot

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestNetworkManagerKeepsUnchangedAndReplacesChangedWorkers(t *testing.T) {
	r, _ := fixture(t)
	server := NewServer(r, strings.Repeat("t", 32), "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager := NewNetworkManager(server, ctx)
	var attempts atomic.Int32
	connected := make(chan string, 4)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		connection, err := (&websocket.Upgrader{}).Upgrade(w, q, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		var message map[string]any
		if connection.ReadJSON(&message) != nil {
			return
		}
		attempts.Add(1)
		connected <- q.Header.Get("Authorization")
		for {
			if _, _, err = connection.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer peer.Close()
	spec := NetworkSpec{Name: "bot", Kind: "wsClient", URL: "ws" + strings.TrimPrefix(peer.URL, "http"), Secret: "first"}
	if err := manager.Apply(ctx, []NetworkSpec{spec}); err != nil {
		t.Fatal(err)
	}
	select {
	case value := <-connected:
		if value != "Bearer first" {
			t.Fatal(value)
		}
	case <-time.After(time.Second):
		t.Fatal("connection not started")
	}
	if err := manager.Apply(ctx, []NetworkSpec{spec}); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 1 {
		t.Fatal("unchanged connection restarted")
	}
	invalid := spec
	invalid.URL = "file:///tmp/socket"
	if err := manager.Apply(ctx, []NetworkSpec{invalid}); err == nil || len(manager.Names()) != 1 {
		t.Fatal("invalid plan changed active workers")
	}
	spec.Secret = "second"
	if err := manager.Apply(ctx, []NetworkSpec{spec}); err != nil {
		t.Fatal(err)
	}
	select {
	case value := <-connected:
		if value != "Bearer second" {
			t.Fatal(value)
		}
	case <-time.After(time.Second):
		t.Fatal("changed connection not replaced")
	}
	if attempts.Load() != 2 {
		t.Fatal("replacement overlap or missing")
	}
	if err := manager.Apply(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if len(manager.Names()) != 0 {
		t.Fatal("removed connection retained")
	}
	_ = manager.Close(ctx)
	_ = server.Close(ctx)
}
func TestNetworkManagerDeadlineDoesNotReplaceUnstoppedWorker(t *testing.T) {
	r, _ := fixture(t)
	server := NewServer(r, strings.Repeat("t", 32), "")
	manager := NewNetworkManager(server, context.Background())
	done := make(chan struct{})
	var cancelled atomic.Bool
	worker := &networkWorker{signature: "old", done: done, cancel: func() { cancelled.Store(true) }}
	manager.workers["bot"] = worker
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	err := manager.Apply(ctx, []NetworkSpec{{Name: "bot", Kind: "wsClient", URL: "ws://127.0.0.1:1", Secret: "new"}})
	if err == nil || !cancelled.Load() || manager.workers["bot"] != worker {
		t.Fatal("timed-out shutdown replaced a live worker")
	}
	close(done)
	if err = manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = server.Close(context.Background())
}
