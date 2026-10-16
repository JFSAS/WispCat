package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JFSAS/wechat-runtime/pkg/client"
)

func TestMCPDocsAndWriteGate(t *testing.T) {
	var calls atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer synthetic-token" {
			t.Error("missing token")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "retcode": 0, "data": map[string]bool{"online": true}})
	}))
	defer peer.Close()
	c, err := client.New(peer.URL, "synthetic-token")
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"docs", "read", "write"} {
		s := &Server{Mode: mode, Client: c}
		if mode == "docs" {
			s.Client = nil
		}
		input := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05"}}
{"jsonrpc":"2.0","id":2,"method":"tools/list"}
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"query_action","arguments":{"action":"send_private_msg","params":{}}}}
{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"invoke_action","arguments":{"action":"get_status","params":{}}}}
`
		before := calls.Load()
		session := startMCPSession(t, s)
		session.send(input)
		replies := map[string]mcpReply{}
		for range 4 {
			reply := session.next()
			replies[string(reply.ID)] = reply
		}
		session.finish()
		if mode != "write" && calls.Load() != before {
			t.Fatal("MCP gate allowed a write-only tool")
		}
		if mode == "write" && calls.Load() != before+1 {
			t.Fatal("write tool not dispatched")
		}
		var gate struct {
			IsError bool `json:"isError"`
		}
		if json.Unmarshal(replies["3"].Result, &gate) != nil || !gate.IsError {
			t.Fatal("query_action executed a side-effect action")
		}
	}
}

func TestMCPCancellationReturnsWhileInputRemainsOpen(t *testing.T) {
	input, writer := io.Pipe()
	defer input.Close()
	defer writer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- (&Server{Mode: "docs"}).Run(ctx, input, io.Discard) }()
	if _, err := writer.Write([]byte("{")); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("MCP waited for another stdin line after cancellation")
	}
}

func TestMCPStreamingActionsKeepExecutionAndTransportGates(t *testing.T) {
	var calls atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"status":"ok","retcode":0,"data":{}}`))
	}))
	defer peer.Close()
	c, err := client.New(peer.URL, "synthetic-token")
	if err != nil {
		t.Fatal(err)
	}
	s := Server{Mode: "write", Client: c}
	for _, tc := range []struct {
		tool, action string
		calls        int32
		failed       bool
	}{
		{"query_action", "upload_file_stream", 0, true}, {"query_action", "clean_stream_temp_file", 0, true},
		{"invoke_action", "download_file_stream", 0, true}, {"invoke_action", "upload_file_stream", 1, false},
	} {
		raw, _ := json.Marshal(map[string]any{"name": tc.tool, "arguments": map[string]any{"action": tc.action, "params": map[string]any{}}})
		before := calls.Load()
		value, err := s.handle(context.Background(), request{Method: "tools/call", Params: raw})
		if err != nil || value.(map[string]any)["isError"] != tc.failed || calls.Load()-before != tc.calls {
			t.Fatal(tc.action, err, value, calls.Load()-before)
		}
	}
}
