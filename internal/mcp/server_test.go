package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

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
		input := strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05"}}
{"jsonrpc":"2.0","id":2,"method":"tools/list"}
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"query_action","arguments":{"action":"send_private_msg","params":{}}}}
{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"invoke_action","arguments":{"action":"get_status","params":{}}}}
`)
		var output bytes.Buffer
		before := calls.Load()
		if err = s.Run(context.Background(), input, &output); err != nil {
			t.Fatal(err)
		}
		dec := json.NewDecoder(&output)
		var replies []map[string]any
		for dec.More() {
			var v map[string]any
			if err = dec.Decode(&v); err != nil {
				t.Fatal(err)
			}
			replies = append(replies, v)
		}
		if len(replies) != 4 {
			t.Fatal(replies)
		}
		if mode != "write" && calls.Load() != before {
			t.Fatal("MCP gate allowed a write-only tool")
		}
		if mode == "write" && calls.Load() != before+1 {
			t.Fatal("write tool not dispatched")
		}
		gate := replies[2]["result"].(map[string]any)
		if gate["isError"] != true {
			t.Fatal("query_action executed a side-effect action")
		}
	}
}
