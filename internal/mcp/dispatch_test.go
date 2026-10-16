package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/JFSAS/wechat-runtime/pkg/client"
)

type mcpReply struct {
	ID     json.RawMessage `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type mcpSession struct {
	t       *testing.T
	input   *io.PipeWriter
	output  *io.PipeReader
	replies chan mcpReply
	done    chan struct{}
	cancel  context.CancelFunc
	err     error
}

func startMCPSession(t *testing.T, server *Server) *mcpSession {
	t.Helper()
	input, writer := io.Pipe()
	reader, output := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	s := &mcpSession{t: t, input: writer, output: reader, replies: make(chan mcpReply, 32), done: make(chan struct{}), cancel: cancel}
	go func() {
		defer close(s.done)
		defer output.Close()
		s.err = server.Run(ctx, input, output)
	}()
	go func() {
		defer close(s.replies)
		decoder := json.NewDecoder(reader)
		for {
			var reply mcpReply
			if decoder.Decode(&reply) != nil {
				return
			}
			s.replies <- reply
		}
	}()
	t.Cleanup(func() {
		cancel()
		_ = writer.Close()
		_ = reader.Close()
		_ = input.Close()
		select {
		case <-s.done:
		case <-time.After(3 * time.Second):
			t.Error("MCP did not join after test transport closed")
		}
	})
	return s
}

func (s *mcpSession) send(line string) {
	s.t.Helper()
	if _, err := io.WriteString(s.input, strings.TrimSuffix(line, "\n")+"\n"); err != nil {
		s.t.Fatal(err)
	}
}

func (s *mcpSession) next() mcpReply {
	s.t.Helper()
	select {
	case reply, ok := <-s.replies:
		if !ok {
			s.t.Fatal("MCP output closed before required reply")
		}
		return reply
	case <-time.After(3 * time.Second):
		s.t.Fatal("MCP reply blocked")
		return mcpReply{}
	}
}

func (s *mcpSession) finish() {
	s.t.Helper()
	_ = s.input.Close()
	select {
	case <-s.done:
		if s.err != nil {
			s.t.Fatal(s.err)
		}
	case <-time.After(3 * time.Second):
		s.t.Fatal("MCP did not exit after EOF")
	}
}

func mcpAction(id, action, params string) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"method":"tools/call","params":{"name":"query_action","arguments":{"action":%q,"params":%s}}}`, id, action, params)
}

func mcpCancel(id string) string {
	return `{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":` + id + `}}`
}

func waitMCP[T any](t *testing.T, values <-chan T) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("MCP fixture did not progress")
		var zero T
		return zero
	}
}

func TestMCPConcurrentActionCancellation(t *testing.T) {
	started, cancelled := make(chan struct{}, 1), make(chan struct{}, 1)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if r.URL.Path == "/get_status" {
			started <- struct{}{}
			<-r.Context().Done()
			cancelled <- struct{}{}
			return
		}
		_, _ = io.WriteString(w, `{"status":"ok","retcode":0,"data":{"user_id":123}}`)
	}))
	t.Cleanup(func() { peer.CloseClientConnections(); peer.Close() })
	c, _ := client.New(peer.URL, "synthetic-token")
	s := startMCPSession(t, &Server{Mode: "read", Client: c})
	s.send(mcpAction(`"pending"`, "get_status", `{}`))
	waitMCP(t, started)
	s.send(`{"jsonrpc":"2.0","id":1,"method":"ping"}`)
	if reply := s.next(); string(reply.ID) != "1" || reply.Error != nil {
		t.Fatal(reply)
	}
	s.send(mcpAction("2", "get_login_info", `{}`))
	if reply := s.next(); string(reply.ID) != "2" || reply.Error != nil {
		t.Fatal("second external action waited for stalled first action", reply)
	}
	s.send(mcpCancel(`"\u0070ending"`))
	waitMCP(t, cancelled)
	s.send(mcpAction("3", "get_login_info", `{}`))
	if reply := s.next(); string(reply.ID) != "3" || reply.Error != nil {
		t.Fatal(reply)
	}
	s.finish()
	for reply := range s.replies {
		t.Fatal("cancelled request produced a late reply", reply)
	}
}

func TestMCPAdmissionLimitKeepsControlAndCancellationAvailable(t *testing.T) {
	started := make(chan struct{}, maxPendingCalls)
	peer := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		started <- struct{}{}
		<-r.Context().Done()
	}))
	t.Cleanup(func() { peer.CloseClientConnections(); peer.Close() })
	c, _ := client.New(peer.URL, "synthetic-token")
	s := startMCPSession(t, &Server{Mode: "read", Client: c})
	for index := range maxPendingCalls {
		s.send(mcpAction(fmt.Sprint(index), "get_status", `{}`))
		waitMCP(t, started)
	}
	s.send(mcpAction(`"overflow"`, "get_status", `{}`))
	if reply := s.next(); string(reply.ID) != `"overflow"` || reply.Error == nil || reply.Error.Code != -32000 {
		t.Fatal("admission limit did not refuse ninth action", reply)
	}
	s.send(mcpAction("0", "get_status", `{}`))
	if reply := s.next(); string(reply.ID) != "null" || reply.Error == nil || reply.Error.Code != -32600 {
		t.Fatal("duplicate pending ID was accepted", reply)
	}
	for _, id := range []string{"null", "true", "1.5", "[]", "{}", `"` + strings.Repeat("x", maxRequestIDBytes) + `"`} {
		s.send(`{"jsonrpc":"2.0","id":` + id + `,"method":"ping"}`)
		if reply := s.next(); string(reply.ID) != "null" || reply.Error == nil || reply.Error.Code != -32600 {
			t.Fatal("invalid ID accepted", reply)
		}
	}
	s.send(`{"jsonrpc":"2.0","id":"control","method":"ping"}`)
	if reply := s.next(); string(reply.ID) != `"control"` || reply.Error != nil {
		t.Fatal("control request blocked by full action slots", reply)
	}
	for index := range maxPendingCalls {
		s.send(mcpCancel(fmt.Sprint(index)))
	}
	s.finish()
	for reply := range s.replies {
		t.Fatal("cancelled request produced a late reply", reply)
	}
}

func TestMCPCancellationMatchesExactIDTypeAndIntegerPrecision(t *testing.T) {
	started := make(chan context.Context, 3)
	peer := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		started <- r.Context()
		<-r.Context().Done()
	}))
	t.Cleanup(func() { peer.CloseClientConnections(); peer.Close() })
	c, _ := client.New(peer.URL, "synthetic-token")
	s := startMCPSession(t, &Server{Mode: "read", Client: c})
	ids := []string{"9007199254740993", "9007199254740992", `"9007199254740993"`}
	contexts := []context.Context{}
	for _, id := range ids {
		s.send(mcpAction(id, "get_status", `{}`))
		contexts = append(contexts, waitMCP(t, started))
	}
	s.send(mcpCancel(ids[0]))
	waitMCP(t, contexts[0].Done())
	if contexts[1].Err() != nil || contexts[2].Err() != nil {
		t.Fatal("cancellation collapsed integer precision or string/integer identity")
	}
	s.send(mcpCancel(ids[1]))
	s.send(mcpCancel(ids[2]))
	s.finish()
}

func TestMCPUnicodeIDsRejectReplacementCollisions(t *testing.T) {
	started := make(chan context.Context, 2)
	peer := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		started <- r.Context()
		<-r.Context().Done()
	}))
	t.Cleanup(func() { peer.CloseClientConnections(); peer.Close() })
	c, _ := client.New(peer.URL, "synthetic-token")
	s := startMCPSession(t, &Server{Mode: "read", Client: c})
	s.send(mcpAction(`"\ufffd"`, "get_status", `{}`))
	replacement := waitMCP(t, started)
	for _, id := range []string{`"\ud800"`, `"\ud801"`, `"\udfff"`, `"\ud800\u0041"`, string([]byte{'"', 0xff, '"'})} {
		s.send(`{"jsonrpc":"2.0","id":` + id + `,"method":"ping"}`)
		if reply := s.next(); reply.Error == nil || reply.Error.Code != -32600 || string(reply.ID) != "null" {
			t.Fatal("invalid Unicode request ID was accepted", reply)
		}
		s.send(mcpCancel(id))
	}
	s.send(`{"jsonrpc":"2.0","id":"barrier","method":"ping"}`)
	if reply := s.next(); string(reply.ID) != `"barrier"` || replacement.Err() != nil {
		t.Fatal("invalid Unicode cancellation aliased a valid replacement character", reply)
	}
	for _, id := range []string{`"\\ud800"`, `"\u0041"`, `"\u0000"`} {
		s.send(`{"jsonrpc":"2.0","id":` + id + `,"method":"ping"}`)
		if reply := s.next(); string(reply.ID) != id || reply.Error != nil {
			t.Fatal("valid JSON string escape was rejected", reply)
		}
	}
	s.send(mcpAction(`"\ud83d\ude80"`, "get_status", `{}`))
	rocket := waitMCP(t, started)
	s.send(mcpCancel(`"🚀"`))
	waitMCP(t, rocket.Done())
	if replacement.Err() != nil {
		t.Fatal("valid non-BMP cancellation aliased a sibling")
	}
	s.send(mcpCancel(`"�"`))
	s.finish()
}

func TestMCPShutdownJoinsFileCleanup(t *testing.T) {
	for _, shutdown := range []string{"eof", "signal", "output_failure"} {
		t.Run(shutdown, func(t *testing.T) {
			started, cleanup := make(chan struct{}, 1), make(chan struct{}, 1)
			release := make(chan struct{})
			defer close(release)
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "application/x-ndjson")
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				started <- struct{}{}
				<-r.Context().Done()
			}))
			t.Cleanup(func() { peer.CloseClientConnections(); peer.Close() })
			c, _ := client.New(peer.URL, "synthetic-token")
			transfers := mcpTransfers(t, TransferConfig{})
			transfers.removeFile = func(name string) error {
				cleanup <- struct{}{}
				<-release
				return transfers.root.Remove(name)
			}
			s := startMCPSession(t, &Server{Mode: "write", Client: c, Transfers: transfers})
			s.send(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"invoke_action","arguments":{"action":"download_file_stream","params":{"file":"synthetic"}}}}`)
			waitMCP(t, started)
			switch shutdown {
			case "eof":
				_ = s.input.Close()
			case "signal":
				s.cancel()
			case "output_failure":
				_ = s.output.Close()
				s.send(`{"jsonrpc":"2.0","id":2,"method":"ping"}`)
			}
			waitMCP(t, cleanup)
			select {
			case <-s.done:
				t.Fatal("Run returned before its file cleanup joined")
			default:
			}
			// Release now while keeping failure cleanup safe on every test exit.
			release <- struct{}{}
			waitMCP(t, s.done)
			if shutdown == "signal" && !errors.Is(s.err, context.Canceled) || shutdown == "output_failure" && s.err == nil || shutdown == "eof" && s.err != nil {
				t.Fatal(s.err)
			}
			entries, err := transfers.entries()
			if err != nil || len(entries) != 2 {
				t.Fatal("partial file survived joined cancellation", entries, err)
			}
		})
	}
}

func TestMCPQueuedFileCallCanCancelBeforeActiveFileCompletes(t *testing.T) {
	started := make(chan struct{}, 1)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		started <- struct{}{}
		<-r.Context().Done()
	}))
	t.Cleanup(func() { peer.CloseClientConnections(); peer.Close() })
	c, _ := client.New(peer.URL, "synthetic-token")
	transfers := mcpTransfers(t, TransferConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := make(chan error, 1)
	go func() {
		_, err := transfers.call(ctx, c, "download_file_stream", map[string]any{"file": "synthetic"}, "")
		first <- err
	}()
	waitMCP(t, started)
	waiting, stop := context.WithCancel(context.Background())
	second := make(chan error, 1)
	go func() {
		_, err := transfers.call(waiting, c, "download_file_stream", map[string]any{"file": "synthetic"}, "")
		second <- err
	}()
	stop()
	if err := waitMCP(t, second); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if ctx.Err() != nil {
		t.Fatal("waiting cancellation interrupted active owner")
	}
	cancel()
	waitMCP(t, first)
}
