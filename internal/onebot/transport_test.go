package onebot

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestHTTPAuthenticationFormatsAndErrors(t *testing.T) {
	r, _ := fixture(t)
	s := NewServer(r, strings.Repeat("t", 32), "127.0.0.1:5700")
	for _, test := range []struct {
		method, path, content, body, auth, host, origin string
		code                                            int
	}{
		{"GET", "/get_status", "", "", "", "127.0.0.1:5700", "", 401}, {"GET", "/get_status", "", "", "Bearer bad", "127.0.0.1:5700", "", 403},
		{"GET", "/get_status", "", "", "Bearer " + s.Token, "evil.test", "", 403}, {"GET", "/get_status", "", "", "Bearer " + s.Token, s.Host, "http://evil.test", 403},
		{"GET", "/get_status", "", "", "Bearer " + s.Token, s.Host, "", 200}, {"POST", "/get_status", "application/json", "{}", "Bearer " + s.Token, s.Host, "", 200},
		{"POST", "/get_status", "application/json", "{} {}", "Bearer " + s.Token, s.Host, "", 400}, {"POST", "/get_status", "text/plain", "{}", "Bearer " + s.Token, s.Host, "", 406},
		{"POST", "/get_status", "application/x-www-form-urlencoded", "foo=bar", "Bearer " + s.Token, s.Host, "", 200}, {"GET", "/missing", "", "", "Bearer " + s.Token, s.Host, "", 404},
	} {
		q := httptest.NewRequest(test.method, "http://"+test.host+test.path, strings.NewReader(test.body))
		q.Host = test.host
		q.Header.Set("Authorization", test.auth)
		q.Header.Set("Content-Type", test.content)
		q.Header.Set("Origin", test.origin)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, q)
		if w.Code != test.code {
			t.Fatalf("%s: got %d want %d %s", test.path, w.Code, test.code, w.Body.String())
		}
	}
}
func readJSON(t *testing.T, c *websocket.Conn) map[string]any {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	var value map[string]any
	if err := c.ReadJSON(&value); err != nil {
		t.Fatal(err)
	}
	return value
}
func TestForwardWebSocketEventsAndActionsShareOneConnection(t *testing.T) {
	r, channel := fixture(t)
	s := NewServer(r, strings.Repeat("t", 32), "")
	peer := httptest.NewServer(s)
	defer peer.Close()
	defer s.Close(context.Background())
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(peer.URL, "http")+"/?access_token="+s.Token, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if value := readJSON(t, conn); value["meta_event_type"] != "lifecycle" || value["sub_type"] != "connect" {
		t.Fatal(value)
	}
	if err = conn.WriteJSON(Request{Action: "get_login_info", Echo: json.RawMessage(`{"n":7}`)}); err != nil {
		t.Fatal(err)
	}
	response := readJSON(t, conn)
	if response["status"] != "ok" || response["echo"].(map[string]any)["n"] != float64(7) {
		t.Fatal(response)
	}
	channel.append("private", "new")
	event := readJSON(t, conn)
	if event["post_type"] != "message" || event["raw_message"] != "new" {
		t.Fatal(event)
	}
	if err = conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"unsupported","echo":[1,"x"]}`)); err != nil {
		t.Fatal(err)
	}
	response = readJSON(t, conn)
	if response["retcode"] != float64(1404) || len(response["echo"].([]any)) != 2 {
		t.Fatal(response)
	}
}
func TestReverseWebSocketHeadersReconnectAndShutdown(t *testing.T) {
	r, channel := fixture(t)
	s := NewServer(r, strings.Repeat("t", 32), "")
	var attempts atomic.Int32
	connected := make(chan struct{}, 4)
	failures := make(chan string, 4)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		if q.Header.Get("X-Client-Role") != "Universal" || q.Header.Get("X-Self-ID") == "" || q.Header.Get("Authorization") != "Bearer synthetic-reverse-token" {
			failures <- "missing reverse handshake headers"
			http.Error(w, "bad", 403)
			return
		}
		c, e := (&websocket.Upgrader{}).Upgrade(w, q, nil)
		if e != nil {
			return
		}
		defer c.Close()
		var event map[string]any
		if c.ReadJSON(&event) != nil {
			return
		}
		if event["meta_event_type"] != "lifecycle" {
			failures <- "missing lifecycle"
			return
		}
		attempt := attempts.Add(1)
		connected <- struct{}{}
		if attempt == 1 {
			return
		}
		_ = c.WriteJSON(Request{Action: "get_status", Echo: json.RawMessage(`"reverse"`)})
		for {
			if c.ReadJSON(&event) != nil {
				return
			}
			if event["echo"] == "reverse" && event["status"] != "ok" {
				failures <- "action failed"
			}
		}
	}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- s.RunReverse(ctx, ReverseConfig{Name: "bot", URL: "ws" + strings.TrimPrefix(peer.URL, "http"), Token: "synthetic-reverse-token"})
	}()
	for range 2 {
		select {
		case <-connected:
		case failure := <-failures:
			t.Fatal(failure)
		case <-time.After(4 * time.Second):
			t.Fatal("reverse connection or reconnect missing")
		}
	}
	channel.append("private", "after-reconnect")
	shutdown, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	if err := s.Close(shutdown); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("reverse goroutine not stopped")
	}
	peer.Close()
}
func TestCancelledPendingActionDoesNotHoldWebSocketShutdown(t *testing.T) {
	r, c := fixture(t)
	c.pending = true
	server := NewServer(r, strings.Repeat("t", 32), "")
	peer := httptest.NewServer(server)
	defer peer.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(peer.URL, "http")+"/?access_token="+server.Token, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	readJSON(t, conn)
	id, _ := r.State.ID("account-a", "user", "friend")
	if err = conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"send_private_msg","params":{"user_id":`+jsonNumber(id)+`,"message":"queued"},"echo":1}`)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		c.mu.Lock()
		n := len(c.sends)
		c.mu.Unlock()
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("action did not queue")
		}
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = server.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
