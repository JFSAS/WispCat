package host

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JFSAS/wechat-runtime/internal/onebot"
	"github.com/JFSAS/wechat-runtime/pkg/bot"
	"github.com/gorilla/websocket"
)

func adminInbound(t *testing.T, h *Host, method, path, body string, code int) map[string]any {
	t.Helper()
	q, _ := http.NewRequest(method, "http://"+h.Address()+path, strings.NewReader(body))
	q.Header.Set("Authorization", "Bearer synthetic-admin-token")
	q.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Do(q)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(response.Body)
	if response.StatusCode != code {
		t.Fatal(method, path, response.StatusCode, string(raw))
	}
	var result map[string]any
	if json.Unmarshal(raw, &result) != nil {
		t.Fatal(string(raw))
	}
	return result
}

func inboundHTTP(t *testing.T, address, secret string, code int) {
	t.Helper()
	q, _ := http.NewRequest("GET", "http://"+address+"/get_status", nil)
	q.Header.Set("Authorization", "Bearer "+secret)
	response, err := (&http.Client{Timeout: time.Second}).Do(q)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != code {
		t.Fatal(response.StatusCode, code)
	}
}

func TestInboundSaveApplyFailureKeepsSessionsAndRestartRestores(t *testing.T) {
	cfg := testConfig(t)
	h, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	if err = h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	first, second := h.accounts[0], h.accounts[1]
	before := first.channel.Snapshot()
	secondAddress := second.listener.Addr().String()
	oldAddress := first.listener.Addr().String()
	id, err := h.state.ID("synthetic-account", "user", "stable-person")
	if err != nil {
		t.Fatal(err)
	}
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	newAddress := blocker.Addr().String()
	initial := adminInbound(t, h, "GET", "/accounts/first/listener", "", 200)
	body, _ := json.Marshal(map[string]any{"revision": initial["revision"], "plan": inboundPlan{newAddress, "TEST_BOT_TOKEN"}})
	saved := adminInbound(t, h, "POST", "/accounts/first/listener", string(body), 200)
	if saved["applied"] != false {
		t.Fatal("save applied listener", saved)
	}
	adminInbound(t, h, "POST", "/accounts/first/listener", string(body), 409)
	adminInbound(t, h, "POST", "/accounts/first/listener/apply", "{}", 409)
	failed := adminInbound(t, h, "GET", "/accounts/first/listener", "", 200)
	if failed["applied"] != false || failed["listener"].(map[string]any)["error"] != "listener_bind_failed" {
		t.Fatal(failed)
	}
	inboundHTTP(t, oldAddress, "synthetic-onebot-token", 200)
	_ = blocker.Close()
	adminInbound(t, h, "POST", "/accounts/first/listener/apply", "{}", 202)
	applied := adminInbound(t, h, "GET", "/accounts/first/listener", "", 200)
	if applied["applied"] != true || applied["listener"].(map[string]any)["address"] != newAddress {
		t.Fatal(applied)
	}
	inboundHTTP(t, newAddress, "synthetic-onebot-token", 200)
	conn, err := net.DialTimeout("tcp", oldAddress, time.Second)
	if err == nil {
		conn.Close()
		t.Fatal("old listener still accepts")
	}
	if h.accounts[0] != first || h.accounts[1] != second || first.channel.Snapshot().Epoch != before.Epoch || second.listener.Addr().String() != secondAddress {
		t.Fatal("listener apply replaced protocol/account state")
	}
	inboundHTTP(t, secondAddress, "synthetic-onebot-token", 200)
	for _, account := range h.accounts {
		raw, _ := json.Marshal(account.channel.Status())
		var status map[string]any
		_ = json.Unmarshal(raw, &status)
		for _, counter := range []string{"auth_calls", "sync_calls", "send_calls"} {
			if status[counter] != float64(0) {
				t.Fatal(counter, status)
			}
		}
	}
	if err = h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	next, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close(context.Background())
	if next.config.Accounts[0].Listen != newAddress {
		t.Fatal("saved listener lost on reopen")
	}
	if err = next.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	inboundHTTP(t, newAddress, "synthetic-onebot-token", 200)
	restored, err := next.state.ID("synthetic-account", "user", "stable-person")
	if err != nil || restored != id {
		t.Fatal(restored, id, err)
	}
}

type inboundChannel struct {
	bot.Channel
	entered   chan struct{}
	release   chan struct{}
	enterOnce sync.Once
}

func (c *inboundChannel) Snapshot() bot.Snapshot {
	return bot.Snapshot{Account: "synthetic", Self: bot.User{ID: "owner"}, Epoch: 1, State: "offline"}
}
func (c *inboundChannel) Events(string, uint64, int) ([]bot.Event, error) { return nil, nil }
func (c *inboundChannel) Download(ctx context.Context, _, _, file string) (bot.Content, error) {
	if file == "blocked" {
		c.enterOnce.Do(func() { close(c.entered) })
		<-c.release
	}
	return bot.Content{Data: []byte("synthetic-image-bytes"), MIME: "application/octet-stream"}, nil
}

func inboundFixture(t *testing.T) (*inboundServer, *onebot.Server, *inboundChannel) {
	t.Helper()
	state, err := onebot.OpenState(filepath.Join(t.TempDir(), "onebot"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = state.Close() })
	channel := &inboundChannel{}
	adapter := onebot.NewServer(onebot.New(channel, state), "synthetic-old-token", "")
	t.Cleanup(func() { _ = adapter.Close(context.Background()) })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	manager := newInboundServer(adapter, context.Background(), listener, "127.0.0.1:0", "synthetic-old-token")
	t.Cleanup(func() { _ = manager.close(context.Background()) })
	return manager, adapter, channel
}

func readInboundSocket(t *testing.T, c *websocket.Conn) map[string]any {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	var value map[string]any
	if err := c.ReadJSON(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestInboundTokenRotationPreservesPortAndReverseConnection(t *testing.T) {
	manager, adapter, _ := inboundFixture(t)
	before, _ := manager.snapshot("", "")
	forward, _, err := websocket.DefaultDialer.Dial("ws://"+before.Address+"/?access_token=synthetic-old-token", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer forward.Close()
	if readInboundSocket(t, forward)["meta_event_type"] != "lifecycle" {
		t.Fatal("missing lifecycle")
	}
	var connects atomic.Int32
	reverseReady := make(chan *websocket.Conn, 1)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		connects.Add(1)
		reverseReady <- conn
	}))
	defer peer.Close()
	network := onebot.NewNetworkManager(adapter, context.Background())
	defer network.Close(context.Background())
	if err = network.Apply(context.Background(), []onebot.NetworkSpec{{Name: "reverse", Kind: "wsClient", URL: "ws" + strings.TrimPrefix(peer.URL, "http"), Secret: "synthetic-reverse-token"}}); err != nil {
		t.Fatal(err)
	}
	var reverse *websocket.Conn
	select {
	case reverse = <-reverseReady:
	case <-time.After(2 * time.Second):
		t.Fatal("reverse did not connect")
	}
	defer reverse.Close()
	readInboundSocket(t, reverse)
	oldLink := adapter.Runtime.MediaURL("synthetic", "friend", "image")
	if err = manager.apply(context.Background(), "127.0.0.1:0", "synthetic-new-token"); err != nil {
		t.Fatal(err)
	}
	after, match := manager.snapshot("127.0.0.1:0", "synthetic-new-token")
	if !match || after.Address != before.Address || after.Retiring {
		t.Fatal(before, after)
	}
	inboundHTTP(t, after.Address, "synthetic-old-token", 403)
	inboundHTTP(t, after.Address, "synthetic-new-token", 200)
	_ = forward.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err = forward.ReadMessage(); err == nil {
		t.Fatal("old authenticated socket survived rotation")
	}
	if err = reverse.WriteJSON(onebot.Request{Action: "get_status", Echo: json.RawMessage(`"still-connected"`)}); err != nil {
		t.Fatal(err)
	}
	response := readInboundSocket(t, reverse)
	if response["status"] != "ok" || response["echo"] != "still-connected" || connects.Load() != 1 {
		t.Fatal(response, connects.Load())
	}
	for _, tc := range []struct {
		url  string
		code int
	}{{oldLink, 403}, {adapter.Runtime.MediaURL("synthetic", "friend", "image"), 200}} {
		res, err := (&http.Client{Timeout: time.Second}).Get(tc.url)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != tc.code || tc.code == 200 && !bytes.Equal(raw, []byte("synthetic-image-bytes")) {
			t.Fatal(res.StatusCode, string(raw))
		}
	}
}

func TestInboundRetirementTimeoutRetainsOwnershipUntilRequestReturns(t *testing.T) {
	cfg := testConfig(t)
	h, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	if err = h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	channel := &inboundChannel{entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(channel.release) })
	account := h.accounts[0]
	account.adapter.Runtime.Channel = channel
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		res, _ := (&http.Client{Timeout: 3 * time.Second}).Get(account.adapter.Runtime.MediaURL("synthetic", "friend", "blocked"))
		if res != nil {
			res.Body.Close()
		}
	}()
	select {
	case <-channel.entered:
	case <-time.After(time.Second):
		t.Fatal("download did not enter")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = account.inbound.manager.apply(ctx, account.config.Listen, "synthetic-new-token")
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	status, match := account.inbound.manager.snapshot(account.config.Listen, "synthetic-new-token")
	if !match || !status.Retiring || status.Error != "listener_retirement_pending" {
		t.Fatal(status, match)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = account.inbound.manager.apply(ctx, account.config.Listen, "synthetic-another-token")
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = h.Close(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if other, err := New(cfg); err == nil {
		other.Close(context.Background())
		t.Fatal("state ownership released under a live request")
	}
	release.Do(func() { close(channel.release) })
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("request did not leave")
	}
	if err = h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	next, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_ = next.Close(context.Background())
}

func TestInboundManagementRejectsInvalidAndUnauthenticatedChanges(t *testing.T) {
	cfg := testConfig(t)
	h, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	if err = h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/accounts/first/listener", "/accounts/first/listener/apply"} {
		q, _ := http.NewRequest("POST", "http://"+h.Address()+path, strings.NewReader("{}"))
		res, err := http.DefaultClient.Do(q)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != 401 {
			t.Fatal(path, res.StatusCode)
		}
	}
	initial := adminInbound(t, h, "GET", "/accounts/first/listener", "", 200)
	for _, plan := range []inboundPlan{{"0.0.0.0:5700", "TEST_BOT_TOKEN"}, {"127.0.0.1:garbage", "TEST_BOT_TOKEN"}, {"127.0.0.1:65536", "TEST_BOT_TOKEN"}, {"127.0.0.1:0", "MISSING_LISTENER_TOKEN"}} {
		body, _ := json.Marshal(map[string]any{"revision": initial["revision"], "plan": plan})
		adminInbound(t, h, "POST", "/accounts/first/listener", string(body), 409)
	}
	adminInbound(t, h, "POST", "/accounts/first/listener", `{"revision":"x","plan":{"listen":"127.0.0.1:0","token_env":"TEST_BOT_TOKEN","token":"must-not-persist"}}`, 400)
	adminInbound(t, h, "POST", "/accounts/first/listener/apply", `{"unexpected":true}`, 400)
	view := adminInbound(t, h, "GET", "/accounts/first/listener", "", 200)
	if view["revision"] != initial["revision"] || view["applied"] != true {
		t.Fatal(view)
	}
}

func TestInboundNewTokenEnvironmentOverridesRetiredBootstrap(t *testing.T) {
	cfg := testConfig(t)
	cfg.Accounts = cfg.Accounts[:1]
	t.Setenv("REPLACEMENT_BOT_TOKEN", "synthetic-replacement-token")
	h, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	if err = h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	initial := adminInbound(t, h, "GET", "/accounts/first/listener", "", 200)
	plan := inboundPlan{Listen: cfg.Accounts[0].Listen, TokenEnv: "REPLACEMENT_BOT_TOKEN"}
	body, _ := json.Marshal(map[string]any{"revision": initial["revision"], "plan": plan})
	adminInbound(t, h, "POST", "/accounts/first/listener", string(body), 200)
	adminInbound(t, h, "POST", "/accounts/first/listener/apply", "{}", 202)
	address := h.accounts[0].listener.Addr().String()
	inboundHTTP(t, address, "synthetic-replacement-token", 200)
	// Environment changes under the same name are not hidden by equal JSON
	// revisions: the actual live secret must match before applied is true.
	t.Setenv("REPLACEMENT_BOT_TOKEN", "synthetic-rotated-token")
	changed := adminInbound(t, h, "GET", "/accounts/first/listener", "", 200)
	if changed["applied"] != false {
		t.Fatal(changed)
	}
	adminInbound(t, h, "POST", "/accounts/first/listener/apply", "{}", 202)
	inboundHTTP(t, address, "synthetic-replacement-token", 403)
	inboundHTTP(t, address, "synthetic-rotated-token", 200)
	if err = h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_BOT_TOKEN", "")
	next, err := New(cfg)
	if err != nil {
		t.Fatal("retired bootstrap token still required", err)
	}
	defer next.Close(context.Background())
	if err = next.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	inboundHTTP(t, next.accounts[0].listener.Addr().String(), "synthetic-rotated-token", 200)
}
