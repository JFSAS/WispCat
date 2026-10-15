package host

import (
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
	"testing"
	"time"

	"github.com/JFSAS/wechat-runtime/internal/instances"
)

func testConfig(t *testing.T) Config {
	t.Helper()
	t.Setenv("TEST_ADMIN_TOKEN", "synthetic-admin-token")
	t.Setenv("TEST_BOT_TOKEN", "synthetic-onebot-token")
	return Config{StateDir: t.TempDir(), Listen: "127.0.0.1:0", TokenEnv: "TEST_ADMIN_TOKEN", Accounts: []Account{{Name: "first", Instance: "instance:first", Mode: "offline", Listen: "127.0.0.1:0", TokenEnv: "TEST_BOT_TOKEN"}, {Name: "second", Instance: "instance:second", Mode: "offline", Listen: "127.0.0.1:0", TokenEnv: "TEST_BOT_TOKEN"}}}
}

func TestStorageDiagnosticsAndIndexOwnershipCleanup(t *testing.T) {
	cfg := testConfig(t)
	h, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	if err = h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	id, err := h.state.ID("synthetic-account", "user", "private-person")
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("GET", "http://"+h.Address()+"/storage", nil)
	request.Header.Set("Authorization", "Bearer synthetic-admin-token")
	response := httptest.NewRecorder()
	h.ServeHTTP(response, request)
	var stats map[string]any
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &stats) != nil || stats["identities"] != float64(1) || stats["file_bytes"].(float64) <= 0 {
		t.Fatal(response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "private-person") || strings.Contains(response.Body.String(), "synthetic-account") {
		t.Fatal("storage diagnostics exposed private identities")
	}
	if err = h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = h.state.ID("synthetic-account", "user", "private-person"); err == nil {
		t.Fatal("host shutdown left index open")
	}
	next, err := New(cfg)
	if err != nil {
		t.Fatal("closed host retained index ownership", err)
	}
	defer next.Close(context.Background())
	restored, err := next.state.ID("synthetic-account", "user", "private-person")
	if err != nil || restored != id {
		t.Fatal(restored, id, err)
	}
}

func TestSharedPublicIDsReserveDisabledLegacyInstances(t *testing.T) {
	cfg := testConfig(t)
	cfg.Accounts = cfg.Accounts[:1]
	catalog, err := instances.Open(cfg.StateDir, cfg.Accounts[0].Instance)
	if err != nil {
		t.Fatal(err)
	}
	disabled, err := catalog.Ensure("instance:disabled")
	if err != nil {
		t.Fatal(err)
	}
	oldID := writeLegacyPublicID(t, filepath.Join(disabled.Directory, "onebot"), "account-disabled")
	h, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	state := h.accounts[0].adapter.Runtime.State
	newID, err := state.ID("account-new", "user", "owner")
	if err != nil || newID == oldID {
		t.Fatal("new account claimed disabled instance's public ID", newID, oldID, err)
	}
	if err = h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	cfg.Accounts = append(cfg.Accounts, Account{Name: "disabled", Instance: "instance:disabled", Mode: "offline", Listen: "127.0.0.1:0", TokenEnv: "TEST_BOT_TOKEN"})
	reopened, err := New(cfg)
	if err != nil {
		t.Fatal("re-enabled legacy instance collided with new account", err)
	}
	defer reopened.Close(context.Background())
	restored, err := reopened.accounts[1].adapter.Runtime.State.ID("account-disabled", "user", "owner")
	if err != nil || restored != oldID {
		t.Fatal("disabled instance lost its public ID", restored, oldID, err)
	}
}

func TestCloseReleasesListenerNeverRegisteredWithHTTPServer(t *testing.T) {
	cfg := testConfig(t)
	h, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	h.listener, h.admin, h.started = listener, &http.Server{Handler: h}, true
	if err = h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	rebound, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal("partial startup leaked listener", err)
	}
	rebound.Close()
}

func TestTimedOutCloseStillCompletesOwnershipCleanup(t *testing.T) {
	cfg := testConfig(t)
	h, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	h.wg.Add(1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err = h.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if _, err = New(cfg); err == nil {
		t.Fatal("ownership released before work stopped")
	}
	h.wg.Done()
	wait, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err = h.Close(wait); err != nil {
		t.Fatal(err)
	}
	next, err := New(cfg)
	if err != nil {
		t.Fatal("cleanup was abandoned after caller timeout", err)
	}
	_ = next.Close(context.Background())
}
func TestManagementConcurrencyIsBounded(t *testing.T) {
	cfg := testConfig(t)
	h, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err = h.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	var work sync.WaitGroup
	var writers []*io.PipeWriter
	for range 16 {
		reader, writer := io.Pipe()
		writers = append(writers, writer)
		q := httptest.NewRequest("POST", "http://"+h.Address()+"/accounts/first/offline", reader)
		q.Header.Set("Authorization", "Bearer synthetic-admin-token")
		work.Add(1)
		go func() { defer work.Done(); h.ServeHTTP(httptest.NewRecorder(), q) }()
	}
	deadline := time.Now().Add(time.Second)
	for len(h.requests) != 16 {
		if time.Now().After(deadline) {
			t.Fatal("management requests did not enter")
		}
		time.Sleep(time.Millisecond)
	}
	q := httptest.NewRequest("GET", "http://"+h.Address()+"/accounts", nil)
	q.Header.Set("Authorization", "Bearer synthetic-admin-token")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, q)
	for _, writer := range writers {
		_ = writer.Close()
	}
	work.Wait()
	if w.Code != 503 || h.admin.ReadTimeout == 0 {
		t.Fatal("management admission/read timeout missing", w.Code)
	}
}
func TestOfflineMultiAccountRuntimeAndStateLock(t *testing.T) {
	cfg := testConfig(t)
	h, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err = h.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	if _, err = New(cfg); err == nil {
		t.Fatal("second process can own the same root")
	}
	request, _ := http.NewRequest("GET", "http://"+h.Address()+"/accounts", nil)
	request.Header.Set("Authorization", "Bearer synthetic-admin-token")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var accounts []struct {
		Name   string         `json:"name"`
		Status map[string]any `json:"status"`
	}
	if json.NewDecoder(response.Body).Decode(&accounts) != nil || len(accounts) != 2 {
		t.Fatal("account status missing")
	}
	for _, a := range accounts {
		for _, key := range []string{"auth_calls", "sync_calls", "send_calls"} {
			if a.Status[key] != float64(0) {
				t.Fatal("offline runtime attempted protocol I/O", a.Name, key, a.Status)
			}
		}
	}
	if h.accounts[0].channel.Snapshot().Epoch == h.accounts[1].channel.Snapshot().Epoch {
		t.Fatal("instances share a send epoch")
	}
	first, _ := h.accounts[0].adapter.Runtime.State.ID("synthetic-a", "user", "owner-a")
	second, _ := h.accounts[1].adapter.Runtime.State.ID("synthetic-b", "user", "owner-b")
	if first == second {
		t.Fatal("two accounts advertise the same bot self_id")
	}
	for _, a := range h.accounts {
		q, _ := http.NewRequest("GET", "http://"+a.listener.Addr().String()+"/get_version_info", nil)
		q.Header.Set("Authorization", "Bearer synthetic-onebot-token")
		res, e := http.DefaultClient.Do(q)
		if e != nil {
			t.Fatal(e)
		}
		var v map[string]any
		e = json.NewDecoder(res.Body).Decode(&v)
		res.Body.Close()
		if e != nil || v["status"] != "ok" {
			t.Fatal(v, e)
		}
	}
	shutdown, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err = h.Close(shutdown); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(cfg)
	if err != nil {
		t.Fatal("state lock not released", err)
	}
	_ = reopened.Close(context.Background())
}
func TestInvalidConfigFailsBeforeRuntimeStarts(t *testing.T) {
	cfg := testConfig(t)
	for _, mutate := range []func(*Config){func(c *Config) { c.Listen = "0.0.0.0:5099" }, func(c *Config) { c.Accounts[1].Instance = c.Accounts[0].Instance }, func(c *Config) { c.Accounts[0].Mode = "qr" }, func(c *Config) {
		c.Accounts[0].Reverse = []Reverse{{Name: "bad", URL: "http://example.invalid/ws", TokenEnv: "TEST_BOT_TOKEN"}}
	}} {
		copy := cfg
		copy.Accounts = append([]Account(nil), cfg.Accounts...)
		mutate(&copy)
		if copy.Validate() == nil {
			t.Fatal("invalid topology accepted")
		}
	}
}
func TestAdminRequiresAuthAndRejectsUnexpectedBodies(t *testing.T) {
	cfg := testConfig(t)
	h, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err = h.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	for _, test := range []struct {
		method, path, body, auth string
		code                     int
	}{{"GET", "/accounts", "", "", 401}, {"POST", "/accounts/first/offline", `{"wechat_app":"other"}`, "Bearer synthetic-admin-token", 400}, {"POST", "/accounts/first/offline", `{}`, "Bearer synthetic-admin-token", 202}, {"POST", "/accounts/first/offline", `{} {}`, "Bearer synthetic-admin-token", 400}} {
		q, _ := http.NewRequest(test.method, "http://"+h.Address()+test.path, strings.NewReader(test.body))
		q.Header.Set("Authorization", test.auth)
		res, e := http.DefaultClient.Do(q)
		if e != nil {
			t.Fatal(e)
		}
		res.Body.Close()
		if res.StatusCode != test.code {
			t.Fatal(test, res.StatusCode)
		}
	}
}
