package host

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func testConfig(t *testing.T) Config {
	t.Helper()
	t.Setenv("TEST_ADMIN_TOKEN", "synthetic-admin-token")
	t.Setenv("TEST_BOT_TOKEN", "synthetic-onebot-token")
	return Config{StateDir: t.TempDir(), Listen: "127.0.0.1:0", TokenEnv: "TEST_ADMIN_TOKEN", Accounts: []Account{{Name: "first", Instance: "instance:first", Mode: "offline", Listen: "127.0.0.1:0", TokenEnv: "TEST_BOT_TOKEN"}, {Name: "second", Instance: "instance:second", Mode: "offline", Listen: "127.0.0.1:0", TokenEnv: "TEST_BOT_TOKEN"}}}
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
