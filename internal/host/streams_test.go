package host

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JFSAS/wispcat/internal/instances"
	"github.com/JFSAS/wispcat/internal/onebot"
)

func TestStreamManagementAuthenticationAndShutdownOwnership(t *testing.T) {
	cfg := testConfig(t)
	h, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	if err = h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		method, path, body, token string
		code                      int
	}{
		{"GET", "/accounts/first/streams", "", "", 401},
		{"POST", "/accounts/first/streams/clean", "{}", "", 401},
		{"GET", "/accounts/first/streams", "", "synthetic-admin-token", 200},
		{"GET", "/accounts/second/streams", "", "synthetic-admin-token", 200},
		{"POST", "/accounts/first/streams/clean", `{"path":"/tmp"}`, "synthetic-admin-token", 400},
		{"POST", "/accounts/first/streams/clean", "{}", "synthetic-admin-token", 200},
	} {
		request := httptest.NewRequest(tc.method, "http://"+h.Address()+tc.path, strings.NewReader(tc.body))
		if tc.token != "" {
			request.Header.Set("Authorization", "Bearer "+tc.token)
		}
		response := httptest.NewRecorder()
		h.ServeHTTP(response, request)
		if response.Code != tc.code {
			t.Fatal(tc.path, response.Code, response.Body.String())
		}
		if tc.code == 200 && tc.method == "GET" {
			var value onebot.StreamSnapshot
			if json.Unmarshal(response.Body.Bytes(), &value) != nil || value.QuotaBytes != 128<<20 || value.TotalBytes != 0 {
				t.Fatal(response.Body.String())
			}
		}
	}
	if err = h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, account := range h.accounts {
		if _, err = account.adapter.Runtime.Streams.Snapshot(""); err == nil {
			t.Fatal("host left stream storage open")
		}
	}
	next, err := New(cfg)
	if err != nil {
		t.Fatal("stream ownership survived shutdown", err)
	}
	_ = next.Close(context.Background())
}

func TestHostPartialSetupReleasesStreamOwners(t *testing.T) {
	cfg := testConfig(t)
	catalog, err := instances.Open(cfg.StateDir, cfg.Accounts[0].Instance)
	if err != nil {
		t.Fatal(err)
	}
	first, err := catalog.Ensure(cfg.Accounts[0].Instance)
	if err != nil {
		t.Fatal(err)
	}
	second, err := catalog.Ensure(cfg.Accounts[1].Instance)
	if err != nil {
		t.Fatal(err)
	}
	blocker, err := onebot.OpenStreams(filepath.Join(second.Directory, "stream-temp"))
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	if h, err := New(cfg); err == nil {
		_ = h.Close(context.Background())
		t.Fatal("constructor ignored occupied second stream store")
	}
	reopened, err := onebot.OpenStreams(filepath.Join(first.Directory, "stream-temp"))
	if err != nil {
		t.Fatal("failed constructor leaked first stream owner", err)
	}
	_ = reopened.Close()
}
