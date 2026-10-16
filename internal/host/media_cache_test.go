package host

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestIncomingMediaManagementIsAuthenticatedOfflineAndOwned(t *testing.T) {
	h, err := New(testConfig(t))
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
		{"GET", "/accounts/first/media-cache", "", "", 401},
		{"POST", "/accounts/first/media-cache/compact", "{}", "", 401},
		{"GET", "/accounts/first/media-cache", "", "synthetic-admin-token", 200},
		{"GET", "/accounts/second/media-cache", "", "synthetic-admin-token", 200},
		{"POST", "/accounts/first/media-cache/compact", `{"path":"/tmp"}`, "synthetic-admin-token", 400},
		{"POST", "/accounts/first/media-cache/compact", "{}", "synthetic-admin-token", 409},
	} {
		q := httptest.NewRequest(tc.method, "http://"+h.Address()+tc.path, strings.NewReader(tc.body))
		if tc.token != "" {
			q.Header.Set("Authorization", "Bearer "+tc.token)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, q)
		if w.Code != tc.code {
			t.Fatal(tc.path, w.Code, w.Body.String())
		}
		if tc.code == 200 {
			var value map[string]any
			if json.Unmarshal(w.Body.Bytes(), &value) != nil || value["format"] != "encrypted_incoming_media_v1" || value["accounted_file_limit"] != float64(512) || value["tasks"] != float64(0) {
				t.Fatal(w.Body.String())
			}
		}
	}
	for _, account := range h.accounts {
		raw, _ := json.Marshal(account.channel.Status())
		var value map[string]any
		_ = json.Unmarshal(raw, &value)
		for _, name := range []string{"auth_calls", "sync_calls", "send_calls"} {
			if value[name] != float64(0) {
				t.Fatal("media management started protocol activity", name)
			}
		}
	}
	if err = h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, account := range h.accounts {
		if _, err = account.bot.IncomingMediaStatus(); err == nil {
			t.Fatal("media history remained open after shutdown")
		}
	}
}
