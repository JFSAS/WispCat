package client

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClientPreservesActionFailureWithoutFollowingRedirects(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer synthetic-token" {
			t.Error("token missing")
		}
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "https://example.invalid", 302)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"failed","retcode":100,"message":"delivery_unknown","data":{"wechat_delivery_id":"id"}}`))
	}))
	defer peer.Close()
	c, err := New(peer.URL, "synthetic-token")
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Call(context.Background(), "send_private_msg", map[string]any{})
	var action *ActionError
	if !errors.As(err, &action) || action.Response.Message != "delivery_unknown" || string(action.Response.Data) != `{"wechat_delivery_id":"id"}` {
		t.Fatal("delivery uncertainty lost", err)
	}
	if _, err = c.Call(context.Background(), "redirect", nil); err == nil {
		t.Fatal("redirect followed")
	}
	if _, err = New("http://user:password@localhost/", "token"); err == nil {
		t.Fatal("credential URL accepted")
	}
}
