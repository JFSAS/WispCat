package onebot

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/JFSAS/wechat-runtime/pkg/bot"
)

type mediaChannel struct {
	*fakeChannel
	content bot.Content
}

func (c mediaChannel) Download(context.Context, string, string, string) (bot.Content, error) {
	return c.content, nil
}
func TestReceivedMediaLinkIsSignedBoundedAndAccountScoped(t *testing.T) {
	r, c := fixture(t)
	r.Channel = mediaChannel{c, bot.Content{Data: []byte("synthetic image"), MIME: "image/png", Name: "image.png"}}
	s := NewServer(r, strings.Repeat("s", 32), "")
	peer := httptest.NewServer(s)
	defer peer.Close()
	s.Host = strings.TrimPrefix(peer.URL, "http://")
	event := bot.Event{ID: "media-event", Account: "account-a", Conversation: bot.Conversation{ID: "friend", Kind: "private"}, Sender: bot.User{ID: "friend"}, Time: 1700000000, Message: []bot.Segment{{Type: "image", Data: map[string]string{"file": "source/regular"}}}}
	converted, err := r.Convert(event)
	if err != nil {
		t.Fatal(err)
	}
	segment := converted["message"].([]bot.Segment)[0]
	link := segment.Data["url"]
	if link == "" || segment.Data["file"] == "source/regular" || strings.Contains(link, s.Token) {
		t.Fatal("unsafe media reference", segment)
	}
	response, err := http.Get(link)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != 200 || !bytes.Equal(raw, []byte("synthetic image")) {
		t.Fatal(response.StatusCode, string(raw))
	}
	response, err = http.Get(strings.Replace(link, "account-a", "account-b", 1))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 403 {
		t.Fatal("altered account link accepted")
	}
	c.mu.Lock()
	c.snap.Account = "account-b"
	c.mu.Unlock()
	response, err = http.Get(link)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 409 {
		t.Fatal("link crossed account switch")
	}
}
func TestMediaURLDoesNotFollowRedirectOrReadLocalPath(t *testing.T) {
	r, c := fixture(t)
	id, _ := r.State.ID("account-a", "user", "friend")
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("synthetic media")) }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 302) }))
	defer redirect.Close()
	for _, source := range []string{redirect.URL, "file:///private/example"} {
		params, _ := json.Marshal(map[string]any{"user_id": id, "message": []bot.Segment{{Type: "image", Data: map[string]string{"file": source}}}})
		if response := invoke(t, r, "send_private_msg", string(params)); response.Retcode == 0 {
			t.Fatal("invalid media source accepted")
		}
	}
	if len(c.sends) != 0 {
		t.Fatal("invalid media request queued")
	}
	params, _ := json.Marshal(map[string]any{"user_id": id, "message": []bot.Segment{{Type: "image", Data: map[string]string{"file": target.URL}}}})
	if response := invoke(t, r, "send_private_msg", string(params)); response.Retcode != 0 || len(c.sends) != 1 {
		t.Fatal(response)
	}
	if string(c.sends[0].Upload.Data) != "synthetic media" {
		t.Fatal("media content changed")
	}
}
func TestHTTPPostSignatureEventAndQuickReply(t *testing.T) {
	r, c := fixture(t)
	s := NewServer(r, strings.Repeat("t", 32), "")
	enabled := make(chan struct{}, 1)
	reported := make(chan struct{}, 1)
	invalid := make(chan string, 4)
	secret := "synthetic-post-secret"
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		raw, _ := io.ReadAll(q.Body)
		mac := hmac.New(sha1.New, []byte(secret))
		mac.Write(raw)
		if q.Header.Get("X-Signature") != "sha1="+hex.EncodeToString(mac.Sum(nil)) || q.Header.Get("X-Self-ID") == "" {
			invalid <- "invalid signature or identity"
		}
		var v map[string]any
		_ = json.Unmarshal(raw, &v)
		if v["meta_event_type"] == "lifecycle" {
			enabled <- struct{}{}
		}
		if v["post_type"] == "message" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"reply":"pong"}`))
			reported <- struct{}{}
		}
	}))
	defer peer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.RunPost(ctx, PostConfig{Name: "http-bot", URL: peer.URL, Secret: secret}) }()
	select {
	case <-enabled:
	case <-time.After(time.Second):
		t.Fatal("missing enable event")
	}
	c.append("private", "ping")
	select {
	case <-reported:
	case <-time.After(time.Second):
		t.Fatal("missing HTTP event")
	}
	deadline := time.Now().Add(time.Second)
	for {
		c.mu.Lock()
		n := len(c.sends)
		text := ""
		if n > 0 {
			text = c.sends[0].Text
		}
		c.mu.Unlock()
		if n == 1 && text == "pong" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("quick reply not sent")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case failure := <-invalid:
		t.Fatal(failure)
	default:
	}
	shutdown, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := s.Close(shutdown); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
