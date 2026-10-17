package onebot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/JFSAS/wispcat/pkg/bot"
)

type unavailableStorageChannel struct {
	*fakeChannel
	queued bool
}

func storageFault() error { return fmt.Errorf("private storage detail: %w", bot.ErrStorage) }
func (c unavailableStorageChannel) Message(string, string) (bot.Event, error) {
	return bot.Event{}, storageFault()
}
func (c unavailableStorageChannel) Delivery(string, string) (bot.Delivery, error) {
	return bot.Delivery{}, storageFault()
}
func (c unavailableStorageChannel) Download(context.Context, string, string, string) (bot.Content, error) {
	return bot.Content{}, storageFault()
}
func (c unavailableStorageChannel) Submit(context.Context, bot.Send) (bot.Delivery, error) {
	if c.queued {
		return bot.Delivery{ID: "queued", State: "queued"}, nil
	}
	return bot.Delivery{}, storageFault()
}

func TestArchiveStorageFailuresRemainObservableAcrossOneBotInterfaces(t *testing.T) {
	r, c := fixture(t)
	r.Channel = unavailableStorageChannel{fakeChannel: c}
	user, err := r.State.ID("account-a", "user", "friend")
	if err != nil {
		t.Fatal(err)
	}
	message, err := r.State.ID("account-a", "message", "native-receipt")
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := r.State.ID("account-a", "message", "delivery/archived")
	if err != nil {
		t.Fatal(err)
	}
	event, err := r.Convert(bot.Event{ID: "media", Account: "account-a", Conversation: bot.Conversation{ID: "friend", Kind: "private"}, Sender: bot.User{ID: "friend"}, Message: []bot.Segment{{Type: "image", Data: map[string]string{"file": "native/media"}}}})
	if err != nil {
		t.Fatal(err)
	}
	file := event["message"].([]bot.Segment)[0].Data["file"]
	for _, tc := range []struct {
		action string
		params string
		queued bool
	}{
		{"get_msg", fmt.Sprintf(`{"message_id":%d}`, message), false},
		{"get_msg", fmt.Sprintf(`{"message_id":%d}`, delivery), false},
		{"wechat_get_delivery", `{"delivery_id":"archived"}`, false},
		{"get_image", fmt.Sprintf(`{"file":%q}`, file), false},
		{"send_private_msg", fmt.Sprintf(`{"user_id":%d,"message":"hello"}`, user), false},
		{"send_private_msg", fmt.Sprintf(`{"user_id":%d,"message":"hello"}`, user), true},
	} {
		r.Channel = unavailableStorageChannel{fakeChannel: c, queued: tc.queued}
		response := invoke(t, r, tc.action, tc.params)
		if response.Status != "failed" || response.Retcode != 1500 || response.Message != "runtime_storage_unavailable" || string(response.Echo) != `{"id":42}` {
			t.Fatalf("%s queued=%v: %+v", tc.action, tc.queued, response)
		}
		raw, _ := json.Marshal(response)
		if strings.Contains(string(raw), "private storage detail") {
			t.Fatal("storage diagnostics leaked through OneBot response")
		}
	}
	s := NewServer(r, strings.Repeat("s", 32), "127.0.0.1:12345")
	request := httptest.NewRequest("GET", s.mediaURL("account-a", "friend", "native/media"), nil)
	response := httptest.NewRecorder()
	s.ServeHTTP(response, request)
	if response.Code != 503 || strings.TrimSpace(response.Body.String()) != "runtime_storage_unavailable" {
		t.Fatal(response.Code, response.Body.String())
	}
}

func TestStorageSnapshotFailurePrecedesConnectionAndCursorEffects(t *testing.T) {
	r, c := fixture(t)
	c.snap.Error = "event_history_unavailable"
	response := invoke(t, r, "get_status", `{}`)
	if response.Retcode != 1500 || response.Message != "runtime_storage_unavailable" {
		t.Fatal(response)
	}
	var requests atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer peer.Close()
	s := NewServer(r, strings.Repeat("t", 32), "127.0.0.1:12345")
	defer s.Close(context.Background())
	q := httptest.NewRequest("GET", "http://127.0.0.1:12345/ws", nil)
	q.Header.Set("Authorization", "Bearer "+s.Token)
	q.Header.Set("Upgrade", "websocket")
	q.Header.Set("Connection", "Upgrade")
	output := httptest.NewRecorder()
	s.ServeHTTP(output, q)
	if output.Code != 503 {
		t.Fatal(output.Code, output.Body.String())
	}
	if err := s.RunPost(context.Background(), PostConfig{Name: "post", URL: peer.URL}); !errors.Is(err, bot.ErrStorage) {
		t.Fatal(err)
	}
	if err := s.RunReverse(context.Background(), ReverseConfig{Name: "reverse", URL: strings.Replace(peer.URL, "http:", "ws:", 1), Role: "Universal"}); !errors.Is(err, bot.ErrStorage) {
		t.Fatal(err)
	}
	stats, err := r.State.Stats()
	if err != nil || stats.Cursors != 0 || stats.Identities != 0 || requests.Load() != 0 {
		t.Fatal("unavailable history advanced a consumer or connected", stats, requests.Load(), err)
	}
}

type unavailableDirectoryChannel struct{ *fakeChannel }

func (c unavailableDirectoryChannel) Users() ([]bot.User, error) { return nil, bot.ErrStorage }
func (c unavailableDirectoryChannel) Conversations() ([]bot.Conversation, error) {
	return nil, bot.ErrStorage
}
func (c unavailableDirectoryChannel) User(string, string) (bot.User, error) {
	return bot.User{}, bot.ErrStorage
}
func (c unavailableDirectoryChannel) Conversation(string, string) (bot.Conversation, error) {
	return bot.Conversation{}, bot.ErrStorage
}
func (c unavailableDirectoryChannel) Member(string, string, string) (bot.Membership, error) {
	return bot.Membership{}, bot.ErrStorage
}
func TestDirectoryErrorsRemainStorageFailuresAcrossOneBotActions(t *testing.T) {
	r, c := fixture(t)
	r.Channel = unavailableDirectoryChannel{c}
	user, _ := r.State.ID("account-a", "user", "friend")
	group, _ := r.State.ID("account-a", "group", "room")
	for _, tc := range []struct{ action, params string }{
		{"get_friend_list", `{}`}, {"get_group_list", `{}`},
		{"get_group_info", fmt.Sprintf(`{"group_id":%d}`, group)},
		{"get_group_member_info", fmt.Sprintf(`{"group_id":%d,"user_id":%d}`, group, user)},
		{"get_stranger_info", fmt.Sprintf(`{"user_id":%d}`, user)},
		{"send_private_msg", fmt.Sprintf(`{"user_id":%d,"message":"hello"}`, user)},
	} {
		got := invoke(t, r, tc.action, tc.params)
		if got.Retcode != 1500 || got.Message != "runtime_storage_unavailable" {
			t.Fatal(tc.action, got)
		}
	}
	if len(c.sends) != 0 {
		t.Fatal("directory failure queued a send")
	}
}
