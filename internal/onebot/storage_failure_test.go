package onebot

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/JFSAS/wechat-runtime/pkg/bot"
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
