package onebot

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/JFSAS/wispcat/pkg/bot"
)

func TestRequestLookupReturnsOriginalReceiptWithoutResubmission(t *testing.T) {
	r, c := fixture(t)
	id, _ := r.State.ID("account-a", "user", "friend")
	sent := invoke(t, r, "send_private_msg", fmt.Sprintf(`{"user_id":%d,"message":"once","wechat_request_id":"durable-key"}`, id))
	if sent.Retcode != 0 {
		t.Fatal(sent)
	}
	looked := invoke(t, r, "wechat_get_delivery_by_request", `{"wechat_request_id":"durable-key"}`)
	data := looked.Data.(map[string]any)
	a, _ := json.Marshal(sent.Data)
	b, _ := json.Marshal(data["receipt"])
	if looked.Retcode != 0 || string(a) != string(b) || len(c.sends) != 1 {
		t.Fatal("lookup changed receipt or re-executed send", looked)
	}
	for _, action := range r.Actions() {
		if action.Name == "wechat_get_delivery_by_request" && (!action.ReadOnly || action.Category != "messages") {
			t.Fatal("lookup was advertised as a mutation", action)
		}
	}
}

func TestRequestLookupDoesNotPromoteUnconfirmedStatesToReceipts(t *testing.T) {
	r, c := fixture(t)
	for _, state := range []string{"queued", "sending", "unknown", "failed", "cancelled"} {
		c.deliveries["unconfirmed"] = bot.Delivery{ID: "unconfirmed", State: state, Conversation: "friend"}
		out := invoke(t, r, "wechat_get_delivery_by_request", `{"wechat_request_id":"unconfirmed"}`)
		data := out.Data.(map[string]any)
		if out.Retcode != 0 || data["receipt"] != nil || data["delivery"].(bot.Delivery).State != state || len(c.sends) != 0 {
			t.Fatal("read success became send success", out)
		}
	}
	for _, params := range []string{`{}`, `{"wechat_request_id":null}`, `{"wechat_request_id":1}`, `{"wechat_request_id":""}`, `{"wechat_request_id":"` + strings.Repeat("x", 129) + `"}`} {
		if out := invoke(t, r, "wechat_get_delivery_by_request", params); out.Retcode != 1400 {
			t.Fatal("invalid lookup key accepted", out)
		}
	}
	if out := invoke(t, r, "wechat_get_delivery_by_request", `{"wechat_request_id":"missing"}`); out.Retcode == 0 {
		t.Fatal("missing request became success", out)
	}
}

type requestLookupFailure struct{ *fakeChannel }

func (requestLookupFailure) DeliveryByRequest(string, string) (bot.Delivery, error) {
	return bot.Delivery{}, fmt.Errorf("private backend detail: %w", bot.ErrStorage)
}

func TestRequestLookupStorageErrorsRemainExplicitAndPrivate(t *testing.T) {
	r, c := fixture(t)
	r.Channel = requestLookupFailure{c}
	out := invoke(t, r, "wechat_get_delivery_by_request", `{"wechat_request_id":"key"}`)
	raw, _ := json.Marshal(out)
	if out.Retcode != 1500 || strings.Contains(string(raw), "private backend detail") {
		t.Fatal("lookup lost or exposed storage failure", out)
	}
	r.Channel = c
	c.deliveries["confirmed"] = bot.Delivery{ID: "confirmed", State: "accepted", Conversation: "friend"}
	_ = r.State.Close()
	if out := invoke(t, r, "wechat_get_delivery_by_request", `{"wechat_request_id":"confirmed"}`); out.Retcode != 1500 {
		t.Fatal("receipt escaped without durable public IDs", out)
	}
}
