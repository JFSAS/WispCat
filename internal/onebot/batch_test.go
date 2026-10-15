package onebot

import (
	"encoding/json"
	"testing"
)

func TestMixedMessageReturnsIndividualNativeReceipts(t *testing.T) {
	r, c := fixture(t)
	id, _ := r.State.ID("account-a", "user", "friend")
	result := invoke(t, r, "send_private_msg", `{"user_id":`+jsonNumber(id)+`,"message":[{"type":"text","data":{"text":"before"}},{"type":"image","data":{"file":"base64://c3ludGhldGlj"}},{"type":"text","data":{"text":"after"}}],"wechat_request_id":"mixed"}`)
	if result.Retcode != 0 || len(c.sends) != 1 || len(c.sends[0].Outputs) != 3 {
		t.Fatal(result)
	}
	out := result.Data.(map[string]any)
	ids := out["wechat_message_ids"].([]int64)
	if len(ids) != 3 || ids[0] == ids[1] || ids[1] == ids[2] || out["message_id"] != ids[0] {
		t.Fatal("native receipts collapsed", out)
	}
	parts := c.sends[0].Outputs
	if parts[0].Text != "before" || parts[1].Upload == nil || parts[2].Text != "after" {
		t.Fatal("input order lost")
	}
	params, _ := json.Marshal(map[string]any{"user_id": id, "message": "different", "wechat_request_id": "mixed"})
	if changed := invoke(t, r, "send_private_msg", string(params)); changed.Retcode == 0 {
		t.Fatal("mixed key conflict accepted")
	}
}

func TestTextBeforeMentionKeepsLogicalOrder(t *testing.T) {
	r, c := fixture(t)
	group, _ := r.State.ID("account-a", "group", "room")
	user, _ := r.State.ID("account-a", "user", "friend")
	result := invoke(t, r, "send_group_msg", `{"group_id":`+jsonNumber(group)+`,"message":[{"type":"text","data":{"text":"before"}},{"type":"at","data":{"qq":"`+jsonNumber(user)+`"}},{"type":"text","data":{"text":"after"}}]}`)
	if result.Retcode != 0 || len(c.sends) != 1 || len(c.sends[0].Outputs) != 2 {
		t.Fatal(result)
	}
	parts := c.sends[0].Outputs
	if parts[0].Text != "before" || len(parts[0].Mentions) != 0 || parts[1].Text != "after" || len(parts[1].Mentions) != 1 {
		t.Fatal("mention moved ahead of preceding text", parts)
	}
}
