package onebot

import "github.com/JFSAS/wispcat/pkg/bot"

func (r *Runtime) deliveryByRequest(q Request, snap bot.Snapshot) Response {
	key, err := paramString(q.Params, "wechat_request_id")
	if err != nil || len(key) > 128 {
		return fail(1400, "invalid_wechat_request_id")
	}
	delivery, err := r.Channel.DeliveryByRequest(snap.Account, key)
	if err != nil {
		return channelFailure(err, 100, "delivery_unavailable")
	}
	data := map[string]any{"wechat_request_id": key, "delivery": delivery}
	if delivery.State == "accepted" || delivery.State == "echo_observed" {
		receipt, err := r.confirmedReceipt(snap.Account, key, delivery)
		if err != nil {
			return fail(1500, "state_unavailable")
		}
		data["receipt"] = receipt
	}
	return good(data)
}

// Confirmed lookup and synchronous sends must publish exactly the same durable
// public IDs. Query success for queued/unknown/failed is not send success.
func (r *Runtime) confirmedReceipt(account, key string, delivery bot.Delivery) (map[string]any, error) {
	parts := delivery.Parts
	if len(parts) == 0 {
		parts = []bot.Delivery{delivery}
	}
	messageIDs := []int64{}
	deliveryIDs := []string{}
	for _, part := range parts {
		messageKey := part.MessageID
		if messageKey == "" {
			messageKey = "delivery/" + part.ID
		}
		id, err := r.State.ID(account, "message", messageKey)
		if err != nil {
			return nil, err
		}
		messageIDs = append(messageIDs, id)
		deliveryIDs = append(deliveryIDs, part.ID)
	}
	return map[string]any{"message_id": messageIDs[0], "wechat_message_ids": messageIDs,
		"wechat_delivery_id": delivery.ID, "wechat_delivery_ids": deliveryIDs, "wechat_request_id": key}, nil
}
