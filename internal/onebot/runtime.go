// Package onebot converts a normalized channel to the OneBot v11 interface.
package onebot

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/JFSAS/wechat-runtime/pkg/bot"
)

type Request struct {
	Action string                     `json:"action"`
	Params map[string]json.RawMessage `json:"params,omitempty"`
	Echo   json.RawMessage            `json:"echo,omitempty"`
}
type Response struct {
	Status  string          `json:"status"`
	Retcode int             `json:"retcode"`
	Data    any             `json:"data"`
	Message string          `json:"message,omitempty"`
	Wording string          `json:"wording,omitempty"`
	Echo    json.RawMessage `json:"echo,omitempty"`
}
type Action struct {
	Name     string         `json:"name"`
	Summary  string         `json:"summary"`
	ReadOnly bool           `json:"read_only"`
	Schema   map[string]any `json:"input_schema"`
}
type Runtime struct {
	Channel       bot.Channel
	State         *State
	ActionTimeout time.Duration
	MediaURL      func(string, string, string) string
}

func New(channel bot.Channel, state *State) *Runtime {
	return &Runtime{Channel: channel, State: state, ActionTimeout: 30 * time.Second}
}
func good(data any) Response { return Response{Status: "ok", Data: data} }
func fail(code int, category string) Response {
	return Response{Status: "failed", Retcode: code, Message: category, Wording: category}
}
func (r *Runtime) Actions() []Action {
	items := []Action{}
	for _, v := range []struct {
		name, summary string
		read          bool
		required      []string
	}{
		{"get_version_info", "Runtime and protocol versions", true, nil}, {"get_status", "Channel connection status", true, nil}, {"get_login_info", "Current mapped account identity", true, nil},
		{"get_friend_list", "Known private contacts (cached)", true, nil}, {"get_group_list", "Known groups (cached)", true, nil}, {"get_group_info", "Cached group information", true, []string{"group_id"}},
		{"get_group_member_info", "Observed group member information; authority/roster completeness unknown", true, []string{"group_id", "user_id"}},
		{"get_stranger_info", "Known user information", true, []string{"user_id"}}, {"get_msg", "Read an observed message or delivery", true, []string{"message_id"}},
		{"send_msg", "Send text, mentions and bounded image/file parts", false, []string{"message"}}, {"send_private_msg", "Send a private message", false, []string{"user_id", "message"}}, {"send_group_msg", "Send a group message", false, []string{"group_id", "message"}},
		{"get_image", "Download a received image and resolve a bounded media URL", true, []string{"file"}},
		{"can_send_image", "Image submission capability", true, nil}, {"can_send_record", "Voice submission capability", true, nil}, {"wechat_get_capabilities", "Declared compatibility limits", true, nil}, {"wechat_get_delivery", "Read durable delivery status", true, []string{"delivery_id"}},
	} {
		p := map[string]any{}
		for _, name := range []string{"user_id", "group_id", "message_id", "self_id"} {
			p[name] = map[string]any{"anyOf": []any{map[string]any{"type": "integer", "minimum": 1, "maximum": 2147483647}, map[string]any{"type": "string", "pattern": "^[1-9][0-9]*$"}}}
		}
		p["message"] = map[string]any{"anyOf": []any{map[string]any{"type": "string"}, map[string]any{"type": "array", "items": map[string]any{"type": "object", "required": []string{"type", "data"}}}}}
		p["no_cache"] = map[string]any{"type": "boolean"}
		p["auto_escape"] = map[string]any{"type": "boolean"}
		p["message_type"] = map[string]any{"type": "string", "enum": []string{"private", "group"}}
		p["file"] = map[string]any{"type": "string"}
		p["delivery_id"] = map[string]any{"type": "string"}
		p["wechat_request_id"] = map[string]any{"type": "string", "maxLength": 128}
		names := append([]string(nil), v.required...)
		switch v.name {
		case "get_group_member_info":
			names = []string{"group_id", "user_id", "no_cache"}
		case "send_msg":
			names = []string{"message", "message_type", "user_id", "group_id", "auto_escape", "wechat_request_id"}
		case "send_private_msg":
			names = []string{"message", "user_id", "auto_escape", "wechat_request_id"}
		case "send_group_msg":
			names = []string{"message", "group_id", "auto_escape", "wechat_request_id"}
		}
		properties := map[string]any{"self_id": p["self_id"]}
		for _, name := range names {
			properties[name] = p[name]
		}
		schema := map[string]any{"type": "object", "properties": properties}
		if len(v.required) > 0 {
			schema["required"] = v.required
		}
		items = append(items, Action{v.name, v.summary, v.read, schema})
	}
	return items
}
func (r *Runtime) known(action string) bool {
	for _, a := range r.Actions() {
		if a.Name == action {
			return true
		}
	}
	return false
}
func (r *Runtime) Invoke(ctx context.Context, q Request) Response {
	limit := r.ActionTimeout
	if limit <= 0 {
		limit = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	out := r.invoke(ctx, q)
	out.Echo = q.Echo
	return out
}
func (r *Runtime) invoke(ctx context.Context, q Request) Response {
	if !r.known(q.Action) {
		return fail(1404, "action_not_supported")
	}
	if ctx.Err() != nil {
		return fail(1500, "request_cancelled")
	}
	snap := r.Channel.Snapshot()
	var self int64
	var err error
	if _, provided := q.Params["self_id"]; provided {
		requested, e := paramID(q.Params, "self_id")
		if e != nil {
			return fail(1400, "invalid_self_id")
		}
		if snap.Account == "" {
			return fail(1500, "account_unavailable")
		}
		self, err = r.State.ID(snap.Account, "user", snap.Self.ID)
		if err != nil {
			return fail(1500, "state_unavailable")
		}
		if requested != self {
			return fail(100, "account_scope_changed")
		}
	}
	if q.Action == "get_version_info" {
		return good(map[string]any{"app_name": "wechat-runtime", "app_version": "0.1.0-dev", "protocol_version": "v11", "platform": "wechat"})
	}
	if q.Action == "get_status" {
		return good(map[string]any{"online": snap.Online, "good": snap.Online, "wechat_state": snap.State})
	}
	if q.Action == "wechat_get_capabilities" {
		return good(map[string]any{"platform": "wechat", "actions": r.Actions(), "segments": []string{"text", "at", "image", "file"}, "media_input": "base64 or HTTP(S); up to 20 ordered native messages, 16MiB total media", "member_list_complete": false, "event_acknowledgement": false, "unsupported": []string{"reply segment", "recall", "voice send", "group administration", "friend approval"}})
	}
	if q.Action == "can_send_image" {
		return good(map[string]bool{"yes": true})
	}
	if q.Action == "can_send_record" {
		return good(map[string]bool{"yes": false})
	}
	if snap.Account == "" {
		return fail(1500, "account_unavailable")
	}
	if self == 0 {
		self, err = r.State.ID(snap.Account, "user", snap.Self.ID)
		if err != nil {
			return fail(1500, "state_unavailable")
		}
	}
	switch q.Action {
	case "get_group_member_info":
		group, e := paramID(q.Params, "group_id")
		if e != nil {
			return fail(1400, "invalid_group_id")
		}
		user, e := paramID(q.Params, "user_id")
		if e != nil {
			return fail(1400, "invalid_user_id")
		}
		fresh, e := paramBool(q.Params, "no_cache")
		if e != nil {
			return fail(1400, "invalid_no_cache")
		}
		if fresh {
			return fail(100, "fresh_member_lookup_not_supported")
		}
		g, ok, e := r.State.resolveChecked(snap.Account, "group", group)
		if e != nil {
			return fail(1500, "state_unavailable")
		}
		if !ok {
			return fail(100, "group_unavailable")
		}
		u, ok, e := r.State.resolveChecked(snap.Account, "user", user)
		if e != nil {
			return fail(1500, "state_unavailable")
		}
		if !ok {
			return fail(100, "member_unavailable")
		}
		roster, e := r.Channel.Members(snap.Account, g.Key)
		if e != nil {
			return fail(100, "group_unavailable")
		}
		for _, member := range roster.Users {
			if member.ID == u.Key {
				return good(map[string]any{"group_id": group, "user_id": user, "nickname": member.Name, "card": "", "wechat_role_known": false, "wechat_roster_complete": roster.Complete, "wechat_source": roster.Source})
			}
		}
		return fail(100, "member_unavailable")
	case "get_login_info":
		return good(map[string]any{"user_id": self, "nickname": snap.Self.Name})
	case "get_friend_list":
		items := []any{}
		for _, c := range r.Channel.Conversations() {
			if c.Kind == "group" {
				continue
			}
			id, e := r.State.ID(snap.Account, "user", c.ID)
			if e != nil {
				return fail(1500, "state_unavailable")
			}
			items = append(items, map[string]any{"user_id": id, "nickname": c.Name, "remark": c.Name})
		}
		return good(items)
	case "get_group_list", "get_group_info":
		items := []any{}
		var want int64
		if q.Action == "get_group_info" {
			want, err = paramID(q.Params, "group_id")
			if err != nil {
				return fail(1400, "invalid_group_id")
			}
		}
		for _, c := range r.Channel.Conversations() {
			if c.Kind != "group" {
				continue
			}
			id, e := r.State.ID(snap.Account, "group", c.ID)
			if e != nil {
				return fail(1500, "state_unavailable")
			}
			v := map[string]any{"group_id": id, "group_name": c.Name, "wechat_members_complete": false}
			if want == id {
				return good(v)
			}
			items = append(items, v)
		}
		if want != 0 {
			return fail(100, "group_unavailable")
		}
		return good(items)
	case "get_stranger_info":
		id, e := paramID(q.Params, "user_id")
		if e != nil {
			return fail(1400, "invalid_user_id")
		}
		ref, ok, e := r.State.resolveChecked(snap.Account, "user", id)
		if e != nil {
			return fail(1500, "state_unavailable")
		}
		if !ok {
			return fail(100, "user_unavailable")
		}
		for _, u := range r.Channel.Users() {
			if u.ID == ref.Key {
				return good(map[string]any{"user_id": id, "nickname": u.Name, "sex": "unknown", "age": 0})
			}
		}
		return fail(100, "user_unavailable")
	case "get_msg":
		id, e := paramID(q.Params, "message_id")
		if e != nil {
			return fail(1400, "invalid_message_id")
		}
		ref, ok, e := r.State.resolveChecked(snap.Account, "message", id)
		if e != nil {
			return fail(1500, "state_unavailable")
		}
		if !ok {
			return fail(100, "message_unavailable")
		}
		if strings.HasPrefix(ref.Key, "delivery/") {
			d, e := r.Channel.Delivery(snap.Account, strings.TrimPrefix(ref.Key, "delivery/"))
			if e != nil {
				return fail(100, "message_unavailable")
			}
			conversation := bot.Conversation{ID: d.Conversation, Kind: "private"}
			for _, c := range r.Channel.Conversations() {
				if c.ID == d.Conversation {
					conversation = c
					break
				}
			}
			message := d.Message
			if len(message) == 0 {
				message = []bot.Segment{{Type: "text", Data: map[string]string{"text": d.Text}}}
			}
			v, e := r.Convert(bot.Event{ID: ref.Key, Account: snap.Account, Conversation: conversation, Sender: snap.Self, Time: d.Time, Message: message, Own: true})
			if e != nil {
				return fail(1500, "message_conversion_failed")
			}
			v["real_id"] = id
			v["wechat_delivery_state"] = d.State
			delete(v, "post_type")
			delete(v, "self_id")
			return good(v)
		}
		ev, e := r.Channel.Message(snap.Account, ref.Key)
		if e != nil {
			return fail(100, "message_unavailable")
		}
		v, e := r.Convert(ev)
		if e != nil {
			return fail(1500, "message_conversion_failed")
		}
		v["real_id"] = id
		delete(v, "post_type")
		delete(v, "self_id")
		return good(v)
	case "wechat_get_delivery":
		id, e := paramString(q.Params, "delivery_id")
		if e != nil {
			return fail(1400, "invalid_delivery_id")
		}
		d, e := r.Channel.Delivery(snap.Account, id)
		if e != nil {
			return fail(100, "delivery_unavailable")
		}
		return good(d)
	case "get_image":
		file, e := paramString(q.Params, "file")
		if e != nil {
			return fail(1400, "invalid_file")
		}
		conversation, source, e := r.fileReference(snap.Account, file)
		if e != nil {
			if errors.Is(e, errStateUnavailable) {
				return fail(1500, "state_unavailable")
			}
			return fail(100, "file_unavailable")
		}
		wait, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		content, e := r.Channel.Download(wait, snap.Account, conversation, source)
		if e != nil {
			return fail(100, "media_unavailable")
		}
		link := ""
		if r.MediaURL != nil {
			link = r.MediaURL(snap.Account, conversation, source)
		}
		return good(map[string]any{"file": file, "filename": content.Name, "size": len(content.Data), "url": link})
	case "send_msg", "send_private_msg", "send_group_msg":
		return r.send(ctx, q, snap)
	}
	return fail(1404, "action_not_supported")
}
func paramString(p map[string]json.RawMessage, key string) (string, error) {
	var value string
	if json.Unmarshal(p[key], &value) != nil || value == "" {
		return "", errors.New("invalid_" + key)
	}
	return value, nil
}
func paramID(p map[string]json.RawMessage, key string) (int64, error) {
	raw := strings.TrimSpace(string(p[key]))
	if strings.HasPrefix(raw, "\"") {
		s, e := paramString(p, key)
		if e != nil {
			return 0, e
		}
		raw = s
	}
	n, e := strconv.ParseInt(raw, 10, 64)
	if e != nil || n < 1 || n > 2147483647 {
		return 0, errors.New("invalid_" + key)
	}
	return n, nil
}
func paramBool(p map[string]json.RawMessage, key string) (bool, error) {
	raw, ok := p[key]
	if !ok {
		return false, nil
	}
	switch string(raw) {
	case "true", "\"true\"", "1", "\"1\"":
		return true, nil
	case "false", "\"false\"", "0", "\"0\"":
		return false, nil
	}
	return false, errors.New("invalid_" + key)
}

func (r *Runtime) send(ctx context.Context, q Request, snap bot.Snapshot) Response {
	group := q.Action == "send_group_msg"
	if q.Action == "send_msg" {
		kind, _ := paramString(q.Params, "message_type")
		_, has := q.Params["group_id"]
		group = kind == "group" || kind == "" && has
		if kind != "" && kind != "private" && kind != "group" {
			return fail(1400, "invalid_message_type")
		}
	}
	field, kind := "user_id", "user"
	if group {
		field, kind = "group_id", "group"
	}
	id, err := paramID(q.Params, field)
	if err != nil {
		return fail(1400, "invalid_"+field)
	}
	ref, ok, err := r.State.resolveChecked(snap.Account, kind, id)
	if err != nil {
		return fail(1500, "state_unavailable")
	}
	if !ok {
		return fail(100, "target_unavailable")
	}
	exists := false
	for _, c := range r.Channel.Conversations() {
		if c.ID == ref.Key && ((c.Kind == "group") == group) {
			exists = true
			break
		}
	}
	if !exists {
		return fail(100, "target_unavailable")
	}
	req, err := r.prepareSend(ctx, q.Params, snap, ref.Key, group)
	if err != nil {
		if errors.Is(err, errStateUnavailable) {
			return fail(1500, "state_unavailable")
		}
		return fail(1400, err.Error())
	}
	d, err := r.Channel.Submit(ctx, req)
	if err != nil {
		if errors.Is(err, bot.ErrConflict) {
			return fail(100, "idempotency_conflict")
		}
		if errors.Is(err, bot.ErrScope) {
			return fail(100, "account_scope_changed")
		}
		return fail(100, "send_not_queued")
	}
	timeout := r.ActionTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	wait, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		switch d.State {
		case "accepted", "echo_observed":
			parts := d.Parts
			if len(parts) == 0 {
				parts = []bot.Delivery{d}
			}
			messageIDs := []int64{}
			deliveryIDs := []string{}
			for _, part := range parts {
				messageKey := part.MessageID
				if messageKey == "" {
					messageKey = "delivery/" + part.ID
				}
				mid, e := r.State.ID(snap.Account, "message", messageKey)
				if e != nil {
					return fail(1500, "state_unavailable")
				}
				messageIDs = append(messageIDs, mid)
				deliveryIDs = append(deliveryIDs, part.ID)
			}
			return good(map[string]any{"message_id": messageIDs[0], "wechat_message_ids": messageIDs, "wechat_delivery_id": d.ID, "wechat_delivery_ids": deliveryIDs, "wechat_request_id": req.Key})
		case "failed", "unknown", "cancelled":
			v := fail(100, "delivery_"+d.State)
			v.Data = map[string]any{"wechat_delivery_id": d.ID, "wechat_request_id": req.Key, "wechat_parts": d.Parts}
			return v
		}
		select {
		case <-wait.Done():
			v := fail(100, "delivery_pending_or_unknown")
			v.Data = map[string]any{"wechat_delivery_id": d.ID, "wechat_request_id": req.Key, "wechat_parts": d.Parts}
			return v
		case <-tick.C:
		}
		d, err = r.Channel.Delivery(snap.Account, d.ID)
		if err != nil {
			return fail(100, "delivery_unavailable")
		}
	}
}
func parseMessage(raw json.RawMessage, auto bool) ([]bot.Segment, error) {
	var text string
	if json.Unmarshal(raw, &text) == nil {
		if auto {
			return []bot.Segment{{Type: "text", Data: map[string]string{"text": text}}}, nil
		}
		return parseCQ(text)
	}
	var parts []bot.Segment
	if json.Unmarshal(raw, &parts) != nil || len(parts) == 0 || len(parts) > 100 {
		return nil, errors.New("invalid_message")
	}
	for _, s := range parts {
		if s.Type == "" || s.Data == nil {
			return nil, errors.New("invalid_segment")
		}
	}
	return parts, nil
}
func unescape(s string, parameter bool) string {
	if parameter {
		s = strings.ReplaceAll(s, "&#44;", ",")
	}
	s = strings.ReplaceAll(s, "&#91;", "[")
	s = strings.ReplaceAll(s, "&#93;", "]")
	return strings.ReplaceAll(s, "&amp;", "&")
}
func parseCQ(text string) ([]bot.Segment, error) {
	result := []bot.Segment{}
	for len(text) > 0 {
		n := strings.Index(text, "[CQ:")
		if n < 0 {
			result = append(result, bot.Segment{Type: "text", Data: map[string]string{"text": unescape(text, false)}})
			break
		}
		if n > 0 {
			result = append(result, bot.Segment{Type: "text", Data: map[string]string{"text": unescape(text[:n], false)}})
		}
		text = text[n+4:]
		end := strings.IndexByte(text, ']')
		if end < 0 {
			return nil, errors.New("invalid_cq_code")
		}
		fields := strings.Split(text[:end], ",")
		seg := bot.Segment{Type: fields[0], Data: map[string]string{}}
		for _, f := range fields[1:] {
			key, value, ok := strings.Cut(f, "=")
			if !ok || key == "" {
				return nil, errors.New("invalid_cq_code")
			}
			if _, duplicate := seg.Data[key]; duplicate {
				return nil, errors.New("invalid_cq_code")
			}
			seg.Data[key] = unescape(value, true)
		}
		result = append(result, seg)
		text = text[end+1:]
		if len(result) > 100 {
			return nil, errors.New("message_too_complex")
		}
	}
	if len(result) == 0 {
		return nil, errors.New("empty_message")
	}
	return result, nil
}
func (r *Runtime) Convert(ev bot.Event) (map[string]any, error) {
	snap := r.Channel.Snapshot()
	if snap.Account != ev.Account {
		return nil, bot.ErrScope
	}
	identities := []Identity{{"user", snap.Self.ID}, {"user", ev.Sender.ID}, {"message", ev.ID}}
	if ev.Conversation.Kind == "group" {
		identities = append(identities, Identity{"group", ev.Conversation.ID})
	}
	for _, part := range ev.Message {
		switch part.Type {
		case "at":
			identities = append(identities, Identity{"user", part.Data["user_id"]})
		case "image", "file":
			identities = append(identities, Identity{"file", ev.Conversation.ID + "\x00" + part.Data["file"]})
		}
	}
	ids, err := r.State.IDBatch(ev.Account, identities)
	if err != nil {
		return nil, err
	}
	lookup := map[Identity]int64{}
	for i, identity := range identities {
		lookup[identity] = ids[i]
	}
	self, sender, mid := ids[0], ids[1], ids[2]
	segments := []bot.Segment{}
	for _, s := range ev.Message {
		data := map[string]string{}
		for k, v := range s.Data {
			data[k] = v
		}
		if s.Type == "at" {
			n := lookup[Identity{"user", data["user_id"]}]
			data = map[string]string{"qq": strconv.FormatInt(n, 10)}
		}
		if s.Type == "image" || s.Type == "file" {
			source := data["file"]
			n := lookup[Identity{"file", ev.Conversation.ID + "\x00" + source}]
			data["file"] = strconv.FormatInt(n, 10)
			if r.MediaURL != nil {
				data["url"] = r.MediaURL(ev.Account, ev.Conversation.ID, source)
			}
		}

		segments = append(segments, bot.Segment{Type: s.Type, Data: data})
	}
	kind, sub := "private", "friend"
	if ev.Conversation.Kind == "group" {
		kind, sub = "group", "normal"
	}
	v := map[string]any{"time": ev.Time, "self_id": self, "post_type": "message", "message_type": kind, "sub_type": sub, "message_id": mid, "user_id": sender, "message": segments, "raw_message": renderCQ(segments), "font": 0, "sender": map[string]any{"user_id": sender, "nickname": ev.Sender.Name, "sex": "unknown", "age": 0}, "wechat_event_id": ev.ID, "wechat_sequence": ev.Sequence}
	if kind == "group" {
		n := lookup[Identity{"group", ev.Conversation.ID}]
		v["group_id"] = n
		v["anonymous"] = nil
		v["sender"].(map[string]any)["wechat_role_known"] = false
	}
	return v, nil
}
