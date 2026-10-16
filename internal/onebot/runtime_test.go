package onebot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JFSAS/wechat-runtime/pkg/bot"
)

type fakeChannel struct {
	mu         sync.Mutex
	snap       bot.Snapshot
	events     []bot.Event
	sends      []bot.Send
	deliveries map[string]bot.Delivery
	keys       map[string]string
	pending    bool
}

func fixture(t *testing.T) (*Runtime, *fakeChannel) {
	t.Helper()
	state, e := OpenState(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = state.Close() })
	c := &fakeChannel{snap: bot.Snapshot{Account: "account-a", Self: bot.User{ID: "owner", Name: "Bot"}, Online: true, State: "connected", Epoch: 1}, deliveries: map[string]bot.Delivery{}, keys: map[string]string{}}
	return New(c, state), c
}
func (c *fakeChannel) Snapshot() bot.Snapshot { c.mu.Lock(); defer c.mu.Unlock(); return c.snap }
func (c *fakeChannel) Users() ([]bot.User, error) {
	return []bot.User{{ID: "owner", Name: "Bot"}, {ID: "friend", Name: "Friend"}}, nil
}
func (c *fakeChannel) Members(account, conversation string) (bot.Roster, error) {
	if account != c.Snapshot().Account {
		return bot.Roster{}, bot.ErrScope
	}
	if conversation != "room" {
		return bot.Roster{}, bot.ErrNotFound
	}
	users, _ := c.Users()
	return bot.Roster{Users: users, Complete: false, Source: "observed_messages"}, nil
}
func (c *fakeChannel) Conversations() ([]bot.Conversation, error) {
	return []bot.Conversation{{ID: "friend", Kind: "private", Name: "Friend"}, {ID: "room", Kind: "group", Name: "Group"}}, nil
}
func (c *fakeChannel) Events(account string, after uint64, limit int) ([]bot.Event, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.snap.Account != account {
		return nil, bot.ErrScope
	}
	if after > c.snap.Cursor || after < c.snap.FirstCursor {
		return nil, bot.ErrCursor
	}
	r := []bot.Event{}
	for _, v := range c.events {
		if v.Sequence > after {
			r = append(r, v)
			if len(r) >= limit {
				break
			}
		}
	}
	return r, nil
}
func (c *fakeChannel) Message(account, id string) (bot.Event, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, v := range c.events {
		if v.Account == account && v.ID == id {
			return v, nil
		}
	}
	return bot.Event{}, bot.ErrNotFound
}
func (c *fakeChannel) Submit(ctx context.Context, q bot.Send) (bot.Delivery, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if q.Account != c.snap.Account || q.Epoch != c.snap.Epoch {
		return bot.Delivery{}, bot.ErrScope
	}
	raw, _ := json.Marshal(q)
	if old, ok := c.keys[q.Key]; ok {
		if old != string(raw) {
			return bot.Delivery{}, bot.ErrConflict
		}
		return c.deliveries[q.Key], nil
	}
	c.keys[q.Key] = string(raw)
	c.sends = append(c.sends, q)
	state := "accepted"
	if c.pending {
		state = "queued"
	}
	d := bot.Delivery{ID: q.Key, State: state, Conversation: q.Conversation, Text: q.Text, Time: time.Now().Unix()}
	if len(q.Outputs) > 0 {
		parts := []bot.Delivery{}
		for i, out := range q.Outputs {
			part := bot.Delivery{ID: q.Key, State: state, Conversation: q.Conversation, Text: out.Text, Time: d.Time}
			if i > 0 {
				part.ID = q.Key + "/" + strconv.Itoa(i)
			}
			if out.Upload != nil {
				part.Message = []bot.Segment{{Type: out.Upload.Kind, Data: map[string]string{"file": part.ID + "/cache"}}}
			}
			parts = append(parts, part)
			c.deliveries[part.ID] = part
		}
		d = parts[0]
		d.Parts = parts
	}
	c.deliveries[d.ID] = d
	return d, nil
}
func (c *fakeChannel) Delivery(account, id string) (bot.Delivery, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if account != c.snap.Account {
		return bot.Delivery{}, bot.ErrScope
	}
	d, ok := c.deliveries[id]
	if !ok {
		return d, bot.ErrNotFound
	}
	return d, nil
}
func (c *fakeChannel) DeliveryByRequest(account, key string) (bot.Delivery, error) {
	return c.Delivery(account, key)
}
func (c *fakeChannel) Download(context.Context, string, string, string) (bot.Content, error) {
	return bot.Content{}, bot.ErrUnsupported
}
func (c *fakeChannel) append(kind, text string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snap.Cursor++
	c.events = append(c.events, bot.Event{ID: "event-" + text, Sequence: c.snap.Cursor, Account: c.snap.Account, Conversation: bot.Conversation{ID: "friend", Kind: kind}, Sender: bot.User{ID: "friend", Name: "Friend"}, Time: 1700000000, Message: []bot.Segment{{Type: "text", Data: map[string]string{"text": text}}}})
}
func invoke(t *testing.T, r *Runtime, action, params string) Response {
	t.Helper()
	var p map[string]json.RawMessage
	if json.Unmarshal([]byte(params), &p) != nil {
		t.Fatal("bad test JSON")
	}
	return r.Invoke(context.Background(), Request{Action: action, Params: p, Echo: json.RawMessage(`{"id":42}`)})
}

func TestStateIDsSurviveRestartAndRejectCorruption(t *testing.T) {
	dir := t.TempDir()
	s := openLegacyFixture(t, dir)
	a, _ := s.ID("a", "user", "person")
	b, _ := s.ID("a", "group", "person")
	c, _ := s.ID("b", "user", "person")
	if a == b || a == c || b == c {
		t.Fatal("mapping collision")
	}
	if err := s.setCursor("a", "consumer", 17); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenState(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := reopened.ID("a", "user", "person")
	cursor, cursorErr := reopened.cursor("a", "consumer", 0)
	if got != a || cursor != 17 || cursorErr != nil {
		t.Fatal("identity or cursor changed after restart")
	}
	if _, ok, err := reopened.resolveChecked("b", "user", a); ok || err != nil {
		t.Fatal("cross-account reverse lookup")
	}
	if err = reopened.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "onebot.state"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("person")) {
		t.Fatal("mapping stored in plaintext")
	}
	raw[len(raw)-1] ^= 1
	if err = os.WriteFile(filepath.Join(dir, "onebot.state"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenState(dir); err == nil {
		t.Fatal("tampered state reset or accepted")
	}
	if err = os.Remove(filepath.Join(dir, "onebot.key")); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenState(dir); err == nil {
		t.Fatal("missing key replaced for an existing state")
	}
	if _, err = os.Stat(filepath.Join(dir, "onebot.key")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("replacement key created")
	}
}
func TestSendMapsTargetAndWaitsForReceipt(t *testing.T) {
	r, c := fixture(t)
	friends := invoke(t, r, "get_friend_list", `{}`)
	user := friends.Data.([]any)[0].(map[string]any)["user_id"].(int64)
	params := `{"user_id":` + jsonNumber(user) + `,"message":[{"type":"text","data":{"text":"hello"}}],"wechat_request_id":"stable"}`
	result := invoke(t, r, "send_private_msg", params)
	if result.Retcode != 0 || result.Status != "ok" || string(result.Echo) != `{"id":42}` {
		t.Fatal(result)
	}
	if got := invoke(t, r, "send_private_msg", params); got.Retcode != 0 || len(c.sends) != 1 {
		t.Fatal("idempotent action duplicated", got)
	}
	if c.sends[0].Conversation != "friend" || c.sends[0].Text != "hello" {
		t.Fatal("bad target conversion")
	}
	if got := invoke(t, r, "send_private_msg", strings.Replace(params, "hello", "different", 1)); got.Retcode == 0 || len(c.sends) != 1 {
		t.Fatal("conflicting key accepted")
	}
	ref := result.Data.(map[string]any)["message_id"].(int64)
	if got := invoke(t, r, "get_msg", `{"message_id":`+jsonNumber(ref)+`}`); got.Retcode != 0 {
		t.Fatal(got)
	}
}
func jsonNumber(v int64) string { raw, _ := json.Marshal(v); return string(raw) }

type cancelAfterSubmitChannel struct {
	*fakeChannel
	cancel context.CancelFunc
}

func (c *cancelAfterSubmitChannel) Submit(ctx context.Context, request bot.Send) (bot.Delivery, error) {
	delivery, err := c.fakeChannel.Submit(ctx, request)
	if err == nil {
		c.cancel()
	}
	return delivery, err
}

func TestPendingSendIsNotSuccessAndIsNotReplayed(t *testing.T) {
	r, c := fixture(t)
	c.pending = true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Channel = &cancelAfterSubmitChannel{fakeChannel: c, cancel: cancel}
	id, _ := r.State.ID("account-a", "user", "friend")
	var params map[string]json.RawMessage
	_ = json.Unmarshal([]byte(`{"user_id":`+jsonNumber(id)+`,"message":"hello","wechat_request_id":"pending"}`), &params)
	// Cancel only after the synthetic channel has committed the submission.
	// A short wall-clock deadline can expire during ID/storage work instead.
	result := r.Invoke(ctx, Request{Action: "send_private_msg", Params: params, Echo: json.RawMessage(`{"id":42}`)})
	if result.Retcode == 0 || result.Message != "delivery_pending_or_unknown" || len(c.sends) != 1 {
		t.Fatal(result)
	}
	c.mu.Lock()
	d := c.deliveries["pending"]
	d.State = "unknown"
	c.deliveries["pending"] = d
	c.mu.Unlock()
	result = invoke(t, r, "send_private_msg", `{"user_id":`+jsonNumber(id)+`,"message":"hello","wechat_request_id":"pending"}`)
	if result.Message != "delivery_unknown" || len(c.sends) != 1 {
		t.Fatal(result)
	}
}
func TestInvalidSegmentsAndIDsNeverSendPartialContent(t *testing.T) {
	r, c := fixture(t)
	id, _ := r.State.ID("account-a", "user", "friend")
	for _, message := range []string{`[{"type":"text","data":{"text":"hello"}},{"type":"reply","data":{"id":"1"}}]`, `[{"type":"at","data":{"qq":"all"}}]`, `"[CQ:unknown,x=y]"`, `[{"type":"image","data":{"file":"/private/file"}}]`} {
		v := invoke(t, r, "send_private_msg", `{"user_id":`+jsonNumber(id)+`,"message":`+message+`}`)
		if v.Retcode == 0 {
			t.Fatal("unsupported message accepted", v)
		}
	}
	for _, bad := range []string{`1.5`, `0`, `-1`, `""`, `"abc"`, `2147483648`} {
		if v := invoke(t, r, "send_private_msg", `{"user_id":`+bad+`,"message":"hello"}`); v.Retcode != 1400 {
			t.Fatal("invalid identifier accepted", bad, v)
		}
	}
	if len(c.sends) != 0 {
		t.Fatal("invalid input caused a partial send")
	}
}
func TestEventConvertsIntoOneBotMessageAndKeepsInputImmutable(t *testing.T) {
	r, c := fixture(t)
	c.append("group", "hi")
	ev := c.events[0]
	ev.Conversation.ID = "room"
	ev.Message = append([]bot.Segment{{Type: "at", Data: map[string]string{"user_id": "owner"}}}, ev.Message...)
	v, err := r.Convert(ev)
	if err != nil {
		t.Fatal(err)
	}
	if v["post_type"] != "message" || v["message_type"] != "group" || v["sub_type"] != "normal" || v["group_id"] == nil {
		t.Fatal(v)
	}
	self := v["self_id"].(int64)
	seg := v["message"].([]bot.Segment)[0]
	if seg.Data["qq"] != jsonNumber(self) || ev.Message[0].Data["user_id"] != "owner" {
		t.Fatal("mention identity lost or input mutated")
	}
	private := ev
	private.Conversation = bot.Conversation{ID: "friend", Kind: "private"}
	p, e := r.Convert(private)
	if e != nil || p["user_id"] != v["user_id"] {
		t.Fatal("group and private user IDs differ")
	}
}
func TestCQEscapingAndAutoEscape(t *testing.T) {
	parts, err := parseMessage(json.RawMessage(`"hi &amp; &#91;x&#93;[CQ:at,qq=123]"`), false)
	if err != nil || len(parts) != 2 || parts[0].Data["text"] != "hi & [x]" || parts[1].Data["qq"] != "123" {
		t.Fatal(parts, err)
	}
	parts, err = parseMessage(json.RawMessage(`"[CQ:at,qq=123]"`), true)
	if err != nil || len(parts) != 1 || parts[0].Type != "text" {
		t.Fatal(parts, err)
	}
}
func TestEchoDoesNotBecomeAnIdempotencyKey(t *testing.T) {
	r, c := fixture(t)
	id, _ := r.State.ID("account-a", "user", "friend")
	for range 2 {
		v := invoke(t, r, "send_private_msg", `{"user_id":`+jsonNumber(id)+`,"message":"same"}`)
		if v.Retcode != 0 {
			t.Fatal(v)
		}
	}
	if len(c.sends) != 2 || c.sends[0].Key == c.sends[1].Key {
		t.Fatal("echo used to deduplicate legitimate actions")
	}
}
func TestCatalogIsUniqueAndExecutable(t *testing.T) {
	r, _ := fixture(t)
	seen := map[string]bool{}
	for _, a := range r.Actions() {
		if seen[a.Name] || a.Schema == nil {
			t.Fatal("invalid action catalog")
		}
		seen[a.Name] = true
		result := invoke(t, r, a.Name, `{}`)
		if result.Retcode == 1404 {
			t.Fatalf("catalog action %s has no implementation", a.Name)
		}
	}
}

func (c *fakeChannel) User(account, id string) (bot.User, error) {
	if account != c.Snapshot().Account {
		return bot.User{}, bot.ErrScope
	}
	rows, err := c.Users()
	if err != nil {
		return bot.User{}, err
	}
	for _, row := range rows {
		if row.ID == id {
			return row, nil
		}
	}
	return bot.User{}, bot.ErrNotFound
}
func (c *fakeChannel) Conversation(account, id string) (bot.Conversation, error) {
	if account != c.Snapshot().Account {
		return bot.Conversation{}, bot.ErrScope
	}
	rows, err := c.Conversations()
	if err != nil {
		return bot.Conversation{}, err
	}
	for _, row := range rows {
		if row.ID == id {
			return row, nil
		}
	}
	return bot.Conversation{}, bot.ErrNotFound
}

func (c *fakeChannel) Member(account, conversation, user string) (bot.Membership, error) {
	roster, err := c.Members(account, conversation)
	if err != nil {
		return bot.Membership{}, err
	}
	for _, member := range roster.Users {
		if member.ID == user {
			return bot.Membership{User: member, Complete: roster.Complete, Source: roster.Source}, nil
		}
	}
	return bot.Membership{}, bot.ErrNotFound
}
