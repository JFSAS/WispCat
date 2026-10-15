package service

import (
	"context"
	"strings"
	"testing"

	"github.com/JFSAS/wechat-runtime/internal/protocol"
	"github.com/JFSAS/wechat-runtime/pkg/bot"
)

func TestBotCanonicalSenderAcrossPrivateAndTwoGroups(t *testing.T) {
	s := recoveryFixture(t)
	a := BotAdapter{Service: s}
	for _, m := range []protocol.Message{{Sender: []byte("friend"), Recipient: s.data.Session.Bootstrap.Username, Content: []byte("private"), Type: 1, Timestamp: 1700000000, ServerID: 1}, {Sender: []byte("one@chatroom"), Recipient: s.data.Session.Bootstrap.Username, Content: []byte("friend:\ngroup one"), Type: 1, Timestamp: 1700000001, ServerID: 2}, {Sender: []byte("two@chatroom"), Recipient: s.data.Session.Bootstrap.Username, Content: []byte("friend:\ngroup two"), Type: 1, Timestamp: 1700000002, ServerID: 3}} {
		s.ingestLocked(protocol.SyncResult{Messages: []protocol.Message{m}})
	}
	if err := s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	events, err := a.Events(s.data.Binding, 0, 100)
	if err != nil || len(events) != 3 {
		t.Fatal(events, err)
	}
	for _, v := range events {
		if v.Sender.ID != events[0].Sender.ID {
			t.Fatal("group-local identity escaped into robot interface")
		}
		raw := v.Sender.ID
		if strings.Contains(raw, "friend") {
			t.Fatal("raw username escaped")
		}
	}
	if events[1].Conversation.ID == events[2].Conversation.ID {
		t.Fatal("group identities collapsed")
	}
}
func TestBotSubmissionIdempotencyRestartAndMentions(t *testing.T) {
	s := recoveryFixture(t)
	s.data.NextClientID = 1
	room := []byte("one@chatroom")
	message := protocol.Message{Sender: room, Recipient: s.data.Session.Bootstrap.Username, Content: []byte("friend:\nhi"), Type: 1, Timestamp: 1700000000, ServerID: 1}
	s.ingestLocked(protocol.SyncResult{Messages: []protocol.Message{message}})
	if err := s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	a := BotAdapter{Service: s}
	snap := a.Snapshot()
	req := bot.Send{Account: snap.Account, Epoch: snap.Epoch, Conversation: s.sessionID(room), Key: "request", Text: "reply", Mentions: []string{s.sessionID([]byte("friend"))}}
	d, err := a.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	j := s.data.Jobs[d.ID]
	mentions, err := protocol.ParseMentions([]byte(j.Source))
	if err != nil || len(mentions) != 1 || mentions[0] != "friend" {
		t.Fatal("native mention protocol changed", j.Source, err)
	}
	if !strings.HasSuffix(j.Content, "reply") {
		t.Fatal(j.Content)
	}
	loaded, err := s.checkpoint.Load()
	if err != nil {
		t.Fatal(err)
	}
	restarted := New(s.directory, s.interval, s.source.App)
	restarted.data = loaded
	restarted.state, restarted.mode = "connected", "online"
	req.Epoch = restarted.modeEpoch
	again, err := (BotAdapter{Service: restarted}).Submit(context.Background(), req)
	if err != nil || again.ID != d.ID || len(restarted.data.Jobs) != 1 {
		t.Fatal("restart duplicated durable send", again, err)
	}
	req.Text = "changed"
	if _, err = (BotAdapter{Service: restarted}).Submit(context.Background(), req); err == nil {
		t.Fatal("conflicting duplicate key accepted")
	}
}
func TestBotRejectsCancelledAndStaleAccountRequests(t *testing.T) {
	s, conversation, _ := mediaFixture(t, mediaTestPNG(t), "image", "")
	a := BotAdapter{Service: s}
	snap := a.Snapshot()
	req := bot.Send{Account: snap.Account, Epoch: snap.Epoch + 1, Conversation: conversation, Key: "test", Text: "hello"}
	if _, err := a.Submit(context.Background(), req); err == nil {
		t.Fatal("stale epoch accepted")
	}
	req.Epoch = snap.Epoch
	req.Account = "other"
	if _, err := a.Submit(context.Background(), req); err == nil {
		t.Fatal("wrong account accepted")
	}
	req.Account = snap.Account
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := a.Submit(ctx, req); err == nil {
		t.Fatal("cancelled request queued")
	}
	if len(s.data.Jobs) != 0 {
		t.Fatal("invalid request changed the journal")
	}
}
