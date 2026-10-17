package onebot

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JFSAS/wispcat/pkg/bot"
)

func TestCompleteRequestValidationPrecedesMediaFetch(t *testing.T) {
	r, c := fixture(t)
	var fetches atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) { fetches.Add(1); w.Write([]byte("synthetic image")) }))
	defer peer.Close()
	id, _ := r.State.ID("account-a", "user", "friend")
	for _, extra := range []map[string]any{{"message": []bot.Segment{{Type: "image", Data: map[string]string{"file": peer.URL}}, {Type: "reply", Data: map[string]string{"id": "1"}}}}, {"message": []bot.Segment{{Type: "image", Data: map[string]string{"file": peer.URL}}, {Type: "text", Data: map[string]string{"text": strings.Repeat("x", 4097)}}}}, {"message": []bot.Segment{{Type: "image", Data: map[string]string{"file": peer.URL}}}, "wechat_request_id": 42}, {"message": []bot.Segment{{Type: "image", Data: map[string]string{"file": peer.URL, "name": "../../file"}}}}} {
		extra["user_id"] = id
		raw, _ := json.Marshal(extra)
		if result := invoke(t, r, "send_private_msg", string(raw)); result.Retcode == 0 {
			t.Fatal("invalid request accepted")
		}
	}
	if fetches.Load() != 0 || len(c.sends) != 0 {
		t.Fatal("invalid request fetched media or queued content")
	}
}
func TestCQRenderingPreservesSegmentsAndLiteralText(t *testing.T) {
	segments := []bot.Segment{{Type: "text", Data: map[string]string{"text": "literal & [CQ:at,qq=7]"}}, {Type: "at", Data: map[string]string{"qq": "123"}}, {Type: "image", Data: map[string]string{"file": "1", "url": "http://localhost/image?a=1&b=2,c"}}}
	got, err := parseCQ(renderCQ(segments))
	if err != nil || !reflect.DeepEqual(got, segments) {
		t.Fatal("raw_message cannot reconstruct segments", got, err)
	}
}

type waitingMediaChannel struct {
	*fakeChannel
	entered, exited chan struct{}
}

func (c waitingMediaChannel) Download(ctx context.Context, account, conversation, file string) (bot.Content, error) {
	close(c.entered)
	<-ctx.Done()
	close(c.exited)
	return bot.Content{}, ctx.Err()
}
func TestServerShutdownCancelsAndAwaitsSignedMedia(t *testing.T) {
	r, c := fixture(t)
	media := waitingMediaChannel{fakeChannel: c, entered: make(chan struct{}), exited: make(chan struct{})}
	r.Channel = media
	s := NewServer(r, strings.Repeat("t", 32), "")
	peer := httptest.NewServer(s)
	defer peer.Close()
	s.Host = strings.TrimPrefix(peer.URL, "http://")
	done := make(chan struct{})
	go func() {
		defer close(done)
		res, err := http.Get(s.mediaURL("account-a", "friend", "source/regular"))
		if err == nil {
			res.Body.Close()
		}
	}()
	select {
	case <-media.entered:
	case <-time.After(time.Second):
		t.Fatal("download did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-media.exited:
	default:
		t.Fatal("adapter closed before media stopped")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("media handler survived shutdown")
	}
}
func TestHTTPPostPreservesTerminalCursorFailure(t *testing.T) {
	r, c := fixture(t)
	c.snap.FirstCursor, c.snap.Cursor = 1, 1
	if err := r.State.setCursor("account-a", "cursor-test", 0); err != nil {
		t.Fatal(err)
	}
	s := NewServer(r, strings.Repeat("t", 32), "")
	peer := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer peer.Close()
	err := s.RunPost(context.Background(), PostConfig{Name: "cursor-test", URL: peer.URL})
	if !errors.Is(err, bot.ErrCursor) {
		t.Fatal(err)
	}
	statuses := s.Statuses()
	if len(statuses) != 1 || statuses[0].State != "failed" || statuses[0].Error != "event_cursor_unavailable" {
		t.Fatal("terminal error lost", statuses)
	}
}
func TestOutgoingGroupGetMsgKeepsConversationKind(t *testing.T) {
	r, _ := fixture(t)
	id, _ := r.State.ID("account-a", "group", "room")
	sent := invoke(t, r, "send_group_msg", `{"group_id":`+jsonNumber(id)+`,"message":"hello"}`)
	if sent.Retcode != 0 {
		t.Fatal(sent)
	}
	mid := sent.Data.(map[string]any)["message_id"].(int64)
	message := invoke(t, r, "get_msg", `{"message_id":`+jsonNumber(mid)+`}`)
	if message.Retcode != 0 {
		t.Fatal(message)
	}
	v := message.Data.(map[string]any)
	if v["message_type"] != "group" || v["group_id"] != id {
		t.Fatal("outgoing group message became private", v)
	}
}

func TestGroupMembershipValidationPrecedesMediaFetch(t *testing.T) {
	r, c := fixture(t)
	var fetches atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fetches.Add(1); w.Write([]byte("synthetic")) }))
	defer peer.Close()
	group, _ := r.State.ID("account-a", "group", "room")
	stranger, _ := r.State.ID("account-a", "user", "not-observed-in-room")
	raw, _ := json.Marshal(map[string]any{"group_id": group, "message": []bot.Segment{{Type: "image", Data: map[string]string{"file": peer.URL}}, {Type: "at", Data: map[string]string{"qq": strconv.FormatInt(stranger, 10)}}}})
	got := invoke(t, r, "send_group_msg", string(raw))
	if got.Retcode == 0 || fetches.Load() != 0 || len(c.sends) != 0 {
		t.Fatal("invalid group mention fetched media or sent", got, fetches.Load())
	}
}
