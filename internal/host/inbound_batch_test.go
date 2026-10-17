package host

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JFSAS/wispcat/pkg/bot"
	"github.com/JFSAS/wispcat/pkg/client"
	"github.com/gorilla/websocket"
)

type batchChannel struct {
	inboundChannel
	account string
}

func (c *batchChannel) Snapshot() bot.Snapshot {
	return bot.Snapshot{Account: c.account, Self: bot.User{ID: "owner-" + c.account, Name: c.account}, Epoch: 1, State: "offline"}
}
func (c *batchChannel) Download(ctx context.Context, account, actor, file string) (bot.Content, error) {
	if account != c.account {
		return bot.Content{}, bot.ErrScope
	}
	if file == "blocked" {
		return c.inboundChannel.Download(ctx, account, actor, file)
	}
	return bot.Content{Data: []byte(c.account), MIME: "application/octet-stream"}, nil
}

func batchHost(t *testing.T) (*Host, Config) {
	t.Helper()
	cfg := testConfig(t)
	t.Setenv("TEST_SECOND_BOT_TOKEN", "synthetic-second-token")
	cfg.Accounts[1].TokenEnv = "TEST_SECOND_BOT_TOKEN"
	h, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close(context.Background()) })
	if err = h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, owner := range h.accounts {
		owner.adapter.Runtime.Channel = &batchChannel{account: owner.config.Name}
	}
	return h, cfg
}

func batchAddress(owner *accountRuntime) string {
	status, _ := owner.inbound.manager.snapshot("", "")
	return status.Address
}
func batchSave(t *testing.T, owner *accountRuntime, address string) {
	t.Helper()
	if err := owner.inbound.save(owner.inbound.revision, inboundPlan{Listen: address, TokenEnv: owner.inbound.saved.TokenEnv}); err != nil {
		t.Fatal(err)
	}
}
func batchRevision(h *Host) string { return h.listenerBatchView()["revision"].(string) }
func batchLogin(t *testing.T, address, secret string) int64 {
	t.Helper()
	c, err := client.New("http://"+address, secret)
	if err != nil {
		t.Fatal(err)
	}
	response, err := c.Call(context.Background(), "get_login_info", nil)
	if err != nil {
		t.Fatal(err)
	}
	var data struct {
		UserID int64 `json:"user_id"`
	}
	if json.Unmarshal(response.Data, &data) != nil || data.UserID <= 0 {
		t.Fatal(string(response.Data))
	}
	return data.UserID
}

func batchIdle(t *testing.T, address, secret string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_, err = fmt.Fprintf(conn, "GET /get_status HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\n\r\n", address, secret)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal(response.StatusCode)
	}
	return conn
}

func TestListenerBatchSwapsScopesAndRetirementKeepsSiblingSocket(t *testing.T) {
	h, cfg := batchHost(t)
	a, b := h.accounts[0], h.accounts[1]
	pa, pb := batchAddress(a), batchAddress(b)
	ida, idb := batchLogin(t, pa, "synthetic-onebot-token"), batchLogin(t, pb, "synthetic-second-token")
	if ida == idb {
		t.Fatal("account IDs collide")
	}
	nativeA, nativeB := a.channel.Snapshot(), b.channel.Snapshot()
	idle := batchIdle(t, pa, "synthetic-onebot-token")
	forward, _, err := websocket.DefaultDialer.Dial("ws://"+pa+"/?access_token=synthetic-onebot-token", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer forward.Close()
	readInboundSocket(t, forward)
	batchSave(t, a, pb)
	batchSave(t, b, pa)
	revision := batchRevision(h)
	request, _ := json.Marshal(map[string]string{"revision": revision})
	result := adminInbound(t, h, "POST", "/listeners/apply", string(request), 200)
	if result["applied"] != true || batchAddress(a) != pb || batchAddress(b) != pa {
		t.Fatal(result)
	}
	if batchLogin(t, pb, "synthetic-onebot-token") != ida || batchLogin(t, pa, "synthetic-second-token") != idb {
		t.Fatal("socket target did not follow its account")
	}
	inboundHTTP(t, pa, "synthetic-onebot-token", 403)
	inboundHTTP(t, pb, "synthetic-second-token", 403)
	_ = idle.SetReadDeadline(time.Now().Add(time.Second))
	if _, err = idle.Read(make([]byte, 1)); err == nil {
		t.Fatal("old authenticated idle connection survived")
	}
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		t.Fatal("idle connection remained open until test timeout")
	}
	_ = forward.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err = forward.ReadMessage(); err == nil {
		t.Fatal("old account WebSocket survived handoff")
	}
	for _, owner := range []*accountRuntime{a, b} {
		url := owner.adapter.Runtime.MediaURL(owner.config.Name, "friend", "image")
		response, e := http.Get(url)
		if e != nil {
			t.Fatal(e)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != 200 || string(body) != owner.config.Name {
			t.Fatal(response.StatusCode, string(body))
		}
	}
	if a.channel.Snapshot().Epoch != nativeA.Epoch || b.channel.Snapshot().Epoch != nativeB.Epoch {
		t.Fatal("protocol epoch changed")
	}
	plan := topologyPlan{Version: 1, Accounts: []Account{cfg.Accounts[1]}}
	if err = h.topology.save(h.topology.revision, plan); err != nil {
		t.Fatal(err)
	}
	if err = h.topology.apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	if batchLogin(t, pa, "synthetic-second-token") != idb {
		t.Fatal("retiring first account closed sibling's swapped socket")
	}
	if err = h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	next, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close(context.Background())
	if err = next.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	next.accounts[0].adapter.Runtime.Channel = &batchChannel{account: "second"}
	if batchAddress(next.accounts[0]) != pa || batchLogin(t, pa, "synthetic-second-token") != idb {
		t.Fatal("saved swap or ID lost on reopen")
	}
}

func TestListenerBatchPreflightAndRevisionFailuresDoNotCancelOldPeers(t *testing.T) {
	h, _ := batchHost(t)
	a, b := h.accounts[0], h.accounts[1]
	pa, pb := batchAddress(a), batchAddress(b)
	forward, _, err := websocket.DefaultDialer.Dial("ws://"+pa+"/?access_token=synthetic-onebot-token", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer forward.Close()
	readInboundSocket(t, forward)
	stale := batchRevision(h)
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	batchSave(t, a, blocker.Addr().String())
	if err = h.applySavedListeners(context.Background(), stale); err == nil || err.Error() != "listener_batch_revision_conflict" {
		t.Fatal(err)
	}
	if err = h.applySavedListeners(context.Background(), batchRevision(h)); err == nil || err.Error() != "listener_bind_failed" {
		t.Fatal(err)
	}
	batchSave(t, a, pb)
	host, port, _ := net.SplitHostPort(pb)
	batchSave(t, b, net.JoinHostPort(host, "0"+port))
	if err = h.applySavedListeners(context.Background(), batchRevision(h)); err == nil || err.Error() != "duplicate_listener" {
		t.Fatal(err)
	}
	if batchAddress(a) != pa || batchAddress(b) != pb {
		t.Fatal("preflight changed an active address")
	}
	if err = forward.WriteJSON(map[string]any{"action": "get_status", "echo": "old-peer"}); err != nil {
		t.Fatal(err)
	}
	if readInboundSocket(t, forward)["echo"] != "old-peer" {
		t.Fatal("old socket stopped on failed prepare")
	}
}

func TestListenerBatchTimeoutRollsBackAndRetainsRequestOwnership(t *testing.T) {
	h, cfg := batchHost(t)
	a, b := h.accounts[0], h.accounts[1]
	pa, pb := batchAddress(a), batchAddress(b)
	channel := &batchChannel{account: "first", inboundChannel: inboundChannel{entered: make(chan struct{}), release: make(chan struct{})}}
	a.adapter.Runtime.Channel = channel
	var release sync.Once
	defer release.Do(func() { close(channel.release) })
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		response, _ := http.Get(a.adapter.Runtime.MediaURL("first", "friend", "blocked"))
		if response != nil {
			response.Body.Close()
		}
	}()
	select {
	case <-channel.entered:
	case <-time.After(time.Second):
		t.Fatal("old request did not enter")
	}
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	newAddress := reservation.Addr().String()
	reservation.Close()
	batchSave(t, a, newAddress)
	batchSave(t, b, pa)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	err = h.applySavedListeners(ctx, batchRevision(h))
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if batchAddress(a) != pa || batchAddress(b) != pb {
		t.Fatal("failed join published new ownership")
	}
	status, _ := a.inbound.manager.snapshot("", "")
	if !status.Retiring {
		t.Fatal("live old request lost its retirement owner")
	}
	inboundHTTP(t, pa, "synthetic-onebot-token", 200)
	inboundHTTP(t, pb, "synthetic-second-token", 200)
	probe, err := net.Listen("tcp", newAddress)
	if err != nil {
		t.Fatal("prepared socket leaked", err)
	}
	probe.Close()
	ctx, cancel = context.WithTimeout(context.Background(), 25*time.Millisecond)
	err = h.Close(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if other, e := New(cfg); e == nil {
		other.Close(context.Background())
		t.Fatal("root released under old request")
	}
	release.Do(func() { close(channel.release) })
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("old request did not leave")
	}
	if err = h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestListenerBatchNoopAndEphemeralSocketClaim(t *testing.T) {
	h, _ := batchHost(t)
	a, b := h.accounts[0], h.accounts[1]
	pa, pb := batchAddress(a), batchAddress(b)
	forward, _, err := websocket.DefaultDialer.Dial("ws://"+pa+"/?access_token=synthetic-onebot-token", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer forward.Close()
	readInboundSocket(t, forward)
	if err = h.applySavedListeners(context.Background(), batchRevision(h)); err != nil {
		t.Fatal(err)
	}
	if batchAddress(a) != pa || batchAddress(b) != pb {
		t.Fatal("noop replaced ephemeral sockets")
	}
	if err = forward.WriteJSON(map[string]any{"action": "get_status", "echo": "noop"}); err != nil {
		t.Fatal(err)
	}
	if readInboundSocket(t, forward)["echo"] != "noop" {
		t.Fatal("noop closed connection")
	}
	batchSave(t, a, pb)
	if err = h.applySavedListeners(context.Background(), batchRevision(h)); err != nil {
		t.Fatal(err)
	}
	if batchAddress(a) != pb || batchAddress(b) == pb || batchAddress(b) == pa {
		t.Fatal("ephemeral socket was assigned twice")
	}
	inboundHTTP(t, pb, "synthetic-onebot-token", 200)
	inboundHTTP(t, batchAddress(b), "synthetic-second-token", 200)
}

func TestListenerBatchManagementRejectsUnauthorizedAndInvalidBodies(t *testing.T) {
	h, _ := batchHost(t)
	for _, tc := range []struct {
		method, path, body string
		code               int
	}{
		{"GET", "/listeners", "", 401}, {"POST", "/listeners/apply", "{}", 401},
	} {
		q := httptest.NewRequest(tc.method, "http://"+h.Address()+tc.path, strings.NewReader(tc.body))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, q)
		if w.Code != tc.code {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	for _, body := range []string{`{"revision":"x","extra":true}`, `{"revision":"x"} {}`, `{"revision":null}`} {
		adminInbound(t, h, "POST", "/listeners/apply", body, 400)
	}
	adminInbound(t, h, "POST", "/listeners/apply", `{"revision":"stale"}`, 409)
}

func TestListenerBatchRejectsParsedOldRequestAfterAccountHandoff(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	handlerDone := make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	var calls [2]atomic.Int32
	managers := []*inboundServer{}
	for i := 0; i < 2; i++ {
		index := i
		target := listenerTarget{serve: func(w http.ResponseWriter, r *http.Request, address, secret string) {
			if r.Header.Get("Authorization") != "Bearer "+secret {
				t.Error("fixture credential changed")
			}
			calls[index].Add(1)
			w.WriteHeader(200)
		}, publish: func(string, string) {}}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		binding := newInboundBinding(target, listener, newInboundEpoch(context.Background(), "synthetic-shared-token"))
		if i == 0 {
			inner := binding.http.Handler
			binding.http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(handlerDone)
				close(entered)
				<-release
				inner.ServeHTTP(w, r)
			})
		}
		manager := &inboundServer{target: target, parent: context.Background(), active: binding, listen: "127.0.0.1:0"}
		managers = append(managers, manager)
		t.Cleanup(func() { _ = manager.close(context.Background()) })
		binding.start()
	}
	pa, _ := managers[0].snapshot("", "")
	pb, _ := managers[1].snapshot("", "")
	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		request, _ := http.NewRequest("POST", "http://"+pa.Address+"/mutation", nil)
		request.Header.Set("Authorization", "Bearer synthetic-shared-token")
		response, _ := (&http.Client{Timeout: time.Second}).Do(request)
		if response != nil {
			response.Body.Close()
		}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("request did not reach parsed-entry barrier")
	}
	if err := applyListenerBatch(context.Background(), []batchListener{{managers[0], pb.Address, "synthetic-shared-token"}, {managers[1], pa.Address, "synthetic-shared-token"}}); err != nil {
		t.Fatal(err)
	}
	unblock.Do(func() { close(release) })
	select {
	case <-clientDone:
	case <-time.After(time.Second):
		t.Fatal("old client remained open")
	}
	// Server.Close does not join HTTP handlers. Join the exact late handler
	// before testing that neither target ran, including on a closed connection.
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("late handler did not leave")
	}
	if calls[0].Load() != 0 || calls[1].Load() != 0 {
		t.Fatal("parsed old request crossed its account epoch", calls[0].Load(), calls[1].Load())
	}
}

func TestListenerBatchRemovalJoinsManagementLeaseWithoutHoldingListenerGate(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(fmt.Sprintf("retry=%t", retry), func(t *testing.T) {
			h, _ := batchHost(t)
			removed := h.accounts[0]
			reader, writer := io.Pipe()
			defer writer.Close()
			body := &topologyBody{ReadCloser: reader, entered: make(chan struct{})}
			request := httptest.NewRequest("POST", "http://"+h.Address()+"/accounts/"+removed.config.Name+"/listener/apply", body)
			request.Header.Set("Authorization", "Bearer synthetic-admin-token")
			response := httptest.NewRecorder()
			handlerDone := make(chan struct{})
			go func() { defer close(handlerDone); h.ServeHTTP(response, request) }()
			select {
			case <-body.entered: // ServeHTTP has registered the account lease.
			case <-time.After(time.Second):
				t.Fatal("management request did not acquire its lease")
			}
			plan := topologySaved(t, h)
			plan.Accounts = plan.Accounts[1:]
			topologySave(t, h, plan, 200)
			if retry {
				ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
				err := h.topology.apply(ctx)
				cancel()
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			applied := make(chan error, 1)
			go func() { applied <- h.topology.apply(ctx) }()
			select {
			case <-removed.inbound.manager.parent.Done(): // Removal has stopped admission.
			case <-ctx.Done():
				t.Fatal("account was not retired")
			}
			if _, err := io.WriteString(writer, "{}"); err != nil {
				t.Fatal(err)
			}
			_ = writer.Close()
			select {
			case err := <-applied:
				if err != nil {
					t.Fatal("membership waited on its own listener gate", err)
				}
			case <-ctx.Done():
				t.Fatal("membership and admitted listener request deadlocked")
			}
			select {
			case <-handlerDone:
			case <-time.After(time.Second):
				t.Fatal("admitted management request did not leave")
			}
			if response.Code != 409 {
				t.Fatal(response.Code, response.Body.String())
			}
			if len(topologyOwners(h)) != 1 {
				t.Fatal("membership publication failed")
			}
		})
	}
}

func TestListenerBatchConnectionLimitAndCredentialRotationCloseIdleSockets(t *testing.T) {
	h, _ := batchHost(t)
	owner := h.accounts[0]
	binding := owner.inbound.manager.active
	connections := []net.Conn{}
	defer func() {
		for _, conn := range connections {
			_ = conn.Close()
		}
	}()
	for i := 0; i < maxInboundConnections; i++ {
		conn, err := net.DialTimeout("tcp", batchAddress(owner), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, conn)
	}
	until := time.Now().Add(3 * time.Second)
	for {
		binding.mu.Lock()
		count := len(binding.connections)
		binding.mu.Unlock()
		if count == maxInboundConnections {
			break
		}
		if time.Now().After(until) {
			t.Fatal("idle sockets were not admitted", count)
		}
		time.Sleep(time.Millisecond)
	}
	rejected, err := net.DialTimeout("tcp", batchAddress(owner), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer rejected.Close()
	_ = rejected.SetReadDeadline(time.Now().Add(3 * time.Second))
	if n, err := rejected.Read(make([]byte, 1)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatal("connection above limit was not closed", n, err)
	}
	if err = owner.inbound.manager.apply(context.Background(), "127.0.0.1:0", "synthetic-rotated-token"); err != nil {
		t.Fatal(err)
	}
	for _, conn := range connections {
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		if n, err := conn.Read(make([]byte, 1)); n != 0 || !errors.Is(err, io.EOF) {
			t.Fatal("idle connection survived rotation", n, err)
		}
	}
	inboundHTTP(t, batchAddress(owner), "synthetic-rotated-token", 200)
}
