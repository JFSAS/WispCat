package host

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JFSAS/wechat-runtime/internal/instances"
	"github.com/JFSAS/wechat-runtime/internal/store"
)

func topologyFixture(t *testing.T) (*Host, Config) {
	t.Helper()
	cfg := testConfig(t)
	h, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close(context.Background()) })
	if err = h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return h, cfg
}

func topologySaved(t *testing.T, h *Host) topologyPlan {
	t.Helper()
	view := adminInbound(t, h, "GET", "/topology", "", 200)
	raw, _ := json.Marshal(view["saved"])
	var plan topologyPlan
	if json.Unmarshal(raw, &plan) != nil {
		t.Fatal(string(raw))
	}
	return plan
}

func topologySave(t *testing.T, h *Host, plan topologyPlan, code int) map[string]any {
	t.Helper()
	view := adminInbound(t, h, "GET", "/topology", "", 200)
	body, _ := json.Marshal(map[string]any{"revision": view["revision"], "plan": plan})
	return adminInbound(t, h, "POST", "/topology", string(body), code)
}

func topologyOwners(h *Host) []*accountRuntime {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*accountRuntime(nil), h.accounts...)
}

func TestTopologyAddRemoveIdleReopenPreservesOwnersAndIDs(t *testing.T) {
	h, cfg := topologyFixture(t)
	original := topologyOwners(h)
	first, second := original[0], original[1]
	firstEpoch, secondEpoch := first.channel.Snapshot().Epoch, second.channel.Snapshot().Epoch
	firstAddress, secondAddress := first.listener.Addr().String(), second.listener.Addr().String()
	id, err := h.state.ID("synthetic-first", "user", "permanent-person")
	if err != nil {
		t.Fatal(err)
	}
	profile, err := instances.Preview(cfg.StateDir, first.config.Instance)
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(profile.Directory, "retained-fixture")
	if err = store.Atomic(marker, []byte("private synthetic retained data")); err != nil {
		t.Fatal(err)
	}
	plan := topologySaved(t, h)
	plan.Accounts = append(plan.Accounts, Account{Name: "third", Instance: "instance:third", Mode: "offline", Listen: "127.0.0.1:0", TokenEnv: "TEST_BOT_TOKEN"})
	if topologySave(t, h, plan, 200)["applied"] != false || len(topologyOwners(h)) != 2 {
		t.Fatal("save changed active membership")
	}
	adminInbound(t, h, "POST", "/topology/apply", "{}", 200)
	owners := topologyOwners(h)
	if len(owners) != 3 || owners[0] != first || owners[1] != second || first.channel.Snapshot().Epoch != firstEpoch || second.channel.Snapshot().Epoch != secondEpoch {
		t.Fatal("add replaced a retained owner")
	}
	inboundHTTP(t, firstAddress, "synthetic-onebot-token", 200)
	inboundHTTP(t, secondAddress, "synthetic-onebot-token", 200)
	for _, a := range owners {
		raw, _ := json.Marshal(a.channel.Status())
		var status map[string]any
		_ = json.Unmarshal(raw, &status)
		for _, key := range []string{"auth_calls", "sync_calls", "send_calls"} {
			if status[key] != float64(0) {
				t.Fatal(key, status)
			}
		}
	}
	plan.Accounts = plan.Accounts[1:]
	topologySave(t, h, plan, 200)
	adminInbound(t, h, "POST", "/topology/apply", "{}", 200)
	select {
	case <-first.done:
	default:
		t.Fatal("removed owner still running")
	}
	if _, err = first.bot.HistoryStatus(); err == nil {
		t.Fatal("history owner retained after successful stop")
	}
	if _, err = first.adapter.Runtime.Streams.Snapshot(""); err == nil {
		t.Fatal("stream owner retained after successful stop")
	}
	conn, err := net.DialTimeout("tcp", firstAddress, time.Second)
	if err == nil {
		conn.Close()
		t.Fatal("removed listener retained")
	}
	if topologyOwners(h)[0] != second {
		t.Fatal("remove restarted sibling")
	}
	plan.Accounts = []Account{}
	topologySave(t, h, plan, 200)
	adminInbound(t, h, "POST", "/topology/apply", "{}", 200)
	if len(topologyOwners(h)) != 0 {
		t.Fatal("not management-only")
	}
	adminInbound(t, h, "GET", "/storage", "", 200)
	if other, err := New(cfg); err == nil {
		other.Close(context.Background())
		t.Fatal("idle host released root ownership")
	}
	plan.Accounts = []Account{cfg.Accounts[0]}
	topologySave(t, h, plan, 200)
	adminInbound(t, h, "POST", "/topology/apply", "{}", 200)
	if topologyOwners(h)[0] == first {
		t.Fatal("reused closed owner")
	}
	restored, err := h.state.ID("synthetic-first", "user", "permanent-person")
	if err != nil || restored != id {
		t.Fatal(restored, id, err)
	}
	if raw, err := store.ReadPrivate(marker, 128); err != nil || string(raw) != "private synthetic retained data" {
		t.Fatal("stop removed private data", err)
	}
	if err = h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	next, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close(context.Background())
	if len(next.accounts) != 1 || next.accounts[0].config.Name != "first" {
		t.Fatal("bootstrap resurrected removed accounts")
	}
	if err = next.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	restored, err = next.state.ID("synthetic-first", "user", "permanent-person")
	if err != nil || restored != id {
		t.Fatal(restored, id, err)
	}
}

func TestTopologyOccupiedNewPortKeepsActiveOwnersAndCanRetry(t *testing.T) {
	h, _ := topologyFixture(t)
	owners := topologyOwners(h)
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	plan := topologySaved(t, h)
	plan.Accounts = append(plan.Accounts, Account{Name: "added", Instance: "instance:added", Mode: "offline", Listen: blocker.Addr().String(), TokenEnv: "TEST_BOT_TOKEN"})
	topologySave(t, h, plan, 200)
	adminInbound(t, h, "POST", "/topology/apply", "{}", 409)
	for i, a := range topologyOwners(h) {
		if a != owners[i] {
			t.Fatal("preflight replaced existing owner")
		}
		inboundHTTP(t, a.listener.Addr().String(), "synthetic-onebot-token", 200)
	}
	view := adminInbound(t, h, "GET", "/topology", "", 200)
	if view["applied"] != false || view["error"] != "topology_bind_failed" {
		t.Fatal(view)
	}
	_ = blocker.Close()
	adminInbound(t, h, "POST", "/topology/apply", "{}", 200)
	if len(topologyOwners(h)) != 3 {
		t.Fatal("retry did not add account")
	}
}

func TestTopologyRetirementDeadlineFencesReenableAndRootRelease(t *testing.T) {
	h, cfg := topologyFixture(t)
	first := topologyOwners(h)[0]
	channel := &inboundChannel{entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(channel.release) })
	first.adapter.Runtime.Channel = channel
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		res, _ := (&http.Client{Timeout: 3 * time.Second}).Get(first.adapter.Runtime.MediaURL("synthetic", "friend", "blocked"))
		if res != nil {
			res.Body.Close()
		}
	}()
	select {
	case <-channel.entered:
	case <-time.After(time.Second):
		t.Fatal("request did not enter")
	}
	initial := topologySaved(t, h)
	plan := cloneTopology(initial)
	plan.Accounts = plan.Accounts[1:]
	topologySave(t, h, plan, 200)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err := h.topology.apply(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	view := adminInbound(t, h, "GET", "/topology", "", 200)
	if len(view["active"].([]any)) != 1 || len(view["retiring"].([]any)) != 1 || view["applied"] != false {
		t.Fatal(view)
	}
	if _, err = first.bot.HistoryStatus(); err != nil {
		t.Fatal("closed storage under a request", err)
	}
	topologySave(t, h, initial, 200)
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = h.topology.apply(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) || len(topologyOwners(h)) != 1 {
		t.Fatal("duplicate owner started before old request left", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = h.Close(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if other, err := New(cfg); err == nil {
		other.Close(context.Background())
		t.Fatal("root released under old account request")
	}
	release.Do(func() { close(channel.release) })
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("request did not exit")
	}
	if err = h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	next, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close(context.Background())
	if len(next.accounts) != 2 {
		t.Fatal("saved reenable not restored")
	}
}

type topologyBody struct {
	io.ReadCloser
	entered chan struct{}
	once    sync.Once
}

func (b *topologyBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.entered) })
	return b.ReadCloser.Read(p)
}

func TestTopologyManagementLeaseKeepsStorageUntilHandlerReturns(t *testing.T) {
	h, _ := topologyFixture(t)
	first := topologyOwners(h)[0]
	reader, writer := io.Pipe()
	defer writer.Close()
	body := &topologyBody{ReadCloser: reader, entered: make(chan struct{})}
	q := httptest.NewRequest("POST", "http://"+h.Address()+"/accounts/first/offline", body)
	q.Header.Set("Authorization", "Bearer synthetic-admin-token")
	w := httptest.NewRecorder()
	handlerDone := make(chan struct{})
	go func() { defer close(handlerDone); h.ServeHTTP(w, q) }()
	select {
	case <-body.entered:
	case <-time.After(time.Second):
		t.Fatal("body not read")
	}
	plan := topologySaved(t, h)
	plan.Accounts = plan.Accounts[1:]
	topologySave(t, h, plan, 200)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err := h.topology.apply(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if _, err = first.bot.HistoryStatus(); err != nil {
		t.Fatal("management lease lost storage", err)
	}
	_ = writer.Close()
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("handler did not exit")
	}
	if w.Code != 400 {
		t.Fatal(w.Code)
	}
	adminInbound(t, h, "POST", "/topology/apply", "{}", 200)
	if _, err = first.bot.HistoryStatus(); err == nil {
		t.Fatal("storage not released after lease")
	}
}

func TestTopologyCorruptOrMissingPrivatePlanNeverFallsBack(t *testing.T) {
	h, cfg := topologyFixture(t)
	plan := topologySaved(t, h)
	plan.Accounts = []Account{}
	topologySave(t, h, plan, 200)
	adminInbound(t, h, "POST", "/topology/apply", "{}", 200)
	if err := h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(cfg.StateDir, "topology.json")
	guard := filepath.Join(cfg.StateDir, "topology.required")
	for _, raw := range []string{`null`, `{}`, `{"version":1}`, `{"accounts":[]}`, `{"version":1,"accounts":null}`, `{"version":1,"accounts":[],"extra":true}`} {
		if err := store.Atomic(path, []byte(raw)); err != nil {
			t.Fatal(err)
		}
		if other, err := New(cfg); err == nil {
			other.Close(context.Background())
			t.Fatal("invalid plan reopened bootstrap", raw)
		}
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if other, err := New(cfg); err == nil {
		other.Close(context.Background())
		t.Fatal("missing required plan reopened bootstrap")
	}
	if err := store.Atomic(path, []byte(`{"version":1,"accounts":[]}`)); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(guard); err != nil {
		t.Fatal(err)
	}
	if other, err := New(cfg); err == nil {
		other.Close(context.Background())
		t.Fatal("missing guard accepted")
	}
	if err := store.Atomic(guard, []byte(topologyGuard)); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_BOT_TOKEN", "") // Removed bootstrap credentials are retired too.
	next, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close(context.Background())
	if len(next.accounts) != 0 {
		t.Fatal("repair lost empty membership")
	}
	if err = next.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	adminInbound(t, next, "GET", "/storage", "", 200)
}

func TestTopologySaveCanUndoUnappliedRemovalAndRejectsIdentityMutation(t *testing.T) {
	h, _ := topologyFixture(t)
	initial := topologySaved(t, h)
	view := adminInbound(t, h, "GET", "/topology", "", 200)
	revision := view["revision"]
	empty := topologyPlan{Version: 1, Accounts: []Account{}}
	topologySave(t, h, empty, 200)
	if topologySave(t, h, initial, 200)["applied"] != true {
		t.Fatal("restoring original plan changed live owners")
	}
	bad := cloneTopology(initial)
	bad.Accounts[0].TokenEnv = "TEST_ADMIN_TOKEN"
	topologySave(t, h, bad, 409)
	bad = cloneTopology(initial)
	bad.Accounts[0].Name = "renamed"
	topologySave(t, h, bad, 409)
	topologySave(t, h, empty, 200)
	body, _ := json.Marshal(map[string]any{"revision": revision, "plan": initial})
	adminInbound(t, h, "POST", "/topology", string(body), 409)
	for _, path := range []string{"/topology", "/topology/apply"} {
		q, _ := http.NewRequest("POST", "http://"+h.Address()+path, strings.NewReader("{}"))
		res, err := http.DefaultClient.Do(q)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != 401 {
			t.Fatal(path, res.StatusCode)
		}
	}
	adminInbound(t, h, "POST", "/topology", `{"revision":"x","plan":{"version":1,"accounts":null}}`, 409)
	adminInbound(t, h, "POST", "/topology/apply", `{"unexpected":true}`, 400)
}

func TestTopologyConcurrentQueriesDoNotLeaseRetiredOwners(t *testing.T) {
	h, _ := topologyFixture(t)
	initial := topologySaved(t, h)
	stop := make(chan struct{})
	var done sync.WaitGroup
	var stopOnce sync.Once
	failures := make(chan string, 4)
	for range 4 {
		done.Add(1)
		go func() {
			defer done.Done()
			client := &http.Client{Timeout: 2 * time.Second}
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, path := range []string{"/accounts", "/accounts/first/status", "/storage"} {
					q, _ := http.NewRequest("GET", "http://"+h.Address()+path, nil)
					q.Header.Set("Authorization", "Bearer synthetic-admin-token")
					res, err := client.Do(q)
					if err != nil {
						failures <- err.Error()
						return
					}
					raw, _ := io.ReadAll(res.Body)
					res.Body.Close()
					if res.StatusCode != 200 && !(path == "/accounts/first/status" && res.StatusCode == 404) {
						failures <- string(raw)
						return
					}
				}
			}
		}()
	}
	defer func() { stopOnce.Do(func() { close(stop) }); done.Wait() }()
	for range 4 {
		topologySave(t, h, topologyPlan{Version: 1, Accounts: []Account{}}, 200)
		adminInbound(t, h, "POST", "/topology/apply", "{}", 200)
		topologySave(t, h, initial, 200)
		adminInbound(t, h, "POST", "/topology/apply", "{}", 200)
	}
	stopOnce.Do(func() { close(stop) })
	done.Wait()
	select {
	case err := <-failures:
		t.Fatal(err)
	default:
	}
}
