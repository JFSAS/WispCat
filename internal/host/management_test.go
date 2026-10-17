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

	"github.com/JFSAS/wispcat/internal/store"
)

func managementJSON(t *testing.T, base, secret, method, path, body string, code int) map[string]any {
	t.Helper()
	q, _ := http.NewRequest(method, base+path, strings.NewReader(body))
	q.Header.Set("Authorization", "Bearer "+secret)
	q.Header.Set("Content-Type", "application/json")
	res, err := (&http.Client{Timeout: 3 * time.Second}).Do(q)
	if err != nil {
		t.Fatal(method, path, err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal("response truncated", err)
	}
	if res.StatusCode != code {
		t.Fatal(method, path, res.StatusCode, string(raw))
	}
	if code == 202 && res.ContentLength != int64(len(raw)) {
		t.Fatal("handover acknowledgement length", res.ContentLength, len(raw))
	}
	var result map[string]any
	if json.Unmarshal(raw, &result) != nil {
		if code >= 400 {
			return map[string]any{"error": strings.TrimSpace(string(raw))}
		}
		t.Fatal(string(raw))
	}
	return result
}
func saveManagement(t *testing.T, h *Host, secret string, plan managementPlan, code int) map[string]any {
	t.Helper()
	base := "http://" + h.Address()
	state := managementJSON(t, base, secret, "GET", "/management", "", 200)
	raw, _ := json.Marshal(map[string]any{"revision": state["revision"], "plan": plan})
	return managementJSON(t, base, secret, "POST", "/management", string(raw), code)
}
func awaitManagement(t *testing.T, address, secret, id string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	client := &http.Client{Timeout: 250 * time.Millisecond}
	for time.Now().Before(deadline) {
		q, _ := http.NewRequest("GET", "http://"+address+"/management/operations/"+id, nil)
		q.Header.Set("Authorization", "Bearer "+secret)
		if res, err := client.Do(q); err == nil {
			raw, _ := io.ReadAll(res.Body)
			res.Body.Close()
			var state map[string]any
			if res.StatusCode == 200 && json.Unmarshal(raw, &state) == nil {
				if state["state"] == "applied" {
					return state
				}
				if state["state"] == "failed" {
					t.Fatal(state)
				}
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("management handover did not finish")
	return nil
}
func applyManagement(t *testing.T, h *Host, oldSecret, newSecret string) (string, string) {
	t.Helper()
	result := managementJSON(t, "http://"+h.Address(), oldSecret, "POST", "/management/apply", "{}", 202)
	address := result["management_address"].(string)
	id := result["operation"].(map[string]any)["id"].(string)
	awaitManagement(t, address, newSecret, id)
	return address, id
}

func TestManagementAddressAndTokenHandoverPreservesAccountsAndReopens(t *testing.T) {
	h, cfg := topologyFixture(t)
	t.Setenv("NEXT_ADMIN_TOKEN", "synthetic-next-admin-token")
	owners := topologyOwners(h)
	epochs := []uint64{owners[0].channel.Snapshot().Epoch, owners[1].channel.Snapshot().Epoch}
	old := h.Address()
	id, err := h.state.ID("synthetic", "user", "permanent")
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	next := reservation.Addr().String()
	reservation.Close()
	plan := managementPlan{Version: 1, Listen: next, TokenEnv: "NEXT_ADMIN_TOKEN"}
	if saveManagement(t, h, "synthetic-admin-token", plan, 200)["applied"] != false || h.Address() != old {
		t.Fatal("save changed active endpoint")
	}
	managementJSON(t, "http://"+old, "synthetic-admin-token", "GET", "/storage", "", 200)
	address, _ := applyManagement(t, h, "synthetic-admin-token", "synthetic-next-admin-token")
	if address != next || h.Address() != next {
		t.Fatal(address, next, h.Address())
	}
	conn, err := net.DialTimeout("tcp", old, time.Second)
	if err == nil {
		conn.Close()
		t.Fatal("old management listener retained")
	}
	managementJSON(t, "http://"+next, "synthetic-admin-token", "GET", "/management", "", 401)
	for i, owner := range topologyOwners(h) {
		if owner != owners[i] || owner.channel.Snapshot().Epoch != epochs[i] {
			t.Fatal("management handover restarted account")
		}
		inboundHTTP(t, owner.listener.Addr().String(), "synthetic-onebot-token", 200)
	}
	got, err := h.state.ID("synthetic", "user", "permanent")
	if err != nil || got != id {
		t.Fatal(got, id, err)
	}
	if err = h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_ADMIN_TOKEN", "")
	reopened, err := New(cfg)
	if err != nil {
		t.Fatal("retired management bootstrap token was still required", err)
	}
	defer reopened.Close(context.Background())
	if err = reopened.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	managementJSON(t, "http://"+reopened.Address(), "synthetic-next-admin-token", "GET", "/accounts/first/status", "", 200)
}

func TestManagementTokenRotationsFlushCompleteAcknowledgementsAndBoundHistory(t *testing.T) {
	h, _ := topologyFixture(t)
	t.Setenv("NEXT_ADMIN_TOKEN", "synthetic-next-admin-token")
	address := h.Address()
	secret := "synthetic-admin-token"
	var ids []string
	for i := range 20 {
		name, next := "NEXT_ADMIN_TOKEN", "synthetic-next-admin-token"
		if i%2 == 1 {
			name, next = "TEST_ADMIN_TOKEN", "synthetic-admin-token"
		}
		saveManagement(t, h, secret, managementPlan{Version: 1, Listen: "127.0.0.1:0", TokenEnv: name}, 200)
		actual, id := applyManagement(t, h, secret, next)
		ids = append(ids, id)
		if actual != address {
			t.Fatal("token rotation moved ephemeral port", actual, address)
		}
		managementJSON(t, "http://"+address, secret, "GET", "/storage", "", 401)
		secret = next
	}
	for i, id := range ids {
		code := 200
		if i < len(ids)-16 {
			code = 404
		}
		managementJSON(t, "http://"+address, secret, "GET", "/management/operations/"+id, "", code)
	}
}

func TestManagementNewEnvironmentReferenceWithSameValuePublishesWithoutDisconnect(t *testing.T) {
	h, _ := topologyFixture(t)
	t.Setenv("ALIAS_ADMIN_TOKEN", "synthetic-admin-token")
	old := h.Address()
	saveManagement(t, h, "synthetic-admin-token", managementPlan{Version: 1, Listen: "127.0.0.1:0", TokenEnv: "ALIAS_ADMIN_TOKEN"}, 200)
	address, _ := applyManagement(t, h, "synthetic-admin-token", "synthetic-admin-token")
	state := managementJSON(t, "http://"+address, "synthetic-admin-token", "GET", "/management", "", 200)
	if address != old || state["applied"] != true || state["active"].(map[string]any)["plan"].(map[string]any)["token_env"] != "ALIAS_ADMIN_TOKEN" {
		t.Fatal(state)
	}
}

func TestManagementActiveConfigurationKeepsAccountEditorsAfterBootstrapRetirement(t *testing.T) {
	cfg := testConfig(t)
	cfg.Accounts = cfg.Accounts[:1]
	t.Setenv("NEXT_ADMIN_TOKEN", "synthetic-next-admin-token")
	t.Setenv("NEXT_BOT_TOKEN", "synthetic-next-bot-token")
	h, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	if err = h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	saveManagement(t, h, "synthetic-admin-token", managementPlan{Version: 1, Listen: "127.0.0.1:0", TokenEnv: "NEXT_ADMIN_TOKEN"}, 200)
	address, _ := applyManagement(t, h, "synthetic-admin-token", "synthetic-next-admin-token")
	base := "http://" + address
	t.Setenv("TEST_ADMIN_TOKEN", "")
	state := managementJSON(t, base, "synthetic-next-admin-token", "GET", "/accounts/first/listener", "", 200)
	raw, _ := json.Marshal(map[string]any{"revision": state["revision"], "plan": inboundPlan{Listen: "127.0.0.1:0", TokenEnv: "NEXT_BOT_TOKEN"}})
	managementJSON(t, base, "synthetic-next-admin-token", "POST", "/accounts/first/listener", string(raw), 200)
	managementJSON(t, base, "synthetic-next-admin-token", "POST", "/accounts/first/listener/apply", "{}", 202)
	t.Setenv("TEST_BOT_TOKEN", "")
	state = managementJSON(t, base, "synthetic-next-admin-token", "GET", "/accounts/first/connections", "", 200)
	raw, _ = json.Marshal(map[string]any{"revision": state["revision"], "plan": connectionPlan{Reverse: []Reverse{}, HTTPPost: []Post{}}})
	managementJSON(t, base, "synthetic-next-admin-token", "POST", "/accounts/first/connections", string(raw), 200)
	managementJSON(t, base, "synthetic-next-admin-token", "POST", "/accounts/first/connections/apply", "{}", 202)
	inboundHTTP(t, topologyOwners(h)[0].listener.Addr().String(), "synthetic-next-bot-token", 200)
}

type managementFlushFailure struct{ *httptest.ResponseRecorder }

func (w managementFlushFailure) FlushError() error { return errors.New("synthetic flush failure") }

func TestManagementBindAndFlushFailuresKeepOldEndpointAndReleaseReservation(t *testing.T) {
	h, _ := topologyFixture(t)
	t.Setenv("NEXT_ADMIN_TOKEN", "synthetic-next-admin-token")
	old := h.Address()
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	plan := managementPlan{Version: 1, Listen: blocker.Addr().String(), TokenEnv: "NEXT_ADMIN_TOKEN"}
	saveManagement(t, h, "synthetic-admin-token", plan, 200)
	managementJSON(t, "http://"+old, "synthetic-admin-token", "POST", "/management/apply", "{}", 409)
	managementJSON(t, "http://"+old, "synthetic-admin-token", "GET", "/storage", "", 200)
	blocker.Close()
	q := httptest.NewRequest("POST", "http://"+old+"/management/apply", strings.NewReader("{}"))
	q.Header.Set("Authorization", "Bearer synthetic-admin-token")
	w := managementFlushFailure{httptest.NewRecorder()}
	h.ServeHTTP(w, q)
	if h.Address() != old {
		t.Fatal("failed flush dispatched change")
	}
	view := managementJSON(t, "http://"+old, "synthetic-admin-token", "GET", "/management", "", 200)
	if view["operation"].(map[string]any)["state"] != "failed" {
		t.Fatal(view)
	}
	res, err := net.Listen("tcp", plan.Listen)
	if err != nil {
		t.Fatal("failed response leaked prepared socket", err)
	}
	res.Close()
	applyManagement(t, h, "synthetic-admin-token", "synthetic-next-admin-token")
}

func TestManagementRetirementRemainsObservableAndShutdownRetainsRoot(t *testing.T) {
	h, cfg := topologyFixture(t)
	t.Setenv("NEXT_ADMIN_TOKEN", "synthetic-next-admin-token")
	if err := h.management.manager.close(context.Background()); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	target := listenerTarget{publish: h.management.publish, serve: func(w http.ResponseWriter, r *http.Request, address, secret string) {
		if r.URL.Path == "/fixture/block" {
			close(entered)
			<-release
			return
		}
		h.ServeEndpoint(w, r, address, secret)
	}}
	h.management.mu.Lock()
	h.management.manager = newListenerServer(target, h.parent, listener, "127.0.0.1:0", "synthetic-admin-token")
	h.management.mu.Unlock()
	old := h.Address()
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		res, _ := (&http.Client{Timeout: 3 * time.Second}).Get("http://" + old + "/fixture/block")
		if res != nil {
			res.Body.Close()
		}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("blocking handler did not enter")
	}
	saveManagement(t, h, "synthetic-admin-token", managementPlan{Version: 1, Listen: "127.0.0.1:0", TokenEnv: "NEXT_ADMIN_TOKEN"}, 200)
	ack := managementJSON(t, "http://"+old, "synthetic-admin-token", "POST", "/management/apply", "{}", 202)
	address, id := ack["management_address"].(string), ack["operation"].(map[string]any)["id"].(string)
	deadline := time.Now().Add(2 * time.Second)
	for {
		state := managementJSON(t, "http://"+address, "synthetic-next-admin-token", "GET", "/management/operations/"+id, "", 200)
		if state["state"] == "retiring" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(state)
		}
		time.Sleep(5 * time.Millisecond)
	}
	managementJSON(t, "http://"+address, "synthetic-next-admin-token", "POST", "/management/apply", "{}", 409)
	if _, err = h.state.Stats(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = h.Close(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if other, err := New(cfg); err == nil {
		other.Close(context.Background())
		t.Fatal("root released under old management handler")
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("blocked request did not exit")
	}
	if err = h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestManagementPersistedCorruptionAndMissingRecordsDoNotRestoreBootstrap(t *testing.T) {
	h, cfg := topologyFixture(t)
	t.Setenv("NEXT_ADMIN_TOKEN", "synthetic-next-admin-token")
	plan := managementPlan{Version: 1, Listen: "127.0.0.1:0", TokenEnv: "NEXT_ADMIN_TOKEN"}
	saveManagement(t, h, "synthetic-admin-token", plan, 200)
	applyManagement(t, h, "synthetic-admin-token", "synthetic-next-admin-token")
	if err := h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	path, guard := filepath.Join(cfg.StateDir, "management.json"), filepath.Join(cfg.StateDir, "management.required")
	for _, raw := range []string{`null`, `{}`, `{"version":1}`, `{"listen":"127.0.0.1:0","token_env":"NEXT_ADMIN_TOKEN"}`, `{"version":1,"listen":"0.0.0.0:5099","token_env":"NEXT_ADMIN_TOKEN"}`} {
		if err := store.Atomic(path, []byte(raw)); err != nil {
			t.Fatal(err)
		}
		if other, err := New(cfg); err == nil {
			other.Close(context.Background())
			t.Fatal("invalid management record restored bootstrap", raw)
		}
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if other, err := New(cfg); err == nil {
		other.Close(context.Background())
		t.Fatal("missing required plan accepted")
	}
	raw, _ := json.Marshal(plan)
	if err := store.Atomic(path, raw); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(guard); err != nil {
		t.Fatal(err)
	}
	if other, err := New(cfg); err == nil {
		other.Close(context.Background())
		t.Fatal("missing required marker accepted")
	}
	if err := store.Atomic(guard, []byte(managementGuard)); err != nil {
		t.Fatal(err)
	}
	next, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	next.Close(context.Background())
}

func TestManagementRoutesRequireAuthenticationAndStrictBodies(t *testing.T) {
	h, _ := topologyFixture(t)
	base := "http://" + h.Address()
	for _, path := range []string{"/management", "/management/apply", "/management/operations/" + strings.Repeat("a", 32)} {
		managementJSON(t, base, "wrong-synthetic-token", "GET", path, "", 401)
	}
	for _, body := range []string{`null`, `[]`, `{"unexpected":true}`, `{} {}`} {
		managementJSON(t, base, "synthetic-admin-token", "POST", "/management/apply", body, 400)
	}
	managementJSON(t, base, "synthetic-admin-token", "POST", "/management", `{"revision":"x","plan":{"version":1,"listen":"127.0.0.1:0","token_env":"TEST_ADMIN_TOKEN","token":"never-persist"}}`, 400)
	state := managementJSON(t, base, "synthetic-admin-token", "GET", "/management", "", 200)
	raw, _ := json.Marshal(map[string]any{"revision": "stale", "plan": state["saved"]})
	managementJSON(t, base, "synthetic-admin-token", "POST", "/management", string(raw), 409)
	bytes, _ := json.Marshal(state)
	if strings.Contains(string(bytes), "synthetic-admin-token") {
		t.Fatal("management status exposed token")
	}
}
