package host

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/JFSAS/wispcat/internal/store"
)

func configurationFixture(t *testing.T, h *Host) (configurationPlan, string) {
	t.Helper()
	view := h.configurationView()
	raw, _ := json.Marshal(view["saved"])
	var plan configurationPlan
	if json.Unmarshal(raw, &plan) != nil || plan.Version != 1 {
		t.Fatal(view)
	}
	return plan, view["revision"].(string)
}

func configurationNewCredentials(t *testing.T, plan *configurationPlan) {
	t.Helper()
	t.Setenv("CONFIG_NEXT_ADMIN", "synthetic-configuration-admin")
	t.Setenv("CONFIG_NEXT_BOT", "synthetic-configuration-bot")
	plan.Management.TokenEnv = "CONFIG_NEXT_ADMIN"
	for i := range plan.Accounts {
		plan.Accounts[i].TokenEnv = "CONFIG_NEXT_BOT"
	}
}

func TestConfigurationSavesWholePlanWithoutApplyingAndReopens(t *testing.T) {
	h, cfg := batchHost(t)
	owners := topologyOwners(h)
	addresses := []string{batchAddress(owners[0]), batchAddress(owners[1])}
	admin := h.Address()
	id, err := h.state.ID("synthetic-configuration", "user", "permanent")
	if err != nil {
		t.Fatal(err)
	}
	plan, revision := configurationFixture(t, h)
	configurationNewCredentials(t, &plan)
	plan.Accounts[0].Listen, plan.Accounts[1].Listen = addresses[1], addresses[0]
	if err = h.saveConfiguration(revision, plan); err != nil {
		t.Fatal(err)
	}
	if h.Address() != admin || batchAddress(owners[0]) != addresses[0] || batchAddress(owners[1]) != addresses[1] || topologyOwners(h)[0] != owners[0] {
		t.Fatal("save activated desired state")
	}
	inboundHTTP(t, addresses[0], "synthetic-onebot-token", 200)
	inboundHTTP(t, addresses[1], "synthetic-second-token", 200)
	after, _ := configurationFixture(t, h)
	if !reflect.DeepEqual(after, plan) {
		t.Fatal("whole saved plan differs", after, plan)
	}
	for _, name := range []string{"configuration-transaction.json", "configuration.required"} {
		if _, err = os.Lstat(filepath.Join(cfg.StateDir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("completed journal retained", name, err)
		}
	}
	if err = h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_ADMIN_TOKEN", "")
	t.Setenv("TEST_BOT_TOKEN", "")
	t.Setenv("TEST_SECOND_BOT_TOKEN", "")
	next, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close(context.Background())
	if err = next.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if actual, e := next.state.ID("synthetic-configuration", "user", "permanent"); e != nil || actual != id {
		t.Fatal("identity changed", actual, e)
	}
	if batchAddress(next.accounts[0]) != addresses[1] || batchAddress(next.accounts[1]) != addresses[0] {
		t.Fatal("restart did not load complete listener swap")
	}
	inboundHTTP(t, addresses[1], "synthetic-configuration-bot", 200)
	inboundHTTP(t, addresses[0], "synthetic-configuration-bot", 200)
	managementJSON(t, "http://"+next.Address(), "synthetic-configuration-admin", "GET", "/configuration", "", 200)
}

func TestConfigurationRejectsInvalidAndStaleBeforeJournal(t *testing.T) {
	h, cfg := batchHost(t)
	original, revision := configurationFixture(t, h)
	for _, change := range []func(*configurationPlan){
		func(p *configurationPlan) { p.Accounts[0].TokenEnv = "UNAVAILABLE_CONFIG_TOKEN" },
		func(p *configurationPlan) {
			p.Accounts[0].Listen = "127.0.0.1:12345"
			p.Accounts[1].Listen = "127.0.0.1:012345"
		},
		func(p *configurationPlan) {
			p.Management.Listen = "127.0.0.1:12345"
			p.Accounts[0].Listen = "127.0.0.1:012345"
		},
		func(p *configurationPlan) { p.Accounts[0].Mode = "online" },
		func(p *configurationPlan) { p.Accounts[0].Instance = "instance:other" },
		func(p *configurationPlan) { p.Accounts = p.Accounts[:1]; p.Accounts[0].Name = "renamed" },
	} {
		raw, _ := json.Marshal(original)
		var plan configurationPlan
		_ = json.Unmarshal(raw, &plan)
		change(&plan)
		if err := h.saveConfiguration(revision, plan); err == nil {
			t.Fatal("invalid plan saved", plan)
		}
		current, actual := configurationFixture(t, h)
		if actual != revision || !reflect.DeepEqual(current, original) {
			t.Fatal("failed save changed desired state")
		}
		if _, err := os.Lstat(filepath.Join(cfg.StateDir, "configuration-transaction.json")); !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	if err := h.saveConfiguration("stale", original); err == nil || err.Error() != "configuration_revision_conflict" {
		t.Fatal(err)
	}
}

func TestConfigurationProjectionFailureBlocksEditsAndRollsForward(t *testing.T) {
	h, cfg := batchHost(t)
	owners := topologyOwners(h)
	old := []string{batchAddress(owners[0]), batchAddress(owners[1])}
	plan, revision := configurationFixture(t, h)
	configurationNewCredentials(t, &plan)
	plan.Accounts[0].Listen, plan.Accounts[1].Listen = old[1], old[0]
	obstruction := filepath.Join(cfg.StateDir, "topology.json")
	if err := os.Mkdir(obstruction, 0700); err != nil {
		t.Fatal(err)
	}
	if err := h.saveConfiguration(revision, plan); err == nil {
		t.Fatal("projection obstruction ignored")
	}
	if view := h.configurationView(); view["persistence"] != "recovery_pending" || view["revision"] != configRevision(plan) {
		t.Fatal(view)
	}
	checks := []func() error{
		func() error { return owners[0].inbound.save(owners[0].inbound.revision, owners[0].inbound.saved) },
		func() error { return owners[0].inbound.apply(context.Background()) },
		func() error {
			return owners[0].connections.save(owners[0].connections.revision, owners[0].connections.saved)
		},
		func() error { return owners[0].connections.apply(context.Background()) },
		func() error { return h.management.save(h.management.revision, h.management.saved) },
		func() error { _, err := h.management.begin(context.Background()); return err },
		func() error { return h.topology.save(h.topology.revision, h.topology.saved) },
		func() error { return h.topology.apply(context.Background()) },
		func() error { return h.applySavedListeners(context.Background(), batchRevision(h)) },
	}
	for _, check := range checks {
		if err := check(); err == nil || err.Error() != "configuration_recovery_pending" {
			t.Fatal("pending transaction allowed mutation", err)
		}
	}
	raw, _ := json.Marshal(cfg)
	path := filepath.Join(t.TempDir(), "bootstrap.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := ReadConfig(path)
	if err != nil || !reflect.DeepEqual(loaded.Accounts, plan.Accounts) || loaded.TokenEnv != plan.Management.TokenEnv {
		t.Fatal("read observed partial files", loaded, err)
	}
	if other, err := New(cfg); err == nil {
		other.Close(context.Background())
		t.Fatal("other Host repaired an owned root")
	}
	if info, err := os.Stat(obstruction); err != nil || !info.IsDir() {
		t.Fatal("read-only load repaired files", err)
	}
	if err = os.Remove(obstruction); err != nil {
		t.Fatal(err)
	}
	if err = h.resumeConfiguration(); err != nil {
		t.Fatal(err)
	}
	if view := h.configurationView(); view["persistence"] != "ready" || view["revision"] != configRevision(plan) {
		t.Fatal(view)
	}
	if topologyOwners(h)[0] != owners[0] || batchAddress(owners[0]) != old[0] {
		t.Fatal("recovery activated configuration")
	}
	if err = h.applySavedListeners(context.Background(), batchRevision(h)); err != nil {
		t.Fatal(err)
	}
	if batchAddress(owners[0]) != old[1] || batchAddress(owners[1]) != old[0] {
		t.Fatal("committed listener plans were not usable")
	}
}

func TestConfigurationProcessKillRecoversCommittedPartialProjection(t *testing.T) {
	if root := os.Getenv("WECHAT_CONFIGURATION_CHILD_ROOT"); root != "" {
		cfg := Config{StateDir: root, Listen: "127.0.0.1:0", TokenEnv: "CONFIG_CHILD_OLD_ADMIN", Accounts: []Account{{Name: "first", Instance: "instance:child-first", Mode: "offline", Listen: "127.0.0.1:0", TokenEnv: "CONFIG_CHILD_OLD_BOT"}, {Name: "second", Instance: "instance:child-second", Mode: "offline", Listen: "127.0.0.1:0", TokenEnv: "CONFIG_CHILD_OLD_BOT"}}}
		h, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err = h.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		id, err := h.state.ID("configuration-child", "user", "durable-person")
		if err != nil {
			t.Fatal(err)
		}
		plan, revision := configurationFixture(t, h)
		plan.Management.TokenEnv = "CONFIG_CHILD_NEW_ADMIN"
		for i := range plan.Accounts {
			plan.Accounts[i].TokenEnv = "CONFIG_CHILD_NEW_BOT"
		}
		plan.Accounts[0].HTTPPost = []Post{{Name: "local-post", URL: os.Getenv("CONFIG_CHILD_POST_URL"), SecretEnv: "CONFIG_CHILD_NEW_BOT"}}
		if err = os.Mkdir(filepath.Join(root, "topology.json"), 0700); err != nil {
			t.Fatal(err)
		}
		if err = h.saveConfiguration(revision, plan); err == nil {
			t.Fatal("fixture did not stop after partial projection")
		}
		if !h.configurationPending {
			t.Fatal("fixture did not retain committed transaction")
		}
		raw, _ := json.Marshal(map[string]any{"id": id, "revision": configRevision(plan), "plan": plan})
		fmt.Println("CONFIGURATION_COMMITTED=" + string(raw))
		select {}
	}
	root := t.TempDir()
	post := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.Copy(io.Discard, r.Body); w.WriteHeader(200) }))
	defer post.Close()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, "-test.run=^TestConfigurationProcessKillRecoversCommittedPartialProjection$")
	command.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + t.TempDir(), "WECHAT_CONFIGURATION_CHILD_ROOT=" + root, "CONFIG_CHILD_OLD_ADMIN=synthetic-child-old-admin", "CONFIG_CHILD_OLD_BOT=synthetic-child-old-bot", "CONFIG_CHILD_NEW_ADMIN=synthetic-child-new-admin", "CONFIG_CHILD_NEW_BOT=synthetic-child-new-bot", "CONFIG_CHILD_POST_URL=" + post.URL}
	for _, name := range []string{"LD_LIBRARY_PATH", "DYLD_LIBRARY_PATH"} {
		if value := os.Getenv(name); value != "" {
			command.Env = append(command.Env, name+"="+value)
		}
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	command.Stderr = command.Stdout
	if err = command.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 4096), maxConfigurationJournalBytes)
	var committed struct {
		ID       int64             `json:"id"`
		Revision string            `json:"revision"`
		Plan     configurationPlan `json:"plan"`
	}
	for scanner.Scan() {
		if line := scanner.Text(); strings.HasPrefix(line, "CONFIGURATION_COMMITTED=") {
			if json.Unmarshal([]byte(strings.TrimPrefix(line, "CONFIGURATION_COMMITTED=")), &committed) != nil {
				t.Fatal(line)
			}
			break
		}
	}
	if committed.ID == 0 {
		t.Fatal("child never committed a complete transaction", scanner.Err())
	}
	if err = command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	err = command.Wait()
	waited = true
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		t.Fatal("child was not killed", err)
	}
	if err = os.Remove(filepath.Join(root, "topology.json")); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CONFIG_CHILD_NEW_ADMIN", "synthetic-child-new-admin")
	t.Setenv("CONFIG_CHILD_NEW_BOT", "synthetic-child-new-bot")
	cfg := Config{StateDir: root, Listen: "127.0.0.1:0", TokenEnv: "RETIRED_CHILD_ADMIN", Accounts: []Account{}}
	h, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	if actual, e := h.state.ID("configuration-child", "user", "durable-person"); e != nil || actual != committed.ID {
		t.Fatal("root reopen lost public identity", actual, e)
	}
	actual, revision := configurationFixture(t, h)
	if !reflect.DeepEqual(actual, committed.Plan) || revision != committed.Revision {
		t.Fatal("reopen mixed transaction generations", actual, committed.Plan)
	}
	if err = h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, owner := range h.accounts {
		inboundHTTP(t, batchAddress(owner), "synthetic-child-new-bot", 200)
	}
	if _, err = os.Stat(filepath.Join(root, "configuration-transaction.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("recovery journal retained", err)
	}
}

func TestConfigurationLateLegacyEditorRejectsReplacedRevision(t *testing.T) {
	h, _ := batchHost(t)
	owner := h.accounts[0]
	oldRevision := owner.inbound.revision
	reader, writer := io.Pipe()
	defer writer.Close()
	body := &topologyBody{ReadCloser: reader, entered: make(chan struct{})}
	request := httptest.NewRequest("POST", "http://"+h.Address()+"/accounts/"+owner.config.Name+"/listener", body)
	request.Header.Set("Authorization", "Bearer synthetic-admin-token")
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { defer close(done); h.ServeHTTP(response, request) }()
	select {
	case <-body.entered:
	case <-time.After(time.Second):
		t.Fatal("legacy request did not acquire lease")
	}
	plan, revision := configurationFixture(t, h)
	configurationNewCredentials(t, &plan)
	if err := h.saveConfiguration(revision, plan); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]any{"revision": oldRevision, "plan": inboundPlan{Listen: "127.0.0.1:0", TokenEnv: "TEST_BOT_TOKEN"}})
	if _, err := writer.Write(raw); err != nil {
		t.Fatal(err)
	}
	_ = writer.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("legacy editor deadlocked")
	}
	if response.Code != 409 {
		t.Fatal(response.Code, response.Body.String())
	}
	after, _ := configurationFixture(t, h)
	if !reflect.DeepEqual(after, plan) {
		t.Fatal("late editor overwrote full transaction")
	}
}

func TestConfigurationJournalCorruptionAndMissingRequiredDataFailClosed(t *testing.T) {
	for _, kind := range []string{"missing", "malformed", "unknown", "permission", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			cfg := testConfig(t)
			if err := store.Atomic(filepath.Join(cfg.StateDir, "configuration.required"), []byte(configurationGuard)); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(cfg.StateDir, "configuration-transaction.json")
			switch kind {
			case "malformed":
				if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			case "unknown":
				if err := os.WriteFile(path, []byte(`{"version":1,"extra":true}`), 0600); err != nil {
					t.Fatal(err)
				}
			case "permission":
				if err := os.WriteFile(path, []byte(`{}`), 0644); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(filepath.Join(t.TempDir(), "foreign"), path); err != nil {
					t.Fatal(err)
				}
			}
			if h, err := New(cfg); err == nil {
				h.Close(context.Background())
				t.Fatal("invalid transaction fell back to bootstrap")
			}
			if _, err := os.Stat(filepath.Join(cfg.StateDir, "onebot")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("invalid transaction opened protocol state", err)
			}
		})
	}
	t.Run("journal-before-marker", func(t *testing.T) {
		cfg := testConfig(t)
		plan := configurationPlan{Version: 1, Management: managementPlan{Version: 1, Listen: cfg.Listen, TokenEnv: cfg.TokenEnv}, Accounts: cfg.Accounts}
		journal := configurationJournal{Version: 1, Plan: plan, Topology: topologyPlan{Version: 1, Accounts: cfg.Accounts}}
		raw, _ := json.Marshal(journal)
		if err := store.Atomic(filepath.Join(cfg.StateDir, "configuration-transaction.json"), raw); err != nil {
			t.Fatal(err)
		}
		h, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer h.Close(context.Background())
		if _, err = os.Lstat(filepath.Join(cfg.StateDir, "configuration-transaction.json")); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("early committed journal was not recovered", err)
		}
	})
}

func TestConfigurationCallerSlicesAndRetiredCredentialRepair(t *testing.T) {
	h, _ := batchHost(t)
	plan, revision := configurationFixture(t, h)
	t.Setenv("TEST_BOT_TOKEN", "")
	// Saved plan/revision remains readable so its environment reference can be repaired.
	_, repairRevision := configurationFixture(t, h)
	if repairRevision != revision {
		t.Fatal("environment values changed the public revision")
	}
	configurationNewCredentials(t, &plan)
	plan.Accounts[0].Reverse = []Reverse{{Name: "original", URL: "ws://127.0.0.1:1", TokenEnv: "CONFIG_NEXT_BOT", Role: "Universal"}}
	if err := h.saveConfiguration(revision, plan); err != nil {
		t.Fatal(err)
	}
	plan.Accounts[0].Reverse[0].Name = "caller-mutated"
	saved := h.accounts[0].connections.view()["saved"].(connectionPlan)
	if saved.Reverse[0].Name != "original" || h.accounts[0].connections.revision != planRevision(saved) {
		t.Fatal("caller retained mutable saved state", saved)
	}
}

func TestConfigurationManagementAuthenticationAndStrictBodies(t *testing.T) {
	h, _ := batchHost(t)
	for _, path := range []string{"/configuration", "/configuration/recover"} {
		request := httptest.NewRequest("POST", "http://"+h.Address()+path, strings.NewReader(`{}`))
		response := httptest.NewRecorder()
		h.ServeHTTP(response, request)
		if response.Code != 401 {
			t.Fatal(path, response.Code)
		}
	}
	for _, body := range []string{`{}`, `null`, `{"revision":null,"plan":{}}`, `{"revision":"x","plan":null}`, `{"revision":"x","plan":{},"unknown":true}`, `{"revision":"x","plan":{}} {}`} {
		adminInbound(t, h, "POST", "/configuration", body, 400)
	}
	for _, body := range []string{`null`, `[]`, `{"unknown":true}`, `{} {}`} {
		adminInbound(t, h, "POST", "/configuration/recover", body, 400)
	}
	adminInbound(t, h, "POST", "/configuration/recover", `{}`, 200)
}

func TestConfigurationFinalUnlinkSyncFailureKeepsRecoveryFence(t *testing.T) {
	h, cfg := batchHost(t)
	plan, revision := configurationFixture(t, h)
	configurationNewCredentials(t, &plan)
	calls := 0
	injected := errors.New("synthetic_directory_sync_failure")
	h.configurationSync = func(root string) error {
		calls++
		if calls == 2 || calls == 3 {
			return injected
		}
		return syncConfigurationDirectory(root)
	}
	if err := h.saveConfiguration(revision, plan); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	if calls != 2 || !h.configurationPending {
		t.Fatal("final sync fault was not retained", calls)
	}
	if _, err := os.Lstat(filepath.Join(cfg.StateDir, "configuration-transaction.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if err := h.resumeConfiguration(); !errors.Is(err, injected) || !h.configurationPending {
		t.Fatal("missing journal cleared an unsynchronized deletion", err)
	}
	owner := h.accounts[0]
	if err := owner.inbound.save(owner.inbound.revision, owner.inbound.saved); err == nil || err.Error() != "configuration_recovery_pending" {
		t.Fatal(err)
	}
	if err := h.resumeConfiguration(); err != nil {
		t.Fatal(err)
	}
	if calls != 4 || h.configurationPending {
		t.Fatal("final directory sync not confirmed", calls)
	}
	if err := owner.inbound.save(owner.inbound.revision, inboundPlan{Listen: "127.0.0.1:0", TokenEnv: "TEST_BOT_TOKEN"}); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close(context.Background())
	if reopened.accounts[0].inbound.saved.TokenEnv != "TEST_BOT_TOKEN" {
		t.Fatal("recovery replayed an older transaction over the later edit")
	}
}

func TestConfigurationRetiringInstanceCannotBeReusedUnderOldEditorLease(t *testing.T) {
	h, _ := batchHost(t)
	removed := h.accounts[0]
	reader, writer := io.Pipe()
	defer writer.Close()
	body := &topologyBody{ReadCloser: reader, entered: make(chan struct{})}
	request := httptest.NewRequest("POST", "http://"+h.Address()+"/accounts/"+removed.config.Name+"/listener", body)
	request.Header.Set("Authorization", "Bearer synthetic-admin-token")
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { defer close(done); h.ServeHTTP(response, request) }()
	select {
	case <-body.entered:
	case <-time.After(time.Second):
		t.Fatal("old editor did not acquire lease")
	}
	topology := topologySaved(t, h)
	topology.Accounts = topology.Accounts[1:]
	topologySave(t, h, topology, 200)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	err := h.topology.apply(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	plan, revision := configurationFixture(t, h)
	desired := removed.config
	desired.Name = "renamed-after-stop"
	plan.Accounts = append(plan.Accounts, desired)
	if err = h.saveConfiguration(revision, plan); err == nil || err.Error() != "account_retirement_pending" {
		t.Fatal("retiring instance reused", err)
	}
	raw, _ := json.Marshal(map[string]any{"revision": removed.inbound.revision, "plan": removed.inbound.saved})
	if _, err = writer.Write(raw); err != nil {
		t.Fatal(err)
	}
	_ = writer.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("old editor did not finish")
	}
	if response.Code != 200 {
		t.Fatal(response.Code, response.Body.String())
	}
	select {
	case <-removed.done:
	case <-time.After(time.Second):
		t.Fatal("old owner still owns instance")
	}
	if err = h.saveConfiguration(revision, plan); err != nil {
		t.Fatal("stopped instance could not be configured", err)
	}
	actual, _ := configurationFixture(t, h)
	if !reflect.DeepEqual(actual, plan) {
		t.Fatal("old editor overwrote renamed configuration")
	}
}

func TestConfigurationRejectsOversizedIndividualProjectionsBeforeCommit(t *testing.T) {
	for _, kind := range []string{"management", "listener"} {
		t.Run(kind, func(t *testing.T) {
			h, cfg := batchHost(t)
			plan, revision := configurationFixture(t, h)
			name := "CONFIG_SYNTHETIC_" + strings.Repeat("X", 9000)
			t.Setenv(name, "synthetic-long-name-token")
			category := "listener_config_too_large"
			if kind == "management" {
				plan.Management.TokenEnv = name
				category = "management_config_too_large"
			} else {
				plan.Accounts[0].TokenEnv = name
			}
			raw, _ := json.Marshal(map[string]any{"revision": revision, "plan": plan})
			view := adminInbound(t, h, "POST", "/configuration", string(raw), 409)
			if view["category"] != category {
				t.Fatal(view)
			}
			_, after := configurationFixture(t, h)
			if after != revision {
				t.Fatal("oversized projection changed saved state")
			}
			if _, err := os.Lstat(filepath.Join(cfg.StateDir, "configuration-transaction.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("oversized projection committed journal", err)
			}
		})
	}
}

func TestConfigurationJournalWriteUncertaintyKeepsCommittedDesiredState(t *testing.T) {
	for _, published := range []bool{false, true} {
		t.Run(fmt.Sprintf("published=%t", published), func(t *testing.T) {
			h, _ := batchHost(t)
			before, revision := configurationFixture(t, h)
			raw, _ := json.Marshal(before)
			var desired configurationPlan
			_ = json.Unmarshal(raw, &desired)
			configurationNewCredentials(t, &desired)
			injected := errors.New("synthetic_journal_write_failure")
			h.configurationWrite = func(path string, body []byte) error {
				if published {
					if err := store.Atomic(path, body); err != nil {
						return err
					}
				}
				return injected
			}
			if err := h.saveConfiguration(revision, desired); !errors.Is(err, injected) {
				t.Fatal(err)
			}
			actual, _ := configurationFixture(t, h)
			if published {
				if !h.configurationPending || h.configurationError != "configuration_commit_uncertain" || !reflect.DeepEqual(actual, desired) {
					t.Fatal("published journal was treated as rolled back", actual)
				}
				if err := h.resumeConfiguration(); err != nil {
					t.Fatal(err)
				}
			} else if h.configurationPending || !reflect.DeepEqual(actual, before) {
				t.Fatal("failed precommit changed saved state", actual)
			}
		})
	}
}
