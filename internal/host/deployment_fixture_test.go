//go:build integrationfixtures

package host

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JFSAS/wechat-runtime/internal/instances"
	"github.com/JFSAS/wechat-runtime/internal/protocol"
	"github.com/JFSAS/wechat-runtime/internal/store"
	"github.com/JFSAS/wechat-runtime/pkg/client"
)

func deploymentConfig(t *testing.T) Config {
	t.Helper()
	t.Setenv("DEPLOYMENT_TEST_ADMIN", "synthetic-deployment-admin-token")
	t.Setenv("DEPLOYMENT_TEST_BOT", "synthetic-deployment-bot-token")
	return Config{StateDir: t.TempDir(), Listen: "127.0.0.1:0", TokenEnv: "DEPLOYMENT_TEST_ADMIN", Accounts: []Account{{Name: "synthetic-a", Instance: "instance:deployment-synthetic-a", Mode: "offline", Listen: "127.0.0.1:0", TokenEnv: "DEPLOYMENT_TEST_BOT"}}}
}

func TestDeploymentFixtureRejectsForeignProfileBeforeAnyRecoveryWrites(t *testing.T) {
	c := deploymentConfig(t)
	if err := store.Atomic(filepath.Join(c.StateDir, "synthetic-deployment.required"), []byte(DeploymentFixtureMarker)); err != nil {
		t.Fatal(err)
	}
	catalog, err := instances.Open(c.StateDir, c.Accounts[0].Instance)
	if err != nil {
		t.Fatal(err)
	}
	profile, err := catalog.Ensure(c.Accounts[0].Instance)
	if err != nil {
		t.Fatal(err)
	}
	b := protocol.Bootstrap{Username: []byte("foreign-synthetic-example"), UIN: 42, DeviceID: bytes.Repeat([]byte{8}, 16)}
	cp := store.Checkpoint{Directory: filepath.Join(profile.Directory, "checkpoints"), Binding: b.Binding()}
	data := &store.Data{Schema: 1, Binding: b.Binding(), APIKey: bytes.Repeat([]byte{1}, 32), Session: &protocol.Session{Bootstrap: b, Original: bytes.Repeat([]byte{2}, 16), ClientKey: bytes.Repeat([]byte{3}, 24), ServerKey: bytes.Repeat([]byte{4}, 24)}}
	if err := cp.Save(data); err != nil {
		t.Fatal(err)
	}
	snapshot := func() map[string][32]byte {
		result := map[string][32]byte{}
		err := filepath.WalkDir(c.StateDir, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				info, e := entry.Info()
				if e != nil {
					return e
				}
				result[path] = sha256.Sum256([]byte("directory:" + info.Mode().String()))
			} else {
				raw, e := os.ReadFile(path)
				if e != nil {
					return e
				}
				result[path] = sha256.Sum256(raw)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	before := snapshot()
	if h, err := NewDeploymentFixture(c); err == nil {
		_ = h.Close(context.Background())
		t.Fatal("Host fixture admitted foreign profile")
	}
	after := snapshot()
	if len(before) != len(after) {
		t.Fatal("refusal created recovery files")
	}
	for path, digest := range before {
		if after[path] != digest {
			t.Fatal("refusal modified preexisting profile", path)
		}
	}
}

func TestDeploymentFixtureRejectsNonprivateDummyDirectoryWithoutChmod(t *testing.T) {
	c := deploymentConfig(t)
	if err := store.Atomic(filepath.Join(c.StateDir, "synthetic-deployment.required"), []byte(DeploymentFixtureMarker)); err != nil {
		t.Fatal(err)
	}
	h, err := NewDeploymentFixture(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	profiles, err := (&instances.Catalog{Root: c.StateDir}).List()
	if err != nil || len(profiles) != 1 {
		t.Fatal(err)
	}
	cp, exists, err := store.Unique(filepath.Join(profiles[0].Directory, "checkpoints"))
	if err != nil || !exists {
		t.Fatal(err)
	}
	data, err := cp.Load()
	if err != nil {
		t.Fatal(err)
	}
	data.APIKey[0] ^= 1
	if err := cp.Save(data); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(cp.Directory, 0755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(cp.Directory, cp.Binding+".checkpoint")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if h, err := NewDeploymentFixture(c); err == nil {
		_ = h.Close(context.Background())
		t.Fatal("nonprivate dummy namespace accepted")
	}
	info, err := os.Stat(cp.Directory)
	if err != nil || info.Mode().Perm() != 0755 {
		t.Fatal("refusal changed directory permissions", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("refusal rewrote checkpoint", err)
	}
}

func TestDeploymentFixtureRequiresMarkedRootAndOfflineSyntheticAccount(t *testing.T) {
	c := deploymentConfig(t)
	if h, err := NewDeploymentFixture(c); err == nil {
		_ = h.Close(context.Background())
		t.Fatal("unmarked root admitted")
	}
	if _, err := os.Stat(filepath.Join(c.StateDir, "backend.lock")); !os.IsNotExist(err) {
		t.Fatal("refused constructor touched root ownership", err)
	}
	if err := store.Atomic(filepath.Join(c.StateDir, "synthetic-deployment.required"), []byte(DeploymentFixtureMarker)); err != nil {
		t.Fatal(err)
	}
	c.Accounts[0].Mode = "online"
	if h, err := NewDeploymentFixture(c); err == nil {
		_ = h.Close(context.Background())
		t.Fatal("online bootstrap admitted to synthetic driver")
	}
}

func TestDeploymentFixtureNativeQueueAndIDsSurviveActualOwnerReopen(t *testing.T) {
	c := deploymentConfig(t)
	if err := store.Atomic(filepath.Join(c.StateDir, "synthetic-deployment.required"), []byte(DeploymentFixtureMarker)); err != nil {
		t.Fatal(err)
	}
	h, err := NewDeploymentFixture(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close(context.Background()) })
	if err = h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = h.DeploymentInject("synthetic-a", "private", "native fixture ping", false); err != nil {
		t.Fatal(err)
	}
	api, err := client.New("http://"+h.accounts[0].listener.Addr().String(), "synthetic-deployment-bot-token")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	response, err := api.Call(ctx, "get_friend_list", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	var users []struct {
		ID   int64  `json:"user_id"`
		Name string `json:"nickname"`
	}
	if json.Unmarshal(response.Data, &users) != nil {
		t.Fatal(string(response.Data))
	}
	var id int64
	for _, user := range users {
		if user.Name == "Synthetic Friend synthetic-a" {
			id = user.ID
		}
	}
	if id == 0 {
		t.Fatal("actual service directory user absent")
	}
	response, err = api.Call(ctx, "send_private_msg", map[string]any{"user_id": id, "message": "native fixture reply", "request_id": "deployment-reopen"})
	if err != nil {
		t.Fatal(err)
	}
	var sent struct {
		ID int64 `json:"message_id"`
	}
	if json.Unmarshal(response.Data, &sent) != nil || sent.ID == 0 {
		t.Fatal(string(response.Data))
	}
	before, err := api.Call(ctx, "get_msg", map[string]any{"message_id": sent.ID})
	if err != nil {
		t.Fatal(err)
	}
	for _, counter := range []string{"auth_calls", "sync_calls", "send_calls", "contact_list_calls", "contact_brief_calls"} {
		if h.accounts[0].channel.Status()[counter] != uint64(0) {
			t.Fatal("real protocol counter changed", counter, h.accounts[0].channel.Status())
		}
	}
	if err = h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	next, err := NewDeploymentFixture(c)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close(context.Background())
	if err = next.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	api, err = client.New("http://"+next.accounts[0].listener.Addr().String(), "synthetic-deployment-bot-token")
	if err != nil {
		t.Fatal(err)
	}
	after, err := api.Call(ctx, "get_msg", map[string]any{"message_id": sent.ID})
	if err != nil || string(before.Data) != string(after.Data) {
		t.Fatal("durable reply changed on owner reopen", err, string(before.Data), string(after.Data))
	}
}
