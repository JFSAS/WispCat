package host

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestConnectionSettingsSeparateSavedAndActiveWithoutRestartingChannel(t *testing.T) {
	cfg := testConfig(t)
	h, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err = h.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	a := h.accounts[0]
	settings := a.connections
	epoch := a.channel.Snapshot().Epoch
	before := settings.view()
	revision := before["revision"].(string)
	plan := connectionPlan{Reverse: []Reverse{{Name: "new-bot", URL: "ws://127.0.0.1:12345/onebot/v11/ws", TokenEnv: "TEST_BOT_TOKEN"}}}
	if err = settings.save(revision, plan); err != nil {
		t.Fatal(err)
	}
	saved := settings.view()
	if saved["applied"] != false || len(settings.manager.Names()) != 0 || a.channel.Snapshot().Epoch != epoch {
		t.Fatal("save applied connections or restarted protocol")
	}
	if err = settings.save(revision, plan); err == nil {
		t.Fatal("stale config revision accepted")
	}
	if err = settings.apply(ctx); err != nil {
		t.Fatal(err)
	}
	if settings.view()["applied"] != true || len(settings.manager.Names()) != 1 || a.channel.Snapshot().Epoch != epoch {
		t.Fatal("apply lost config or restarted protocol")
	}
	reopened, err := openConnectionSettings(filepath.Dir(settings.path), cfg, a.config)
	if err != nil || len(reopened.saved.Reverse) != 1 {
		t.Fatal("saved connections not restored", err)
	}
}

func TestSavedConnectionPlanSupersedesObsoleteTokenEnvironment(t *testing.T) {
	cfg := testConfig(t)
	t.Setenv("OLD_CONNECTION_TOKEN", "synthetic-old-token")
	t.Setenv("NEW_CONNECTION_TOKEN", "synthetic-new-token")
	cfg.Accounts[0].Reverse = []Reverse{{Name: "bot", URL: "ws://127.0.0.1:12345/ws", TokenEnv: "OLD_CONNECTION_TOKEN"}}
	h, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	settings := h.accounts[0].connections
	plan := connectionPlan{Reverse: []Reverse{{Name: "bot", URL: "ws://127.0.0.1:12345/ws", TokenEnv: "NEW_CONNECTION_TOKEN"}}}
	if err = settings.save(settings.revision, plan); err != nil {
		t.Fatal(err)
	}
	if err = h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = os.Unsetenv("OLD_CONNECTION_TOKEN"); err != nil {
		t.Fatal(err)
	}
	restarted, err := New(cfg)
	if err != nil {
		t.Fatal("obsolete startup token blocked saved plan", err)
	}
	defer restarted.Close(context.Background())
	if restarted.accounts[0].connections.saved.Reverse[0].TokenEnv != "NEW_CONNECTION_TOKEN" || cfg.Accounts[0].Reverse[0].TokenEnv != "OLD_CONNECTION_TOKEN" {
		t.Fatal("saved plan did not override or mutated input config")
	}
}
