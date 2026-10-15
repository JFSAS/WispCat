package instances

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JFSAS/wechat-runtime/internal/protocol"
	"github.com/JFSAS/wechat-runtime/internal/store"
)

func legacyFixture(t *testing.T, root, app string) *store.Data {
	t.Helper()
	b := protocol.Bootstrap{UIN: 1, Username: []byte("synthetic-owner"), DeviceID: []byte("synthetic-device")}
	d := &store.Data{Schema: 1, SourceApp: app, Binding: b.Binding(), APIKey: bytes.Repeat([]byte{1}, 32), Session: &protocol.Session{Bootstrap: b, Original: bytes.Repeat([]byte{2}, 16), ClientKey: bytes.Repeat([]byte{3}, 24), ServerKey: bytes.Repeat([]byte{4}, 24)}}
	if err := (store.Checkpoint{Directory: filepath.Join(root, "checkpoints"), Binding: d.Binding}).Save(d); err != nil {
		t.Fatal(err)
	}
	return d
}
func TestMigrationKeepsEncryptedLegacyDataAndCreatesIndependentProfiles(t *testing.T) {
	root := t.TempDir()
	appA, appB := "/Applications/WeChat.app", "/Applications/微信2.app"
	data := legacyFixture(t, root, appA)
	id := "11111111111111111111111111111111"
	cache := store.MediaCache{Directory: filepath.Join(root, "media"), Binding: data.Binding}
	if err := cache.Save(id, []byte("synthetic media")); err != nil {
		t.Fatal(err)
	}
	if err := store.Atomic(filepath.Join(root, "login-device.id"), []byte("existing-device")); err != nil {
		t.Fatal(err)
	}
	if err := store.Atomic(filepath.Join(root, "codex-workspace", "context.txt"), []byte("legacy workspace")); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(filepath.Join(root, "checkpoints", data.Binding+".checkpoint"))
	if err != nil {
		t.Fatal(err)
	}
	c, err := Open(root, appB)
	if err != nil {
		t.Fatal(err)
	}
	a, err := c.Ensure(appA)
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Ensure(appB)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Trim(b.ID, "abcdefghijklmnopqrstuvwxyz0123456789-") != "" {
		t.Fatal("instance ID cannot be used in browser request headers")
	}
	if a.Directory == b.Directory {
		t.Fatal("instances share state")
	}
	cp, exists, err := store.Unique(filepath.Join(a.Directory, "checkpoints"))
	if err != nil || !exists {
		t.Fatal("old checkpoint missing", err)
	}
	loaded, err := cp.Load()
	if err != nil || loaded.Binding != data.Binding {
		t.Fatal("migration cannot decrypt", err)
	}
	media, err := (store.MediaCache{Directory: filepath.Join(a.Directory, "media"), Binding: data.Binding}).Load(id)
	if err != nil || string(media) != "synthetic media" {
		t.Fatal("migration lost media", err)
	}
	if _, err = os.Stat(filepath.Join(b.Directory, "checkpoints")); !os.IsNotExist(err) {
		t.Fatal("new profile inherited old account")
	}
	unchanged, _ := os.ReadFile(filepath.Join(root, "checkpoints", data.Binding+".checkpoint"))
	if !bytes.Equal(original, unchanged) {
		t.Fatal("legacy checkpoint mutated")
	}
	if err = c.Activate(b); err != nil {
		t.Fatal(err)
	}
	if selected, err := LastApp(root, appA); err != nil || selected != appB {
		t.Fatal("selection not restored", err)
	}
	// The old root is now a backup; subsequent startup must not re-import it.
	if err = os.WriteFile(filepath.Join(root, "checkpoints", data.Binding+".checkpoint"), []byte("old backup no longer usable"), 0600); err != nil {
		t.Fatal(err)
	}
	again, err := Open(root, appB)
	if err != nil {
		t.Fatal("read obsolete backup", err)
	}
	restored, err := again.Ensure(appA)
	if err != nil || restored != a {
		t.Fatal("profile identity changed", err)
	}
	info, _ := os.Stat(a.Directory)
	if info.Mode().Perm() != 0700 {
		t.Fatal("profile permissions")
	}
	info, _ = os.Stat(filepath.Join(a.Directory, "login-device.id"))
	if info.Mode().Perm() != 0600 {
		t.Fatal("file permissions")
	}
}
func TestSelectionLookupDoesNotCreateStateAndSameNamesDoNotCollide(t *testing.T) {
	root := filepath.Join(t.TempDir(), "absent")
	if _, err := LastApp(root, "/Applications/WeChat.app"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatal("lookup created state")
	}
	c, err := Open(root, "/Applications/WeChat.app")
	if err != nil {
		t.Fatal(err)
	}
	a, _ := c.Ensure("/Applications/WeChat.app")
	b, _ := c.Ensure("/Users/synthetic/WeChat.app")
	if a.ID == b.ID {
		t.Fatal("same basename collides")
	}
	if _, err := c.Ensure("relative.app"); err == nil {
		t.Fatal("relative path accepted")
	}
}
func TestMigrationFailurePublishesNoPartialProfileAndCanRetry(t *testing.T) {
	root := t.TempDir()
	app := "/Applications/WeChat.app"
	legacyFixture(t, root, app)
	if err := os.Symlink(t.TempDir(), filepath.Join(root, "media")); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root, app); err == nil {
		t.Fatal("migration followed symlink")
	}
	p, _ := profile(root, app)
	if _, err := os.Stat(p.Directory); !os.IsNotExist(err) {
		t.Fatal("published partial migration")
	}
	if err := os.Remove(filepath.Join(root, "media")); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root, app); err != nil {
		t.Fatal("retry failed", err)
	}
}

func TestLogicalInstanceAndRelocatedRoot(t *testing.T) {
	root := t.TempDir()
	c, err := Open(root, "instance:server")
	if err != nil {
		t.Fatal(err)
	}
	original, err := c.Ensure("instance:server")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Activate(original); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "moved")
	if err = copyPrivate(root, target); err != nil {
		t.Fatal(err)
	}
	moved, err := Open(target, "instance:default")
	if err != nil {
		t.Fatal(err)
	}
	entries, err := moved.List()
	if err != nil || len(entries) != 1 {
		t.Fatal("moved metadata was not portable", err)
	}
	if entries[0].ID != original.ID || !strings.HasPrefix(entries[0].Directory, target+string(os.PathSeparator)) {
		t.Fatal("metadata references previous host")
	}
	if app, err := LastApp(target, "instance:default"); err != nil || app != "instance:server" {
		t.Fatal("last instance lost", err)
	}
	for _, bad := range []string{"instance:", "instance:..", "instance:a/b", "instance:a\nother"} {
		if _, err := moved.Ensure(bad); err == nil {
			t.Fatal("invalid instance accepted")
		}
	}
}
