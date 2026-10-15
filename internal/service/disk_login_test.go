package service

import (
	"context"
	"errors"
	"github.com/JFSAS/wechat-runtime/internal/account"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JFSAS/wechat-runtime/internal/protocol"
)

func diskAppFixture(t *testing.T, name, bundle string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.MkdirAll(filepath.Join(path, "Contents"), 0700); err != nil {
		t.Fatal(err)
	}
	raw := `<plist><dict><key>CFBundleIdentifier</key><string>` + bundle + `</string><key>CFBundleShortVersionString</key><string>4.1.13</string></dict></plist>`
	if err := os.WriteFile(filepath.Join(path, "Contents/Info.plist"), []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func TestDiskSourceChangesOnlyAtSerialWorkerBoundary(t *testing.T) {
	s := recoveryFixture(t)
	oldSource, oldClient, oldEpoch := s.source.App, s.client, s.modeEpoch
	target := diskAppFixture(t, "微信2.app", "com.tencent.xinWeChat2")
	cancelled := false
	s.cancelRPC = func() { cancelled = true }
	if err := s.QueueDiskLoginFrom(target); err != nil {
		t.Fatal(err)
	}
	if s.source.App != oldSource || s.client != oldClient || s.Status()["selected_wechat_app"] != target || !cancelled || s.modeEpoch <= oldEpoch || !s.fresh {
		t.Fatal("request replaced an in-flight client or lost the selected path")
	}
	s.mu.Lock()
	changed := s.applyPendingAppLocked()
	s.mu.Unlock()
	if !changed || s.source.App != target || s.client != nil || s.pendingApp != "" {
		t.Fatal("worker did not install selected source")
	}
}
func TestInvalidDiskSourceAndAbandonedSelectionDoNotChangeSource(t *testing.T) {
	s := recoveryFixture(t)
	original := s.source.App
	epoch := s.modeEpoch
	if err := s.QueueDiskLoginFrom("relative.app"); err == nil || s.modeEpoch != epoch || s.source.App != original {
		t.Fatal("invalid path changed login state")
	}
	target := diskAppFixture(t, "微信2.app", "com.tencent.xinWeChat2")
	for _, action := range []string{"offline", "qr"} {
		if err := s.QueueDiskLoginFrom(target); err != nil {
			t.Fatal(err)
		}
		if action == "offline" {
			_ = s.SetMode("offline", false)
		} else {
			if _, err := s.QueueLogin(); err != nil {
				t.Fatal(err)
			}
		}
		s.mu.Lock()
		changed := s.applyPendingAppLocked()
		s.mu.Unlock()
		if changed || s.source.App != original {
			t.Fatal("cancelled selection was still applied", action)
		}
	}
}
func TestDiskSelectionDoesNotBypassDifferentAccountProtection(t *testing.T) {
	s := recoveryFixture(t)
	old := s.data.Session.Bootstrap
	other := old
	other.UIN++
	other.Username = []byte("synthetic-other-owner")
	if _, err := cacheForLogin(s.data, other); err == nil {
		t.Fatal("source selection bypassed account binding")
	}
	s.block(&protocol.Error{Category: "account_scope_changed"})
	status := s.Status()
	if status["state"] != "scope_changed" || !strings.Contains(status["detail"].(string), "原有缓存已保留") {
		t.Fatal("account mismatch not explained")
	}
	if string(s.data.Session.Bootstrap.Username) != string(old.Username) {
		t.Fatal("old account overwritten")
	}
}
func TestSourceQueueAndPublicSnapshotsRemainRaceFree(t *testing.T) {
	s := New(t.TempDir(), time.Second, "/synthetic/WeChat.app")
	target := diskAppFixture(t, "Clone.app", "com.tencent.xinWeChat2")
	var wait sync.WaitGroup
	for i := 0; i < 3; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for n := 0; n < 25; n++ {
				_ = s.QueueDiskLoginFrom(target)
				_ = s.Status()
				_ = s.SetMode("offline", false)
			}
		}()
	}
	wait.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.Start(ctx, "offline")
	s.Wait()
}

func TestSlowAppValidationCannotOverrideANewerLoginChoice(t *testing.T) {
	s := recoveryFixture(t)
	old := s.source.App
	entered, release := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- s.queueDiskLoginFrom("/synthetic/Clone.app", func(string) (account.Installation, error) {
			close(entered)
			<-release
			return account.Installation{Path: "/synthetic/Clone.app"}, nil
		})
	}()
	<-entered
	if err := s.SetMode("offline", false); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-result; !errors.Is(err, ErrLoginSuperseded) || s.mode != "offline" || s.pendingApp != "" || s.source.App != old {
		t.Fatal("slow directory validation replaced newer offline choice", err)
	}
}
