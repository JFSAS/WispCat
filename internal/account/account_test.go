package account

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func syntheticConfig(t *testing.T, home, bundle string) string {
	t.Helper()
	path := filepath.Join(home, "Library", "Containers", bundle, "Data", "Documents", "xwechat_files", "all_users", "config", "global_config")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("synthetic config"), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDiskScopeSelectsTheRequestedInstance(t *testing.T) {
	home := t.TempDir()
	original := syntheticConfig(t, home, "com.tencent.xinWeChat")
	clone := syntheticConfig(t, home, "com.tencent.xinWeChat2")
	for app, want := range map[string]string{DefaultApp: original, "/Applications/微信2.app": clone} {
		scope, err := (Source{App: app}).diskScopeAt(home)
		if err != nil || scope.Config != want || scope.PID != "" {
			t.Fatal("disk discovery selected the wrong instance or required a process", err)
		}
	}
}

func TestScopeUsesDiskWithoutConsultingAProcess(t *testing.T) {
	want := Scope{Config: "/synthetic/config"}
	got, err := resolveScope(func() (Scope, error) { return want, nil }, func() (Scope, error) {
		t.Fatal("a disk configuration consulted the official process")
		return Scope{}, ErrLogin
	})
	if err != nil || got != want {
		t.Fatal("disk source was not retained", err)
	}
}

func TestProcessFallbackOnlyRunsWhenDiskFilesAreMissing(t *testing.T) {
	for _, diskErr := range []error{ErrDiskNotFound, ErrDiskAmbiguous, ErrLogin} {
		called := false
		_, err := resolveScope(func() (Scope, error) { return Scope{}, diskErr }, func() (Scope, error) {
			called = true
			return Scope{Config: "/synthetic/custom-location"}, nil
		})
		if diskErr == ErrDiskNotFound {
			if !called || err != nil {
				t.Fatal("missing disk files did not use the process locator", err)
			}
		} else if called || !errors.Is(err, diskErr) {
			t.Fatal("an ambiguous or unusable disk source fell back to another account")
		}
	}
}

func TestMissingDiskScopeDoesNotSelectAnotherInstance(t *testing.T) {
	home := t.TempDir()
	syntheticConfig(t, home, "com.tencent.xinWeChat")
	_, err := (Source{App: "/Applications/微信2.app"}).diskScopeAt(home)
	if !errors.Is(err, ErrDiskNotFound) {
		t.Fatal("missing clone selected original credentials", err)
	}
}

func TestAmbiguousDiskScopeDoesNotPickAnArbitraryFile(t *testing.T) {
	home := t.TempDir()
	syntheticConfig(t, home, "com.tencent.xinWeChat")
	path := filepath.Join(home, "Library", "Application Support", "com.tencent.xinWeChat", "xwechat_files", "all_users", "config", "global_config")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("another synthetic config"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := (Source{App: DefaultApp}).diskScopeAt(home)
	if !errors.Is(err, ErrDiskAmbiguous) {
		t.Fatal("ambiguous files were silently selected", err)
	}
}

func TestImportErrorKeepsClassificationAndHidesUnderlyingText(t *testing.T) {
	err := &ImportError{Stage: "decode_credentials", Err: ErrLogin}
	if !errors.Is(err, ErrLogin) || err.Error() != "official_disk_import_failed" {
		t.Fatal("import diagnostics lost the original classification")
	}
}
