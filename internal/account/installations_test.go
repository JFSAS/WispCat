package account

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func appFixture(t *testing.T, path, bundle string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(path, "Contents"), 0700); err != nil {
		t.Fatal(err)
	}
	body := `<?xml version="1.0"?><plist version="1.0"><dict><key>CFBundleIdentifier</key><string>` + bundle + `</string><key>CFBundleShortVersionString</key><string>4.1.13</string></dict></plist>`
	if err := os.WriteFile(filepath.Join(path, "Contents/Info.plist"), []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func TestAppDiscoverySeparatesWeChatInstallationsAndExcludesOtherProducts(t *testing.T) {
	root := t.TempDir()
	a := appFixture(t, filepath.Join(root, "WeChat.app"), "com.tencent.xinWeChat")
	b := appFixture(t, filepath.Join(root, "微信2.app"), "com.tencent.xinWeChat2")
	appFixture(t, filepath.Join(root, "企业微信.app"), "com.tencent.WeWorkMac")
	appFixture(t, filepath.Join(root, "wechatwebdevtools.app"), "com.tencent.webplusdevtools")
	apps := discoverAppsAt([]string{root}, a)
	if len(apps) != 2 || apps[0].Path != a || apps[1].Path != b || !apps[0].Available || apps[1].BundleID != "com.tencent.xinWeChat2" {
		t.Fatal("discovery mixed installations", apps)
	}
}
func TestCustomAppPathUsesItsOwnBundleContainer(t *testing.T) {
	root, home := t.TempDir(), t.TempDir()
	original := syntheticConfig(t, home, "com.tencent.xinWeChat")
	clone := syntheticConfig(t, home, "com.tencent.xinWeChat2")
	for _, tc := range []struct{ folder, bundle, want string }{{"Personal.app", "com.tencent.xinWeChat", original}, {"Work.app", "com.tencent.xinWeChat2", clone}} {
		path := appFixture(t, filepath.Join(root, tc.folder), tc.bundle)
		if app, err := ResolveApp(path); err != nil || app.Path != path {
			t.Fatal("custom app rejected", err)
		}
		scope, err := (Source{App: path}).diskScopeAt(home)
		if err != nil || scope.Config != tc.want {
			t.Fatal("custom app chose another container", err)
		}
		wantError := ErrVersion
		if runtime.GOOS != "darwin" {
			wantError = ErrDiskPlatform
		}
		if err = (Source{App: path}).Verify(); !errors.Is(err, wantError) {
			t.Fatal("metadata-only app bypassed binary hash guard")
		}
	}
}
func TestAppSelectionRejectsInvalidPathsAndUnknownBundles(t *testing.T) {
	for _, path := range []string{"", "WeChat.app", "~/Applications/WeChat.app", "/Applications", "/Applications/WeChat.app\nother"} {
		if _, err := NormalizeAppPath(path); !errors.Is(err, ErrAppPath) {
			t.Fatal("invalid app path accepted", path)
		}
	}
	path := appFixture(t, filepath.Join(t.TempDir(), "Unknown.app"), "unknown.bundle")
	if _, err := ResolveApp(path); !errors.Is(err, ErrVersion) {
		t.Fatal("unrecognized bundle allowed")
	}
	if _, err := ResolveApp(filepath.Join(t.TempDir(), "Missing.app")); !errors.Is(err, ErrAppMissing) {
		t.Fatal("missing app allowed")
	}
}
func TestBinaryPlistMetadataCanBeListedWithoutCredentialReads(t *testing.T) {
	if _, err := os.Stat("/usr/bin/plutil"); err != nil {
		t.Skip("macOS plutil unavailable")
	}
	xml := []byte(`<plist><dict><key>CFBundleIdentifier</key><string>com.tencent.xinWeChat2</string><key>CFBundleShortVersionString</key><string>4.1.13</string></dict></plist>`)
	command := exec.Command("/usr/bin/plutil", "-convert", "binary1", "-o", "-", "-")
	command.Stdin = bytes.NewReader(xml)
	raw, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := bundleMetadata(raw)
	if err != nil || metadata["CFBundleIdentifier"] != "com.tencent.xinWeChat2" {
		t.Fatal("binary plist detection failed", err)
	}
}
