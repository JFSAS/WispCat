package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JFSAS/wechat-runtime/internal/account"
	"github.com/JFSAS/wechat-runtime/internal/credentials"
	"github.com/JFSAS/wechat-runtime/internal/protocol"
)

func TestExportUsesSelectedSourceAndNeverOverwrites(t *testing.T) {
	t.Setenv("TEST_EXPORT_PASSWORD", "synthetic-export-password")
	first, second := bytes.Repeat([]byte{7}, 24), []byte("synthetic-second")
	b := protocol.Bootstrap{Username: []byte("synthetic-owner"), UIN: 123, DeviceID: []byte("synthetic-device-000"), LocalID: 42, KeyInfo: []byte("synthetic-key-info"), First: first, Second: second, AAK: protocol.Join(protocol.Blob(1, protocol.Buffer(first)), protocol.Blob(2, protocol.Buffer(second)))}
	path := filepath.Join(t.TempDir(), "login.wxlogin")
	args := []string{"--app", "/synthetic/Selected.app", "--out", path, "--passphrase-env", "TEST_EXPORT_PASSWORD"}
	var out bytes.Buffer
	load := func(app string) (protocol.Bootstrap, error) {
		if app != "/synthetic/Selected.app" {
			t.Fatal("wrong installation")
		}
		return b, nil
	}
	if err := run(args, &out, load); err != nil {
		t.Fatal(err)
	}
	file, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0600 || strings.Contains(out.String(), string(b.Username)) || strings.Contains(out.String(), string(b.KeyInfo)) {
		t.Fatal("credential disclosure or public permissions")
	}
	got, err := credentials.Open(file, os.Getenv("TEST_EXPORT_PASSWORD"))
	if err != nil || got.UIN != b.UIN {
		t.Fatal("unusable file")
	}
	if err := run(args, &out, load); err == nil {
		t.Fatal("overwrote existing export")
	}
	again, _ := os.ReadFile(path)
	if !bytes.Equal(file, again) {
		t.Fatal("changed existing export")
	}
}

func TestExportFailsBeforeReadingWithoutPasswordAndRedactsErrors(t *testing.T) {
	t.Setenv("WECHAT_CREDENTIAL_PASSPHRASE", "")
	called := false
	load := func(string) (protocol.Bootstrap, error) {
		called = true
		return protocol.Bootstrap{}, &account.ImportError{Stage: "read_config", Err: errors.New("secret-value")}
	}
	if err := run(nil, &bytes.Buffer{}, load); err == nil || called {
		t.Fatal("read credentials without valid export password")
	}
	t.Setenv("WECHAT_CREDENTIAL_PASSPHRASE", "synthetic-export-password")
	err := run(nil, &bytes.Buffer{}, load)
	if err == nil || strings.Contains(err.Error(), "secret-value") || !strings.Contains(err.Error(), "read_config") {
		t.Fatal("disk diagnostics not redacted")
	}
}
