//go:build integrationfixtures

package service

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDeploymentDriverRefusesOtherCheckpointsWithoutOverwriting(t *testing.T) {
	s := recoveryFixture(t)
	if err := os.Chmod(s.directory, 0700); err != nil {
		t.Fatal(err)
	}
	s.source.App = "instance:deployment-synthetic-foreign"
	if err := s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(s.checkpoint.Directory, s.data.Binding+".checkpoint")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = NewDeploymentDriver(s, "synthetic-foreign"); err == nil {
		t.Fatal("nonfixture keys accepted")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("refused fixture overwrote source checkpoint", err)
	}
}

func TestDeploymentDriverOnlineStartFailsWithoutProtocolRequests(t *testing.T) {
	s := New(t.TempDir(), time.Second, "instance:deployment-synthetic-test")
	if err := os.Chmod(s.directory, 0700); err != nil {
		t.Fatal(err)
	}
	h, err := OpenEventHistory(s.directory)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if err = (BotAdapter{Service: s}).EnableHistory(h); err != nil {
		t.Fatal(err)
	}
	d, err := NewDeploymentDriver(s, "synthetic-test")
	if err != nil {
		t.Fatal(err)
	}
	d.Start(context.Background(), "online")
	done := make(chan struct{})
	go func() { d.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("refused fixture did not join")
	}
	if d.Inspect().Error == "" {
		t.Fatal("online fixture start silently accepted")
	}
	for _, name := range []string{"auth", "sync", "send", "list", "brief"} {
		if s.calls[name] != 0 {
			t.Fatal("refused fixture executed protocol request", name)
		}
	}
}
