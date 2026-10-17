package service

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/JFSAS/wispcat/internal/protocol"
	"github.com/JFSAS/wispcat/internal/store"
)

func credentialBootstrap() protocol.Bootstrap {
	first, second := bytes.Repeat([]byte{7}, 24), []byte("synthetic-second")
	return protocol.Bootstrap{Username: []byte("synthetic-owner"), UIN: 123, DeviceID: []byte("synthetic-device-000"), LocalID: 42, KeyInfo: []byte("synthetic-key-info"), First: first, Second: second, AAK: protocol.Join(protocol.Blob(1, protocol.Buffer(first)), protocol.Blob(2, protocol.Buffer(second)))}
}

func credentialAuthenticator(_ context.Context, c *protocol.Client) error {
	c.State = &protocol.Session{Bootstrap: c.Bootstrap, Original: bytes.Repeat([]byte{2}, 16), ClientKey: bytes.Repeat([]byte{3}, 24), ServerKey: bytes.Repeat([]byte{4}, 24), Cursor: bytes.Clone(c.SavedCursor)}
	return nil
}

func TestCredentialLoginValidatesBeforeAdoptionAndPersistsHistory(t *testing.T) {
	s := New(t.TempDir(), time.Second, "instance:linux")
	h, err := OpenEventHistory(s.directory)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if err = (BotAdapter{Service: s}).EnableHistory(h); err != nil {
		t.Fatal(err)
	}
	s.credentialAuth = credentialAuthenticator
	s.loginValidate = func(_ context.Context, c *protocol.Client) (protocol.SyncResult, error) {
		if s.Status()["state"] == "connected" || s.data != nil {
			t.Fatal("adopted before first Sync")
		}
		c.State.Confirmed = true
		return protocol.SyncResult{Messages: []protocol.Message{{Sender: []byte("synthetic-friend"), Recipient: c.Bootstrap.Username, Content: []byte("synthetic imported message"), Type: 1, ServerID: 55, Timestamp: 1700000000}}}, nil
	}
	b := credentialBootstrap()
	if err = s.QueueCredentialLogin(b); err != nil {
		t.Fatal(err)
	}
	b.KeyInfo[0] ^= 1 // queue owns its own copy
	if err = s.QueueCredentialLogin(credentialBootstrap()); err == nil {
		t.Fatal("accepted parallel import")
	}
	if s.Status()["state"] != "logging_in" || s.Status()["auth_calls"] != uint64(0) {
		t.Fatal("queue performed authentication")
	}
	s.workCredentialLogin(context.Background())
	if s.state != "connected" || s.sessionState != "validated" || s.calls["auth"] != 1 || s.calls["sync"] != 1 || s.calls["send"] != 0 {
		t.Fatal("login not validated")
	}
	if s.data.LoginSource != "credential_file" || s.data.SourceApp != "instance:linux" || s.data.Session.Bootstrap.KeyInfo[0] != 's' {
		t.Fatal("wrong source or shared bootstrap")
	}
	stats, err := h.Stats(s.data.Binding)
	if err != nil || stats.Messages != 1 {
		t.Fatal("first Sync was not durable", stats, err)
	}
	next := New(s.directory, time.Second, "instance:linux")
	if err = next.prepare(false, false); err != nil {
		t.Fatal(err)
	}
	if next.data.LoginSource != "credential_file" || !next.data.Session.Confirmed {
		t.Fatal("checkpoint not portable")
	}
}

func TestCredentialLoginPreservesExistingNamespaceAndRejectsOtherAccount(t *testing.T) {
	for _, different := range []bool{false, true} {
		s := recoveryFixture(t)
		if err := s.checkpoint.Save(s.data); err != nil {
			t.Fatal(err)
		}
		binding := s.data.Binding
		s.data.Session.Cursor = []byte("old-cursor")
		b := credentialBootstrap()
		b.DeviceID = []byte("different-device-000")
		if different {
			b.UIN++
		}
		s.credentialAuth = func(ctx context.Context, c *protocol.Client) error {
			if different {
				t.Fatal("other account reached authentication")
			}
			if string(c.SavedCursor) != "old-cursor" {
				t.Fatal("lost sync cursor")
			}
			return credentialAuthenticator(ctx, c)
		}
		s.loginValidate = func(_ context.Context, c *protocol.Client) (protocol.SyncResult, error) {
			c.State.Confirmed = true
			return protocol.SyncResult{}, nil
		}
		if err := s.QueueCredentialLogin(b); err != nil {
			t.Fatal(err)
		}
		s.workCredentialLogin(context.Background())
		if different {
			if s.category != "account_scope_changed" || s.calls["auth"] != 0 || s.data.Binding != binding {
				t.Fatal("account scope changed")
			}
		} else if s.state != "connected" || s.data.Binding != binding || !s.data.ValidBinding() || len(s.data.BindingDeviceID) != 16 {
			t.Fatal("same account lost namespace")
		}
	}
}

func TestCredentialFailureOrCancellationNeverReplacesCheckpoint(t *testing.T) {
	for _, stage := range []string{"auth", "sync", "timeout", "offline", "qr"} {
		t.Run(stage, func(t *testing.T) {
			s := recoveryFixture(t)
			if err := s.checkpoint.Save(s.data); err != nil {
				t.Fatal(err)
			}
			// Compare the logical durable session as the encrypted format uses random nonces.
			before, err := s.checkpoint.Load()
			if err != nil {
				t.Fatal(err)
			}
			s.credentialAuth = func(ctx context.Context, c *protocol.Client) error {
				if stage == "auth" {
					return &protocol.Error{Category: "authentication_rejected"}
				}
				if stage == "offline" {
					_ = s.SetMode("offline", false)
				}
				if stage == "qr" {
					_, _ = s.QueueLogin()
				}
				return credentialAuthenticator(ctx, c)
			}
			s.loginValidate = func(_ context.Context, c *protocol.Client) (protocol.SyncResult, error) {
				return protocol.SyncResult{}, errors.New("synthetic-sync-failure")
			}
			if err := s.QueueCredentialLogin(credentialBootstrap()); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if stage == "timeout" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			s.workCredentialLogin(ctx)
			after, err := s.checkpoint.Load()
			if err != nil || !bytes.Equal(after.Session.Bootstrap.AAK, before.Session.Bootstrap.AAK) || s.state == "connected" {
				t.Fatal("failed import adopted")
			}
			if stage == "timeout" && (s.mode != "blocked" || s.credential != nil) {
				t.Fatal("timed out import stuck pending")
			}
			if stage == "offline" && s.mode != "offline" {
				t.Fatal("late auth replaced offline choice")
			}
			if stage == "qr" && s.mode != "qr" {
				t.Fatal("late auth replaced QR choice")
			}
		})
	}
}

func TestSerialWorkerCancelsCredentialRPCAndJoins(t *testing.T) {
	s := New(t.TempDir(), time.Second, "instance:linux")
	entered, release := make(chan struct{}), make(chan struct{})
	s.credentialAuth = func(ctx context.Context, _ *protocol.Client) error {
		close(entered)
		<-ctx.Done()
		close(release)
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx, "offline")
	if err := s.QueueCredentialLogin(credentialBootstrap()); err != nil {
		cancel()
		s.Wait()
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		cancel()
		s.Wait()
		t.Fatal("worker did not import")
	}
	cancel()
	s.Wait()
	select {
	case <-release:
	default:
		t.Fatal("worker was not joined")
	}
	if s.credential != nil || s.calls["auth"] != 1 || s.calls["sync"] != 0 {
		t.Fatal("shutdown retained import or started Sync")
	}
	if _, exists, err := store.Unique(filepath.Join(s.directory, "checkpoints")); err != nil || exists {
		t.Fatal("failed import persisted a session")
	}
	if err := s.QueueCredentialLogin(credentialBootstrap()); err == nil {
		t.Fatal("stopped worker accepted credentials")
	}
}

func TestCredentialLoginOwnsFreshCancelableRPC(t *testing.T) {
	s := New(t.TempDir(), time.Second, "instance:linux")
	old, done := s.rpc(context.Background())
	if err := s.QueueCredentialLogin(credentialBootstrap()); err != nil {
		t.Fatal(err)
	}
	if old.Err() != context.Canceled {
		t.Fatal("previous RPC was not cancelled")
	}
	done()
	s.credentialAuth = func(ctx context.Context, _ *protocol.Client) error {
		if ctx.Err() != nil {
			t.Fatal("import inherited previous cancellation")
		}
		_ = s.SetMode("offline", false)
		if ctx.Err() != context.Canceled {
			t.Fatal("mode switch did not cancel import RPC")
		}
		return ctx.Err()
	}
	s.workCredentialLogin(context.Background())
	if s.mode != "offline" || s.data != nil {
		t.Fatal("late import replaced offline choice")
	}
}
