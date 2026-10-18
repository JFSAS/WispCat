package service

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JFSAS/wispcat/internal/native"
	"github.com/JFSAS/wispcat/internal/protocol"
	"github.com/JFSAS/wispcat/internal/store"
)

func TestAutomaticStartupWaitsForALoginChoice(t *testing.T) {
	for _, rejectedQR := range []bool{false, true} {
		s := New(t.TempDir(), time.Second, "/synthetic/WeChat.app")
		if rejectedQR {
			s = recoveryFixture(t)
			s.data.LoginSource, s.data.SessionInvalid = "qr", true
			if err := s.checkpoint.Save(s.data); err != nil {
				t.Fatal(err)
			}
		}
		var qrRequests atomic.Int32
		s.loginFactory = func() (loginEngine, error) {
			qrRequests.Add(1)
			return &fakeLogin{}, nil
		}
		ctx, cancel := context.WithCancel(context.Background())
		s.Start(ctx, "online")
		deadline := time.Now().Add(2 * time.Second)
		for s.Status()["state"] != "needs_login" && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		status := s.Status()
		cancel()
		s.Wait()
		if status["state"] != "needs_login" || s.login.ID != "" || qrRequests.Load() != 0 || s.connection.DiskRead != "not_attempted" {
			t.Fatal("automatic startup selected a QR or disk login instead of waiting for the user")
		}
	}
}

func recoveryFixture(t *testing.T) *Service {
	t.Helper()
	s := New(t.TempDir(), 5*time.Second, "/synthetic/WeChat.app")
	b := protocol.Bootstrap{Username: []byte("synthetic-owner"), UIN: 123, DeviceID: []byte("synthetic-device"), AAK: []byte("synthetic-ticket")}
	s.data = &store.Data{Schema: 1, Binding: b.Binding(), SourceApp: s.source.App, APIKey: bytes.Repeat([]byte{1}, 32), Session: &protocol.Session{Bootstrap: b, Original: bytes.Repeat([]byte{2}, 16), ClientKey: bytes.Repeat([]byte{3}, 24), ServerKey: bytes.Repeat([]byte{4}, 24)}}
	s.checkpoint = store.Checkpoint{Directory: filepath.Join(s.directory, "checkpoints"), Binding: s.data.Binding}
	s.client = &protocol.Client{Bootstrap: b, State: cloneSession(s.data.Session)}
	s.state, s.sessionState, s.lastSync = "connected", "validated", 123456
	return s
}

func TestRejectedIndependentTicketRequiresQRAndKeepsOriginOnRestart(t *testing.T) {
	s := recoveryFixture(t)
	s.data.LoginSource = "qr"
	s.block(&protocol.Error{Category: "authentication_rejected"})
	if !strings.Contains(s.detail, "扫码") || strings.Contains(s.connection.NextStep, "从官方磁盘") {
		t.Fatal("independent rejection advised an official credential import")
	}
	restarted := New(s.directory, time.Second, s.source.App)
	err := restarted.prepare(true, false)
	var p *protocol.Error
	if !errors.As(err, &p) || p.Category != "login_required" || restarted.client != nil || restarted.data.LoginSource != "qr" {
		t.Fatal("known rejected independent checkpoint was retried or lost its origin", err)
	}
}

func TestNetworkOutageKeepsCacheAndContinuesAutomaticRecovery(t *testing.T) {
	s := recoveryFixture(t)
	r := sessionRecovery{}
	for range 6 {
		delay, retry := s.recoverFailure(io.ErrUnexpectedEOF, &r)
		status := s.Status()
		if !retry || delay > 30*time.Second || delay <= 0 || status["mode"] != "online" || status["session_state"] != "validated" || status["last_sync_at"] != int64(123456) {
			t.Fatal("network outage invalidated a credential or stopped automatic recovery")
		}
		if s.data.SessionInvalid || !bytes.Equal(s.data.Session.Bootstrap.AAK, []byte("synthetic-ticket")) {
			t.Fatal("network failure replaced or rejected saved credentials")
		}
	}
	if s.state != "network_unavailable" {
		t.Fatal("persistent network outage was mislabeled as disconnected")
	}
	if _, retry := s.recoverFailure(io.ErrUnexpectedEOF, &r); retry || s.mode != "blocked" || s.data.SessionInvalid {
		t.Fatal("network retries did not stop at their bound while retaining the cache")
	}
}

func TestSyncProtocolFailureHasBoundedRecovery(t *testing.T) {
	s := recoveryFixture(t)
	s.mode, s.connection.Stage = "online", "sync"
	r := sessionRecovery{}
	for i := 0; i < 3; i++ {
		delay, retry := s.recoverFailure(protocol.ErrWire, &r)
		if retry != (i < 2) || (retry && delay <= 0) || s.data.SessionInvalid {
			t.Fatalf("sync protocol failure %d: retry=%v delay=%v state=%s", i+1, retry, delay, s.state)
		}
	}
	if s.mode != "blocked" || s.category != "protocol_retry_exhausted" {
		t.Fatal("persistent malformed responses did not stop with a specific error")
	}
}

func TestCertificateFailureDoesNotRetryOrInvalidateCache(t *testing.T) {
	s := recoveryFixture(t)
	_, retry := s.recoverFailure(&native.ExchangeError{Stage: "verify_certificate", Retryable: false}, &sessionRecovery{})
	if retry || s.mode != "blocked" || s.data.SessionInvalid {
		t.Fatal("untrusted TLS peer was retried or treated as rejected credentials")
	}
}

func TestExpiredSessionRefreshesCacheBeforeDeclaringDisconnected(t *testing.T) {
	s := recoveryFixture(t)
	r := sessionRecovery{}
	delay, retry := s.recoverFailure(&protocol.Error{Category: "session_expired", Code: -13}, &r)
	if !retry || delay != 0 || !r.reauth || s.state != "recovering" || s.data.SessionInvalid {
		t.Fatal("expired session did not first use its cached automatic-login ticket")
	}
	_, retry = s.recoverFailure(io.ErrUnexpectedEOF, &r)
	if !retry || !r.reauth || s.state != "recovering" || s.data.SessionInvalid {
		t.Fatal("temporary reauthentication failure was treated as a rejected ticket")
	}
	// Authentication succeeds, but the server rejects the replacement session
	// before any successful sync. Do not start an endless authentication loop.
	r.reauth = false
	_, retry = s.recoverFailure(&protocol.Error{Category: "session_expired", Code: -13}, &r)
	if retry || s.state != "disconnected" || !s.data.SessionInvalid {
		t.Fatal("a rejected refreshed session caused repeated authentication")
	}
}

func TestRejectedCacheIsPreservedAndRememberedAcrossRestart(t *testing.T) {
	s := recoveryFixture(t)
	_, retry := s.recoverFailure(&protocol.Error{Category: "authentication_rejected"}, &sessionRecovery{reauth: true})
	if retry || s.state != "disconnected" || s.sessionState != "invalid" {
		t.Fatal("rejected cache did not require explicit disk reconnect")
	}
	loaded, err := s.checkpoint.Load()
	if err != nil || !loaded.SessionInvalid || !bytes.Equal(loaded.Session.ClientKey, s.data.Session.ClientKey) {
		t.Fatal("cache rejection discarded messages/keys or lost its invalid marker", err)
	}
	restarted := New(s.directory, time.Second, s.source.App)
	err = restarted.prepare(true, false)
	var p *protocol.Error
	if !errors.As(err, &p) || p.Category != "authentication_rejected" || restarted.client != nil {
		t.Fatal("restart imported official disk or retried a known rejected cache", err)
	}
}

func TestCachedSessionRestoresWithoutAnOfficialProcess(t *testing.T) {
	s := recoveryFixture(t)
	if err := s.checkpoint.Save(s.data); err != nil {
		t.Fatal(err)
	}
	restarted := New(s.directory, time.Second, s.source.App)
	if err := restarted.prepare(false, false); err != nil {
		t.Fatal(err)
	}
	if !restarted.restored || restarted.client == nil || !bytes.Equal(restarted.client.Bootstrap.AAK, s.client.Bootstrap.AAK) {
		t.Fatal("restoration failed to retain the cached ticket")
	}
}

func TestHTTPOutageAndProtocolFailureDoNotInvalidateCache(t *testing.T) {
	for _, err := range []error{&protocol.Error{Category: "http_response_failed", Code: 503}, protocol.ErrWire} {
		s := recoveryFixture(t)
		_, retry := s.recoverFailure(err, &sessionRecovery{})
		if s.data.SessionInvalid || s.state == "disconnected" {
			t.Fatal("unconfirmed protocol/network failure was labeled credential rejection")
		}
		if retry != !errors.Is(err, protocol.ErrWire) {
			t.Fatal("HTTP outage did not retry or invalid protocol was retried blindly")
		}
	}
}

func TestCacheRecoveryDoesNotDependOnOriginalInstallation(t *testing.T) {
	s := recoveryFixture(t)
	s.data.Session.Profile = protocol.MacCompatibilityProfile()
	identity, err := protocol.NewClientIdentity(s.data.Session.Bootstrap.DeviceID)
	if err != nil {
		t.Fatal(err)
	}
	s.data.Session.Identity = &identity
	if err = s.checkpoint.Save(s.data); err != nil {
		t.Fatal(err)
	}
	restarted := New(s.directory, time.Second, "instance:server")
	if err = restarted.prepare(true, false); err != nil {
		t.Fatal("cache still depends on official installation", err)
	}
	if restarted.client == nil || restarted.client.State.Identity == nil || *restarted.client.State.Identity != identity || restarted.client.State.Profile != s.data.Session.Profile {
		t.Fatal("cached identity changed")
	}
	if restarted.connection.DiskRead != "not_attempted" || restarted.Status()["auth_calls"] != uint64(0) {
		t.Fatal("prepare imported credentials or authenticated")
	}
}
