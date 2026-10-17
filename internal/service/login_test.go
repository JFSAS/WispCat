package service

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JFSAS/wispcat/internal/native"
	"github.com/JFSAS/wispcat/internal/protocol"
	"github.com/JFSAS/wispcat/internal/store"
)

type fakeLogin struct {
	state                string
	starts, polls, auths int
	onStart              func()
	onPoll               func() error
	authError            error
	candidate            *protocol.Session
}

func (f *fakeLogin) Start(context.Context, time.Time) error {
	f.starts++
	f.state = "waiting_scan"
	if f.onStart != nil {
		f.onStart()
	}
	return nil
}
func (f *fakeLogin) Poll(context.Context, time.Time) error {
	f.polls++
	if f.onPoll != nil {
		if err := f.onPoll(); err != nil {
			return err
		}
	}
	f.state = "confirmed"
	return nil
}
func (f *fakeLogin) Authenticate(context.Context) (*protocol.Session, error) {
	f.auths++
	f.state = "authenticated"
	if f.authError != nil {
		return nil, f.authError
	}
	return f.candidate, nil
}

func TestQRTransientQueryFailureKeepsChallengeAndCanFinish(t *testing.T) {
	s, f := loginFixture(t)
	if _, err := s.QueueLogin(); err != nil {
		t.Fatal(err)
	}
	s.workLogin(context.Background())
	id := s.login.ID
	f.onPoll = func() error { return &native.ExchangeError{Stage: "connect", Retryable: true} }
	delay := s.workLogin(context.Background())
	if s.mode != "qr" || s.login.ID != id || s.login.State != "waiting_scan" || len(s.login.image) == 0 || f.auths != 0 || delay <= 0 {
		t.Fatal("query interruption destroyed a valid challenge")
	}
	f.onPoll = nil
	s.workLogin(context.Background())
	if s.state != "connected" || f.starts != 1 || f.auths != 1 || s.login.Error != "" {
		t.Fatal("query recovery restarted login or retained a stale error")
	}
}

func TestQRDoesNotRetryCertificateRejectionOrAuthentication(t *testing.T) {
	for _, atAuth := range []bool{false, true} {
		s, f := loginFixture(t)
		if _, err := s.QueueLogin(); err != nil {
			t.Fatal(err)
		}
		s.workLogin(context.Background())
		if atAuth {
			f.authError = &native.ExchangeError{Stage: "read", Retryable: true}
		} else {
			f.onPoll = func() error { return &native.ExchangeError{Stage: "verify_certificate", VerifyResult: 20} }
		}
		s.workLogin(context.Background())
		if s.mode != "blocked" || s.login.State != "failed" || s.login.networkFailures != 0 || s.client != nil {
			t.Fatal("unsafe login retry", atAuth)
		}
	}
}
func (f *fakeLogin) Status() protocol.LoginStatus {
	return protocol.LoginStatus{State: f.state, ImageReady: f.state == "waiting_scan"}
}
func (f *fakeLogin) Image() ([]byte, error) { return []byte("synthetic image"), nil }
func (f *fakeLogin) Cancel(state string)    { f.state = state }
func loginCandidate() *protocol.Session {
	b := protocol.Bootstrap{Username: []byte("synthetic-qr-owner"), UIN: 456, DeviceID: []byte("i00000000000000000000000000000000"), AAK: []byte("synthetic-new-ticket")}
	return &protocol.Session{Bootstrap: b, Original: bytes.Repeat([]byte{2}, 16), ClientKey: bytes.Repeat([]byte{3}, 24), ServerKey: bytes.Repeat([]byte{4}, 24), Cookie: []byte("synthetic"), AuthGeneration: 1, AuthenticatedAt: 12345}
}
func loginFixture(t *testing.T) (*Service, *fakeLogin) {
	t.Helper()
	s := New(t.TempDir(), time.Second, "/synthetic/WeChat.app")
	f := &fakeLogin{candidate: loginCandidate()}
	s.loginFactory = func() (loginEngine, error) { return f, nil }
	s.loginValidate = func(ctx context.Context, c *protocol.Client) (protocol.SyncResult, error) {
		c.State.Confirmed = true
		return protocol.SyncResult{}, nil
	}
	return s, f
}
func TestQRServiceCoalescesAndSavesOnlyValidatedSession(t *testing.T) {
	s, f := loginFixture(t)
	first, err := s.QueueLogin()
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.QueueLogin()
	if err != nil || first["id"] != second["id"] {
		t.Fatal("duplicate QR request", err)
	}
	s.workLogin(context.Background())
	if f.starts != 1 || s.data != nil || s.client != nil {
		t.Fatal("unconfirmed scan installed a session")
	}
	s.workLogin(context.Background())
	if f.auths != 1 || s.state != "connected" || s.mode != "online" {
		t.Fatal("validated login was not installed")
	}
	loaded, err := s.checkpoint.Load()
	if err != nil || loaded.LoginSource != "qr" || loaded.Session.AuthGeneration != 1 || loaded.Session.AuthenticatedAt != 12345 || loaded.SessionInvalid {
		t.Fatal("credential checkpoint", err)
	}
	if !bytes.Equal(loaded.Session.ClientKey, f.candidate.ClientKey) || !bytes.Equal(loaded.Session.Bootstrap.AAK, f.candidate.Bootstrap.AAK) {
		t.Fatal("mixed credential generations")
	}
	if _, err = s.QueueLogin(); err != nil || s.mode != "qr" || s.data != nil && s.data.LoginSource != "qr" {
		t.Fatal("explicit QR login could not replace a connected session", err)
	}
}

func TestCacheResumeDoesNotSelectALoginMethod(t *testing.T) {
	s := recoveryFixture(t)
	s.data.LoginSource = "qr"
	s.state, s.mode = "offline", "offline"
	if err := s.SetMode("online", false); err != nil || s.fresh || !s.reload || s.mode != "online" {
		t.Fatal("valid independent checkpoint was replaced by official import", err)
	}
	s.data.SessionInvalid = true
	s.state, s.mode = "disconnected", "blocked"
	if err := s.SetMode("online", false); err != nil || s.fresh || s.mode == "qr" || s.login.ID != "" {
		t.Fatal("automatic cache resume selected a login method", err)
	}
}

func TestExplicitDiskLoginDoesNotBecomeQR(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		s := recoveryFixture(t)
		s.data.LoginSource, s.data.SessionInvalid = "qr", invalid
		s.state, s.mode = "disconnected", "blocked"
		if err := s.QueueDiskLogin(); err != nil || !s.fresh || s.mode != "online" || s.login.ID != "" {
			t.Fatal("explicit disk import changed into a QR login", err)
		}
	}
}

func TestDiskLoginDiscardsPendingQRResult(t *testing.T) {
	s, f := loginFixture(t)
	f.onStart = func() {
		if err := s.QueueDiskLogin(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.QueueLogin(); err != nil {
		t.Fatal(err)
	}
	s.workLogin(context.Background())
	if s.mode != "online" || !s.fresh || s.login.State != "cancelled" || len(s.login.image) != 0 || f.state != "cancelled" {
		t.Fatal("late QR result replaced an explicit disk-login request")
	}
}

func TestDiskDeviceCanReplaceQRWithoutLosingCache(t *testing.T) {
	s := recoveryFixture(t)
	s.data.LoginSource = "qr"
	s.data.Messages = []protocol.Message{{Content: []byte("synthetic cached message")}}
	b := s.data.Session.Bootstrap
	b.DeviceID = []byte("different-official-device")
	data, err := cacheForLogin(s.data, b)
	if err != nil {
		t.Fatal(err)
	}
	if data.Binding != s.data.Binding || !bytes.Equal(data.APIKey, s.data.APIKey) || len(data.Messages) != 1 || len(s.data.BindingDeviceID) != 0 {
		t.Fatal("disk-device selection changed or lost the existing cache")
	}
	candidate := cloneSession(s.data.Session)
	candidate.Bootstrap = b
	s.data = data
	s.client = &protocol.Client{Bootstrap: b, State: candidate}
	s.connection.Source = "official_disk"
	if err := s.saveAuthenticationLocked(); err != nil || s.data.LoginSource != "official_disk" || !s.data.ValidBinding() {
		t.Fatal("disk login could not save its source with the old cache namespace")
	}
	loaded, err := s.checkpoint.Load()
	if err != nil || loaded.LoginSource != "official_disk" || !bytes.Equal(loaded.Session.Bootstrap.DeviceID, b.DeviceID) || len(loaded.Messages) != 1 {
		t.Fatal("disk login cannot restore the existing history", err)
	}
}

func TestQueuedDiskLoginDoesNotRelabelTheOldQRSession(t *testing.T) {
	s := recoveryFixture(t)
	s.data.LoginSource = "qr"
	if err := s.QueueDiskLogin(); err != nil {
		t.Fatal(err)
	}
	s.snapshotLocked()
	if s.data.LoginSource != "qr" {
		t.Fatal("unverified disk login changed the saved QR session's source")
	}
}
func TestQRCancellationDiscardsLateResult(t *testing.T) {
	s, f := loginFixture(t)
	f.onStart = s.CancelLogin
	if _, err := s.QueueLogin(); err != nil {
		t.Fatal(err)
	}
	s.workLogin(context.Background())
	if s.mode != "offline" || s.login.State != "cancelled" || s.data != nil || s.client != nil || f.state != "cancelled" {
		t.Fatal("cancelled challenge returned to service")
	}
}
func TestQRCheckpointFailureDoesNotEnableMessaging(t *testing.T) {
	s, f := loginFixture(t)
	if _, err := s.QueueLogin(); err != nil {
		t.Fatal(err)
	}
	s.workLogin(context.Background())
	if err := os.Remove(filepath.Join(s.directory, "checkpoints")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.directory, "checkpoints"), []byte("synthetic obstruction"), 0600); err != nil {
		t.Fatal(err)
	}
	s.workLogin(context.Background())
	if s.client != nil || s.state == "connected" || f.auths != 1 || s.login.Error != "checkpoint_save_failed" {
		t.Fatal("unwritable cache enabled messaging", s.login.Error)
	}
}
func TestQRRejectsSwitchingCachedAccount(t *testing.T) {
	s := recoveryFixture(t)
	s.state = "disconnected"
	f := &fakeLogin{candidate: loginCandidate()}
	s.loginFactory = func() (loginEngine, error) { return f, nil }
	s.loginValidate = func(ctx context.Context, c *protocol.Client) (protocol.SyncResult, error) {
		c.State.Confirmed = true
		return protocol.SyncResult{}, nil
	}
	before := s.data.Binding
	if _, err := s.QueueLogin(); err != nil {
		t.Fatal(err)
	}
	s.workLogin(context.Background())
	s.workLogin(context.Background())
	if s.login.Error != "account_scope_changed" || s.data.Binding != before || s.state == "connected" {
		t.Fatal("different QR account replaced a cache")
	}
}

func TestQRSameAccountNewDevicePreservesCacheAndRestarts(t *testing.T) {
	s := recoveryFixture(t)
	s.state, s.mode = "disconnected", "blocked"
	beforeBinding := s.data.Binding
	beforeAPIKey := bytes.Clone(s.data.APIKey)
	beforeSessionID := s.sessionID([]byte("filehelper"))
	cache := store.MediaCache{Directory: filepath.Join(s.directory, "media"), Binding: beforeBinding}
	cacheID := "00112233445566778899aabbccddeeff"
	if err := cache.Save(cacheID, []byte("synthetic cached attachment")); err != nil {
		t.Fatal(err)
	}
	if err := s.checkpoint.Save(s.data); err != nil {
		t.Fatal(err)
	}
	candidate := loginCandidate()
	candidate.Bootstrap.Username = bytes.Clone(s.data.Session.Bootstrap.Username)
	candidate.Bootstrap.UIN = s.data.Session.Bootstrap.UIN
	f := &fakeLogin{candidate: candidate}
	s.loginFactory = func() (loginEngine, error) { return f, nil }
	s.loginValidate = func(ctx context.Context, c *protocol.Client) (protocol.SyncResult, error) {
		c.State.Confirmed = true
		return protocol.SyncResult{}, nil
	}
	if _, err := s.QueueLogin(); err != nil {
		t.Fatal(err)
	}
	s.workLogin(context.Background())
	s.workLogin(context.Background())
	if s.state != "connected" || s.login.Error != "" {
		t.Fatalf("same account QR with independent device rejected: state=%s error=%s", s.state, s.login.Error)
	}
	if s.data.Binding != beforeBinding || !bytes.Equal(s.data.APIKey, beforeAPIKey) || s.sessionID([]byte("filehelper")) != beforeSessionID {
		t.Fatal("same account login lost the existing cache namespace")
	}
	if !bytes.Equal(s.data.Session.Bootstrap.DeviceID, candidate.Bootstrap.DeviceID) || !s.mediaScopeLocked() {
		t.Fatal("active QR device or media scope is invalid")
	}
	loaded, err := s.checkpoint.Load()
	if err != nil || loaded.Binding != beforeBinding || loaded.LoginSource != "qr" || !bytes.Equal(loaded.Session.Bootstrap.DeviceID, candidate.Bootstrap.DeviceID) {
		t.Fatal("new QR session cannot restart from the original cache", err)
	}
	content, err := cache.Load(cacheID)
	if err != nil || string(content) != "synthetic cached attachment" {
		t.Fatal("device rotation invalidated encrypted media", err)
	}
}

func TestQRCacheDeviceRotationStillRejectsDifferentAccountIdentity(t *testing.T) {
	for _, change := range []string{"username", "uin"} {
		t.Run(change, func(t *testing.T) {
			s := recoveryFixture(t)
			beforeBinding := s.data.Binding
			candidate := loginCandidate()
			candidate.Bootstrap.Username = bytes.Clone(s.data.Session.Bootstrap.Username)
			candidate.Bootstrap.UIN = s.data.Session.Bootstrap.UIN
			if change == "username" {
				candidate.Bootstrap.Username = []byte("different-synthetic-owner")
			} else {
				candidate.Bootstrap.UIN++
			}
			candidate.Confirmed = true
			if err := s.adoptLoginLocked(&protocol.Client{Bootstrap: candidate.Bootstrap, State: candidate}, protocol.SyncResult{}); err == nil {
				t.Fatal("different identity adopted")
			}
			if s.data.Binding != beforeBinding || len(s.data.BindingDeviceID) != 0 {
				t.Fatal("rejected account mutated cache")
			}
		})
	}
}

func TestNativeQRPreparationNeedsNoOfficialInstallation(t *testing.T) {
	s := New(t.TempDir(), time.Second, "instance:server")
	first, err := s.nativeLogin()
	if err != nil {
		t.Fatal("QR setup required an installation", err)
	}
	if first.Status().State != "idle" {
		t.Fatal("preparation started a login request")
	}
	saved, err := store.ReadPrivate(filepath.Join(s.directory, "login-device.id"), 128)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.nativeLogin(); err != nil {
		t.Fatal(err)
	}
	again, err := store.ReadPrivate(filepath.Join(s.directory, "login-device.id"), 128)
	if err != nil || !bytes.Equal(saved, again) {
		t.Fatal("QR device rotated", err)
	}
}

func TestQRPreparationRecoversMissingDeviceFileFromCopiedCheckpoint(t *testing.T) {
	original := recoveryFixture(t)
	device, err := protocol.GenerateLoginDevice()
	if err != nil {
		t.Fatal(err)
	}
	original.data.Session.Bootstrap.DeviceID = device
	original.data.Binding = original.data.Session.Bootstrap.Binding()
	original.data.LoginSource = "qr"
	original.data.Session.Profile = protocol.MacCompatibilityProfile()
	original.data.Session.Profile.DeviceType = "UnifiedPCMac 14 arm64"
	saved, _ := protocol.NewClientIdentity(device)
	saved.Computer = "Saved-MacBook"
	saved.TimezoneHours = 0
	original.data.Session.Identity = &saved
	cp := store.Checkpoint{Directory: filepath.Join(original.directory, "checkpoints"), Binding: original.data.Binding}
	if err = cp.Save(original.data); err != nil {
		t.Fatal(err)
	}
	moved := New(original.directory, time.Second, "instance:server")
	if err = moved.prepare(false, false); err != nil {
		t.Fatal(err)
	}
	identity, err := moved.prepareLoginIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if identity.Profile != original.data.Session.Profile || *identity.Runtime != saved {
		t.Fatal("QR refresh replaced saved protocol identity")
	}
	if _, err = moved.nativeLogin(); err != nil {
		t.Fatal(err)
	}
	restored, err := store.ReadPrivate(filepath.Join(moved.directory, "login-device.id"), 128)
	if err != nil || !bytes.Equal(device, restored) {
		t.Fatal("copied QR device was rotated", err)
	}
}
