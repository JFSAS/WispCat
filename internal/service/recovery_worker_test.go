package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/JFSAS/wispcat/internal/protocol"
	"github.com/JFSAS/wispcat/internal/store"
)

func runRecoveryWorker(t *testing.T, s *Service) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx, "online")
	t.Cleanup(func() { cancel(); s.Wait() })
	return cancel
}

func waitRecoveryState(t *testing.T, s *Service, wanted string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s.Status()["state"] == wanted {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("expected %s, got %v", wanted, s.Status()["state"])
}

func immediateRecoveryWait(context.Context, time.Duration) error { return nil }

func TestRecoveryWorkerCapsFailedAuthenticationRequests(t *testing.T) {
	s := recoveryFixture(t)
	if err := s.checkpoint.Save(s.data); err != nil {
		t.Fatal(err)
	}
	s.recoveryWait = immediateRecoveryWait
	s.syncRPC = func(context.Context, uint32) (protocol.SyncResult, error) {
		return protocol.SyncResult{}, &protocol.Error{Category: "session_expired", Code: -13}
	}
	var attempts atomic.Int32
	s.authenticateRPC = func(context.Context) error { attempts.Add(1); return io.ErrUnexpectedEOF }
	runRecoveryWorker(t, s)
	waitRecoveryState(t, s, "needs_attention")
	status := s.Status()
	if attempts.Load() != authenticationAttemptLimit || status["error_category"] != "authentication_retry_exhausted" || s.data.SessionInvalid {
		t.Fatalf("unbounded or destructive authentication recovery: attempts=%d category=%v", attempts.Load(), status["error_category"])
	}
	time.Sleep(20 * time.Millisecond)
	if attempts.Load() != authenticationAttemptLimit {
		t.Fatal("blocked worker attempted another login")
	}
}

func TestRecoveryWorkerRetriesOnlySyncWithoutLoggingInAgain(t *testing.T) {
	s := recoveryFixture(t)
	if err := s.checkpoint.Save(s.data); err != nil {
		t.Fatal(err)
	}
	s.interval, s.recoveryWait = time.Millisecond, immediateRecoveryWait
	var syncs atomic.Int32
	s.syncRPC = func(ctx context.Context, _ uint32) (protocol.SyncResult, error) {
		switch syncs.Add(1) {
		case 1:
			return protocol.SyncResult{}, nil
		case 2:
			return protocol.SyncResult{}, protocol.ErrWire
		case 3:
			return protocol.SyncResult{Messages: historyMessages(s, 1)}, nil
		default:
			<-ctx.Done()
			return protocol.SyncResult{}, ctx.Err()
		}
	}
	runRecoveryWorker(t, s)
	deadline := time.Now().Add(3 * time.Second)
	for syncs.Load() < 4 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	status := s.Status()
	if syncs.Load() != 4 || status["state"] != "connected" || status["auth_calls"] != uint64(0) {
		t.Fatalf("transient sync error did not recover without reauthentication: %v", status["state"])
	}
	loaded, err := s.checkpoint.Load()
	if err != nil || len(loaded.Messages) != 1 {
		t.Fatal("recovered message was not durable", err)
	}
}

func TestRecoveryWorkerRetriesPendingSaveBeforeAnotherNetworkCall(t *testing.T) {
	for _, exhausted := range []bool{false, true} {
		t.Run(map[bool]string{false: "recovered", true: "exhausted"}[exhausted], func(t *testing.T) {
			a, h := historyFixture(t)
			s := a.Service
			s.interval = time.Millisecond
			var syncs, saves atomic.Int32
			s.syncRPC = func(ctx context.Context, _ uint32) (protocol.SyncResult, error) {
				if syncs.Add(1) == 1 {
					return protocol.SyncResult{Messages: historyMessages(s, 1)}, nil
				}
				<-ctx.Done()
				return protocol.SyncResult{}, ctx.Err()
			}
			s.checkpointWrite = func(data *store.Data) error {
				if syncs.Load() == 1 && (saves.Add(1) <= 2 || exhausted) {
					return &os.PathError{Op: "write", Path: "PRIVATE_PATH_AND_CREDENTIAL", Err: syscall.ENOSPC}
				}
				return s.checkpoint.Save(data)
			}
			s.recoveryWait = func(ctx context.Context, delay time.Duration) error {
				if syncs.Load() != 1 || s.Status()["auth_calls"] != uint64(0) {
					t.Error("new network work started before pending save committed")
				}
				old, err := s.checkpoint.Load()
				if err != nil || len(old.Messages) != 0 {
					t.Error("durable checkpoint advanced despite failed save", err)
				}
				if exhausted && delay == storageProbeInterval {
					<-ctx.Done()
					return ctx.Err()
				}
				return nil
			}
			var reports []failureReport
			s.failureSink = func(report failureReport) { reports = append(reports, report) }
			runRecoveryWorker(t, s)
			if exhausted {
				waitRecoveryState(t, s, "storage_paused")
				if saves.Load() != saveRetryLimit+1 || syncs.Load() != 1 || s.Status()["error_category"] != "storage_unavailable" {
					t.Fatal("save retries escaped their bound or fetched another batch")
				}
			} else {
				deadline := time.Now().Add(3 * time.Second)
				for syncs.Load() < 2 && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
				if syncs.Load() != 2 || saves.Load() != 3 || s.Status()["state"] != "connected" {
					t.Fatal("save did not recover before resuming sync")
				}
				loaded, err := s.checkpoint.Load()
				if err != nil || len(loaded.Messages) != 1 {
					t.Fatal("pending batch was lost", err)
				}
				stats, err := h.Stats(s.data.Binding)
				if err != nil || stats.Messages != 1 {
					t.Fatal("save retry duplicated history", err)
				}
			}
			s.mu.Lock()
			raw, _ := json.Marshal(reports)
			s.mu.Unlock()
			if strings.Contains(string(raw), "PRIVATE_PATH_AND_CREDENTIAL") || !strings.Contains(string(raw), "checkpoint") || !strings.Contains(string(raw), "storage_full") {
				t.Fatal("unsafe or insufficient save diagnostics", string(raw))
			}
		})
	}
}

func TestSaveRecoveryCanBeCancelledByOfflineMode(t *testing.T) {
	s := recoveryFixture(t)
	s.mode = "online"
	epoch := s.modeEpoch
	var saves int
	s.checkpointWrite = func(*store.Data) error { saves++; return syscall.ENOSPC }
	s.recoveryWait = func(context.Context, time.Duration) error { return s.SetMode("offline", false) }
	err := s.retrySave(context.Background(), epoch, storageFailure("checkpoint", syscall.ENOSPC))
	if !errors.Is(err, errRecoveryInterrupted) || saves != 0 || s.Status()["mode"] != "offline" {
		t.Fatal("save retry ignored explicit offline operation")
	}
}

func TestFailedSendJournalRemainsQueuedBeforeNetworkDispatch(t *testing.T) {
	s := recoveryFixture(t)
	s.data.Jobs = map[string]*store.Job{"synthetic-job": {Status: "queued", ClientID: 1, Recipient: "synthetic-friend", Content: "synthetic text"}}
	s.checkpointWrite = func(*store.Data) error { return syscall.ENOSPC }
	err := s.sendQueued(context.Background())
	if classifyFailure(err, "send").class != "storage" || s.data.Jobs["synthetic-job"].Status != "queued" || s.data.Jobs["synthetic-job"].Attempts != 0 || s.calls["send"] != 0 {
		t.Fatal("journal failure stranded or dispatched a message before its state was durable")
	}
}

func TestFailedSyncPreservesCursorForRetry(t *testing.T) {
	s := recoveryFixture(t)
	s.client.State.Cursor = []byte("synthetic-saved-cursor")
	s.syncRPC = func(context.Context, uint32) (protocol.SyncResult, error) {
		s.client.State.Cursor = nil
		s.client.State.Sequence++
		return protocol.SyncResult{}, protocol.ErrWire
	}
	if _, err := s.sync(context.Background(), 2); !errors.Is(err, protocol.ErrWire) || string(s.client.State.Cursor) != "synthetic-saved-cursor" || s.client.State.Sequence != 1 {
		t.Fatal("failed response lost its saved cursor or reused a request sequence")
	}
}

func TestPermanentProtocolErrorsAreNotRetried(t *testing.T) {
	for _, err := range []error{&protocol.Error{Category: "session_header_mismatch"}, &protocol.Error{Category: "account_scope_changed"}, errors.New("authenticated payload rejected")} {
		s := recoveryFixture(t)
		s.connection.Stage = "sync"
		if _, retry := s.recoverFailure(err, &sessionRecovery{}); retry {
			t.Fatal("identity or integrity failure was retried")
		}
	}
}

func TestFailureDiagnosticsDoNotRetainRawErrors(t *testing.T) {
	s := recoveryFixture(t)
	s.operation = "sync"
	var reports []failureReport
	s.failureSink = func(report failureReport) { reports = append(reports, report) }
	_, _ = s.recoverFailure(errors.New("PRIVATE_RESPONSE_CREDENTIAL_BODY"), &sessionRecovery{})
	raw, _ := json.Marshal(reports)
	if strings.Contains(string(raw), "PRIVATE_RESPONSE_CREDENTIAL_BODY") || !strings.Contains(string(raw), "unexpected_runtime_error") || !strings.Contains(string(raw), "blocked") {
		t.Fatal("raw error leaked or no diagnostic was emitted")
	}
}
