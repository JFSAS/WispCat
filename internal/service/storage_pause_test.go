package service

import (
	"context"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/JFSAS/wispcat/internal/protocol"
	"github.com/JFSAS/wispcat/internal/store"
)

func TestStoragePauseResumesSamePendingBatchWithoutLogin(t *testing.T) {
	a, history := historyFixture(t)
	s := a.Service
	s.interval = time.Millisecond
	var syncs, saves atomic.Int32
	var writable atomic.Bool
	probe := make(chan struct{}, 1)
	release := make(chan struct{}, 1)
	s.syncRPC = func(ctx context.Context, _ uint32) (protocol.SyncResult, error) {
		if syncs.Add(1) == 1 {
			return protocol.SyncResult{Messages: historyMessages(s, 1)}, nil
		}
		<-ctx.Done()
		return protocol.SyncResult{}, ctx.Err()
	}
	s.checkpointWrite = func(data *store.Data) error {
		saves.Add(1)
		if !writable.Load() {
			return syscall.ENOSPC
		}
		return s.checkpoint.Save(data)
	}
	s.recoveryWait = func(ctx context.Context, delay time.Duration) error {
		if delay == time.Minute {
			select {
			case probe <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	runRecoveryWorker(t, s)
	select {
	case <-probe:
	case <-time.After(2 * time.Second):
		t.Fatal("exhausted save retries never scheduled a local storage probe")
	}
	status := s.Status()
	connection := status["connection"].(connectionAttempt)
	if status["state"] != "storage_paused" || status["mode"] != "online" || syncs.Load() != 1 || saves.Load() != 4 || status["auth_calls"] != uint64(0) {
		t.Fatal("storage pause discarded the pending operation or started network work")
	}
	if connection.Failure == nil || connection.Failure.NextRetryAt < time.Now().Add(50*time.Second).Unix() {
		t.Fatal("storage pause does not expose its next automatic save attempt")
	}
	// Let one slow save fail too. The next minute must schedule another local
	// save without cycling through login, fresh Sync or the fast retry burst.
	release <- struct{}{}
	select {
	case <-probe:
	case <-time.After(2 * time.Second):
		t.Fatal("a failed storage probe permanently stopped the worker")
	}
	if saves.Load() != 5 || syncs.Load() != 1 || s.Status()["auth_calls"] != uint64(0) {
		t.Fatal("storage probes restarted network work or reset the fast retry budget")
	}
	epoch := s.modeEpoch
	writable.Store(true)
	release <- struct{}{}
	deadline := time.Now().Add(2 * time.Second)
	for syncs.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if syncs.Load() != 2 || s.Status()["state"] != "connected" || s.Status()["auth_calls"] != uint64(0) || s.modeEpoch != epoch {
		t.Fatal("writable storage did not resume the existing worker automatically")
	}
	loaded, err := s.checkpoint.Load()
	if err != nil || len(loaded.Messages) != 1 {
		t.Fatal("pending batch was not saved", err)
	}
	stats, err := history.Stats(loaded.Binding)
	if err != nil || stats.Messages != 1 {
		t.Fatal("storage recovery duplicated or lost history", err)
	}
}

func TestStoragePauseAfterAuthenticationUsesFreshRPC(t *testing.T) {
	s := recoveryFixture(t)
	if err := s.checkpoint.Save(s.data); err != nil {
		t.Fatal(err)
	}
	s.interval = time.Millisecond
	var syncs, auth atomic.Int32
	var writable atomic.Bool
	s.syncRPC = func(ctx context.Context, _ uint32) (protocol.SyncResult, error) {
		switch syncs.Add(1) {
		case 1:
			return protocol.SyncResult{}, &protocol.Error{Category: "session_expired", Code: -13}
		case 2:
			if ctx.Err() != nil {
				t.Error("resumed Sync reused the expired authentication RPC context")
			}
			return protocol.SyncResult{}, nil
		default:
			<-ctx.Done()
			return protocol.SyncResult{}, ctx.Err()
		}
	}
	s.authenticateRPC = func(context.Context) error { auth.Add(1); return nil }
	s.checkpointWrite = func(data *store.Data) error {
		if !writable.Load() {
			return syscall.ENOSPC
		}
		return s.checkpoint.Save(data)
	}
	s.recoveryWait = func(_ context.Context, delay time.Duration) error {
		if delay == time.Minute {
			s.mu.RLock()
			cancelRPC := s.cancelRPC
			s.mu.RUnlock()
			if cancelRPC != nil {
				t.Error("authentication RPC remained active during local storage pause")
				cancelRPC() // Model its deadline expiring during a long disk outage.
			}
			writable.Store(true)
		}
		return nil
	}
	runRecoveryWorker(t, s)
	deadline := time.Now().Add(3 * time.Second)
	for syncs.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if syncs.Load() != 3 || auth.Load() != 1 || s.Status()["state"] != "connected" {
		t.Fatal("save recovery restarted authentication or failed to resume")
	}
}

func TestStoragePauseWaitHonorsOfflineAndShutdown(t *testing.T) {
	for _, offline := range []bool{false, true} {
		t.Run(map[bool]string{true: "offline", false: "shutdown"}[offline], func(t *testing.T) {
			s := recoveryFixture(t)
			if err := s.checkpoint.Save(s.data); err != nil {
				t.Fatal(err)
			}
			s.syncRPC = func(context.Context, uint32) (protocol.SyncResult, error) { return protocol.SyncResult{}, nil }
			s.checkpointWrite = func(*store.Data) error { return syscall.ENOSPC }
			waiting := make(chan struct{})
			s.recoveryWait = func(ctx context.Context, delay time.Duration) error {
				if delay != time.Minute {
					return nil
				}
				// Use the real timer/wake path for the long pause, with no fake
				// cancellation behavior hiding a missed manual-mode signal.
				s.recoveryWait = nil
				s.mu.RLock()
				epoch := s.modeEpoch
				s.mu.RUnlock()
				close(waiting)
				return s.waitRecovery(ctx, delay, epoch)
			}
			cancel := runRecoveryWorker(t, s)
			select {
			case <-waiting:
			case <-time.After(2 * time.Second):
				t.Fatal("no storage pause")
			}
			if offline {
				if err := s.SetMode("offline", false); err != nil {
					t.Fatal(err)
				}
				waitRecoveryState(t, s, "offline")
			} else {
				cancel()
				select {
				case <-s.done:
				case <-time.After(time.Second):
					t.Fatal("shutdown waited for the storage timer")
				}
			}
		})
	}
}
