package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/JFSAS/wispcat/internal/store"
)

func snapshotArchiveFiles(t *testing.T, a *DeliveryArchive) map[string][]byte {
	t.Helper()
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	result := map[string][]byte{}
	for _, name := range []string{"deliveries.db", "deliveries.key", "deliveries.db.ready"} {
		raw, err := os.ReadFile(filepath.Join(a.directory, "delivery-archive", name))
		if err != nil {
			t.Fatal(err)
		}
		result[name] = raw
	}
	return result
}

func TestPendingArchiveBindsGuardFailureBeforeCommitProgress(t *testing.T) {
	for _, rollback := range []bool{false, true} {
		t.Run(map[bool]string{false: "committed-index", true: "old-index-and-queued-checkpoint"}[rollback], func(t *testing.T) {
			a, request := archiveFixture(t)
			delivery, err := a.Submit(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			queued, err := a.Service.checkpoint.Load()
			if err != nil {
				t.Fatal(err)
			}
			before := snapshotArchiveFiles(t, a.Archive)
			a.Archive, err = OpenDeliveryArchive(a.Service.directory)
			if err != nil {
				t.Fatal(err)
			}
			for i, part := range delivery.Parts {
				job := a.Service.data.Jobs[part.ID]
				job.Status, job.ServerID = "accepted", uint64(15000+i)
				if job.Media != nil {
					job.Media.Stage = "completed"
				}
			}
			a.Archive.guardWrite = func([]byte) error { return errors.New("synthetic guard write failure; old guard remains") }
			if _, err = a.ArchiveCompleted(context.Background()); err == nil {
				t.Fatal("guard failure was hidden")
			}
			if _, err = os.Lstat(filepath.Join(a.Service.directory, "delivery-archive.pending")); err != nil {
				t.Fatal("missing precommit archive intent", err)
			}
			if err = a.Archive.Close(); err != nil {
				t.Fatal(err)
			}
			if err = a.Service.checkpoint.Save(queued); err != nil {
				t.Fatal(err)
			}
			if rollback {
				for name, raw := range before {
					if err = os.WriteFile(filepath.Join(a.Service.directory, "delivery-archive", name), raw, 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			reopened, err := OpenDeliveryArchive(a.Service.directory)
			if rollback {
				if err == nil {
					_ = reopened.Close()
					t.Fatal("old guard accepted a combination that could replay acknowledged sends")
				}
				disk, err := a.Service.checkpoint.Load()
				if err != nil || len(disk.Jobs) != 4 {
					t.Fatal("rejected recovery modified the queued checkpoint", err)
				}
				return
			}
			if err != nil {
				t.Fatal("committed index could not finish the failed guard update", err)
			}
			defer reopened.Close()
			if err = reopened.Recover(a.Service.directory); err != nil {
				t.Fatal(err)
			}
			disk, err := a.Service.checkpoint.Load()
			if err != nil || len(disk.Jobs) != 0 {
				t.Fatal("pending commit recovery left replayable jobs", err)
			}
		})
	}
}

func TestPendingArchiveCanFinishTransactionFromDurableTerminalCheckpoint(t *testing.T) {
	a, request := archiveFixture(t)
	acknowledgeArchiveFixture(t, a, request)
	a.Archive.pendingWrite = func(raw []byte) error {
		if err := store.Atomic(filepath.Join(a.Service.directory, "delivery-archive.pending"), raw); err != nil {
			return err
		}
		return errors.New("synthetic failure after intent sync; database transaction rolls back")
	}
	if _, err := a.ArchiveCompleted(context.Background()); err == nil || len(a.Service.data.Jobs) != 4 {
		t.Fatal("interrupted prepare removed active outcomes", err)
	}
	stats, err := a.Archive.Stats(request.Account)
	if err != nil || stats.ArchivedJobs != 0 {
		t.Fatal("fault did not roll back the archive transaction", stats, err)
	}
	if err = a.Archive.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenDeliveryArchive(a.Service.directory)
	if err != nil {
		t.Fatal("terminal checkpoint could not complete prepared archive rows", err)
	}
	defer reopened.Close()
	if err = reopened.Recover(a.Service.directory); err != nil {
		t.Fatal(err)
	}
	stats, err = reopened.Stats(request.Account)
	if err != nil || stats.ArchivedJobs != 4 || stats.ArchivedBatches != 1 {
		t.Fatal("recovered transaction lost or double counted outcomes", stats, err)
	}
	disk, err := a.Service.checkpoint.Load()
	if err != nil || len(disk.Jobs) != 0 {
		t.Fatal("completed archive was not reconciled before startup", err)
	}
}
