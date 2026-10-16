package onebot

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestLiveLegacyImportBatchIsAtomicAndRetainsFingerprints(t *testing.T) {
	root := t.TempDir()
	state, err := OpenState(filepath.Join(root, "global"))
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	id, err := state.ID("active", "user", "owner")
	if err != nil {
		t.Fatal(err)
	}
	first := openLegacyFixture(t, filepath.Join(root, "first"))
	first.data.Next = 20000
	first.ID("first", "user", "owner")
	first.setCursor("first", "consumer", 7)
	second := openLegacyFixture(t, filepath.Join(root, "second"))
	second.ID("second", "user", "owner")
	before, _ := state.Stats()
	if err = state.ImportLegacyDirectories([]string{first.directory, second.directory}); err == nil {
		t.Fatal("collision accepted")
	}
	after, _ := state.Stats()
	if after.Identities != before.Identities || after.Imports != before.Imports || after.NextID != before.NextID {
		t.Fatal("partial batch committed", before, after)
	}
	second.data.Next = 30001
	second.data.IDs = map[string]int64{identityKey("second", "user", "owner"): 30000}
	second.data.Refs = map[string]reference{"30000": {"second", "user", "owner"}}
	if err = second.saveLocked(); err != nil {
		t.Fatal(err)
	}
	if err = state.ImportLegacyDirectories([]string{first.directory, second.directory}); err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		account string
		id      int64
	}{{"active", id}, {"first", 20000}, {"second", 30000}} {
		got, err := state.ID(item.account, "user", "owner")
		if err != nil || got != item.id {
			t.Fatal(item, got, err)
		}
	}
	if cursor, err := state.cursor("first", "consumer", 0); err != nil || cursor != 7 {
		t.Fatal(cursor, err)
	}
	if err = os.Remove(filepath.Join(first.directory, "onebot.key")); err != nil {
		t.Fatal(err)
	}
	if err = state.ImportLegacyDirectories([]string{first.directory}); err != nil {
		t.Fatal("unchanged import required old key", err)
	}
	if err = first.setCursor("first", "consumer", 9); err != nil {
		t.Fatal(err)
	}
	if err = state.ImportLegacyDirectories([]string{first.directory}); err == nil {
		t.Fatal("changed source bypassed fingerprint")
	}
	if got, err := state.ID("active", "user", "owner"); err != nil || got != id {
		t.Fatal(got, id, err)
	}
}

func TestLiveLegacyImportSerializesWithAllocations(t *testing.T) {
	state, err := OpenState(filepath.Join(t.TempDir(), "global"))
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	legacy := openLegacyFixture(t, filepath.Join(t.TempDir(), "legacy"))
	legacy.data.Next = 50000
	legacy.ID("legacy", "user", "owner")
	start := make(chan struct{})
	var work sync.WaitGroup
	failures := make(chan error, 4)
	for worker := range 4 {
		work.Add(1)
		go func(worker int) {
			defer work.Done()
			<-start
			for i := range 50 {
				key := string(rune('a'+worker)) + string(rune(1000+i))
				if _, err := state.ID("active", "user", key); err != nil {
					failures <- err
					return
				}
			}
		}(worker)
	}
	close(start)
	if err = state.ImportLegacyDirectories([]string{legacy.directory}); err != nil {
		t.Fatal(err)
	}
	work.Wait()
	select {
	case err := <-failures:
		t.Fatal(err)
	default:
	}
	seen := map[int64]bool{50000: true}
	for worker := range 4 {
		for i := range 50 {
			key := string(rune('a'+worker)) + string(rune(1000+i))
			id, err := state.ID("active", "user", key)
			if err != nil || seen[id] {
				t.Fatal(id, err)
			}
			seen[id] = true
		}
	}
	if id, err := state.ID("legacy", "user", "owner"); err != nil || id != 50000 {
		t.Fatal(id, err)
	}
}
