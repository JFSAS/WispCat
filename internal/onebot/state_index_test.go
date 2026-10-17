package onebot

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JFSAS/wispcat/internal/securedb"
	"github.com/JFSAS/wispcat/pkg/bot"
)

func TestFileMappingStorageFailureIsNotReportedAsMissing(t *testing.T) {
	r, _ := fixture(t)
	id, err := r.State.ID("account-a", "file", "room\x00synthetic-file")
	if err != nil {
		t.Fatal(err)
	}
	if err = r.State.db.Update(func(tx *securedb.Tx) error {
		return tx.Put("references", []byte(jsonNumber(id)), []byte("invalid-reference"))
	}); err != nil {
		t.Fatal(err)
	}
	result := invoke(t, r, "get_image", `{"file":"`+jsonNumber(id)+`"}`)
	if result.Retcode != 1500 || result.Message != "state_unavailable" {
		t.Fatal("storage corruption was reported as an absent file", result)
	}
}

func TestIndexInvalidMetadataCannotResetOrOverwritePublishedIDs(t *testing.T) {
	for _, metadata := range []string{`{"version":2}`, `{"version":2,"next":10000,"ids":0,"cursors":0,"imports":0}`} {
		t.Run(metadata, func(t *testing.T) {
			dir := t.TempDir()
			s, err := OpenState(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			id, err := s.ID("account", "user", "published-person")
			if err != nil {
				t.Fatal(err)
			}
			if err = s.db.Update(func(tx *securedb.Tx) error { return tx.Put("meta", []byte("state"), []byte(metadata)) }); err != nil {
				t.Fatal(err)
			}
			if _, err = s.ID("account", "user", "another-person"); err == nil {
				t.Fatal("bad metadata reused an exposed number")
			}
			ref, ok, err := s.resolveChecked("account", "user", id)
			if err != nil || !ok || ref.Key != "published-person" {
				t.Fatal("exposed reference was overwritten", ref, err)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			if metadata == `{"version":2}` {
				if reopened, err := OpenState(dir); err == nil {
					_ = reopened.Close()
					t.Fatal("incomplete metadata received initialization defaults on restart")
				}
			}
		})
	}
}

func TestStateIndexBeyondLegacyCardinalityAndSnapshotLimit(t *testing.T) {
	directory := t.TempDir()
	s, err := OpenState(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	const total = 100001
	started := time.Now()
	for offset := 0; offset < total; offset += 1000 {
		refs := make([]Identity, min(1000, total-offset))
		for i := range refs {
			refs[i] = Identity{"message", fmt.Sprintf("%080d", offset+i)}
		}
		ids, e := s.IDBatch("synthetic-load-account", refs)
		if e != nil || len(ids) != len(refs) {
			t.Fatal("incremental index hit the old snapshot/cardinality limit", offset, e)
		}
		for i, id := range ids {
			if id != int64(10000+offset+i) {
				t.Fatal("numeric identity collision", offset, i, id)
			}
		}
	}
	stats, err := s.Stats()
	if err != nil || stats.Identities != total || stats.FileBytes <= maxStateBytes {
		t.Fatal("fixture did not exceed both legacy limits", stats, err)
	}
	allocationMillis := time.Since(started).Milliseconds()
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	reopenStart := time.Now()
	reopened, err := OpenState(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for _, index := range []int{0, 999, total - 1} {
		key := fmt.Sprintf("%080d", index)
		id, e := reopened.ID("synthetic-load-account", "message", key)
		if e != nil || id != int64(10000+index) {
			t.Fatal("large index changed IDs after restart", index, id, e)
		}
		if _, ok, e := reopened.resolveChecked("another-account", "message", id); ok || e != nil {
			t.Fatal("large index leaked an account mapping", e)
		}
	}
	report, err := json.Marshal(map[string]any{
		"scope":  "synthetic batched identity allocation, persistence and reopen; not bot throughput or a long-duration soak",
		"system": runtime.GOOS, "architecture": runtime.GOARCH, "go_version": runtime.Version(),
		"identities": total, "batch_size": 1000, "file_bytes": stats.FileBytes,
		"allocation_millis": allocationMillis, "reopen_and_sample_millis": time.Since(reopenStart).Milliseconds(),
		"legacy_cardinality_exceeded": true, "legacy_file_size_exceeded": true,
		"sampled_ids_stable": true, "account_scope_verified": true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("STATE_INDEX_LOAD_REPORT=" + string(report))
}

func TestStateIndexConcurrentAllocationAndBatchValidation(t *testing.T) {
	s, err := OpenState(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var wg sync.WaitGroup
	ids := make(chan int64, 16)
	errors := make(chan error, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, e := s.ID("account", "user", "shared-person")
			ids <- id
			errors <- e
		}()
	}
	wg.Wait()
	close(ids)
	close(errors)
	for e := range errors {
		if e != nil {
			t.Fatal(e)
		}
	}
	for id := range ids {
		if id != 10000 {
			t.Fatal("concurrent lookup allocated another ID", id)
		}
	}
	before, _ := s.Stats()
	if ids, err := s.IDBatch("account", []Identity{{"message", "must-not-publish"}, {"user", ""}}); err == nil || ids != nil {
		t.Fatal("invalid batch partially returned identities", ids, err)
	}
	after, _ := s.Stats()
	if before.Identities != after.Identities || before.NextID != after.NextID {
		t.Fatal("invalid batch changed persistent mappings", before, after)
	}
}

func TestConvertRejectsInvalidMentionBeforeAllocatingIDs(t *testing.T) {
	r, _ := fixture(t)
	before, _ := r.State.Stats()
	_, err := r.Convert(bot.Event{Account: "account-a", ID: "unpublished-event", Sender: bot.User{ID: "friend"}, Conversation: bot.Conversation{ID: "room", Kind: "group"}, Message: []bot.Segment{{Type: "at", Data: map[string]string{"user_id": ""}}}})
	if err == nil {
		t.Fatal("invalid mention accepted")
	}
	after, _ := r.State.Stats()
	if before.Identities != after.Identities {
		t.Fatal("invalid event published partial identity allocations")
	}
}

func TestStateIndexCommittedIDsSurviveKilledProcess(t *testing.T) {
	dir := t.TempDir()
	child := exec.Command(os.Args[0], "-test.run=^TestStateIndexCrashChild$")
	child.Env = append(os.Environ(), "WECHAT_INDEX_CRASH_FIXTURE="+dir)
	stdout, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
	ready := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		if scanner.Scan() {
			ready <- scanner.Text()
		} else {
			ready <- ""
		}
	}()
	select {
	case line := <-ready:
		if line != "committed" {
			t.Fatal("child did not commit", line)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("child did not reach acknowledged commit")
	}
	if err = child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = child.Wait(); err == nil {
		t.Fatal("crash fixture exited cleanly")
	}
	s, err := OpenState(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	id, err := s.ID("account", "message", "acknowledged")
	if err != nil || id != 10000 {
		t.Fatal("acknowledged ID lost after process kill", id, err)
	}
	next, err := s.ID("account", "message", "after-crash")
	if err != nil || next <= id {
		t.Fatal("public ID reused after crash", next, err)
	}
}

func TestStateIndexCrashChild(t *testing.T) {
	directory := os.Getenv("WECHAT_INDEX_CRASH_FIXTURE")
	if directory == "" {
		return
	}
	if !strings.Contains(directory, "TestStateIndexCommittedIDsSurviveKilledProcess") {
		t.Fatal("crash fixture must use its own temporary directory")
	}
	s, err := OpenState(directory)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ID("account", "message", "acknowledged"); err != nil {
		t.Fatal(err)
	}
	fmt.Println("committed")
	for {
		time.Sleep(time.Second)
	}
}
