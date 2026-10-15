package onebot

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestSharedStateRejectsMigrationBeyondReadableSnapshotSize(t *testing.T) {
	root := t.TempDir()
	legacyDir := filepath.Join(root, "legacy")
	legacy, err := OpenState(legacyDir)
	if err != nil {
		t.Fatal(err)
	}
	// Build valid, bounded identity keys close to the encrypted read limit.
	// Track encoded insertion sizes instead of writing 2,000 full snapshots.
	raw, _ := json.Marshal(legacy.data)
	size := len(raw)
	target := maxStateBytes - legacy.aead.NonceSize() - legacy.aead.Overhead() - 100
	for index := 0; ; index++ {
		id := legacy.data.Next
		prefix := fmt.Sprintf("person-%05d-", index)
		key := prefix
		ref := reference{"account-a", "user", key}
		ids, _ := json.Marshal(map[string]int64{identityKey(ref.Account, ref.Kind, ref.Key): id})
		refs, _ := json.Marshal(map[string]reference{strconv.FormatInt(id, 10): ref})
		cost := len(ids) + len(refs) - 2
		if index == 0 {
			cost -= 2 // No commas are needed for the first entries.
		}
		length := min(8192-len(prefix), (target-size-cost)/2)
		if length < 0 {
			break
		}
		key += strings.Repeat("x", length)
		ref.Key = key
		legacy.data.IDs[identityKey(ref.Account, ref.Kind, key)] = id
		legacy.data.Refs[strconv.FormatInt(id, 10)] = ref
		legacy.data.Next++
		size += cost + length*2
	}
	if err = legacy.saveLocked(); err != nil {
		t.Fatal("near-limit legacy fixture was not readable", err)
	}
	if _, err = OpenState(legacyDir); err != nil {
		t.Fatal("valid near-limit legacy state cannot reopen", err)
	}
	sharedDir := filepath.Join(root, "shared")
	if _, err = OpenSharedState(sharedDir, []string{legacyDir}); err == nil || err.Error() != "onebot_state_size_limit" {
		t.Fatal("migration published a snapshot too large to reopen", err)
	}
	shared, err := OpenState(sharedDir)
	if err != nil || len(shared.data.IDs) != 0 {
		t.Fatal("rejected migration partially published mappings", err)
	}
	if _, err = OpenState(legacyDir); err != nil {
		t.Fatal("failed migration damaged legacy state", err)
	}
}

func TestSharedStatePreservesLegacyIDsAndConsumerProgress(t *testing.T) {
	root := t.TempDir()
	legacyDir := filepath.Join(root, "legacy")
	legacy, err := OpenState(legacyDir)
	if err != nil {
		t.Fatal(err)
	}
	id, err := legacy.ID("account-a", "user", "owner")
	if err != nil {
		t.Fatal(err)
	}
	if err = legacy.setCursor("account-a", "bot", 17); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(legacyDir, "onebot.state"))
	if err != nil {
		t.Fatal(err)
	}
	globalDir := filepath.Join(root, "global")
	shared, err := OpenSharedState(globalDir, []string{legacyDir})
	if err != nil {
		t.Fatal(err)
	}
	same, _ := shared.ID("account-a", "user", "owner")
	different, _ := shared.ID("account-b", "user", "owner")
	if same != id || different == id || shared.cursor("account-a", "bot", 0) != 17 {
		t.Fatal("legacy identity/progress changed or self IDs collided")
	}
	after, _ := os.ReadFile(filepath.Join(legacyDir, "onebot.state"))
	if string(before) != string(after) {
		t.Fatal("migration modified legacy state")
	}
	reopened, err := OpenSharedState(globalDir, []string{legacyDir})
	if err != nil {
		t.Fatal(err)
	}
	stable, _ := reopened.ID("account-b", "user", "owner")
	if stable != different {
		t.Fatal("shared IDs changed after restart")
	}
}
func TestSharedStateRejectsConflictingLegacyIDsWithoutRenumbering(t *testing.T) {
	root := t.TempDir()
	aDir, bDir := filepath.Join(root, "a"), filepath.Join(root, "b")
	a, _ := OpenState(aDir)
	b, _ := OpenState(bDir)
	aID, _ := a.ID("account-a", "user", "owner")
	bID, _ := b.ID("account-b", "user", "owner")
	if aID != bID {
		t.Fatal("fixture did not recreate independent-state collision")
	}
	global := filepath.Join(root, "global")
	if _, err := OpenSharedState(global, []string{aDir, bDir}); err == nil {
		t.Fatal("conflicting public IDs silently renumbered")
	}
	state, err := OpenState(global)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.data.IDs) != 0 {
		t.Fatal("failed import partially published mappings")
	}
	if id, _ := a.ID("account-a", "user", "owner"); id != aID {
		t.Fatal("legacy public ID changed")
	}
}
func TestSharedStateRejectsLegacyWritesAfterMigration(t *testing.T) {
	root := t.TempDir()
	legacyDir := filepath.Join(root, "legacy")
	legacy, _ := OpenState(legacyDir)
	legacy.ID("account-a", "user", "owner")
	global := filepath.Join(root, "global")
	if _, err := OpenSharedState(global, []string{legacyDir}); err != nil {
		t.Fatal(err)
	}
	legacy.ID("account-a", "user", "new-person")
	if _, err := OpenSharedState(global, []string{legacyDir}); err == nil {
		t.Fatal("downgraded legacy writer bypassed migration fence")
	}
}
func TestSharedStateSkipsItsOwnRelativeDirectory(t *testing.T) {
	root := t.TempDir()
	state, err := OpenState(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = state.ID("account-a", "user", "owner"); err != nil {
		t.Fatal(err)
	}
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	relative, err := filepath.Rel(cwd, root)
	if err != nil {
		t.Fatal(err)
	}
	shared, err := OpenSharedState(relative, []string{root})
	if err != nil {
		t.Fatal(err)
	}
	if len(shared.data.Imports) != 0 {
		t.Fatal("shared state imported itself and would fence its own later writes")
	}
}
func TestObservedMemberInfoDoesNotInventAuthorityOrCompleteRoster(t *testing.T) {
	r, c := fixture(t)
	group, _ := r.State.ID("account-a", "group", "room")
	user, _ := r.State.ID("account-a", "user", "friend")
	q := `{"group_id":` + jsonNumber(group) + `,"user_id":` + jsonNumber(user) + `}`
	result := invoke(t, r, "get_group_member_info", q)
	if result.Retcode != 0 {
		t.Fatal(result)
	}
	data := result.Data.(map[string]any)
	if _, invented := data["role"]; invented || data["wechat_role_known"] != false || data["wechat_roster_complete"] != false {
		t.Fatal("cache query invented authoritative membership", data)
	}
	c.mu.Lock()
	c.snap.Account = "account-b"
	c.mu.Unlock()
	if result = invoke(t, r, "get_group_member_info", q); result.Retcode == 0 {
		t.Fatal("member lookup crossed account scope")
	}
}

func TestExplicitSelfIDFencesAccountStatus(t *testing.T) {
	r, c := fixture(t)
	// Use the actual snapshot identity; the fake's raw ID is deliberately opaque.
	self, err := r.State.ID(c.Snapshot().Account, "user", c.Snapshot().Self.ID)
	if err != nil {
		t.Fatal(err)
	}
	if result := invoke(t, r, "get_status", `{"self_id":`+jsonNumber(self)+`}`); result.Retcode != 0 {
		t.Fatal(result)
	}
	for _, q := range []string{`{"self_id":1}`, `{"self_id":"invalid"}`} {
		if result := invoke(t, r, "get_status", q); result.Retcode == 0 {
			t.Fatal("wrong or invalid self_id returned another account's status", q, result)
		}
	}
	c.mu.Lock()
	c.snap.Account = ""
	c.mu.Unlock()
	if result := invoke(t, r, "get_status", `{}`); result.Retcode != 0 {
		t.Fatal("unscoped offline status must remain available", result)
	}
	if result := invoke(t, r, "get_status", `{"self_id":`+jsonNumber(self)+`}`); result.Retcode == 0 {
		t.Fatal("unavailable account accepted stale self_id", result)
	}
}
