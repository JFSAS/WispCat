package onebot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestSharedStatePreservesPriorImportsWithoutLegacyKeys(t *testing.T) {
	root := t.TempDir()
	instanceDir := filepath.Join(root, "instance")
	instance := openLegacyFixture(t, instanceDir)
	id, err := instance.ID("account-a", "user", "owner")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(instanceDir, "onebot.state"))
	if err != nil {
		t.Fatal(err)
	}
	pathHash := sha256.Sum256([]byte(instanceDir))
	contentHash := sha256.Sum256(raw)
	globalDir := filepath.Join(root, "global")
	global := openLegacyFixture(t, globalDir)
	global.data.Next = instance.data.Next
	global.data.IDs, global.data.Refs = instance.data.IDs, instance.data.Refs
	global.data.Imports = map[string]string{hex.EncodeToString(pathHash[:]): hex.EncodeToString(contentHash[:])}
	if err = global.saveLocked(); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(filepath.Join(instanceDir, "onebot.key")); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		state, err := OpenSharedState(globalDir, []string{instanceDir})
		if err != nil {
			t.Fatal("an already-imported unchanged source required its old key", err)
		}
		got, err := state.ID("account-a", "user", "owner")
		_ = state.Close()
		if err != nil || got != id {
			t.Fatal(got, id, err)
		}
	}
	if err = instance.setCursor("account-a", "bot", 9); err != nil {
		t.Fatal(err)
	}
	if state, err := OpenSharedState(globalDir, []string{instanceDir}); err == nil {
		_ = state.Close()
		t.Fatal("changed imported ciphertext bypassed the fingerprint fence")
	}
}

func TestSharedStateRootAndInstancesMigrateAtomically(t *testing.T) {
	root := t.TempDir()
	globalDir, instanceDir := filepath.Join(root, "global"), filepath.Join(root, "instance")
	global := openLegacyFixture(t, globalDir)
	instance := openLegacyFixture(t, instanceDir)
	global.ID("account-a", "user", "owner")
	instance.ID("account-b", "user", "owner")
	if state, err := OpenSharedState(globalDir, []string{instanceDir}); err == nil {
		_ = state.Close()
		t.Fatal("root/instance collision was accepted")
	}
	// Repair the old source after the failed first attempt. Its new fingerprint
	// must remain importable because no application migration was committed.
	if err := global.setCursor("account-a", "bot", 5); err != nil {
		t.Fatal(err)
	}
	instance.data.Next = 20001
	instance.data.IDs = map[string]int64{identityKey("account-b", "user", "owner"): 20000}
	instance.data.Refs = map[string]reference{"20000": {"account-b", "user", "owner"}}
	if err := instance.saveLocked(); err != nil {
		t.Fatal(err)
	}
	state, err := OpenSharedState(globalDir, []string{instanceDir})
	if err != nil {
		t.Fatal("failed import froze the old root fingerprint", err)
	}
	defer state.Close()
	a, _ := state.ID("account-a", "user", "owner")
	b, _ := state.ID("account-b", "user", "owner")
	cursor, err := state.cursor("account-a", "bot", 0)
	if a != 10000 || b != 20000 || cursor != 5 || err != nil {
		t.Fatal("repaired migration lost an identity or cursor", a, b, cursor, err)
	}
}

func TestSharedStateMigratesNearLimitSnapshotWithoutRewritingIt(t *testing.T) {
	root := t.TempDir()
	legacyDir := filepath.Join(root, "legacy")
	legacy := openLegacyFixture(t, legacyDir)
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
	if err := legacy.saveLocked(); err != nil {
		t.Fatal("near-limit legacy fixture was not readable", err)
	}
	if _, _, err := readLegacy(legacyDir); err != nil {
		t.Fatal("valid near-limit legacy state cannot reopen", err)
	}
	sharedDir := filepath.Join(root, "shared")
	shared, err := OpenSharedState(sharedDir, []string{legacyDir})
	if err != nil {
		t.Fatal("incremental index could not import a readable legacy snapshot", err)
	}
	stats, err := shared.Stats()
	if err != nil || stats.Identities != uint64(len(legacy.data.IDs)) {
		t.Fatal(stats, err)
	}
	if err = shared.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSharedState(sharedDir, []string{legacyDir})
	if err != nil {
		t.Fatal("migrated index could not reopen", err)
	}
	defer reopened.Close()
	if _, _, err = readLegacy(legacyDir); err != nil {
		t.Fatal("migration damaged the legacy snapshot", err)
	}
}

func TestSharedStatePreservesLegacyIDsAndConsumerProgress(t *testing.T) {
	root := t.TempDir()
	legacyDir := filepath.Join(root, "legacy")
	legacy := openLegacyFixture(t, legacyDir)
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
	cursor, cursorErr := shared.cursor("account-a", "bot", 0)
	if same != id || different == id || cursor != 17 || cursorErr != nil {
		t.Fatal("legacy identity/progress changed or self IDs collided")
	}
	after, _ := os.ReadFile(filepath.Join(legacyDir, "onebot.state"))
	if string(before) != string(after) {
		t.Fatal("migration modified legacy state")
	}
	if err = shared.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSharedState(globalDir, []string{legacyDir})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	stable, _ := reopened.ID("account-b", "user", "owner")
	if stable != different {
		t.Fatal("shared IDs changed after restart")
	}
}
func TestSharedStateRejectsConflictingLegacyIDsWithoutRenumbering(t *testing.T) {
	root := t.TempDir()
	aDir, bDir := filepath.Join(root, "a"), filepath.Join(root, "b")
	a := openLegacyFixture(t, aDir)
	b := openLegacyFixture(t, bDir)
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
	defer state.Close()
	stats, err := state.Stats()
	if err != nil || stats.Identities != 0 {
		t.Fatal("failed import partially published mappings")
	}
	if id, _ := a.ID("account-a", "user", "owner"); id != aID {
		t.Fatal("legacy public ID changed")
	}
}
func TestSharedStateRejectsLegacyWritesAfterMigration(t *testing.T) {
	root := t.TempDir()
	legacyDir := filepath.Join(root, "legacy")
	legacy := openLegacyFixture(t, legacyDir)
	legacy.ID("account-a", "user", "owner")
	global := filepath.Join(root, "global")
	state, err := OpenSharedState(global, []string{legacyDir})
	if err != nil {
		t.Fatal(err)
	}
	if err = state.Close(); err != nil {
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
	if err = state.Close(); err != nil {
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
	defer shared.Close()
	stats, err := shared.Stats()
	if err != nil || stats.Imports != 0 {
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
