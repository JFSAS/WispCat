package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"testing"

	"github.com/JFSAS/wechat-runtime/internal/protocol"
)

func syntheticData(username string) *Data {
	bootstrap := protocol.Bootstrap{
		Username: []byte(username), UIN: 12345,
		DeviceID: []byte("synthetic-device"), AAK: []byte("synthetic-ticket"),
	}
	return &Data{
		Schema: 1, Binding: bootstrap.Binding(), SourceApp: "/synthetic/WeChat.app",
		APIKey: bytes.Repeat([]byte{1}, 32), NextClientID: 42,
		Session: &protocol.Session{
			Bootstrap: bootstrap, Original: bytes.Repeat([]byte{2}, 16),
			ClientKey: bytes.Repeat([]byte{3}, 24), ServerKey: bytes.Repeat([]byte{4}, 24),
		},
		Jobs: map[string]*Job{"synthetic-job": {
			Recipient: "synthetic-peer", Content: "synthetic text", Status: "accepted",
		}},
		Messages: []protocol.Message{{Sender: []byte(username), Recipient: []byte("synthetic-peer"), Content: []byte("synthetic text"), Type: 1}},
	}
}

func syntheticCheckpoint(t *testing.T) (Checkpoint, *Data) {
	t.Helper()
	data := syntheticData("synthetic-owner")
	return Checkpoint{Directory: filepath.Join(t.TempDir(), "checkpoints"), Binding: data.Binding}, data
}

func checkpointFiles(c Checkpoint) (string, string) {
	return filepath.Join(c.Directory, c.Binding+".checkpoint"), filepath.Join(c.Directory, c.Binding+".key")
}

func TestCheckpointRoundTrip(t *testing.T) {
	c, data := syntheticCheckpoint(t)
	if err := c.Save(data); err != nil {
		t.Fatal(err)
	}
	checkpointPath, keyPath := checkpointFiles(c)
	key, err := os.ReadFile(keyPath)
	if err != nil || len(key) != 32 {
		t.Fatal("missing 32-byte local key", err)
	}
	for path, mode := range map[string]os.FileMode{c.Directory: 0700, keyPath: 0600, checkpointPath: 0600} {
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != mode {
			t.Fatal("incorrect private permissions", err)
		}
	}
	sealed, err := os.ReadFile(checkpointPath)
	if err != nil || !bytes.HasPrefix(sealed, []byte(Magic)) || bytes.Contains(sealed, []byte("synthetic text")) {
		t.Fatal("checkpoint is not sealed with the new format", err)
	}
	restored, err := c.Load()
	if err != nil || !reflect.DeepEqual(restored, data) {
		t.Fatal("restart changed checkpoint state", err)
	}
	data.NextClientID++
	if err = c.Save(data); err != nil {
		t.Fatal(err)
	}
	again, err := os.ReadFile(keyPath)
	if err != nil || !bytes.Equal(key, again) {
		t.Fatal("save unexpectedly rotated the local key", err)
	}
}

func TestCheckpointCacheIdentityRemainsBoundAcrossQRDeviceRotation(t *testing.T) {
	c, data := syntheticCheckpoint(t)
	data.BindingDeviceID = bytes.Clone(data.Session.Bootstrap.DeviceID[:16])
	data.Session.Bootstrap.DeviceID = []byte("i00112233445566778899aabbccddeeff")
	data.LoginSource = "qr"
	if !data.ValidBinding() || data.Binding == data.Session.Bootstrap.Binding() {
		t.Fatal("cache and live device identities were not separated")
	}
	if err := c.Save(data); err != nil {
		t.Fatal(err)
	}
	loaded, err := c.Load()
	if err != nil || !loaded.ValidBinding() {
		t.Fatal("rotated device checkpoint cannot load", err)
	}
	for _, change := range []string{"username", "uin", "cache-device", "cache-device-length"} {
		t.Run(change, func(t *testing.T) {
			copyData := *loaded
			copySession := *loaded.Session
			copyData.Session = &copySession
			copyData.BindingDeviceID = bytes.Clone(loaded.BindingDeviceID)
			switch change {
			case "username":
				copySession.Bootstrap.Username = []byte("other-owner")
			case "uin":
				copySession.Bootstrap.UIN++
			case "cache-device":
				copyData.BindingDeviceID[0] ^= 1
			case "cache-device-length":
				copyData.BindingDeviceID = copyData.BindingDeviceID[:15]
			}
			if copyData.ValidBinding() || c.Save(&copyData) == nil {
				t.Fatal("changed cache identity accepted")
			}
		})
	}
}

func TestCheckpointTamper(t *testing.T) {
	c, data := syntheticCheckpoint(t)
	if err := c.Save(data); err != nil {
		t.Fatal(err)
	}
	path, _ := checkpointFiles(c)
	sealed, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sealed[len(sealed)-1] ^= 1
	if err = os.WriteFile(path, sealed, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Load(); err == nil {
		t.Fatal("modified checkpoint was accepted")
	}
}

func TestCheckpointMissingKeyNeverRotates(t *testing.T) {
	c, data := syntheticCheckpoint(t)
	if err := c.Save(data); err != nil {
		t.Fatal(err)
	}
	path, keyPath := checkpointFiles(c)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(keyPath); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Load(); !errors.Is(err, ErrKeyMissing) {
		t.Fatal("missing key was not reported", err)
	}
	if err = c.Save(data); !errors.Is(err, ErrKeyMissing) {
		t.Fatal("lost key was replaced", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(original, after) {
		t.Fatal("unreadable checkpoint was overwritten", err)
	}
	if _, err = os.Lstat(keyPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("replacement key appeared", err)
	}
}

func TestCheckpointRejectsInvalidKey(t *testing.T) {
	for _, length := range []int{0, 31, 33} {
		t.Run(strconv.Itoa(length), func(t *testing.T) {
			c, data := syntheticCheckpoint(t)
			if err := c.Save(data); err != nil {
				t.Fatal(err)
			}
			_, path := checkpointFiles(c)
			invalid := make([]byte, length)
			if err := os.WriteFile(path, invalid, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Load(); err == nil {
				t.Fatal("invalid key was accepted")
			}
			if err := c.Save(data); err == nil {
				t.Fatal("invalid key was replaced")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(invalid, after) {
				t.Fatal("invalid key was modified", err)
			}
		})
	}
}

func TestCheckpointRejectsInsecureKey(t *testing.T) {
	c, data := syntheticCheckpoint(t)
	if err := c.Save(data); err != nil {
		t.Fatal(err)
	}
	_, path := checkpointFiles(c)
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Load(); err == nil {
		t.Fatal("world-readable key was accepted")
	}
	if err := c.Save(data); err == nil {
		t.Fatal("world-readable key was used for saving")
	}
}

func TestCheckpointRejectsKeySymlink(t *testing.T) {
	c, data := syntheticCheckpoint(t)
	if err := c.Save(data); err != nil {
		t.Fatal(err)
	}
	_, path := checkpointFiles(c)
	target := filepath.Join(t.TempDir(), "key-target")
	if err := os.Rename(path, target); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Load(); err == nil {
		t.Fatal("key symlink was followed")
	}
	if err := c.Save(data); err == nil {
		t.Fatal("key symlink was used for saving")
	}
}

func TestLegacyCheckpointPreserved(t *testing.T) {
	c, data := syntheticCheckpoint(t)
	path, keyPath := checkpointFiles(c)
	legacy := append([]byte(legacyMagic), make([]byte, 28)...)
	if err := Atomic(path, legacy); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Load(); !errors.Is(err, ErrLegacyCheckpoint) {
		t.Fatal("legacy checkpoint was not recognized", err)
	}
	if err := c.Save(data); !errors.Is(err, ErrLegacyCheckpoint) {
		t.Fatal("legacy checkpoint was overwritten", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(legacy, after) {
		t.Fatal("legacy bytes changed", err)
	}
	if _, err = os.Lstat(keyPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("a replacement key was generated for legacy data", err)
	}
}

func TestKeyCreationConcurrent(t *testing.T) {
	c, _ := syntheticCheckpoint(t)
	var group sync.WaitGroup
	keys := make(chan []byte, 16)
	errorsFound := make(chan error, 16)
	for range 16 {
		group.Add(1)
		go func() {
			defer group.Done()
			key, err := c.key(true)
			if err != nil {
				errorsFound <- err
				return
			}
			keys <- key
		}()
	}
	group.Wait()
	close(keys)
	close(errorsFound)
	for err := range errorsFound {
		t.Fatal(err)
	}
	var first []byte
	for key := range keys {
		if first == nil {
			first = key
		} else if !bytes.Equal(first, key) {
			t.Fatal("concurrent creators received different keys")
		}
	}
	if len(first) != 32 {
		t.Fatal("no key was created")
	}
}

func TestCheckpointAccountIsolation(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "checkpoints")
	one, two := syntheticData("synthetic-owner-one"), syntheticData("synthetic-owner-two")
	a := Checkpoint{directory, one.Binding}
	b := Checkpoint{directory, two.Binding}
	if err := a.Save(one); err != nil {
		t.Fatal(err)
	}
	if err := b.Save(two); err != nil {
		t.Fatal(err)
	}
	aPath, aKey := checkpointFiles(a)
	bPath, bKey := checkpointFiles(b)
	aBytes, err := os.ReadFile(aKey)
	if err != nil {
		t.Fatal(err)
	}
	bBytes, err := os.ReadFile(bKey)
	if err != nil || bytes.Equal(aBytes, bBytes) {
		t.Fatal("accounts share a local key", err)
	}
	sealed, err := os.ReadFile(aPath)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(bPath, sealed, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = b.Load(); err == nil {
		t.Fatal("another account's checkpoint was accepted")
	}
}

func TestCheckpointPreservesOpaqueLegacyJournal(t *testing.T) {
	c, data := syntheticCheckpoint(t)
	legacy := &CodexState{Threads: map[string]string{"synthetic-peer": "synthetic-thread"}, Jobs: map[string]*CodexJob{
		"pending": {Peer: "synthetic-peer", Input: "synthetic input", Status: "pending"},
		"running": {Peer: "synthetic-peer", Input: "synthetic input", Status: "running", TurnID: "synthetic-turn"},
		"ready":   {Peer: "synthetic-peer", Input: "synthetic input", Status: "ready", Reply: "synthetic reply"},
	}}
	data.LegacyCodex, _ = json.Marshal(legacy)
	if err := c.Save(data); err != nil {
		t.Fatal(err)
	}
	loaded, err := c.Load()
	if err != nil {
		t.Fatal(err)
	}
	var restored CodexState
	if err := json.Unmarshal(loaded.LegacyCodex, &restored); err != nil {
		t.Fatal(err)
	}
	if restored.Threads["synthetic-peer"] != "synthetic-thread" || restored.Jobs["pending"].Status != "pending" || restored.Jobs["ready"].Reply != "synthetic reply" || restored.Jobs["running"].Status != "running" {
		t.Fatal("opaque legacy state changed; recovery belongs to the reply scheduler")
	}
}
