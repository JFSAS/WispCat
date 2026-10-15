package store

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMediaCacheAuthenticatedBindingJobAndPrivateFiles(t *testing.T) {
	cache := MediaCache{Directory: filepath.Join(t.TempDir(), "media"), Binding: strings.Repeat("a", 64)}
	id := strings.Repeat("b", 32)
	data := []byte("synthetic private downloaded document body which must never be stored in plaintext")
	if err := cache.Save(id, data); err != nil {
		t.Fatal(err)
	}
	path, keyPath, _ := cache.paths(id)
	encrypted, err := os.ReadFile(path)
	if err != nil || bytes.Contains(encrypted, data) {
		t.Fatal("media was not sealed", err)
	}
	for _, file := range []string{path, keyPath} {
		stat, err := os.Stat(file)
		if err != nil || stat.Mode().Perm() != 0600 {
			t.Fatal("media file permissions are not private", err)
		}
	}
	stat, err := os.Stat(cache.Directory)
	if err != nil || stat.Mode().Perm() != 0700 {
		t.Fatal("media directory permissions are not private", err)
	}
	loaded, err := cache.Load(id)
	if err != nil || !bytes.Equal(loaded, data) {
		t.Fatal("valid media cannot be opened", err)
	}
	if err := cache.Save(id, data); err != nil {
		t.Fatal(err)
	}
	resealed, _ := os.ReadFile(path)
	if bytes.Equal(resealed, encrypted) {
		t.Fatal("media encryption reused the nonce")
	}
	otherID := strings.Repeat("c", 32)
	if err := os.WriteFile(filepath.Join(cache.Directory, otherID+".enc"), resealed, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Load(otherID); !errors.Is(err, ErrMediaCache) {
		t.Fatal("renaming encrypted media bypasses job AAD", err)
	}
	other := MediaCache{Directory: cache.Directory, Binding: strings.Repeat("d", 64)}
	key, _ := os.ReadFile(keyPath)
	if err := os.WriteFile(filepath.Join(other.Directory, other.Binding+".key"), key, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := other.Load(id); !errors.Is(err, ErrMediaCache) {
		t.Fatal("copied key bypasses account AAD", err)
	}
	resealed[len(resealed)-1] ^= 1
	os.WriteFile(path, resealed, 0600)
	if _, err := cache.Load(id); !errors.Is(err, ErrMediaCache) {
		t.Fatal("tampered media was accepted", err)
	}
}

func TestMediaCacheRefusesTraversalSymlinksAndUnsafeKey(t *testing.T) {
	cache := MediaCache{Directory: filepath.Join(t.TempDir(), "media"), Binding: strings.Repeat("a", 64)}
	if err := cache.Save("../outside", []byte("data")); !errors.Is(err, ErrMediaCache) {
		t.Fatal("invalid job ID becomes a path", err)
	}
	id := strings.Repeat("b", 32)
	if err := Directory(cache.Directory); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "synthetic-outside")
	if err := os.WriteFile(outside, bytes.Repeat([]byte{42}, 32), 0600); err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(cache.Directory, cache.Binding+".key")
	if err := os.Symlink(outside, keyPath); err != nil {
		t.Fatal(err)
	}
	if err := cache.Save(id, []byte("data")); !errors.Is(err, ErrMediaCache) {
		t.Fatal("symlink key accepted", err)
	}
	os.Remove(keyPath)
	os.WriteFile(keyPath, bytes.Repeat([]byte{42}, 32), 0644)
	if err := cache.Save(id, []byte("data")); !errors.Is(err, ErrMediaCache) {
		t.Fatal("public key file accepted", err)
	}
}

func TestCheckpointRestoresMediaInterruptedWithoutFakeSuccess(t *testing.T) {
	checkpoint, data := syntheticCheckpoint(t)
	queuedID, activeID := strings.Repeat("a", 32), strings.Repeat("b", 32)
	newJob := func(status string) *MediaJob {
		return &MediaJob{Binding: data.Binding, SessionID: strings.Repeat("c", 32), MessageID: strings.Repeat("d", 64), Variant: "regular", Kind: "image", Name: "image", Status: status}
	}
	data.MediaJobs = map[string]*MediaJob{queuedID: newJob("queued"), activeID: newJob("downloading")}
	data.SessionInvalid = true
	if err := checkpoint.Save(data); err != nil {
		t.Fatal(err)
	}
	loaded, err := checkpoint.Load()
	if err != nil || !loaded.SessionInvalid || loaded.MediaJobs[queuedID].Status != "queued" || loaded.MediaJobs[activeID].Status != "failed" || loaded.MediaJobs[activeID].Error != "media_interrupted" {
		t.Fatal("media recovery replayed unknown work or dropped session invalidation", err)
	}
	data.MediaJobs[queuedID].Binding = strings.Repeat("f", 64)
	if err := checkpoint.Save(data); err != nil {
		t.Fatal(err)
	}
	if _, err := checkpoint.Load(); err == nil {
		t.Fatal("cross-account media checkpoint task accepted")
	}
}
