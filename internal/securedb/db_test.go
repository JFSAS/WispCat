package securedb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func TestConcurrentFirstOpenCannotReplaceWinningKey(t *testing.T) {
	dir := t.TempDir()
	var group sync.WaitGroup
	start := make(chan struct{})
	opened := make(chan *DB, 2)
	for range 2 {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			db, _ := Open(dir, "fixture")
			opened <- db
		}()
	}
	close(start)
	group.Wait()
	close(opened)
	var winner *DB
	for db := range opened {
		if db != nil {
			if winner != nil {
				_ = db.Close()
				t.Fatal("two exclusive owners opened the same index")
			}
			winner = db
		}
	}
	if winner == nil {
		t.Fatal("neither first opener acquired the index")
	}
	key, err := os.ReadFile(filepath.Join(dir, "fixture.key"))
	if err != nil || !bytes.Equal(key, winner.key) {
		t.Fatal("losing opener replaced the active encryption key", err)
	}
	if err = winner.Update(func(tx *Tx) error { return tx.Put("records", []byte("committed"), []byte("keep")) }); err != nil {
		t.Fatal(err)
	}
	if err = winner.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir, "fixture")
	if err != nil {
		t.Fatal("concurrent first open made the index unrecoverable", err)
	}
	defer reopened.Close()
	if err = reopened.View(func(tx *Tx) error {
		value, e := tx.Get("records", []byte("committed"))
		if e == nil && string(value) != "keep" {
			return errors.New("committed record disappeared")
		}
		return e
	}); err != nil {
		t.Fatal(err)
	}
}

func TestEncryptedIndexRollbackRestartAndPrivateLookup(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	key := db.Key("identity", "synthetic-private-account", "synthetic-private-person")
	if bytes.Equal(key, db.Key("identity", "synthetic-private-account\x00synthetic-private-person")) {
		t.Fatal("ambiguous composite keys")
	}
	if err = db.Update(func(tx *Tx) error { return tx.Put("records", key, []byte("synthetic-private-content")) }); err != nil {
		t.Fatal(err)
	}
	rollback := errors.New("injected_rollback")
	if err = db.Update(func(tx *Tx) error {
		if e := tx.Put("records", []byte("discard"), []byte("uncommitted")); e != nil {
			return e
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "fixture.db"))
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"synthetic-private-account", "synthetic-private-person", "synthetic-private-content"} {
		if bytes.Contains(raw, []byte(secret)) {
			t.Fatal("private value or lookup key stored in plaintext", secret)
		}
	}
	reopened, err := Open(dir, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	err = reopened.View(func(tx *Tx) error {
		value, e := tx.Get("records", key)
		if e != nil || string(value) != "synthetic-private-content" {
			t.Fatal(string(value), e)
		}
		value, e = tx.Get("records", []byte("discard"))
		if e != nil || value != nil {
			t.Fatal("rolled-back record survived", string(value), e)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestEncryptedIndexRejectsRecordSubstitution(t *testing.T) {
	db, err := Open(t.TempDir(), "fixture")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Update(func(tx *Tx) error { return tx.Put("records", []byte("source"), []byte("private-value")) }); err != nil {
		t.Fatal(err)
	}
	if err = db.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("records"))
		return bucket.Put([]byte("target"), bytes.Clone(bucket.Get([]byte("source"))))
	}); err != nil {
		t.Fatal(err)
	}
	if err = db.View(func(tx *Tx) error { _, e := tx.Get("records", []byte("target")); return e }); err == nil {
		t.Fatal("ciphertext could be moved to a different lookup identity")
	}
}

func TestEncryptedIndexMissingFilesDoNotResetState(t *testing.T) {
	for _, name := range []string{"fixture.db", "fixture.key"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			db, err := Open(dir, "fixture")
			if err != nil {
				t.Fatal(err)
			}
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, name)
			if err = os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if reopened, err := Open(dir, "fixture"); err == nil {
				_ = reopened.Close()
				t.Fatal("missing state was silently recreated")
			}
			if _, err = os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("missing private file was replaced", err)
			}
		})
	}
}

func TestEncryptedIndexRejectsSymlinkAndBroadPermissions(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(t.TempDir(), "unrelated")
	if err := os.WriteFile(target, []byte("do-not-change"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(dir, "fixture.db")); err != nil {
		t.Fatal(err)
	}
	if db, err := Open(dir, "fixture"); err == nil {
		_ = db.Close()
		t.Fatal("index followed a symlink")
	}
	raw, _ := os.ReadFile(target)
	if string(raw) != "do-not-change" {
		t.Fatal("unrelated file modified")
	}
	dir = t.TempDir()
	db, err := Open(dir, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	if err = os.Chmod(filepath.Join(dir, "fixture.db"), 0644); err != nil {
		t.Fatal(err)
	}
	if db, err = Open(dir, "fixture"); err == nil {
		_ = db.Close()
		t.Fatal("index accepted broad file permissions")
	}
}

func TestEncryptedPrefixScanBoundariesAndLimit(t *testing.T) {
	db, err := Open(t.TempDir(), "fixture")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Update(func(tx *Tx) error {
		for _, key := range []string{"prefix-0", "prefix-1", "prefix-2", "unrelated"} {
			if e := tx.Put("range", []byte(key), []byte("value-"+key)); e != nil {
				return e
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var keys []string
	if err = db.View(func(tx *Tx) error {
		return tx.Scan("range", []byte("prefix-"), []byte("prefix-1"), 2, func(key, value []byte) (bool, error) {
			if string(value) != "value-"+string(key) {
				t.Fatal("range value lost authentication/context")
			}
			keys = append(keys, string(key))
			return true, nil
		})
	}); err != nil || len(keys) != 2 || keys[0] != "prefix-1" || keys[1] != "prefix-2" {
		t.Fatal("scan crossed a prefix or lower boundary", keys, err)
	}
	if err = db.View(func(tx *Tx) error {
		return tx.Scan("range", []byte("prefix-"), []byte("prefix-"), 1, func([]byte, []byte) (bool, error) { return true, nil })
	}); err == nil {
		t.Fatal("scan limit did not bound traversal")
	}
}

func TestEncryptedWholeBucketMigrationPagesPreserveEveryRow(t *testing.T) {
	db, err := Open(t.TempDir(), "fixture")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Update(func(tx *Tx) error {
		for i := 0; i < 301; i++ {
			key := make([]byte, 2)
			binary.BigEndian.PutUint16(key, uint16(i))
			if err := tx.Put("migration", key, key); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var after []byte
	seen := map[uint16]bool{}
	for {
		count := 0
		err = db.View(func(tx *Tx) error {
			return tx.ScanAll("migration", after, 64, func(key, value []byte) (bool, error) {
				id := binary.BigEndian.Uint16(key)
				if seen[id] || !bytes.Equal(key, value) {
					t.Fatal("page replayed or corrupted a row", id)
				}
				seen[id] = true
				after = append(bytes.Clone(key), 0)
				count++
				return count < 64, nil
			})
		})
		if err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			break
		}
	}
	if len(seen) != 301 {
		t.Fatal("migration skipped a binary key range", len(seen))
	}
}
