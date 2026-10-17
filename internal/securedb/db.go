// Package securedb stores encrypted records in a synchronous transactional index.
// Keys derived from private identities are HMACs, never plaintext identifiers.
package securedb

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/JFSAS/wispcat/internal/store"
	bolt "go.etcd.io/bbolt"
)

const domain = "wechat-runtime/securedb/v1"
const maxRecordBytes = 1 << 20

type DB struct {
	db   *bolt.DB
	key  []byte
	aead cipher.AEAD
	path string
}
type Tx struct {
	tx *bolt.Tx
	db *DB
}

func Open(directory, name string) (*DB, error) {
	if name == "" || strings.Trim(name, "abcdefghijklmnopqrstuvwxyz0123456789-") != "" {
		return nil, errors.New("invalid_index_name")
	}
	if err := store.Directory(directory); err != nil {
		return nil, err
	}
	// Serialize key creation before bbolt opens/locks the data file. A losing
	// opener must never replace the key already used by a successful opener.
	initialization, err := openPrivate(filepath.Join(directory, name+".db.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	defer initialization.Close()
	deadline := time.Now().Add(time.Second)
	for {
		err = syscall.Flock(int(initialization.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			break
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) || time.Now().After(deadline) {
			return nil, errors.New("index_initialization_locked")
		}
		time.Sleep(10 * time.Millisecond)
	}
	defer syscall.Flock(int(initialization.Fd()), syscall.LOCK_UN)
	readyPath := filepath.Join(directory, name+".db.ready")
	ready, readyErr := store.ReadPrivate(readyPath, 128)
	if readyErr != nil && !errors.Is(readyErr, os.ErrNotExist) {
		return nil, readyErr
	}
	if readyErr == nil {
		if string(ready) != domain {
			return nil, errors.New("index_marker_invalid")
		}
		if _, e := os.Lstat(filepath.Join(directory, name+".db")); e != nil {
			return nil, errors.New("index_missing_preserve_original_files")
		}
	}
	keyPath := filepath.Join(directory, name+".key")
	key, err := store.ReadPrivate(keyPath, 32)
	if errors.Is(err, os.ErrNotExist) {
		for _, extension := range []string{".db", ".state", ".db.ready"} {
			if _, e := os.Lstat(filepath.Join(directory, name+extension)); !errors.Is(e, os.ErrNotExist) {
				return nil, errors.New("index_key_missing_for_existing_state")
			}
		}
		key = make([]byte, 32)
		if _, err = rand.Read(key); err != nil {
			return nil, err
		}
		if err = store.Atomic(keyPath, key); err != nil {
			return nil, err
		}
	}
	if err != nil || len(key) != 32 {
		return nil, errors.New("index_key_invalid")
	}
	derive := hmac.New(sha256.New, key)
	_, _ = derive.Write([]byte(domain + "/encryption"))
	block, err := aes.NewCipher(derive.Sum(nil))
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(directory, name+".db")
	raw, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second, OpenFile: openPrivate})
	if err != nil {
		return nil, err
	}
	db := &DB{db: raw, key: key, aead: aead, path: path}
	err = db.Update(func(tx *Tx) error {
		value, e := tx.Get("_format", []byte("version"))
		if e != nil {
			return e
		}
		if value != nil {
			if string(value) != domain {
				return errors.New("index_format_invalid")
			}
			return nil
		}
		if readyErr == nil {
			return errors.New("index_format_missing")
		}
		// An interrupted first transaction leaves no buckets. Missing metadata
		// from a nonempty index must never be treated as a new database.
		if e = tx.tx.ForEach(func([]byte, *bolt.Bucket) error { return errors.New("index_format_missing") }); e != nil {
			return e
		}
		return tx.Put("_format", []byte("version"), []byte(domain))
	})
	if err == nil {
		parent, e := os.Open(directory)
		if e != nil {
			err = e
		} else {
			err = parent.Sync()
			_ = parent.Close()
		}
	}
	if err == nil && errors.Is(readyErr, os.ErrNotExist) {
		err = store.Atomic(readyPath, []byte(domain))
	}
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	return db, nil
}

func openPrivate(path string, flags int, mode os.FileMode) (*os.File, error) {
	fd, err := syscall.Open(path, flags|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, uint32(mode.Perm()))
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	info, err := f.Stat()
	if err == nil && (!info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0) {
		err = errors.New("index_file_permissions_invalid")
	}
	if err == nil {
		if stat, ok := info.Sys().(*syscall.Stat_t); !ok || stat.Uid != uint32(os.Getuid()) {
			err = errors.New("index_file_owner_invalid")
		}
	}
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

func (db *DB) Close() error { return db.db.Close() }
func (db *DB) View(f func(*Tx) error) error {
	return db.db.View(func(tx *bolt.Tx) error { return f(&Tx{tx: tx, db: db}) })
}
func (db *DB) Update(f func(*Tx) error) error {
	return db.db.Update(func(tx *bolt.Tx) error { return f(&Tx{tx: tx, db: db}) })
}
func (db *DB) Size() (int64, error) {
	info, err := os.Stat(db.path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// Key uses length prefixes so embedded NULs in file/message keys cannot create
// ambiguous composite identities. The database never stores these input parts.
func (db *DB) Key(parts ...string) []byte {
	mac := hmac.New(sha256.New, db.key)
	_, _ = mac.Write([]byte(domain + "/lookup"))
	for _, part := range parts {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(part)))
		_, _ = mac.Write(length[:])
		_, _ = mac.Write([]byte(part))
	}
	return mac.Sum(nil)
}

func (tx *Tx) Get(bucket string, key []byte) ([]byte, error) {
	b := tx.tx.Bucket([]byte(bucket))
	if b == nil {
		return nil, nil
	}
	raw := b.Get(key)
	if raw == nil {
		return nil, nil
	}
	if len(raw) < tx.db.aead.NonceSize()+tx.db.aead.Overhead() || len(raw) > maxRecordBytes+64 {
		return nil, errors.New("index_record_invalid")
	}
	plain, err := tx.db.aead.Open(nil, raw[:tx.db.aead.NonceSize()], raw[tx.db.aead.NonceSize():], recordAAD(bucket, key))
	if err != nil {
		return nil, errors.New("index_record_authentication_failed")
	}
	return plain, nil
}
func (tx *Tx) HasRecords(bucket string) bool {
	b := tx.tx.Bucket([]byte(bucket))
	if b == nil {
		return false
	}
	key, _ := b.Cursor().First()
	return key != nil
}

// Scan visits a bounded ordered prefix range inside the current transaction.
// Keys and decrypted values belong to the caller, not to the database mmap.
// Returning false stops the scan; exhausting limit before stopping is an error.
func (tx *Tx) Scan(bucket string, prefix, start []byte, limit int, visit func([]byte, []byte) (bool, error)) error {
	if len(prefix) == 0 || len(start) < len(prefix) || !bytes.HasPrefix(start, prefix) || limit < 1 || limit > 10000 {
		return errors.New("index_scan_invalid")
	}
	return tx.scan(bucket, prefix, start, limit, visit)
}

// ScanAll visits a bounded page of a whole bucket for incremental migrations.
// Callers stop explicitly at their page boundary and persist an exclusive cursor.
func (tx *Tx) ScanAll(bucket string, start []byte, limit int, visit func([]byte, []byte) (bool, error)) error {
	if limit < 1 || limit > 10000 {
		return errors.New("index_scan_invalid")
	}
	return tx.scan(bucket, nil, start, limit, visit)
}

func (tx *Tx) scan(bucket string, prefix, start []byte, limit int, visit func([]byte, []byte) (bool, error)) error {
	b := tx.tx.Bucket([]byte(bucket))
	if b == nil {
		return nil
	}
	cursor := b.Cursor()
	count := 0
	for key, _ := cursor.Seek(start); key != nil && bytes.HasPrefix(key, prefix); key, _ = cursor.Next() {
		if count == limit {
			return errors.New("index_scan_limit")
		}
		count++
		value, err := tx.Get(bucket, key)
		if err != nil {
			return err
		}
		more, err := visit(bytes.Clone(key), value)
		if err != nil || !more {
			return err
		}
	}
	return nil
}
func (tx *Tx) Put(bucket string, key, value []byte) error {
	if bucket == "" || len(bucket) > 64 || len(key) == 0 || len(key) > 128 || len(value) > maxRecordBytes {
		return errors.New("index_record_size_invalid")
	}
	nonce := make([]byte, tx.db.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	raw := append(nonce, tx.db.aead.Seal(nil, nonce, value, recordAAD(bucket, key))...)
	b, err := tx.tx.CreateBucketIfNotExists([]byte(bucket))
	if err != nil {
		return err
	}
	return b.Put(key, raw)
}

// Delete removes one index record inside its caller's transaction. The caller
// remains responsible for committing replacement metadata and related indexes.
func (tx *Tx) Delete(bucket string, key []byte) error {
	if bucket == "" || len(bucket) > 64 || len(key) == 0 || len(key) > 128 {
		return errors.New("index_record_size_invalid")
	}
	b := tx.tx.Bucket([]byte(bucket))
	if b == nil {
		return nil
	}
	return b.Delete(key)
}

func recordAAD(bucket string, key []byte) []byte {
	return append([]byte(domain+"\x00"+bucket+"\x00"), key...)
}
