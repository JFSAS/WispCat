package store

import (
	"crypto/rand"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func readKey(path string) ([]byte, error) {
	key, err := ReadPrivate(path, 32)
	if err != nil {
		return nil, err
	}
	if len(key) != 32 {
		return nil, errors.New("local checkpoint key must contain 32 bytes")
	}
	return key, nil
}

// key never accesses the OS credential store. Each account has a private
// 32-byte key beside its checkpoint, and a lost key is never replaced silently.
func (c Checkpoint) key(create bool) ([]byte, error) {
	checkpointPath, err := c.path()
	if err != nil {
		return nil, err
	}
	if err = Directory(c.Directory); err != nil {
		return nil, err
	}
	path := strings.TrimSuffix(checkpointPath, ".checkpoint") + ".key"
	key, err := readKey(path)
	if err == nil {
		return key, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if !create {
		return nil, ErrKeyMissing
	}
	if _, err = os.Lstat(checkpointPath); err == nil {
		return nil, ErrKeyMissing
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	key = make([]byte, 32)
	if _, err = rand.Read(key); err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(c.Directory, ".key-*")
	if err != nil {
		return nil, err
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(0600); err != nil {
		f.Close()
		return nil, err
	}
	n, err := f.Write(key)
	if err == nil && n != len(key) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	// Publish only a complete file, without replacing a concurrent creator's key.
	if err = os.Link(f.Name(), path); errors.Is(err, os.ErrExist) {
		return readKey(path)
	} else if err != nil {
		return nil, err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	if err = directory.Sync(); err != nil {
		return nil, err
	}
	return key, nil
}
