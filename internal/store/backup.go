package store

import (
	"errors"
	"os"
	"strings"
)

// Backup preserves the exact encrypted checkpoint before a consumer migration.
// Existing backups are never replaced; suffixes do not match Unique's glob.
func (c Checkpoint) Backup(label string) error {
	if label == "" || len(label) > 64 || strings.Trim(label, "abcdefghijklmnopqrstuvwxyz0123456789-") != "" {
		return errors.New("invalid_backup_label")
	}
	path, err := c.path()
	if err != nil {
		return err
	}
	destination := path + ".before-" + label
	if _, err = ReadPrivate(destination, 32<<20); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	raw, err := ReadPrivate(path, 32<<20)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return Atomic(destination, raw)
}
