package onebot

import (
	"errors"
	"path/filepath"

	"github.com/JFSAS/wispcat/internal/securedb"
)

// ImportLegacyDirectories reserves mappings from newly enabled local instances
// without reopening the shared index or renumbering existing public IDs. The
// whole batch commits in one write transaction, serialized with live allocation.
func (s *State) ImportLegacyDirectories(directories []string) error {
	if len(directories) > 16 {
		return errors.New("legacy_import_batch_limit")
	}
	sources := make([]string, 0, len(directories))
	seen := map[string]bool{}
	for _, directory := range directories {
		if directory == "" {
			return errors.New("legacy_import_directory_required")
		}
		absolute, err := filepath.Abs(directory)
		if err != nil {
			return err
		}
		if !seen[absolute] {
			sources = append(sources, absolute)
			seen[absolute] = true
		}
	}
	return s.db.Update(func(tx *securedb.Tx) error {
		meta, err := readMeta(tx)
		if err != nil {
			return err
		}
		for _, source := range sources {
			if err = s.importLegacy(tx, &meta, source, false); err != nil {
				return err
			}
		}
		return putMeta(tx, meta)
	})
}
