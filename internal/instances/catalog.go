// Package instances owns private state namespaces below one application root.
package instances

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/JFSAS/wechat-runtime/internal/account"
	"github.com/JFSAS/wechat-runtime/internal/store"
)

type Profile struct {
	ID        string `json:"id"`
	App       string `json:"wechat_app"`
	Directory string `json:"directory"`
}
type Catalog struct{ Root string }

func profile(root, app string) (Profile, error) {
	app, err := NormalizeKey(app)
	if err != nil {
		return Profile{}, err
	}
	hash := sha256.Sum256([]byte(app))
	name := DisplayName(app)
	name = strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' {
			return r
		}
		return '-'
	}, name)
	if len([]rune(name)) > 32 {
		name = string([]rune(name)[:32])
	}
	suffix := hex.EncodeToString(hash[:8])
	id := "wechat-" + suffix
	return Profile{ID: id, App: app, Directory: filepath.Join(root, "instances", name+"-"+suffix)}, nil
}

// LastApp reads only the selection marker. The legacy checkpoint is inspected
// only before the first migration, to recover the original installation.
func LastApp(root, fallback string) (string, error) {
	raw, err := store.ReadPrivate(filepath.Join(root, "active-instance.json"), 8192)
	if err == nil {
		var marker struct {
			App string `json:"wechat_app"`
		}
		if json.Unmarshal(raw, &marker) != nil {
			return "", errors.New("instance_selection_invalid")
		}
		return NormalizeKey(marker.App)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	cp, exists, err := legacyCheckpoint(root)
	if err != nil {
		return "", err
	}
	if exists {
		data, err := cp.Load()
		if err != nil {
			return "", err
		}
		if data.SourceApp != "" {
			return NormalizeKey(data.SourceApp)
		}
	}
	return NormalizeKey(fallback)
}

func Open(root, fallback string) (*Catalog, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err = store.Directory(root); err != nil {
		return nil, err
	}
	if err = store.Directory(filepath.Join(root, "instances")); err != nil {
		return nil, err
	}
	c := &Catalog{Root: root}
	if _, err := os.Lstat(filepath.Join(root, "active-instance.json")); err == nil {
		if _, err := LastApp(root, fallback); err != nil {
			return nil, err
		}
		return c, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	// Copy, never move, legacy state. Publishing the completed directory is
	// atomic; an interrupted copy cannot become an active partial profile.
	cp, exists, err := legacyCheckpoint(root)
	if err != nil {
		return nil, err
	}
	if exists {
		data, err := cp.Load()
		if err != nil {
			return nil, err
		}
		app := data.SourceApp
		if app == "" {
			app = fallback
		}
		if _, err = c.ensure(app, true); err != nil {
			return nil, err
		}
	}
	return c, nil
}
func (c *Catalog) Ensure(app string) (Profile, error) { return c.ensure(app, false) }
func (c *Catalog) ensure(app string, legacy bool) (Profile, error) {
	p, err := profile(c.Root, app)
	if err != nil {
		return p, err
	}
	if info, err := os.Lstat(p.Directory); err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return p, errors.New("instance_directory_invalid")
		}
		raw, err := store.ReadPrivate(filepath.Join(p.Directory, "instance.json"), 8192)
		var existing Profile
		if err != nil || json.Unmarshal(raw, &existing) != nil || existing.ID != p.ID || existing.App != p.App {
			return p, errors.New("instance_metadata_invalid")
		}
		return p, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return p, err
	}
	staging, err := os.MkdirTemp(filepath.Join(c.Root, "instances"), ".creating-")
	if err != nil {
		return p, err
	}
	defer os.RemoveAll(staging)
	if legacy {
		for _, name := range []string{"checkpoints", "media", "login-device.id", "runtime", "codex-workspace"} {
			if err := copyPrivate(filepath.Join(c.Root, name), filepath.Join(staging, name)); err != nil {
				return p, err
			}
		}
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return p, err
	}
	if err = store.Atomic(filepath.Join(staging, "instance.json"), raw); err != nil {
		return p, err
	}
	if err = os.Rename(staging, p.Directory); err != nil {
		return p, err
	}
	parent, err := os.Open(filepath.Dir(p.Directory))
	if err != nil {
		return p, err
	}
	defer parent.Close()
	return p, parent.Sync()
}
func (c *Catalog) Activate(p Profile) error {
	expected, err := profile(c.Root, p.App)
	if err != nil || expected != p {
		return errors.New("instance_metadata_invalid")
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return store.Atomic(filepath.Join(c.Root, "active-instance.json"), raw)
}
func (c *Catalog) List() ([]Profile, error) {
	entries, err := os.ReadDir(filepath.Join(c.Root, "instances"))
	if err != nil {
		return nil, err
	}
	result := []Profile{}
	for _, entry := range entries {
		if !entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		raw, err := store.ReadPrivate(filepath.Join(c.Root, "instances", entry.Name(), "instance.json"), 8192)
		if err != nil {
			return nil, err
		}
		var p Profile
		if json.Unmarshal(raw, &p) != nil {
			return nil, errors.New("instance_metadata_invalid")
		}
		expected, err := profile(c.Root, p.App)
		if err != nil || (p.ID != expected.ID || p.App != expected.App) || entry.Name() != filepath.Base(expected.Directory) {
			return nil, errors.New("instance_metadata_invalid")
		}
		result = append(result, expected)
	}
	return result, nil
}
func copyPrivate(source, destination string) error {
	info, err := os.Lstat(source)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return errors.New("legacy_state_contains_symlink")
	}
	if info.IsDir() {
		if err = store.Directory(destination); err != nil {
			return err
		}
		entries, err := os.ReadDir(source)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err = copyPrivate(filepath.Join(source, entry.Name()), filepath.Join(destination, entry.Name())); err != nil {
				return err
			}
		}
		d, err := os.Open(destination)
		if err != nil {
			return err
		}
		defer d.Close()
		return d.Sync()
	}
	if !info.Mode().IsRegular() {
		return errors.New("legacy_state_contains_special_file")
	}
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	if err == nil {
		err = out.Sync()
	}
	closeErr := out.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func legacyCheckpoint(root string) (store.Checkpoint, bool, error) {
	path := filepath.Join(root, "checkpoints")
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return store.Checkpoint{}, false, nil
	} else if err != nil {
		return store.Checkpoint{}, false, err
	}
	return store.Unique(path)
}

// Logical instances can restore a copied cache or log in by QR without an
// official installation. Existing App-path keys remain stable for migration.
func NormalizeKey(key string) (string, error) {
	if !strings.HasPrefix(key, "instance:") {
		return account.NormalizeAppPath(key)
	}
	name := strings.TrimPrefix(key, "instance:")
	if len([]rune(name)) < 1 || len([]rune(name)) > 64 {
		return "", errors.New("invalid_instance_name")
	}
	for _, r := range name {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '-' && r != '_' && r != '.' {
			return "", errors.New("invalid_instance_name")
		}
	}
	if name == "." || name == ".." {
		return "", errors.New("invalid_instance_name")
	}
	return key, nil
}
func DisplayName(key string) string {
	if strings.HasPrefix(key, "instance:") {
		return strings.TrimPrefix(key, "instance:")
	}
	return strings.TrimSuffix(filepath.Base(key), filepath.Ext(key))
}
