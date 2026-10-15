// Package store maintains encrypted checkpoints and private local key files.
package store

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/JFSAS/wechat-runtime/internal/messaging"
	"github.com/JFSAS/wechat-runtime/internal/protocol"
)

const Magic = "REA-WX-STATE\x02"
const legacyMagic = "REA-WX-STATE\x01"

var (
	ErrLegacyCheckpoint = errors.New("checkpoint uses a legacy Keychain key; preserve it and use a new checkpoint directory")
	ErrKeyMissing       = errors.New("local checkpoint key is missing; restore the original key file")
)

type Job struct {
	Origin             string         `json:"origin,omitempty"`
	DispatchScope      string         `json:"dispatch_scope,omitempty"`
	DispatchGeneration uint64         `json:"dispatch_generation,omitempty"`
	Source             string         `json:"source,omitempty"`
	GroupAuto          bool           `json:"group_auto,omitempty"`
	GroupTriggerAt     uint32         `json:"group_trigger_at,omitempty"`
	Kind               string         `json:"kind,omitempty"`
	Media              *OutgoingMedia `json:"media,omitempty"`
	Recipient          string         `json:"recipient"`
	Content            string         `json:"content"`
	ClientID           uint32         `json:"client_id"`
	CreateTime         uint32         `json:"create_time"`
	Status             string         `json:"status"`
	ServerID           uint64         `json:"server_id"`
	Attempts           uint32         `json:"attempts"`
	ErrorCategory      *string        `json:"error_category"`
	ErrorCode          int64          `json:"error_code,omitempty"`
}
type Data struct {
	Schema  int    `json:"schema"`
	Binding string `json:"account_binding"`
	// BindingDeviceID anchors the existing cache namespace when the same
	// account authorizes a new QR device. It is not the live session device.
	BindingDeviceID     []byte                             `json:"cache_binding_device_id,omitempty"`
	SourceApp           string                             `json:"source_app,omitempty"`
	LoginSource         string                             `json:"login_source,omitempty"`
	Session             *protocol.Session                  `json:"session"`
	SessionInvalid      bool                               `json:"session_invalid,omitempty"`
	NextClientID        uint32                             `json:"next_client_id"`
	APIKey              []byte                             `json:"api_id_key"`
	Jobs                map[string]*Job                    `json:"jobs"`
	Messages            []protocol.Message                 `json:"messages"`
	Contacts            []protocol.Contact                 `json:"contacts"`
	Pinned              [][]byte                           `json:"pinned_peers"`
	Unavailable         [][]byte                           `json:"unavailable_peers"`
	LegacyCodex         json.RawMessage                    `json:"codex,omitempty"`
	Consumers           map[string]json.RawMessage         `json:"consumers,omitempty"`
	Events              []messaging.Event                  `json:"events,omitempty"`
	EventSequence       uint64                             `json:"event_sequence,omitempty"`
	DeliveryBatches     map[string]messaging.DeliveryBatch `json:"delivery_batches,omitempty"`
	DispatchGenerations map[string]uint64                  `json:"dispatch_generations,omitempty"`
	Artifacts           map[string]messaging.Artifact      `json:"artifacts,omitempty"`
	MediaJobs           map[string]*MediaJob               `json:"media_jobs,omitempty"`
}

type CodexState struct {
	Groups       map[string]GroupPolicy `json:"groups,omitempty"`
	Threads      map[string]string      `json:"threads"`
	Jobs         map[string]*CodexJob   `json:"jobs"`
	NextSequence uint64                 `json:"next_sequence"`
}

// ValidBinding checks the encrypted cache against the live account identity
// and the device that originally established this local namespace. QR session
// devices can rotate without moving cached history or encrypted media.
func (data *Data) ValidBinding() bool {
	if data == nil || data.Session == nil || data.Session.Bootstrap.UIN == 0 || len(data.Session.Bootstrap.Username) == 0 {
		return false
	}
	identity := data.Session.Bootstrap
	if len(data.BindingDeviceID) != 0 {
		if len(data.BindingDeviceID) != 16 {
			return false
		}
		identity.DeviceID = data.BindingDeviceID
	}
	return identity.Binding() == data.Binding
}

type CodexJob struct {
	Group       bool         `json:"group,omitempty"`
	GroupSender string       `json:"group_sender,omitempty"`
	Peer        string       `json:"peer"`
	Input       string       `json:"input"`
	MediaID     string       `json:"media_id,omitempty"`
	Timestamp   uint32       `json:"timestamp"`
	Sequence    uint64       `json:"sequence"`
	Status      string       `json:"status"`
	ThreadID    string       `json:"thread_id,omitempty"`
	TurnID      string       `json:"turn_id,omitempty"`
	Reply       string       `json:"reply,omitempty"`
	Images      []CodexImage `json:"images,omitempty"`
	OutgoingIDs []string     `json:"outgoing_ids,omitempty"`
	Error       string       `json:"error,omitempty"`
	ErrorMethod string       `json:"error_method,omitempty"`
	ErrorCode   int          `json:"error_code,omitempty"`
	ErrorReason string       `json:"error_reason,omitempty"`
}

// CodexImage references an encrypted, account-bound generated artifact. It
// never stores an App Server path or inline image bytes in the checkpoint.
type CodexImage struct {
	CacheID string `json:"cache_id"`
	Name    string `json:"name"`
	MIME    string `json:"mime"`
	Size    int64  `json:"size"`
	Digest  string `json:"digest"`
}

type GroupPolicy struct {
	Mode  string `json:"mode"`
	Since uint32 `json:"since"`
}

func Directory(path string) error {
	info, e := os.Lstat(path)
	if e != nil && !errors.Is(e, os.ErrNotExist) {
		return errors.New("state directory is unavailable")
	}
	if e == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("state directory is not a real directory")
		}
		if stat, ok := info.Sys().(*syscall.Stat_t); ok && stat.Uid != uint32(os.Getuid()) {
			return errors.New("state directory owner mismatch")
		}
	}
	if e = os.MkdirAll(path, 0700); e != nil {
		return errors.New("state directory cannot be created")
	}
	return os.Chmod(path, 0700)
}
func ReadPrivate(path string, max int64) ([]byte, error) {
	fd, e := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if e != nil {
		return nil, e
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	info, e := f.Stat()
	if e != nil {
		return nil, e
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("private file permissions are invalid")
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); ok && stat.Uid != uint32(os.Getuid()) {
		return nil, errors.New("private file owner mismatch")
	}
	b, e := io.ReadAll(io.LimitReader(f, max+1))
	if e != nil || int64(len(b)) > max {
		return nil, errors.New("private file size is invalid")
	}
	return b, nil
}
func Atomic(path string, b []byte) error {
	if e := Directory(filepath.Dir(path)); e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(path), ".write-*")
	if e != nil {
		return errors.New("private temporary file failed")
	}
	name := f.Name()
	defer os.Remove(name)
	if e = f.Chmod(0600); e != nil {
		f.Close()
		return e
	}
	if _, e = f.Write(b); e != nil {
		f.Close()
		return e
	}
	if e = f.Sync(); e != nil {
		f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = os.Rename(name, path); e != nil {
		return e
	}
	d, e := os.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}

type Checkpoint struct{ Directory, Binding string }

func (c Checkpoint) path() (string, error) {
	if len(c.Binding) != 64 || strings.Trim(c.Binding, "0123456789abcdef") != "" {
		return "", errors.New("invalid checkpoint binding")
	}
	return filepath.Join(c.Directory, c.Binding+".checkpoint"), nil
}
func (c Checkpoint) Load() (*Data, error) {
	path, e := c.path()
	if e != nil {
		return nil, e
	}
	b, e := ReadPrivate(path, 32<<20)
	if e != nil {
		return nil, e
	}
	if len(b) >= len(legacyMagic) && string(b[:len(legacyMagic)]) == legacyMagic {
		return nil, ErrLegacyCheckpoint
	}
	if len(b) < len(Magic)+28 || string(b[:len(Magic)]) != Magic {
		return nil, errors.New("checkpoint format is invalid")
	}
	key, e := c.key(false)
	if e != nil {
		return nil, e
	}
	block, e := aes.NewCipher(key)
	if e != nil {
		return nil, e
	}
	g, e := cipher.NewGCM(block)
	if e != nil {
		return nil, e
	}
	plain, e := g.Open(nil, b[len(Magic):len(Magic)+12], b[len(Magic)+12:], []byte(Magic+c.Binding))
	if e != nil {
		return nil, errors.New("checkpoint authentication failed")
	}
	var data Data
	if e = json.Unmarshal(plain, &data); e != nil {
		return nil, errors.New("checkpoint data is invalid")
	}
	if data.Schema != 1 || data.Binding != c.Binding || !data.ValidBinding() || len(data.APIKey) != 32 || len(data.Session.ClientKey) != 24 || len(data.Session.ServerKey) != 24 || len(data.Session.Original) != 16 || len(data.Session.Cookie) > 15 {
		return nil, errors.New("checkpoint account or keys are invalid")
	}
	if len(data.Session.Cursor) > 0 {
		if _, e = protocol.MergeCursor(nil, data.Session.Cursor); e != nil {
			return nil, e
		}
	}
	if data.Jobs == nil {
		data.Jobs = map[string]*Job{}
	}
	for _, job := range data.Jobs {
		if job == nil {
			return nil, errors.New("send checkpoint task is invalid")
		}
		if job.Status == "sending" || job.Status == "uploading" || job.Status == "submitting" {
			job.Status = "unknown"
			reason := "interrupted_send"
			job.ErrorCategory = &reason
		}
	}
	if err := restoreOutgoingMedia(&data); err != nil {
		return nil, err
	}
	if err := restoreMediaJobs(&data); err != nil {
		return nil, err
	}
	return &data, nil
}
func (c Checkpoint) Save(data *Data) error {
	if data.Schema != 1 || data.Binding != c.Binding || !data.ValidBinding() {
		return errors.New("checkpoint account is invalid")
	}
	path, e := c.path()
	if e != nil {
		return e
	}
	// Never overwrite a legacy cache with data sealed under a different key.
	if existing, readErr := ReadPrivate(path, 32<<20); readErr == nil {
		if len(existing) >= len(legacyMagic) && string(existing[:len(legacyMagic)]) == legacyMagic {
			return ErrLegacyCheckpoint
		}
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return readErr
	}
	plain, e := json.Marshal(data)
	if e != nil {
		return errors.New("checkpoint serialization failed")
	}
	if len(plain) > 31<<20 {
		return errors.New("checkpoint exceeds size bound")
	}
	key, e := c.key(true)
	if e != nil {
		return e
	}
	block, e := aes.NewCipher(key)
	if e != nil {
		return e
	}
	g, e := cipher.NewGCM(block)
	if e != nil {
		return e
	}
	nonce := make([]byte, 12)
	if _, e = rand.Read(nonce); e != nil {
		return e
	}
	out := append([]byte(Magic), nonce...)
	out = g.Seal(out, nonce, plain, []byte(Magic+c.Binding))
	return Atomic(path, out)
}
func Unique(directory string) (Checkpoint, bool, error) {
	if e := Directory(directory); e != nil {
		return Checkpoint{}, false, e
	}
	paths, e := filepath.Glob(filepath.Join(directory, "*.checkpoint"))
	if e != nil {
		return Checkpoint{}, false, e
	}
	if len(paths) == 0 {
		return Checkpoint{}, false, nil
	}
	if len(paths) != 1 {
		return Checkpoint{}, false, errors.New("multiple account checkpoints; choose an account before connecting")
	}
	return Checkpoint{directory, strings.TrimSuffix(filepath.Base(paths[0]), ".checkpoint")}, true, nil
}
