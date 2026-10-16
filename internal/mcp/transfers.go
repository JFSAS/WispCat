package mcp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/JFSAS/wechat-runtime/pkg/client"
)

const transferFiles = 128
const transferMarker = "wechat-runtime/mcp-downloads/v1\n"

var transferName = regexp.MustCompile(`^mcp-[a-f0-9]{32}\.(part|bin)$`)
var errTransferStorage = errors.New("mcp_stream_storage_unavailable")
var errTransferLimit = errors.New("mcp_stream_limit")

// TransferConfig describes files on the MCP host, never on the OneBot host.
// DownloadDir must be a dedicated private directory. An empty value creates a
// unique temporary directory whose completed files remain after Close.
type TransferConfig struct {
	DownloadDir  string
	UploadRoot   string
	MaxFileBytes int64
	Timeout      time.Duration
}

// Transfers serializes local file operations and holds an exclusive directory
// lock. os.Root pins both directories and prevents path/symlink escapes.
type Transfers struct {
	gate          chan struct{}
	directory     string
	root, upload  *os.Root
	lock          *os.File
	directoryInfo os.FileInfo
	max           int64
	timeout       time.Duration
	closed        bool
	failed        bool
	removeFile    func(string) error
	syncDir       func() error
}

func OpenTransfers(config TransferConfig) (result *Transfers, err error) {
	if config.MaxFileBytes == 0 {
		config.MaxFileBytes = client.MaxStreamFileBytes
	}
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	if config.MaxFileBytes < 1 || config.MaxFileBytes > client.MaxStreamFileBytes || config.Timeout < time.Millisecond || config.Timeout > 10*time.Minute {
		return nil, errors.New("invalid_mcp_transfer_config")
	}
	directory := config.DownloadDir
	if directory == "" {
		directory, err = os.MkdirTemp("", "wechat-mcp-downloads-")
	} else {
		err = os.MkdirAll(directory, 0700)
	}
	if err != nil {
		return nil, errTransferStorage
	}
	directory, err = filepath.EvalSymlinks(directory)
	if err != nil {
		return nil, errTransferStorage
	}
	directory, err = filepath.Abs(directory)
	if err != nil {
		return nil, errTransferStorage
	}
	t := &Transfers{gate: make(chan struct{}, 1), directory: directory, max: config.MaxFileBytes, timeout: config.Timeout}
	defer func() {
		if err != nil {
			_ = t.Close()
		}
	}()
	t.root, err = os.OpenRoot(directory)
	if err != nil {
		return nil, errTransferStorage
	}
	dir, e := t.root.Open(".")
	if e != nil {
		return nil, errTransferStorage
	}
	t.directoryInfo, e = dir.Stat()
	_ = dir.Close()
	if e != nil || !t.directoryInfo.IsDir() || t.directoryInfo.Mode().Perm()&0077 != 0 || !owned(t.directoryInfo) {
		return nil, errTransferStorage
	}
	t.lock, e = t.root.OpenFile(".owner.lock", os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if e != nil {
		return nil, errTransferStorage
	}
	info, e := t.lock.Stat()
	if e != nil || !privateFile(info, 1) {
		return nil, errTransferStorage
	}
	if syscall.Flock(int(t.lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		return nil, errors.New("mcp_stream_directory_in_use")
	}
	if e = t.initialize(); e != nil {
		return nil, e
	}
	if config.UploadRoot != "" {
		uploadDirectory, pathErr := filepath.Abs(config.UploadRoot)
		if pathErr != nil {
			return nil, errors.New("invalid_mcp_upload_root")
		}
		t.upload, e = os.OpenRoot(uploadDirectory)
		if e != nil {
			return nil, errors.New("invalid_mcp_upload_root")
		}
	}
	return t, nil
}

func owned(info os.FileInfo) bool {
	value, ok := info.Sys().(*syscall.Stat_t)
	return ok && value.Uid == uint32(os.Geteuid())
}
func links(info os.FileInfo) uint64 {
	value, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0
	}
	return uint64(value.Nlink)
}
func privateFile(info os.FileInfo, n uint64) bool {
	return info.Mode().IsRegular() && info.Mode().Perm() == 0600 && owned(info) && links(info) == n
}

func (t *Transfers) entries() ([]os.DirEntry, error) {
	dir, err := t.root.Open(".")
	if err != nil {
		return nil, errTransferStorage
	}
	defer dir.Close()
	entries, err := dir.ReadDir(transferFiles + 4)
	if err != nil && err != io.EOF || len(entries) >= transferFiles+4 {
		return nil, errTransferStorage
	}
	return entries, nil
}

func (t *Transfers) initialize() error {
	entries, err := t.entries()
	if err != nil {
		return err
	}
	marker, err := t.root.OpenFile(".wechat-mcp-downloads", os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		if len(entries) != 1 || entries[0].Name() != ".owner.lock" {
			return errTransferStorage
		}
		marker, err = t.root.OpenFile(".wechat-mcp-downloads", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return errTransferStorage
		}
		_, writeErr := io.WriteString(marker, transferMarker)
		syncErr := marker.Sync()
		closeErr := marker.Close()
		if writeErr != nil || syncErr != nil || closeErr != nil || t.syncDirectory() != nil {
			return errTransferStorage
		}
	} else {
		if err != nil {
			return errTransferStorage
		}
		info, e := marker.Stat()
		raw, readErr := io.ReadAll(io.LimitReader(marker, 128))
		closeErr := marker.Close()
		if e != nil || !privateFile(info, 1) || readErr != nil || closeErr != nil || string(raw) != transferMarker {
			return errTransferStorage
		}
	}
	// Validate every entry before removing only orphaned, owned partials. A crash
	// between Link and Remove may leave the matching .part/.bin hard-link pair.
	entries, err = t.entries()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == ".owner.lock" || name == ".wechat-mcp-downloads" {
			continue
		}
		info, e := t.root.Lstat(name)
		if e != nil || !transferName.MatchString(name) || !privateFile(info, 1) && !privateFile(info, 2) || info.Size() > client.MaxStreamFileBytes {
			return errTransferStorage
		}
		if links(info) == 2 {
			other := strings.TrimSuffix(strings.TrimSuffix(name, ".part"), ".bin")
			if strings.HasSuffix(name, ".part") {
				other += ".bin"
			} else {
				other += ".part"
			}
			paired, e := t.root.Lstat(other)
			if e != nil || !os.SameFile(info, paired) {
				return errTransferStorage
			}
		}
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".part") {
			if t.root.Remove(entry.Name()) != nil {
				return errTransferStorage
			}
		}
	}
	if t.syncDirectory() != nil {
		return errTransferStorage
	}
	_, _, err = t.inventory()
	return err
}

func (t *Transfers) inventory() (bytes int64, count int, err error) {
	entries, err := t.entries()
	if err != nil {
		return 0, 0, err
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == ".owner.lock" || name == ".wechat-mcp-downloads" {
			continue
		}
		info, e := t.root.Lstat(name)
		if e != nil || !transferName.MatchString(name) || !privateFile(info, 1) || info.Size() > client.MaxStreamFileBytes {
			return 0, 0, errTransferStorage
		}
		bytes += info.Size()
		count++
	}
	return bytes, count, nil
}

func (t *Transfers) syncDirectory() error {
	if t.syncDir != nil {
		return t.syncDir()
	}
	dir, err := t.root.Open(".")
	if err != nil {
		return errTransferStorage
	}
	defer dir.Close()
	return dir.Sync()
}

func (t *Transfers) sameDirectory() bool {
	info, err := os.Lstat(t.directory)
	return err == nil && info.IsDir() && info.Mode().Perm()&0077 == 0 && owned(info) && os.SameFile(info, t.directoryInfo)
}

func (t *Transfers) Close() error {
	if t.gate == nil {
		return errTransferStorage
	}
	t.gate <- struct{}{}
	defer func() { <-t.gate }()
	if t.closed {
		return nil
	}
	t.closed = true
	var failures []error
	if t.upload != nil {
		failures = append(failures, t.upload.Close())
	}
	if t.lock != nil {
		failures = append(failures, t.lock.Close())
	}
	if t.root != nil {
		failures = append(failures, t.root.Close())
	}
	if errors.Join(failures...) != nil || t.failed {
		return errTransferStorage
	}
	return nil
}

type transferResult struct {
	client.StreamFile
	Location string `json:"location"`
	Terminal bool   `json:"terminal"`
}

func (t *Transfers) call(ctx context.Context, c *client.Client, action string, params map[string]any, input string) (transferResult, error) {
	if t.gate == nil {
		return transferResult{}, errTransferStorage
	}
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	select {
	case t.gate <- struct{}{}:
		defer func() { <-t.gate }()
	case <-ctx.Done():
		return transferResult{}, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return transferResult{}, err
	}
	if t.closed || t.failed {
		return transferResult{}, errTransferStorage
	}
	if input != "" {
		result, err := t.uploadFile(ctx, c, params, input)
		if err != nil && ctx.Err() != nil {
			err = errors.Join(err, ctx.Err())
		}
		return result, err
	}
	result, err := t.downloadFile(ctx, c, action, params)
	if err != nil && ctx.Err() != nil {
		err = errors.Join(err, ctx.Err())
	}
	return result, err
}

func transferFailure(err error) string {
	switch {
	case errors.Is(err, errTransferStorage):
		return errTransferStorage.Error()
	case errors.Is(err, errTransferLimit):
		return errTransferLimit.Error()
	case errors.Is(err, context.DeadlineExceeded):
		return "mcp_stream_timeout"
	case errors.Is(err, context.Canceled):
		return "mcp_stream_cancelled"
	}
	switch err.Error() {
	case "mcp_upload_root_required", "conflicting_upload_params", "mcp_upload_path_denied", "mcp_upload_regular_file_required", "invalid_upload_filename":
		return err.Error()
	}
	return "mcp_file_transfer_failed"
}

type checkedUpload struct {
	file    *os.File
	initial os.FileInfo
}

func (r checkedUpload) Read(body []byte) (int, error) {
	n, err := r.file.Read(body)
	if err == io.EOF {
		after, e := r.file.Stat()
		if e != nil || after.Size() != r.initial.Size() || !after.ModTime().Equal(r.initial.ModTime()) {
			return n, errors.New("mcp_upload_source_changed")
		}
	}
	return n, err
}

func (t *Transfers) uploadFile(ctx context.Context, c *client.Client, params map[string]any, input string) (transferResult, error) {
	if t.upload == nil {
		return transferResult{}, errors.New("mcp_upload_root_required")
	}
	options := map[string]any{}
	for name, value := range params {
		if name != "filename" && name != "self_id" && name != "file_retention" {
			return transferResult{}, errors.New("conflicting_upload_params")
		}
		if name != "filename" {
			options[name] = value
		}
	}
	name := input
	if filepath.IsAbs(input) {
		var err error
		name, err = filepath.Rel(t.upload.Name(), input)
		if err != nil {
			return transferResult{}, errors.New("mcp_upload_path_denied")
		}
	}
	f, err := t.upload.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return transferResult{}, errors.New("mcp_upload_path_denied")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return transferResult{}, errors.New("mcp_upload_regular_file_required")
	}
	if info.Size() < 1 || info.Size() > t.max {
		return transferResult{}, errTransferLimit
	}
	filename := filepath.Base(input)
	if value, ok := params["filename"]; ok {
		var valid bool
		filename, valid = value.(string)
		if !valid || filename == "" {
			return transferResult{}, errors.New("invalid_upload_filename")
		}
	}
	file, err := c.UploadFileWithParams(ctx, filename, checkedUpload{f, info}, info.Size(), options)
	return transferResult{StreamFile: file, Location: "runtime_temporary_reference", Terminal: err == nil}, err
}

type transferWriter struct {
	file      *os.File
	remaining int64
}

func (w *transferWriter) Write(body []byte) (int, error) {
	if int64(len(body)) > w.remaining {
		return 0, errTransferLimit
	}
	n, err := w.file.Write(body)
	w.remaining -= int64(n)
	return n, err
}

func (t *Transfers) downloadFile(ctx context.Context, c *client.Client, action string, params map[string]any) (result transferResult, err error) {
	used, count, err := t.inventory()
	if err != nil {
		return result, err
	}
	if used+t.max > 2*t.max || count >= transferFiles {
		return result, errTransferLimit
	}
	if !t.sameDirectory() {
		return result, errTransferStorage
	}
	var random [16]byte
	if _, err = rand.Read(random[:]); err != nil {
		return result, errTransferStorage
	}
	base := "mcp-" + hex.EncodeToString(random[:])
	part, final := base+".part", base+".bin"
	f, err := t.root.OpenFile(part, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return result, errTransferStorage
	}
	published := false
	linked := false
	defer func() {
		_ = f.Close()
		if !published {
			var failures []error
			remove := t.root.Remove
			if t.removeFile != nil {
				remove = t.removeFile
			}
			if failure := remove(part); failure != nil && !errors.Is(failure, os.ErrNotExist) {
				failures = append(failures, failure)
			}
			if linked {
				if failure := remove(final); failure != nil && !errors.Is(failure, os.ErrNotExist) {
					failures = append(failures, failure)
				}
			}
			failures = append(failures, t.syncDirectory())
			if errors.Join(failures...) != nil {
				t.failed = true
				err = errors.Join(err, errTransferStorage)
			}
		}
	}()
	file, err := c.DownloadStream(ctx, action, params, &transferWriter{f, t.max})
	if err != nil {
		return result, err
	}
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	if f.Sync() != nil || !t.sameDirectory() {
		return result, errTransferStorage
	}
	verified, statErr := f.Stat()
	if statErr != nil || !privateFile(verified, 1) || !t.matchesFile(part, verified, 1) {
		return result, errTransferStorage
	}
	// Link never overwrites an existing destination. Both names remain within
	// the pinned root; recovery knows this one matching hard-link transition.
	if t.root.Link(part, final) != nil {
		return result, errTransferStorage
	}
	linked = true
	if !t.matchesFile(part, verified, 2) || !t.matchesFile(final, verified, 2) {
		return result, errTransferStorage
	}
	if t.root.Remove(part) != nil || t.syncDirectory() != nil || !t.sameDirectory() || !t.matchesFile(final, verified, 1) || f.Close() != nil {
		return result, errTransferStorage
	}
	published = true
	file.FilePath = filepath.Join(t.directory, final)
	return transferResult{StreamFile: file, Location: "mcp_host", Terminal: true}, nil
}

func (t *Transfers) matchesFile(name string, expected os.FileInfo, n uint64) bool {
	info, err := t.root.Lstat(name)
	return err == nil && privateFile(info, n) && os.SameFile(info, expected)
}
