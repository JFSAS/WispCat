package onebot

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/JFSAS/wechat-runtime/internal/store"
)

const (
	streamFileLimit  = 16 << 20
	streamChunkLimit = 256 << 10
	streamChunkCount = 1024
	streamSlots      = 64
	streamDiskLimit  = 128 << 20
)

var (
	errStreamInvalid  = errors.New("invalid_stream_request")
	errStreamConflict = errors.New("stream_conflict")
	errStreamMissing  = errors.New("stream_unavailable")
	errStreamLimit    = errors.New("stream_size_limit")
	errStreamStorage  = errors.New("stream_storage_unavailable")
	errStreamClosed   = errors.New("stream_storage_closed")
)

type streamUpload struct {
	ID                      string
	Name                    string
	Total                   int
	Index                   *int
	Data                    []byte
	Size                    *int64
	SHA256                  string
	Retention               *time.Duration
	Complete, Verify, Reset bool
}

type streamItem struct {
	account, id, token, name, expected, digest string
	total                                      int
	size                                       *int64
	chunks                                     map[int]string
	pending                                    map[int]pendingStreamChunk
	pendingBytes                               int64
	bytes, reserved                            int64
	deadline                                   time.Time
	retention                                  time.Duration
	complete                                   bool
	readers                                    int
}

type pendingStreamChunk struct {
	digest string
	size   int64
}

// Streams owns only encrypted, short-lived transfer chunks. Durable messages,
// request keys, media receipts and the native media cache are outside its root.
type Streams struct {
	mu              sync.Mutex
	directory       string
	lock            *os.File
	items           map[string]*streamItem
	used            int64
	closed          bool
	now             func() time.Time
	stop, done      chan struct{}
	closeOnce       sync.Once
	closeErr        error
	writeChunk      func(store.MediaCache, string, []byte) error
	cleanupFailures map[string]bool
}

type StreamSnapshot struct {
	TotalBytes     int64 `json:"total_bytes"`
	ReservedBytes  int64 `json:"reserved_bytes"`
	FileCount      int   `json:"file_count"`
	Uploads        int   `json:"active_uploads"`
	Completed      int   `json:"completed_files"`
	Readers        int   `json:"active_readers"`
	QuotaBytes     int64 `json:"quota_bytes"`
	MaxFileBytes   int64 `json:"max_file_bytes"`
	MaxChunkBytes  int64 `json:"max_chunk_bytes"`
	CleanupPending bool  `json:"cleanup_pending"`
}

type StreamCleanup struct {
	Removed       int   `json:"removed"`
	FreedBytes    int64 `json:"freed_bytes"`
	SkippedActive int   `json:"skipped_active"`
}

func OpenStreams(directory string) (*Streams, error) {
	if err := store.Directory(directory); err != nil {
		return nil, errStreamStorage
	}
	fd, err := syscall.Open(filepath.Join(directory, ".lock"), syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, errStreamStorage
	}
	lock := os.NewFile(uintptr(fd), ".lock")
	if _, err = store.ReadPrivate(filepath.Join(directory, ".lock"), 1); err != nil {
		_ = lock.Close()
		return nil, errStreamStorage
	}
	if err = syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		return nil, errStreamStorage
	}
	s := &Streams{directory: directory, lock: lock, items: map[string]*streamItem{}, cleanupFailures: map[string]bool{}, now: time.Now, stop: make(chan struct{}), done: make(chan struct{})}
	entries, err := os.ReadDir(directory)
	if err == nil {
		for _, entry := range entries {
			if entry.Name() == ".lock" {
				continue
			}
			if !streamHex(entry.Name(), 32) {
				err = errStreamStorage
				break
			}
			_, _, err = s.removeDirectory(entry.Name())
			if err != nil {
				break
			}
		}
	}
	if err != nil {
		_ = syscall.Flock(fd, syscall.LOCK_UN)
		_ = lock.Close()
		return nil, errStreamStorage
	}
	go func() {
		defer close(s.done)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-ticker.C:
				s.mu.Lock()
				_, _ = s.cleanLocked("", true)
				s.mu.Unlock()
			}
		}
	}()
	return s, nil
}

func streamHex(v string, n int) bool { return len(v) == n && strings.Trim(v, "0123456789abcdef") == "" }
func streamSafeID(id string) bool {
	if len(id) == 0 || len(id) > 128 {
		return false
	}
	for _, c := range id {
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}
func streamBinding(account string) string {
	v := sha256.Sum256([]byte("stream-account\x00" + account))
	return hex.EncodeToString(v[:])
}
func streamPart(index int) string {
	return strings.Repeat("0", 32-len(strconv.FormatInt(int64(index), 16))) + strconv.FormatInt(int64(index), 16)
}
func (s *Streams) cache(item *streamItem) store.MediaCache {
	return store.MediaCache{Directory: filepath.Join(s.directory, item.token), Binding: streamBinding(item.account)}
}
func (s *Streams) packet(item *streamItem, status string) map[string]any {
	typ := "stream"
	if item.complete {
		typ = "response"
	}
	result := map[string]any{"type": typ, "stream_id": item.id, "status": status, "received_chunks": len(item.chunks), "total_chunks": item.total}
	if item.complete {
		result["file_path"], result["file_name"], result["file_size"], result["sha256"] = "stream://"+item.token, item.name, item.bytes, item.digest
	}
	return result
}

func (s *Streams) upload(ctx context.Context, account string, p streamUpload) (result map[string]any, failure error) {
	if account == "" || len(account) > 256 || !streamSafeID(p.ID) || p.Total < 0 || p.Total > streamChunkCount || (p.Index != nil && (*p.Index < 0 || *p.Index >= streamChunkCount)) || len(p.Data) > streamChunkLimit || (p.Size != nil && (*p.Size <= 0 || *p.Size > streamFileLimit)) || (p.SHA256 != "" && !streamHex(p.SHA256, 64)) || (p.Name != "" && !validUploadName(p.Name)) || (p.Retention != nil && (*p.Retention < 0 || *p.Retention > time.Hour)) {
		return nil, errStreamInvalid
	}
	if p.Reset && (p.Index != nil || p.Complete || p.Verify) || p.Verify && (p.Index != nil || p.Complete) || p.Index == nil && len(p.Data) > 0 || p.Index != nil && len(p.Data) == 0 {
		return nil, errStreamInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errStreamClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := s.cleanLocked("", true); err != nil {
		return nil, err
	}
	var item *streamItem
	created := false
	defer func() {
		if failure != nil && created && len(item.chunks) == 0 && len(item.pending) == 0 {
			delete(s.items, item.token)
			s.used -= item.reserved
		}
	}()
	for _, candidate := range s.items {
		if candidate.account == account && candidate.id == p.ID {
			item = candidate
			break
		}
	}
	if p.Reset {
		if item != nil {
			if item.readers != 0 {
				return nil, errStreamConflict
			}
			if _, _, err := s.removeDirectory(item.token); err != nil {
				return nil, err
			}
			delete(s.items, item.token)
			s.used -= item.reserved
		}
		return map[string]any{"type": "reset", "stream_id": p.ID, "status": "reset"}, nil
	}
	if p.Verify {
		if item == nil {
			return nil, errStreamMissing
		}
		if item.complete {
			return s.packet(item, "file_complete"), nil
		}
		return s.packet(item, "file_created"), nil
	}
	if item == nil {
		if p.Total < 1 || p.Index != nil && *p.Index >= p.Total {
			return nil, errStreamInvalid
		}
		if len(s.items) >= streamSlots || s.used+128+streamChunkLimit+128 > streamDiskLimit {
			return nil, errStreamLimit
		}
		random := make([]byte, 16)
		if _, err := rand.Read(random); err != nil {
			return nil, errStreamStorage
		}
		name := p.Name
		if name == "" {
			name = "upload.bin"
		}
		item = &streamItem{account: account, id: p.ID, token: hex.EncodeToString(random), name: name, total: p.Total, chunks: map[int]string{}, pending: map[int]pendingStreamChunk{}, retention: 5 * time.Minute, reserved: 128, deadline: s.now().Add(10 * time.Minute)}
		s.items[item.token] = item
		s.used += item.reserved
		created = true
	}
	if p.Total != 0 && p.Total != item.total || p.Name != "" && p.Name != item.name || p.Index != nil && *p.Index >= item.total || p.Size != nil && item.size != nil && *p.Size != *item.size || p.SHA256 != "" && item.expected != "" && p.SHA256 != item.expected || p.Retention != nil && item.complete && *p.Retention != item.retention {
		return nil, errStreamConflict
	}
	if item.complete {
		if p.Size != nil && *p.Size != item.bytes || p.SHA256 != "" && p.SHA256 != item.digest {
			return nil, errStreamConflict
		}
		if p.Index != nil {
			digest := sha256.Sum256(p.Data)
			if item.chunks[*p.Index] != hex.EncodeToString(digest[:]) {
				return nil, errStreamConflict
			}
		}
		return s.packet(item, "file_complete"), nil
	}
	nextSize, nextExpected, nextRetention := item.size, item.expected, item.retention
	if p.Size != nil {
		if *p.Size < item.bytes+item.pendingBytes {
			return nil, errStreamConflict
		}
		value := *p.Size
		nextSize = &value
	}
	if p.SHA256 != "" {
		nextExpected = p.SHA256
	}
	if p.Retention != nil {
		nextRetention = *p.Retention
	}
	commitMetadata := func() { item.size, item.expected, item.retention = nextSize, nextExpected, nextRetention }
	if p.Index != nil {
		digest := sha256.Sum256(p.Data)
		hash := hex.EncodeToString(digest[:])
		if old, ok := item.chunks[*p.Index]; ok {
			if old != hash {
				return nil, errStreamConflict
			}
		} else {
			cost := int64(len(p.Data)) + 128
			if pending, ok := item.pending[*p.Index]; ok {
				if pending.digest != hash || pending.size != int64(len(p.Data)) {
					return nil, errStreamConflict
				}
			} else {
				if item.bytes+item.pendingBytes+int64(len(p.Data)) > streamFileLimit || nextSize != nil && item.bytes+item.pendingBytes+int64(len(p.Data)) > *nextSize || s.used+cost+streamChunkLimit+128 > streamDiskLimit {
					return nil, errStreamLimit
				}
				// Reserve before touching disk. A failed atomic Save can already have
				// renamed the chunk, so its quota cannot be released speculatively.
				item.pending[*p.Index] = pendingStreamChunk{digest: hash, size: int64(len(p.Data))}
				item.pendingBytes += int64(len(p.Data))
				item.reserved += cost
				s.used += cost
			}
			write := s.writeChunk
			if write == nil {
				write = func(cache store.MediaCache, id string, data []byte) error { return cache.Save(id, data) }
			}
			if err := write(s.cache(item), streamPart(*p.Index), p.Data); err != nil {
				return nil, errStreamStorage
			}
			item.chunks[*p.Index] = hash
			item.bytes += int64(len(p.Data))
			item.pendingBytes -= int64(len(p.Data))
			delete(item.pending, *p.Index)
		}
		// Match the reference's chunk acknowledgement; completion is an explicit
		// subsequent call, avoiding two meanings for the final chunk response.
		if !p.Complete {
			item.deadline = s.now().Add(10 * time.Minute)
			commitMetadata()
			return s.packet(item, "chunk_received"), nil
		}
	}
	if !p.Complete && len(item.chunks) != item.total {
		commitMetadata()
		return s.packet(item, "file_created"), nil
	}
	if len(item.chunks) != item.total || nextSize != nil && item.bytes != *nextSize {
		return nil, errStreamInvalid
	}
	hash := sha256.New()
	for i := 0; i < item.total; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		data, err := s.partLocked(item, i)
		if err != nil {
			return nil, err
		}
		_, _ = hash.Write(data)
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	if nextExpected != "" && digest != nextExpected {
		return nil, errStreamConflict
	}
	commitMetadata()
	item.complete, item.digest = true, digest
	item.deadline = s.now().Add(item.retention)
	if item.retention == 0 {
		item.deadline = time.Time{}
	}
	return s.packet(item, "file_complete"), nil
}

func (s *Streams) partLocked(item *streamItem, index int) ([]byte, error) {
	data, err := s.cache(item).Load(streamPart(index))
	if err != nil || len(data) > streamChunkLimit {
		return nil, errStreamStorage
	}
	digest := sha256.Sum256(data)
	if item.chunks[index] != hex.EncodeToString(digest[:]) {
		return nil, errStreamStorage
	}
	return data, nil
}

type streamReader struct {
	store    *Streams
	item     *streamItem
	index    int
	buffered *bytes.Reader
	closed   bool
}

func (s *Streams) acquire(account, reference string) (*streamReader, error) {
	token := strings.TrimPrefix(reference, "stream://")
	if reference == token || !streamHex(token, 32) {
		return nil, errStreamMissing
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errStreamClosed
	}
	if _, err := s.cleanLocked("", true); err != nil {
		return nil, err
	}
	item := s.items[token]
	if item == nil || item.account != account || !item.complete || !item.deadline.IsZero() && !s.now().Before(item.deadline) {
		return nil, errStreamMissing
	}
	item.readers++
	return &streamReader{store: s, item: item}, nil
}
func (r *streamReader) Read(p []byte) (int, error) {
	s := r.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.closed || s.closed {
		return 0, errStreamClosed
	}
	if len(p) == 0 {
		return 0, nil
	}
	for r.buffered == nil || r.buffered.Len() == 0 {
		if r.index >= r.item.total {
			return 0, io.EOF
		}
		data, err := s.partLocked(r.item, r.index)
		if err != nil {
			return 0, err
		}
		r.index++
		r.buffered = bytes.NewReader(data)
	}
	return r.buffered.Read(p)
}
func (r *streamReader) Close() error {
	s := r.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if !r.closed {
		r.closed = true
		r.item.readers--
		r.buffered = nil
	}
	return nil
}

func (s *Streams) removeDirectory(token string) (int, int64, error) {
	n, bytes, err := s.removeOwnedDirectory(token)
	if err != nil {
		s.cleanupFailures[token] = true
	} else {
		delete(s.cleanupFailures, token)
	}
	return n, bytes, err
}

func (s *Streams) removeOwnedDirectory(token string) (int, int64, error) {
	path := filepath.Join(s.directory, token)
	if err := store.Directory(s.directory); err != nil {
		return 0, 0, errStreamStorage
	}
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil
	}
	if err := store.Directory(path); err != nil {
		return 0, 0, errStreamStorage
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return 0, 0, errStreamStorage
	}
	// Validate the entire owned directory before deleting a single file.
	for _, entry := range entries {
		n := entry.Name()
		atomicTemporary := strings.HasPrefix(n, ".write-") && len(n) > 7 && len(n) <= 27 && strings.Trim(n[7:], "0123456789") == ""
		if !(atomicTemporary || strings.HasSuffix(n, ".enc") && streamHex(strings.TrimSuffix(n, ".enc"), 32) || strings.HasSuffix(n, ".key") && streamHex(strings.TrimSuffix(n, ".key"), 64)) {
			return 0, 0, errStreamStorage
		}
		info, e := os.Lstat(filepath.Join(path, n))
		if e != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
			return 0, 0, errStreamStorage
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok || st.Uid != uint32(os.Getuid()) {
			return 0, 0, errStreamStorage
		}
	}
	count := 0
	var freed int64
	for _, entry := range entries {
		info, e := os.Lstat(filepath.Join(path, entry.Name()))
		if e != nil {
			return count, freed, errStreamStorage
		}
		if e = os.Remove(filepath.Join(path, entry.Name())); e != nil {
			return count, freed, errStreamStorage
		}
		count++
		freed += info.Size()
	}
	if err = os.Remove(path); err != nil {
		return count, freed, errStreamStorage
	}
	return count, freed, nil
}
func (s *Streams) cleanLocked(account string, expiredOnly bool) (result StreamCleanup, failure error) {
	for token, item := range s.items {
		if account != "" && item.account != account {
			continue
		}
		expired := !item.deadline.IsZero() && !s.now().Before(item.deadline)
		if expiredOnly && !expired {
			continue
		}
		if item.readers > 0 || !expiredOnly && !item.complete && !expired {
			result.SkippedActive++
			continue
		}
		removed, freed, err := s.removeDirectory(token)
		result.Removed += removed
		result.FreedBytes += freed
		if err != nil {
			return result, err
		}
		s.used -= item.reserved
		delete(s.items, token)
	}
	return result, nil
}
func (s *Streams) Clean(account string) (StreamCleanup, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return StreamCleanup{}, errStreamClosed
	}
	return s.cleanLocked(account, false)
}
func (s *Streams) Snapshot(account string) (StreamSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := StreamSnapshot{QuotaBytes: streamDiskLimit, MaxFileBytes: streamFileLimit, MaxChunkBytes: streamChunkLimit, CleanupPending: len(s.cleanupFailures) > 0}
	if s.closed {
		return result, errStreamClosed
	}
	for _, item := range s.items {
		if account != "" && item.account != account {
			continue
		}
		result.ReservedBytes += item.reserved
		result.Readers += item.readers
		if item.complete {
			result.Completed++
		} else {
			result.Uploads++
		}
		path := filepath.Join(s.directory, item.token)
		if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err := store.Directory(path); err != nil {
			return result, errStreamStorage
		}
		entries, err := os.ReadDir(path)
		if err != nil {
			return result, errStreamStorage
		}
		for _, entry := range entries {
			info, err := os.Lstat(filepath.Join(path, entry.Name()))
			if err != nil || !info.Mode().IsRegular() {
				return result, errStreamStorage
			}
			result.FileCount++
			result.TotalBytes += info.Size()
		}
	}
	return result, nil
}
func (s *Streams) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		close(s.stop)
		for token := range s.items {
			if _, _, err := s.removeDirectory(token); err != nil {
				s.closeErr = err
			}
		}
		s.items = nil
		s.mu.Unlock()
		<-s.done
		_ = syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN)
		if err := s.lock.Close(); err != nil {
			s.closeErr = errStreamStorage
		}
	})
	return s.closeErr
}
