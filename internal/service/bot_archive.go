package service

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/JFSAS/wechat-runtime/internal/media"
	"github.com/JFSAS/wechat-runtime/internal/protocol"
	"github.com/JFSAS/wechat-runtime/internal/securedb"
	"github.com/JFSAS/wechat-runtime/internal/store"
	"github.com/JFSAS/wechat-runtime/pkg/bot"
)

var ErrDeliveryArchive = fmt.Errorf("delivery_archive_unavailable: %w", bot.ErrStorage)

type archivedBatch struct {
	Schema      int         `json:"schema"`
	Account     string      `json:"account"`
	Key         string      `json:"key"`
	Fingerprint string      `json:"fingerprint"`
	IDs         []string    `json:"ids"`
	Jobs        []store.Job `json:"jobs"`
	ArchivedAt  int64       `json:"archived_at"`
	NativeNext  uint32      `json:"native_next"`
}

func (archivedBatch) String() string { return "archivedBatch(redacted)" }

type archiveCounts struct {
	Schema             int    `json:"schema"`
	Batches            uint64 `json:"batches"`
	Jobs               uint64 `json:"jobs"`
	MediaBytes         int64  `json:"media_bytes"`
	LastAt             int64  `json:"last_at"`
	NativeReservations uint64 `json:"native_reservations"`
}
type ArchiveStats struct {
	Format             string `json:"format"`
	ArchivedBatches    uint64 `json:"archived_batches"`
	ArchivedJobs       uint64 `json:"archived_jobs"`
	RetainedMediaBytes int64  `json:"retained_media_bytes"`
	LastArchivedAt     int64  `json:"last_archived_at"`
	FileBytes          int64  `json:"file_bytes"`
	NativeReservations uint64 `json:"native_reservations"`
}

// DeliveryArchive keeps immutable terminal outcomes independently of the active
// checkpoint. Its owner closes it only after all channel/adapter users stop.
type DeliveryArchive struct {
	db            *securedb.DB
	directory     string
	identity      string
	writeMu       sync.Mutex
	closed        bool
	guardWrite    func([]byte) error
	pendingWrite  func([]byte) error
	reservationMu sync.Mutex
	reservations  map[string]archiveMediaReservation
}

type archiveGuard struct {
	Schema             int    `json:"schema"`
	ID                 string `json:"id"`
	Batches            uint64 `json:"minimum_batches"`
	Jobs               uint64 `json:"minimum_jobs"`
	NativeReservations uint64 `json:"minimum_native_reservations"`
}

func OpenDeliveryArchive(directory string) (*DeliveryArchive, error) {
	if _, err := os.Lstat(filepath.Join(directory, "delivery-archive.pending")); err == nil {
		if _, e := os.Lstat(filepath.Join(directory, "delivery-archive", "deliveries.db")); e != nil {
			return nil, ErrDeliveryArchive
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, ErrDeliveryArchive
	}
	var guard *archiveGuard
	rawGuard, guardErr := store.ReadPrivate(filepath.Join(directory, "delivery-archive.required"), 1024)
	if guardErr == nil {
		var value archiveGuard
		var fields map[string]json.RawMessage
		if json.Unmarshal(rawGuard, &fields) != nil {
			return nil, ErrDeliveryArchive
		}
		for _, name := range []string{"schema", "id", "minimum_batches", "minimum_jobs", "minimum_native_reservations"} {
			if fields[name] == nil || string(fields[name]) == "null" {
				return nil, ErrDeliveryArchive
			}
		}
		if json.Unmarshal(rawGuard, &value) != nil || value.Schema != 1 || !mediaID(value.ID, 64) || (value.Batches == 0 && value.NativeReservations == 0) || value.Jobs < value.Batches {
			return nil, ErrDeliveryArchive
		}
		guard = &value
		if _, err := os.Lstat(filepath.Join(directory, "delivery-archive", "deliveries.db")); err != nil {
			return nil, ErrDeliveryArchive
		}
	} else if !errors.Is(guardErr, os.ErrNotExist) {
		return nil, ErrDeliveryArchive
	}
	db, err := securedb.Open(filepath.Join(directory, "delivery-archive"), "deliveries")
	if err != nil {
		return nil, ErrDeliveryArchive
	}
	a := &DeliveryArchive{db: db, directory: directory, reservations: map[string]archiveMediaReservation{}}
	err = db.Update(func(tx *securedb.Tx) error {
		raw, e := tx.Get("meta", []byte("total"))
		if e != nil {
			return e
		}
		if raw != nil {
			counts, e := decodeArchiveCounts(raw)
			if e != nil {
				return e
			}
			id, e := tx.Get("meta", []byte("archive-id"))
			if e != nil || !mediaID(string(id), 64) {
				return ErrDeliveryArchive
			}
			a.identity = string(id)
			if guard != nil && (guard.ID != a.identity || counts.Batches < guard.Batches || counts.Jobs < guard.Jobs || counts.NativeReservations < guard.NativeReservations) {
				return ErrDeliveryArchive
			}
			return nil
		}
		if tx.HasRecords("batches") || guard != nil {
			return ErrDeliveryArchive
		}
		identity := make([]byte, 32)
		if _, e = rand.Read(identity); e != nil {
			return e
		}
		a.identity = hex.EncodeToString(identity)
		if e = tx.Put("meta", []byte("archive-id"), []byte(a.identity)); e != nil {
			return e
		}
		return putArchiveCounts(tx, []byte("total"), archiveCounts{Schema: 1})
	})
	if err == nil {
		err = a.resolvePendingLocked()
	}
	if err == nil {
		err = a.ensureGuard()
	}
	if err != nil {
		_ = db.Close()
		return nil, ErrDeliveryArchive
	}
	return a, nil
}
func (a *DeliveryArchive) Close() error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	if a.closed {
		return nil
	}
	a.closed = true
	return a.db.Close()
}

func (a *DeliveryArchive) ensureGuard() error {
	stats, err := a.Stats("")
	if err != nil || (stats.ArchivedBatches == 0 && stats.NativeReservations == 0) {
		return err
	}
	guard := archiveGuard{Schema: 1, ID: a.identity, Batches: stats.ArchivedBatches, Jobs: stats.ArchivedJobs, NativeReservations: stats.NativeReservations}
	raw, err := json.Marshal(guard)
	if err != nil {
		return err
	}
	if a.guardWrite != nil {
		return a.guardWrite(raw)
	}
	return store.Atomic(filepath.Join(a.directory, "delivery-archive.required"), raw)
}

func terminalBotJob(job *store.Job) bool {
	if job == nil || job.Origin != "onebot" {
		return false
	}
	switch job.Status {
	case "accepted", "echo_observed", "unknown", "failed", "cancelled":
		return true
	}
	return false
}
func validateArchivedBatch(row archivedBatch) error {
	if row.Schema != 1 || !mediaID(row.Account, 64) || !strings.HasPrefix(row.Key, "bot/") || len(row.Key) > 164 || !mediaID(row.Fingerprint, 64) || len(row.IDs) == 0 || len(row.IDs) > 20 || len(row.Jobs) != len(row.IDs) || row.NativeNext == 0 || row.NativeNext > 0x7fffffff {
		return ErrDeliveryArchive
	}
	seen := map[string]bool{}
	for i, id := range row.IDs {
		job := &row.Jobs[i]
		if !mediaID(id, 32) || seen[id] || !terminalBotJob(job) || job.Recipient == "" || len(job.Recipient) > 8192 {
			return ErrDeliveryArchive
		}
		if job.Media != nil && (job.Media.Binding != row.Account || job.Media.Size < 1 || job.Media.Size > MaxMediaSendBytes) {
			return ErrDeliveryArchive
		}
		seen[id] = true
	}
	return nil
}
func decodeArchiveCounts(raw []byte) (archiveCounts, error) {
	var counts archiveCounts
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return counts, ErrDeliveryArchive
	}
	for _, name := range []string{"schema", "batches", "jobs", "media_bytes", "last_at", "native_reservations"} {
		if fields[name] == nil || string(fields[name]) == "null" {
			return counts, ErrDeliveryArchive
		}
	}
	if json.Unmarshal(raw, &counts) != nil || counts.Schema != 1 || counts.MediaBytes < 0 || counts.Jobs < counts.Batches || counts.Jobs-counts.Batches > counts.Batches*19 {
		return counts, ErrDeliveryArchive
	}
	return counts, nil
}
func readArchiveCounts(tx *securedb.Tx, key []byte, optional bool) (archiveCounts, error) {
	raw, err := tx.Get("meta", key)
	if err != nil {
		return archiveCounts{}, err
	}
	if optional && raw == nil {
		return archiveCounts{Schema: 1}, nil
	}
	return decodeArchiveCounts(raw)
}
func putArchiveCounts(tx *securedb.Tx, key []byte, counts archiveCounts) error {
	raw, err := json.Marshal(counts)
	if err != nil {
		return err
	}
	return tx.Put("meta", key, raw)
}
func (a *DeliveryArchive) readBatch(tx *securedb.Tx, account, key string) (*archivedBatch, error) {
	raw, err := tx.Get("batches", a.db.Key("batch", account, key))
	if err != nil || raw == nil {
		return nil, err
	}
	var row archivedBatch
	if json.Unmarshal(raw, &row) != nil || validateArchivedBatch(row) != nil || row.Account != account || row.Key != key {
		return nil, ErrDeliveryArchive
	}
	return &row, nil
}
func (a *DeliveryArchive) batch(account, key string) (*archivedBatch, error) {
	var row *archivedBatch
	err := a.db.View(func(tx *securedb.Tx) error {
		var e error
		row, e = a.readBatch(tx, account, key)
		return e
	})
	if err != nil {
		return nil, ErrDeliveryArchive
	}
	return row, nil
}
func (a *DeliveryArchive) indexed(account, bucket, key string) (*archivedBatch, error) {
	var row *archivedBatch
	err := a.db.View(func(tx *securedb.Tx) error {
		raw, e := tx.Get(bucket, a.db.Key(bucket, account, key))
		if e != nil || raw == nil {
			return e
		}
		var batchKey string
		if json.Unmarshal(raw, &batchKey) != nil || batchKey == "" {
			return ErrDeliveryArchive
		}
		row, e = a.readBatch(tx, account, batchKey)
		if e == nil && row == nil {
			return ErrDeliveryArchive
		}
		return e
	})
	if err != nil {
		return nil, ErrDeliveryArchive
	}
	return row, nil
}
func (a *DeliveryArchive) Stats(account string) (ArchiveStats, error) {
	result := ArchiveStats{Format: "encrypted_delivery_archive_v1"}
	err := a.db.View(func(tx *securedb.Tx) error {
		key, optional := []byte("total"), false
		if account != "" {
			key, optional = a.db.Key("counts", account), true
		}
		counts, e := readArchiveCounts(tx, key, optional)
		result.ArchivedBatches, result.ArchivedJobs = counts.Batches, counts.Jobs
		result.RetainedMediaBytes, result.LastArchivedAt = counts.MediaBytes, counts.LastAt
		result.NativeReservations = counts.NativeReservations
		return e
	})
	if err != nil {
		return result, ErrDeliveryArchive
	}
	result.FileBytes, err = a.db.Size()
	if err != nil {
		return result, ErrDeliveryArchive
	}
	return result, nil
}

func compatibleArchiveRows(first, second archivedBatch) bool {
	if first.Account != second.Account || first.Key != second.Key || first.Fingerprint != second.Fingerprint || !reflect.DeepEqual(first.IDs, second.IDs) {
		return false
	}
	// A later sync may label an already verified receipt echo_observed. It does
	// not change the immutable platform outcome kept in the archive.
	canonical := func(jobs []store.Job) []store.Job {
		copy := append([]store.Job(nil), jobs...)
		for i := range copy {
			if copy[i].Status == "echo_observed" {
				copy[i].Status = "accepted"
			}
		}
		return copy
	}
	return reflect.DeepEqual(canonical(first.Jobs), canonical(second.Jobs))
}

func (a *DeliveryArchive) store(rows []archivedBatch, owner []byte) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	if a.closed {
		return ErrDeliveryArchive
	}
	if err := a.resolvePendingLocked(); err != nil {
		return err
	}
	return a.storeRowsLocked(rows, owner, false)
}

func (a *DeliveryArchive) storeRowsLocked(rows []archivedBatch, owner []byte, recovering bool) error {
	if len(rows) == 0 || len(rows) > 64 {
		return ErrDeliveryArchive
	}
	for _, row := range rows {
		if validateArchivedBatch(row) != nil || row.Account != rows[0].Account {
			return ErrDeliveryArchive
		}
	}
	err := a.db.Update(func(tx *securedb.Tx) error {
		total, e := readArchiveCounts(tx, []byte("total"), false)
		if e != nil {
			return e
		}
		beforeTotal := total
		beforeAccount, e := readArchiveCounts(tx, a.db.Key("counts", rows[0].Account), true)
		if e != nil {
			return e
		}
		for _, row := range rows {
			if _, e = a.nativeLedger(tx, row.Account, row.NativeNext); e != nil {
				return e
			}
			old, e := a.readBatch(tx, row.Account, row.Key)
			if e != nil {
				return e
			}
			if old != nil {
				if !compatibleArchiveRows(*old, row) {
					return ErrDeliveryArchive
				}
				continue
			}
			countsKey := a.db.Key("counts", row.Account)
			counts, e := readArchiveCounts(tx, countsKey, true)
			if e != nil {
				return e
			}
			index := func(bucket, key string) error {
				lookup := a.db.Key(bucket, row.Account, key)
				previous, err := tx.Get(bucket, lookup)
				if err != nil || previous != nil {
					return ErrDeliveryArchive
				}
				raw, _ := json.Marshal(row.Key)
				return tx.Put(bucket, lookup, raw)
			}
			if e = index("batch-hashes", batchHash(row.Account, row.Key)); e != nil {
				return e
			}
			var mediaBytes int64
			for i, id := range row.IDs {
				job := &row.Jobs[i]
				if e = index("jobs", id); e != nil {
					return e
				}
				if job.ServerID != 0 {
					mid := messageKey(protocol.Message{Sender: owner, Recipient: []byte(job.Recipient), ServerID: job.ServerID}, owner)
					if e = index("messages", mid); e != nil {
						return e
					}
				}
				if job.Media != nil {
					mediaBytes += job.Media.Size
				}
				if e = a.storeEcho(tx, row.Account, id, job); e != nil {
					return e
				}
			}
			raw, e := json.Marshal(row)
			if e != nil {
				return e
			}
			if e = tx.Put("batches", a.db.Key("batch", row.Account, row.Key), raw); e != nil {
				return e
			}
			for _, counters := range []*archiveCounts{&counts, &total} {
				counters.Batches++
				counters.Jobs += uint64(len(row.IDs))
				counters.MediaBytes += mediaBytes
				counters.LastAt = max(counters.LastAt, row.ArchivedAt)
			}
			if e = putArchiveCounts(tx, countsKey, counts); e != nil {
				return e
			}
		}
		if e = putArchiveCounts(tx, []byte("total"), total); e != nil {
			return e
		}
		if !recovering {
			afterAccount, e := readArchiveCounts(tx, a.db.Key("counts", rows[0].Account), false)
			if e != nil {
				return e
			}
			return a.writePending(rows, beforeTotal, beforeAccount, total, afterAccount)
		}
		return nil
	})
	if err != nil {
		return ErrDeliveryArchive
	}
	if recovering {
		return nil
	}
	if err = a.ensureGuard(); err != nil {
		return ErrDeliveryArchive
	}
	return a.clearPending()
}

func echoPrefix(db *securedb.DB, account, recipient, kind, content string, receipt bool) []byte {
	return db.Key("echo", account, recipient, kind, content, strconv.FormatBool(receipt))
}
func echoIndexKey(prefix []byte, when uint32, id string) []byte {
	key := append([]byte(nil), prefix...)
	var stamp [4]byte
	binary.BigEndian.PutUint32(stamp[:], when)
	key = append(key, stamp[:]...)
	return append(key, id...)
}
func (a *DeliveryArchive) storeEcho(tx *securedb.Tx, account, id string, job *store.Job) error {
	put := func(kind, content string) error {
		prefix := echoPrefix(a.db, account, job.Recipient, kind, content, job.ServerID != 0)
		value, _ := json.Marshal(id)
		return tx.Put("echoes", echoIndexKey(prefix, job.CreateTime, id), value)
	}
	if err := put("text", job.Content); err != nil {
		return err
	}
	if job.Media != nil && job.Media.MD5 != "" && (job.Status == "accepted" || job.Status == "echo_observed" || job.Status == "unknown") {
		return put(job.Kind, strings.ToLower(job.Media.MD5))
	}
	return nil
}

func (a *DeliveryArchive) localEcho(account string, m protocol.Message, owner []byte) (bool, error) {
	if !isOwnMessage(m, owner) && !bytes.Equal(m.Sender, []byte("filehelper")) {
		return false, nil
	}
	if m.ServerID != 0 {
		row, err := a.indexed(account, "messages", messageKey(m, owner))
		if err != nil || row != nil {
			return row != nil, err
		}
	}
	peer := string(m.Peer(owner))
	lookups := []struct{ kind, content string }{{"text", string(normalizedMessage(m, owner).Content)}}
	if m.Type == 3 || m.Type == 49 {
		if description, err := media.Parse(m.Type, normalizedMessage(m, owner).Content); err == nil {
			lookups = append(lookups, struct{ kind, content string }{string(description.Public().Kind), strings.ToLower(description.DownloadReference().MD5())})
		}
	}
	found := false
	err := a.db.View(func(tx *securedb.Tx) error {
		for _, lookup := range lookups {
			if lookup.kind != "text" && lookup.content == "" {
				continue
			}
			for _, receipt := range []bool{false, true} {
				if m.ServerID != 0 && receipt {
					continue
				}
				prefix := echoPrefix(a.db, account, peer, lookup.kind, lookup.content, receipt)
				low, high := max(int64(0), int64(m.Timestamp)-300), min(int64(0xffffffff), int64(m.Timestamp)+300)
				err := tx.Scan("echoes", prefix, echoIndexKey(prefix, uint32(low), ""), 2, func(key, raw []byte) (bool, error) {
					if len(key) != len(prefix)+4+32 {
						return false, ErrDeliveryArchive
					}
					if int64(binary.BigEndian.Uint32(key[len(prefix):])) > high {
						return false, nil
					}
					var id string
					if json.Unmarshal(raw, &id) != nil || !mediaID(id, 32) || string(key[len(prefix)+4:]) != id {
						return false, ErrDeliveryArchive
					}
					found = true
					return false, nil
				})
				if err != nil || found {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return false, ErrDeliveryArchive
	}
	return found, nil
}
