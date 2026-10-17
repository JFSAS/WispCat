package service

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/JFSAS/wispcat/internal/securedb"
	"github.com/JFSAS/wispcat/internal/store"
)

const incomingMediaFiles = 512

type incomingMediaMeta struct {
	Schema         int    `json:"schema"`
	Account        string `json:"account"`
	Namespace      string `json:"namespace"`
	Revision       uint64 `json:"revision"`
	Tasks          uint64 `json:"tasks"`
	ResidentFiles  uint64 `json:"resident_files"`
	ResidentBytes  int64  `json:"resident_bytes"`
	ReclaimedBytes int64  `json:"reclaimed_bytes"`
}

type incomingMediaRecord struct {
	Schema     int            `json:"schema"`
	Account    string         `json:"account"`
	ID         string         `json:"id"`
	Job        store.MediaJob `json:"job"`
	Phase      string         `json:"phase"`
	Bytes      int64          `json:"bytes"`
	Digest     string         `json:"digest"`
	CapturedAt int64          `json:"captured_at"`
}

func (incomingMediaRecord) String() string   { return "incomingMediaRecord(redacted)" }
func (incomingMediaRecord) GoString() string { return "incomingMediaRecord(redacted)" }

func (h *EventHistory) mediaMeta(tx *securedb.Tx, account, namespace string) (incomingMediaMeta, bool, error) {
	var meta incomingMediaMeta
	raw, err := tx.Get("incoming-media-meta", h.db.Key("incoming-media", account))
	if err != nil {
		return meta, false, err
	}
	if raw == nil {
		if tx.HasRecords("incoming-media") {
			return meta, false, ErrEventHistory
		}
		return incomingMediaMeta{Schema: 1, Account: account, Namespace: namespace}, false, nil
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return meta, false, ErrEventHistory
	}
	for _, name := range []string{"schema", "account", "namespace", "revision", "tasks", "resident_files", "resident_bytes", "reclaimed_bytes"} {
		if fields[name] == nil || string(fields[name]) == "null" {
			return meta, false, ErrEventHistory
		}
	}
	if json.Unmarshal(raw, &meta) != nil || meta.Schema != 1 || meta.Account != account || !mediaID(meta.Namespace, 64) || namespace != "" && namespace != meta.Namespace || meta.Tasks > meta.Revision || meta.ResidentFiles > incomingMediaFiles || meta.ResidentBytes < 0 || meta.ResidentBytes > int64(incomingMediaFiles)*store.MaxMediaBytes || meta.ReclaimedBytes < 0 {
		return meta, false, ErrEventHistory
	}
	return meta, true, nil
}
func (h *EventHistory) putMediaMeta(tx *securedb.Tx, meta incomingMediaMeta) error {
	raw, _ := json.Marshal(meta)
	return tx.Put("incoming-media-meta", h.db.Key("incoming-media", meta.Account), raw)
}
func (h *EventHistory) mediaKey(account, id string) []byte {
	return h.db.Key("incoming-media", account, id)
}
func (h *EventHistory) mediaLatestKey(account string, job store.MediaJob) []byte {
	return h.db.Key("incoming-media-latest", account, job.SessionID, job.MessageID, job.Variant)
}
func (h *EventHistory) mediaLiveKey(row incomingMediaRecord) []byte {
	prefix := h.db.Key("incoming-media-live", row.Account)
	sequence := make([]byte, 8)
	binary.BigEndian.PutUint64(sequence, uint64(row.CapturedAt))
	return append(append(prefix, sequence...), h.db.Key("id", row.ID)...)
}
func validIncomingMediaJob(account, id string, job store.MediaJob) bool {
	if !mediaID(id, 32) || job.Binding != account || !mediaID(job.SessionID, 32) || !mediaID(job.MessageID, 64) || job.CreatedAt < 0 || job.Size < 0 || job.Size > store.MaxMediaBytes || len(job.Name) > 1024 || !utf8.ValidString(job.Name) || strings.ContainsAny(job.Name, "/\\") {
		return false
	}
	for _, c := range job.Name {
		if unicode.IsControl(c) || unicode.In(c, unicode.Cf) {
			return false
		}
	}
	if job.Kind != "image" && job.Kind != "file" || job.Variant != "regular" && job.Variant != "thumbnail" && job.Variant != "original" && job.Variant != "attachment" || job.Kind == "file" && job.Variant != "attachment" || job.Kind == "image" && job.Variant == "attachment" {
		return false
	}
	if job.Status != "downloading" && job.Status != "downloaded" && job.Status != "failed" {
		return false
	}
	return job.Status != "downloaded" || (mediaID(job.Digest, 64) && job.MIME != "")
}
func sameIncomingMediaIdentity(a, b store.MediaJob) bool {
	return a.Binding == b.Binding && a.SessionID == b.SessionID && a.MessageID == b.MessageID && a.Variant == b.Variant && a.Kind == b.Kind && a.CreatedAt == b.CreatedAt
}
func (h *EventHistory) mediaRecord(tx *securedb.Tx, account, id string) (*incomingMediaRecord, error) {
	raw, err := tx.Get("incoming-media", h.mediaKey(account, id))
	if err != nil || raw == nil {
		return nil, err
	}
	var row incomingMediaRecord
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return nil, ErrEventHistory
	}
	for _, name := range []string{"schema", "account", "id", "job", "phase", "bytes", "digest", "captured_at"} {
		if fields[name] == nil || string(fields[name]) == "null" {
			return nil, ErrEventHistory
		}
	}
	if json.Unmarshal(raw, &row) != nil || row.Schema != 1 || row.Account != account || row.ID != id || row.CapturedAt < 0 || !validIncomingMediaJob(account, id, row.Job) || row.Bytes < 0 || row.Bytes > store.MaxMediaBytes || row.Bytes > 0 && !mediaID(row.Digest, 64) {
		return nil, ErrEventHistory
	}
	if row.Phase != "reserved" && row.Phase != "resident" && row.Phase != "pending_delete" && row.Phase != "evicted" {
		return nil, ErrEventHistory
	}
	if row.Phase == "reserved" && row.Job.Status != "downloading" || row.Phase != "reserved" && row.Job.Status == "downloading" {
		return nil, ErrEventHistory
	}
	return &row, nil
}
func (h *EventHistory) putMediaRecord(tx *securedb.Tx, meta *incomingMediaMeta, row incomingMediaRecord, old *incomingMediaRecord) error {
	if meta.Revision == math.MaxUint64 {
		return ErrEventHistory
	}
	if old != nil {
		if !sameIncomingMediaIdentity(old.Job, row.Job) || old.CapturedAt != row.CapturedAt {
			return ErrEventHistory
		}
		before, _ := json.Marshal(old)
		after, _ := json.Marshal(row)
		if bytes.Equal(before, after) {
			return nil
		}
	} else {
		if row.Job.Status != "downloading" {
			meta.Tasks++
		}
	}
	charged := func(r *incomingMediaRecord) bool { return r != nil && r.Phase != "evicted" }
	if charged(old) {
		if meta.ResidentFiles == 0 || meta.ResidentBytes < old.Bytes {
			return ErrEventHistory
		}
		meta.ResidentFiles--
		meta.ResidentBytes -= old.Bytes
		if err := tx.Delete("incoming-media-live", h.mediaLiveKey(*old)); err != nil {
			return err
		}
	}
	if charged(&row) {
		meta.ResidentFiles++
		meta.ResidentBytes += row.Bytes
	}
	if meta.ResidentFiles > incomingMediaFiles || meta.ResidentBytes < 0 || meta.ResidentBytes > int64(incomingMediaFiles)*store.MaxMediaBytes {
		return ErrEventHistory
	}
	if old != nil && old.Job.Status == "downloading" && row.Job.Status != "downloading" {
		meta.Tasks++
	}
	meta.Revision++
	value, _ := json.Marshal(row)
	if err := tx.Put("incoming-media", h.mediaKey(row.Account, row.ID), value); err != nil {
		return err
	}
	id, _ := json.Marshal(row.ID)
	if row.Job.Status != "downloading" {
		if err := tx.Put("incoming-media-latest", h.mediaLatestKey(row.Account, row.Job), id); err != nil {
			return err
		}
	}
	if charged(&row) {
		if err := tx.Put("incoming-media-live", h.mediaLiveKey(row), id); err != nil {
			return err
		}
	}
	pendingKey := append(h.db.Key("incoming-media-pending", row.Account), h.db.Key("id", row.ID)...)
	reservationKey := append(h.db.Key("incoming-media-reservations", row.Account), h.db.Key("id", row.ID)...)
	if row.Phase == "reserved" {
		if err := tx.Put("incoming-media-reservations", reservationKey, id); err != nil {
			return err
		}
	} else if err := tx.Delete("incoming-media-reservations", reservationKey); err != nil {
		return err
	}
	if row.Phase == "pending_delete" {
		return tx.Put("incoming-media-pending", pendingKey, id)
	}
	return tx.Delete("incoming-media-pending", pendingKey)
}

func (h *EventHistory) captureMedia(tx *securedb.Tx, s *Service) error {
	meta, _, err := h.mediaMeta(tx, s.data.Binding, historyNamespace(s.data))
	if err != nil {
		return err
	}
	for id, job := range s.data.MediaJobs {
		if job == nil {
			return ErrEventHistory
		}
		old, err := h.mediaRecord(tx, s.data.Binding, id)
		if err != nil {
			return err
		}
		if old != nil && !sameIncomingMediaIdentity(old.Job, *job) {
			return ErrEventHistory
		}
		if job.Status != "downloaded" && job.Status != "failed" {
			continue
		}
		if !validIncomingMediaJob(s.data.Binding, id, *job) {
			return ErrEventHistory
		}
		if old != nil && old.Job.Status != "downloading" {
			continue
		}
		row := incomingMediaRecord{Schema: 1, Account: s.data.Binding, ID: id, Job: *job, Phase: "evicted", CapturedAt: time.Now().UnixNano()}
		if old != nil {
			row.CapturedAt, row.Bytes, row.Digest = old.CapturedAt, old.Bytes, old.Digest
			row.Phase = "pending_delete"
		}
		if job.Status == "downloaded" {
			row.Bytes, row.Digest = job.Size, job.Digest
			_, present, err := h.readMediaPayload(row)
			if err != nil {
				return err
			}
			if present {
				row.Phase = "resident"
			} else {
				row.Phase = "evicted"
			}
		} else if old == nil {
			if _, err := os.Lstat(filepath.Join(h.directory, "media", id+".enc")); err == nil {
				data, e := (store.MediaCache{Directory: filepath.Join(h.directory, "media"), Binding: row.Account}).Load(id)
				if e != nil {
					return ErrEventHistory
				}
				digest := sha256.Sum256(data)
				row.Bytes, row.Digest, row.Phase = int64(len(data)), hex.EncodeToString(digest[:]), "resident"
			} else if !os.IsNotExist(err) {
				return ErrEventHistory
			}
		}
		if err = h.putMediaRecord(tx, &meta, row, old); err != nil {
			return err
		}
	}
	return h.putMediaMeta(tx, meta)
}
func (h *EventHistory) readMediaPayload(row incomingMediaRecord) ([]byte, bool, error) {
	path := filepath.Join(h.directory, "media", row.ID+".enc")
	if _, err := os.Lstat(path); os.IsNotExist(err) {
		return nil, false, nil
	} else if err != nil {
		return nil, false, ErrEventHistory
	}
	data, err := (store.MediaCache{Directory: filepath.Join(h.directory, "media"), Binding: row.Account}).Load(row.ID)
	if err != nil {
		return nil, false, ErrEventHistory
	}
	digest := sha256.Sum256(data)
	if int64(len(data)) != row.Bytes || hex.EncodeToString(digest[:]) != row.Digest {
		return nil, false, ErrEventHistory
	}
	return data, true, nil
}

func (h *EventHistory) pruneMediaLocked(s *Service) error {
	ids := []string{}
	err := h.db.View(func(tx *securedb.Tx) error {
		for id, job := range s.data.MediaJobs {
			row, err := h.mediaRecord(tx, s.data.Binding, id)
			if err != nil {
				return err
			}
			if row == nil || row.Job.Status == "downloading" {
				continue
			}
			if !sameIncomingMediaIdentity(row.Job, *job) {
				return ErrEventHistory
			}
			ids = append(ids, id)
		}
		return nil
	})
	if err != nil {
		return ErrEventHistory
	}
	for _, id := range ids {
		delete(s.data.MediaJobs, id)
	}
	return nil
}
