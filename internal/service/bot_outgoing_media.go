package service

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"

	"github.com/JFSAS/wispcat/internal/securedb"
	"github.com/JFSAS/wispcat/internal/store"
)

// Immutable archive counts still describe every original media fact. This
// separate ledger accounts for disposable, confirmed outgoing payload copies.
type outgoingMediaMeta struct {
	Schema         int    `json:"schema"`
	Revision       uint64 `json:"revision"`
	ReleasedBytes  int64  `json:"released_bytes"`
	ReleasedFiles  uint64 `json:"released_files"`
	Pending        uint64 `json:"pending"`
	PendingAccount string `json:"pending_account"`
	Migrated       bool   `json:"migrated"`
	After          string `json:"migration_after"`
}

type outgoingMediaAccount struct {
	Schema        int    `json:"schema"`
	ReleasedBytes int64  `json:"released_bytes"`
	ReleasedFiles uint64 `json:"released_files"`
	After         string `json:"scan_after"`
}

type outgoingMediaRef struct {
	Schema  int    `json:"schema"`
	Account string `json:"account"`
	ID      string `json:"id"`
	Batch   string `json:"batch"`
	Outcome string `json:"outcome"`
	Phase   string `json:"phase"`
}

func strictMediaJSON(raw []byte, value any, names ...string) error {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return ErrDeliveryArchive
	}
	for _, name := range names {
		if fields[name] == nil || string(fields[name]) == "null" {
			return ErrDeliveryArchive
		}
	}
	if json.Unmarshal(raw, value) != nil {
		return ErrDeliveryArchive
	}
	return nil
}

func (a *DeliveryArchive) mediaMeta(tx *securedb.Tx) (outgoingMediaMeta, error) {
	var m outgoingMediaMeta
	raw, err := tx.Get("meta", []byte("media-lifecycle"))
	if err != nil || strictMediaJSON(raw, &m, "schema", "revision", "released_bytes", "released_files", "pending", "pending_account", "migrated", "migration_after") != nil || m.Schema != 1 || m.ReleasedBytes < 0 || m.Pending > 64 || (m.Pending == 0) != (m.PendingAccount == "") || (m.Pending > 0 && !mediaID(m.PendingAccount, 64)) || (m.After != "" && !mediaID(m.After, 64)) {
		return m, ErrDeliveryArchive
	}
	return m, nil
}

func (a *DeliveryArchive) putMediaMeta(tx *securedb.Tx, m outgoingMediaMeta) error {
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return tx.Put("meta", []byte("media-lifecycle"), raw)
}

func advanceOutgoingMedia(m *outgoingMediaMeta) error {
	if m.Revision == math.MaxUint64 {
		return ErrDeliveryArchive
	}
	m.Revision++
	return nil
}

func (a *DeliveryArchive) mediaAccount(tx *securedb.Tx, account string) (outgoingMediaAccount, error) {
	m := outgoingMediaAccount{Schema: 1}
	raw, err := tx.Get("outgoing-media-counts", a.db.Key("account", account))
	if err != nil {
		return m, ErrDeliveryArchive
	}
	if raw == nil {
		return m, nil
	}
	if strictMediaJSON(raw, &m, "schema", "released_bytes", "released_files", "scan_after") != nil || m.Schema != 1 || m.ReleasedBytes < 0 || (m.After != "" && !mediaID(m.After, 144)) {
		return m, ErrDeliveryArchive
	}
	return m, nil
}

func (a *DeliveryArchive) putMediaAccount(tx *securedb.Tx, account string, value outgoingMediaAccount) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return tx.Put("outgoing-media-counts", a.db.Key("account", account), raw)
}

func (a *DeliveryArchive) openMediaLifecycle(guard *archiveGuard) error {
	return a.db.Update(func(tx *securedb.Tx) error {
		raw, err := tx.Get("meta", []byte("media-lifecycle"))
		if err != nil {
			return err
		}
		if raw == nil {
			if (guard != nil && guard.Schema == 2) || tx.HasRecords("outgoing-media-releases") || tx.HasRecords("outgoing-media-candidates") || tx.HasRecords("outgoing-media-counts") {
				return ErrDeliveryArchive
			}
			return a.putMediaMeta(tx, outgoingMediaMeta{Schema: 1})
		}
		m, err := a.mediaMeta(tx)
		if err != nil || (guard != nil && guard.Schema == 2 && m.Revision < guard.MediaRevision) {
			return ErrDeliveryArchive
		}
		counts, err := readArchiveCounts(tx, []byte("total"), false)
		if err != nil || m.ReleasedBytes > counts.MediaBytes {
			return ErrDeliveryArchive
		}
		return nil
	})
}

func confirmedOutgoingMedia(job store.Job) bool {
	return job.Media != nil && (job.Status == "accepted" || job.Status == "echo_observed") && job.ServerID != 0 && mediaID(job.Media.Digest, 64)
}

func (a *DeliveryArchive) outgoingMediaKey(row archivedBatch, id string) []byte {
	key := a.db.Key("outgoing-media-candidates", row.Account)
	var stamp [8]byte
	binary.BigEndian.PutUint64(stamp[:], uint64(max(int64(0), row.ArchivedAt)))
	key = append(key, stamp[:]...)
	return append(key, []byte(id)...)
}

func (a *DeliveryArchive) indexOutgoingMedia(tx *securedb.Tx, meta *outgoingMediaMeta, row archivedBatch) error {
	for i, id := range row.IDs {
		if !confirmedOutgoingMedia(row.Jobs[i]) {
			continue
		}
		ref := outgoingMediaRef{Schema: 1, Account: row.Account, ID: id, Batch: row.Key, Outcome: outcomeHash(row), Phase: "resident"}
		released, err := a.mediaRelease(tx, row.Account, id)
		if err != nil {
			return err
		}
		if released != nil {
			if released.Batch != ref.Batch || released.Outcome != ref.Outcome {
				return ErrDeliveryArchive
			}
			continue
		}
		key := a.outgoingMediaKey(row, id)
		raw, _ := json.Marshal(ref)
		previous, err := tx.Get("outgoing-media-candidates", key)
		if err != nil {
			return err
		}
		if previous != nil {
			if !bytes.Equal(previous, raw) {
				return ErrDeliveryArchive
			}
			continue
		}
		if err = tx.Put("outgoing-media-candidates", key, raw); err != nil {
			return err
		}
		if err = advanceOutgoingMedia(meta); err != nil {
			return err
		}
	}
	return nil
}

func decodeOutgoingMedia(raw []byte) (outgoingMediaRef, error) {
	var ref outgoingMediaRef
	if strictMediaJSON(raw, &ref, "schema", "account", "id", "batch", "outcome", "phase") != nil || ref.Schema != 1 || !mediaID(ref.Account, 64) || !mediaID(ref.ID, 32) || !mediaID(ref.Outcome, 64) || len(ref.Batch) > 164 || len(ref.Batch) < 5 || ref.Batch[:4] != "bot/" {
		return ref, ErrDeliveryArchive
	}
	switch ref.Phase {
	case "resident", "pending_delete", "evicted":
	default:
		return ref, ErrDeliveryArchive
	}
	return ref, nil
}

func (a *DeliveryArchive) mediaRelease(tx *securedb.Tx, account, id string) (*outgoingMediaRef, error) {
	raw, err := tx.Get("outgoing-media-releases", a.db.Key("release", account, id))
	if err != nil {
		return nil, ErrDeliveryArchive
	}
	if raw == nil {
		return nil, nil
	}
	ref, err := decodeOutgoingMedia(raw)
	if err != nil || ref.Account != account || ref.ID != id || ref.Phase == "resident" {
		return nil, ErrDeliveryArchive
	}
	return &ref, nil
}

func (a *DeliveryArchive) outgoingMediaJob(tx *securedb.Tx, ref outgoingMediaRef) (archivedBatch, store.Job, error) {
	row, err := a.readBatch(tx, ref.Account, ref.Batch)
	if err != nil || row == nil || outcomeHash(*row) != ref.Outcome {
		return archivedBatch{}, store.Job{}, ErrDeliveryArchive
	}
	for i, id := range row.IDs {
		if id == ref.ID && confirmedOutgoingMedia(row.Jobs[i]) {
			return *row, row.Jobs[i], nil
		}
	}
	return archivedBatch{}, store.Job{}, ErrDeliveryArchive
}

// migrateOutgoingMediaLocked indexes one page of old immutable archive rows.
// New writes are indexed in their original archive transaction.
func (a *DeliveryArchive) migrateOutgoingMediaLocked() error {
	changed := false
	err := a.db.Update(func(tx *securedb.Tx) error {
		meta, err := a.mediaMeta(tx)
		if err != nil || meta.Migrated {
			return err
		}
		var start []byte
		if meta.After != "" {
			start, _ = hex.DecodeString(meta.After)
			start = append(start, 0)
		}
		count := 0
		err = tx.ScanAll("batches", start, 256, func(key, raw []byte) (bool, error) {
			var row archivedBatch
			if json.Unmarshal(raw, &row) != nil || validateArchivedBatch(row) != nil || !bytes.Equal(key, a.db.Key("batch", row.Account, row.Key)) {
				return false, ErrDeliveryArchive
			}
			if err := a.indexOutgoingMedia(tx, &meta, row); err != nil {
				return false, err
			}
			meta.After = hex.EncodeToString(key)
			count++
			return count < 256, nil
		})
		if err != nil {
			return err
		}
		meta.Migrated = count < 256
		if err = advanceOutgoingMedia(&meta); err != nil {
			return err
		}
		changed = true
		return a.putMediaMeta(tx, meta)
	})
	if err == nil && changed {
		err = a.ensureGuard()
	}
	if err != nil {
		a.mediaUnavailable.Store(true)
		return ErrDeliveryArchive
	}
	return nil
}
