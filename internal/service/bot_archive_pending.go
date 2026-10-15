package service

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/JFSAS/wechat-runtime/internal/securedb"
	"github.com/JFSAS/wechat-runtime/internal/store"
)

type pendingArchiveRef struct {
	BatchHash   string `json:"batch_hash"`
	OutcomeHash string `json:"outcome_hash"`
	ArchivedAt  int64  `json:"archived_at"`
	NativeNext  uint32 `json:"native_next"`
}
type pendingArchive struct {
	Schema        int                 `json:"schema"`
	ID            string              `json:"id"`
	Account       string              `json:"account"`
	BeforeTotal   archiveCounts       `json:"before_total"`
	BeforeAccount archiveCounts       `json:"before_account"`
	AfterTotal    archiveCounts       `json:"after_total"`
	AfterAccount  archiveCounts       `json:"after_account"`
	Rows          []pendingArchiveRef `json:"rows"`
	MAC           string              `json:"mac"`
}

func outcomeHash(row archivedBatch) string {
	row.ArchivedAt, row.NativeNext = 0, 0
	row.Jobs = append([]store.Job(nil), row.Jobs...)
	for i := range row.Jobs {
		if row.Jobs[i].Status == "echo_observed" {
			row.Jobs[i].Status = "accepted"
		}
	}
	raw, _ := json.Marshal(row)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}
func (a *DeliveryArchive) pendingMAC(p pendingArchive) string {
	p.MAC = ""
	raw, _ := json.Marshal(p)
	return hex.EncodeToString(a.db.Key("archive-pending", string(raw)))
}
func (a *DeliveryArchive) writePending(rows []archivedBatch, beforeTotal, beforeAccount, afterTotal, afterAccount archiveCounts) error {
	p := pendingArchive{Schema: 1, ID: a.identity, Account: rows[0].Account, BeforeTotal: beforeTotal, BeforeAccount: beforeAccount, AfterTotal: afterTotal, AfterAccount: afterAccount}
	for _, row := range rows {
		p.Rows = append(p.Rows, pendingArchiveRef{BatchHash: batchHash(row.Account, row.Key), OutcomeHash: outcomeHash(row), ArchivedAt: row.ArchivedAt, NativeNext: row.NativeNext})
	}
	p.MAC = a.pendingMAC(p)
	raw, err := json.Marshal(p)
	if err != nil {
		return err
	}
	// This intent is durable before bbolt's commit. It binds the exact outcomes
	// without storing body/media/recipient data outside encrypted storage.
	if a.pendingWrite != nil {
		return a.pendingWrite(raw)
	}
	return store.Atomic(filepath.Join(a.directory, "delivery-archive.pending"), raw)
}
func (a *DeliveryArchive) clearPending() error {
	if err := clearBotIntent(filepath.Join(a.directory, "delivery-archive.pending")); err != nil {
		return ErrDeliveryArchive
	}
	return nil
}
func archiveCountsAtLeast(current, target archiveCounts) bool {
	return current.Schema == 1 && target.Schema == 1 && current.Batches >= target.Batches && current.Jobs >= target.Jobs && current.MediaBytes >= target.MediaBytes && current.NativeReservations >= target.NativeReservations && current.LastAt >= target.LastAt
}

// resolvePendingLocked completes an interrupted archive transaction or refuses
// a rolled-back combination whose checkpoint no longer proves the same outcomes.
// Callers hold writeMu, except construction before the archive is published.
func (a *DeliveryArchive) resolvePendingLocked() error {
	raw, err := store.ReadPrivate(filepath.Join(a.directory, "delivery-archive.pending"), 128<<10)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	var pending pendingArchive
	if err != nil || json.Unmarshal(raw, &pending) != nil || pending.Schema != 1 || pending.ID != a.identity || !mediaID(pending.Account, 64) || len(pending.Rows) < 1 || len(pending.Rows) > 64 || !hmac.Equal([]byte(pending.MAC), []byte(a.pendingMAC(pending))) {
		return ErrDeliveryArchive
	}
	var total, account archiveCounts
	err = a.db.View(func(tx *securedb.Tx) error {
		var e error
		total, e = readArchiveCounts(tx, []byte("total"), false)
		if e != nil {
			return e
		}
		account, e = readArchiveCounts(tx, a.db.Key("counts", pending.Account), true)
		return e
	})
	if err != nil {
		return ErrDeliveryArchive
	}
	if !archiveCountsAtLeast(total, pending.AfterTotal) || !archiveCountsAtLeast(account, pending.AfterAccount) {
		if total != pending.BeforeTotal || account != pending.BeforeAccount {
			return ErrDeliveryArchive
		}
		cp, exists, err := store.Unique(filepath.Join(a.directory, "checkpoints"))
		if err != nil || !exists {
			return ErrDeliveryArchive
		}
		data, err := cp.Load()
		if err != nil || data.Binding != pending.Account {
			return ErrDeliveryArchive
		}
		byHash := map[string]string{}
		for key := range data.DeliveryBatches {
			byHash[batchHash(data.Binding, key)] = key
		}
		var rows []archivedBatch
		for _, ref := range pending.Rows {
			key, ok := byHash[ref.BatchHash]
			if !ok {
				return ErrDeliveryArchive
			}
			batch := data.DeliveryBatches[key]
			row := archivedBatch{Schema: 1, Account: data.Binding, Key: key, Fingerprint: batch.Fingerprint, IDs: batch.IDs, ArchivedAt: ref.ArchivedAt, NativeNext: ref.NativeNext}
			for _, id := range batch.IDs {
				job := data.Jobs[id]
				if !terminalBotJob(job) {
					return ErrDeliveryArchive
				}
				row.Jobs = append(row.Jobs, *job)
			}
			if outcomeHash(row) != ref.OutcomeHash {
				return ErrDeliveryArchive
			}
			rows = append(rows, row)
		}
		if err = a.storeRowsLocked(rows, data.Session.Bootstrap.Username, true); err != nil {
			return err
		}
	}
	for _, ref := range pending.Rows {
		row, err := a.indexed(pending.Account, "batch-hashes", ref.BatchHash)
		if err != nil || row == nil || outcomeHash(*row) != ref.OutcomeHash {
			return ErrDeliveryArchive
		}
	}
	err = a.db.View(func(tx *securedb.Tx) error {
		total, e := readArchiveCounts(tx, []byte("total"), false)
		if e != nil || !archiveCountsAtLeast(total, pending.AfterTotal) {
			return ErrDeliveryArchive
		}
		account, e := readArchiveCounts(tx, a.db.Key("counts", pending.Account), true)
		if e != nil || !archiveCountsAtLeast(account, pending.AfterAccount) {
			return ErrDeliveryArchive
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err = a.ensureGuard(); err != nil {
		return ErrDeliveryArchive
	}
	return a.clearPending()
}
