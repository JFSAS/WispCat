package service

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/JFSAS/wispcat/internal/store"
)

type botIntent struct {
	Schema      int      `json:"schema"`
	Account     string   `json:"account"`
	BatchHash   string   `json:"batch_hash"`
	Fingerprint string   `json:"fingerprint"`
	IDs         []string `json:"ids"`
}

func batchHash(account, key string) string {
	hash := sha256.Sum256([]byte(account + "\x00" + key))
	return hex.EncodeToString(hash[:])
}
func writeBotIntent(directory, account, key, fingerprint string, ids []string) (string, error) {
	intent := botIntent{Schema: 1, Account: account, BatchHash: batchHash(account, key), Fingerprint: fingerprint, IDs: ids}
	path := filepath.Join(directory, "bot-intents", intent.BatchHash+".json")
	raw, _ := json.Marshal(intent)
	if err := store.Atomic(path, raw); err != nil {
		return path, err
	}
	return path, nil
}
func clearBotIntent(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
func checkpointDefinitelyExcludes(cp store.Checkpoint, key, fingerprint string) bool {
	if info, err := os.Stat(cp.Directory); err == nil && !info.IsDir() {
		return true
	}
	data, err := cp.Load()
	if err != nil {
		return false
	}
	batch, exists := data.DeliveryBatches[key]
	return !exists || batch.Fingerprint != fingerprint
}

// RecoverBotIntents runs before a channel worker starts. An intent survives
// only an uncertain queue commit/cleanup; queued parts cannot be replayed until
// the checkpoint has been reconciled and durably saved in a conservative state.
func RecoverBotIntents(directory string, archives ...*DeliveryArchive) error {
	root := filepath.Join(directory, "bot-intents")
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return errors.New("delivery_intent_directory_unavailable")
	}
	if len(entries) > 10000 {
		return errors.New("delivery_intent_limit")
	}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), ".write-") {
			continue
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			return errors.New("delivery_intent_invalid")
		}
		path := filepath.Join(root, entry.Name())
		raw, err := store.ReadPrivate(path, 16384)
		var intent botIntent
		if err != nil || json.Unmarshal(raw, &intent) != nil || intent.Schema != 1 || !mediaID(intent.Account, 64) || !mediaID(intent.BatchHash, 64) || entry.Name() != intent.BatchHash+".json" || !mediaID(intent.Fingerprint, 64) || len(intent.IDs) == 0 || len(intent.IDs) > 20 {
			return errors.New("delivery_intent_invalid")
		}
		for _, id := range intent.IDs {
			if !mediaID(id, 32) {
				return errors.New("delivery_intent_invalid")
			}
		}
		cp, exists, err := store.Unique(filepath.Join(directory, "checkpoints"))
		if err != nil || !exists {
			return errors.New("delivery_intent_checkpoint_unavailable")
		}
		data, err := cp.Load()
		if err != nil || data.Binding != intent.Account {
			return errors.New("delivery_intent_account_mismatch")
		}
		found := false
		for key, batch := range data.DeliveryBatches {
			if batchHash(intent.Account, key) != intent.BatchHash {
				continue
			}
			if batch.Fingerprint != intent.Fingerprint || !reflect.DeepEqual(batch.IDs, intent.IDs) {
				return errors.New("delivery_intent_conflict")
			}
			found = true
			for _, id := range batch.IDs {
				job := data.Jobs[id]
				if job == nil {
					return errors.New("delivery_intent_missing_part")
				}
				if job.Status == "accepted" || job.Status == "echo_observed" {
					if job.ServerID != 0 {
						continue
					}
				}
				if job.Status == "failed" || job.Status == "cancelled" || (job.Status == "unknown" && job.ErrorCategory != nil) {
					continue
				}
				job.Status = "unknown"
				category := "queue_commit_uncertain"
				job.ErrorCategory = &category
			}
			if err = cp.Save(data); err != nil {
				return errors.New("delivery_intent_recovery_not_durable")
			}
			break
		}
		if !found {
			archived := false
			if len(archives) > 0 && archives[0] != nil {
				row, e := archives[0].indexed(intent.Account, "batch-hashes", intent.BatchHash)
				if e != nil {
					return e
				}
				if row != nil {
					if row.Fingerprint != intent.Fingerprint || !reflect.DeepEqual(row.IDs, intent.IDs) {
						return errors.New("delivery_intent_archive_conflict")
					}
					archived = true
				}
			}
			cache := store.MediaCache{Directory: filepath.Join(directory, "media"), Binding: intent.Account}
			for _, id := range intent.IDs {
				if data.Jobs[id] != nil {
					return errors.New("delivery_intent_orphan_conflict")
				}
				if !archived {
					if err = cache.Remove(id); err != nil && !errors.Is(err, os.ErrNotExist) {
						return errors.New("delivery_intent_cleanup_failed")
					}
				}
			}
		}
		if err = clearBotIntent(path); err != nil {
			return errors.New("delivery_intent_cleanup_failed")
		}
	}
	return nil
}
