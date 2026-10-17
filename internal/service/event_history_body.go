package service

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"

	"github.com/JFSAS/wispcat/internal/securedb"
	"github.com/JFSAS/wispcat/pkg/bot"
)

const historyChunkBytes = 512 << 10
const historyMaxBodyBytes = 128 << 20

type historyBody struct {
	Schema int    `json:"schema"`
	Bytes  int    `json:"bytes"`
	Chunks int    `json:"chunks"`
	Digest string `json:"digest"`
}

type chunkedRecord struct {
	bucket      string
	key         []byte
	chunkBucket string
	chunkKey    func(int) []byte
}

func (h *EventHistory) bodyLocation(account, id string) chunkedRecord {
	return chunkedRecord{bucket: "messages", key: h.db.Key("message", account, id), chunkBucket: "bodies", chunkKey: func(i int) []byte { return h.db.Key("body", account, id, strconv.Itoa(i)) }}
}
func (h *EventHistory) readBody(tx *securedb.Tx, account, id string) ([]byte, error) {
	return h.bodyLocation(account, id).read(tx)
}
func (h *EventHistory) writeBody(tx *securedb.Tx, account, id string, raw []byte) error {
	return h.bodyLocation(account, id).write(tx, raw)
}
func (record chunkedRecord) read(tx *securedb.Tx) ([]byte, error) {
	return record.readLimit(tx, historyMaxBodyBytes)
}
func (record chunkedRecord) readLimit(tx *securedb.Tx, limit int) ([]byte, error) {
	raw, err := tx.Get(record.bucket, record.key)
	if err != nil || raw == nil {
		return nil, err
	}
	var ref historyBody
	if json.Unmarshal(raw, &ref) != nil || ref.Schema != 1 || ref.Bytes < 1 || ref.Bytes > historyMaxBodyBytes || ref.Chunks != (ref.Bytes+historyChunkBytes-1)/historyChunkBytes || !mediaID(ref.Digest, 64) {
		return nil, ErrEventHistory
	}
	if ref.Bytes > limit {
		return nil, bot.ErrLimit
	}
	body := make([]byte, 0, ref.Bytes)
	for i := 0; i < ref.Chunks; i++ {
		chunk, err := tx.Get(record.chunkBucket, record.chunkKey(i))
		expected := min(historyChunkBytes, ref.Bytes-len(body))
		if err != nil || len(chunk) != expected {
			return nil, ErrEventHistory
		}
		body = append(body, chunk...)
	}
	digest := sha256.Sum256(body)
	if hex.EncodeToString(digest[:]) != ref.Digest {
		return nil, ErrEventHistory
	}
	return body, nil
}
func (record chunkedRecord) write(tx *securedb.Tx, raw []byte) error {
	if len(raw) < 1 || len(raw) > historyMaxBodyBytes {
		return ErrEventHistory
	}
	digest := sha256.Sum256(raw)
	ref := historyBody{Schema: 1, Bytes: len(raw), Chunks: (len(raw) + historyChunkBytes - 1) / historyChunkBytes, Digest: hex.EncodeToString(digest[:])}
	for i := 0; i < ref.Chunks; i++ {
		start := i * historyChunkBytes
		if err := tx.Put(record.chunkBucket, record.chunkKey(i), raw[start:min(start+historyChunkBytes, len(raw))]); err != nil {
			return err
		}
	}
	value, _ := json.Marshal(ref)
	return tx.Put(record.bucket, record.key, value)
}
