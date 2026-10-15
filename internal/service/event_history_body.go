package service

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"

	"github.com/JFSAS/wechat-runtime/internal/securedb"
)

const historyChunkBytes = 512 << 10
const historyMaxBodyBytes = 128 << 20

type historyBody struct {
	Schema int    `json:"schema"`
	Bytes  int    `json:"bytes"`
	Chunks int    `json:"chunks"`
	Digest string `json:"digest"`
}

func (h *EventHistory) readBody(tx *securedb.Tx, account, id string) ([]byte, error) {
	raw, err := tx.Get("messages", h.db.Key("message", account, id))
	if err != nil || raw == nil {
		return nil, err
	}
	var ref historyBody
	if json.Unmarshal(raw, &ref) != nil || ref.Schema != 1 || ref.Bytes < 1 || ref.Bytes > historyMaxBodyBytes || ref.Chunks != (ref.Bytes+historyChunkBytes-1)/historyChunkBytes || !mediaID(ref.Digest, 64) {
		return nil, ErrEventHistory
	}
	body := make([]byte, 0, ref.Bytes)
	for i := 0; i < ref.Chunks; i++ {
		chunk, err := tx.Get("bodies", h.db.Key("body", account, id, strconv.Itoa(i)))
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
func (h *EventHistory) writeBody(tx *securedb.Tx, account, id string, raw []byte) error {
	if len(raw) < 1 || len(raw) > historyMaxBodyBytes {
		return ErrEventHistory
	}
	digest := sha256.Sum256(raw)
	ref := historyBody{Schema: 1, Bytes: len(raw), Chunks: (len(raw) + historyChunkBytes - 1) / historyChunkBytes, Digest: hex.EncodeToString(digest[:])}
	for i := 0; i < ref.Chunks; i++ {
		start := i * historyChunkBytes
		if err := tx.Put("bodies", h.db.Key("body", account, id, strconv.Itoa(i)), raw[start:min(start+historyChunkBytes, len(raw))]); err != nil {
			return err
		}
	}
	value, _ := json.Marshal(ref)
	return tx.Put("messages", h.db.Key("message", account, id), value)
}
