package service

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"

	"github.com/JFSAS/wispcat/internal/messaging"
	"github.com/JFSAS/wispcat/internal/protocol"
	"github.com/JFSAS/wispcat/internal/securedb"
)

const historyWorkingEntries = 512
const historyWorkingBytes = 4 << 20

func rawDigest(message protocol.Message) string {
	raw, _ := json.Marshal(message)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func (h *EventHistory) privateFactLocation(bucket, account, id string) chunkedRecord {
	return chunkedRecord{bucket: bucket, key: h.db.Key(bucket, account, id), chunkBucket: "native-fact-bodies", chunkKey: func(i int) []byte { return h.db.Key("native-fact", bucket, account, id, strconv.Itoa(i)) }}
}
func (h *EventHistory) putPrivateFact(tx *securedb.Tx, directory *directoryMeta, bucket, id string, raw []byte) error {
	record := h.privateFactLocation(bucket, directory.Account, id)
	old, err := record.read(tx)
	if err != nil {
		return err
	}
	if old != nil {
		if !bytes.Equal(old, raw) {
			return ErrEventHistory
		}
		index, err := tx.Get("native-fact-ids", append(h.db.Key("native-fact-ids", bucket, directory.Account), h.db.Key("id", id)...))
		expected, _ := json.Marshal(id)
		if err != nil || !bytes.Equal(index, expected) {
			return ErrEventHistory
		}
		return nil
	}
	if directory.Revision == ^uint64(0) {
		return ErrEventHistory
	}
	directory.Revision++
	if bucket == "raw-variants" {
		directory.Variants++
	} else {
		directory.Fragments++
	}
	index, _ := json.Marshal(id)
	if err := tx.Put("native-fact-ids", append(h.db.Key("native-fact-ids", bucket, directory.Account), h.db.Key("id", id)...), index); err != nil {
		return err
	}
	return record.write(tx, raw)
}
func (h *EventHistory) retainVariant(tx *securedb.Tx, directory *directoryMeta, id string, message protocol.Message) error {
	raw, _ := json.Marshal(message)
	return h.putPrivateFact(tx, directory, "raw-variants", id+"/"+rawDigest(message), raw)
}
func (h *EventHistory) retainFragment(tx *securedb.Tx, directory *directoryMeta, event messaging.Event) error {
	raw, _ := json.Marshal(event)
	sum := sha256.Sum256(raw)
	return h.putPrivateFact(tx, directory, "event-fragments", hex.EncodeToString(sum[:]), raw)
}
func (h *EventHistory) compactWorkingLocked(s *Service) error {
	keepMessages := make([]bool, len(s.data.Messages))
	keepEvents := make([]bool, len(s.data.Events))
	kept := map[string]bool{}
	// Reserve JSON array/object delimiters too. Counting each record with a
	// trailing comma then conservatively overestimates the final serialized size.
	count, used := 0, len(`{"Messages":[],"Events":[]}`)
	for i := len(s.data.Messages) - 1; i >= 0; i-- {
		raw, _ := json.Marshal(s.data.Messages[i])
		if count < historyWorkingEntries && used+len(raw)+1 <= historyWorkingBytes {
			keepMessages[i] = true
			count++
			used += len(raw) + 1
			kept[messageKey(s.data.Messages[i], s.data.Session.Bootstrap.Username)] = true
		}
	}
	count = 0
	for i := len(s.data.Events) - 1; i >= 0; i-- {
		raw, _ := json.Marshal(s.data.Events[i])
		if kept[s.data.Events[i].Message] && count < historyWorkingEntries && used+len(raw)+1 <= historyWorkingBytes {
			keepEvents[i] = true
			count++
			used += len(raw) + 1
		}
	}
	// Never drop the last readable native fact unless its complete encrypted copy
	// is verified. Fragments preserve legacy events that have no surviving body.
	err := h.db.View(func(tx *securedb.Tx) error {
		for i, message := range s.data.Messages {
			if keepMessages[i] {
				continue
			}
			id := messageKey(message, s.data.Session.Bootstrap.Username)
			row, err := h.record(tx, s.data.Binding, id)
			if err != nil || row == nil {
				return ErrEventHistory
			}
			expected, _ := json.Marshal(message)
			stored, _ := json.Marshal(row.Raw)
			if bytes.Equal(expected, stored) {
				continue
			}
			variant, err := h.privateFactLocation("raw-variants", s.data.Binding, id+"/"+rawDigest(message)).read(tx)
			if err != nil || !bytes.Equal(expected, variant) {
				return ErrEventHistory
			}
		}
		for i, event := range s.data.Events {
			if keepEvents[i] {
				continue
			}
			raw, _ := json.Marshal(event)
			sum := sha256.Sum256(raw)
			fragment, err := h.privateFactLocation("event-fragments", s.data.Binding, hex.EncodeToString(sum[:])).read(tx)
			if err != nil || !bytes.Equal(raw, fragment) {
				return ErrEventHistory
			}
		}
		return nil
	})
	if err != nil {
		return ErrEventHistory
	}
	messages := make([]protocol.Message, 0, len(kept))
	events := make([]messaging.Event, 0, count)
	for i, message := range s.data.Messages {
		if keepMessages[i] {
			messages = append(messages, message)
		}
	}
	for i, event := range s.data.Events {
		if keepEvents[i] {
			events = append(events, event)
		}
	}
	s.data.Messages, s.data.Events = messages, events
	h.workingBytes = used
	return nil
}
