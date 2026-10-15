package service

import (
	"encoding/json"
	"math"

	"github.com/JFSAS/wechat-runtime/internal/securedb"
)

type nativeLedger struct {
	Schema int    `json:"schema"`
	Next   uint64 `json:"next"`
}

func (a *DeliveryArchive) nativeLedger(tx *securedb.Tx, account string, seed uint32) (nativeLedger, error) {
	key := a.db.Key("native", account)
	raw, err := tx.Get("native", key)
	if err != nil {
		return nativeLedger{}, err
	}
	if raw != nil {
		var ledger nativeLedger
		if json.Unmarshal(raw, &ledger) != nil || ledger.Schema != 1 || ledger.Next == 0 {
			return ledger, ErrDeliveryArchive
		}
		counts, err := readArchiveCounts(tx, a.db.Key("counts", account), true)
		if err != nil || ledger.Next <= counts.NativeReservations {
			return ledger, ErrDeliveryArchive
		}
		return ledger, nil
	}
	counts, err := readArchiveCounts(tx, a.db.Key("counts", account), true)
	if err != nil || counts.NativeReservations != 0 || counts.Batches != 0 {
		return nativeLedger{}, ErrDeliveryArchive
	}
	if seed == 0 || seed > 0x7fffffff {
		return nativeLedger{}, ErrDeliveryArchive
	}
	ledger := nativeLedger{Schema: 1, Next: uint64(seed)}
	raw, _ = json.Marshal(ledger)
	if err = tx.Put("native", key, raw); err != nil {
		return ledger, err
	}
	return ledger, nil
}

// reserveNative keeps the next native counter outside a checkpoint that may be
// restored from an older backup. Unused reservations remain gaps on failure.
// Wire values keep the original positive uint31 shape and nonwrapping batches.
func (a *DeliveryArchive) reserveNative(account string, seed uint32, count int) (uint32, error) {
	if count < 1 || count > 20 {
		return 0, ErrDeliveryArchive
	}
	if seed == 0 {
		seed = 1
	}
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	if a.closed {
		return 0, ErrDeliveryArchive
	}
	if err := a.resolvePendingLocked(); err != nil {
		return 0, err
	}
	var start uint32
	err := a.db.Update(func(tx *securedb.Tx) error {
		ledger, e := a.nativeLedger(tx, account, seed)
		if e != nil {
			return e
		}
		const maxCounter = uint64(0x7fffffff)
		if ledger.Next > math.MaxUint64-maxCounter-uint64(count) {
			return ErrDeliveryArchive
		}
		wire := (ledger.Next-1)%maxCounter + 1
		if wire+uint64(count)-1 > maxCounter {
			ledger.Next += maxCounter - wire + 1
			wire = 1
		}
		start = uint32(wire)
		ledger.Next += uint64(count)
		raw, _ := json.Marshal(ledger)
		if e = tx.Put("native", a.db.Key("native", account), raw); e != nil {
			return e
		}
		for _, key := range [][]byte{[]byte("total"), a.db.Key("counts", account)} {
			counts, e := readArchiveCounts(tx, key, string(key) != "total")
			if e != nil || counts.NativeReservations > math.MaxUint64-uint64(count) {
				return ErrDeliveryArchive
			}
			counts.NativeReservations += uint64(count)
			if e = putArchiveCounts(tx, key, counts); e != nil {
				return e
			}
		}
		return nil
	})
	if err != nil {
		return 0, ErrDeliveryArchive
	}
	if err = a.ensureGuard(); err != nil {
		return 0, ErrDeliveryArchive
	}
	return start, nil
}
