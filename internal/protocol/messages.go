package protocol

import (
	"bytes"
	"sort"
	"unicode/utf8"
)

func ParseContact(b []byte, deleted bool) (Contact, error) {
	var c Contact
	f, e := Parse(b)
	if e != nil {
		return c, e
	}
	raw, ok := f.B(1)
	if !ok {
		return c, ErrWire
	}
	c.Username, e = Unstring(raw)
	if e != nil || len(c.Username) == 0 || len(c.Username) > 512 || !utf8.Valid(c.Username) || bytes.IndexByte(c.Username, 0) >= 0 {
		return c, ErrWire
	}
	c.Deleted = deleted
	if deleted {
		return c, nil
	}
	for tag, target := range map[int]**string{2: &c.Nickname, 10: &c.Remark} {
		parts := f.All(tag)
		if len(parts) > 0 {
			v, e := Unstring(Join(parts...))
			if e != nil || !utf8.Valid(v) {
				return c, ErrWire
			}
			s := string(v)
			*target = &s
		}
	}
	return c, nil
}
func DecodeSync(b []byte) (SyncResult, error) {
	var r SyncResult
	f, e := Parse(b)
	if e != nil {
		return r, e
	}
	ret, ok := f.N(1)
	if !ok {
		return r, ErrWire
	}
	if int32(ret) != 0 {
		return r, &Error{"sync_business_error", int32(ret)}
	}
	continued, cok := f.N(3)
	key, kok := f.B(4)
	cmds, lok := f.B(2)
	if !cok || !kok || !lok {
		return r, ErrWire
	}
	r.Continue = uint32(continued)
	r.Cursor, e = Unbuffer(key)
	if e != nil {
		return r, e
	}
	list, e := Parse(cmds)
	if e != nil {
		return r, e
	}
	count, present := list.N(1)
	items := list.All(2)
	if !present || count != uint64(len(items)) {
		return r, ErrWire
	}
	for _, item := range items {
		v, e := Parse(item)
		if e != nil {
			return r, e
		}
		code, p := v.N(1)
		buf, q := v.B(2)
		if !p || !q {
			return r, ErrWire
		}
		payload, e := Unbuffer(buf)
		if e != nil {
			return r, e
		}
		switch code {
		case 2, 17, 4:
			c, e := ParseContact(payload, code == 4)
			if e != nil {
				return r, e
			}
			r.Contacts = append(r.Contacts, c)
		case 5:
			m, e := Parse(payload)
			if e != nil {
				return r, e
			}
			for tag := 1; tag <= 9; tag++ {
				if len(m[tag]) == 0 {
					return r, ErrWire
				}
			}
			var values [3][]byte
			for i, tag := range []int{2, 3, 5} {
				s, ok := m.B(tag)
				if !ok {
					return r, ErrWire
				}
				values[i], e = Unstring(s)
				if e != nil {
					return r, e
				}
			}
			t, _ := m.N(4)
			when, _ := m.N(9)
			id, _ := m.N(12)
			source, _ := m.B(10)
			r.Messages = append(r.Messages, Message{Sender: values[0], Recipient: values[1], Content: values[2], Type: uint32(t), Timestamp: uint32(when), ServerID: id, Source: bytes.Clone(source)})
		}
	}
	return r, nil
}
func MergeCursor(old, incoming []byte) ([]byte, error) {
	values := map[uint32]uint32{}
	for _, b := range [][]byte{old, incoming} {
		if len(b) == 0 {
			continue
		}
		f, e := Parse(b)
		if e != nil {
			return nil, e
		}
		count, ok := f.N(1)
		items := f.All(2)
		if !ok || count != uint64(len(items)) || len(items) > 10000 {
			return nil, ErrWire
		}
		seen := map[uint32]bool{}
		for _, item := range items {
			v, e := Parse(item)
			if e != nil {
				return nil, e
			}
			k, ok := v.N(1)
			n, p := v.N(2)
			if !ok || !p || k > 0xffffffff || n > 0xffffffff || seen[uint32(k)] {
				return nil, ErrWire
			}
			seen[uint32(k)] = true
			if uint32(n) > values[uint32(k)] {
				values[uint32(k)] = uint32(n)
			} else if _, ok := values[uint32(k)]; !ok {
				values[uint32(k)] = uint32(n)
			}
		}
	}
	if len(incoming) == 0 {
		return nil, ErrWire
	}
	keys := make([]uint32, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	out := Uint(1, uint64(len(keys)))
	for _, k := range keys {
		out = append(out, Blob(2, Join(Uint(1, uint64(k)), Uint(2, uint64(values[k]))))...)
	}
	return out, nil
}
func DecodeReceipt(b []byte, recipient []byte) (Receipt, error) {
	var r Receipt
	f, e := Parse(b)
	if e != nil {
		return r, e
	}
	count, ok := f.N(2)
	items := f.All(3)
	if !ok || count != 1 || len(items) != 1 {
		return r, ErrWire
	}
	v, e := Parse(items[0])
	if e != nil {
		return r, e
	}
	for i := 1; i <= 7; i++ {
		if len(v[i]) == 0 {
			return r, ErrWire
		}
	}
	ret, _ := v.N(1)
	name, ok := v.B(2)
	if !ok {
		return r, ErrWire
	}
	peer, e := Unstring(name)
	if e != nil || !bytes.Equal(peer, recipient) {
		return r, ErrWire
	}
	if int32(ret) != 0 {
		return r, &Error{"send_item_rejected", int32(ret)}
	}
	id, p := v.N(8)
	when, _ := v.N(5)
	if !p || id == 0 {
		return r, ErrWire
	}
	return Receipt{id, uint32(when)}, nil
}
