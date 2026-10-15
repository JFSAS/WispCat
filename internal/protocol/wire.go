package protocol

import (
	"encoding/binary"
	"errors"
)

var ErrWire = errors.New("invalid protocol encoding")

type Field struct {
	Wire   byte
	Number uint64
	Bytes  []byte
}
type Fields map[int][]Field

func Uint(tag int, value uint64) []byte {
	b := binary.AppendUvarint(nil, uint64(tag<<3))
	return binary.AppendUvarint(b, value)
}
func Blob(tag int, value []byte) []byte {
	b := binary.AppendUvarint(nil, uint64(tag<<3|2))
	b = binary.AppendUvarint(b, uint64(len(value)))
	return append(b, value...)
}
func Join(parts ...[]byte) []byte {
	var b []byte
	for _, p := range parts {
		b = append(b, p...)
	}
	return b
}
func Buffer(b []byte) []byte { return Join(Uint(1, uint64(len(b))), Blob(2, b)) }
func String(b []byte) []byte { return Blob(1, b) }
func Parse(b []byte) (Fields, error) {
	if len(b) > 16<<20 {
		return nil, ErrWire
	}
	f := make(Fields)
	for len(b) > 0 {
		k, n := binary.Uvarint(b)
		if n <= 0 || k>>3 == 0 || k>>3 > 0x1fffffff {
			return nil, ErrWire
		}
		b = b[n:]
		v := Field{Wire: byte(k & 7)}
		switch v.Wire {
		case 0:
			v.Number, n = binary.Uvarint(b)
			if n <= 0 {
				return nil, ErrWire
			}
			b = b[n:]
		case 1, 2, 5:
			var size uint64
			if v.Wire == 2 {
				size, n = binary.Uvarint(b)
				if n <= 0 {
					return nil, ErrWire
				}
				b = b[n:]
			} else if v.Wire == 1 {
				size = 8
			} else {
				size = 4
			}
			if size > uint64(len(b)) {
				return nil, ErrWire
			}
			v.Bytes = b[:int(size)]
			b = b[int(size):]
		default:
			return nil, ErrWire
		}
		f[int(k>>3)] = append(f[int(k>>3)], v)
	}
	return f, nil
}
func (f Fields) N(tag int) (uint64, bool) {
	a := f[tag]
	for i := len(a) - 1; i >= 0; i-- {
		if a[i].Wire == 0 {
			return a[i].Number, true
		}
	}
	return 0, false
}
func (f Fields) B(tag int) ([]byte, bool) {
	a := f[tag]
	for i := len(a) - 1; i >= 0; i-- {
		if a[i].Wire == 2 {
			return a[i].Bytes, true
		}
	}
	return nil, false
}
func (f Fields) All(tag int) [][]byte {
	var r [][]byte
	for _, v := range f[tag] {
		if v.Wire == 2 {
			r = append(r, v.Bytes)
		}
	}
	return r
}
func (f Fields) Message(tag int) (Fields, error) { return Parse(Join(f.All(tag)...)) }
func Unbuffer(b []byte) ([]byte, error) {
	f, e := Parse(b)
	if e != nil {
		return nil, e
	}
	n, ok := f.N(1)
	v, _ := f.B(2)
	if !ok || n != uint64(len(v)) {
		return nil, ErrWire
	}
	return v, nil
}
func Unstring(b []byte) ([]byte, error) {
	f, e := Parse(b)
	if e != nil {
		return nil, e
	}
	v, _ := f.B(1)
	return v, nil
}
