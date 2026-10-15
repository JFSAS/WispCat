package protocol

import (
	"crypto/md5"
	"encoding/binary"
	"hash/adler32"
)

type Header struct {
	Version, UIN            uint32
	Command                 uint16
	Original, Transformed   uint32
	Cookie                  []byte
	Compression, Encryption byte
	Unknown37, Config       uint16
	Checksum, Notice        uint32
	Flag                    byte
	Sequence                uint16
}

func (h Header) Encode() ([]byte, error) {
	if len(h.Cookie) > 15 || h.Compression > 3 || h.Encryption > 15 {
		return nil, ErrWire
	}
	b := []byte{0xbf, h.Compression, h.Encryption<<4 | byte(len(h.Cookie))}
	b = binary.BigEndian.AppendUint32(b, h.Version)
	b = binary.BigEndian.AppendUint32(b, h.UIN)
	b = append(b, h.Cookie...)
	for _, n := range []uint64{uint64(h.Command), uint64(h.Original), uint64(h.Transformed), uint64(h.Unknown37), uint64(h.Config), uint64(h.Checksum)} {
		b = binary.AppendUvarint(b, n)
	}
	b = append(b, h.Flag)
	b = binary.AppendUvarint(b, uint64(h.Notice))
	b = append(b, 0, 0, 0)
	b = binary.BigEndian.AppendUint16(b, h.Sequence)
	if len(b) > 63 {
		return nil, ErrWire
	}
	b[1] = byte(len(b))<<2 | h.Compression
	return b, nil
}
func DecodeHeader(b []byte) (Header, int, error) {
	var h Header
	if len(b) < 2 {
		return h, 0, ErrWire
	}
	extended := b[0] == 0xbf
	at := 0
	if extended {
		at = 1
	}
	if len(b) < at+10 {
		return h, 0, ErrWire
	}
	size := int(b[at] >> 2)
	if size < at+10 || size > len(b) {
		return h, 0, ErrWire
	}
	end := b[:size]
	h.Compression = end[at] & 3
	h.Encryption = end[at+1] >> 4
	cl := int(end[at+1] & 15)
	at += 2
	h.Version = binary.BigEndian.Uint32(end[at:])
	h.UIN = binary.BigEndian.Uint32(end[at+4:])
	at += 8
	if cl > size-at {
		return h, 0, ErrWire
	}
	h.Cookie = append([]byte(nil), end[at:at+cl]...)
	at += cl
	next := func(bits uint) (uint64, error) {
		v, n := binary.Uvarint(end[at:])
		if n <= 0 || (bits < 64 && v >= uint64(1)<<bits) {
			return 0, ErrWire
		}
		at += n
		return v, nil
	}
	var values [5]uint64
	for i, bits := range []uint{16, 32, 32, 16, 16} {
		if i == 4 && at == size {
			break
		}
		v, e := next(bits)
		if e != nil {
			return h, 0, e
		}
		values[i] = v
	}
	h.Command = uint16(values[0])
	h.Original = uint32(values[1])
	h.Transformed = uint32(values[2])
	h.Unknown37 = uint16(values[3])
	h.Config = uint16(values[4])
	h.Flag = 255
	if extended && at < size {
		v, e := next(32)
		if e != nil {
			return h, 0, e
		}
		h.Checksum = uint32(v)
	}
	if extended && at < size {
		h.Flag = end[at]
		at++
	}
	if extended && at < size {
		v, e := next(32)
		if e != nil {
			return h, 0, e
		}
		h.Notice = uint32(v)
	}
	if extended && at < size {
		at++
	}
	if extended && at < size {
		at++
	}
	if extended && at < size {
		if _, e := next(16); e != nil {
			return h, 0, e
		}
	}
	if extended && at < size {
		if size-at < 2 {
			return h, 0, ErrWire
		}
		h.Sequence = binary.BigEndian.Uint16(end[at:])
		at += 2
	}
	if at != size {
		return h, 0, ErrWire
	}
	return h, size, nil
}
func Checksum(uin uint32, b []byte) uint32 {
	a := md5.Sum(binary.BigEndian.AppendUint32(nil, uin))
	seed := binary.BigEndian.AppendUint32(nil, uint32(len(b)))
	seed = append(seed, a[:]...)
	s := md5.Sum(seed)
	return adler32.Checksum(append(s[:], b...))
}
