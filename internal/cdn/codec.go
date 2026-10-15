package cdn

import (
	"encoding/binary"
	"math"
)

const (
	HeaderBytes      = 25
	DownloadCommand  = 20000
	DefaultMaxBytes  = 16 << 20
	MaxMetadataBytes = 128 << 10
	maxFields        = 128
	maxKeyBytes      = 128
)

type field struct {
	key   string
	value []byte
}

func appendField(dst []byte, key string, value []byte) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(key)))
	dst = append(dst, key...)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(value)))
	return append(dst, value...)
}

// makeFrame follows 5778cc8: uin is the one host-endian field, while the two
// lengths and command use network byte order. Native ARM64 is little endian.
func makeFrame(body []byte, command uint16, uin uint32) ([]byte, error) {
	if uint64(len(body)) > math.MaxUint32-HeaderBytes {
		return nil, fail("size_limit")
	}
	frame := make([]byte, HeaderBytes, HeaderBytes+len(body))
	frame[0] = 0xab
	binary.BigEndian.PutUint32(frame[1:5], uint32(HeaderBytes+len(body)))
	binary.BigEndian.PutUint16(frame[5:7], command)
	binary.LittleEndian.PutUint32(frame[7:11], uin)
	binary.BigEndian.PutUint32(frame[21:25], uint32(len(body)))
	return append(frame, body...), nil
}

func frameBody(frame []byte, limit int64) ([]byte, error) {
	if len(frame) < HeaderBytes || frame[0] != 0xab {
		return nil, fail("invalid_frame")
	}
	total := uint64(binary.BigEndian.Uint32(frame[1:5]))
	body := uint64(binary.BigEndian.Uint32(frame[21:25]))
	if total != body+HeaderBytes || total != uint64(len(frame)) {
		return nil, fail("invalid_frame")
	}
	if body > uint64(limit)+MaxMetadataBytes+16 {
		return nil, fail("size_limit")
	}
	return frame[HeaderBytes:], nil
}

// parseBody implements 577a228 / 577b258. At filedata, native DecodeVariant
// computes payload length from the entire remaining frame, not the declared
// value length. That value is only a lower-bound consistency check. Other
// fields use their declared length. Rejecting declared>remaining is stricter
// than the native diagnostic-only assertion and never truncates accepted data.
func parseBody(body []byte) (map[string][]byte, []byte, error) {
	metadata := make(map[string][]byte)
	consumed := 0
	for len(body) > 0 {
		if len(metadata) >= maxFields || len(body) < 8 {
			return nil, nil, fail("invalid_tlv")
		}
		keyLen := uint64(binary.BigEndian.Uint32(body[:4]))
		if keyLen == 0 || keyLen > maxKeyBytes || keyLen+8 > uint64(len(body)) {
			return nil, nil, fail("invalid_tlv")
		}
		key := string(body[4 : 4+keyLen])
		if !validKey(key) {
			return nil, nil, fail("invalid_tlv")
		}
		valueLen := uint64(binary.BigEndian.Uint32(body[4+keyLen : 8+keyLen]))
		payload := body[8+keyLen:]
		if valueLen > uint64(len(payload)) {
			return nil, nil, fail("invalid_tlv")
		}
		if key == "filedata" {
			return metadata, payload, nil
		}
		if _, exists := metadata[key]; exists {
			return nil, nil, fail("duplicate_field")
		}
		consumed += int(keyLen + valueLen + 8)
		if consumed > MaxMetadataBytes {
			return nil, nil, fail("size_limit")
		}
		metadata[key] = payload[:valueLen]
		body = payload[valueLen:]
	}
	return metadata, nil, nil
}

func validKey(key string) bool {
	for i := range len(key) {
		c := key[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return false
		}
	}
	return true
}
