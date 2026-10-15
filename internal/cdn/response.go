package cdn

import (
	"crypto/md5"
	"crypto/subtle"
	"sort"
	"strconv"
)

// DecodeResponse validates one complete legacy stream frame without networking.
// filedata consumes all remaining frame bytes per native DecodeVariant. A
// successful result requires a raw MD5 from the server or an expected MD5 from
// the caller. Missing integrity material is an error, not a claimed download.
func DecodeResponse(frame []byte, request DownloadRequest, seq uint32, maxBytes int64) (*Result, error) {
	if maxBytes == 0 {
		maxBytes = DefaultMaxBytes
	}
	if maxBytes < 1 || maxBytes > 1<<30 {
		return nil, fail("invalid_limit")
	}
	body, err := frameBody(frame, maxBytes)
	if err != nil {
		return nil, err
	}
	metadata, ciphertext, err := parseBody(body)
	if err != nil {
		return nil, err
	}
	if _, ok := metadata["meta"]; ok {
		// The admitted profile has no meta key. Do not substitute an unrelated
		// WeChat login or attachment AES key for a different protocol key.
		return nil, fail("unsupported_meta_encryption")
	}
	ret, err := parseSigned(metadata["retcode"])
	if err != nil {
		return nil, fail("missing_status")
	}
	if ret != 0 {
		return nil, &Error{Category: "server_error", Code: ret}
	}
	gotSeq, err := parseUnsigned(metadata["seq"])
	if err != nil || seq == 0 || gotSeq != uint64(seq) {
		return nil, fail("sequence_mismatch")
	}
	if ciphertext == nil {
		return nil, fail("missing_filedata")
	}
	if len(ciphertext) > int(maxBytes)+16 {
		return nil, fail("size_limit")
	}
	rawSize := request.RawSize
	serverRawSize := false
	if value, ok := metadata["rawfilesize"]; ok {
		serverRawSize = true
		rawSize, err = parseUnsigned(value)
		if err != nil {
			return nil, fail("invalid_raw_size")
		}
	}
	if rawSize > uint64(maxBytes) {
		return nil, fail("size_limit")
	}
	if serverRawSize && uint64(len(ciphertext)) != (rawSize&^15)+16 {
		return nil, &Error{Category: "cipher_size_mismatch", ExpectedBytes: (rawSize &^ 15) + 16, ReceivedBytes: uint64(len(ciphertext))}
	}
	if value, ok := metadata["totalsize"]; ok {
		total, err := parseUnsigned(value)
		if err != nil || total != uint64(len(ciphertext)) {
			return nil, fail("cipher_size_mismatch")
		}
	}
	plain, err := decryptContent(ciphertext, request.AESKey)
	if err != nil {
		return nil, err
	}
	if serverRawSize && uint64(len(plain)) != rawSize {
		return nil, fail("raw_size_mismatch")
	}
	if !serverRawSize {
		// Some image responses advertise only encrypted totalsize. The XML
		// length is a hint, so derive the raw length from validated PKCS padding
		// and still require the server or caller's plaintext MD5 below.
		rawSize = uint64(len(plain))
		if rawSize > uint64(maxBytes) {
			return nil, fail("size_limit")
		}
	}
	var expected [16]byte
	var hasExpected bool
	if value, ok := metadata["rawfilemd5"]; ok && len(value) > 0 {
		expected, err = parseMD5(value)
		if err != nil {
			return nil, err
		}
		hasExpected = true
	}
	if request.ExpectedMD5 != "" {
		callerDigest, digestErr := parseMD5([]byte(request.ExpectedMD5))
		if digestErr != nil {
			return nil, digestErr
		}
		if hasExpected && subtle.ConstantTimeCompare(callerDigest[:], expected[:]) != 1 {
			return nil, fail("checksum_mismatch")
		}
		expected, hasExpected = callerDigest, true
	}
	if !hasExpected {
		return nil, fail("missing_checksum")
	}
	actual := md5.Sum(plain)
	if subtle.ConstantTimeCompare(actual[:], expected[:]) != 1 {
		return nil, fail("checksum_mismatch")
	}
	keys := make([]string, 0, len(metadata))
	for key := range metadata {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return &Result{Data: plain, RawSize: rawSize, CipherBytes: len(ciphertext), ChecksumVerified: true, MetadataKeys: keys}, nil
}

func parseSigned(value []byte) (int64, error) {
	if len(value) == 0 || len(value) > 20 {
		return 0, fail("invalid_number")
	}
	n, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil {
		return 0, fail("invalid_number")
	}
	return n, nil
}

func parseUnsigned(value []byte) (uint64, error) {
	if len(value) == 0 || len(value) > 20 {
		return 0, fail("invalid_number")
	}
	n, err := strconv.ParseUint(string(value), 10, 64)
	if err != nil {
		return 0, fail("invalid_number")
	}
	return n, nil
}
