package cdn

import (
	"crypto/aes"
	"crypto/md5"
	"crypto/subtle"
	"encoding/hex"
	"sort"
)

// DecodeUploadAck requires a complete acknowledgement. A caller reading a
// stream must use DecodeUploadProgress and continue on the same connection.
func DecodeUploadAck(frame []byte, r UploadRequest, seq uint32, maxBytes int64) (*UploadResult, error) {
	result, completed, err := DecodeUploadProgress(frame, r, seq, maxBytes)
	if err != nil {
		return nil, err
	}
	if !completed {
		return nil, fail("missing_file_id")
	}
	return result, nil
}

// DecodeUploadProgress follows SafeC2CUploadTask::_HandleNormalResp581b850 and
// completion predicate581d38c for ordinary command30000 (TaskParam864=0).
// Successful progress has a nil result and completed=false. Only a nonempty
// fileid completes the upload; recvlen is optional and is not a delivery receipt.
// Account-specific skeybuf decoding is not implemented and is never bypassed.
func DecodeUploadProgress(frame []byte, r UploadRequest, seq uint32, maxBytes int64) (*UploadResult, bool, error) {
	if maxBytes == 0 {
		maxBytes = DefaultMaxBytes
	}
	if maxBytes < 1 || maxBytes > 1<<30 {
		return nil, false, fail("invalid_limit")
	}
	if len(r.Data) == 0 || int64(len(r.Data)) > maxBytes {
		return nil, false, fail("size_limit")
	}
	body, err := frameBody(frame, maxBytes)
	if err != nil {
		return nil, false, err
	}
	metadata, payload, err := parseBody(body)
	if err != nil {
		return nil, false, err
	}
	if payload != nil {
		return nil, false, fail("unexpected_upload_payload")
	}
	if _, ok := metadata["meta"]; ok {
		return nil, false, fail("unsupported_meta_encryption")
	}
	ret, err := parseSigned(metadata["retcode"])
	if err != nil {
		return nil, false, fail("missing_status")
	}
	if ret != 0 {
		return nil, false, &Error{Category: "server_error", Code: ret}
	}
	gotSeq, err := parseUnsigned(metadata["seq"])
	if err != nil || seq == 0 || gotSeq != uint64(seq) {
		return nil, false, fail("sequence_mismatch")
	}
	if raw, ok := metadata["skeyresp"]; ok {
		status, err := parseSigned(raw)
		if err != nil {
			return nil, false, fail("invalid_session_key_status")
		}
		if status != 0 {
			return nil, false, &Error{Category: "session_key_rejected", Code: status}
		}
	}
	if len(metadata["skeybuf"]) != 0 {
		// Native invokes the app callback568ec00 before it permits completion.
		// Treat this as unsupported until that decoding contract is established.
		return nil, false, fail("unsupported_session_key")
	}
	if raw, ok := metadata["recvlen"]; ok {
		ack, err := parseUnsigned(raw)
		cipherLen := uint64(len(r.Data)/aes.BlockSize+1) * aes.BlockSize
		if err != nil || ack > cipherLen {
			return nil, false, fail("invalid_ack_length")
		}
	}
	if raw, ok := metadata["rawtotalsize"]; ok {
		n, err := parseUnsigned(raw)
		if err != nil || n != uint64(len(r.Data)) {
			return nil, false, fail("raw_size_mismatch")
		}
	}
	if key, ok := metadata["aeskey"]; ok && len(key) > 0 {
		decoded, err := hex.DecodeString(string(key))
		if err != nil || subtle.ConstantTimeCompare(decoded, r.AESKey[:]) != 1 {
			return nil, false, fail("unexpected_key_rotation")
		}
	}
	digest := md5.Sum(r.Data)
	verified := false
	for _, name := range []string{"rawfilemd5", "filemd5"} {
		if raw, ok := metadata[name]; ok && len(raw) > 0 {
			got, err := parseMD5(raw)
			if err != nil || subtle.ConstantTimeCompare(got[:], digest[:]) != 1 {
				return nil, false, fail("checksum_mismatch")
			}
			verified = true
		}
	}
	fileID := metadata["fileid"]
	if len(fileID) == 0 {
		return nil, false, nil
	}
	if len(fileID) > 8192 {
		return nil, false, fail("invalid_file_id")
	}
	keys := make([]string, 0, len(metadata))
	for key := range metadata {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return &UploadResult{
		FileID: string(fileID), AESKey: r.AESKey, Size: uint64(len(r.Data)),
		MD5: hex.EncodeToString(digest[:]), ServerAccepted: true,
		ChecksumVerified: verified, MetadataKeys: keys,
	}, true, nil
}
