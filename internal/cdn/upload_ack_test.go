package cdn

import (
	"crypto/aes"
	"encoding/binary"
	"errors"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func syntheticProgressFrame(values map[string]string) []byte {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var body []byte
	for _, key := range keys {
		body = append(body, wireKV(key, []byte(values[key]))...)
	}
	header := make([]byte, HeaderBytes)
	header[0] = 0xab
	binary.BigEndian.PutUint32(header[1:5], uint32(HeaderBytes+len(body)))
	binary.BigEndian.PutUint16(header[5:7], UploadCommand)
	binary.BigEndian.PutUint32(header[21:25], uint32(len(body)))
	return append(header, body...)
}

func TestSafeUploadProgressDoesNotReportAcceptance(t *testing.T) {
	_, request := syntheticUpload()
	for _, values := range []map[string]string{
		{"retcode": "0", "seq": "29", "recvlen": "16"},
		{"retcode": "0", "seq": "29", "skeyresp": "0"},
	} {
		result, complete, err := DecodeUploadProgress(syntheticProgressFrame(values), request, 29, 1024)
		if err != nil || complete || result != nil {
			t.Fatal("native progress is incorrectly treated as a completed upload")
		}
		if result, err := DecodeUploadAck(syntheticProgressFrame(values), request, 29, 1024); err == nil || result != nil {
			t.Fatal("single-ack API accepts a progress-only response")
		}
	}
}

func TestSafeUploadCompletionFollowsNativeFileIDPredicate(t *testing.T) {
	_, request := syntheticUpload()
	for _, recvLen := range []string{"", "16"} {
		values := map[string]string{"retcode": "0", "seq": "29", "fileid": "synthetic_uploaded_ref"}
		if recvLen != "" {
			values["recvlen"] = recvLen
		}
		result, complete, err := DecodeUploadProgress(syntheticProgressFrame(values), request, 29, 1024)
		if err != nil || !complete || result == nil || !result.ServerAccepted || result.FileID != values["fileid"] || result.AESKey != request.AESKey {
			t.Fatal("native final fileid is not recognized")
		}
	}
}

func TestSafeUploadSessionKeyFailureCannotBecomeAcceptance(t *testing.T) {
	_, request := syntheticUpload()
	values := map[string]string{"retcode": "0", "seq": "29", "fileid": "synthetic_uploaded_ref", "skeyresp": "-7"}
	result, complete, err := DecodeUploadProgress(syntheticProgressFrame(values), request, 29, 1024)
	var status *Error
	if result != nil || complete || !errors.As(err, &status) || status.Category != "session_key_rejected" || status.Code != -7 {
		t.Fatal("nonzero skeyresp becomes upload acceptance")
	}
	delete(values, "skeyresp")
	values["skeybuf"] = "SYNTHETIC_PRIVATE_SESSION_KEY_BUFFER"
	result, complete, err = DecodeUploadProgress(syntheticProgressFrame(values), request, 29, 1024)
	if result != nil || complete || !errors.As(err, &status) || status.Category != "unsupported_session_key" || strings.Contains(err.Error(), values["skeybuf"]) {
		t.Fatal("undecoded skeybuf is ignored or exposed")
	}
}

func TestSafeUploadAcknowledgedLengthIsBoundedByCipherBytes(t *testing.T) {
	_, request := syntheticUpload()
	cipherLen := uint64(len(request.Data)/aes.BlockSize+1) * aes.BlockSize
	values := map[string]string{"retcode": "0", "seq": "29", "recvlen": strconv.FormatUint(cipherLen, 10)}
	result, complete, err := DecodeUploadProgress(syntheticProgressFrame(values), request, 29, 1024)
	if result != nil || complete || err != nil {
		t.Fatal("full byte acknowledgement without fileid is not valid progress")
	}
	for _, invalid := range []string{"-1", "invalid", strconv.FormatUint(cipherLen+1, 10)} {
		values["recvlen"] = invalid
		result, complete, err = DecodeUploadProgress(syntheticProgressFrame(values), request, 29, 1024)
		if result != nil || complete || err == nil {
			t.Fatal("invalid byte acknowledgement is accepted")
		}
	}
}
