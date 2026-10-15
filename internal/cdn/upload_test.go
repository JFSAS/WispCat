package cdn

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"
)

func syntheticUpload() (Profile, UploadRequest) {
	p, d := syntheticInputs()
	data := []byte("unaltered synthetic file content")
	crc := crc32.ChecksumIEEE(data)
	return p, UploadRequest{Data: data, AESKey: d.AESKey, FileType: 2, FileKey: "synthetic_upload_task_secret", ToUser: "synthetic_peer_secret", Common: &UploadOptions{ChatType: 0, AppType: 1, CompressionType: 0, FileFormat: 0, MidFormat: 0}, RawCRC: &crc, Sequence: 29, TaskID: 31}
}

func uploadAck(fields map[string]string) []byte {
	var body []byte
	for _, key := range []string{"retcode", "seq", "fileid", "rawtotalsize", "rawfilemd5", "filemd5", "aeskey", "recvlen", "midimglen", "skeybuf", "skeyresp"} {
		if value, ok := fields[key]; ok {
			body = append(body, wireKV(key, []byte(value))...)
		}
	}
	frame := make([]byte, 25)
	frame[0] = 0xab
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(body)+25))
	binary.BigEndian.PutUint16(frame[5:7], 30000)
	binary.BigEndian.PutUint32(frame[21:25], uint32(len(body)))
	return append(frame, body...)
}

func TestUploadNativeStreamFrameAndRoundtripCiphertext(t *testing.T) {
	p, r := syntheticUpload()
	packet, err := EncodeUpload(p, r)
	if err != nil {
		t.Fatal(err)
	}
	frame := packet.Bytes()
	if binary.BigEndian.Uint16(frame[5:7]) != 30000 || int(binary.BigEndian.Uint32(frame[1:5])) != len(frame) {
		t.Fatal("upload command or complete stream length changed")
	}
	// Native57792f0 builds [meta_length]+old_fields before the second pack.
	// Upload parsing can reject an otherwise complete key set in map order.
	body := packet.Body()
	keyLength := binary.BigEndian.Uint32(body[:4])
	if keyLength != uint32(len("meta_length")) || string(body[4:4+keyLength]) != "meta_length" {
		t.Fatal("native first upload key must be meta_length")
	}
	fields, ciphertext, err := parseBody(packet.Body())
	if err != nil {
		t.Fatal(err)
	}
	plain, err := decryptContent(ciphertext, r.AESKey)
	if err != nil || !bytes.Equal(plain, r.Data) {
		t.Fatal("upload alters plaintext or AES padding")
	}
	for key, want := range map[string]string{"seq": "29", "taskid": "31", "hasthumb": "0", "rawthumbsize": "0", "thumbtotalsize": "0", "encthumbcrc": "0", "largesvideo": "0", "sourceflag": "0", "advideoflag": "0", "setofpicformat": "010000", "rsaver": "1", "enablehit": "0", "dataid": "0", "offset": "0"} {
		if string(fields[key]) != want {
			t.Fatalf("native upload field %s mismatch", key)
		}
	}
	for _, key := range []string{"wxmsgflag", "wxautostart", "downloadbehavor", "downpicformat", "rangestart", "rangeend", "meta"} {
		if _, ok := fields[key]; ok {
			t.Fatalf("unexpected download/cache field %s", key)
		}
	}
	if len(fields["rsavalue"]) != 128 {
		t.Fatal("upload AES key not wrapped as raw RSA bytes")
	}
	var firstMetadataLength int
	for key, value := range fields {
		if key != "meta_length" {
			firstMetadataLength += 8 + len(key) + len(value)
		}
	}
	firstMetadataLength += 25 + 8 + len("filedata")
	got, err := strconv.Atoi(string(fields["meta_length"]))
	if err != nil || got != firstMetadataLength {
		t.Fatal("meta_length includes ciphertext or second-pass metadata")
	}
	h := md5.Sum(r.Data)
	if string(fields["rawfilemd5"]) != hex.EncodeToString(h[:]) || string(fields["filemd5"]) != string(fields["rawfilemd5"]) {
		t.Fatal("plaintext MD5 contract changed")
	}
}

func TestUploadAcknowledgementDistinguishesAcceptanceAndVerification(t *testing.T) {
	_, r := syntheticUpload()
	base := map[string]string{"retcode": "0", "seq": "29", "fileid": "synthetic_returned_fileid_secret"}
	frame := uploadAck(base)
	result, err := DecodeUploadAck(frame, r, 29, 1024)
	if err != nil || !result.ServerAccepted || result.ChecksumVerified {
		t.Fatalf("acceptance incorrectly claimed hash verification: %v %v", result, err)
	}
	h := md5.Sum(r.Data)
	base["rawfilemd5"] = hex.EncodeToString(h[:])
	base["rawtotalsize"] = strconv.Itoa(len(r.Data))
	result, err = DecodeUploadAck(uploadAck(base), r, 29, 1024)
	if err != nil || !result.ChecksumVerified {
		t.Fatalf("matching returned digest rejected: %v", err)
	}
	for key, value := range map[string]string{"retcode": "-5103009", "seq": "30", "fileid": "", "rawfilemd5": strings.Repeat("0", 32), "rawtotalsize": "999", "aeskey": strings.Repeat("0", 32)} {
		t.Run(key, func(t *testing.T) {
			copyFields := make(map[string]string)
			for k, v := range base {
				copyFields[k] = v
			}
			copyFields[key] = value
			if result, err := DecodeUploadAck(uploadAck(copyFields), r, 29, 1024); result != nil || err == nil {
				t.Fatal("invalid upload ack turned into accepted result")
			}
		})
	}
}

func TestUploadLocalSocketEncryptsOriginalBytesAndChecksAck(t *testing.T) {
	listener := syntheticListener(t)
	p, r := syntheticUpload()
	setEndpoint(&p, listener)
	serverErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		header := make([]byte, 25)
		if _, err := io.ReadFull(conn, header); err != nil {
			serverErr <- err
			return
		}
		if binary.BigEndian.Uint16(header[5:7]) != 30000 {
			serverErr <- errors.New("wrong native upload command")
			return
		}
		body := make([]byte, binary.BigEndian.Uint32(header[21:25]))
		if _, err := io.ReadFull(conn, body); err != nil {
			serverErr <- err
			return
		}
		metadata, cipher, err := parseBody(body)
		if err != nil {
			serverErr <- err
			return
		}
		plain, err := decryptContent(cipher, r.AESKey)
		if err != nil || !bytes.Equal(plain, r.Data) {
			serverErr <- errors.New("upload data was changed")
			return
		}
		h := md5.Sum(plain)
		ack := uploadAck(map[string]string{"retcode": "0", "seq": string(metadata["seq"]), "fileid": "synthetic_returned_fileid_secret", "rawfilemd5": hex.EncodeToString(h[:]), "rawtotalsize": strconv.Itoa(len(plain))})
		for start := 0; start < len(ack); {
			end := start + 3
			if end > len(ack) {
				end = len(ack)
			}
			if _, err := conn.Write(ack[start:end]); err != nil {
				serverErr <- err
				return
			}
			start = end
		}
		serverErr <- nil
	}()
	result, err := (&Client{Timeout: time.Second, MaxBytes: 1024}).Upload(context.Background(), p, r)
	if err != nil || !result.ServerAccepted || !result.ChecksumVerified || result.MD5 == "" {
		t.Fatalf("local upload failed: %v %v", result, err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestUploadDiagnosticsRedactDataAndIdentifiers(t *testing.T) {
	_, r := syntheticUpload()
	result := UploadResult{FileID: "synthetic_returned_fileid_secret", AESKey: r.AESKey, Size: uint64(len(r.Data)), MD5: "synthetic_digest_secret", ServerAccepted: true}
	for _, v := range []any{r, &r, result, &result} {
		j, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		for _, text := range []string{fmt.Sprintf("%v", v), fmt.Sprintf("%+v", v), fmt.Sprintf("%#v", v), string(j)} {
			for _, secret := range []string{string(r.Data), "synthetic_returned_fileid_secret", "synthetic_peer_secret", "synthetic_upload_task_secret", "synthetic_digest_secret"} {
				if strings.Contains(text, secret) {
					t.Fatal("upload diagnostics exposed synthetic private material")
				}
			}
		}
	}
}
