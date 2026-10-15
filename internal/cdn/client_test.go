package cdn

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/md5"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

func syntheticInputs() (Profile, DownloadRequest) {
	p := Profile{Version: 1, UIN: 0x01020304, ClientVersion: 123, OSType: "synthetic_os", AuthKey: []byte("synthetic_auth_secret"), NetType: 1}
	r := DownloadRequest{FileID: "synthetic_file_secret", AESKey: [16]byte{'s', 'y', 'n', 't', 'h', 'e', 't', 'i', 'c', '_', 'k', 'e', 'y', '_', '1', '6'}, FileType: 3, RawSize: 16, FileKey: "synthetic_local_task", Common: &CommonOptions{}, Sequence: 19, TaskID: 23}
	return p, r
}

// These independent wire helpers use the native contract, not the production
// encoder. In particular the response fixture can declare a shorter filedata
// length while keeping the full variable payload in its AB frame.
func wireKV(key string, value []byte) []byte {
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(key)))
	b := append([]byte(nil), size[:]...)
	b = append(b, key...)
	binary.BigEndian.PutUint32(size[:], uint32(len(value)))
	b = append(b, size[:]...)
	return append(b, value...)
}

func responseFixture(t *testing.T, plain []byte, key [16]byte, seq uint32, overrides map[string]string, variableDeclared int) []byte {
	t.Helper()
	pad := aes.BlockSize - len(plain)%aes.BlockSize
	padded := append(append([]byte(nil), plain...), bytes.Repeat([]byte{byte(pad)}, pad)...)
	block, err := aes.NewCipher(key[:])
	if err != nil {
		t.Fatal(err)
	}
	ciphertext := make([]byte, len(padded))
	for offset := 0; offset < len(padded); offset += aes.BlockSize {
		block.Encrypt(ciphertext[offset:offset+aes.BlockSize], padded[offset:offset+aes.BlockSize])
	}
	hash := md5.Sum(plain)
	values := map[string]string{"retcode": "0", "seq": strconv.FormatUint(uint64(seq), 10), "rawfilesize": strconv.Itoa(len(plain)), "rawfilemd5": hex.EncodeToString(hash[:])}
	for k, v := range overrides {
		values[k] = v
	}
	var body []byte
	for _, k := range []string{"retcode", "seq", "rawfilesize", "rawfilemd5", "meta"} {
		if value, ok := values[k]; ok {
			body = append(body, wireKV(k, []byte(value))...)
		}
	}
	if variableDeclared < 0 {
		variableDeclared = len(ciphertext)
	}
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], 8)
	body = append(body, n[:]...)
	body = append(body, "filedata"...)
	binary.BigEndian.PutUint32(n[:], uint32(variableDeclared))
	body = append(body, n[:]...)
	body = append(body, ciphertext...)
	header := make([]byte, 25)
	header[0] = 0xab
	binary.BigEndian.PutUint32(header[1:5], uint32(len(body)+25))
	binary.BigEndian.PutUint16(header[5:7], 20000)
	binary.BigEndian.PutUint32(header[21:25], uint32(len(body)))
	return append(header, body...)
}

func decodeRequestFields(t *testing.T, body []byte) map[string][]byte {
	t.Helper()
	fields := make(map[string][]byte)
	for len(body) > 0 {
		if len(body) < 8 {
			t.Fatal("truncated synthetic request")
		}
		keyLen := int(binary.BigEndian.Uint32(body[:4]))
		if keyLen+8 > len(body) {
			t.Fatal("invalid synthetic key length")
		}
		key := string(body[4 : 4+keyLen])
		valueLen := int(binary.BigEndian.Uint32(body[4+keyLen : 8+keyLen]))
		if valueLen+keyLen+8 > len(body) {
			t.Fatal("invalid synthetic value length")
		}
		fields[key] = append([]byte(nil), body[8+keyLen:8+keyLen+valueLen]...)
		body = body[8+keyLen+valueLen:]
	}
	return fields
}

func TestRequestNativeFrameAndLegacyFields(t *testing.T) {
	p, r := syntheticInputs()
	packet, err := EncodeRequest(p, r)
	if err != nil {
		t.Fatal(err)
	}
	frame := packet.Bytes()
	if frame[0] != 0xab || binary.BigEndian.Uint16(frame[5:7]) != 20000 || !bytes.Equal(frame[7:11], []byte{4, 3, 2, 1}) || !bytes.Equal(frame[11:21], make([]byte, 10)) {
		t.Fatal("native frame command/uin endianness changed")
	}
	if int(binary.BigEndian.Uint32(frame[1:5])) != len(frame) || int(binary.BigEndian.Uint32(frame[21:25])) != len(frame)-25 {
		t.Fatal("frame length contract changed")
	}
	fields := decodeRequestFields(t, packet.Body())
	for key, want := range map[string]string{"seq": "19", "ver": "1", "weixinnum": "16909060", "taskid": "23", "totalsize": "32", "rawtotalsize": "16", "filetype": "3", "rsaver": "1", "downpicformat": "1", "offset": "0", "largesvideo": "0", "sourceflag": "0"} {
		if string(fields[key]) != want {
			t.Fatalf("field %s mismatch", key)
		}
	}
	if !bytes.Equal(fields["authkey"], p.AuthKey) || len(fields["rsavalue"]) != 128 {
		t.Fatal("legacy auth or raw RSA ciphertext changed")
	}
	for _, key := range []string{"meta", "rangestart", "rangeend", "aeskey", "touser", "hasthumb", "compresstype", "nocheckaeskey", "enablehit", "existancecheck", "apptype", "filekey"} {
		if _, exists := fields[key]; exists {
			t.Fatalf("unexpected field %s in legacy image stream", key)
		}
	}
	public, err := x509.ParsePKCS1PublicKey(legacyPublicKeyDER)
	if err != nil || public.N.BitLen() != 1024 || public.E != 65537 {
		t.Fatal("compiled public RSA parameters mismatch")
	}
}

func TestVariableFiledataConsumesWholeFrameAndVerifiesPlaintext(t *testing.T) {
	_, r := syntheticInputs()
	plain := []byte("synthetic payload longer than one AES block")
	frame := responseFixture(t, plain, r.AESKey, 19, nil, 1)
	result, err := DecodeResponse(frame, r, 19, 1024)
	if err != nil || !bytes.Equal(result.Data, plain) || !result.ChecksumVerified {
		t.Fatalf("native variable payload failed: result %v error %v", result, err)
	}
	// The server rawfilesize replaces the initial XML hint as native does.
	if result.RawSize != uint64(len(plain)) || r.RawSize == result.RawSize {
		t.Fatal("response raw size was not checked as the server override")
	}
}

func TestResponseRejectsFalseSuccess(t *testing.T) {
	_, r := syntheticInputs()
	plain := []byte("synthetic-data-16")
	for _, tc := range []struct {
		name      string
		overrides map[string]string
		mutate    func([]byte)
		declared  int
		category  string
	}{
		{"server-error", map[string]string{"retcode": "-13"}, nil, -1, "server_error"},
		{"sequence", map[string]string{"seq": "20"}, nil, -1, "sequence_mismatch"},
		{"raw-size", map[string]string{"rawfilesize": "18"}, nil, -1, "raw_size_mismatch"},
		{"md5", map[string]string{"rawfilemd5": strings.Repeat("0", 32)}, nil, -1, "checksum_mismatch"},
		{"no-md5", map[string]string{"rawfilemd5": ""}, nil, -1, "missing_checksum"},
		{"meta", map[string]string{"meta": "synthetic_private_value"}, nil, -1, "unsupported_meta_encryption"},
		{"truncated-variable", nil, nil, 1000, "invalid_tlv"},
		{"frame-length", nil, func(b []byte) { b[4] ^= 1 }, -1, "invalid_frame"},
		{"bad-padding", nil, func(b []byte) { b[len(b)-1] ^= 1 }, -1, "invalid_padding"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frame := responseFixture(t, plain, r.AESKey, 19, tc.overrides, tc.declared)
			if tc.mutate != nil {
				tc.mutate(frame)
			}
			result, err := DecodeResponse(frame, r, 19, 1024)
			var detail *Error
			if result != nil || !errors.As(err, &detail) || detail.Category != tc.category {
				t.Fatalf("wanted %s, got result %v error %v", tc.category, result, err)
			}
		})
	}
}

func TestImageResponseWithoutRawSizeUsesVerifiedPlaintextLength(t *testing.T) {
	_, request := syntheticInputs()
	plain := bytes.Repeat([]byte("synthetic-image"), 130)
	frame := responseFixture(t, plain, request.AESKey, 19, nil, -1)
	metadata, ciphertext, err := parseBody(frame[HeaderBytes:])
	if err != nil {
		t.Fatal(err)
	}
	var body []byte
	for _, key := range []string{"retcode", "seq", "rawfilemd5"} {
		body = append(body, wireKV(key, metadata[key])...)
	}
	body = append(body, wireKV("totalsize", []byte(strconv.Itoa(len(ciphertext))))...)
	body = append(body, wireKV("filedata", ciphertext)...)
	frame = append(append([]byte(nil), frame[:HeaderBytes]...), body...)
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(frame)))
	binary.BigEndian.PutUint32(frame[21:25], uint32(len(body)))
	result, err := DecodeResponse(frame, request, 19, 4096)
	if err != nil || !bytes.Equal(result.Data, plain) || result.RawSize != uint64(len(plain)) {
		t.Fatal("valid image response used the stale XML size hint despite verified server checksum", err)
	}
	request.ExpectedMD5 = strings.Repeat("0", 32)
	if result, err := DecodeResponse(frame, request, 19, 4096); err == nil || result != nil {
		t.Fatal("missing raw-size metadata bypassed the expected checksum")
	}
}

func TestDuplicateFieldsAndLimits(t *testing.T) {
	body := append(wireKV("seq", []byte("1")), wireKV("seq", []byte("2"))...)
	_, _, err := parseBody(body)
	var detail *Error
	if !errors.As(err, &detail) || detail.Category != "duplicate_field" {
		t.Fatalf("duplicate fields accepted: %v", err)
	}
	_, r := syntheticInputs()
	frame := responseFixture(t, bytes.Repeat([]byte("x"), 17), r.AESKey, 19, nil, -1)
	if result, err := DecodeResponse(frame, r, 19, 16); result != nil || err == nil {
		t.Fatal("oversize plaintext accepted")
	}
}

func syntheticListener(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func setEndpoint(p *Profile, listener net.Listener) {
	addr := listener.Addr().(*net.TCPAddr)
	p.Addresses = []string{addr.IP.String()}
	p.Ports = []uint16{uint16(addr.Port)}
}

func TestDownloadSocketExchangeHandlesSplitReads(t *testing.T) {
	listener := syntheticListener(t)
	p, r := syntheticInputs()
	setEndpoint(&p, listener)
	plain := []byte("independent local fake CDN response")
	serverErrors := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErrors <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		header := make([]byte, 25)
		if _, err := io.ReadFull(conn, header); err != nil {
			serverErrors <- err
			return
		}
		body := make([]byte, binary.BigEndian.Uint32(header[21:25]))
		if _, err := io.ReadFull(conn, body); err != nil {
			serverErrors <- err
			return
		}
		fields := decodeRequestFields(t, body)
		if string(fields["seq"]) != "19" || len(fields["rsavalue"]) != 128 {
			serverErrors <- errors.New("synthetic request contract mismatch")
			return
		}
		frame := responseFixture(t, plain, r.AESKey, 19, nil, 1)
		for _, b := range frame {
			if _, err := conn.Write([]byte{b}); err != nil {
				serverErrors <- err
				return
			}
		}
		serverErrors <- nil
	}()
	client := Client{Timeout: time.Second, MaxBytes: 1024}
	result, err := client.Download(context.Background(), p, r)
	if err != nil || !bytes.Equal(result.Data, plain) {
		t.Fatalf("local socket exchange failed: result %v error %v", result, err)
	}
	if err := <-serverErrors; err != nil {
		t.Fatal(err)
	}
}

func TestDownloadCancellationClosesBlockedRead(t *testing.T) {
	listener := syntheticListener(t)
	p, r := syntheticInputs()
	setEndpoint(&p, listener)
	accepted := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		close(accepted)
		_, _ = io.Copy(io.Discard, conn) // Wait for client cancellation to close.
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := (&Client{Timeout: time.Second}).Download(ctx, p, r)
		result <- err
	}()
	<-accepted
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation lost: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled read did not finish")
	}
	<-finished
}

func TestDownloadDeadlineAndOversizeHeader(t *testing.T) {
	for _, oversize := range []bool{false, true} {
		t.Run(strconv.FormatBool(oversize), func(t *testing.T) {
			listener := syntheticListener(t)
			p, r := syntheticInputs()
			setEndpoint(&p, listener)
			finished := make(chan struct{})
			go func() {
				defer close(finished)
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(time.Second))
				if oversize {
					header := make([]byte, 25)
					header[0] = 0xab
					binary.BigEndian.PutUint32(header[1:5], 1<<30)
					binary.BigEndian.PutUint32(header[21:25], 1<<30-25)
					_, _ = conn.Write(header)
				}
				_, _ = io.Copy(io.Discard, conn)
			}()
			start := time.Now()
			result, err := (&Client{Timeout: 100 * time.Millisecond, MaxBytes: 1024}).Download(context.Background(), p, r)
			if result != nil || err == nil || time.Since(start) > time.Second {
				t.Fatalf("bounded read failed: %v %v", result, err)
			}
			if !oversize && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("deadline not retained: %v", err)
			}
			<-finished
		})
	}
}

func TestDiagnosticsNeverExposeCredentialOrContent(t *testing.T) {
	p, r := syntheticInputs()
	packet, err := EncodeRequest(p, r)
	if err != nil {
		t.Fatal(err)
	}
	result := Result{Data: []byte("synthetic_body_secret"), RawSize: 21, CipherBytes: 32, ChecksumVerified: true}
	for _, value := range []any{p, &p, r, &r, packet, &packet, result, &result} {
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		for _, diagnostic := range []string{fmt.Sprintf("%v", value), fmt.Sprintf("%+v", value), fmt.Sprintf("%#v", value), string(encoded)} {
			for _, secret := range []string{"synthetic_auth_secret", "synthetic_file_secret", "synthetic_local_task", "synthetic_body_secret", "synthetic_key_16"} {
				if strings.Contains(diagnostic, secret) {
					t.Fatal("diagnostic exposes synthetic secret")
				}
			}
		}
	}
	r.Common = nil
	if _, err := EncodeRequest(p, r); err == nil || strings.Contains(err.Error(), r.FileID) {
		t.Fatal("unknown common options were silently selected or exposed")
	}
	err = networkError(context.Background(), errors.New("synthetic_auth_secret"))
	if strings.Contains(err.Error(), "synthetic_auth_secret") {
		t.Fatal("raw transport error exposed")
	}
}
