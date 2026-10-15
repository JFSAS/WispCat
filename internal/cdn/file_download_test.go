package cdn

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"testing"
	"time"
)

func TestExplicitFileType5UsesNativeStreamDownloadHeader(t *testing.T) {
	p, r := syntheticInputs()
	r.FileType = 5
	packet, err := EncodeRequest(p, r)
	if err != nil {
		t.Fatal(err)
	}
	fields := decodeRequestFields(t, packet.Body())
	if string(fields["filetype"]) != "5" || string(fields["offset"]) != "0" {
		t.Fatal("small file stream type or first offset changed")
	}
	for _, key := range []string{"downpicformat", "hasthumb", "compresstype", "apptype", "touser", "rangestart", "rangeend", "safeproto"} {
		if _, exists := fields[key]; exists {
			t.Fatalf("explicit type5 has unrelated header %s", key)
		}
	}
}

func TestExplicitFileType5SocketDecryptsAndVerifiesFile(t *testing.T) {
	p, r := syntheticInputs()
	data := bytes.Repeat([]byte("synthetic nonprivate yaml: true\n"), 8000)
	r.FileType = 5
	r.RawSize = uint64(len(data))
	digest := md5.Sum(data)
	r.ExpectedMD5 = hex.EncodeToString(digest[:])
	listener := syntheticListener(t)
	setEndpoint(&p, listener)
	response := responseFixture(t, data, r.AESKey, r.Sequence, map[string]string{"rawfilemd5": ""}, 1)
	serverErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		if _, err := readFrame(conn, int64(len(data))); err != nil {
			serverErr <- err
			return
		}
		serverErr <- writeAll(conn, response)
	}()
	result, err := (&Client{Timeout: time.Second, MaxBytes: int64(len(data))}).Download(context.Background(), p, r)
	if err != nil || result == nil || !result.ChecksumVerified || !bytes.Equal(result.Data, data) {
		t.Fatalf("synthetic file did not round trip and verify: %v %v", result, err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}
