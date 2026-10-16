package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDownloadStreamRejectsIncompleteCorruptAndExtraFrames(t *testing.T) {
	header := `{"status":"ok","retcode":0,"stream":"stream-action","data":{"type":"stream","data_type":"file_info","file_size":1,"file_name":"fixture.bin"}}` + "\n"
	chunk := `{"status":"ok","retcode":0,"stream":"stream-action","data":{"type":"stream","data_type":"file_chunk","index":0,"data":"eA==","size":1}}` + "\n"
	digest := sha256.Sum256([]byte("x"))
	terminal := `{"status":"ok","retcode":0,"stream":"stream-action","data":{"type":"response","data_type":"file_complete","total_bytes":1,"total_chunks":1,"sha256":"` + hex.EncodeToString(digest[:]) + `"}}` + "\n"
	for name, body := range map[string]string{
		"missing_terminal": header + chunk,
		"wrong_index":      header + strings.Replace(chunk, `"index":0`, `"index":1`, 1) + terminal,
		"invalid_base64":   header + strings.Replace(chunk, "eA==", "====", 1) + terminal,
		"wrong_hash":       header + chunk + strings.Replace(terminal, hex.EncodeToString(digest[:]), strings.Repeat("0", 64), 1),
		"extra_terminal":   header + chunk + terminal + terminal,
		"wrong_envelope":   strings.Replace(header, "stream-action", "wrong", 1) + chunk + terminal,
		"terminal_error":   header + chunk + `{"status":"failed","retcode":100,"stream":"stream-action","message":"transfer_failed","data":{"type":"error"}}` + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(body)) }))
			defer peer.Close()
			c, err := New(peer.URL, "synthetic-token")
			if err != nil {
				t.Fatal(err)
			}
			var output bytes.Buffer
			if _, err = c.DownloadFile(context.Background(), "123", &output); err == nil {
				t.Fatal("invalid streamed response reported success")
			}
		})
	}
}
