package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestDownloadStreamRejectsIncompleteCorruptAndExtraFrames(t *testing.T) {
	header := `{"status":"ok","retcode":0,"stream":"stream-action","data":{"type":"stream","data_type":"file_info","file_size":1,"file_name":"fixture.bin"}}` + "\n"
	chunk := `{"status":"ok","retcode":0,"stream":"stream-action","data":{"type":"stream","data_type":"file_chunk","index":0,"data":"eA==","size":1}}` + "\n"
	digest := sha256.Sum256([]byte("x"))
	terminal := `{"status":"ok","retcode":0,"stream":"stream-action","data":{"type":"response","data_type":"file_complete","total_bytes":1,"total_chunks":1,"sha256":"` + hex.EncodeToString(digest[:]) + `"}}` + "\n"
	for name, body := range map[string]string{
		"missing_terminal":    header + chunk,
		"wrong_index":         header + strings.Replace(chunk, `"index":0`, `"index":1`, 1) + terminal,
		"invalid_base64":      header + strings.Replace(chunk, "eA==", "====", 1) + terminal,
		"wrong_hash":          header + chunk + strings.Replace(terminal, hex.EncodeToString(digest[:]), strings.Repeat("0", 64), 1),
		"extra_terminal":      header + chunk + terminal + terminal,
		"wrong_envelope":      strings.Replace(header, "stream-action", "wrong", 1) + chunk + terminal,
		"missing_size":        strings.Replace(header, `"file_size":1,`, "", 1) + chunk + terminal,
		"missing_index":       header + strings.Replace(chunk, `"index":0,`, "", 1) + terminal,
		"noncanonical_base64": header + strings.Replace(chunk, `eA==`, `eA==\n`, 1) + terminal,
		"terminal_error":      header + chunk + `{"status":"failed","retcode":100,"stream":"stream-action","message":"transfer_failed","data":{"type":"error"}}` + "\n",
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

func TestUploadStreamRejectsUnboundAcknowledgementsAndKeepsScopeOnReset(t *testing.T) {
	for _, failure := range []string{"wrong_stream", "wrong_count", "wrong_type", "wrong_envelope", "wrong_completion"} {
		t.Run(failure, func(t *testing.T) {
			var resets, completions atomic.Int32
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var p map[string]any
				if json.NewDecoder(r.Body).Decode(&p) != nil {
					t.Error("invalid request")
					return
				}
				if p["self_id"] != "123" || p["file_retention"] != float64(60000) {
					t.Error("account/retention changed during upload or reset")
				}
				data := map[string]any{"type": "stream", "status": "chunk_received", "stream_id": p["stream_id"], "received_chunks": 1, "total_chunks": 1}
				envelope := map[string]any{"status": "ok", "retcode": 0, "stream": "stream-action", "data": data}
				if p["reset"] == true {
					resets.Add(1)
					data["type"] = "reset"
					_ = json.NewEncoder(w).Encode(envelope)
					return
				}
				if p["is_complete"] == true {
					completions.Add(1)
					data["type"] = "response"
					data["status"] = "file_complete"
					data["file_size"] = 1
					data["sha256"] = p["expected_sha256"]
					data["file_path"] = "stream://synthetic"
					data["file_name"] = "other.bin"
				} else {
					switch failure {
					case "wrong_stream":
						data["stream_id"] = "other"
					case "wrong_count":
						data["received_chunks"] = 0
					case "wrong_type":
						data["type"] = "response"
					case "wrong_envelope":
						delete(envelope, "stream")
					}
				}
				_ = json.NewEncoder(w).Encode(envelope)
			}))
			defer peer.Close()
			c, err := New(peer.URL, "synthetic-token")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = c.UploadFileWithParams(context.Background(), "fixture.bin", strings.NewReader("x"), 1, map[string]any{"self_id": "123", "file_retention": 60000}); err == nil {
				t.Fatal("malformed acknowledgement accepted")
			}
			wantComplete := int32(0)
			if failure == "wrong_completion" {
				wantComplete = 1
			}
			if resets.Load() != 1 || completions.Load() != wantComplete {
				t.Fatal("invalid acknowledgement advanced/failed to reset", resets.Load(), completions.Load())
			}
		})
	}
}
