package mcp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/JFSAS/wechat-runtime/internal/onebot"
	"github.com/JFSAS/wechat-runtime/pkg/bot"
	"github.com/JFSAS/wechat-runtime/pkg/client"
)

// Stream actions need only a synthetic account snapshot; all transfer storage,
// dispatcher, authentication, NDJSON framing and SDK code are the real modules.
type streamAccount struct{ bot.Channel }

func (streamAccount) Snapshot() bot.Snapshot {
	return bot.Snapshot{Account: "synthetic-mcp", Self: bot.User{ID: "owner", Name: "Synthetic"}, Online: true, State: "connected", Epoch: 1}
}

func mcpPeer(t *testing.T, around func(http.Handler) http.Handler) (*client.Client, *onebot.Streams) {
	t.Helper()
	state, err := onebot.OpenState(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	streams, err := onebot.OpenStreams(filepath.Join(t.TempDir(), "streams"))
	if err != nil {
		t.Fatal(err)
	}
	runtime := onebot.New(streamAccount{}, state)
	runtime.Streams = streams
	server := onebot.NewServer(runtime, "synthetic-mcp-token", "")
	var handler http.Handler = server
	if around != nil {
		handler = around(handler)
	}
	peer := httptest.NewServer(handler)
	t.Cleanup(func() { peer.Close(); _ = server.Close(context.Background()); _ = streams.Close(); _ = state.Close() })
	c, err := client.New(peer.URL, "synthetic-mcp-token")
	if err != nil {
		t.Fatal(err)
	}
	return c, streams
}

func mcpTransfers(t *testing.T, config TransferConfig) *Transfers {
	t.Helper()
	if config.DownloadDir == "" {
		config.DownloadDir = t.TempDir()
	}
	if err := os.Chmod(config.DownloadDir, 0700); err != nil {
		t.Fatal(err)
	}
	transfers, err := OpenTransfers(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := transfers.Close(); err != nil {
			t.Error(err)
		}
	})
	return transfers
}

func callMCP(t *testing.T, s *Server, ctx context.Context, tool, action string, params map[string]any, execution any) (json.RawMessage, bool) {
	t.Helper()
	args := map[string]any{"action": action, "params": params}
	if execution != nil {
		args["execution"] = execution
	}
	raw, _ := json.Marshal(map[string]any{"name": tool, "arguments": args})
	value, err := s.handle(ctx, request{Method: "tools/call", Params: raw})
	if err != nil {
		t.Fatal(err)
	}
	result := value.(map[string]any)
	parts := result["content"].([]any)
	return json.RawMessage(parts[0].(map[string]string)["text"]), result["isError"].(bool)
}

func downloadedFiles(t *testing.T, root string) []string {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	var result []string
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".part") {
			t.Fatal("partial download leaked", entry.Name())
		}
		if strings.HasSuffix(entry.Name(), ".bin") {
			result = append(result, filepath.Join(root, entry.Name()))
		}
	}
	return result
}

func TestMCPStreamFilesRoundTripActualOneBotAndReopen(t *testing.T) {
	c, _ := mcpPeer(t, nil)
	uploadRoot := t.TempDir()
	downloadRoot := t.TempDir()
	body := bytes.Repeat([]byte("synthetic multipart MCP data"), 25000)
	source := filepath.Join(uploadRoot, "input with spaces.bin")
	if err := os.WriteFile(source, body, 0600); err != nil {
		t.Fatal(err)
	}
	config := TransferConfig{DownloadDir: downloadRoot, UploadRoot: uploadRoot}
	transfers := mcpTransfers(t, config)
	s := Server{Mode: "write", Client: c, Transfers: transfers}
	login, err := c.Call(context.Background(), "get_login_info", nil)
	if err != nil {
		t.Fatal(err)
	}
	var self struct {
		ID int `json:"user_id"`
	}
	if json.Unmarshal(login.Data, &self) != nil || self.ID == 0 {
		t.Fatal("self identity missing")
	}
	raw, failed := callMCP(t, &s, context.Background(), "invoke_action", "upload_file_stream", map[string]any{"filename": "reply.bin", "self_id": self.ID, "file_retention": 0}, map[string]any{"input_file": source})
	var uploaded transferResult
	if failed || json.Unmarshal(raw, &uploaded) != nil || !uploaded.Terminal || uploaded.Location != "runtime_temporary_reference" || uploaded.FileSize != int64(len(body)) {
		t.Fatal("automatic upload failed", string(raw))
	}
	if _, failed = callMCP(t, &s, context.Background(), "query_action", "download_file_stream", map[string]any{"file": uploaded.FilePath}, nil); !failed || len(downloadedFiles(t, downloadRoot)) != 0 {
		t.Fatal("query wrote a local file")
	}
	raw, failed = callMCP(t, &s, context.Background(), "invoke_action", "download_file_stream", map[string]any{"file": uploaded.FilePath, "self_id": self.ID, "chunk_size": 32768}, nil)
	var downloaded transferResult
	if failed || json.Unmarshal(raw, &downloaded) != nil || !downloaded.Terminal || downloaded.Location != "mcp_host" || downloaded.SHA256 != uploaded.SHA256 || downloaded.FrameCount != (len(body)+32767)/32768+2 {
		t.Fatal("validated download failed", string(raw))
	}
	actual, err := os.ReadFile(downloaded.FilePath)
	if err != nil || !bytes.Equal(actual, body) {
		t.Fatal("published bytes differ", err)
	}
	resolvedRoot, err := filepath.EvalSymlinks(downloadRoot)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(downloaded.FilePath) != resolvedRoot || filepath.Base(downloaded.FilePath) == uploaded.FileName {
		t.Fatal("remote name selected local destination")
	}
	if err = transfers.Close(); err != nil {
		t.Fatal(err)
	}
	s.Transfers = mcpTransfers(t, config)
	actual, err = os.ReadFile(downloaded.FilePath)
	if err != nil || !bytes.Equal(actual, body) {
		t.Fatal("completed file did not survive MCP reopen")
	}
	raw, failed = callMCP(t, &s, context.Background(), "query_action", "test_download_stream", nil, nil)
	var summary struct {
		FrameCount int `json:"frame_count"`
	}
	if failed || json.Unmarshal(raw, &summary) != nil || summary.FrameCount != 11 {
		t.Fatal("synthetic summary missing terminal", string(raw))
	}
}

func validFileFrames(body []byte) string {
	sum := sha256.Sum256(body)
	values := []any{
		map[string]any{"type": "stream", "data_type": "file_info", "file_name": "../../untrusted.bin", "file_size": len(body)},
		map[string]any{"type": "stream", "data_type": "file_chunk", "index": 0, "data": base64.StdEncoding.EncodeToString(body), "size": len(body)},
		map[string]any{"type": "response", "data_type": "file_complete", "total_bytes": len(body), "total_chunks": 1, "sha256": hex.EncodeToString(sum[:])},
	}
	var result strings.Builder
	for _, data := range values {
		raw, _ := json.Marshal(map[string]any{"status": "ok", "retcode": 0, "stream": "stream-action", "data": data})
		result.Write(raw)
		result.WriteByte('\n')
	}
	return result.String()
}

func TestMCPDownloadFailureNeverPublishesAndCancellationReleases(t *testing.T) {
	valid := validFileFrames([]byte("x"))
	lines := strings.SplitAfter(valid, "\n")
	for name, body := range map[string]string{
		"missing_terminal":       lines[0] + lines[1],
		"bad_checksum":           strings.Replace(valid, `"sha256":"`, `"sha256":"0`, 1),
		"extra_terminal":         valid + lines[2],
		"chunk_without_header":   lines[1] + lines[2],
		"summary_file_injection": valid,
	} {
		t.Run(name, func(t *testing.T) {
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, body) }))
			defer peer.Close()
			c, err := client.New(peer.URL, "synthetic-mcp-token")
			if err != nil {
				t.Fatal(err)
			}
			transfers := mcpTransfers(t, TransferConfig{})
			s := Server{Mode: "write", Client: c, Transfers: transfers}
			action := "download_file_stream"
			if name == "summary_file_injection" {
				action = "test_download_stream"
			}
			raw, failed := callMCP(t, &s, context.Background(), "invoke_action", action, map[string]any{"file": "123"}, nil)
			if !failed || bytes.Contains(raw, []byte("file_path")) || len(downloadedFiles(t, transfers.directory)) != 0 {
				t.Fatal("invalid stream published", string(raw))
			}
		})
	}
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, lines[0])
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer peer.Close()
	c, _ := client.New(peer.URL, "synthetic-mcp-token")
	transfers := mcpTransfers(t, TransferConfig{Timeout: 30 * time.Millisecond})
	s := Server{Mode: "write", Client: c, Transfers: transfers}
	if _, failed := callMCP(t, &s, context.Background(), "invoke_action", "download_file_stream", map[string]any{"file": "123"}, nil); !failed {
		t.Fatal("stalled download succeeded")
	}
	if len(downloadedFiles(t, transfers.directory)) != 0 {
		t.Fatal("cancelled download published")
	}
}

func TestMCPUploadPathsConflictsAndMutationCannotComplete(t *testing.T) {
	var calls atomic.Int32
	c, _ := mcpPeer(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); next.ServeHTTP(w, r) })
	})
	uploadRoot := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.bin")
	if err := os.WriteFile(outside, []byte("outside"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(uploadRoot, "escape.bin")); err != nil {
		t.Fatal(err)
	}
	fifo := filepath.Join(uploadRoot, "pipe")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	inside := filepath.Join(uploadRoot, "inside.bin")
	if err := os.WriteFile(inside, []byte("inside"), 0600); err != nil {
		t.Fatal(err)
	}
	s := Server{Mode: "write", Client: c, Transfers: mcpTransfers(t, TransferConfig{UploadRoot: uploadRoot})}
	for _, path := range []string{outside, filepath.Join(uploadRoot, "escape.bin"), fifo, uploadRoot, "../outside.bin"} {
		if _, failed := callMCP(t, &s, context.Background(), "invoke_action", "upload_file_stream", nil, map[string]any{"input_file": path}); !failed {
			t.Fatal("unsafe input admitted", path)
		}
	}
	for _, key := range []string{"stream_id", "chunk_index", "chunk_data", "total_chunks", "file_size", "expected_sha256", "is_complete", "reset", "verify_only"} {
		if _, failed := callMCP(t, &s, context.Background(), "invoke_action", "upload_file_stream", map[string]any{key: true}, map[string]any{"input_file": inside}); !failed {
			t.Fatal("MCP-owned field admitted", key)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("rejected local input reached OneBot")
	}
	var mutated atomic.Bool
	var completed atomic.Int32
	var resets atomic.Int32
	body := bytes.Repeat([]byte("mutating source"), 50000)
	if err := os.WriteFile(inside, body, 0600); err != nil {
		t.Fatal(err)
	}
	c2, streams := mcpPeer(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, _ := io.ReadAll(r.Body)
			r.Body = io.NopCloser(bytes.NewReader(raw))
			var p struct {
				Complete bool `json:"is_complete"`
				Reset    bool `json:"reset"`
			}
			_ = json.Unmarshal(raw, &p)
			if p.Complete {
				completed.Add(1)
			}
			if p.Reset {
				resets.Add(1)
			}
			next.ServeHTTP(w, r)
			if !p.Reset && mutated.CompareAndSwap(false, true) {
				future := time.Now().Add(time.Hour)
				if err := os.Chtimes(inside, future, future); err != nil {
					t.Error(err)
				}
			}
		})
	})
	s.Client = c2
	if _, failed := callMCP(t, &s, context.Background(), "invoke_action", "upload_file_stream", nil, map[string]any{"input_file": inside}); !failed {
		t.Fatal("changed source was committed")
	}
	stats, err := streams.Snapshot("")
	if err != nil || completed.Load() != 0 || resets.Load() != 1 || stats.FileCount != 0 || stats.ReservedBytes != 0 {
		t.Fatal("mutation failed to reset before commit", completed.Load(), resets.Load(), stats, err)
	}
}

func TestMCPDownloadQuotaLockOrphanRecoveryAndDirectoryReplacement(t *testing.T) {
	c, _ := mcpPeer(t, nil)
	remote, err := c.UploadFile(context.Background(), "small.bin", strings.NewReader("1234"), 4)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	config := TransferConfig{DownloadDir: directory, MaxFileBytes: 4}
	transfers := mcpTransfers(t, config)
	if second, err := OpenTransfers(config); err == nil {
		_ = second.Close()
		t.Fatal("second directory owner admitted")
	}
	for i := 0; i < 2; i++ {
		if _, err := transfers.call(context.Background(), c, "download_file_stream", map[string]any{"file": remote.FilePath}, ""); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := transfers.call(context.Background(), c, "download_file_stream", map[string]any{"file": remote.FilePath}, ""); err == nil {
		t.Fatal("retained files not charged")
	}
	files := downloadedFiles(t, directory)
	if err := os.Remove(files[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := transfers.call(context.Background(), c, "download_file_stream", map[string]any{"file": remote.FilePath}, ""); err != nil {
		t.Fatal("operator deletion did not release quota", err)
	}
	if err := transfers.Close(); err != nil {
		t.Fatal(err)
	}
	part := filepath.Join(directory, "mcp-"+strings.Repeat("a", 32)+".part")
	if err := os.WriteFile(part, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	pairedPart := filepath.Join(directory, "mcp-"+strings.Repeat("b", 32)+".part")
	pairedFinal := strings.TrimSuffix(pairedPart, ".part") + ".bin"
	if err := os.WriteFile(pairedPart, []byte("y"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(pairedPart, pairedFinal); err != nil {
		t.Fatal(err)
	}
	transfers = mcpTransfers(t, config)
	if _, err := os.Stat(part); !os.IsNotExist(err) {
		t.Fatal("orphan partial remained", err)
	}
	if actual, err := os.ReadFile(pairedFinal); err != nil || string(actual) != "y" {
		t.Fatal("interrupted publication lost complete bytes", err)
	}
	downloadedFiles(t, directory)
	if err := transfers.Close(); err != nil {
		t.Fatal(err)
	}
	for _, path := range downloadedFiles(t, directory) {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	transfers = mcpTransfers(t, config)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if err := os.Rename(directory, directory+"-old"); err != nil {
			t.Error(err)
			return
		}
		if err := os.Mkdir(directory, 0700); err != nil {
			t.Error(err)
			return
		}
		if err := os.WriteFile(filepath.Join(directory, "sentinel"), []byte("untouched"), 0600); err != nil {
			t.Error(err)
			return
		}
		_, _ = io.WriteString(w, validFileFrames([]byte("x")))
	}))
	defer peer.Close()
	other, _ := client.New(peer.URL, "synthetic-mcp-token")
	if _, err := transfers.call(context.Background(), other, "download_file_stream", map[string]any{"file": "123"}, ""); err == nil {
		t.Fatal("replaced output directory accepted")
	}
	if actual, err := os.ReadFile(filepath.Join(directory, "sentinel")); err != nil || string(actual) != "untouched" {
		t.Fatal("replacement directory modified")
	}
	if len(downloadedFiles(t, directory+"-old")) != 0 {
		t.Fatal("old pinned directory leaked partial")
	}
	if err := os.RemoveAll(directory + "-old"); err != nil {
		t.Fatal(err)
	}
}

func TestMCPCleanupFailureFencesOwnerUntilSuccessfulReopen(t *testing.T) {
	for _, fault := range []string{"unlink", "directory_sync"} {
		t.Run(fault, func(t *testing.T) {
			var calls atomic.Int32
			var valid atomic.Bool
			body := validFileFrames([]byte("x"))
			lines := strings.SplitAfter(body, "\n")
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				if valid.Load() {
					_, _ = io.WriteString(w, body)
				} else {
					_, _ = io.WriteString(w, lines[0]+lines[1])
				}
			}))
			defer peer.Close()
			c, _ := client.New(peer.URL, "synthetic-mcp-token")
			config := TransferConfig{DownloadDir: t.TempDir()}
			transfers := mcpTransfers(t, config)
			if fault == "unlink" {
				transfers.removeFile = func(string) error { return errors.New("synthetic unlink failure") }
			} else {
				transfers.syncDir = func() error { return errors.New("synthetic sync failure") }
			}
			if _, err := transfers.call(context.Background(), c, "download_file_stream", map[string]any{"file": "123"}, ""); !errors.Is(err, errTransferStorage) {
				t.Fatal("cleanup failure not observable", err)
			}
			transfers.removeFile = nil
			transfers.syncDir = nil
			if _, err := transfers.call(context.Background(), c, "download_file_stream", map[string]any{"file": "123"}, ""); !errors.Is(err, errTransferStorage) || calls.Load() != 1 {
				t.Fatal("uncertain cleanup released quota for another transfer", calls.Load(), err)
			}
			if err := transfers.Close(); !errors.Is(err, errTransferStorage) {
				t.Fatal(err)
			}
			valid.Store(true)
			reopened := mcpTransfers(t, config)
			if _, err := reopened.call(context.Background(), c, "download_file_stream", map[string]any{"file": "123"}, ""); err != nil || calls.Load() != 2 {
				t.Fatal("successful startup reconciliation did not restore owner", err)
			}
			downloadedFiles(t, reopened.directory)
		})
	}
}

func TestMCPPublicationRefusesSubstitutedFileOrDirectory(t *testing.T) {
	for _, replacement := range []string{"part_symlink", "part_regular", "directory_symlink", "directory_permissions"} {
		t.Run(replacement, func(t *testing.T) {
			transfers := mcpTransfers(t, TransferConfig{})
			directory := transfers.directory
			outside := filepath.Join(t.TempDir(), "outside")
			if err := os.WriteFile(outside, []byte("untouched"), 0600); err != nil {
				t.Fatal(err)
			}
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if strings.HasPrefix(replacement, "part_") {
					entries, err := os.ReadDir(directory)
					if err != nil {
						t.Error(err)
						return
					}
					for _, entry := range entries {
						if strings.HasSuffix(entry.Name(), ".part") {
							path := filepath.Join(directory, entry.Name())
							if err = os.Remove(path); err != nil {
								t.Error(err)
								return
							}
							if replacement == "part_symlink" {
								err = os.Symlink(outside, path)
							} else {
								err = os.WriteFile(path, []byte("substitute"), 0600)
							}
							if err != nil {
								t.Error(err)
								return
							}
						}
					}
				} else if replacement == "directory_symlink" {
					if err := os.Rename(directory, directory+"-old"); err != nil {
						t.Error(err)
						return
					}
					if err := os.Symlink(directory+"-old", directory); err != nil {
						t.Error(err)
						return
					}
				} else {
					if err := os.Chmod(directory, 0755); err != nil {
						t.Error(err)
						return
					}
				}
				_, _ = io.WriteString(w, validFileFrames([]byte("x")))
			}))
			defer peer.Close()
			c, _ := client.New(peer.URL, "synthetic-mcp-token")
			if _, err := transfers.call(context.Background(), c, "download_file_stream", map[string]any{"file": "123"}, ""); !errors.Is(err, errTransferStorage) {
				t.Fatal("substitution produced a successful publication", err)
			}
			if actual, err := os.ReadFile(outside); err != nil || string(actual) != "untouched" {
				t.Fatal("substitution modified outside bytes")
			}
			if replacement == "directory_symlink" {
				downloadedFiles(t, directory+"-old")
				if err := os.Remove(directory); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(directory+"-old", directory); err != nil {
					t.Fatal(err)
				}
			}
			if len(downloadedFiles(t, directory)) != 0 {
				t.Fatal("substitute was published")
			}
		})
	}
}

func TestMCPCancelledDownloadRetainsCleanupFailureThroughClose(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, strings.SplitAfter(validFileFrames([]byte("x")), "\n")[0])
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer peer.Close()
	c, _ := client.New(peer.URL, "synthetic-mcp-token")
	config := TransferConfig{DownloadDir: t.TempDir(), Timeout: 20 * time.Millisecond}
	transfers := mcpTransfers(t, config)
	transfers.syncDir = func() error { return errors.New("synthetic cleanup sync failure") }
	if _, err := transfers.call(context.Background(), c, "download_file_stream", map[string]any{"file": "123"}, ""); !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, errTransferStorage) {
		t.Fatal("cancellation hid cleanup failure", err)
	}
	if err := transfers.Close(); !errors.Is(err, errTransferStorage) {
		t.Fatal("Close hid already-failed cleanup", err)
	}
	// Reporting the error must not keep the old directory lock held.
	reopened := mcpTransfers(t, config)
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}
