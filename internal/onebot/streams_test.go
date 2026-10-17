package onebot

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"image"
	"image/png"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JFSAS/wispcat/internal/store"
	"github.com/JFSAS/wispcat/pkg/bot"
	"github.com/JFSAS/wispcat/pkg/client"
	"github.com/gorilla/websocket"
)

func streamFixture(t *testing.T) (*Runtime, *fakeChannel, *Streams) {
	t.Helper()
	r, c := fixture(t)
	s, err := OpenStreams(filepath.Join(t.TempDir(), "streams"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	r.Streams = s
	return r, c, s
}
func streamInvoke(r *Runtime, action string, params any) Response {
	raw, _ := json.Marshal(params)
	var q map[string]json.RawMessage
	_ = json.Unmarshal(raw, &q)
	return r.Invoke(context.Background(), Request{Action: action, Params: q})
}
func uploadStreamFixture(t *testing.T, s *Streams, account, id string, data []byte) string {
	t.Helper()
	total := (len(data) + streamChunkLimit - 1) / streamChunkLimit
	for index := 0; index < total; index++ {
		end := (index + 1) * streamChunkLimit
		if end > len(data) {
			end = len(data)
		}
		_, err := s.upload(context.Background(), account, streamUpload{ID: id, Name: "fixture.bin", Total: total, Index: &index, Data: data[index*streamChunkLimit : end]})
		if err != nil {
			t.Fatal(err)
		}
	}
	out, err := s.upload(context.Background(), account, streamUpload{ID: id, Complete: true})
	if err != nil {
		t.Fatal(err)
	}
	return out["file_path"].(string)
}

func TestStreamEncryptedChunksIsolationAndActiveCleanup(t *testing.T) {
	_, _, s := streamFixture(t)
	body := bytes.Repeat([]byte("private temporary transfer marker"), 20000)
	total := (len(body) + streamChunkLimit - 1) / streamChunkLimit
	for _, index := range []int{2, 0, 1, 0} {
		end := (index + 1) * streamChunkLimit
		if end > len(body) {
			end = len(body)
		}
		if _, err := s.upload(context.Background(), "account-a", streamUpload{ID: "same-name", Total: total, Index: &index, Data: body[index*streamChunkLimit : end]}); err != nil {
			t.Fatal(err)
		}
	}
	index := 0
	if _, err := s.upload(context.Background(), "account-a", streamUpload{ID: "same-name", Index: &index, Data: []byte("conflicting bytes")}); !errors.Is(err, errStreamConflict) {
		t.Fatal(err)
	}
	out, err := s.upload(context.Background(), "account-a", streamUpload{ID: "same-name", Complete: true})
	if err != nil {
		t.Fatal(err)
	}
	reference := out["file_path"].(string)
	if _, err = s.acquire("account-b", reference); !errors.Is(err, errStreamMissing) {
		t.Fatal("cross-account transfer resolved", err)
	}
	reader, err := s.acquire("account-a", reference)
	if err != nil {
		t.Fatal(err)
	}
	cleanup, err := s.Clean("account-a")
	if err != nil || cleanup.Removed != 0 || cleanup.SkippedActive != 1 {
		t.Fatal(cleanup, err)
	}
	actual, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(actual, body) {
		t.Fatal("leased transfer changed", err)
	}
	_ = reader.Close()
	err = filepath.WalkDir(s.directory, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(raw, []byte("private temporary transfer marker")) {
			t.Fatal("plaintext transfer persisted")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.upload(context.Background(), "account-a", streamUpload{ID: "in-progress", Total: 2, Index: &index, Data: []byte("pending")})
	if err != nil {
		t.Fatal(err)
	}
	cleanup, err = s.Clean("account-a")
	if err != nil || cleanup.Removed < 2 || cleanup.SkippedActive != 1 {
		t.Fatal(cleanup, err)
	}
	if _, err = s.acquire("account-a", reference); !errors.Is(err, errStreamMissing) {
		t.Fatal("cleaned token remained readable", err)
	}
	s.mu.Lock()
	future := s.now().Add(11 * time.Minute)
	s.now = func() time.Time { return future }
	s.mu.Unlock()
	if _, err = s.Clean("account-a"); err != nil {
		t.Fatal(err)
	}
	stats, err := s.Snapshot("")
	if err != nil || stats.FileCount != 0 || stats.ReservedBytes != 0 || stats.Uploads != 0 {
		t.Fatal(stats, err)
	}
}

func TestStreamAmbiguousChunkCommitRetainsQuotaAndNeedsRetry(t *testing.T) {
	_, _, s := streamFixture(t)
	index := 0
	data := bytes.Repeat([]byte("x"), 4096)
	s.writeChunk = func(cache store.MediaCache, id string, data []byte) error {
		if err := cache.Save(id, data); err != nil {
			return err
		}
		return errors.New("synthetic post-rename sync failure")
	}
	_, err := s.upload(context.Background(), "account-a", streamUpload{ID: "ambiguous", Total: 1, Index: &index, Data: data})
	if !errors.Is(err, errStreamStorage) {
		t.Fatal(err)
	}
	stats, err := s.Snapshot("account-a")
	if err != nil || stats.ReservedBytes <= int64(len(data)) || stats.FileCount != 2 {
		t.Fatal(stats, err)
	}
	status, err := s.upload(context.Background(), "account-a", streamUpload{ID: "ambiguous", Verify: true})
	if err != nil || status["received_chunks"] != 0 {
		t.Fatal("failed write acknowledged", status, err)
	}
	if _, err = s.upload(context.Background(), "account-a", streamUpload{ID: "ambiguous", Index: &index, Data: []byte("different")}); !errors.Is(err, errStreamConflict) {
		t.Fatal(err)
	}
	s.writeChunk = nil
	if _, err = s.upload(context.Background(), "account-a", streamUpload{ID: "ambiguous", Index: &index, Data: data}); err != nil {
		t.Fatal(err)
	}
	after, err := s.Snapshot("account-a")
	if err != nil || after.ReservedBytes != stats.ReservedBytes {
		t.Fatal("retry double-counted", after, err)
	}
	if _, err = s.upload(context.Background(), "account-a", streamUpload{ID: "ambiguous", Complete: true}); err != nil {
		t.Fatal(err)
	}
}

func TestStreamMetadataAndRequestBoundsPrecedeEffects(t *testing.T) {
	r, _, s := streamFixture(t)
	for _, params := range []map[string]any{
		{"stream_id": "../escape", "total_chunks": 1}, {"stream_id": "s", "total_chunks": 0}, {"stream_id": "s", "total_chunks": 1025},
		{"stream_id": "s", "total_chunks": 1, "filename": "../../private"}, {"stream_id": "s", "total_chunks": 1, "file_size": streamFileLimit + 1},
		{"stream_id": "s", "total_chunks": 1, "chunk_index": -1, "chunk_data": "YQ=="}, {"stream_id": "s", "total_chunks": 1, "chunk_index": 0, "chunk_data": "invalid!=="},
		{"stream_id": "s", "total_chunks": 1, "expected_sha256": "bad"}, {"stream_id": "s", "total_chunks": 1, "file_retention": 3600001},
		{"stream_id": "s", "total_chunks": 1, "chunk_index": 0, "chunk_data": base64.StdEncoding.EncodeToString(make([]byte, streamChunkLimit+1))},
	} {
		if response := streamInvoke(r, "upload_file_stream", params); response.Retcode == 0 {
			t.Fatal("invalid transfer accepted")
		}
	}
	stats, err := s.Snapshot("")
	if err != nil || stats.Uploads != 0 || stats.FileCount != 0 {
		t.Fatal("invalid input created state", stats, err)
	}
	body := []byte("complete metadata validation")
	ref := uploadStreamFixture(t, s, "account-a", "complete", body)
	wrongSize := int64(len(body) + 1)
	for _, p := range []streamUpload{{ID: "complete", Size: &wrongSize}, {ID: "complete", SHA256: strings.Repeat("0", 64)}} {
		if _, err := s.upload(context.Background(), "account-a", p); !errors.Is(err, errStreamConflict) {
			t.Fatal(err)
		}
	}
	reader, err := s.acquire("account-a", ref)
	if err != nil {
		t.Fatal(err)
	}
	_ = reader.Close()
}

func TestStreamConcurrentDuplicatesAndIntegrityFailure(t *testing.T) {
	_, _, s := streamFixture(t)
	index := 0
	body := bytes.Repeat([]byte("parallel"), 1000)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.upload(context.Background(), "account-a", streamUpload{ID: "concurrent", Total: 1, Index: &index, Data: body}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	out, err := s.upload(context.Background(), "account-a", streamUpload{ID: "concurrent", Complete: true})
	if err != nil || out["received_chunks"] != 1 {
		t.Fatal(out, err)
	}
	ref := out["file_path"].(string)
	reader, err := s.acquire("account-a", ref)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(s.directory, strings.TrimPrefix(ref, "stream://"), streamPart(0)+".enc")
	if err = os.WriteFile(path, []byte("corrupt encrypted chunk"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = io.ReadAll(reader); !errors.Is(err, errStreamStorage) {
		t.Fatal("corrupt bytes became content", err)
	}
	_ = reader.Close()
}

func TestStreamDiskBudgetBoundsRealEncryptedFiles(t *testing.T) {
	_, _, s := streamFixture(t)
	body := bytes.Repeat([]byte("q"), streamChunkLimit)
	limited := false
	lastID := ""
	for file := 0; file < 9 && !limited; file++ {
		lastID = "quota-" + strconv.Itoa(file)
		for index := 0; index < streamFileLimit/streamChunkLimit; index++ {
			_, err := s.upload(context.Background(), "account-a", streamUpload{ID: lastID, Total: streamFileLimit / streamChunkLimit, Index: &index, Data: body})
			if errors.Is(err, errStreamLimit) {
				limited = true
				break
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		if !limited {
			if _, err := s.upload(context.Background(), "account-a", streamUpload{ID: lastID, Complete: true}); err != nil {
				t.Fatal(err)
			}
		}
	}
	stats, err := s.Snapshot("account-a")
	if err != nil || !limited || stats.TotalBytes > streamDiskLimit || stats.ReservedBytes > streamDiskLimit || stats.Completed < 6 {
		t.Fatal(stats, limited, err)
	}
	t.Logf("STREAM_STORAGE_LOAD_REPORT={\"total_bytes\":%d,\"reserved_bytes\":%d,\"file_count\":%d,\"completed_files\":%d,\"active_uploads\":%d,\"quota_bytes\":%d,\"limit_observed\":true,\"live_wechat_requests\":0}", stats.TotalBytes, stats.ReservedBytes, stats.FileCount, stats.Completed, stats.Uploads, streamDiskLimit)
	cleanup, err := s.Clean("account-a")
	if err != nil || cleanup.SkippedActive != 1 {
		t.Fatal(cleanup, err)
	}
	if _, err = s.upload(context.Background(), "account-a", streamUpload{ID: lastID, Reset: true}); err != nil {
		t.Fatal(err)
	}
	stats, err = s.Snapshot("")
	if err != nil || stats.TotalBytes != 0 || stats.ReservedBytes != 0 {
		t.Fatal(stats, err)
	}
}

func TestStreamCrashCleanupAndExclusiveOwnership(t *testing.T) {
	if directory := os.Getenv("WX_STREAM_TEST_CHILD"); directory != "" {
		s, err := OpenStreams(directory)
		if err != nil {
			t.Fatal(err)
		}
		index := 0
		if os.Getenv("WX_STREAM_TEST_WINDOW") == "atomic" {
			s.writeChunk = func(cache store.MediaCache, id string, data []byte) error {
				if err := cache.Save(id, data); err != nil {
					return err
				}
				ciphertext, err := os.ReadFile(filepath.Join(cache.Directory, id+".enc"))
				if err != nil {
					return err
				}
				if err = os.Remove(filepath.Join(cache.Directory, id+".enc")); err != nil {
					return err
				}
				temporary, err := os.CreateTemp(cache.Directory, ".write-*")
				if err != nil {
					return err
				}
				if _, err = temporary.Write(ciphertext[:len(ciphertext)/2]); err != nil {
					return err
				}
				if err = temporary.Sync(); err != nil {
					return err
				}
				_, _ = os.Stdout.WriteString("STREAM_CHILD_READY\n")
				select {}
			}
		}
		if _, err = s.upload(context.Background(), "synthetic", streamUpload{ID: "crash", Total: 1, Index: &index, Data: []byte("orphan fixture")}); err != nil {
			t.Fatal(err)
		}
		_, _ = os.Stdout.WriteString("STREAM_CHILD_READY\n")
		select {}
	}
	for _, window := range []string{"acknowledged", "atomic"} {
		t.Run(window, func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "streams")
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStreamCrashCleanupAndExclusiveOwnership$")
			command.Env = []string{"WX_STREAM_TEST_CHILD=" + directory, "WX_STREAM_TEST_WINDOW=" + window, "PATH=" + os.Getenv("PATH")}
			stdout, err := command.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err = command.Start(); err != nil {
				t.Fatal(err)
			}
			ready := make(chan bool, 1)
			go func() {
				scan := bufio.NewScanner(stdout)
				for scan.Scan() {
					if scan.Text() == "STREAM_CHILD_READY" {
						ready <- true
						return
					}
				}
				ready <- false
			}()
			select {
			case ok := <-ready:
				if !ok {
					_ = command.Process.Kill()
					_ = command.Wait()
					t.Fatal("child failed")
				}
			case <-ctx.Done():
				_ = command.Process.Kill()
				_ = command.Wait()
				t.Fatal(ctx.Err())
			}
			if _, err = OpenStreams(directory); err == nil {
				_ = command.Process.Kill()
				_ = command.Wait()
				t.Fatal("second owner acquired live storage")
			}
			_ = command.Process.Kill()
			_ = command.Wait()
			s, err := OpenStreams(directory)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			entries, err := os.ReadDir(directory)
			if err != nil || len(entries) != 1 || entries[0].Name() != ".lock" {
				t.Fatal("crashed ephemeral chunks survived restart", err)
			}
		})
	}
}

func TestStreamConflictCannotChangeUploadMetadata(t *testing.T) {
	_, _, s := streamFixture(t)
	index := 0
	body := []byte("original content")
	if _, err := s.upload(context.Background(), "account-a", streamUpload{ID: "metadata", Total: 1, Index: &index, Data: body}); err != nil {
		t.Fatal(err)
	}
	size := int64(1000)
	retention := time.Hour
	_, err := s.upload(context.Background(), "account-a", streamUpload{ID: "metadata", Index: &index, Data: []byte("conflict"), Size: &size, SHA256: strings.Repeat("0", 64), Retention: &retention})
	if !errors.Is(err, errStreamConflict) {
		t.Fatal(err)
	}
	out, err := s.upload(context.Background(), "account-a", streamUpload{ID: "metadata", Complete: true})
	if err != nil || out["file_size"] != int64(len(body)) {
		t.Fatal("rejected chunk poisoned valid upload", out, err)
	}
}

func TestStreamCancellationAndStorageFailurePrecedeSuccessTerminal(t *testing.T) {
	for _, fault := range []string{"cancel", "storage"} {
		t.Run(fault, func(t *testing.T) {
			r, c, s := streamFixture(t)
			ref := uploadStreamFixture(t, s, "account-a", "terminal", []byte("one final chunk"))
			raw, _ := json.Marshal(ref)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			success := false
			failed := false
			err := r.InvokeFrames(ctx, Request{Action: "download_file_stream", Params: map[string]json.RawMessage{"file": raw}}, func(response Response) error {
				data := response.Data.(map[string]any)
				if data["data_type"] == "file_chunk" {
					if fault == "cancel" {
						cancel()
					} else {
						c.mu.Lock()
						c.snap.Error = "synthetic_storage_failure"
						c.mu.Unlock()
					}
				}
				if data["type"] == "response" {
					success = true
				}
				if response.Retcode == 1500 {
					failed = true
				}
				return nil
			})
			if success || fault == "cancel" && !errors.Is(err, context.Canceled) || fault == "storage" && (!failed || err != nil) {
				t.Fatal("unsafe terminal", fault, success, failed, err)
			}
		})
	}
}

func TestStreamStorageFailureStaysTypedDuringSend(t *testing.T) {
	r, c, s := streamFixture(t)
	ref := uploadStreamFixture(t, s, "account-a", "broken", []byte("file fixture"))
	token := strings.TrimPrefix(ref, "stream://")
	if err := os.WriteFile(filepath.Join(s.directory, token, streamPart(0)+".enc"), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	user, _ := r.State.ID("account-a", "user", "friend")
	out := streamInvoke(r, "send_private_msg", map[string]any{"user_id": user, "message": []any{map[string]any{"type": "file", "data": map[string]string{"file": ref, "name": "fixture.bin"}}}})
	if out.Retcode != 1500 || out.Message != "runtime_storage_unavailable" || len(c.sends) != 0 {
		t.Fatal("stream storage failure downgraded to invalid input", out)
	}
}

func TestStreamDownloadSourcesAndLocalPathRefusal(t *testing.T) {
	r, c, _ := streamFixture(t)
	var pngBody bytes.Buffer
	if err := png.Encode(&pngBody, image.NewRGBA(image.Rect(0, 0, 1, 1))); err != nil {
		t.Fatal(err)
	}
	r.Channel = mediaChannel{c, bot.Content{Data: pngBody.Bytes(), MIME: "image/png", Name: "fixture.png"}}
	fileID, err := r.State.ID("account-a", "file", "friend\x00source/regular")
	if err != nil {
		t.Fatal(err)
	}
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/redirect" {
			http.Redirect(w, request, "https://example.invalid", 302)
			return
		}
		_, _ = w.Write(pngBody.Bytes())
	}))
	defer peer.Close()
	for _, source := range []string{strconv.FormatInt(fileID, 10), peer.URL} {
		raw, _ := json.Marshal(source)
		var terminal Response
		var received bytes.Buffer
		err = r.InvokeFrames(context.Background(), Request{Action: "download_file_image_stream", Params: map[string]json.RawMessage{"file": raw}}, func(response Response) error {
			terminal = response
			data := response.Data.(map[string]any)
			if data["data_type"] == "file_chunk" {
				body, e := base64.StdEncoding.DecodeString(data["data"].(string))
				if e != nil {
					return e
				}
				_, _ = received.Write(body)
			}
			return nil
		})
		if err != nil || terminal.Retcode != 0 || !bytes.Equal(received.Bytes(), pngBody.Bytes()) {
			t.Fatal("download source failed", terminal, err)
		}
	}
	for _, source := range []string{"file:///unrelated/private.txt", "/unrelated/private.txt", peer.URL + "/redirect"} {
		raw, _ := json.Marshal(source)
		var terminal Response
		err = r.InvokeFrames(context.Background(), Request{Action: "download_file_stream", Params: map[string]json.RawMessage{"file": raw}}, func(response Response) error { terminal = response; return nil })
		if err != nil || terminal.Retcode == 0 {
			t.Fatal("unsafe source accepted", terminal, err)
		}
	}
	r.Channel = unavailableStorageChannel{fakeChannel: c}
	raw, _ := json.Marshal(strconv.FormatInt(fileID, 10))
	var terminal Response
	if err = r.InvokeFrames(context.Background(), Request{Action: "download_file_stream", Params: map[string]json.RawMessage{"file": raw}}, func(response Response) error { terminal = response; return nil }); err != nil || terminal.Retcode != 1500 {
		t.Fatal("native storage error not preserved", terminal, err)
	}
}

func TestStreamUnsafeRootAndEntryCannotEscapeCleanup(t *testing.T) {
	directory := t.TempDir()
	outside := t.TempDir()
	secret := filepath.Join(outside, "private.txt")
	if err := os.WriteFile(secret, []byte("unrelated fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(directory, "linked")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	if s, err := OpenStreams(link); err == nil {
		_ = s.Close()
		t.Fatal("symlink root accepted")
	}
	_, _, s := streamFixture(t)
	ref := uploadStreamFixture(t, s, "account-a", "cleanup", []byte("payload"))
	token := strings.TrimPrefix(ref, "stream://")
	path := filepath.Join(s.directory, token, streamPart(0)+".enc")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(secret, path); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Clean("account-a"); !errors.Is(err, errStreamStorage) {
		t.Fatal("unsafe cleanup silently succeeded", err)
	}
	actual, err := os.ReadFile(secret)
	if err != nil || string(actual) != "unrelated fixture" {
		t.Fatal("cleanup modified unrelated content", err)
	}
	// Remove the injected link so normal fixture close can release its owner.
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
}

func TestStreamSDKUploadDownloadSendAndCleanupOverHTTP(t *testing.T) {
	r, c, s := streamFixture(t)
	server := NewServer(r, "synthetic-token", "")
	peer := httptest.NewServer(server)
	defer peer.Close()
	defer server.Close(context.Background())
	sdk, err := client.New(peer.URL, "synthetic-token")
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte("sdk multipart fixture"), 30000)
	file, err := sdk.UploadFile(context.Background(), "upload.bin", bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	download, err := sdk.DownloadFile(context.Background(), file.FilePath, &output)
	if err != nil || !bytes.Equal(output.Bytes(), body) || download.SHA256 != file.SHA256 {
		t.Fatal("SDK stream round trip failed", err)
	}
	user, _ := r.State.ID("account-a", "user", "friend")
	_, err = sdk.Call(context.Background(), "send_private_msg", map[string]any{"user_id": user, "message": []any{map[string]any{"type": "file", "data": map[string]string{"file": file.FilePath, "name": "upload.bin"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(c.sends) != 1 || c.sends[0].Upload == nil || !bytes.Equal(c.sends[0].Upload.Data, body) {
		t.Fatal("stream reference did not reach normalized submit")
	}
	if _, err = sdk.Call(context.Background(), "clean_stream_temp_file", nil); err != nil {
		t.Fatal(err)
	}
	stats, err := s.Snapshot("")
	if err != nil || stats.ReservedBytes != 0 {
		t.Fatal(stats, err)
	}
	if _, err = sdk.DownloadFile(context.Background(), file.FilePath, io.Discard); err == nil {
		t.Fatal("cleaned stream still served")
	}
}

func TestStreamWebSocketFramesPreserveStructuredEchoAndErrors(t *testing.T) {
	r, _, _ := streamFixture(t)
	server := NewServer(r, "synthetic-token", "")
	peer := httptest.NewServer(server)
	defer peer.Close()
	defer server.Close(context.Background())
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(peer.URL, "http")+"/api", http.Header{"Authorization": []string{"Bearer synthetic-token"}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	echo := json.RawMessage(`{"request":[1,"stream"],"n":null}`)
	if err = conn.WriteJSON(Request{Action: "test_download_stream", Echo: echo}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 11; i++ {
		var response Response
		if err = conn.ReadJSON(&response); err != nil {
			t.Fatal(err)
		}
		if response.Stream != "stream-action" || response.Status != "ok" || !bytes.Equal(response.Echo, echo) {
			t.Fatal("stream echo/envelope changed", response)
		}
		data := response.Data.(map[string]any)
		if i < 10 && data["type"] != "stream" || i == 10 && data["type"] != "response" {
			t.Fatal(data)
		}
	}
	if err = conn.WriteJSON(Request{Action: "test_download_stream", Params: map[string]json.RawMessage{"error": json.RawMessage("true")}, Echo: echo}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 11; i++ {
		var response Response
		if err = conn.ReadJSON(&response); err != nil {
			t.Fatal(err)
		}
		if i == 10 && (response.Status != "failed" || response.Data.(map[string]any)["type"] != "error") {
			t.Fatal("missing terminal error", response)
		}
	}
}

func TestStreamSinkFailureAndAccountChangeStopFrames(t *testing.T) {
	r, c, _ := streamFixture(t)
	count := 0
	err := r.InvokeFrames(context.Background(), Request{Action: "test_download_stream"}, func(Response) error { count++; return io.ErrClosedPipe })
	if !errors.Is(err, io.ErrClosedPipe) || count != 1 {
		t.Fatal("sink failure emitted extra frames", count, err)
	}
	count = 0
	err = r.InvokeFrames(context.Background(), Request{Action: "test_download_stream"}, func(Response) error { count++; c.mu.Lock(); c.snap.Account = "account-b"; c.mu.Unlock(); return nil })
	if err == nil || count != 1 {
		t.Fatal("account changed during stream", count, err)
	}
}

func TestStreamCompletionHashAndExpiredReaderFences(t *testing.T) {
	r, _, s := streamFixture(t)
	body := []byte("hash verification fixture")
	index := 0
	if _, err := s.upload(context.Background(), "account-a", streamUpload{ID: "hash", Total: 1, Index: &index, Data: body}); err != nil {
		t.Fatal(err)
	}
	if response := streamInvoke(r, "upload_file_stream", map[string]any{"stream_id": "hash", "is_complete": true, "expected_sha256": strings.Repeat("0", 64)}); response.Retcode == 0 {
		t.Fatal("wrong final hash accepted")
	}
	if _, err := s.acquire("account-a", "stream://"+strings.Repeat("0", 32)); !errors.Is(err, errStreamMissing) {
		t.Fatal(err)
	}
	if _, err := s.upload(context.Background(), "account-a", streamUpload{ID: "hash", Reset: true}); err != nil {
		t.Fatal(err)
	}
	ref := uploadStreamFixture(t, s, "account-a", "correct", body)
	reader, err := s.acquire("account-a", ref)
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	future := s.now().Add(6 * time.Minute)
	s.now = func() time.Time { return future }
	s.mu.Unlock()
	if _, err = s.Clean("account-a"); err != nil {
		t.Fatal(err)
	}
	if reopened, e := s.acquire("account-a", ref); e == nil {
		_ = reopened.Close()
		t.Fatal("expired active lease allowed a new reader")
	}
	if data, err := io.ReadAll(reader); err != nil || !bytes.Equal(data, body) {
		t.Fatal("active lease expired during read", err)
	}
	_ = reader.Close()
	if _, err = s.acquire("account-a", ref); !errors.Is(err, errStreamMissing) {
		t.Fatal("expired reference reopened", err)
	}
	closingRef := uploadStreamFixture(t, s, "account-a", "closing", body)
	active, err := s.acquire("account-a", closingRef)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = active.Read(make([]byte, 1)); !errors.Is(err, errStreamClosed) {
		t.Fatal("closed storage served bytes", err)
	}
	_ = active.Close()
}
