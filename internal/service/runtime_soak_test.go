//go:build runtimefixtures

package service

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JFSAS/wispcat/internal/cdn"
	"github.com/JFSAS/wispcat/internal/onebot"
	"github.com/JFSAS/wispcat/internal/protocol"
	"github.com/JFSAS/wispcat/internal/store"
	"github.com/JFSAS/wispcat/pkg/client"
)

const soakToken = "synthetic-offline-soak-token"

// The runner binds this into the fixture binary before measurement starts.
var soakSourceManifestSHA string

type soakMetrics struct {
	completed, sends, downloads, streams, heartbeats atomic.Uint64
	mu                                               sync.Mutex
	self, firstUser, firstMessage, firstReply        int64
	firstAction                                      string
	firstParams                                      map[string]any
	firstKind, firstBody, firstReplyFile             string
	firstGroup                                       int64
	latencyMax                                       time.Duration
}

type soakAccount struct {
	adapter BotAdapter
	server  *onebot.Server
	http    *httptest.Server
	client  *client.Client
	metrics *soakMetrics
	body    []byte
	ready   chan struct{}
}

type soakPhase struct {
	ctx      context.Context
	cancel   context.CancelFunc
	accounts []*soakAccount
	state    *onebot.State
	wg       sync.WaitGroup
	errors   chan error
	closed   bool
}

func (p *soakPhase) fail(err error) {
	if err != nil && p.ctx.Err() == nil {
		select {
		case p.errors <- err:
		default:
		}
	}
}

func (p *soakPhase) close() error {
	if p.closed {
		return nil
	}
	p.cancel()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var result error
	for _, a := range p.accounts {
		if a.server != nil {
			result = errors.Join(result, a.server.Close(ctx))
		}
		if a.http != nil {
			a.http.Close()
		}
	}
	done := make(chan struct{})
	go func() { p.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		// Never close databases while a remaining user may still own them.
		return errors.Join(result, ctx.Err())
	}
	for _, a := range p.accounts {
		if a.server != nil && a.server.Runtime.Streams != nil {
			result = errors.Join(result, a.server.Runtime.Streams.Close())
		}
		if a.adapter.Service.history != nil {
			result = errors.Join(result, a.adapter.Service.history.Close())
		}
		if a.adapter.Archive != nil {
			result = errors.Join(result, a.adapter.Archive.Close())
		}
	}
	if p.state != nil {
		result = errors.Join(result, p.state.Close())
	}
	// Cleanup callbacks retain p until test end. Drop closed owners so the
	// quiescent heap samples do not measure the fixture retaining old phases.
	p.accounts, p.state, p.closed = nil, nil, true
	return result
}

func soakDownload(ctx context.Context, c *client.Client, reference string, expected []byte) error {
	var output bytes.Buffer
	if _, err := c.DownloadFile(ctx, reference, &output); err != nil {
		return err
	}
	if !bytes.Equal(output.Bytes(), expected) {
		return errors.New("soak_download_content_mismatch")
	}
	return nil
}

func newSoakPhase(t *testing.T, root string, metrics []*soakMetrics, body []byte, phase int) *soakPhase {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	p := &soakPhase{ctx: ctx, cancel: cancel, errors: make(chan error, 8)}
	t.Cleanup(func() { _ = p.close() })
	state, err := onebot.OpenSharedState(filepath.Join(root, "onebot"), nil)
	if err != nil {
		t.Fatal(err)
	}
	p.state = state
	for index, metric := range metrics {
		directory := filepath.Join(root, fmt.Sprintf("account-%d", index))
		s := New(directory, time.Second, "/synthetic/WeChat.app")
		if phase == 0 {
			b := protocol.Bootstrap{Username: []byte(fmt.Sprintf("synthetic-owner-%d", index)), UIN: uint32(1000 + index), DeviceID: []byte(fmt.Sprintf("synthetic-device-%d", index))}
			s.data = &store.Data{Schema: 1, Binding: b.Binding(), SourceApp: s.source.App, APIKey: bytes.Repeat([]byte{byte(index + 1)}, 32), NextClientID: 1, Session: &protocol.Session{Bootstrap: b, Original: bytes.Repeat([]byte{2}, 16), ClientKey: bytes.Repeat([]byte{3}, 24), ServerKey: bytes.Repeat([]byte{4}, 24)}}
			s.checkpoint = store.Checkpoint{Directory: filepath.Join(directory, "checkpoints"), Binding: s.data.Binding}
			if err := s.saveLocked(); err != nil {
				t.Fatal(err)
			}
		}
		archive, err := OpenDeliveryArchive(directory)
		if err != nil {
			t.Fatal(err)
		}
		a := &soakAccount{adapter: BotAdapter{Service: s, Archive: archive}, metrics: metric, body: body, ready: make(chan struct{})}
		p.accounts = append(p.accounts, a)
		if err = archive.Recover(directory); err != nil {
			t.Fatal(err)
		}
		if err = RecoverBotIntents(directory, archive); err != nil {
			t.Fatal(err)
		}
		history, err := OpenEventHistory(directory)
		if err != nil {
			t.Fatal(err)
		}
		if err = a.adapter.EnableHistory(history); err != nil {
			_ = history.Close()
			t.Fatal(err)
		}
		if err = a.adapter.RecoverOutgoingMedia(); err != nil {
			t.Fatal(err)
		}
		if err = s.prepare(false, false); err != nil {
			t.Fatal(err)
		}
		s.mu.Lock()
		s.state, s.mode, s.sessionState = "connected", "online", "validated"
		s.mu.Unlock()
		s.SetMediaDownloader(func(context.Context, MediaDownloadRequest) (*cdn.Result, error) {
			metric.downloads.Add(1)
			return verifiedMedia(body), nil
		})
		a.adapter.wrapArchiveMediaDownloader()
		s.SetMediaSender(func(ctx context.Context, request MediaSendRequest) (MediaSendResult, error) {
			if !bytes.Equal(request.Data, body) || request.Kind != "image" {
				return MediaSendResult{}, errors.New("soak_sender_received_unexpected_content")
			}
			progress := request.Metadata
			progress.Stage, progress.FileID = "uploaded", "synthetic-soak-upload"
			progress.MD5 = fmt.Sprintf("%x", md5.Sum(body))
			progress.UploadSize = uint32(len(body))
			if err := request.SaveProgress(progress); err != nil {
				return MediaSendResult{}, err
			}
			progress.Stage = "submitting"
			if err := request.SaveProgress(progress); err != nil {
				return MediaSendResult{}, err
			}
			id := metric.sends.Add(1)
			return MediaSendResult{Receipt: protocol.MediaSubmissionReceipt{ServerAccepted: true, ServerID: 900000000 + id}, SubmissionStarted: true}, nil
		})
		server := onebot.NewServer(onebot.New(a.adapter, state), soakToken, "")
		a.server = server
		server.Runtime.Streams, err = onebot.OpenStreams(filepath.Join(directory, "stream-temp"))
		if err != nil {
			t.Fatal(err)
		}
		a.http = httptest.NewServer(server)
		server.Host = strings.TrimPrefix(a.http.URL, "http://")
		a.client, err = client.New(a.http.URL, soakToken)
		if err != nil {
			t.Fatal(err)
		}
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					// A fixture bug must not fall through to the real text sender.
					s.mu.RLock()
					for _, job := range s.data.Jobs {
						if job.Media == nil {
							s.mu.RUnlock()
							p.fail(errors.New("soak_refuses_real_text_transport"))
							return
						}
					}
					s.mu.RUnlock()
					if err := s.sendQueued(ctx); err != nil {
						p.fail(err)
						return
					}
					if err := s.downloadQueued(ctx); err != nil {
						p.fail(err)
						return
					}
				}
			}
		}()
		p.wg.Add(1)
		go func() { defer p.wg.Done(); a.adapter.RunArchive(ctx) }()
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.fail(a.client.Events(ctx, func(raw json.RawMessage) error { return a.consume(ctx, raw) }))
		}()
	}
	for _, a := range p.accounts {
		select {
		case <-a.ready:
		case err := <-p.errors:
			t.Fatal(err)
		case <-time.After(10 * time.Second):
			t.Fatal("soak_event_subscription_not_ready")
		}
	}
	return p
}

func (a *soakAccount) consume(parent context.Context, raw json.RawMessage) error {
	var event struct {
		PostType string           `json:"post_type"`
		Meta     string           `json:"meta_event_type"`
		Kind     string           `json:"message_type"`
		Self     int64            `json:"self_id"`
		User     int64            `json:"user_id"`
		Group    int64            `json:"group_id"`
		Message  int64            `json:"message_id"`
		Sequence uint64           `json:"wechat_sequence"`
		ID       string           `json:"wechat_event_id"`
		Segments []client.Segment `json:"message"`
	}
	if err := json.Unmarshal(raw, &event); err != nil {
		return err
	}
	if event.PostType == "meta_event" {
		if event.Meta == "lifecycle" {
			a.metrics.mu.Lock()
			if a.metrics.self != 0 && a.metrics.self != event.Self {
				a.metrics.mu.Unlock()
				return errors.New("soak_self_id_changed_on_restart")
			}
			a.metrics.self = event.Self
			a.metrics.mu.Unlock()
			close(a.ready)
		} else if event.Meta == "heartbeat" {
			a.metrics.heartbeats.Add(1)
		}
		return nil
	}
	started := time.Now()
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	if event.PostType != "message" || event.Sequence != a.metrics.completed.Load()+1 || event.ID == "" || event.Message <= 0 {
		return errors.New("soak_event_gap_duplicate_or_invalid_envelope")
	}
	a.metrics.mu.Lock()
	identityChanged := event.Self != a.metrics.self || event.User <= 0 || (a.metrics.firstUser != 0 && event.User != a.metrics.firstUser)
	a.metrics.mu.Unlock()
	if identityChanged {
		return errors.New("soak_canonical_sender_changed_across_chat_or_reopen")
	}
	expectedKind, expectedSegment := "private", "text"
	if event.Sequence%2 == 1 {
		expectedKind = "group"
	} else if event.Sequence%4 == 0 {
		expectedSegment = "image"
	}
	if event.Kind != expectedKind || (event.Kind == "group") != (event.Group > 0) || event.User == event.Self || len(event.Segments) != 1 || event.Segments[0].Type != expectedSegment {
		return errors.New("soak_injected_message_shape_changed")
	}
	if expectedSegment == "text" && event.Segments[0].Data["text"] != fmt.Sprintf("synthetic-soak-event-%d", event.Sequence) {
		return errors.New("soak_injected_text_body_changed")
	}
	for _, segment := range event.Segments {
		if segment.Type == "image" {
			if err := soakDownload(ctx, a.client, segment.Data["file"], a.body); err != nil {
				return fmt.Errorf("soak_incoming_download: %w", err)
			}
		}
	}
	if event.Kind == "group" && event.Sequence%12 == 1 {
		if _, err := a.client.Call(ctx, "get_group_member_info", map[string]any{"group_id": event.Group, "user_id": event.User}); err != nil {
			return err
		}
	}
	action := "send_private_msg"
	params := map[string]any{"user_id": event.User, "message": []client.Segment{client.Image(a.body)}, "wechat_request_id": "offline-soak/" + event.ID}
	if event.Kind == "group" {
		action = "send_group_msg"
		delete(params, "user_id")
		params["group_id"] = event.Group
	}
	response, err := a.client.Call(ctx, action, params)
	if err != nil {
		return err
	}
	var reply struct {
		ID int64 `json:"message_id"`
	}
	if json.Unmarshal(response.Data, &reply) != nil || reply.ID <= 0 {
		return errors.New("soak_reply_missing_verified_receipt")
	}
	if event.Sequence%16 == 0 {
		message, err := a.client.Call(ctx, "get_msg", map[string]any{"message_id": reply.ID})
		if err != nil {
			return err
		}
		var received struct {
			Message []client.Segment `json:"message"`
		}
		if json.Unmarshal(message.Data, &received) != nil || len(received.Message) != 1 || received.Message[0].Type != "image" {
			return errors.New("soak_reply_query_changed_media")
		}
		if err := soakDownload(ctx, a.client, received.Message[0].Data["file"], a.body); err != nil {
			return fmt.Errorf("soak_outgoing_download: %w", err)
		}
	}
	if event.Sequence%32 == 0 {
		data := bytes.Repeat([]byte{byte(event.Sequence % 251)}, (256<<10)+123)
		file, err := a.client.UploadFile(ctx, "synthetic-transfer.bin", bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return err
		}
		if err = soakDownload(ctx, a.client, file.FilePath, data); err != nil {
			return fmt.Errorf("soak_temp_roundtrip: %w", err)
		}
		if _, err = a.client.Call(ctx, "clean_stream_temp_file", nil); err != nil {
			return err
		}
		a.metrics.streams.Add(1)
	}
	a.metrics.mu.Lock()
	if a.metrics.firstMessage == 0 {
		a.metrics.firstMessage, a.metrics.firstUser, a.metrics.firstReply = event.Message, event.User, reply.ID
		a.metrics.firstAction, a.metrics.firstParams = action, params
		a.metrics.firstKind, a.metrics.firstGroup = event.Kind, event.Group
		if len(event.Segments) != 1 || event.Segments[0].Type != "text" {
			a.metrics.mu.Unlock()
			return errors.New("soak_first_text_fixture_changed")
		}
		a.metrics.firstBody = event.Segments[0].Data["text"]
	}
	a.metrics.latencyMax = max(a.metrics.latencyMax, time.Since(started))
	a.metrics.mu.Unlock()
	a.metrics.completed.Store(event.Sequence)
	return nil
}

func (p *soakPhase) wait(t *testing.T, expected uint64) {
	t.Helper()
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-p.errors:
			t.Fatal("soak worker/consumer failed", err)
		default:
		}
		ready := true
		for _, a := range p.accounts {
			ready = ready && a.metrics.completed.Load() == expected
		}
		if ready {
			return
		}
		select {
		case err := <-p.errors:
			t.Fatal("soak worker/consumer failed", err)
		case <-deadline.C:
			t.Fatal("soak consumer did not complete every injected event")
		case <-ticker.C:
		}
	}
}

func (p *soakPhase) verifyPersistentReferences(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(p.ctx, 15*time.Second)
	defer cancel()
	for index, a := range p.accounts {
		metric := a.metrics
		metric.mu.Lock()
		firstMessage, firstReply, action, params := metric.firstMessage, metric.firstReply, metric.firstAction, metric.firstParams
		firstUser, self, kind, group, body := metric.firstUser, metric.self, metric.firstKind, metric.firstGroup, metric.firstBody
		metric.mu.Unlock()
		if firstMessage == 0 {
			continue
		}
		for _, id := range []int64{firstMessage, firstReply} {
			response, err := a.client.Call(ctx, "get_msg", map[string]any{"message_id": id})
			if err != nil {
				t.Fatal("soak oldest message/receipt unavailable", err)
			}
			var message struct {
				ID       int64            `json:"message_id"`
				Real     int64            `json:"real_id"`
				User     int64            `json:"user_id"`
				Group    int64            `json:"group_id"`
				Kind     string           `json:"message_type"`
				Raw      string           `json:"raw_message"`
				Segments []client.Segment `json:"message"`
			}
			if json.Unmarshal(response.Data, &message) != nil || message.ID != id || message.Real != id || message.Kind != kind || message.Group != group || len(message.Segments) != 1 {
				t.Fatal("soak oldest message metadata changed")
			}
			if id == firstMessage {
				if message.User != firstUser || message.Raw != body || message.Segments[0].Type != "text" || message.Segments[0].Data["text"] != body {
					t.Fatal("soak oldest received body/sender changed")
				}
			} else {
				if message.User != self || message.Segments[0].Type != "image" || message.Segments[0].Data["file"] == "" {
					t.Fatal("soak oldest receipt media/sender changed")
				}
				file := message.Segments[0].Data["file"]
				metric.mu.Lock()
				oldFile := metric.firstReplyFile
				metric.firstReplyFile = file
				metric.mu.Unlock()
				if oldFile != "" && oldFile != file {
					t.Fatal("soak oldest outgoing file ID changed")
				}
				if err := soakDownload(ctx, a.client, file, a.body); err != nil {
					t.Fatal("soak oldest outgoing content changed", err)
				}
			}
		}
		before := metric.sends.Load()
		reply, err := a.client.Call(ctx, action, params)
		var same struct {
			ID int64 `json:"message_id"`
		}
		if err != nil || json.Unmarshal(reply.Data, &same) != nil || same.ID != firstReply || metric.sends.Load() != before {
			t.Fatal("soak old request changed or replayed", err)
		}
		other := p.accounts[1-index].metrics
		other.mu.Lock()
		otherMessage := other.firstMessage
		other.mu.Unlock()
		if otherMessage != 0 {
			_, err = a.client.Call(ctx, "get_msg", map[string]any{"message_id": otherMessage})
			var actionError *client.ActionError
			if !errors.As(err, &actionError) || actionError.Response.Retcode != 100 {
				t.Fatal("soak public message crossed account scope", err)
			}
		}
	}
}

func TestRuntimeOfflineSoak(t *testing.T) {
	if !mediaID(soakSourceManifestSHA, 64) {
		t.Fatal("run through tools/soak-offline-checks.sh to bind the source manifest")
	}
	duration := 10 * time.Minute
	if raw := os.Getenv("WECHAT_SOAK_DURATION"); raw != "" {
		var err error
		duration, err = time.ParseDuration(raw)
		if err != nil || duration < 8*time.Second || duration > time.Hour {
			t.Fatal("WECHAT_SOAK_DURATION must be8s..1h")
		}
	}
	root, body := t.TempDir(), mediaTestPNG(t)
	metrics := []*soakMetrics{{}, {}}
	started := time.Now()
	var expected uint64
	var samples []map[string]any
	var oldTemporary []string
	template, _, _ := mediaFixture(t, body, "image", "")
	imageTemplate := template.data.Messages[0]
	for phase := 0; phase < 4; phase++ {
		p := newSoakPhase(t, root, metrics, body, phase)
		p.verifyPersistentReferences(t)
		for index, reference := range oldTemporary {
			ctx, cancel := context.WithTimeout(p.ctx, 5*time.Second)
			var output bytes.Buffer
			_, err := p.accounts[index].client.DownloadFile(ctx, reference, &output)
			cancel()
			var actionError *client.ActionError
			if !errors.As(err, &actionError) || output.Len() != 0 {
				t.Fatal("soak temporary reference survived reopen", err)
			}
		}
		end := time.Now().Add(duration / 4)
		ticker := time.NewTicker(100 * time.Millisecond)
		for time.Now().Before(end) {
			<-ticker.C
			expected++
			for _, a := range p.accounts {
				s := a.adapter.Service
				s.mu.Lock()
				message := protocol.Message{Sender: []byte("friend"), Recipient: bytes.Clone(s.data.Session.Bootstrap.Username), Timestamp: uint32(1700000000 + expected), ServerID: 100000 + expected, Type: 1, Content: []byte(fmt.Sprintf("synthetic-soak-event-%d", expected))}
				if expected%2 == 1 {
					message.Sender = []byte("room@chatroom")
					message.Content = append([]byte("friend:\n"), message.Content...)
				} else if expected%4 == 0 {
					message.Type, message.Content = 3, bytes.Clone(imageTemplate.Content)
				}
				s.ingestLocked(protocol.SyncResult{Messages: []protocol.Message{message}})
				err := s.saveLocked()
				s.mu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			}
			p.wait(t, expected)
		}
		ticker.Stop()
		p.verifyPersistentReferences(t)
		oldTemporary = nil
		for index, a := range p.accounts {
			stats, err := a.adapter.Service.history.Stats(a.adapter.Snapshot().Account)
			if err != nil || stats.Cursor != expected || stats.Messages != expected || stats.WorkingCacheBytes > historyWorkingBytes {
				t.Fatal("soak history counter/window mismatch", stats, err)
			}
			streams, err := a.server.Runtime.Streams.Snapshot(a.adapter.Snapshot().Account)
			if err != nil || streams.FileCount != 0 || streams.Readers != 0 || streams.Uploads != 0 {
				t.Fatal("soak transfer ownership leaked", streams, err)
			}
			s := a.adapter.Service
			s.mu.RLock()
			if len(s.data.MediaJobs) != 0 || len(s.data.Jobs) > archiveHighWater+20 || s.calls["auth"] != 0 || s.calls["sync"] != 0 || s.category != "" {
				s.mu.RUnlock()
				t.Fatal("soak native working set or no-auth invariant failed")
			}
			s.mu.RUnlock()
			samples = append(samples, map[string]any{"phase": phase + 1, "account": index, "completed": a.metrics.completed.Load(), "history_cursor": stats.Cursor, "history_file_bytes": stats.FileBytes, "working_cache_bytes": stats.WorkingCacheBytes})
			file, err := a.client.UploadFile(p.ctx, "temporary-until-reopen.png", bytes.NewReader(body), int64(len(body)))
			if err != nil {
				t.Fatal(err)
			}
			oldTemporary = append(oldTemporary, file.FilePath)
		}
		if err := p.close(); err != nil {
			t.Fatal("soak shutdown retained owners", err)
		}
		runtime.GC()
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		samples = append(samples, map[string]any{"phase": phase + 1, "quiescent_goroutines": runtime.NumGoroutine(), "heap_alloc_bytes_after_gc": memory.HeapAlloc})
		t.Logf("offline soak phase%d complete; events/account%d", phase+1, expected)
	}
	if metrics[0].self == 0 || metrics[0].self == metrics[1].self || metrics[0].firstUser == metrics[1].firstUser {
		t.Fatal("soak accounts reused public identities")
	}
	var accounts []map[string]any
	for _, m := range metrics {
		if m.sends.Load() != expected || m.completed.Load() != expected || (duration >= time.Minute && (m.heartbeats.Load() == 0 || m.streams.Load() == 0)) {
			t.Fatal("soak did not exercise matching replies and heartbeats")
		}
		accounts = append(accounts, map[string]any{"events": m.completed.Load(), "synthetic_native_receipts": m.sends.Load(), "synthetic_downloads": m.downloads.Load(), "stream_roundtrips": m.streams.Load(), "heartbeats": m.heartbeats.Load(), "max_consumer_cycle_millis": m.latencyMax.Milliseconds()})
	}
	report := map[string]any{"source_manifest_sha256": soakSourceManifestSHA, "system": runtime.GOOS, "architecture": runtime.GOARCH, "configured_duration_millis": duration.Milliseconds(), "elapsed_millis": time.Since(started).Milliseconds(), "accounts": accounts, "phases": 4, "planned_restarts": 3, "oldest_queries_and_idempotency_after_reopen": true, "cross_account_public_id_fencing": true, "temporary_references_rejected_after_reopen": true, "samples": samples, "live_wechat_or_model_requests": 0, "production_session_worker": false, "scope": "timed synthetic ingestion and native media workers with fake CDN/receipts, real BotAdapter/history/archive/OneBot HTTP and forward event WS/Go SDK/temp streams; orderly reopen, not process crash, real platform or full deployment soak"}
	raw, _ := json.Marshal(report)
	t.Log("RUNTIME_SOAK_REPORT=" + string(raw))
}
