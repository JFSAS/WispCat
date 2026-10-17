package onebot

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JFSAS/wispcat/pkg/bot"
)

type deadlineProbeWriter struct {
	*httptest.ResponseRecorder
	mu       sync.Mutex
	deadline time.Time
	calls    int
	entered  chan struct{}
	release  chan struct{}
}

func (w *deadlineProbeWriter) SetWriteDeadline(deadline time.Time) error {
	w.mu.Lock()
	w.calls++
	call := w.calls
	w.mu.Unlock()
	if call == 2 && w.entered != nil {
		close(w.entered)
		<-w.release
	}
	w.mu.Lock()
	w.deadline = deadline
	w.mu.Unlock()
	return nil
}

func TestHTTPWriteCancellationWinsBeforeAndDuringStreamRenewal(t *testing.T) {
	for _, source := range []string{"request", "server"} {
		for _, phase := range []string{"before", "during"} {
			t.Run(source+"/"+phase, func(t *testing.T) {
				r, _ := fixture(t)
				s := NewServer(r, "synthetic-token", "")
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				defer s.cancel()
				writer := &deadlineProbeWriter{ResponseRecorder: httptest.NewRecorder()}
				budget, err := s.beginHTTPWrite(ctx, writer)
				if err != nil {
					t.Fatal(err)
				}
				cancelWrite := cancel
				if source == "server" {
					cancelWrite = s.cancel
				}
				if phase == "during" {
					writer.entered, writer.release = make(chan struct{}), make(chan struct{})
					renewed := make(chan error, 1)
					go func() { renewed <- budget.renew() }()
					<-writer.entered
					cancelWrite()
					close(writer.release)
					<-renewed
				} else {
					// Models cancellation after InvokeFrames' context check but
					// before the emitter attempts its next deadline renewal.
					cancelWrite()
				}
				budget.close() // Wait until the cancellation deadline is installed.
				if err := budget.renew(); !errors.Is(err, context.Canceled) {
					t.Fatal("canceled stream allowed another frame", err)
				}
				writer.mu.Lock()
				deadline := writer.deadline
				writer.mu.Unlock()
				if deadline.IsZero() || deadline.After(time.Now()) {
					t.Fatal("stream renewal overwrote the cancellation deadline", deadline)
				}
			})
		}
	}
}

type downloadContextChannel struct {
	mediaChannel
	downloadContext context.Context
}

func (c *downloadContextChannel) Download(ctx context.Context, _, _, _ string) (bot.Content, error) {
	c.downloadContext = ctx
	return c.content, nil
}

type downloadScopeWriter struct {
	*deadlineProbeWriter
	channel *downloadContextChannel
	t       *testing.T
}

func (w downloadScopeWriter) Write(data []byte) (int, error) {
	if w.channel.downloadContext == nil || !errors.Is(w.channel.downloadContext.Err(), context.Canceled) {
		w.t.Error("download context was not released before writing the response")
	}
	return w.ResponseRecorder.Write(data)
}

func TestHTTPMediaWriteSurvivesCompletedDownloadContext(t *testing.T) {
	r, c := fixture(t)
	channel := &downloadContextChannel{mediaChannel: mediaChannel{c, bot.Content{Data: []byte("download completed"), MIME: "text/plain"}}}
	r.Channel = channel
	s := NewServer(r, "synthetic-token", "127.0.0.1:12345")
	defer s.cancel()
	probe := &deadlineProbeWriter{ResponseRecorder: httptest.NewRecorder()}
	writer := downloadScopeWriter{probe, channel, t}
	request := httptest.NewRequest(http.MethodGet, s.mediaURL("account-a", "friend", "registered"), nil)
	s.ServeHTTP(writer, request)
	if probe.Code != http.StatusOK || probe.Body.String() != "download completed" || !probe.deadline.After(time.Now().Add(4*time.Second)) {
		t.Fatal("completed download consumed the response write budget", probe.Code, probe.Body.String(), probe.deadline)
	}
}

type smallWriteListener struct {
	net.Listener
	accepts atomic.Int32
}

func (l *smallWriteListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		l.accepts.Add(1)
		_ = conn.(*net.TCPConn).SetWriteBuffer(4096)
	}
	return conn, err
}

func blockedHTTPFixture(t *testing.T, kind string) (*Server, *httptest.Server, *net.TCPConn, <-chan struct{}) {
	t.Helper()
	r, c := fixture(t)
	data := bytes.Repeat([]byte("large synthetic response\n"), (8<<20)/25+1)
	r.Channel = mediaChannel{c, bot.Content{Data: data, MIME: "application/octet-stream", Name: "large.bin"}}
	s := NewServer(r, "synthetic-http-write-token", "")
	done := make(chan struct{})
	var once sync.Once
	peer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		defer once.Do(func() { close(done) })
		s.ServeHTTP(w, q)
	}))
	peer.Listener = &smallWriteListener{Listener: peer.Listener}
	peer.Start()
	t.Cleanup(func() { peer.CloseClientConnections(); peer.Close(); _ = s.Close(context.Background()) })
	s.Host = strings.TrimPrefix(peer.URL, "http://")
	path := s.mediaURL("account-a", "friend", "registered")
	if kind == "json" {
		event := bot.Event{ID: "large-observed", Account: "account-a", Conversation: bot.Conversation{ID: "friend", Kind: "private"}, Sender: bot.User{ID: "friend"}, Message: []bot.Segment{{Type: "text", Data: map[string]string{"text": string(data)}}}}
		c.events = []bot.Event{event}
		id, err := r.State.ID("account-a", "message", event.ID)
		if err != nil {
			t.Fatal(err)
		}
		path = peer.URL + "/get_msg?message_id=" + fmt.Sprint(id)
	} else if kind == "stream" {
		store, err := OpenStreams(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		r.Streams = store
		id, err := r.State.ID("account-a", "file", "friend\x00registered")
		if err != nil {
			t.Fatal(err)
		}
		path = peer.URL + "/download_file_stream?file=" + fmt.Sprint(id)
	}
	address, err := net.ResolveTCPAddr("tcp", s.Host)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialTCP("tcp", nil, address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetReadBuffer(4096)
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	request, _ := http.NewRequest("GET", path, nil)
	request.Header.Set("Authorization", "Bearer "+s.Token)
	if err = request.Write(conn); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), request)
	if err != nil || response.StatusCode != 200 {
		t.Fatal("large response did not start", err)
	}
	// Deliberately leave the body unread. Both TCP buffers are much smaller
	// than the payload, so the handler must retain its admitted request slot.
	select {
	case <-done:
		t.Fatal("fixture response did not encounter backpressure")
	default:
	}
	return s, peer, conn, done
}

func TestHTTPBackpressureCancellationJoinsMediaJSONAndStream(t *testing.T) {
	for _, kind := range []string{"media", "json", "stream"} {
		t.Run(kind, func(t *testing.T) {
			s, _, _, done := blockedHTTPFixture(t, kind)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := s.Close(ctx); err != nil {
				t.Fatal("server cancellation left a blocked response owner", err)
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("HTTP wrapper did not observe the joined handler")
			}
			if len(s.httpSlots) != 0 {
				t.Fatal("blocked HTTP slot leaked")
			}
		})
	}
}

func TestHTTPMediaSlowReaderHasBoundedWriteLifetime(t *testing.T) {
	s, _, _, done := blockedHTTPFixture(t, "media")
	select {
	case <-done:
	case <-time.After(responseWriteTimeout + 3*time.Second):
		t.Fatal("unread media response held its slot without a deadline")
	}
	if len(s.httpSlots) != 0 {
		t.Fatal("timed-out media response retained request slot")
	}
}

func TestHTTPAbandonedLargeMediaCanRetryAndReuseConnection(t *testing.T) {
	s, peer, conn, done := blockedHTTPFixture(t, "media")
	_ = conn.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("abandoned response did not join")
	}
	client := &http.Client{Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
	link := s.mediaURL("account-a", "friend", "registered")
	var sum [32]byte
	for index := range 2 {
		response, err := client.Get(link)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil || len(body) < 8<<20 || response.StatusCode != 200 {
			t.Fatal("retry failed", len(body), err)
		}
		if index == 0 {
			sum = sha256.Sum256(body)
		} else if sha256.Sum256(body) != sum {
			t.Fatal("retry changed media content")
		}
	}
	if peer.Listener.(*smallWriteListener).accepts.Load() != 2 {
		t.Fatal("write deadline prevented keep-alive reuse")
	}
}
