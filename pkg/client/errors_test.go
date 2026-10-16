package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func exchangeContext(t *testing.T, c *Client, mode string) (context.Context, func(), error) {
	t.Helper()
	switch mode {
	case "cancel":
		ctx, cancel := context.WithCancelCause(context.Background())
		t.Cleanup(func() { cancel(nil) })
		return ctx, func() { cancel(errors.New("private cancellation detail")) }, context.Canceled
	case "parent_deadline":
		ctx, cancel := context.WithTimeout(context.Background(), 450*time.Millisecond)
		t.Cleanup(cancel)
		return ctx, func() {}, context.DeadlineExceeded
	case "client_deadline":
		c.http.Timeout = 450 * time.Millisecond
		return context.Background(), func() {}, context.DeadlineExceeded
	default:
		t.Fatal(mode)
		return nil, nil, nil
	}
}

func exchangeWait(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("SDK exchange did not return")
		return nil
	}
}

func assertExchangeError(t *testing.T, err, category, stopped error) {
	t.Helper()
	if !errors.Is(err, category) || !errors.Is(err, stopped) {
		t.Fatal("exchange category or cancellation identity lost", err)
	}
	if strings.Contains(err.Error(), "private") || strings.Contains(err.Error(), "127.0.0.1") {
		t.Fatal("transport/cancel details escaped", err)
	}
}

type exchangeReadObserver struct {
	io.ReadCloser
	started   func()
	afterLine bool
	seenLine  bool
}

func (r *exchangeReadObserver) Read(p []byte) (int, error) {
	if !r.afterLine || r.seenLine {
		r.started()
	}
	n, err := r.ReadCloser.Read(p)
	if bytes.IndexByte(p[:n], '\n') >= 0 {
		r.seenLine = true
	}
	return n, err
}

func observeExchangeBody(t *testing.T, c *Client, afterLine bool) <-chan struct{} {
	t.Helper()
	ready := make(chan struct{})
	var once sync.Once
	transport := http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(transport.CloseIdleConnections)
	c.http.Transport = exchangeTransportFunc(func(r *http.Request) (*http.Response, error) {
		response, err := transport.RoundTrip(r)
		if err == nil {
			response.Body = &exchangeReadObserver{ReadCloser: response.Body, started: func() { once.Do(func() { close(ready) }) }, afterLine: afterLine}
		}
		return response, err
	})
	return ready
}

func awaitExchangeBody(t *testing.T, ready <-chan struct{}) {
	t.Helper()
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("SDK did not enter response-body reading")
	}
}

func TestSDKCallAndStreamKeepCancellationBeforeHeaders(t *testing.T) {
	for _, operation := range []string{"call", "stream"} {
		for _, mode := range []string{"cancel", "parent_deadline", "client_deadline"} {
			t.Run(operation+"/"+mode, func(t *testing.T) {
				started := make(chan struct{})
				finished := make(chan struct{})
				peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer close(finished)
					_, _ = io.Copy(io.Discard, r.Body)
					_ = r.Body.Close()
					close(started)
					<-r.Context().Done()
				}))
				t.Cleanup(peer.Close)
				c, _ := New(peer.URL+"/private-endpoint", "synthetic-token")
				ctx, trigger, stopped := exchangeContext(t, c, mode)
				done := make(chan error, 1)
				go func() {
					var err error
					if operation == "call" {
						_, err = c.Call(ctx, "get_status", nil)
					} else {
						_, err = c.Stream(ctx, "test_download_stream", nil, func(Response) error { return nil })
					}
					done <- err
				}()
				select {
				case <-started:
				case <-time.After(time.Second):
					t.Fatal("request did not start")
				}
				trigger()
				assertExchangeError(t, exchangeWait(t, done), ErrTransport, stopped)
				select {
				case <-finished:
				case <-time.After(time.Second):
					t.Fatal("canceled request retained the server handler")
				}
			})
		}
	}
}

func TestSDKCallKeepsBodyCancellationAndClientDeadline(t *testing.T) {
	for _, mode := range []string{"cancel", "parent_deadline", "client_deadline"} {
		t.Run(mode, func(t *testing.T) {
			started := make(chan struct{})
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				_ = r.Body.Close()
				w.Header().Set("Content-Length", "10000")
				_, _ = io.WriteString(w, `{"status":"ok","retcode":0,"data":{"accepted":true}}`)
				w.(http.Flusher).Flush()
				close(started)
				<-r.Context().Done()
			}))
			t.Cleanup(peer.Close)
			c, _ := New(peer.URL, "synthetic-token")
			reading := observeExchangeBody(t, c, false)
			ctx, trigger, stopped := exchangeContext(t, c, mode)
			done := make(chan error, 1)
			go func() { _, err := c.Call(ctx, "send_private_msg", nil); done <- err }()
			<-started
			awaitExchangeBody(t, reading)
			trigger()
			assertExchangeError(t, exchangeWait(t, done), ErrInvalidResponse, stopped)
		})
	}
}

func TestSDKStreamCancellationNeverPublishesTerminalWithoutEOF(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		for _, mode := range []string{"cancel", "parent_deadline", "client_deadline"} {
			t.Run(fmt.Sprint(terminal)+"/"+mode, func(t *testing.T) {
				started := make(chan struct{})
				peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.Copy(io.Discard, r.Body)
					_ = r.Body.Close()
					kind := "stream"
					if terminal {
						kind = "response"
					}
					_, _ = fmt.Fprintf(w, "{\"status\":\"ok\",\"retcode\":0,\"stream\":\"stream-action\",\"data\":{\"type\":%q}}\n", kind)
					w.(http.Flusher).Flush()
					close(started)
					<-r.Context().Done()
				}))
				t.Cleanup(peer.Close)
				c, _ := New(peer.URL, "synthetic-token")
				reading := observeExchangeBody(t, c, terminal)
				ctx, trigger, stopped := exchangeContext(t, c, mode)
				var published atomic.Int32
				done := make(chan error, 1)
				go func() {
					_, err := c.Stream(ctx, "test_download_stream", nil, func(frame Response) error {
						if strings.Contains(string(frame.Data), "response") {
							published.Add(1)
						}
						return nil
					})
					done <- err
				}()
				<-started
				awaitExchangeBody(t, reading)
				trigger()
				assertExchangeError(t, exchangeWait(t, done), ErrIncompleteStream, stopped)
				if published.Load() != 0 {
					t.Fatal("terminal was published before EOF")
				}
			})
		}
	}
}

type exchangeTransportFunc func(*http.Request) (*http.Response, error)

func (f exchangeTransportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSDKExchangeCategoriesSanitizeCausesAndKeepHTTPFailure(t *testing.T) {
	c, _ := New("http://127.0.0.1/private-endpoint", "synthetic-token")
	c.http.Transport = exchangeTransportFunc(func(*http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("private authorization detail: %w", context.DeadlineExceeded)
	})
	_, err := c.Call(context.Background(), "get_status", nil)
	assertExchangeError(t, err, ErrTransport, context.DeadlineExceeded)
	var exposed *url.Error
	if errors.As(err, &exposed) {
		t.Fatal("URL error exposed through unwrapping")
	}
	c.http.Transport = exchangeTransportFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 503, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("private upstream body"))}, nil
	})
	if _, err = c.Call(context.Background(), "get_status", nil); !errors.Is(err, ErrHTTPStatus) || strings.Contains(err.Error(), "private") {
		t.Fatal("HTTP category or privacy lost", err)
	}
}

type exchangeEOFCancel struct {
	*strings.Reader
	cancel context.CancelFunc
}

func (r exchangeEOFCancel) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF {
		r.cancel()
	}
	return n, err
}

func TestSDKCompleteResponsesSurviveLateCancellation(t *testing.T) {
	for _, success := range []bool{false, true} {
		t.Run(fmt.Sprint(success), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			body := `{"status":"failed","retcode":100,"message":"delivery_unknown","data":{"wechat_delivery_id":"known"}}`
			if success {
				body = `{"status":"ok","retcode":0,"data":{"message_id":123}}`
			}
			c, _ := New("http://127.0.0.1", "synthetic-token")
			c.http.Transport = exchangeTransportFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(exchangeEOFCancel{strings.NewReader(body), cancel})}, nil
			})
			response, err := c.Call(ctx, "send_private_msg", nil)
			var action *ActionError
			if ctx.Err() == nil || success && (err != nil || response.Retcode != 0) || !success && (!errors.As(err, &action) || action.Response.Message != "delivery_unknown") {
				t.Fatal("late cancellation overwrote a complete result", response, err)
			}
		})
	}
}

func TestSDKCanceledCommittedExchangeCanQueryWithoutAutomaticReplay(t *testing.T) {
	var commits atomic.Int32
	started := make(chan struct{})
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var params map[string]any
		_ = json.NewDecoder(r.Body).Decode(&params)
		_ = r.Body.Close()
		if params["wechat_request_id"] != "persisted-key" {
			t.Error("consumer key lost")
		}
		if r.URL.Path == "/send_private_msg" {
			commits.Add(1) // Synthetic unit ledger only; not native platform evidence.
			w.Header().Set("Content-Length", "10000")
			_, _ = io.WriteString(w, `{"status":"ok","retcode":0`)
			w.(http.Flusher).Flush()
			close(started)
			<-r.Context().Done()
			return
		}
		_, _ = io.WriteString(w, `{"status":"ok","retcode":0,"data":{"delivery":{"state":"accepted"},"receipt":{"wechat_delivery_id":"known"}}}`)
	}))
	defer peer.Close()
	c, _ := New(peer.URL, "synthetic-token")
	reading := observeExchangeBody(t, c, false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := c.Call(ctx, "send_private_msg", map[string]any{"wechat_request_id": "persisted-key"})
		done <- err
	}()
	<-started
	awaitExchangeBody(t, reading)
	cancel()
	assertExchangeError(t, exchangeWait(t, done), ErrInvalidResponse, context.Canceled)
	queried, err := c.Call(context.Background(), "wechat_get_delivery_by_request", map[string]any{"wechat_request_id": "persisted-key"})
	if err != nil || commits.Load() != 1 || !strings.Contains(string(queried.Data), "known") {
		t.Fatal("cancel was treated as rollback or replayed", commits.Load(), queried, err)
	}
}
