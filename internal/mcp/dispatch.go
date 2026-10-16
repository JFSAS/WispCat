package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"
)

const maxPendingCalls = 8
const maxRequestIDBytes = 1024

type pendingCall struct {
	id        json.RawMessage
	cancel    context.CancelFunc
	cancelled bool
}

type completedCall struct {
	key    string
	result any
	err    error
}

// requestKey preserves the distinction between string and integer IDs without
// float conversion. Equivalent string escapes and integer negative zero match.
func requestKey(id json.RawMessage) (string, bool) {
	id = bytes.TrimSpace(id)
	if len(id) == 0 || len(id) > maxRequestIDBytes {
		return "", false
	}
	if id[0] == '"' {
		var value string
		if json.Unmarshal(id, &value) != nil || !validIDUnicode(id) {
			return "", false
		}
		return "s:" + value, true
	}
	value := string(id)
	digits := strings.TrimPrefix(value, "-")
	if digits == "" || len(digits) > 1 && digits[0] == '0' {
		return "", false
	}
	for _, digit := range digits {
		if digit < '0' || digit > '9' {
			return "", false
		}
	}
	if digits == "0" {
		value = "0"
	}
	return "n:" + value, true
}

// encoding/json replaces invalid UTF-8 and unpaired UTF-16 surrogates rather
// than rejecting them. Such replacement must not alias another request owner.
// The caller has already validated JSON string syntax and the encoded length.
func validIDUnicode(id []byte) bool {
	if !utf8.Valid(id) {
		return false
	}
	for i := 1; i < len(id)-1; i++ {
		if id[i] != '\\' {
			continue
		}
		i++
		if id[i] != 'u' {
			continue
		}
		value, _ := strconv.ParseUint(string(id[i+1:i+5]), 16, 16)
		if value >= 0xdc00 && value <= 0xdfff {
			return false
		}
		if value >= 0xd800 && value <= 0xdbff {
			if i+10 >= len(id)-1 || id[i+5] != '\\' || id[i+6] != 'u' {
				return false
			}
			low, err := strconv.ParseUint(string(id[i+7:i+11]), 16, 16)
			if err != nil || low < 0xdc00 || low > 0xdfff {
				return false
			}
			i += 6
		}
		i += 4
	}
	return true
}

func rpcFailure(id json.RawMessage, code int, message string) response {
	return response{Version: "2.0", ID: id, Error: map[string]any{"code": code, "message": message}}
}

func (s *Server) serve(ctx context.Context, input io.Reader, output io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() { cancel(); workers.Wait() }()
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	lines := make(chan []byte)
	readError := make(chan error, 1)
	go func() {
		defer func() { readError <- scanner.Err(); close(lines) }()
		for scanner.Scan() {
			line := bytes.Clone(scanner.Bytes())
			select {
			case lines <- line:
			case <-ctx.Done():
				return
			}
		}
	}()
	encoder := json.NewEncoder(output)
	pending := make(map[string]*pendingCall)
	// Each admitted worker sends exactly once. The bound lets all workers join
	// on output failure, EOF or shutdown without a reader draining their results.
	completed := make(chan completedCall, maxPendingCalls)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case done := <-completed:
			call := pending[done.key]
			call.cancel()
			if !call.cancelled {
				out := response{Version: "2.0", ID: call.id, Result: done.result}
				if done.err != nil {
					out = rpcFailure(call.id, -32602, done.err.Error())
				}
				if err := encoder.Encode(out); err != nil {
					return err
				}
			}
			delete(pending, done.key)
		case line, ok := <-lines:
			if !ok {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				// The input transport has closed. Deferred cancellation joins all
				// admitted actions before the caller may release file ownership.
				return <-readError
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			var q request
			if json.Unmarshal(line, &q) != nil {
				if err := encoder.Encode(rpcFailure(json.RawMessage("null"), -32700, "invalid_json_rpc")); err != nil {
					return err
				}
				continue
			}
			if q.Version != "2.0" || q.Method == "" {
				if err := encoder.Encode(rpcFailure(json.RawMessage("null"), -32600, "invalid_json_rpc")); err != nil {
					return err
				}
				continue
			}
			if q.ID == nil {
				if q.Method == "notifications/cancelled" {
					var params struct {
						ID json.RawMessage `json:"requestId"`
					}
					if json.Unmarshal(q.Params, &params) == nil {
						if key, valid := requestKey(params.ID); valid {
							if call := pending[key]; call != nil {
								call.cancelled = true
								call.cancel()
							}
						}
					}
				}
				continue
			}
			key, valid := requestKey(q.ID)
			if !valid || pending[key] != nil {
				if err := encoder.Encode(rpcFailure(json.RawMessage("null"), -32600, "invalid_or_pending_request_id")); err != nil {
					return err
				}
				continue
			}
			if q.Method != "tools/call" {
				// Bounded local control/catalog methods remain available while
				// all external action slots are occupied; only this loop writes.
				result, err := s.handle(ctx, q)
				out := response{Version: "2.0", ID: q.ID, Result: result}
				if err != nil {
					code := -32602
					if err.Error() == "method_unavailable" {
						code = -32601
					}
					out = rpcFailure(q.ID, code, err.Error())
				}
				if err := encoder.Encode(out); err != nil {
					return err
				}
				continue
			}
			if len(pending) >= maxPendingCalls {
				if err := encoder.Encode(rpcFailure(q.ID, -32000, "mcp_server_busy")); err != nil {
					return err
				}
				continue
			}
			callCtx, stop := context.WithCancel(ctx)
			pending[key] = &pendingCall{id: bytes.Clone(q.ID), cancel: stop}
			workers.Add(1)
			go func() {
				defer workers.Done()
				result, err := s.handle(callCtx, q)
				completed <- completedCall{key: key, result: result, err: err}
			}()
		}
	}
}
