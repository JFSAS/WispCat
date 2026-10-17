package onebot

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
	"strconv"
	"strings"
	"time"

	"github.com/JFSAS/wispcat/pkg/bot"
)

func streamAction(action string) bool {
	switch action {
	case "upload_file_stream", "download_file_stream", "download_file_image_stream", "test_download_stream":
		return true
	}
	return false
}

func streamEnvelope(q Request, out Response) Response {
	out.Echo = q.Echo
	if streamAction(q.Action) {
		out.Stream = "stream-action"
		if out.Status != "ok" {
			out.Data = map[string]any{"type": "error", "data_type": "error"}
		}
	}
	return out
}

// InvokeFrames keeps the normal action dispatcher and account validation while
// allowing one action to emit bounded intermediate responses. A failed sink
// cancels work and never receives an additional success terminal frame.
func (r *Runtime) InvokeFrames(ctx context.Context, q Request, emit func(Response) error) error {
	limit := r.ActionTimeout
	if limit <= 0 {
		limit = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	account := r.Channel.Snapshot().Account
	var sinkErr error
	out := r.invoke(ctx, q, func(data any) error {
		if ctx.Err() != nil {
			sinkErr = ctx.Err()
			return sinkErr
		}
		current := r.Channel.Snapshot()
		if current.Error != "" {
			sinkErr = bot.ErrStorage
			cancel()
			return sinkErr
		}
		if current.Account != account {
			sinkErr = bot.ErrScope
			cancel()
			return sinkErr
		}
		sinkErr = emit(streamEnvelope(q, good(data)))
		if sinkErr != nil {
			cancel()
		}
		return sinkErr
	})
	if sinkErr != nil {
		return sinkErr
	}
	if streamAction(q.Action) {
		if err := ctx.Err(); err != nil {
			return err
		}
		current := r.Channel.Snapshot()
		if current.Error != "" {
			out = fail(1500, "runtime_storage_unavailable")
		} else if current.Account != account {
			out = fail(100, "account_scope_changed")
		}
	}
	return emit(streamEnvelope(q, out))
}

func streamFailure(err error) Response {
	switch {
	case errors.Is(err, errStreamStorage), errors.Is(err, errStreamClosed):
		return fail(1500, "stream_storage_unavailable")
	case errors.Is(err, errStreamLimit):
		return fail(100, "stream_size_limit")
	case errors.Is(err, errStreamConflict):
		return fail(100, "stream_conflict")
	case errors.Is(err, errStreamMissing):
		return fail(100, "stream_unavailable")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return fail(1500, "request_cancelled")
	}
	return fail(1400, "invalid_stream_request")
}

func streamInteger(params map[string]json.RawMessage, key string) (int64, bool, error) {
	raw, ok := params[key]
	if !ok {
		return 0, false, nil
	}
	var value json.Number
	if json.Unmarshal(raw, &value) != nil {
		return 0, true, errStreamInvalid
	}
	n, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil || n < 0 {
		return 0, true, errStreamInvalid
	}
	return n, true, nil
}
func streamString(params map[string]json.RawMessage, key string) (string, error) {
	raw, ok := params[key]
	if !ok {
		return "", nil
	}
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return "", errStreamInvalid
	}
	return value, nil
}

func parseStreamUpload(params map[string]json.RawMessage) (streamUpload, error) {
	var p streamUpload
	var err error
	if p.ID, err = streamString(params, "stream_id"); err != nil {
		return p, err
	}
	if p.Name, err = streamString(params, "filename"); err != nil {
		return p, err
	}
	if p.SHA256, err = streamString(params, "expected_sha256"); err != nil {
		return p, err
	}
	p.SHA256 = strings.ToLower(p.SHA256)
	for key, field := range map[string]*bool{"is_complete": &p.Complete, "verify_only": &p.Verify, "reset": &p.Reset} {
		value, e := paramBool(params, key)
		if e != nil {
			return p, errStreamInvalid
		}
		*field = value
	}
	for _, key := range []string{"total_chunks", "chunk_index", "file_size", "file_retention"} {
		value, exists, e := streamInteger(params, key)
		if e != nil {
			return p, e
		}
		if !exists {
			continue
		}
		switch key {
		case "total_chunks":
			if value < 1 || value > streamChunkCount {
				return p, errStreamInvalid
			}
			p.Total = int(value)
		case "chunk_index":
			if value >= streamChunkCount {
				return p, errStreamInvalid
			}
			index := int(value)
			p.Index = &index
		case "file_size":
			p.Size = &value
		case "file_retention":
			if value > int64(time.Hour/time.Millisecond) {
				return p, errStreamInvalid
			}
			duration := time.Duration(value) * time.Millisecond
			p.Retention = &duration
		}
	}
	if raw, exists := params["chunk_data"]; exists {
		var data string
		if json.Unmarshal(raw, &data) != nil || len(data) > base64.StdEncoding.EncodedLen(streamChunkLimit) {
			return p, errStreamInvalid
		}
		if p.Data, err = base64.StdEncoding.Strict().DecodeString(data); err != nil {
			return p, errStreamInvalid
		}
		if p.Index == nil {
			return p, errStreamInvalid
		}
	}
	return p, nil
}

func (r *Runtime) invokeStream(ctx context.Context, q Request, snap bot.Snapshot, sink func(any) error) Response {
	if q.Action == "test_download_stream" {
		if sink == nil {
			return fail(100, "stream_transport_required")
		}
		wantError, err := paramBool(q.Params, "error")
		if err != nil {
			return streamFailure(err)
		}
		for i := 1; i <= 10; i++ {
			if err := sink(map[string]any{"type": "stream", "data_type": "data_chunk", "data": "Index-> " + strconv.Itoa(i)}); err != nil {
				return streamFailure(err)
			}
		}
		if wantError {
			return fail(100, "stream_test_error")
		}
		return good(map[string]any{"type": "response", "data_type": "data_complete", "data": "Stream transmission complete"})
	}
	if r.Streams == nil {
		return fail(100, "stream_storage_not_configured")
	}
	switch q.Action {
	case "upload_file_stream":
		p, err := parseStreamUpload(q.Params)
		if err != nil {
			return streamFailure(err)
		}
		out, err := r.Streams.upload(ctx, snap.Account, p)
		if err != nil {
			return streamFailure(err)
		}
		return good(out)
	case "clean_stream_temp_file":
		out, err := r.Streams.Clean(snap.Account)
		if err != nil {
			return streamFailure(err)
		}
		return good(out)
	case "download_file_stream", "download_file_image_stream":
		if sink == nil {
			return fail(100, "stream_transport_required")
		}
		file, err := streamString(q.Params, "file")
		if err != nil {
			return streamFailure(err)
		}
		if file == "" {
			file, err = streamString(q.Params, "file_id")
			if err != nil {
				return streamFailure(err)
			}
		}
		size, provided, err := streamInteger(q.Params, "chunk_size")
		if err != nil {
			return streamFailure(err)
		}
		if !provided {
			size = 64 << 10
		}
		if size < 16<<10 || size > streamChunkLimit {
			return streamFailure(errStreamInvalid)
		}
		reader, name, expected, digest, err := r.streamSource(ctx, snap.Account, file)
		if err != nil {
			if errors.Is(err, bot.ErrStorage) || errors.Is(err, errStateUnavailable) {
				return fail(1500, "runtime_storage_unavailable")
			}
			return streamFailure(err)
		}
		defer reader.Close()
		first := make([]byte, int(size))
		n, readErr := io.ReadFull(reader, first)
		if readErr != nil && !errors.Is(readErr, io.ErrUnexpectedEOF) && !errors.Is(readErr, io.EOF) {
			return streamFailure(readErr)
		}
		if q.Action == "download_file_image_stream" && !strings.HasPrefix(http.DetectContentType(first[:n]), "image/") {
			return fail(100, "image_unavailable")
		}
		if err = sink(map[string]any{"type": "stream", "data_type": "file_info", "file_name": name, "file_size": expected, "chunk_size": size}); err != nil {
			return streamFailure(err)
		}
		hash := sha256.New()
		total, chunks := int64(0), 0
		for n > 0 {
			total += int64(n)
			if total > streamFileLimit || chunks >= streamChunkCount {
				return streamFailure(errStreamLimit)
			}
			_, _ = hash.Write(first[:n])
			encoded := base64.StdEncoding.EncodeToString(first[:n])
			if err = sink(map[string]any{"type": "stream", "data_type": "file_chunk", "index": chunks, "data": encoded, "size": n, "base64_size": len(encoded), "progress": total * 100 / expected}); err != nil {
				return streamFailure(err)
			}
			chunks++
			if readErr != nil {
				break
			}
			n, readErr = io.ReadFull(reader, first)
			if readErr != nil && !errors.Is(readErr, io.ErrUnexpectedEOF) && !errors.Is(readErr, io.EOF) {
				return streamFailure(readErr)
			}
		}
		actual := hex.EncodeToString(hash.Sum(nil))
		if total != expected || digest != "" && actual != digest {
			return streamFailure(errStreamStorage)
		}
		return good(map[string]any{"type": "response", "data_type": "file_complete", "file_name": name, "total_chunks": chunks, "total_bytes": total, "sha256": actual})
	}
	return fail(1404, "action_not_supported")
}

func (r *Runtime) streamSource(ctx context.Context, account, file string) (io.ReadCloser, string, int64, string, error) {
	if strings.HasPrefix(file, "stream://") {
		reader, err := r.Streams.acquire(account, file)
		if err != nil {
			return nil, "", 0, "", err
		}
		return reader, reader.item.name, reader.item.bytes, reader.item.digest, nil
	}
	if strings.HasPrefix(file, "http://") || strings.HasPrefix(file, "https://") {
		data, err := fetchMedia(ctx, file)
		if err != nil {
			return nil, "", 0, "", errStreamMissing
		}
		return io.NopCloser(bytes.NewReader(data)), "download.bin", int64(len(data)), "", nil
	}
	conversation, source, err := r.fileReference(account, file)
	if err != nil {
		return nil, "", 0, "", err
	}
	content, err := r.Channel.Download(ctx, account, conversation, source)
	if err != nil {
		return nil, "", 0, "", err
	}
	if len(content.Data) > streamFileLimit {
		return nil, "", 0, "", errStreamLimit
	}
	return io.NopCloser(bytes.NewReader(content.Data)), content.Name, int64(len(content.Data)), "", nil
}

func (r *Runtime) streamUploadBytes(ctx context.Context, account, file string) ([]byte, error) {
	if r.Streams == nil {
		return nil, errStreamMissing
	}
	reader, err := r.Streams.acquire(account, file)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(reader, streamFileLimit+1))
	if err != nil {
		return nil, err
	}
	if len(data) > streamFileLimit {
		return nil, errStreamLimit
	}
	return data, ctx.Err()
}

func streamActions() []Action {
	self := map[string]any{"anyOf": []any{map[string]any{"type": "integer", "minimum": 1, "maximum": 2147483647}, map[string]any{"type": "string", "pattern": "^[1-9][0-9]*$"}}}
	upload := map[string]any{
		"self_id": self, "stream_id": map[string]any{"type": "string", "pattern": "^[A-Za-z0-9_-]{1,128}$"},
		"total_chunks":    map[string]any{"type": "integer", "minimum": 1, "maximum": streamChunkCount},
		"chunk_index":     map[string]any{"type": "integer", "minimum": 0, "maximum": streamChunkCount - 1},
		"chunk_data":      map[string]any{"type": "string", "maxLength": base64.StdEncoding.EncodedLen(streamChunkLimit)},
		"file_size":       map[string]any{"type": "integer", "minimum": 1, "maximum": streamFileLimit},
		"expected_sha256": map[string]any{"type": "string", "pattern": "^[A-Fa-f0-9]{64}$"},
		"filename":        map[string]any{"type": "string", "maxLength": 255},
		"file_retention":  map[string]any{"type": "integer", "minimum": 0, "maximum": 3600000},
		"reset":           map[string]any{"type": "boolean"}, "verify_only": map[string]any{"type": "boolean"}, "is_complete": map[string]any{"type": "boolean"},
	}
	download := map[string]any{"self_id": self, "file": map[string]any{"type": "string"}, "file_id": map[string]any{"type": "string"}, "chunk_size": map[string]any{"type": "integer", "minimum": 16 << 10, "maximum": streamChunkLimit}}
	return []Action{
		{Category: "streams", Name: "upload_file_stream", Summary: "Upload bounded encrypted chunks; returns a scoped stream:// reference", Schema: map[string]any{"type": "object", "required": []string{"stream_id"}, "properties": upload}, Streaming: true},
		{Category: "streams", Name: "clean_stream_temp_file", Summary: "Remove inactive temporary transfers; preserve active uploads and readers", Schema: map[string]any{"type": "object", "properties": map[string]any{"self_id": self}}},
		{Category: "streams", Name: "download_file_stream", Summary: "Stream a scoped upload, received media ID or bounded HTTP(S) source", ReadOnly: true, Schema: map[string]any{"type": "object", "properties": download}, Streaming: true},
		{Category: "streams", Name: "download_file_image_stream", Summary: "Stream image content through bounded response frames", ReadOnly: true, Schema: map[string]any{"type": "object", "properties": download}, Streaming: true},
		{Category: "streams", Name: "test_download_stream", Summary: "Exercise ten synthetic streaming frames without platform requests", ReadOnly: true, Schema: map[string]any{"type": "object", "properties": map[string]any{"self_id": self, "error": map[string]any{"type": "boolean"}}}, Streaming: true},
	}
}
