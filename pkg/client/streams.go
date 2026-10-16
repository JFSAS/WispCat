package client

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"strings"
	"time"
)

const MaxStreamFileBytes = 16 << 20
const StreamChunkBytes = 256 << 10

// Stream consumes NDJSON responses with bounded frames and requires a terminal
// packet plus EOF. Callback errors close the response; an incomplete stream is
// never treated as a successful download. Callbacks must discard partial data
// if Stream returns an error.
func (c *Client) Stream(ctx context.Context, action string, params any, onFrame func(Response) error) (Response, error) {
	var result Response
	if onFrame == nil {
		return result, errors.New("stream_callback_required")
	}
	response, err := c.doAction(ctx, action, params)
	if err != nil {
		return result, err
	}
	defer response.Body.Close()
	limited := &io.LimitedReader{R: response.Body, N: (24 << 20) + 1}
	scanner := bufio.NewScanner(limited)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	terminal := false
	count := 0
	for scanner.Scan() {
		count++
		if count > 1026 || terminal {
			return result, errors.New("invalid_onebot_stream")
		}
		var frame Response
		if json.Unmarshal(scanner.Bytes(), &frame) != nil || frame.Stream != "stream-action" {
			return result, errors.New("invalid_onebot_stream")
		}
		if frame.Status != "ok" || frame.Retcode != 0 {
			return frame, &ActionError{Response: frame}
		}
		var data struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(frame.Data, &data) != nil {
			return result, errors.New("invalid_onebot_stream")
		}
		switch data.Type {
		case "response", "reset":
			terminal = true
			result = frame
		case "stream":
			if err = onFrame(frame); err != nil {
				return result, err
			}
		default:
			return result, errors.New("invalid_onebot_stream")
		}
	}
	if scanner.Err() != nil || !terminal || limited.N <= 0 {
		return result, errors.New("incomplete_onebot_stream")
	}
	if err = onFrame(result); err != nil {
		return result, err
	}
	return result, nil
}

type StreamFile struct {
	FilePath   string `json:"file_path,omitempty"`
	FileName   string `json:"file_name"`
	FileSize   int64  `json:"file_size"`
	SHA256     string `json:"sha256"`
	FrameCount int    `json:"frame_count,omitempty"`
}

// UploadFile transfers exactly size bytes into encrypted temporary storage.
// The returned stream:// reference can be passed to an image/file message.
// It expires, is scoped to this account, and does not survive runtime restart.
func (c *Client) UploadFile(ctx context.Context, name string, input io.Reader, size int64) (StreamFile, error) {
	return c.UploadFileWithParams(ctx, name, input, size, nil)
}

// UploadFileWithParams preserves optional self_id and file_retention on every
// request, including failure cleanup. Transfer fields are owned by this method.
func (c *Client) UploadFileWithParams(ctx context.Context, name string, input io.Reader, size int64, params map[string]any) (StreamFile, error) {
	var result StreamFile
	if input == nil || size <= 0 || size > MaxStreamFileBytes {
		return result, errors.New("invalid_stream_size")
	}
	for key := range params {
		if key != "self_id" && key != "file_retention" {
			return result, errors.New("conflicting_upload_params")
		}
	}
	base := maps.Clone(params)
	call := func(ctx context.Context, fields map[string]any) (Response, error) {
		all := maps.Clone(base)
		if all == nil {
			all = map[string]any{}
		}
		maps.Copy(all, fields)
		return c.Call(ctx, "upload_file_stream", all)
	}
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return result, err
	}
	id := hex.EncodeToString(random)
	completed := false
	touched := false
	defer func() {
		if !completed && touched {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
			defer cancel()
			_, _ = call(cleanup, map[string]any{"stream_id": id, "reset": true})
		}
	}()
	chunks := int((size + StreamChunkBytes - 1) / StreamChunkBytes)
	hash := sha256.New()
	remaining := size
	for index := 0; index < chunks; index++ {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		n := int64(StreamChunkBytes)
		if remaining < n {
			n = remaining
		}
		data := make([]byte, int(n))
		if _, err := io.ReadFull(input, data); err != nil {
			return result, errors.New("stream_source_size_mismatch")
		}
		_, _ = hash.Write(data)
		touched = true
		ack, err := call(ctx, map[string]any{"stream_id": id, "filename": name, "total_chunks": chunks, "file_size": size, "chunk_index": index, "chunk_data": base64.StdEncoding.EncodeToString(data)})
		if err != nil {
			return result, err
		}
		if !validUploadAck(ack, id, "stream", "chunk_received", index+1, chunks) {
			return result, errors.New("invalid_upload_acknowledgement")
		}
		remaining -= n
	}
	var extra [1]byte
	if n, err := input.Read(extra[:]); n != 0 || err != io.EOF {
		return result, errors.New("stream_source_size_mismatch")
	}
	expected := hex.EncodeToString(hash.Sum(nil))
	response, err := call(ctx, map[string]any{"stream_id": id, "is_complete": true, "expected_sha256": expected, "file_size": size})
	if err != nil {
		return result, err
	}
	if !validUploadAck(response, id, "response", "file_complete", chunks, chunks) || json.Unmarshal(response.Data, &result) != nil || !strings.HasPrefix(result.FilePath, "stream://") || result.FilePath == "stream://" || result.FileSize != size || result.SHA256 != expected || result.FileName != name {
		return result, errors.New("invalid_upload_completion")
	}
	completed = true
	return result, nil
}

func validUploadAck(response Response, id, kind, status string, received, total int) bool {
	var value struct {
		Type     string `json:"type"`
		Status   string `json:"status"`
		ID       string `json:"stream_id"`
		Received int    `json:"received_chunks"`
		Total    int    `json:"total_chunks"`
	}
	return response.Stream == "stream-action" && json.Unmarshal(response.Data, &value) == nil && value.Type == kind && value.Status == status && value.ID == id && value.Received == received && value.Total == total
}

// DownloadFile validates frame order, byte counts and the complete SHA256.
// Writes may precede a later failure; callers should use a temporary destination
// and publish it only when this method succeeds.
func (c *Client) DownloadFile(ctx context.Context, reference string, output io.Writer) (StreamFile, error) {
	return c.DownloadStream(ctx, "download_file_stream", map[string]any{"file": reference}, output)
}

// DownloadStream validates a supported file/image action while preserving its
// account selector and chunk parameters. No local path is interpreted by it.
func (c *Client) DownloadStream(ctx context.Context, action string, params any, output io.Writer) (StreamFile, error) {
	var result StreamFile
	if action != "download_file_stream" && action != "download_file_image_stream" {
		return result, errors.New("invalid_download_action")
	}
	if output == nil {
		return result, errors.New("stream_destination_required")
	}
	info := false
	chunks := 0
	var total int64
	hash := sha256.New()
	_, err := c.Stream(ctx, action, params, func(frame Response) error {
		result.FrameCount++
		var data struct {
			Type        string `json:"type"`
			Kind        string `json:"data_type"`
			Name        string `json:"file_name"`
			Size        *int64 `json:"file_size"`
			Index       *int   `json:"index"`
			Data        string `json:"data"`
			ChunkSize   *int   `json:"size"`
			TotalBytes  *int64 `json:"total_bytes"`
			TotalChunks *int   `json:"total_chunks"`
			SHA256      string `json:"sha256"`
		}
		if json.Unmarshal(frame.Data, &data) != nil {
			return errors.New("invalid_stream_frame")
		}
		switch data.Kind {
		case "file_info":
			if info || data.Type != "stream" || data.Size == nil || *data.Size < 0 || *data.Size > MaxStreamFileBytes {
				return errors.New("invalid_stream_file_info")
			}
			info = true
			result.FileName = data.Name
			result.FileSize = *data.Size
		case "file_chunk":
			if !info || data.Type != "stream" || data.Index == nil || *data.Index != chunks || data.ChunkSize == nil || len(data.Data) > base64.StdEncoding.EncodedLen(StreamChunkBytes) {
				return errors.New("invalid_stream_chunk")
			}
			body, err := base64.StdEncoding.Strict().DecodeString(data.Data)
			if err != nil || base64.StdEncoding.EncodeToString(body) != data.Data || len(body) != *data.ChunkSize || len(body) == 0 || total+int64(len(body)) > result.FileSize {
				return errors.New("invalid_stream_chunk")
			}
			n, err := output.Write(body)
			if err != nil {
				return err
			}
			if n != len(body) {
				return io.ErrShortWrite
			}
			_, _ = hash.Write(body)
			total += int64(n)
			chunks++
		case "file_complete":
			if !info || data.Type != "response" || data.TotalBytes == nil || *data.TotalBytes != total || total != result.FileSize || data.TotalChunks == nil || *data.TotalChunks != chunks || data.SHA256 != hex.EncodeToString(hash.Sum(nil)) {
				return errors.New("invalid_stream_completion")
			}
			result.SHA256 = data.SHA256
		default:
			return errors.New("invalid_stream_frame")
		}
		return nil
	})
	return result, err
}
