package mcp

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/JFSAS/wispcat/pkg/client"
)

// Only the synthetic non-file action is summarized. File payloads can never
// enter a tool result through this path, even from a misbehaving endpoint.
func streamSummary(ctx context.Context, c *client.Client, action string, params any) (any, error) {
	if action != "test_download_stream" {
		return nil, errors.New("unsupported_stream_action")
	}
	frames := []client.Response{}
	bytes := 0
	terminal, err := c.Stream(ctx, action, params, func(frame client.Response) error {
		var data struct {
			Type string `json:"type"`
			Kind string `json:"data_type"`
		}
		if json.Unmarshal(frame.Data, &data) != nil || data.Type == "stream" && data.Kind != "data_chunk" || data.Type != "stream" && (data.Type != "response" || data.Kind != "data_complete") {
			return errors.New("unexpected_summary_frame")
		}
		encoded, _ := json.Marshal(frame)
		bytes += len(encoded)
		if bytes > 64<<10 || len(frames) >= 64 {
			return errors.New("mcp_stream_summary_limit")
		}
		frames = append(frames, frame)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{"frames": frames, "frame_count": len(frames), "terminal": terminal}, nil
}
