// Package mcp exposes the runtime's action catalog through MCP stdio.
// Only the external OneBot HTTP client is available in execution mode.
package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/JFSAS/wispcat/internal/catalog"
	"github.com/JFSAS/wispcat/pkg/client"
)

type Server struct {
	Mode      string
	Client    *client.Client
	Transfers *Transfers
}
type request struct {
	Version string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}
type response struct {
	Version string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   any             `json:"error,omitempty"`
}

func (s *Server) Run(ctx context.Context, input io.Reader, output io.Writer) error {
	if s.Mode != "docs" && s.Mode != "read" && s.Mode != "write" {
		return errors.New("invalid_mcp_mode")
	}
	if s.Mode != "docs" && s.Client == nil {
		return errors.New("mcp_endpoint_required")
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return s.serve(ctx, input, output)
}

func (s *Server) handle(ctx context.Context, q request) (any, error) {
	switch q.Method {
	case "initialize":
		var p struct {
			Protocol string `json:"protocolVersion"`
		}
		_ = json.Unmarshal(q.Params, &p)
		version := "2024-11-05"
		if p.Protocol == "2025-03-26" || p.Protocol == "2025-06-18" {
			version = p.Protocol
		}
		return map[string]any{"protocolVersion": version, "capabilities": map[string]any{"tools": map[string]any{}, "resources": map[string]any{}}, "serverInfo": map[string]string{"name": "wispcat-mcp", "version": "0.1.0-alpha.2"}}, nil
	case "ping":
		return map[string]any{}, nil
	case "tools/list":
		tools := catalogTools()
		if s.Mode != "docs" {
			tools = append(tools, tool("query_action", "Call a read-only OneBot action", map[string]any{"action": map[string]string{"type": "string"}, "params": map[string]string{"type": "object"}}, true))
		}
		if s.Mode == "write" {
			tools = append(tools, tool("invoke_action", "Execute a OneBot action; local file transfers require configured MCP host directories", map[string]any{"action": map[string]string{"type": "string"}, "params": map[string]string{"type": "object"}, "execution": map[string]any{"type": "object", "properties": map[string]any{"input_file": map[string]string{"type": "string"}}, "required": []string{"input_file"}, "additionalProperties": false}}, false))
		}
		return map[string]any{"tools": tools}, nil
	case "resources/list":
		return map[string]any{"resources": []any{map[string]string{"uri": "wechat://onebot/actions", "name": "OneBot action catalog", "mimeType": "application/json"}}}, nil
	case "resources/read":
		var p struct {
			URI string `json:"uri"`
		}
		if json.Unmarshal(q.Params, &p) != nil || p.URI != "wechat://onebot/actions" {
			return nil, errors.New("resource_unavailable")
		}
		return map[string]any{"contents": []any{map[string]string{"uri": p.URI, "mimeType": "application/json", "text": string(catalog.JSON)}}}, nil
	case "tools/call":
		var p struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if json.Unmarshal(q.Params, &p) != nil {
			return nil, errors.New("invalid_tool_call")
		}
		switch p.Name {
		case "list_actions", "get_action", "search_actions", "list_categories":
			return catalogCall(p.Name, p.Arguments), nil
		case "query_action", "invoke_action":
			var args struct {
				Action    string          `json:"action"`
				Params    map[string]any  `json:"params"`
				Execution json.RawMessage `json:"execution"`
			}
			if len(p.Arguments) != 0 && json.Unmarshal(p.Arguments, &args) != nil {
				return nil, errors.New("invalid_tool_call")
			}
			if s.Mode == "docs" || p.Name == "invoke_action" && s.Mode != "write" {
				return content("execution_mode_denied", true), nil
			}
			a, ok := catalog.Find(args.Action)
			if !ok {
				return content("action_unavailable", true), nil
			}
			if p.Name == "query_action" && !a.ReadOnly {
				return content("write_action_requires_invoke", true), nil
			}
			input := ""
			if len(args.Execution) != 0 {
				var execution struct {
					InputFile string `json:"input_file"`
				}
				decoder := json.NewDecoder(bytes.NewReader(args.Execution))
				decoder.DisallowUnknownFields()
				if decoder.Decode(&execution) != nil || execution.InputFile == "" || a.Name != "upload_file_stream" || p.Name != "invoke_action" {
					return content("invalid_file_execution", true), nil
				}
				input = execution.InputFile
			}
			fileDownload := a.Name == "download_file_stream" || a.Name == "download_file_image_stream"
			if fileDownload || input != "" {
				if s.Mode != "write" || p.Name != "invoke_action" {
					return content("local_file_requires_invoke", true), nil
				}
				if s.Transfers == nil {
					return content("mcp_file_transfers_not_configured", true), nil
				}
				result, err := s.Transfers.call(ctx, s.Client, a.Name, args.Params, input)
				if err != nil {
					return content(transferFailure(err), true), nil
				}
				return content(result, false), nil
			}
			if a.Streaming && a.Name != "upload_file_stream" {
				result, err := streamSummary(ctx, s.Client, a.Name, args.Params)
				if err != nil {
					return content("mcp_stream_failed", true), nil
				}
				return content(result, false), nil
			}
			result, err := s.Client.Call(ctx, a.Name, args.Params)
			if err != nil {
				var action *client.ActionError
				if errors.As(err, &action) {
					return content(action.Response, true), nil
				}
				return content("runtime_request_failed", true), nil
			}
			return content(result, false), nil
		default:
			return nil, errors.New("tool_unavailable")
		}
	default:
		return nil, errors.New("method_unavailable")
	}
}
func tool(name, description string, properties map[string]any, read bool) any {
	required := []string{}
	if name == "get_action" {
		required = []string{"name"}
	} else if name == "search_actions" {
		required = []string{"query"}
	} else if name == "query_action" || name == "invoke_action" {
		required = []string{"action"}
	}
	schema := map[string]any{"type": "object", "properties": properties, "required": required}
	if name != "query_action" && name != "invoke_action" {
		schema["additionalProperties"] = false
	}
	return map[string]any{"name": name, "description": description, "inputSchema": schema, "annotations": map[string]bool{"readOnlyHint": read, "destructiveHint": !read, "openWorldHint": name == "query_action" || name == "invoke_action"}}
}
func content(value any, failed bool) any {
	raw, _ := json.Marshal(value)
	return map[string]any{"content": []any{map[string]string{"type": "text", "text": string(raw)}}, "isError": failed}
}
