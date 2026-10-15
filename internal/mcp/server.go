// Package mcp exposes the runtime's action catalog through MCP stdio.
// Only the external OneBot HTTP client is available in execution mode.
package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/JFSAS/wechat-runtime/internal/catalog"
	"github.com/JFSAS/wechat-runtime/pkg/client"
)

type Server struct {
	Mode   string
	Client *client.Client
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
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	encoder := json.NewEncoder(output)
	for scanner.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var q request
		if json.Unmarshal(scanner.Bytes(), &q) != nil || q.Version != "2.0" {
			if err := encoder.Encode(response{Version: "2.0", ID: json.RawMessage("null"), Error: map[string]any{"code": -32700, "message": "invalid_json_rpc"}}); err != nil {
				return err
			}
			continue
		}
		if q.ID == nil {
			continue
		}
		out := response{Version: "2.0", ID: q.ID}
		result, err := s.handle(ctx, q)
		if err != nil {
			out.Error = map[string]any{"code": -32602, "message": err.Error()}
		} else {
			out.Result = result
		}
		if err = encoder.Encode(out); err != nil {
			return err
		}
	}
	return scanner.Err()
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
		return map[string]any{"protocolVersion": version, "capabilities": map[string]any{"tools": map[string]any{}, "resources": map[string]any{}}, "serverInfo": map[string]string{"name": "wechat-runtime", "version": "0.1.0-dev"}}, nil
	case "ping":
		return map[string]any{}, nil
	case "tools/list":
		tools := []any{tool("list_actions", "List available actions", map[string]any{}, true), tool("get_action", "Get one action and its parameter schema", map[string]any{"name": map[string]string{"type": "string"}}, true)}
		if s.Mode != "docs" {
			tools = append(tools, tool("query_action", "Call a read-only OneBot action", map[string]any{"action": map[string]string{"type": "string"}, "params": map[string]string{"type": "object"}}, true))
		}
		if s.Mode == "write" {
			tools = append(tools, tool("invoke_action", "Execute a OneBot action", map[string]any{"action": map[string]string{"type": "string"}, "params": map[string]string{"type": "object"}}, false))
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
			Name      string `json:"name"`
			Arguments struct {
				Name   string         `json:"name"`
				Action string         `json:"action"`
				Params map[string]any `json:"params"`
			} `json:"arguments"`
		}
		if json.Unmarshal(q.Params, &p) != nil {
			return nil, errors.New("invalid_tool_call")
		}
		switch p.Name {
		case "list_actions":
			return content(catalog.Actions(), false), nil
		case "get_action":
			a, ok := catalog.Find(p.Arguments.Name)
			if !ok {
				return content("action_unavailable", true), nil
			}
			return content(a, false), nil
		case "query_action", "invoke_action":
			if s.Mode == "docs" || p.Name == "invoke_action" && s.Mode != "write" {
				return content("execution_mode_denied", true), nil
			}
			a, ok := catalog.Find(p.Arguments.Action)
			if !ok {
				return content("action_unavailable", true), nil
			}
			if p.Name == "query_action" && !a.ReadOnly {
				return content("write_action_requires_invoke", true), nil
			}
			result, err := s.Client.Call(ctx, a.Name, p.Arguments.Params)
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
	} else if name == "query_action" || name == "invoke_action" {
		required = []string{"action"}
	}
	return map[string]any{"name": name, "description": description, "inputSchema": map[string]any{"type": "object", "properties": properties, "required": required}, "annotations": map[string]bool{"readOnlyHint": read, "destructiveHint": !read, "openWorldHint": name == "query_action" || name == "invoke_action"}}
}
func content(value any, failed bool) any {
	raw, _ := json.Marshal(value)
	return map[string]any{"content": []any{map[string]string{"type": "text", "text": string(raw)}}, "isError": failed}
}
